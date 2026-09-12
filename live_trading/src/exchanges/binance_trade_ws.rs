/// Binance Futures WebSocket API client for ultra-low-latency order placement.
///
/// Instead of sending orders via HTTP POST to `fapi.binance.com` (which routes
/// through Cloudflare CDN adding ~150-180ms), this client sends order frames over
/// a persistent WebSocket connection to `wss://ws-fapi.binance.com/ws-fapi/v1`,
/// bypassing the CDN entirely and achieving ~10-15ms order RTT from Tokyo.
///
/// Protocol:
///   1. Connect to `wss://ws-fapi.binance.com/ws-fapi/v1`
///   2. Each order message is self-authenticated (apiKey + timestamp + signature in params)
///   3. Response arrives on the same WS, matched by `id` field
///   4. Ping every 3 minutes to keep connection alive (Binance disconnects after 5 min idle)
///   5. Auto-reconnect on any disconnect
///
/// IMPORTANT: Unlike Bybit's Trade WS (sign once on connect), Binance WS API
/// requires HMAC-SHA256 signature per message. However, HMAC computation takes
/// only ~0.1ms so the overhead is negligible.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use serde::Deserialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{oneshot, Mutex};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

type HmacSha256 = Hmac<Sha256>;

/// WebSocket API base URL for Binance USDS-M Futures.
const WS_API_URL: &str = "wss://ws-fapi.binance.com/ws-fapi/v1";

/// Response from a WS API order.place call.
#[derive(Debug, Deserialize)]
#[allow(non_snake_case, dead_code)]
pub struct WsOrderResult {
    pub orderId: Option<u64>,
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub clientOrderId: String,
    #[serde(default)]
    pub avgPrice: String,
    #[serde(default)]
    pub executedQty: String,
    #[serde(default)]
    pub cumQuote: String,
    #[serde(default)]
    pub origQty: String,
    #[serde(default)]
    pub updateTime: u64,
}

/// Top-level WS API response envelope.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct WsApiResponse {
    id: String,
    status: i32,
    result: Option<serde_json::Value>,
    error: Option<WsApiError>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case, dead_code)]
struct WsApiError {
    code: i64,
    msg: String,
}

/// Sender type for pending WS API requests — delivers the raw JSON result.
type PendingRequest = oneshot::Sender<Result<serde_json::Value, String>>;

/// Map of reqId → pending response channel.
type PendingRequestMap = Arc<DashMap<String, PendingRequest>>;

/// Write half of the WebSocket connection, wrapped in a Mutex for thread-safe sends.
type WsSink = Arc<Mutex<Option<futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>
    >,
    Message
>>>>;

/// Binance WS API client handle.
/// Clone-friendly — all internal state is behind Arc.
#[derive(Clone)]
pub struct BinanceTradeWs {
    api_key:      String,
    api_secret:   String,
    sink:         WsSink,
    pending:      PendingRequestMap,
    connected:    Arc<AtomicBool>,
    req_counter:  Arc<AtomicU64>,
}

impl BinanceTradeWs {
    /// Create a new BinanceTradeWs handle. Does NOT connect yet — call `spawn_connection` to start.
    pub fn new(api_key: String, api_secret: String) -> Self {
        BinanceTradeWs {
            api_key,
            api_secret,
            sink:         Arc::new(Mutex::new(None)),
            pending:      Arc::new(DashMap::new()),
            connected:    Arc::new(AtomicBool::new(false)),
            req_counter:  Arc::new(AtomicU64::new(1)),
        }
    }

    /// Returns true if the WS connection is currently active.
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    /// Generate a unique request ID for WS API request/response matching.
    fn next_req_id(&self) -> String {
        let n = self.req_counter.fetch_add(1, Ordering::Relaxed);
        format!("arb_ws_{}", n)
    }

    /// Sign a query string with HMAC-SHA256 (same as REST API).
    #[inline]
    fn sign(&self, payload: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC key error");
        mac.update(payload.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Get current timestamp in milliseconds.
    #[inline]
    fn timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// Spawn the WebSocket connection loop as a background task.
    /// Automatically reconnects on disconnect with a 1-second backoff.
    pub fn spawn_connection(&self) {
        let handle = self.clone();
        tokio::spawn(async move {
            loop {
                eprintln!("[BinanceTradeWS] Connecting to {}...", WS_API_URL);
                handle.run_connection().await;
                handle.connected.store(false, Ordering::Relaxed);
                eprintln!("[BinanceTradeWS] Disconnected — reconnecting in 1s...");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
    }

    /// Run a single WebSocket connection lifecycle.
    async fn run_connection(&self) {
        let ws_url = match url::Url::parse(WS_API_URL) {
            Ok(u) => u,
            Err(e) => {
                eprintln!("[BinanceTradeWS] Invalid URL: {}", e);
                return;
            }
        };

        let (ws_stream, _) = match connect_async(ws_url).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[BinanceTradeWS] Connection failed: {}", e);
                return;
            }
        };

        let (write, mut read) = ws_stream.split();

        // Store the write half for place_order() to use
        {
            let mut sink_guard = self.sink.lock().await;
            *sink_guard = Some(write);
        }
        self.connected.store(true, Ordering::Relaxed);
        eprintln!("[BinanceTradeWS] Connected to Binance WS API");

        // Spawn a ping task to keep the connection alive (every 3 minutes)
        let sink_for_ping = self.sink.clone();
        let connected_for_ping = self.connected.clone();
        let ping_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(180));
            loop {
                interval.tick().await;
                if !connected_for_ping.load(Ordering::Relaxed) {
                    break;
                }
                let mut sink_guard = sink_for_ping.lock().await;
                if let Some(ref mut sink) = *sink_guard {
                    if sink.send(Message::Ping(vec![])).await.is_err() {
                        eprintln!("[BinanceTradeWS] Ping failed");
                        break;
                    }
                }
            }
        });

        // Read loop — dispatch responses to pending request channels
        while let Some(msg) = read.next().await {
            let text = match msg {
                Ok(Message::Text(t)) => t,
                Ok(Message::Ping(p)) => {
                    let mut sink_guard = self.sink.lock().await;
                    if let Some(ref mut sink) = *sink_guard {
                        let _ = sink.send(Message::Pong(p)).await;
                    }
                    continue;
                }
                Ok(Message::Pong(_)) => continue,
                Ok(Message::Close(_)) => {
                    eprintln!("[BinanceTradeWS] Server closed connection");
                    break;
                }
                Err(e) => {
                    eprintln!("[BinanceTradeWS] Read error: {}", e);
                    break;
                }
                _ => continue,
            };

            // Parse the response and dispatch to the waiting caller
            match serde_json::from_str::<WsApiResponse>(&text) {
                Ok(resp) => {
                    let req_id = resp.id.clone();
                    if let Some((_, sender)) = self.pending.remove(&req_id) {
                        if resp.status == 200 {
                            if let Some(result) = resp.result {
                                let _ = sender.send(Ok(result));
                            } else {
                                let _ = sender.send(Err("Empty result in 200 response".to_string()));
                            }
                        } else {
                            let err_msg = resp.error
                                .map(|e| format!("Binance WS API error {}: {}", e.code, e.msg))
                                .unwrap_or_else(|| format!("WS API status {}", resp.status));
                            let _ = sender.send(Err(err_msg));
                        }
                    }
                    // else: response for an already-timed-out request, ignore
                }
                Err(e) => {
                    // Not a WS API response (could be a stream event) — log only at debug level
                    eprintln!("[BinanceTradeWS] Non-API message (parse err: {}): {}", e, &text[..text.len().min(200)]);
                }
            }
        }

        // Cleanup
        ping_task.abort();
        self.connected.store(false, Ordering::Relaxed);
        {
            let mut sink_guard = self.sink.lock().await;
            *sink_guard = None;
        }
        // Fail all pending requests so callers don't hang
        let keys: Vec<String> = self.pending.iter().map(|r| r.key().clone()).collect();
        for key in keys {
            if let Some((_, sender)) = self.pending.remove(&key) {
                let _ = sender.send(Err("WS disconnected".to_string()));
            }
        }
    }

    /// Place a market or limit order via the WebSocket API.
    ///
    /// Returns the parsed order result on success.
    /// `side` should be "BUY" or "SELL".
    /// `client_order_id` is a pre-generated ID for WS fill matching.
    /// `reduce_only` should be `true` for close orders.
    pub async fn place_order(
        &self,
        symbol:          &str,
        side:            &str,
        quantity:        f64,
        client_order_id: &str,
        reduce_only:     bool,
        price:           Option<f64>,
    ) -> Result<WsOrderResult, String> {
        if !self.is_connected() {
            return Err("BinanceTradeWS not connected".to_string());
        }

        let req_id = self.next_req_id();
        let ts = Self::timestamp_ms();
        let reduce_only_str = if reduce_only { "true" } else { "false" };

        // Build the query string for signature computation (same format as REST).
        // Binance WS API signs the alphabetically-sorted params as a query string.
        let (order_type, tif) = if price.is_some() {
            ("LIMIT", "IOC")
        } else {
            ("MARKET", "GTC")
        };

        let query = if let Some(p) = price {
            format!(
                "apiKey={}&newClientOrderId={}&price={:.6}&quantity={:.8}&reduceOnly={}&side={}&symbol={}&timeInForce={}&timestamp={}&type={}",
                self.api_key, client_order_id, p, quantity, reduce_only_str, side, symbol, tif, ts, order_type
            )
        } else {
            format!(
                "apiKey={}&newClientOrderId={}&quantity={:.8}&reduceOnly={}&side={}&symbol={}&timestamp={}&type={}",
                self.api_key, client_order_id, quantity, reduce_only_str, side, symbol, ts, order_type
            )
        };

        let signature = self.sign(&query);

        // Build the WS API request frame
        let mut params = serde_json::json!({
            "apiKey": self.api_key,
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": format!("{:.8}", quantity),
            "newClientOrderId": client_order_id,
            "reduceOnly": reduce_only_str,
            "timestamp": ts,
            "signature": signature,
        });

        if let Some(p) = price {
            params["timeInForce"] = serde_json::json!(tif);
            params["price"] = serde_json::json!(format!("{:.6}", p));
        }

        let frame = serde_json::json!({
            "id": req_id,
            "method": "order.place",
            "params": params,
        });

        let frame_str = frame.to_string();

        // Register the response channel BEFORE sending
        let (tx, rx) = oneshot::channel::<Result<serde_json::Value, String>>();
        self.pending.insert(req_id.clone(), tx);

        if let Some(p) = price {
            eprintln!(
                "[BinanceTradeWS] Sending {} {} {} @ LIMIT IOC {:.6} (reqId={}, clientId={})",
                side, quantity, symbol, p, req_id, client_order_id
            );
        } else {
            eprintln!(
                "[BinanceTradeWS] Sending {} {} {} @ MARKET (reqId={}, clientId={})",
                side, quantity, symbol, req_id, client_order_id
            );
        }

        // Send the frame
        {
            let mut sink_guard = self.sink.lock().await;
            match sink_guard.as_mut() {
                Some(sink) => {
                    if let Err(e) = sink.send(Message::Text(frame_str)).await {
                        self.pending.remove(&req_id);
                        return Err(format!("WS send failed: {}", e));
                    }
                }
                None => {
                    self.pending.remove(&req_id);
                    return Err("WS sink not available".to_string());
                }
            }
        }

        // Wait for the response with a 500ms timeout (generous for a WS RTT of ~10-15ms).
        // This timeout covers extreme cases like WS congestion.
        let timeout = std::time::Duration::from_millis(500);
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(result_json))) => {
                // Parse the WsOrderResult from the response
                match serde_json::from_value::<WsOrderResult>(result_json.clone()) {
                    Ok(order) => {
                        eprintln!(
                            "[BinanceTradeWS] Order accepted: id={:?} status={} filled={} avgPrice={}",
                            order.orderId, order.status, order.executedQty, order.avgPrice
                        );
                        Ok(order)
                    }
                    Err(e) => {
                        Err(format!("Failed to parse WS order result: {} | raw: {}", e, result_json))
                    }
                }
            }
            Ok(Ok(Err(api_err))) => Err(api_err),
            Ok(Err(_)) => {
                self.pending.remove(&req_id);
                Err("WS response channel closed".to_string())
            }
            Err(_) => {
                self.pending.remove(&req_id);
                Err(format!("WS order timeout ({}ms) for reqId={}", timeout.as_millis(), req_id))
            }
        }
    }
}
