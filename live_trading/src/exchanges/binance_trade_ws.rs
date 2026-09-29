use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
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
///   4. Ping every 60s to keep connection alive (Binance disconnects after 5 min idle)
///   5. Liveness probe every 120s via `account.status` to detect zombie connections
///   6. Auto-reconnect on any disconnect with instant retry
///
/// IMPORTANT: Unlike Bybit's Trade WS (sign once on connect), Binance WS API
/// requires HMAC-SHA256 signature per message. However, HMAC computation takes
/// only ~0.1ms so the overhead is negligible.
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{oneshot, Mutex};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

type HmacSha256 = Hmac<Sha256>;

/// WebSocket API base URL for Binance USDS-M Futures.
const WS_API_URL: &str = "wss://ws-fapi.binance.com/ws-fapi/v1";

/// Path to the WS debug log file. This file captures ALL WS events
/// (connect, disconnect, ping, pong, probe, zombie, order routing)
/// so we can diagnose connection drops even with the TUI active.
const WS_LOG_PATH: &str = "ws_debug.log";

/// Write a timestamped line to both stderr and the WS debug log file.
/// This ensures WS diagnostics are always available even when the TUI
/// overwrites the terminal.
macro_rules! ws_log {
    ($($arg:tt)*) => {{
        let msg = format!($($arg)*);
        let ts = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let line = format!("[{}] {}", ts, msg);
        eprintln!("{}", line);
        // Best-effort append to log file
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(WS_LOG_PATH)
        {
            let _ = writeln!(f, "{}", line);
        }
    }};
}

/// How often to send WebSocket pings (seconds).
/// Binance disconnects after 5 min idle. We ping every 60s = 5x safety margin.
const PING_INTERVAL_SECS: u64 = 60;

/// How often to send a lightweight signed request (`account.status`) to keep the
/// connection warm with real traffic and detect zombie connections (seconds).
const LIVENESS_PROBE_INTERVAL_SECS: u64 = 120;

/// If no pong is received within this many seconds after a ping, consider the
/// connection dead and force reconnect.
const PONG_TIMEOUT_SECS: i64 = 15;

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
type WsSink = Arc<
    Mutex<
        Option<
            futures_util::stream::SplitSink<
                tokio_tungstenite::WebSocketStream<
                    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
                >,
                Message,
            >,
        >,
    >,
>;

/// Binance WS API client handle.
/// Clone-friendly — all internal state is behind Arc.
#[derive(Clone)]
pub struct BinanceTradeWs {
    api_key: String,
    api_secret: String,
    sink: WsSink,
    pending: PendingRequestMap,
    connected: Arc<AtomicBool>,
    req_counter: Arc<AtomicU64>,
    /// Epoch millis of the last pong received. Used to detect zombie connections.
    last_pong_ms: Arc<AtomicI64>,
    /// Total number of successful reconnections since process start.
    reconnect_count: Arc<AtomicU64>,
    /// Epoch millis of the last disconnect. Used to track reconnect gaps.
    last_disconnect_ms: Arc<AtomicI64>,
}

impl BinanceTradeWs {
    /// Create a new BinanceTradeWs handle. Does NOT connect yet — call `spawn_connection` to start.
    pub fn new(api_key: String, api_secret: String) -> Self {
        BinanceTradeWs {
            api_key,
            api_secret,
            sink: Arc::new(Mutex::new(None)),
            pending: Arc::new(DashMap::new()),
            connected: Arc::new(AtomicBool::new(false)),
            req_counter: Arc::new(AtomicU64::new(1)),
            last_pong_ms: Arc::new(AtomicI64::new(0)),
            reconnect_count: Arc::new(AtomicU64::new(0)),
            last_disconnect_ms: Arc::new(AtomicI64::new(0)),
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
        let mut mac =
            HmacSha256::new_from_slice(self.api_secret.as_bytes()).expect("HMAC key error");
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

    /// Current epoch millis (i64 for AtomicI64 storage).
    #[inline]
    fn epoch_ms_i64() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    /// Spawn the WebSocket connection loop as a background task.
    /// Automatically reconnects on disconnect with 500ms backoff.
    pub fn spawn_connection(&self) {
        let handle = self.clone();
        tokio::spawn(async move {
            loop {
                ws_log!("[BinanceTradeWS] Connecting to {}...", WS_API_URL);
                handle.run_connection().await;
                handle.connected.store(false, Ordering::Relaxed);
                handle.last_disconnect_ms.store(Self::epoch_ms_i64(), Ordering::Relaxed);
                let n = handle.reconnect_count.fetch_add(1, Ordering::Relaxed) + 1;
                ws_log!(
                    "[BinanceTradeWS] ❌ Disconnected — reconnecting IMMEDIATELY (reconnect #{})",
                    n
                );
                // Reconnect immediately — every millisecond without WS costs us
                // 200ms extra latency per trade. Only add minimal backoff (100ms)
                // to avoid CPU spin if the server is rejecting us.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        });
    }

    /// Wait up to `max_wait_ms` for the WS to reconnect.
    /// Returns true if connected, false if timed out.
    /// Called by the order routing path when WS is momentarily down.
    pub async fn wait_for_reconnect(&self, max_wait_ms: u64) -> bool {
        let start = std::time::Instant::now();
        let check_interval = std::time::Duration::from_millis(5);
        let deadline = std::time::Duration::from_millis(max_wait_ms);
        while start.elapsed() < deadline {
            if self.is_connected() {
                ws_log!(
                    "[BinanceTradeWS] Reconnected after {}ms wait",
                    start.elapsed().as_millis()
                );
                return true;
            }
            tokio::time::sleep(check_interval).await;
        }
        false
    }

    /// Build and send a signed `account.status` probe to keep the connection warm.
    /// This is a lightweight read-only request that verifies the WS is alive end-to-end.
    async fn send_liveness_probe(sink: &WsSink, api_key: &str, api_secret: &str, req_id: &str) {
        let ts = Self::timestamp_ms();
        let query = format!("apiKey={}&timestamp={}", api_key, ts);
        let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes()).expect("HMAC error");
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let frame = serde_json::json!({
            "id": req_id,
            "method": "account.status",
            "params": {
                "apiKey": api_key,
                "timestamp": ts,
                "signature": signature,
            }
        });

        let mut sink_guard = sink.lock().await;
        if let Some(ref mut sink) = *sink_guard {
            if let Err(e) = sink.send(Message::Text(frame.to_string())).await {
                eprintln!("[BinanceTradeWS] Liveness probe send failed: {}", e);
            }
        }
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
        self.last_pong_ms.store(Self::epoch_ms_i64(), Ordering::Relaxed);
        let last_dc = self.last_disconnect_ms.load(Ordering::Relaxed);
        let downtime = if last_dc > 0 {
            Self::epoch_ms_i64() - last_dc
        } else {
            0
        };
        ws_log!("[BinanceTradeWS] ✅ Connected to Binance WS API (downtime={}ms)", downtime);

        // ── Ping task: send WebSocket-level pings every PING_INTERVAL_SECS ──
        let sink_for_ping = self.sink.clone();
        let connected_for_ping = self.connected.clone();
        let last_pong_for_ping = self.last_pong_ms.clone();
        let ping_task = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(PING_INTERVAL_SECS));
            interval.tick().await; // Skip immediate first tick
            loop {
                interval.tick().await;
                if !connected_for_ping.load(Ordering::Relaxed) {
                    break;
                }

                // Check for zombie connection: if last pong is too old, force disconnect
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                let last_pong = last_pong_for_ping.load(Ordering::Relaxed);
                if last_pong > 0 && (now - last_pong) > (PONG_TIMEOUT_SECS * 1000 + PING_INTERVAL_SECS as i64 * 1000) {
                    ws_log!(
                        "[BinanceTradeWS] ⚠️ Zombie connection detected! Last pong was {}ms ago — forcing disconnect",
                        now - last_pong
                    );
                    // Close the sink to force the read loop to exit
                    let mut sink_guard = sink_for_ping.lock().await;
                    if let Some(ref mut sink) = *sink_guard {
                        let _ = sink.close().await;
                    }
                    *sink_guard = None;
                    connected_for_ping.store(false, Ordering::Relaxed);
                    break;
                }

                let mut sink_guard = sink_for_ping.lock().await;
                if let Some(ref mut sink) = *sink_guard {
                    if sink.send(Message::Ping(vec![])).await.is_err() {
                        ws_log!("[BinanceTradeWS] Ping send failed — connection likely dead");
                        break;
                    }
                }
            }
        });

        // ── Liveness probe task: send `account.status` every LIVENESS_PROBE_INTERVAL_SECS ──
        let sink_for_probe = self.sink.clone();
        let connected_for_probe = self.connected.clone();
        let api_key_for_probe = self.api_key.clone();
        let api_secret_for_probe = self.api_secret.clone();
        let probe_counter = self.req_counter.clone();
        let probe_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(
                LIVENESS_PROBE_INTERVAL_SECS,
            ));
            interval.tick().await; // Skip immediate first tick
            loop {
                interval.tick().await;
                if !connected_for_probe.load(Ordering::Relaxed) {
                    break;
                }
                let req_id = format!(
                    "probe_{}",
                    probe_counter.fetch_add(1, Ordering::Relaxed)
                );
                Self::send_liveness_probe(
                    &sink_for_probe,
                    &api_key_for_probe,
                    &api_secret_for_probe,
                    &req_id,
                )
                .await;
            }
        });

        // ── Read loop — dispatch responses to pending request channels ──
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
                Ok(Message::Pong(_)) => {
                    // Update last pong timestamp for zombie detection
                    self.last_pong_ms.store(Self::epoch_ms_i64(), Ordering::Relaxed);
                    continue;
                }
                Ok(Message::Close(_)) => {
                    ws_log!("[BinanceTradeWS] Server closed connection");
                    break;
                }
                Err(e) => {
                    ws_log!("[BinanceTradeWS] Read error: {}", e);
                    break;
                }
                _ => continue,
            };

            // Parse the response and dispatch to the waiting caller
            match serde_json::from_str::<WsApiResponse>(&text) {
                Ok(resp) => {
                    let req_id = resp.id.clone();

                    // Liveness probe responses — just confirm we're alive, don't dispatch
                    if req_id.starts_with("probe_") {
                        if resp.status != 200 {
                            eprintln!(
                                "[BinanceTradeWS] Liveness probe got status {} (expected 200)",
                                resp.status
                            );
                        }
                        // Update pong time since we got a real response
                        self.last_pong_ms.store(Self::epoch_ms_i64(), Ordering::Relaxed);
                        continue;
                    }

                    if let Some((_, sender)) = self.pending.remove(&req_id) {
                        if resp.status == 200 {
                            if let Some(result) = resp.result {
                                let _ = sender.send(Ok(result));
                            } else {
                                let _ =
                                    sender.send(Err("Empty result in 200 response".to_string()));
                            }
                        } else {
                            let err_msg = resp
                                .error
                                .map(|e| format!("Binance WS API error {}: {}", e.code, e.msg))
                                .unwrap_or_else(|| format!("WS API status {}", resp.status));
                            let _ = sender.send(Err(err_msg));
                        }
                    }
                    // else: response for an already-timed-out request, ignore
                }
                Err(e) => {
                    // Not a WS API response (could be a stream event) — log only at debug level
                    eprintln!(
                        "[BinanceTradeWS] Non-API message (parse err: {}): {}",
                        e,
                        &text[..text.len().min(200)]
                    );
                }
            }
        }

        // Cleanup
        ping_task.abort();
        probe_task.abort();
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
        symbol: &str,
        side: &str,
        quantity: f64,
        client_order_id: &str,
        reduce_only: bool,
        _price: Option<f64>,
    ) -> Result<WsOrderResult, String> {
        if !self.is_connected() {
            return Err("BinanceTradeWS not connected".to_string());
        }

        let req_id = self.next_req_id();
        let ts = Self::timestamp_ms();
        let reduce_only_str = if reduce_only { "true" } else { "false" };

        let order_type = "MARKET";

        let query = format!(
            "apiKey={}&newClientOrderId={}&quantity={:.8}&reduceOnly={}&side={}&symbol={}&timestamp={}&type={}",
            self.api_key, client_order_id, quantity, reduce_only_str, side, symbol, ts, order_type
        );

        let signature = self.sign(&query);

        // Build the WS API request frame
        let params = serde_json::json!({
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

        let frame = serde_json::json!({
            "id": req_id,
            "method": "order.place",
            "params": params,
        });

        let frame_str = frame.to_string();

        // Register the response channel BEFORE sending
        let (tx, rx) = oneshot::channel::<Result<serde_json::Value, String>>();
        self.pending.insert(req_id.clone(), tx);

        eprintln!(
            "[BinanceTradeWS] Sending {} {} {} @ MARKET (reqId={}, clientId={})",
            side, quantity, symbol, req_id, client_order_id
        );

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
                    Err(e) => Err(format!(
                        "Failed to parse WS order result: {} | raw: {}",
                        e, result_json
                    )),
                }
            }
            Ok(Ok(Err(api_err))) => Err(api_err),
            Ok(Err(_)) => {
                self.pending.remove(&req_id);
                Err("WS response channel closed".to_string())
            }
            Err(_) => {
                self.pending.remove(&req_id);
                Err(format!(
                    "WS order timeout ({}ms) for reqId={}",
                    timeout.as_millis(),
                    req_id
                ))
            }
        }
    }
}
