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

/// Sender half of the background WS log channel.
/// Send a pre-formatted line here; the background flush task writes it to disk.
/// `try_send` is used so the hot order path is never blocked if the channel is full.
pub(super) static WS_LOG_TX: std::sync::OnceLock<tokio::sync::mpsc::Sender<String>> = std::sync::OnceLock::new();

/// Start the background WS log flusher if not already running.
/// Must be called once from within a tokio runtime (e.g. in `new()` or `spawn_connection()`).
pub(super) fn ensure_ws_log_flusher_running() {
    WS_LOG_TX.get_or_init(|| {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(512);
        tokio::spawn(async move {
            use std::io::Write;
            while let Some(line) = rx.recv().await {
                if let Ok(mut f) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(WS_LOG_PATH)
                {
                    let _ = writeln!(f, "{}", line);
                }
            }
        });
        tx
    });
}

/// Write a timestamped line to stderr and queue it for async disk write.
/// The disk write is performed by a background task — the caller is NEVER blocked.
macro_rules! ws_log {
    ($($arg:tt)*) => {{
        let msg = format!($($arg)*);
        let ts = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let line = format!("[{}] {}", ts, msg);
        eprintln!("{}", line);
        // Non-blocking enqueue for background disk write
        if let Some(tx) = WS_LOG_TX.get() {
            let _ = tx.try_send(line);
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
    /// Consecutive connection failures without a successful connect.
    /// Used to detect persistent WS problems (e.g. auth rejection, network block).
    consecutive_failures: Arc<AtomicU64>,
    /// Epoch millis of the last successful connection. 0 if never connected.
    last_connect_ms: Arc<AtomicI64>,
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
            consecutive_failures: Arc::new(AtomicU64::new(0)),
            last_connect_ms: Arc::new(AtomicI64::new(0)),
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

    /// Get the number of consecutive connection failures.
    /// If > 0, the WS is in a reconnect loop and orders will fall back to REST.
    pub fn consecutive_failures(&self) -> u64 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }

    /// Get total successful reconnections since process start.
    pub fn total_reconnects(&self) -> u64 {
        self.reconnect_count.load(Ordering::Relaxed)
    }

    /// How long since the last disconnect in ms. Returns 0 if never disconnected.
    pub fn last_disconnect_age_ms(&self) -> i64 {
        let dc = self.last_disconnect_ms.load(Ordering::Relaxed);
        if dc == 0 { return 0; }
        Self::epoch_ms_i64() - dc
    }

    /// How long since the last successful connection in ms. Returns -1 if never connected.
    pub fn last_connect_age_ms(&self) -> i64 {
        let c = self.last_connect_ms.load(Ordering::Relaxed);
        if c == 0 { return -1; }
        Self::epoch_ms_i64() - c
    }

    /// Wait up to `max_wait_ms` for the FIRST connection to succeed.
    /// Used at startup to verify the WS API is reachable before trading.
    /// Returns true if connected within the timeout.
    pub async fn wait_for_first_connect(&self, max_wait_ms: u64) -> bool {
        let start = std::time::Instant::now();
        let check_interval = std::time::Duration::from_millis(50);
        let deadline = std::time::Duration::from_millis(max_wait_ms);
        while start.elapsed() < deadline {
            if self.is_connected() {
                ws_log!(
                    "[BinanceTradeWS] ✅ First connection verified after {}ms",
                    start.elapsed().as_millis()
                );
                return true;
            }
            tokio::time::sleep(check_interval).await;
        }
        let failures = self.consecutive_failures();
        ws_log!(
            "[BinanceTradeWS] ⚠️ First connection FAILED after {}ms wait ({} consecutive failures)",
            max_wait_ms, failures
        );
        false
    }

    /// Spawn the WebSocket connection loop as a background task.
    /// Automatically reconnects on disconnect with adaptive backoff.
    pub fn spawn_connection(&self) {
        // Ensure the background log flusher is running before any ws_log! calls.
        ensure_ws_log_flusher_running();
        let handle = self.clone();
        tokio::spawn(async move {
            loop {
                let failures = handle.consecutive_failures.load(Ordering::Relaxed);
                ws_log!(
                    "[BinanceTradeWS] Connecting to {} (consecutive_failures={})...",
                    WS_API_URL, failures
                );
                handle.run_connection().await;
                handle.connected.store(false, Ordering::Relaxed);
                handle.last_disconnect_ms.store(Self::epoch_ms_i64(), Ordering::Relaxed);
                let n = handle.reconnect_count.fetch_add(1, Ordering::Relaxed) + 1;
                let cur_failures = handle.consecutive_failures.load(Ordering::Relaxed);
                ws_log!(
                    "[BinanceTradeWS] ❌ Disconnected (reconnect #{}, consecutive_failures={}) — ALL ORDERS USING SLOW 200ms REST PATH",
                    n, cur_failures
                );
                // Aggressive reconnect: always retry within 50ms to minimize REST fallback time.
                // The old 100ms-5000ms backoff caused extended REST usage (~200ms/trade) after
                // repeated disconnects. The WS connection failing is transient; reconnecting fast
                // is better than staying on the slow REST path for seconds.
                let backoff_ms = if cur_failures > 10 {
                    50u64
                } else if cur_failures > 5 {
                    30
                } else {
                    10
                };
                ws_log!(
                    "[BinanceTradeWS] Reconnecting in {}ms...",
                    backoff_ms
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
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
                ws_log!("[BinanceTradeWS] ❌ Invalid URL: {} — this is a code bug!", e);
                self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        let connect_t0 = std::time::Instant::now();
        let (ws_stream, response) = match connect_async(ws_url).await {
            Ok(r) => r,
            Err(e) => {
                let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
                ws_log!(
                    "[BinanceTradeWS] ❌ Connection FAILED (attempt took {}ms, consecutive_failures={}): {}",
                    connect_t0.elapsed().as_millis(), failures, e
                );
                ws_log!(
                    "[BinanceTradeWS] ⚠️ ALL ORDERS ROUTING VIA SLOW REST API (~200ms) UNTIL WS RECOVERS"
                );
                return;
            }
        };
        let connect_ms = connect_t0.elapsed().as_millis();

        let (write, mut read) = ws_stream.split();

        // Store the write half for place_order() to use
        {
            let mut sink_guard = self.sink.lock().await;
            *sink_guard = Some(write);
        }
        self.connected.store(true, Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed); // Reset on success
        self.last_connect_ms.store(Self::epoch_ms_i64(), Ordering::Relaxed);
        self.last_pong_ms.store(Self::epoch_ms_i64(), Ordering::Relaxed);
        let last_dc = self.last_disconnect_ms.load(Ordering::Relaxed);
        let downtime = if last_dc > 0 {
            Self::epoch_ms_i64() - last_dc
        } else {
            0
        };
        ws_log!(
            "[BinanceTradeWS] ✅ Connected to Binance WS API (handshake={}ms, downtime={}ms, HTTP status={:?})",
            connect_ms, downtime, response.status()
        );

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
        price: Option<f64>,
    ) -> Result<WsOrderResult, String> {
        if !self.is_connected() {
            return Err("BinanceTradeWS not connected".to_string());
        }

        let req_id = self.next_req_id();
        let ts = Self::timestamp_ms();
        let reduce_only_str = if reduce_only { "true" } else { "false" };

        // Use LIMIT IOC when a protective price is provided (slippage protection).
        // The IOC ensures the order fills immediately at or better than the limit price,
        // or expires with 0 fill if the market has moved beyond the limit.
        // Use MARKET only for close/reversal orders where guaranteed fill matters more.
        let (order_type, price_str) = match price {
            Some(p) => ("LIMIT", Some(format!("{:.8}", p))),
            None => ("MARKET", None),
        };

        // Binance requires query params sorted alphabetically for signature.
        // newOrderRespType=RESULT is CRITICAL: without it, Binance defaults to ACK
        // which returns status=NEW with no fill data, forcing a 200ms wait for the
        // private WS ORDER_TRADE_UPDATE. With RESULT, the response includes
        // status=FILLED/EXPIRED + avgPrice + executedQty directly (~3ms total RTT).
        let query = if let Some(ref p) = price_str {
            format!(
                "apiKey={}&newClientOrderId={}&newOrderRespType=RESULT&price={}&quantity={:.8}&reduceOnly={}&side={}&symbol={}&timeInForce=IOC&timestamp={}&type={}",
                self.api_key, client_order_id, p, quantity, reduce_only_str, side, symbol, ts, order_type
            )
        } else {
            format!(
                "apiKey={}&newClientOrderId={}&newOrderRespType=RESULT&quantity={:.8}&reduceOnly={}&side={}&symbol={}&timestamp={}&type={}",
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
            "newOrderRespType": "RESULT",
            "reduceOnly": reduce_only_str,
            "timestamp": ts,
            "signature": signature,
        });
        if let Some(ref p) = price_str {
            params["price"] = serde_json::Value::String(p.clone());
            params["timeInForce"] = serde_json::Value::String("IOC".to_string());
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

        let send_t0 = std::time::Instant::now();
        // Use eprintln only (no disk I/O) on the hot order-send path to avoid
        // blocking the executor with synchronous file writes.
        eprintln!(
            "[BinanceTradeWS] Sending {} {} {} @ {} (reqId={}, clientId={})",
            side, quantity, symbol, order_type, req_id, client_order_id
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

        let send_elapsed_ms = send_t0.elapsed().as_millis();

        // Wait for the response with a 500ms timeout (generous for a WS RTT of ~10-15ms).
        // This timeout covers extreme cases like WS congestion.
        let timeout = std::time::Duration::from_millis(500);
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(result_json))) => {
                let total_rtt_ms = send_t0.elapsed().as_millis();
                // Parse the WsOrderResult from the response
                match serde_json::from_value::<WsOrderResult>(result_json.clone()) {
                    Ok(order) => {
                        ws_log!(
                            "[BinanceTradeWS] ⚡ Order filled via WS in {}ms (send={}ms): id={:?} status={} filled={} avgPrice={}",
                            total_rtt_ms, send_elapsed_ms, order.orderId, order.status, order.executedQty, order.avgPrice
                        );
                        Ok(order)
                    }
                    Err(e) => Err(format!(
                        "Failed to parse WS order result: {} | raw: {}",
                        e, result_json
                    )),
                }
            }
            Ok(Ok(Err(api_err))) => {
                ws_log!(
                    "[BinanceTradeWS] WS API error after {}ms: {}",
                    send_t0.elapsed().as_millis(), api_err
                );
                Err(api_err)
            }
            Ok(Err(_)) => {
                self.pending.remove(&req_id);
                ws_log!(
                    "[BinanceTradeWS] WS channel closed after {}ms for reqId={}",
                    send_t0.elapsed().as_millis(), req_id
                );
                Err("WS response channel closed".to_string())
            }
            Err(_) => {
                self.pending.remove(&req_id);
                ws_log!(
                    "[BinanceTradeWS] ⚠️ WS order TIMEOUT after {}ms for reqId={} — connection may be zombie",
                    timeout.as_millis(), req_id
                );
                Err(format!(
                    "WS order timeout ({}ms) for reqId={}",
                    timeout.as_millis(),
                    req_id
                ))
            }
        }
    }
}
