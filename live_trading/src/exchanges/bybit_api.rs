use hmac::{Hmac, Mac};
use sha2::Sha256;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use super::fill_channel::{FillEvent, LiveBalance, PendingFillMap};
use chrono::Utc;


type HmacSha256 = Hmac<Sha256>;

/// Bybit V5 API authenticated client.
/// Uses HMAC-SHA256 for request signing per Bybit API v5 specification.
/// Fill confirmations are received via private WS (PendingFillMap) instead of REST polling.
#[derive(Clone)]
pub struct BybitClient {
    api_key:       String,
    api_secret:    String,
    /// Client with shorter timeout for latency-critical order placement.
    http_fast:     reqwest::Client,
    /// Client with standard timeout for non-critical queries (balances, positions).
    http_slow:     reqwest::Client,
    base_url:      String,
    recv_window:   String,
    /// Registry of orders waiting for a WS fill event.
    pending_fills: PendingFillMap,
    /// Live USDT balance updated in real-time by the private WS wallet event.
    live_balance:  LiveBalance,
}

/// Bybit generic API response wrapper.
#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
pub struct BybitResponse<T> {
    pub retCode: i32,
    pub retMsg: String,
    pub result: T,
}

/// Bybit wallet balance result.
#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
pub struct BybitWalletResult {
    pub list: Vec<BybitAccount>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case, dead_code)]
pub struct BybitAccount {
    pub accountType: String,
    #[serde(default)]
    pub totalWalletBalance: String,
    #[serde(default)]
    pub totalAvailableBalance: String,
    #[serde(default)]
    pub coin: Vec<BybitCoinBalance>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case, dead_code)]
pub struct BybitCoinBalance {
    pub coin: String,
    #[serde(default)]
    pub availableToWithdraw: String,
    #[serde(default)]
    pub walletBalance: String,
    #[serde(default)]
    pub equity: String,
    #[serde(default)]
    pub unrealisedPnl: String,
}

/// Bybit order result.
#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case, dead_code)]
pub struct BybitOrderResult {
    pub orderId: String,
    pub orderLinkId: String,
}

/// Bybit order detail from query.
#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case)]
#[allow(dead_code)]
pub struct BybitOrderDetailResult {
    pub list: Vec<BybitOrderDetail>,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case, dead_code)]
pub struct BybitOrderDetail {
    pub orderId: String,
    pub symbol: String,
    pub side: String,
    pub orderStatus: String,
    pub avgPrice: String,
    pub qty: String,
    pub cumExecQty: String,
    pub cumExecValue: String,
    pub cumExecFee: String,
    pub updatedTime: String,
}

/// Bybit execution/trade list.
#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case, dead_code)]
pub struct BybitExecutionResult {
    pub list: Vec<BybitExecution>,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case, dead_code)]
pub struct BybitExecution {
    pub orderId: String,
    pub symbol: String,
    pub side: String,
    pub execPrice: String,
    pub execQty: String,
    pub execValue: String,
    pub execFee: String,
    pub closedSize: String,
    pub execTime: String,
}

/// Bybit position info.
#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case, dead_code)]
pub struct BybitPositionResult {
    pub list: Vec<BybitPosition>,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case, dead_code)]
pub struct BybitPosition {
    pub symbol: String,
    pub side: String,
    pub size: String,
    pub avgPrice: String,
    pub unrealisedPnl: String,
    pub leverage: String,
}

/// Structured fill result after placing a market order.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OrderFill {
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub avg_price: f64,
    pub filled_qty: f64,
    pub quote_qty: f64,
    pub commission: f64,
    pub timestamp: u64,
}

impl BybitClient {
    /// Create a new Bybit V5 API client.
    /// Uses a fast 3s timeout for orders and a slower 8s timeout for account queries.
    pub fn new(
        api_key:       String,
        api_secret:    String,
        pending_fills: PendingFillMap,
        live_balance:  LiveBalance,
    ) -> Self {
        let http_fast = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(3))
            .pool_max_idle_per_host(4)
            .tcp_nodelay(true)
            .build()
            .unwrap_or_default();

        let http_slow = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(8))
            .pool_max_idle_per_host(2)
            .tcp_nodelay(true)
            .build()
            .unwrap_or_default();

        BybitClient {
            api_key,
            api_secret,
            http_fast,
            http_slow,
            base_url: "https://api.bybit.com".to_string(),
            recv_window: "5000".to_string(),
            pending_fills,
            live_balance,
        }
    }

    /// Get the live USDT balance (pushed by private WS wallet event).
    /// Falls back to 0.0 if private WS has not yet delivered a balance.
    pub async fn get_live_balance(&self) -> f64 {
        *self.live_balance.read().await
    }

    /// Get current timestamp in milliseconds.
    #[inline]
    fn timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// Sign a payload with HMAC-SHA256 per Bybit V5 spec.
    /// For GET: payload = timestamp + api_key + recv_window + query_string
    /// For POST: payload = timestamp + api_key + recv_window + json_body
    #[inline]
    fn sign(&self, timestamp: u64, payload: &str) -> String {
        let pre_sign = format!(
            "{}{}{}{}",
            timestamp, self.api_key, self.recv_window, payload
        );
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC key error");
        mac.update(pre_sign.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Build auth headers for a Bybit V5 request.
    #[inline]
    fn auth_headers(&self, timestamp: u64, signature: &str) -> Vec<(&'static str, String)> {
        vec![
            ("X-BAPI-API-KEY", self.api_key.clone()),
            ("X-BAPI-SIGN", signature.to_string()),
            ("X-BAPI-SIGN-TYPE", "2".to_string()),
            ("X-BAPI-TIMESTAMP", timestamp.to_string()),
            ("X-BAPI-RECV-WINDOW", self.recv_window.clone()),
        ]
    }

    /// Fetch USDT available balance from the unified trading account.
    pub async fn get_balance(&self) -> Result<f64, String> {
        let ts = Self::timestamp_ms();
        let query = "accountType=UNIFIED&coin=USDT";
        let signature = self.sign(ts, query);
        let url = format!("{}/v5/account/wallet-balance?{}", self.base_url, query);

        let mut req = self.http_slow.get(&url);
        for (key, val) in self.auth_headers(ts, &signature) {
            req = req.header(key, val);
        }

        let resp = req.send().await
            .map_err(|e| format!("Bybit balance request failed: {}", e))?;

        let status = resp.status();
        let text = resp.text().await
            .map_err(|e| format!("Failed to read response: {}", e))?;

        if !status.is_success() {
            return Err(format!("Bybit balance API error ({}): {}", status, text));
        }

        let api_resp: BybitResponse<BybitWalletResult> = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse balance: {} | body: {}", e, text))?;

        if api_resp.retCode != 0 {
            return Err(format!("Bybit API error: {} - {}", api_resp.retCode, api_resp.retMsg));
        }

        let parse_val = |s: &str| -> Option<f64> {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                trimmed.parse::<f64>().ok()
            }
        };

        for account in &api_resp.result.list {
            for coin in &account.coin {
                if coin.coin == "USDT" {
                    // In Bybit UTA, availableToWithdraw may be empty "" when balance is 0.
                    // Check walletBalance first, then availableToWithdraw, then equity.
                    if let Some(b) = parse_val(&coin.walletBalance) {
                        return Ok(b);
                    }
                    if let Some(b) = parse_val(&coin.availableToWithdraw) {
                        return Ok(b);
                    }
                    if let Some(b) = parse_val(&coin.equity) {
                        return Ok(b);
                    }
                    return Ok(0.0);
                }
            }

            // Fallback to account-level totals if coin array didn't list USDT
            if let Some(b) = parse_val(&account.totalAvailableBalance) {
                return Ok(b);
            }
            if let Some(b) = parse_val(&account.totalWalletBalance) {
                return Ok(b);
            }
        }

        Ok(0.0)
    }

    /// Place a market order on Bybit V5 linear perpetuals.
    /// `side` should be "Buy" or "Sell".
    /// `quantity` is in base asset units.
    /// `order_link_id` is a client-generated unique ID used to match WS fills.
    /// `reduce_only` should be `true` for close orders to prevent accidentally opening new positions.
    pub async fn place_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: f64,
        order_link_id: &str,
        reduce_only: bool,
        price: Option<f64>,
    ) -> Result<BybitOrderResult, String> {
        let ts = Self::timestamp_ms();

        let mut body = serde_json::json!({
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": if price.is_some() { "Limit" } else { "Market" },
            "qty": format!("{:.8}", quantity),
            "timeInForce": "IOC",
            "orderLinkId": order_link_id,
            "reduceOnly": reduce_only,
        });

        if let Some(p) = price {
            body["price"] = serde_json::json!(format!("{:.6}", p));
        }

        let body_str = body.to_string();
        let signature = self.sign(ts, &body_str);
        let url = format!("{}/v5/order/create", self.base_url);

        if let Some(p) = price {
            eprintln!("[BybitAPI] Placing {} {} {} @ LIMIT IOC {:.6} (linkId={})", side, quantity, symbol, p, order_link_id);
        } else {
            eprintln!("[BybitAPI] Placing {} {} {} @ MARKET (linkId={})", side, quantity, symbol, order_link_id);
        }

        let mut req = self.http_fast
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body_str);

        for (key, val) in self.auth_headers(ts, &signature) {
            req = req.header(key, val);
        }

        let resp = req.send().await
            .map_err(|e| format!("Bybit order request failed: {}", e))?;

        let status = resp.status();
        let text = resp.text().await
            .map_err(|e| format!("Failed to read response: {}", e))?;

        if !status.is_success() {
            return Err(format!("Bybit order API error ({}): {}", status, text));
        }

        let api_resp: BybitResponse<BybitOrderResult> = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse order response: {} | body: {}", e, text))?;

        if api_resp.retCode != 0 {
            return Err(format!("Bybit order error: {} - {}", api_resp.retCode, api_resp.retMsg));
        }

        eprintln!(
            "[{}][BybitAPI] Order placed: id={} linkId={}",
            Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            api_resp.result.orderId, order_link_id
        );

        Ok(api_resp.result)
    }

    /// Query order detail — kept as manual fallback tool.
    #[allow(dead_code)]
    pub async fn get_order_detail(
        &self,
        symbol: &str,
        order_id: &str,
    ) -> Result<BybitOrderDetail, String> {
        let ts = Self::timestamp_ms();
        let query = format!(
            "category=linear&symbol={}&orderId={}",
            symbol, order_id
        );
        let signature = self.sign(ts, &query);
        let url = format!("{}/v5/order/realtime?{}", self.base_url, query);

        let mut req = self.http_fast.get(&url);
        for (key, val) in self.auth_headers(ts, &signature) {
            req = req.header(key, val);
        }

        let resp = req.send().await
            .map_err(|e| format!("Bybit order detail request failed: {}", e))?;

        let status = resp.status();
        let text = resp.text().await
            .map_err(|e| format!("Failed to read response: {}", e))?;

        if !status.is_success() {
            return Err(format!("Bybit order detail API error ({}): {}", status, text));
        }

        let api_resp: BybitResponse<BybitOrderDetailResult> = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse order detail: {} | body: {}", e, text))?;

        if api_resp.retCode != 0 {
            return Err(format!("Bybit API error: {} - {}", api_resp.retCode, api_resp.retMsg));
        }

        api_resp.result.list.into_iter().next()
            .ok_or_else(|| "Order not found".to_string())
    }

    /// Fetch execution list (trades) for a specific order.
    #[allow(dead_code)]
    pub async fn get_executions(
        &self,
        symbol: &str,
        order_id: &str,
    ) -> Result<Vec<BybitExecution>, String> {
        let ts = Self::timestamp_ms();
        let query = format!(
            "category=linear&symbol={}&orderId={}",
            symbol, order_id
        );
        let signature = self.sign(ts, &query);
        let url = format!("{}/v5/execution/list?{}", self.base_url, query);

        let mut req = self.http_fast.get(&url);
        for (key, val) in self.auth_headers(ts, &signature) {
            req = req.header(key, val);
        }

        let resp = req.send().await
            .map_err(|e| format!("Bybit executions request failed: {}", e))?;

        let status = resp.status();
        let text = resp.text().await
            .map_err(|e| format!("Failed to read response: {}", e))?;

        if !status.is_success() {
            return Err(format!("Bybit executions API error ({}): {}", status, text));
        }

        let api_resp: BybitResponse<BybitExecutionResult> = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse executions: {} | body: {}", e, text))?;

        if api_resp.retCode != 0 {
            return Err(format!("Bybit API error: {} - {}", api_resp.retCode, api_resp.retMsg));
        }

        Ok(api_resp.result.list)
    }

    /// Place a market order and return a structured OrderFill.
    ///
    /// FIX: Registers the oneshot fill channel BEFORE placing the order using a
    /// pre-generated `orderLinkId`. This eliminates the race condition where the
    /// private WS fill event arrives (5–20ms) before the channel was registered
    /// (after the ~150ms REST round-trip), causing a 5s timeout and avg_price=0.0.
    /// `reduce_only`: pass `true` for close orders to prevent opening new positions.
    pub async fn execute_order_with_fill(
        &self,
        symbol:      &str,
        side:        &str,
        quantity:    f64,
        reduce_only: bool,
        price:       Option<f64>,
    ) -> Result<OrderFill, String> {
        // Generate a unique client order ID and register the fill channel FIRST
        // so it's ready before the WS fill event can arrive.
        let order_link_id = format!("arb-{}", std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros());

        let (tx, rx) = oneshot::channel::<FillEvent>();
        self.pending_fills.insert(order_link_id.clone(), tx);

        // Place the order — fill may arrive via WS while this is in-flight
        let order = match self.place_order(symbol, side, quantity, &order_link_id, reduce_only, price).await {
            Ok(o) => o,
            Err(e) => {
                // Order failed — clean up the registered channel
                self.pending_fills.remove(&order_link_id);
                return Err(e);
            }
        };

        // Await fill from private WS — timeout 500ms
        // If it doesn't arrive in 500ms, the event was lost or the WS is disconnected.
        // Fail fast so the other leg can be reversed quickly.
        match tokio::time::timeout(std::time::Duration::from_millis(500), rx).await {
            Ok(Ok(fill)) if fill.is_expired => {
                // WS delivered an Expired/Cancelled event with 0 fill — fast fail in ~20ms.
                // This replaces the old 500ms timeout + REST fallback path.
                eprintln!(
                    "[BybitAPI] Order {} expired (WS fast-fail, ~20ms): qty=0",
                    order.orderId
                );
                Err(format!(
                    "Bybit order {} expired with 0 fill (IOC limit missed — slippage exceeded)",
                    order.orderId
                ))
            }
            Ok(Ok(fill)) => {
                // Fill arrived via WS push — fast path
                eprintln!(
                    "[BybitAPI] Fill (WS): id={} avgPrice={} qty={} fee={}",
                    fill.order_id, fill.avg_price, fill.filled_qty, fill.commission
                );
                Ok(OrderFill {
                    order_id:   order.orderId,
                    symbol:     symbol.to_string(),
                    side:       side.to_string(),
                    avg_price:  fill.avg_price,
                    filled_qty: fill.filled_qty,
                    quote_qty:  fill.quote_qty,
                    commission: fill.commission,
                    timestamp:  fill.timestamp,
                })
            }
            _ => {
                // Timeout or channel closed — WS may be reconnecting
                // Query the order via REST to get real fill data
                self.pending_fills.remove(&order_link_id);
                eprintln!(
                    "[BybitAPI] WARNING: WS fill timeout for order {} — querying REST fallback",
                    order.orderId
                );
                // Attempt REST fallback to get actual fill price
                match self.get_order_detail(symbol, &order.orderId).await {
                    Ok(detail) => {
                        let avg_price  = detail.avgPrice.parse::<f64>().unwrap_or(0.0);
                        let filled_qty = detail.cumExecQty.parse::<f64>().unwrap_or(quantity);
                        let quote_qty  = detail.cumExecValue.parse::<f64>().unwrap_or(0.0);
                        let commission = detail.cumExecFee.parse::<f64>().unwrap_or(0.0).abs();
                        eprintln!(
                            "[BybitAPI] REST fallback: id={} avgPrice={} qty={} fee={}",
                            order.orderId, avg_price, filled_qty, commission
                        );
                        Ok(OrderFill {
                            order_id:   order.orderId,
                            symbol:     symbol.to_string(),
                            side:       side.to_string(),
                            avg_price,
                            filled_qty,
                            quote_qty,
                            commission,
                            timestamp:  0,
                        })
                    }
                    Err(e) => {
                        eprintln!("[BybitAPI] REST fallback also failed: {} — using estimated data", e);
                        Ok(OrderFill {
                            order_id:   order.orderId,
                            symbol:     symbol.to_string(),
                            side:       side.to_string(),
                            avg_price:  0.0, // will be caught by safety check
                            filled_qty: quantity,
                            quote_qty:  0.0,
                            commission: 0.0,
                            timestamp:  0,
                        })
                    }
                }
            }
        }
    }

    /// Set leverage for a specific symbol on Bybit V5 linear perpetuals.
    /// Must be called BEFORE placing an order to ensure the correct leverage is active.
    /// In one-way mode, buyLeverage must equal sellLeverage.
    pub async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<(), String> {
        let ts = Self::timestamp_ms();
        let lev_str = leverage.to_string();

        let body = serde_json::json!({
            "category": "linear",
            "symbol": symbol,
            "buyLeverage": lev_str,
            "sellLeverage": lev_str
        });

        let body_str = body.to_string();
        let signature = self.sign(ts, &body_str);
        let url = format!("{}/v5/position/set-leverage", self.base_url);

        let mut req = self.http_fast
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body_str);

        for (key, val) in self.auth_headers(ts, &signature) {
            req = req.header(key, val);
        }

        let resp = req.send().await
            .map_err(|e| format!("Bybit set leverage request failed: {}", e))?;

        let status = resp.status();
        let text = resp.text().await
            .map_err(|e| format!("Failed to read response: {}", e))?;

        if !status.is_success() {
            return Err(format!("Bybit set leverage API error ({}): {}", status, text));
        }

        // Parse response to check retCode — 110043 means "leverage not modified" which is OK
        #[derive(serde::Deserialize)]
        #[allow(non_snake_case)]
        struct LevResp { retCode: i32, retMsg: String }

        if let Ok(r) = serde_json::from_str::<LevResp>(&text) {
            if r.retCode != 0 && r.retCode != 110043 {
                return Err(format!("Bybit set leverage error: {} - {}", r.retCode, r.retMsg));
            }
        }

        eprintln!("[BybitAPI] Set leverage for {} to {}x", symbol, leverage);
        Ok(())
    }

    /// Fetch all open positions.
    #[allow(dead_code)]
    pub async fn get_positions(&self) -> Result<Vec<BybitPosition>, String> {
        let ts = Self::timestamp_ms();
        let query = "category=linear&settleCoin=USDT";
        let signature = self.sign(ts, query);
        let url = format!("{}/v5/position/list?{}", self.base_url, query);

        let mut req = self.http_slow.get(&url);
        for (key, val) in self.auth_headers(ts, &signature) {
            req = req.header(key, val);
        }

        let resp = req.send().await
            .map_err(|e| format!("Bybit positions request failed: {}", e))?;

        let status = resp.status();
        let text = resp.text().await
            .map_err(|e| format!("Failed to read response: {}", e))?;

        if !status.is_success() {
            return Err(format!("Bybit positions API error ({}): {}", status, text));
        }

        let api_resp: BybitResponse<BybitPositionResult> = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse positions: {} | body: {}", e, text))?;

        if api_resp.retCode != 0 {
            return Err(format!("Bybit API error: {} - {}", api_resp.retCode, api_resp.retMsg));
        }

        // Filter to non-zero positions
        Ok(api_resp.result.list.into_iter().filter(|p| {
            p.size.parse::<f64>().unwrap_or(0.0).abs() > 0.0
        }).collect())
    }
}
