use hmac::{Hmac, Mac};
use sha2::Sha256;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use super::fill_channel::{FillEvent, LiveBalance, PendingFillMap};
use chrono::Utc;


type HmacSha256 = Hmac<Sha256>;

/// Binance Futures authenticated API client.
/// Uses HMAC-SHA256 for request signing per Binance API v1 specification.
/// Fill confirmations are received via private WS (PendingFillMap) instead of REST polling.
#[derive(Clone)]
pub struct BinanceClient {
    api_key:       String,
    api_secret:    String,
    /// Client with shorter timeout for latency-critical order placement.
    http_fast:     reqwest::Client,
    /// Client with standard timeout for non-critical queries (balances, positions).
    http_slow:     reqwest::Client,
    base_url:      String,
    /// Registry of orders waiting for a WS fill event.
    pending_fills: PendingFillMap,
    /// Live USDT balance updated in real-time by the private WS ACCOUNT_UPDATE.
    live_balance:  LiveBalance,
}

/// Response from Binance futures account balance endpoint.
#[derive(Debug, Deserialize)]
#[allow(non_snake_case, dead_code)]
pub struct BinanceBalance {
    pub asset: String,
    pub balance: String,
    pub availableBalance: String,
    pub crossUnPnl: String,
}

/// Response from Binance futures new order endpoint.
/// NOTE: When status="NEW" (rare race where Binance replies before fill),
/// avgPrice, cumQuote, and executedQty may be absent from the JSON body.
/// We use Option to avoid a parse failure that would trigger a false leg reversal.
#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case, dead_code)]
pub struct BinanceOrderResponse {
    pub orderId: u64,
    pub symbol: String,
    pub status: String,
    pub side: String,
    pub origQty: String,
    #[serde(default)]
    pub executedQty: String,
    #[serde(default)]
    pub avgPrice: String,
    #[serde(default)]
    pub cumQuote: String,
    pub updateTime: u64,
}

/// Response from Binance futures order detail query.
#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case, dead_code)]
pub struct BinanceOrderDetail {
    pub orderId: u64,
    pub symbol: String,
    pub status: String,
    pub side: String,
    pub origQty: String,
    pub executedQty: String,
    pub avgPrice: String,
    pub cumQuote: String,
    pub commission: Option<String>,
    pub commissionAsset: Option<String>,
    pub updateTime: u64,
}

/// Response from Binance trade list (user trades).
#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case, dead_code)]
pub struct BinanceTrade {
    pub id: u64,
    pub orderId: u64,
    pub symbol: String,
    pub side: String,
    pub price: String,
    pub qty: String,
    pub quoteQty: String,
    pub commission: String,
    pub commissionAsset: String,
    pub realizedPnl: String,
    pub time: u64,
}

/// Response from Binance positions.
#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case, dead_code)]
pub struct BinancePosition {
    pub symbol: String,
    pub positionAmt: String,
    pub entryPrice: String,
    pub unRealizedProfit: String,
    pub leverage: String,
    pub positionSide: String,
}

/// Structured fill result after placing a market order.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OrderFill {
    pub order_id: u64,
    pub symbol: String,
    pub side: String,
    pub avg_price: f64,
    pub filled_qty: f64,
    pub quote_qty: f64,
    pub commission: f64,
    pub commission_asset: String,
    pub realized_pnl: f64,
    pub timestamp: u64,
}

impl BinanceClient {
    /// Create a new Binance Futures API client.
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

        BinanceClient {
            api_key,
            api_secret,
            http_fast,
            http_slow,
            base_url: "https://fapi.binance.com".to_string(),
            pending_fills,
            live_balance,
        }
    }

    /// Get the live USDT balance (pushed by private WS ACCOUNT_UPDATE).
    /// Falls back to 0.0 if private WS has not yet delivered a balance.
    pub async fn get_live_balance(&self) -> f64 {
        *self.live_balance.read().await
    }

    /// Get current server timestamp in milliseconds.
    #[inline]
    fn timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// Sign a query string with HMAC-SHA256.
    #[inline]
    fn sign(&self, query: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC key error");
        mac.update(query.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Fetch USDT balance from the futures account.
    pub async fn get_balance(&self) -> Result<f64, String> {
        let ts = Self::timestamp_ms();
        let query = format!("timestamp={}", ts);
        let signature = self.sign(&query);
        let url = format!(
            "{}/fapi/v2/balance?{}&signature={}",
            self.base_url, query, signature
        );

        let resp = self.http_slow
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| format!("Binance balance request failed: {}", e))?;

        let status = resp.status();
        let text = resp.text().await.map_err(|e| format!("Failed to read response: {}", e))?;

        if !status.is_success() {
            return Err(format!("Binance balance API error ({}): {}", status, text));
        }

        let balances: Vec<BinanceBalance> = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse balance: {} | body: {}", e, text))?;

        for bal in &balances {
            if bal.asset == "USDT" {
                return bal.availableBalance.parse::<f64>()
                    .map_err(|e| format!("Failed to parse USDT balance: {}", e));
            }
        }

        Err("USDT balance not found".to_string())
    }

    /// Place a market order on Binance Futures.
    /// `side` should be "BUY" or "SELL".
    /// `quantity` is in base asset units (coin quantity).
    /// `client_order_id` is a pre-generated ID used to match WS fills before REST returns.
    pub async fn place_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: f64,
        client_order_id: &str,
        reduce_only: bool,
        price: Option<f64>,
    ) -> Result<BinanceOrderResponse, String> {
        let ts = Self::timestamp_ms();
        let reduce_only_str = if reduce_only { "true" } else { "false" };
        let query = if let Some(p) = price {
            format!(
                "symbol={}&side={}&type=LIMIT&timeInForce=IOC&quantity={:.8}&price={:.6}&newClientOrderId={}&reduceOnly={}&timestamp={}",
                symbol, side, quantity, p, client_order_id, reduce_only_str, ts
            )
        } else {
            format!(
                "symbol={}&side={}&type=MARKET&quantity={:.8}&newClientOrderId={}&reduceOnly={}&timestamp={}",
                symbol, side, quantity, client_order_id, reduce_only_str, ts
            )
        };
        let signature = self.sign(&query);
        let url = format!(
            "{}/fapi/v1/order?{}&signature={}",
            self.base_url, query, signature
        );

        if let Some(p) = price {
            eprintln!("[{}][BinanceAPI] Placing {} {} {} @ LIMIT IOC {:.6} (clientId={})", Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"), side, quantity, symbol, p, client_order_id);
        } else {
            eprintln!("[{}][BinanceAPI] Placing {} {} {} @ MARKET (clientId={})", Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"), side, quantity, symbol, client_order_id);
        }

        let resp = self.http_fast
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| format!("Binance order request failed: {}", e))?;

        let status = resp.status();
        let text = resp.text().await.map_err(|e| format!("Failed to read response: {}", e))?;

        if !status.is_success() {
            return Err(format!("Binance order API error ({}): {}", status, text));
        }

        let order: BinanceOrderResponse = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse order response: {} | body: {}", e, text))?;

        eprintln!(
            "[{}][BinanceAPI] Order accepted: id={} status={} filled={} avgPrice={}",
            Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            order.orderId, order.status, order.executedQty, order.avgPrice
        );

        // status="NEW" means accepted but not yet matched — this is NOT a failure.
        // The WS fill event will arrive within milliseconds. Return the order so
        // market_order_with_fill() can await the WS channel.
        if order.status == "NEW" || order.status == "PARTIALLY_FILLED" {
            eprintln!(
                "[BinanceAPI] Order {} status={} — waiting for WS fill event",
                order.orderId, order.status
            );
        }

        Ok(order)
    }

    /// Fetch trades for a specific order — kept as manual fallback tool.
    #[allow(dead_code)]
    pub async fn get_order_trades(
        &self,
        symbol: &str,
        order_id: u64,
    ) -> Result<Vec<BinanceTrade>, String> {
        let ts = Self::timestamp_ms();
        let query = format!(
            "symbol={}&orderId={}&timestamp={}",
            symbol, order_id, ts
        );
        let signature = self.sign(&query);
        let url = format!(
            "{}/fapi/v1/userTrades?{}&signature={}",
            self.base_url, query, signature
        );

        let resp = self.http_fast
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| format!("Binance trades request failed: {}", e))?;

        let status = resp.status();
        let text = resp.text().await.map_err(|e| format!("Failed to read response: {}", e))?;

        if !status.is_success() {
            return Err(format!("Binance trades API error ({}): {}", status, text));
        }

        let trades: Vec<BinanceTrade> = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse trades: {} | body: {}", e, text))?;

        Ok(trades)
    }

    /// Place a market order and return a structured OrderFill.
    ///
    /// FIX: Registers the oneshot fill channel BEFORE placing the order using a
    /// pre-generated `newClientOrderId`. This eliminates the race condition where
    /// the private WS fill event arrives (5-20ms) before the channel was registered
    /// (after the ~150ms REST round-trip), causing a 5s timeout.
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
        let client_order_id = format!("arb{}", std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros());

        let (tx, rx) = oneshot::channel::<FillEvent>();
        self.pending_fills.insert(client_order_id.clone(), tx);

        // Place the order — fill may arrive via WS while this is in-flight
        let order = match self.place_order(symbol, side, quantity, &client_order_id, reduce_only, price).await {
            Ok(o) => o,
            Err(e) => {
                // Order failed — clean up the registered channel
                self.pending_fills.remove(&client_order_id);
                return Err(e);
            }
        };

        // IOC orders might immediately expire or cancel with 0 fill. Do not wait for WS.
        let executed_qty = order.executedQty.parse::<f64>().unwrap_or(0.0);
        if (order.status == "EXPIRED" || order.status == "CANCELED") && executed_qty == 0.0 {
            self.pending_fills.remove(&client_order_id);
            return Err(format!("Binance IOC order {} expired/canceled with 0 fill (slippage limit exceeded)", order.orderId));
        }

        // Await fill from private WS.
        // For FILLED orders: WS event arrives in ~5-20ms — normal fast path.
        // For NEW orders: Binance replies before matching (rare); we must wait longer.
        // If it doesn't arrive in 500ms, the event was lost, dropped, or the WS is disconnected.
        // We must fail fast so the other leg can be reversed quickly.
        let ws_timeout = std::time::Duration::from_millis(500);

        match tokio::time::timeout(ws_timeout, rx).await {
            Ok(Ok(fill)) if fill.is_expired => {
                // WS delivered an EXPIRED/CANCELED event with 0 fill — fast fail in ~20ms.
                // This replaces the old 500ms timeout + REST fallback path.
                eprintln!(
                    "[BinanceAPI] Order {} expired (WS fast-fail, ~20ms): qty=0",
                    order.orderId
                );
                Err(format!(
                    "Binance order {} expired with 0 fill (IOC limit missed — slippage exceeded)",
                    order.orderId
                ))
            }
            Ok(Ok(fill)) => {
                // Fill arrived via WS push — fast path
                Ok(OrderFill {
                    order_id:         order.orderId,
                    symbol:           order.symbol,
                    side:             order.side,
                    avg_price:        fill.avg_price,
                    filled_qty:       fill.filled_qty,
                    quote_qty:        fill.quote_qty,
                    commission:       fill.commission,
                    commission_asset: "USDT".to_string(),
                    realized_pnl:     0.0,
                    timestamp:        fill.timestamp,
                })
            }
            _ => {
                // Timeout or channel closed.
                // IMPORTANT: If the order was in NEW status on the REST response,
                // the initial avgPrice / executedQty fields are empty strings ("").
                // Using them directly would give fill_qty=0 and avg_price=0 — which
                // triggers a false "leg mismatch" and wrongly reverses the other leg.
                // Instead, query REST for the current order state.
                self.pending_fills.remove(&client_order_id);
                eprintln!(
                    "[BinanceAPI] WARNING: WS fill timeout for order {} (REST status was {}) — querying order detail via REST",
                    order.orderId, order.status
                );

                // Query order detail from REST to get real fill data
                let ts = Self::timestamp_ms();
                let query = format!("symbol={}&orderId={}&timestamp={}", symbol, order.orderId, ts);
                let signature = self.sign(&query);
                let detail_url = format!("{}/fapi/v1/order?{}&signature={}", self.base_url, query, signature);

                match self.http_slow
                    .get(&detail_url)
                    .header("X-MBX-APIKEY", &self.api_key)
                    .send()
                    .await
                {
                    Ok(detail_resp) => {
                        let detail_text = detail_resp.text().await.unwrap_or_default();
                        match serde_json::from_str::<BinanceOrderResponse>(&detail_text) {
                            Ok(detail) => {
                                let avg_price  = detail.avgPrice.parse::<f64>().unwrap_or(0.0);
                                let filled_qty = detail.executedQty.parse::<f64>().unwrap_or(0.0);
                                let quote_qty  = detail.cumQuote.parse::<f64>().unwrap_or(0.0);
                                let commission = quote_qty * 0.0005;
                                eprintln!(
                                    "[BinanceAPI] REST detail: id={} status={} avgPrice={} qty={} fee={}",
                                    order.orderId, detail.status, avg_price, filled_qty, commission
                                );
                                if avg_price <= 0.0 || filled_qty <= 0.0 {
                                    return Err(format!(
                                        "Binance order {} filled with zero data after timeout (status={}, avgPrice={}, qty={}). Check exchange manually.",
                                        order.orderId, detail.status, avg_price, filled_qty
                                    ));
                                }
                                Ok(OrderFill {
                                    order_id:         order.orderId,
                                    symbol:           order.symbol,
                                    side:             order.side,
                                    avg_price,
                                    filled_qty,
                                    quote_qty,
                                    commission,
                                    commission_asset: "USDT".to_string(),
                                    realized_pnl:     0.0,
                                    timestamp:        order.updateTime,
                                })
                            }
                            Err(e) => {
                                // Cannot parse order detail — return error so the caller
                                // can safely reverse the other leg (if any).
                                Err(format!(
                                    "Binance order {} WS timeout + REST detail parse failed: {} | body: {}",
                                    order.orderId, e, detail_text
                                ))
                            }
                        }
                    }
                    Err(e) => {
                        // REST query itself failed — log and return error.
                        Err(format!(
                            "Binance order {} WS timeout + REST query failed: {}. Check exchange manually.",
                            order.orderId, e
                        ))
                    }
                }
            }
        }
    }


    /// Set leverage for a specific symbol on Binance Futures.
    /// Must be called BEFORE placing an order to ensure the correct leverage is active.
    /// Leverage is an integer between 1 and 125 (symbol-dependent maximum).
    pub async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<(), String> {
        let ts = Self::timestamp_ms();
        let query = format!(
            "symbol={}&leverage={}&timestamp={}",
            symbol, leverage, ts
        );
        let signature = self.sign(&query);
        let url = format!(
            "{}/fapi/v1/leverage?{}&signature={}",
            self.base_url, query, signature
        );

        let resp = self.http_fast
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| format!("Binance set leverage request failed: {}", e))?;

        let status = resp.status();
        let text = resp.text().await.map_err(|e| format!("Failed to read response: {}", e))?;

        if !status.is_success() {
            // Binance returns 200 even for "leverage not changed" — a non-200 is a real error
            return Err(format!("Binance set leverage API error ({}): {}", status, text));
        }

        eprintln!("[BinanceAPI] Set leverage for {} to {}x", symbol, leverage);
        Ok(())
    }

    /// Fetch all open positions.
    #[allow(dead_code)]
    pub async fn get_positions(&self) -> Result<Vec<BinancePosition>, String> {
        let ts = Self::timestamp_ms();
        let query = format!("timestamp={}", ts);
        let signature = self.sign(&query);
        let url = format!(
            "{}/fapi/v2/positionRisk?{}&signature={}",
            self.base_url, query, signature
        );

        let resp = self.http_slow
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| format!("Binance positions request failed: {}", e))?;

        let status = resp.status();
        let text = resp.text().await.map_err(|e| format!("Failed to read response: {}", e))?;

        if !status.is_success() {
            return Err(format!("Binance positions API error ({}): {}", status, text));
        }

        let positions: Vec<BinancePosition> = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse positions: {} | body: {}", e, text))?;

        // Filter to only non-zero positions
        Ok(positions.into_iter().filter(|p| {
            p.positionAmt.parse::<f64>().unwrap_or(0.0).abs() > 0.0
        }).collect())
    }
}
