/// Binance Futures private WebSocket client.
///
/// Subscribes to the User Data Stream which delivers:
///   - ORDER_TRADE_UPDATE  → instantly notifies when an order fills
///   - ACCOUNT_UPDATE      → instantly updates USDT balance
///
/// This completely replaces the REST fill-poll loop (was 50ms × 10 = up to 500ms).
/// Fill notifications now arrive in ~5–20ms via push.
///
/// Protocol:
///   1. POST /fapi/v1/listenKey  → get a listenKey (valid 60 min)
///   2. Connect wss://fstream.binance.com/ws/<listenKey>
///   3. Keep alive: PUT /fapi/v1/listenKey every 25 min
///   4. Parse ORDER_TRADE_UPDATE → resolve pending fill channel
///   5. Parse ACCOUNT_UPDATE     → update live balance

use super::fill_channel::{FillEvent, LiveBalance, PendingFillMap};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

// ── Binance User Data Stream event shapes ──────────────────────────────────

#[derive(Debug, Deserialize)]
struct AnyEvent {
    #[serde(rename = "e")]
    event_type: String,
}

/// ORDER_TRADE_UPDATE inner order object.
#[derive(Debug, Deserialize)]
#[allow(non_snake_case, dead_code)]
struct OrderUpdate {
    #[serde(default, rename = "s")]
    symbol: String,
    #[serde(default, rename = "S")]
    side: String,
    #[serde(default, rename = "X")]
    status: String,        // FILLED, PARTIALLY_FILLED, etc.
    #[serde(default, rename = "i")]
    order_id: u64,
    #[serde(default, rename = "c")]
    client_order_id: String, // newClientOrderId we sent — our pre-registered fill key
    #[serde(default, rename = "z")]
    cum_filled_qty: String,     // cumulative filled quantity
    #[serde(default, rename = "ap")]
    avg_price: String,          // average fill price
    #[serde(default, rename = "T")]
    trade_time: u64,
    #[serde(default, rename = "n")]
    commission: String,         // commission amount
    #[serde(default, rename = "N")]
    commission_asset: Option<String>,
    #[serde(default, rename = "cp")]
    cum_quote: Option<String>,  // cumulative quote (notional)
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
struct OrderTradeUpdateEvent {
    #[serde(rename = "o")]
    order: OrderUpdate,
}

/// ACCOUNT_UPDATE balance entry.
#[derive(Debug, Deserialize)]
#[allow(non_snake_case, dead_code)]
struct BalanceEntry {
    #[serde(default, rename = "a")]
    asset: String,
    #[serde(default, rename = "wb")]
    wallet_balance: String,
    #[serde(default, rename = "cw")]
    cross_wallet_balance: String,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
struct AccountUpdateData {
    #[serde(rename = "B")]
    balances: Vec<BalanceEntry>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
struct AccountUpdateEvent {
    #[serde(rename = "a")]
    data: AccountUpdateData,
}

// ── listenKey management ───────────────────────────────────────────────────

async fn create_listen_key(api_key: &str) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    let resp = client
        .post("https://fapi.binance.com/fapi/v1/listenKey")
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(|e| format!("Binance listenKey request failed: {}", e))?;

    let status = resp.status();
    let text = resp.text().await.map_err(|e| e.to_string())?;

    if !status.is_success() {
        return Err(format!("Binance listenKey error ({}): {}", status, text));
    }

    #[derive(Deserialize)]
    struct Resp {
        #[serde(rename = "listenKey")]
        listen_key: String,
    }
    let r: Resp = serde_json::from_str(&text)
        .map_err(|e| format!("Failed to parse listenKey: {}", e))?;

    Ok(r.listen_key)
}

async fn keep_alive_listen_key(api_key: &str, listen_key: &str) {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    match client
        .put("https://fapi.binance.com/fapi/v1/listenKey")
        .header("X-MBX-APIKEY", api_key)
        .query(&[("listenKey", listen_key)])
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => {
            eprintln!("[BinancePrivateWS] listenKey refreshed");
        }
        Ok(r) => {
            eprintln!("[BinancePrivateWS] listenKey refresh failed: {}", r.status());
        }
        Err(e) => {
            eprintln!("[BinancePrivateWS] listenKey refresh error: {}", e);
        }
    }
}

// ── Main task ──────────────────────────────────────────────────────────────

/// Run the Binance private WebSocket listener.
/// Spawns a background task to keep the listenKey alive every 25 minutes.
/// On disconnect, caller should re-invoke (wrapped in a reconnect loop in main.rs).
pub async fn run(
    api_key:       String,
    pending_fills: PendingFillMap,
    live_balance:  LiveBalance,
) {
    // Obtain a listenKey
    let listen_key = match create_listen_key(&api_key).await {
        Ok(k) => k,
        Err(e) => {
            eprintln!("[BinancePrivateWS] Failed to get listenKey: {}", e);
            return;
        }
    };

    eprintln!("[BinancePrivateWS] Connected — listening for fills and balance updates");

    // Keep-alive task: PUT every 25 minutes (listenKey expires after 60 min)
    {
        let ak = api_key.clone();
        let lk = listen_key.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(25 * 60));
            interval.tick().await; // skip first immediate tick
            loop {
                interval.tick().await;
                keep_alive_listen_key(&ak, &lk).await;
            }
        });
    }

    // Connect to the user data stream
    let url = format!("wss://fstream.binance.com/ws/{}", listen_key);
    let ws_url = url::Url::parse(&url).expect("Invalid Binance WS URL");

    let (mut ws, _) = match connect_async(ws_url).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[BinancePrivateWS] WS connect failed: {}", e);
            return;
        }
    };

    // Main message loop
    while let Some(msg) = ws.next().await {
        let text = match msg {
            Ok(Message::Text(t)) => t,
            Ok(Message::Ping(p)) => {
                let _ = ws.send(Message::Pong(p)).await;
                continue;
            }
            Ok(Message::Close(_)) => {
                eprintln!("[BinancePrivateWS] Connection closed by server");
                break;
            }
            Err(e) => {
                eprintln!("[BinancePrivateWS] WS error: {}", e);
                break;
            }
            _ => continue,
        };

        // Peek at the event type
        let event_type = match serde_json::from_str::<AnyEvent>(&text) {
            Ok(e) => e.event_type,
            Err(_) => continue,
        };

        match event_type.as_str() {
            "ORDER_TRADE_UPDATE" => {
                match serde_json::from_str::<OrderTradeUpdateEvent>(&text) {
                    Ok(ev) => {
                        let o = &ev.order;

                        // Wait for terminal state to get final cumulative filled quantity.
                        // IOC orders will be FILLED or EXPIRED (if partially filled).
                        if o.status != "FILLED" && o.status != "EXPIRED" && o.status != "CANCELED" && o.status != "REJECTED" {
                            continue;
                        }

                        let order_id_str = o.order_id.to_string();
                        let avg_price   = o.avg_price.parse::<f64>().unwrap_or(0.0);
                        let filled_qty  = o.cum_filled_qty.parse::<f64>().unwrap_or(0.0);
                        let commission  = o.commission.parse::<f64>().unwrap_or(0.0).abs();
                        let quote_qty   = avg_price * filled_qty;

                        eprintln!(
                            "[BinancePrivateWS] Fill: order={} clientId={} status={} qty={} avgPrice={} fee={}",
                            order_id_str, o.client_order_id, o.status, filled_qty, avg_price, commission
                        );

                        // Match on clientOrderId first (our pre-registered key),
                        // then fall back to orderId for backward compatibility.
                        let matched_key = if !o.client_order_id.is_empty()
                            && pending_fills.contains_key(&o.client_order_id)
                        {
                            o.client_order_id.clone()
                        } else {
                            order_id_str.clone()
                        };

                        // Deliver fill to waiting execute_order_with_fill().
                        // Set is_expired=true for EXPIRED/CANCELED with 0 fill so the caller
                        // can fast-fail in ~20ms instead of hitting the 500ms timeout.
                        let is_expired = (o.status == "EXPIRED" || o.status == "CANCELED" || o.status == "REJECTED")
                            && filled_qty == 0.0;

                        if let Some((_, sender)) = pending_fills.remove(&matched_key) {
                            let _ = sender.send(FillEvent {
                                order_id:   order_id_str,
                                avg_price,
                                filled_qty,
                                quote_qty,
                                commission,
                                timestamp:  o.trade_time,
                                is_expired,
                            });
                        }
                    }
                    Err(e) => {
                        eprintln!("[BinancePrivateWS] Failed to parse ORDER_TRADE_UPDATE: {} | raw: {}", e, text);
                    }
                }
            }

            "ACCOUNT_UPDATE" => {
                if let Ok(ev) = serde_json::from_str::<AccountUpdateEvent>(&text) {
                    for bal in &ev.data.balances {
                        if bal.asset == "USDT" {
                            let raw = if !bal.cross_wallet_balance.is_empty() && bal.cross_wallet_balance != "0" {
                                &bal.cross_wallet_balance
                            } else if !bal.wallet_balance.is_empty() {
                                &bal.wallet_balance
                            } else {
                                ""
                            };
                            if let Ok(b) = raw.parse::<f64>() {
                                *live_balance.write().await = b;
                                eprintln!("[BinancePrivateWS] Balance updated: ${:.2}", b);
                            }
                        }
                    }
                }
            }

            _ => {} // Ignore other event types
        }
    }

    eprintln!("[BinancePrivateWS] Disconnected");
}
