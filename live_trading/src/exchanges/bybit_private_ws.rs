/// Bybit V5 private WebSocket client.
///
/// Subscribes to private topics which deliver:
///   - execution  → instantly notifies when an order fills (fires per-fill, fastest path)
///   - order      → order status updates (terminal states: Filled, Cancelled, Expired)
///   - wallet     → instantly updates USDT balance
///
/// IMPORTANT: We subscribe to BOTH "execution" AND "order" topics:
///   - "execution" fires per fill execution (~5-20ms) — fast path for fills
///   - "order" fires at terminal status — catches Cancelled/Expired 0-fill events
///   - "wallet" updates balance in real-time
///
/// Protocol:
///   1. Connect wss://stream.bybit.com/v5/private
///   2. Send auth: {"op":"auth","args":[apiKey, expires, signature]}
///   3. Subscribe: {"op":"subscribe","args":["execution","order","wallet"]}
///   4. Parse execution events → resolve pending fill channel (fast path)
///   5. Parse order events     → fast-fail on Cancelled/Expired with 0 fill
///   6. Parse wallet events    → update live balance
///   7. Respond to ping with pong to keep connection alive

use super::fill_channel::{FillEvent, LiveBalance, PendingFillMap};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

type HmacSha256 = Hmac<Sha256>;

// ── Bybit private WS event shapes ──────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct AnyMsg {
    #[serde(default)]
    topic: String,
    #[serde(default)]
    op: String,
    #[serde(default)]
    success: bool,
}

/// Execution event data item ("execution" topic — fires per trade execution, FAST).
/// This is the primary fill notification path (~5-20ms after fill).
#[derive(Debug, Deserialize)]
#[allow(non_snake_case, dead_code)]
struct BybitExecEv {
    #[serde(default)]
    pub orderId:      String,
    #[serde(default)]
    pub orderLinkId:  String,
    #[serde(default)]
    pub symbol:       String,
    #[serde(default)]
    pub side:         String,
    #[serde(default)]
    pub execPrice:    String,  // price of THIS execution
    #[serde(default)]
    pub execQty:      String,  // qty of THIS execution
    #[serde(default)]
    pub execValue:    String,  // notional of THIS execution
    #[serde(default)]
    pub execFee:      String,  // fee for THIS execution
    #[serde(default)]
    pub execTime:     String,  // execution timestamp (ms)
    #[serde(default)]
    pub execType:     String,  // "Trade", "Funding", etc.
    #[serde(default)]
    pub closedSize:   String,
}

#[derive(Debug, Deserialize)]
struct ExecEvMsg {
    data: Vec<BybitExecEv>,
}

/// Order event data item ("order" topic — fires at terminal status, used for fast-fail).
#[derive(Debug, Deserialize)]
#[allow(non_snake_case, dead_code)]
struct BybitOrderEv {
    #[serde(default)]
    pub orderId:      String,
    #[serde(default)]
    pub orderLinkId:  String,
    #[serde(default)]
    pub symbol:       String,
    #[serde(default)]
    pub side:         String,
    #[serde(default)]
    pub orderStatus:  String,
    #[serde(default)]
    pub avgPrice:     String,
    #[serde(default)]
    pub cumExecQty:   String,
    #[serde(default)]
    pub cumExecValue: String,
    #[serde(default)]
    pub cumExecFee:   String,
    #[serde(default)]
    pub updatedTime:  String,
}

#[derive(Debug, Deserialize)]
struct OrderEvMsg {
    data: Vec<BybitOrderEv>,
}

/// Wallet event data item.
#[derive(Debug, Deserialize)]
#[allow(non_snake_case, dead_code)]
struct BybitCoin {
    pub coin:                 String,
    #[serde(default)]
    pub availableToWithdraw:  String,
    #[serde(default)]
    pub walletBalance:        String,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case, dead_code)]
struct BybitWalletAccount {
    pub accountType: String,
    #[serde(default)]
    pub coin:        Vec<BybitCoin>,
}

#[derive(Debug, Deserialize)]
struct WalletMsg {
    data: Vec<BybitWalletAccount>,
}

// ── Auth helpers ───────────────────────────────────────────────────────────

fn sign(secret: &str, payload: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC key error");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// ── Main task ──────────────────────────────────────────────────────────────

/// Run the Bybit private WebSocket listener.
/// On disconnect, caller should re-invoke (wrapped in a reconnect loop in main.rs).
pub async fn run(
    api_key:       String,
    api_secret:    String,
    pending_fills: PendingFillMap,
    live_balance:  LiveBalance,
) {
    let ws_url = url::Url::parse("wss://stream.bybit.com/v5/private")
        .expect("Invalid Bybit private WS URL");

    let (mut ws, _) = match connect_async(ws_url).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[BybitPrivateWS] WS connect failed: {}", e);
            return;
        }
    };

    // ── Step 1: Authenticate ──────────────────────────────────────────────
    // Bybit auth: sign( timestamp + api_key + expires )
    // expires = now_ms + 10_000  (10 second window)
    let ts    = timestamp_ms();
    let expires = ts + 10_000;
    let payload = format!("GET/realtime{}", expires);
    let signature = sign(&api_secret, &payload);

    let auth_msg = serde_json::json!({
        "op": "auth",
        "args": [api_key, expires, signature]
    });

    if let Err(e) = ws.send(Message::Text(auth_msg.to_string())).await {
        eprintln!("[BybitPrivateWS] Auth send failed: {}", e);
        return;
    }

    // Wait for auth response
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(t))) => {
                if let Ok(msg) = serde_json::from_str::<AnyMsg>(&t) {
                    if msg.op == "auth" {
                        if msg.success {
                            eprintln!("[BybitPrivateWS] Authenticated");
                        } else {
                            eprintln!("[BybitPrivateWS] Auth FAILED: {}", t);
                            return;
                        }
                        break;
                    }
                }
            }
            Some(Err(e)) => {
                eprintln!("[BybitPrivateWS] Error during auth: {}", e);
                return;
            }
            _ => continue,
        }
    }

    // ── Step 2: Subscribe to topics ──────────────────────────────────────
    // IMPORTANT: Subscribe to "execution" (instant per-fill events) AND "order" (terminal
    // status events for Cancelled/Expired 0-fill fast-fail) AND "wallet" (balance updates).
    // "execution" is the PRIMARY fill path (~5-20ms). "order" catches 0-fill terminations.
    let sub_msg = serde_json::json!({
        "op": "subscribe",
        "args": ["execution", "order", "wallet"]
    });

    if let Err(e) = ws.send(Message::Text(sub_msg.to_string())).await {
        eprintln!("[BybitPrivateWS] Subscribe send failed: {}", e);
        return;
    }

    eprintln!("[BybitPrivateWS] Connected — subscribed to execution+order+wallet topics");

    // ── Step 4: Main message loop ─────────────────────────────────────────
    let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(20));
    ping_interval.tick().await; // skip immediate first tick

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                let ping = serde_json::json!({"op": "ping"});
                if let Err(e) = ws.send(Message::Text(ping.to_string())).await {
                    eprintln!("[BybitPrivateWS] Ping send failed: {}", e);
                    break;
                }
            }

            msg = ws.next() => {
                let text = match msg {
                    Some(Ok(Message::Text(t))) => t,
                    Some(Ok(Message::Ping(p))) => {
                        let _ = ws.send(Message::Pong(p)).await;
                        continue;
                    }
                    Some(Ok(Message::Close(_))) => {
                        eprintln!("[BybitPrivateWS] Connection closed by server");
                        break;
                    }
                    Some(Err(e)) => {
                        eprintln!("[BybitPrivateWS] WS error: {}", e);
                        break;
                    }
                    None => {
                        eprintln!("[BybitPrivateWS] Stream ended");
                        break;
                    }
                    _ => continue,
                };

                let msg_meta = match serde_json::from_str::<AnyMsg>(&text) {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                // Ignore pong/subscribe confirmations
                if msg_meta.op == "pong" || msg_meta.op == "subscribe" || msg_meta.op == "ping" {
                    continue;
                }

                match msg_meta.topic.as_str() {
                    "execution" => {
                        // PRIMARY FILL PATH: fires per-fill in ~5-20ms
                        match serde_json::from_str::<ExecEvMsg>(&text) {
                            Ok(ev) => {
                                for o in ev.data {
                                    // Only process actual trade executions
                                    if o.execType != "Trade" && !o.execType.is_empty() {
                                        continue;
                                    }

                                    let avg_price  = o.execPrice.parse::<f64>().unwrap_or(0.0);
                                    let filled_qty = o.execQty.parse::<f64>().unwrap_or(0.0);
                                    let quote_qty  = o.execValue.parse::<f64>().unwrap_or(0.0);
                                    let commission = o.execFee.parse::<f64>().unwrap_or(0.0).abs();
                                    let ts         = o.execTime.parse::<u64>().unwrap_or(0);

                                    eprintln!(
                                        "[BybitPrivateWS] Execution (FAST): order={} linkId={} qty={} execPrice={} fee={}",
                                        o.orderId, o.orderLinkId, filled_qty, avg_price, commission
                                    );

                                    // Match on orderLinkId first (our pre-registered key),
                                    // then fall back to orderId
                                    let matched_key = if !o.orderLinkId.is_empty()
                                        && pending_fills.contains_key(&o.orderLinkId)
                                    {
                                        o.orderLinkId.clone()
                                    } else if pending_fills.contains_key(&o.orderId) {
                                        o.orderId.clone()
                                    } else {
                                        // No pending fill registered — already resolved or stale
                                        eprintln!("[BybitPrivateWS] No pending fill for linkId={} orderId={}", o.orderLinkId, o.orderId);
                                        continue;
                                    };

                                    if let Some((_, sender)) = pending_fills.remove(&matched_key) {
                                        let _ = sender.send(FillEvent {
                                            order_id:  o.orderId,
                                            avg_price,
                                            filled_qty,
                                            quote_qty,
                                            commission,
                                            timestamp: ts,
                                            is_expired: false, // execution topic only fires for actual fills
                                        });
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("[BybitPrivateWS] Failed to parse execution event: {} | raw: {}", e, text);
                            }
                        }
                    }

                    "order" => {
                        // SECONDARY PATH: terminal status events — used ONLY for fast-fail on
                        // Cancelled/Expired with 0 fill. Filled orders are caught by "execution" above.
                        match serde_json::from_str::<OrderEvMsg>(&text) {
                            Ok(ev) => {
                                for o in ev.data {
                                    // Only process terminal states
                                    if o.orderStatus != "Filled" && o.orderStatus != "PartiallyFilledCanceled"
                                        && o.orderStatus != "Cancelled" && o.orderStatus != "Rejected"
                                        && o.orderStatus != "Expired" {
                                        continue;
                                    }

                                    let avg_price  = o.avgPrice.parse::<f64>().unwrap_or(0.0);
                                    let filled_qty = o.cumExecQty.parse::<f64>().unwrap_or(0.0);
                                    let quote_qty  = o.cumExecValue.parse::<f64>().unwrap_or(0.0);
                                    let commission = o.cumExecFee.parse::<f64>().unwrap_or(0.0).abs();
                                    let ts         = o.updatedTime.parse::<u64>().unwrap_or(0);

                                    // Check if this is a 0-fill termination that needs fast-fail
                                    let is_expired = (o.orderStatus == "Cancelled" || o.orderStatus == "Expired"
                                        || o.orderStatus == "PartiallyFilledCanceled" || o.orderStatus == "Rejected")
                                        && filled_qty == 0.0;

                                    eprintln!(
                                        "[BybitPrivateWS] Order terminal ({}): order={} linkId={} qty={} is_expired={}",
                                        o.orderStatus, o.orderId, o.orderLinkId, filled_qty, is_expired
                                    );

                                    // Only act on this if pending fill still exists
                                    // (execution topic may have already resolved it)
                                    let matched_key = if !o.orderLinkId.is_empty()
                                        && pending_fills.contains_key(&o.orderLinkId)
                                    {
                                        o.orderLinkId.clone()
                                    } else if pending_fills.contains_key(&o.orderId) {
                                        o.orderId.clone()
                                    } else {
                                        // Already resolved by execution topic — skip
                                        continue;
                                    };

                                    if let Some((_, sender)) = pending_fills.remove(&matched_key) {
                                        let _ = sender.send(FillEvent {
                                            order_id:  o.orderId,
                                            avg_price,
                                            filled_qty,
                                            quote_qty,
                                            commission,
                                            timestamp: ts,
                                            is_expired,
                                        });
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("[BybitPrivateWS] Failed to parse order event: {} | raw: {}", e, text);
                            }
                        }
                    }

                    "wallet" => {
                        if let Ok(ev) = serde_json::from_str::<WalletMsg>(&text) {
                            for account in &ev.data {
                                for coin in &account.coin {
                                    if coin.coin == "USDT" {
                                        let raw = if !coin.walletBalance.is_empty() {
                                            &coin.walletBalance
                                        } else {
                                            &coin.availableToWithdraw
                                        };
                                        if let Ok(b) = raw.parse::<f64>() {
                                            *live_balance.write().await = b;
                                            eprintln!("[BybitPrivateWS] Balance updated: ${:.2}", b);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    _ => {} // subscribe confirmations, other topics
                }
            }
        }
    }

    eprintln!("[BybitPrivateWS] Disconnected");
}
