use crate::price_store::{normalize_symbol, FundingStore, PriceStore, SharedStatus};
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use chrono::Utc;


/// Binance exchange info for filtering perpetual USDT-M futures only.
#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
struct ExchangeInfoResponse {
    symbols: Vec<BinanceSymbolInfo>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
struct BinanceSymbolInfo {
    symbol: String,
    status: String,
    contractType: Option<String>,
    marginAsset: Option<String>,
}





/// Binance WebSocket individual bookTicker data (from <symbol>@bookTicker stream).
#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
struct WsBookTicker {
    /// Symbol
    s: String,
    /// Best bid price
    b: String,
    /// Best bid quantity
    #[serde(rename = "B")]
    b_qty: String,
    /// Best ask price
    a: String,
    /// Best ask quantity
    #[serde(rename = "A")]
    a_qty: String,
}

/// Flattened combined bookTicker — avoids intermediate serde_json::Value allocation.
/// Safe because we only subscribe to the !bookTicker stream.
#[derive(Debug, Deserialize)]
struct CombinedBookTicker {
    data: WsBookTicker,
}

/// Parse a bookTicker WS message into the batch map (coalescing — only latest per symbol kept).
#[inline]
fn parse_book_ticker(
    text: &str,
    perpetual_symbols: &std::collections::HashSet<String>,
    batch: &mut std::collections::HashMap<String, (Option<f64>, Option<f64>, Option<f64>, Option<f64>)>,
) {
    if let Ok(msg) = serde_json::from_str::<CombinedBookTicker>(text) {
        if perpetual_symbols.contains(&msg.data.s) {
            let key = normalize_symbol(&msg.data.s);
            let bid = msg.data.b.parse::<f64>().ok();
            let ask = msg.data.a.parse::<f64>().ok();
            let bid_qty = msg.data.b_qty.parse::<f64>().ok();
            let ask_qty = msg.data.a_qty.parse::<f64>().ok();
            batch.insert(key, (bid, ask, bid_qty, ask_qty));
        }
    }
}

/// Clear all Binance prices so stale data doesn't produce fake spreads.
fn clear_binance_prices(store: &PriceStore) {
    for mut entry in store.iter_mut() {
        let v = entry.value_mut();
        v.binance = None;
        v.binance_updated = None;
        v.binance_book = Default::default();
        v.binance_book_updated = None;
    }
}


/// Connects to Binance Futures via WebSocket for real-time price and orderbook data.
/// Pure WS-only: uses !bookTicker all-symbol stream (tick-by-tick, no REST polling).
pub async fn run(store: PriceStore, status: SharedStatus, funding: FundingStore) {
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .tcp_nodelay(true)
        .build()
        .unwrap_or_default();

    // Wait until Binance is enabled
    while !status.binance_enabled.load(Ordering::Relaxed) {
        status.binance_connected.store(false, Ordering::Relaxed);
        clear_binance_prices(&store);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Fetch exchange info to find PERPETUAL USDT-margined contracts
    eprintln!("[Binance] Fetching exchange info for perpetual filter...");
    let perpetual_symbols: std::collections::HashSet<String> = match http_client
        .get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        .send()
        .await
    {
        Ok(r) => match r.json::<ExchangeInfoResponse>().await {
            Ok(info) => {
                let set: std::collections::HashSet<String> = info
                    .symbols
                    .into_iter()
                    .filter(|s| {
                        s.status == "TRADING"
                            && s.contractType.as_deref() == Some("PERPETUAL")
                            && s.marginAsset.as_deref() == Some("USDT")
                    })
                    .map(|s| s.symbol)
                    .collect();

                // Populate default funding for Binance symbols (8h)
                for sym in &set {
                    let key = normalize_symbol(sym);
                    funding.entry(key).or_insert(8);
                }

                eprintln!("[Binance] Found {} USDT-M Perpetual pairs", set.len());
                set
            }
            Err(e) => {
                eprintln!("[Binance] Failed to parse exchange info: {}", e);
                std::collections::HashSet::new()
            }
        },
        Err(e) => {
            eprintln!("[Binance] Failed to fetch exchange info: {}", e);
            std::collections::HashSet::new()
        }
    };

    if perpetual_symbols.is_empty() {
        eprintln!("[Binance] WARNING: Failed to load perpetual symbols — reconnecting");
        status.binance_connected.store(false, Ordering::Relaxed);
        return;
    }

    // ── Connect WebSocket ──
    // Pure real-time !bookTicker stream (tick-by-tick, 0ms exchange delay)
    let ws_url = "wss://fstream.binance.com/stream?streams=!bookTicker";
    let url = Url::parse(ws_url).expect("Failed to parse Binance WS URL");

    eprintln!("[Binance] Connecting WebSocket to {}...", ws_url);
    let (ws_stream, _) = match connect_async(url).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[Binance] WebSocket connection failed: {} — falling back to REST", e);
            status.binance_connected.store(false, Ordering::Relaxed);
            return;
        }
    };

    status.binance_connected.store(true, Ordering::Relaxed);
    eprintln!("[Binance] WebSocket connected (WS-only, !bookTicker)");

    let (mut write, mut read) = ws_stream.split();

    let mut ping_interval = tokio::time::interval(Duration::from_secs(20));
    let mut enable_check = tokio::time::interval(Duration::from_millis(500));
    let mut last_msg_time = tokio::time::Instant::now();
    let dead_timeout = Duration::from_secs(30);

    loop {
        tokio::select! {
            _ = enable_check.tick() => {
                if !status.binance_enabled.load(Ordering::Relaxed) {
                    eprintln!("[Binance] Disabled — disconnecting WebSocket");
                    status.binance_connected.store(false, Ordering::Relaxed);
                    clear_binance_prices(&store);
                    let _ = write.send(Message::Close(None)).await;
                    break;
                }
            }
            _ = ping_interval.tick() => {
                if last_msg_time.elapsed() > dead_timeout {
                    eprintln!("[Binance] No messages for {}s — reconnecting", dead_timeout.as_secs());
                    status.binance_connected.store(false, Ordering::Relaxed);
                    clear_binance_prices(&store);
                    break;
                }
                // Send WebSocket ping frame
                if write.send(Message::Ping(vec![])).await.is_err() {
                    eprintln!("[Binance] Failed to send ping");
                    status.binance_connected.store(false, Ordering::Relaxed);
                    clear_binance_prices(&store);
                    break;
                }
            }

            msg = read.next() => {
                let msg = match msg {
                    Some(m) => m,
                    None => {
                        eprintln!("[Binance] WebSocket stream ended");
                        status.binance_connected.store(false, Ordering::Relaxed);
                        clear_binance_prices(&store);
                        break;
                    }
                };

                match msg {
                    Ok(Message::Text(text)) => {
                        last_msg_time = tokio::time::Instant::now();

                        // ── BUFFER DRAIN: Coalesce all queued messages, keep only latest per symbol ──
                        // The !bookTicker firehose sends 5000+ msgs/sec across all pairs.
                        // Without draining, messages queue up 1-4 seconds behind real-time.
                        let mut batch: std::collections::HashMap<String, (Option<f64>, Option<f64>, Option<f64>, Option<f64>)> =
                            std::collections::HashMap::with_capacity(64);

                        // Parse first message
                        parse_book_ticker(&text, &perpetual_symbols, &mut batch);

                        // Drain all immediately-available buffered messages (non-blocking)
                        let mut should_break = false;
                        loop {
                            match read.next().now_or_never() {
                                Some(Some(Ok(Message::Text(t)))) => {
                                    last_msg_time = tokio::time::Instant::now();
                                    parse_book_ticker(&t, &perpetual_symbols, &mut batch);
                                }
                                Some(Some(Ok(Message::Ping(data)))) => {
                                    last_msg_time = tokio::time::Instant::now();
                                    let _ = write.send(Message::Pong(data)).await;
                                }
                                Some(Some(Ok(Message::Pong(_)))) => {
                                    last_msg_time = tokio::time::Instant::now();
                                }
                                Some(Some(Ok(Message::Close(_)))) => { should_break = true; break; }
                                Some(Some(Err(_))) => { should_break = true; break; }
                                Some(None) => { should_break = true; break; }
                                None => break, // No more buffered — caught up to real-time
                                _ => break,
                            }
                        }

                        // Write coalesced batch to store — only the LATEST price per symbol
                        let now = Instant::now();
                        let epoch_ms = Utc::now().timestamp_millis();
                        for (key, (bid, ask, bid_qty, ask_qty)) in &batch {
                            let mut entry = store.entry(key.clone()).or_default();
                            if let (Some(bp), Some(ap)) = (*bid, *ask) {
                                if bp > 0.0 && ap > 0.0 {
                                    entry.binance = Some((bp + ap) / 2.0);
                                    entry.binance_updated = Some(now);
                                }
                            }
                            entry.binance_book.best_bid = *bid;
                            entry.binance_book.best_bid_qty = *bid_qty;
                            entry.binance_book.best_ask = *ask;
                            entry.binance_book.best_ask_qty = *ask_qty;
                            entry.binance_book_updated = Some(now);
                            entry.binance_book_epoch_ms = Some(epoch_ms);
                        }
                        if !batch.is_empty() {
                            status.binance_updates.fetch_add(batch.len() as u64, Ordering::Relaxed);
                        }

                        if should_break {
                            eprintln!("[Binance] WebSocket closed during buffer drain");
                            status.binance_connected.store(false, Ordering::Relaxed);
                            clear_binance_prices(&store);
                            break;
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        last_msg_time = tokio::time::Instant::now();
                        let _ = write.send(Message::Pong(data)).await;
                    }
                    Ok(Message::Pong(_)) => {
                        last_msg_time = tokio::time::Instant::now();
                    }
                    Ok(Message::Close(_)) => {
                        eprintln!("[Binance] WebSocket closed by server");
                        status.binance_connected.store(false, Ordering::Relaxed);
                        clear_binance_prices(&store);
                        break;
                    }
                    Err(e) => {
                        eprintln!("[Binance] WebSocket error: {}", e);
                        status.binance_connected.store(false, Ordering::Relaxed);
                        clear_binance_prices(&store);
                        break;
                    }
                    _ => {
                        last_msg_time = tokio::time::Instant::now();
                    }
                }
            }
        }
    }
}
