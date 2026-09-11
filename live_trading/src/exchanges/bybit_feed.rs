use crate::price_store::{normalize_symbol, FundingStore, PriceStore, SharedStatus};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use chrono::Utc;


// REST API parsing — instruments
#[derive(Deserialize, Debug)]
struct InstrumentsResponse {
    result: InstrumentsResult,
}

#[derive(Deserialize, Debug)]
struct InstrumentsResult {
    list: Vec<Instrument>,
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct Instrument {
    symbol: String,
    status: String,
    #[serde(default)]
    contractType: Option<String>,
    #[serde(default)]
    symbolType: Option<String>,
    #[serde(default)]
    fundingInterval: Option<u64>, // minutes
}



// WebSocket parsing
#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct WsResponse {
    topic: Option<String>,
    data: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct TickerData {
    symbol: String,
    lastPrice: Option<String>,
    bid1Price: Option<String>,
    bid1Size: Option<String>,
    ask1Price: Option<String>,
    ask1Size: Option<String>,
}

/// Orderbook data from orderbook.1 channel
#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct OrderBookData {
    s: String,
    b: Vec<Vec<String>>,
    a: Vec<Vec<String>>,
}

/// Clear all Bybit prices.
fn clear_bybit_prices(store: &PriceStore) {
    for mut entry in store.iter_mut() {
        let v = entry.value_mut();
        v.bybit = None;
        v.bybit_updated = None;
        v.bybit_book = Default::default();
        v.bybit_book_updated = None;
    }
}


/// Connects to Bybit USDT-M Perpetual Futures WebSocket (read-only, no auth).
pub async fn run(store: PriceStore, status: SharedStatus, funding: FundingStore) {
    // Wait until Bybit is enabled
    while !status.bybit_enabled.load(Ordering::Relaxed) {
        status.bybit_connected.store(false, Ordering::Relaxed);
        clear_bybit_prices(&store);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    eprintln!("[Bybit] Fetching instruments...");

    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .unwrap_or_default();

    let res: InstrumentsResponse = match http_client
        .get("https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000")
        .send()
        .await
    {
        Ok(r) => match r.json().await {
            Ok(j) => j,
            Err(e) => {
                eprintln!("[Bybit] Failed to parse instruments: {}", e);
                return;
            }
        },
        Err(e) => {
            eprintln!("[Bybit] Failed to fetch instruments: {}", e);
            return;
        }
    };

    // Filter: ONLY actively trading LinearPerpetual crypto contracts (not stocks, commodities, or ETFs)
    // NOTE: Bybit lists synthetic US stocks like ONUSDT (On Semiconductor), AAPLUSDT, etc.
    // Filtering out non-crypto prevents ticker collisions against actual crypto tokens.
    let symbols: Vec<String> = res.result.list
        .iter()
        .filter(|i| {
            let s = i.status == "Trading";
            let ct = i.contractType.as_deref() == Some("LinearPerpetual")
                  || i.contractType.as_deref() == Some("PERPETUAL");
            let is_non_crypto = match i.symbolType.as_deref() {
                Some("stock") | Some("commodity") | Some("ETF") => true,
                _ => false,
            };
            s && ct && !is_non_crypto && i.symbol.ends_with("USDT")
        })
        .map(|i| i.symbol.clone())
        .collect();

    // Populate funding intervals
    for inst in &res.result.list {
        if !symbols.contains(&inst.symbol) { continue; }
        let key = normalize_symbol(&inst.symbol);
        let hours = inst.fundingInterval
            .map(|m| (m / 60).max(1) as u32)
            .unwrap_or(8);
        let mut entry = funding.entry(key).or_insert(hours);
        if hours < *entry { *entry = hours; }
    }
    eprintln!("[Bybit] Found {} USDT perpetual symbols", symbols.len());

    // ── Pre-populate all Bybit tickers from REST snapshot (instant 100% price coverage) ──
    eprintln!("[Bybit] Fetching initial ticker snapshot via REST...");
    #[derive(Deserialize)]
    struct TickersRestResponse {
        result: TickersRestResult,
    }
    #[derive(Deserialize)]
    struct TickersRestResult {
        list: Vec<TickerData>,
    }

    if let Ok(res) = http_client
        .get("https://api.bybit.com/v5/market/tickers?category=linear")
        .send()
        .await
    {
        if let Ok(json) = res.json::<TickersRestResponse>().await {
            let now = Instant::now();
            let epoch_ms = Utc::now().timestamp_millis();
            let mut count = 0;
            for t in json.result.list {
                if !symbols.contains(&t.symbol) {
                    continue;
                }
                let key = normalize_symbol(&t.symbol);
                let mut entry = store.entry(key).or_default();
                if let Some(ref bp) = t.bid1Price {
                    if let Ok(bid) = bp.parse::<f64>() {
                        if bid > 0.0 { entry.bybit_book.best_bid = Some(bid); }
                    }
                }
                if let Some(ref bq) = t.bid1Size {
                    if let Ok(qty) = bq.parse::<f64>() { entry.bybit_book.best_bid_qty = Some(qty); }
                }
                if let Some(ref ap) = t.ask1Price {
                    if let Ok(ask) = ap.parse::<f64>() {
                        if ask > 0.0 { entry.bybit_book.best_ask = Some(ask); }
                    }
                }
                if let Some(ref aq) = t.ask1Size {
                    if let Ok(qty) = aq.parse::<f64>() { entry.bybit_book.best_ask_qty = Some(qty); }
                }
                entry.bybit_book_updated = Some(now);
                entry.bybit_book_epoch_ms = Some(epoch_ms);

                let price = match (entry.bybit_book.best_bid, entry.bybit_book.best_ask) {
                    (Some(b), Some(a)) if b > 0.0 && a > 0.0 => (b + a) / 2.0,
                    _ => t.lastPrice.as_deref().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0),
                };
                if price > 0.0 {
                    entry.bybit = Some(price);
                    entry.bybit_updated = Some(now);
                    count += 1;
                }
            }
            eprintln!("[Bybit] Loaded initial prices for {} symbols via REST snapshot", count);
        }
    }

    let connect_url = "wss://stream.bybit.com/v5/public/linear";
    let url = Url::parse(connect_url).expect("Failed to parse Bybit URL");

    eprintln!("[Bybit] Connecting...");
    let (ws_stream, _) = match connect_async(url).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[Bybit] Connection failed: {}", e);
            return;
        }
    };

    status.bybit_connected.store(true, Ordering::Relaxed);
    eprintln!("[Bybit] Connected (Production)");

    let (mut write, mut read) = ws_stream.split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(200);

    // Subscribe to tickers in background task so reading starts immediately
    let sub_tx = tx.clone();
    let sub_symbols = symbols.clone();
    tokio::spawn(async move {
        for chunk in sub_symbols.chunks(20) {
            let mut args: Vec<String> = Vec::with_capacity(chunk.len());
            for s in chunk {
                args.push(format!("tickers.{}", s));
            }
            let sub_msg = serde_json::json!({
                "op": "subscribe",
                "args": args
            });
            if sub_tx.send(Message::Text(sub_msg.to_string())).await.is_err() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        eprintln!("[Bybit] Subscribed to {} perpetual symbols (tickers via WS-only)", sub_symbols.len());
    });

    let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(20));
    let mut enable_check = tokio::time::interval(std::time::Duration::from_millis(500));
    let mut last_msg_time = tokio::time::Instant::now();
    let dead_timeout = std::time::Duration::from_secs(30);

    loop {
        tokio::select! {
            _ = enable_check.tick() => {
                if !status.bybit_enabled.load(Ordering::Relaxed) {
                    eprintln!("[Bybit] Disabled — disconnecting WebSocket");
                    status.bybit_connected.store(false, Ordering::Relaxed);
                    clear_bybit_prices(&store);
                    let _ = write.send(Message::Close(None)).await;
                    break;
                }
            }
            _ = ping_interval.tick() => {
                if last_msg_time.elapsed() > dead_timeout {
                    eprintln!("[Bybit] No messages for {}s — reconnecting", dead_timeout.as_secs());
                    status.bybit_connected.store(false, Ordering::Relaxed);
                    clear_bybit_prices(&store);
                    break;
                }
                let ping_msg = r#"{"op":"ping"}"#.to_string();
                if write.send(Message::Text(ping_msg)).await.is_err() {
                    eprintln!("[Bybit] Failed to send ping");
                    status.bybit_connected.store(false, Ordering::Relaxed);
                    clear_bybit_prices(&store);
                    break;
                }
            }
            Some(out_msg) = rx.recv() => {
                if write.send(out_msg).await.is_err() {
                    eprintln!("[Bybit] Failed to send message");
                    status.bybit_connected.store(false, Ordering::Relaxed);
                    clear_bybit_prices(&store);
                    break;
                }
            }

            msg = read.next() => {
                let msg = match msg {
                    Some(m) => m,
                    None => {
                        eprintln!("[Bybit] Stream ended");
                        status.bybit_connected.store(false, Ordering::Relaxed);
                        clear_bybit_prices(&store);
                        break;
                    }
                };

                match msg {
                    Ok(Message::Text(text)) => {
                        last_msg_time = tokio::time::Instant::now();

                        if let Ok(ws_res) = serde_json::from_str::<WsResponse>(&text) {
                            if let Some(topic) = &ws_res.topic {
                                if topic.starts_with("orderbook.1.") {
                                    if let Some(data) = ws_res.data {
                                        if let Ok(ob) = serde_json::from_value::<OrderBookData>(data) {
                                            let key = normalize_symbol(&ob.s);
                                            if let Some(mut entry) = store.get_mut(&key) {
                                                if let Some(bid) = ob.b.first() {
                                                    if bid.len() >= 2 {
                                                        entry.bybit_book.best_bid = bid[0].parse().ok();
                                                        entry.bybit_book.best_bid_qty = bid[1].parse().ok();
                                                    }
                                                }
                                                if let Some(ask) = ob.a.first() {
                                                    if ask.len() >= 2 {
                                                        entry.bybit_book.best_ask = ask[0].parse().ok();
                                                        entry.bybit_book.best_ask_qty = ask[1].parse().ok();
                                                    }
                                                }
                                                entry.bybit_book_updated = Some(Instant::now());
                                                entry.bybit_book_epoch_ms = Some(Utc::now().timestamp_millis());
                                                status.bybit_updates.fetch_add(1, Ordering::Relaxed);
                                            }
                                        }
                                    }
                                } else if topic.starts_with("tickers.") {
                                    if let Some(data) = ws_res.data {
                                        if let Ok(ticker) = serde_json::from_value::<TickerData>(data) {
                                            let key = normalize_symbol(&ticker.symbol);
                                            let now = Instant::now();
                                            let mut entry = store.entry(key).or_default();

                                            if let Some(ref bp) = ticker.bid1Price {
                                                if let Ok(bid) = bp.parse::<f64>() {
                                                    if bid > 0.0 {
                                                        entry.bybit_book.best_bid = Some(bid);
                                                    }
                                                }
                                            }
                                            if let Some(ref bq) = ticker.bid1Size {
                                                if let Ok(qty) = bq.parse::<f64>() {
                                                    entry.bybit_book.best_bid_qty = Some(qty);
                                                }
                                            }
                                            if let Some(ref ap) = ticker.ask1Price {
                                                if let Ok(ask) = ap.parse::<f64>() {
                                                    if ask > 0.0 {
                                                        entry.bybit_book.best_ask = Some(ask);
                                                    }
                                                }
                                            }
                                            if let Some(ref aq) = ticker.ask1Size {
                                                if let Ok(qty) = aq.parse::<f64>() {
                                                    entry.bybit_book.best_ask_qty = Some(qty);
                                                }
                                            }

                                            entry.bybit_book_updated = Some(now);
                                            entry.bybit_book_epoch_ms = Some(Utc::now().timestamp_millis());

                                            // Reference mid price if both book sides exist, else fallback to lastPrice
                                            let price = match (entry.bybit_book.best_bid, entry.bybit_book.best_ask) {
                                                (Some(b), Some(a)) if b > 0.0 && a > 0.0 => (b + a) / 2.0,
                                                _ => ticker.lastPrice.as_deref().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0),
                                            };

                                            if price > 0.0 {
                                                entry.bybit = Some(price);
                                                entry.bybit_updated = Some(now);
                                                status.bybit_updates.fetch_add(1, Ordering::Relaxed);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        last_msg_time = tokio::time::Instant::now();
                        let _ = write.send(Message::Pong(data)).await;
                    }
                    Ok(Message::Close(_)) => {
                        eprintln!("[Bybit] Connection closed by server");
                        status.bybit_connected.store(false, Ordering::Relaxed);
                        clear_bybit_prices(&store);
                        break;
                    }
                    Err(e) => {
                        eprintln!("[Bybit] Error: {}", e);
                        status.bybit_connected.store(false, Ordering::Relaxed);
                        clear_bybit_prices(&store);
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
