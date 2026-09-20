/// Startup position synchronization from real exchange accounts.
///
/// On startup, fetches all open positions from Binance and Bybit via REST,
/// and reconstructs OpenPosition structs so the bot knows about any positions
/// that were open when it was restarted.
///
/// This replaces the local JSONL reconstruction which used stale cached data.
use crate::exchanges::binance_api::BinanceClient;
use crate::exchanges::bybit_api::BybitClient;
use crate::live_trading::OpenPosition;
use crate::price_store::Exchange;
use chrono::Utc;

/// Fetch and reconcile open positions from both exchanges at startup.
/// Returns a list of `OpenPosition` structs for any hedged pairs found
/// (one LONG on one exchange, SHORT on the same symbol on the other).
pub async fn sync_open_positions(
    binance: &BinanceClient,
    bybit: &BybitClient,
) -> Vec<OpenPosition> {
    let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
    eprintln!(
        "[{}][PositionSync] Fetching open positions from both exchanges...",
        ts
    );

    let (bin_result, byb_result) = tokio::join!(binance.get_positions(), bybit.get_positions(),);

    let bin_positions = match bin_result {
        Ok(p) => {
            eprintln!(
                "[{}][PositionSync] Binance: {} open position(s)",
                Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                p.len()
            );
            p
        }
        Err(e) => {
            eprintln!(
                "[{}][PositionSync] WARNING: Failed to fetch Binance positions: {}",
                Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                e
            );
            vec![]
        }
    };

    let byb_positions = match byb_result {
        Ok(p) => {
            eprintln!(
                "[{}][PositionSync] Bybit: {} open position(s)",
                Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                p.len()
            );
            p
        }
        Err(e) => {
            eprintln!(
                "[{}][PositionSync] WARNING: Failed to fetch Bybit positions: {}",
                Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                e
            );
            vec![]
        }
    };

    let mut synced: Vec<OpenPosition> = Vec::new();

    // Match Binance LONG positions with Bybit SHORT positions (and vice versa)
    use std::collections::HashMap;

    // Build map: symbol -> (side, qty, entry_price) for each exchange
    // Binance position: positionAmt > 0 = LONG, < 0 = SHORT
    let mut bin_map: HashMap<String, (f64, f64)> = HashMap::new(); // symbol -> (positionAmt, entryPrice)
    for pos in &bin_positions {
        let amt: f64 = pos.positionAmt.parse().unwrap_or(0.0);
        let entry: f64 = pos.entryPrice.parse().unwrap_or(0.0);
        if amt.abs() > 0.0 && entry > 0.0 {
            bin_map.insert(pos.symbol.clone(), (amt, entry));
        }
    }

    let mut byb_map: HashMap<String, (f64, f64, String)> = HashMap::new(); // symbol -> (size_signed, entry_price, side)
    for pos in &byb_positions {
        let size: f64 = pos.size.parse().unwrap_or(0.0);
        let entry: f64 = pos.avgPrice.parse().unwrap_or(0.0);
        if size > 0.0 && entry > 0.0 {
            let signed = if pos.side == "Sell" { -size } else { size };
            byb_map.insert(pos.symbol.clone(), (signed, entry, pos.side.clone()));
        }
    }

    // Find hedged pairs: Binance LONG + Bybit SHORT on same symbol
    for (symbol, (bin_amt, bin_entry)) in &bin_map {
        if let Some((byb_amt, byb_entry, byb_side)) = byb_map.get(symbol) {
            // Hedged: Binance LONG, Bybit SHORT
            if *bin_amt > 0.0 && *byb_amt < 0.0 {
                let coin = symbol.trim_end_matches("USDT").to_string();
                let qty = bin_amt.min(byb_amt.abs());
                eprintln!(
                    "[{}][PositionSync] Found hedged pair: {} | BUY Binance @ {:.6} (qty={:.4}) | SELL Bybit @ {:.6} (qty={:.4})",
                    Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                    coin, bin_entry, bin_amt, byb_entry, byb_amt.abs()
                );
                synced.push(OpenPosition {
                    coin: coin.clone(),
                    buy_exchange: Exchange::Binance,
                    sell_exchange: Exchange::Bybit,
                    entry_buy_price: *bin_entry,
                    entry_sell_price: *byb_entry,
                    buy_filled_qty: qty,
                    sell_filled_qty: qty,
                    buy_quote_value: qty * bin_entry,
                    sell_quote_value: qty * byb_entry,
                    entry_buy_commission: 0.0, // not recoverable from position API
                    entry_sell_commission: 0.0,
                    buy_order_id: format!("sync_bin_{}", symbol),
                    sell_order_id: format!("sync_byb_{}", symbol),
                    entry_spread: if *bin_entry > 0.0 {
                        ((byb_entry - bin_entry) / bin_entry) * 100.0
                    } else {
                        0.0
                    },
                    open_time: Utc::now(), // unknown — use now as conservative estimate
                    entry_buy_book_bid: 0.0,
                    entry_buy_book_ask: 0.0,
                    entry_buy_book_bid_qty: None,
                    entry_buy_book_ask_qty: None,
                    entry_sell_book_bid: 0.0,
                    entry_sell_book_ask: 0.0,
                    entry_sell_book_bid_qty: None,
                    entry_sell_book_ask_qty: None,
                    funding_hours: 8,
                });
            }
            // Hedged: Binance SHORT, Bybit LONG
            else if *bin_amt < 0.0 && *byb_amt > 0.0 {
                let coin = symbol.trim_end_matches("USDT").to_string();
                let qty = bin_amt.abs().min(*byb_amt);
                eprintln!(
                    "[{}][PositionSync] Found hedged pair: {} | BUY Bybit @ {:.6} (qty={:.4}) | SELL Binance @ {:.6} (qty={:.4})",
                    Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                    coin, byb_entry, byb_amt, bin_entry, bin_amt.abs()
                );
                synced.push(OpenPosition {
                    coin: coin.clone(),
                    buy_exchange: Exchange::Bybit,
                    sell_exchange: Exchange::Binance,
                    entry_buy_price: *byb_entry,
                    entry_sell_price: *bin_entry,
                    buy_filled_qty: qty,
                    sell_filled_qty: qty,
                    buy_quote_value: qty * byb_entry,
                    sell_quote_value: qty * bin_entry,
                    entry_buy_commission: 0.0,
                    entry_sell_commission: 0.0,
                    buy_order_id: format!("sync_byb_{}", symbol),
                    sell_order_id: format!("sync_bin_{}", symbol),
                    entry_spread: if *byb_entry > 0.0 {
                        ((bin_entry - byb_entry) / byb_entry) * 100.0
                    } else {
                        0.0
                    },
                    open_time: Utc::now(),
                    entry_buy_book_bid: 0.0,
                    entry_buy_book_ask: 0.0,
                    entry_buy_book_bid_qty: None,
                    entry_buy_book_ask_qty: None,
                    entry_sell_book_bid: 0.0,
                    entry_sell_book_ask: 0.0,
                    entry_sell_book_bid_qty: None,
                    entry_sell_book_ask_qty: None,
                    funding_hours: 8,
                });
            }
            // Unhedged positions — log as warning but don't add
            else {
                let _ = byb_side; // suppress unused warning
                eprintln!(
                    "[{}][PositionSync] WARNING: Unhedged position on {}: Binance={:.4}, Bybit={:.4} — CLOSE MANUALLY",
                    Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                    symbol, bin_amt, byb_amt
                );
            }
        } else {
            // Only on Binance, not Bybit
            eprintln!(
                "[{}][PositionSync] WARNING: {} only on Binance (amt={:.4}, entry={:.6}) with no Bybit counterpart — CLOSE MANUALLY",
                Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                symbol, bin_amt, bin_entry
            );
        }
    }

    // Also check for Bybit-only positions (no Binance counterpart)
    for (symbol, (byb_amt, byb_entry, _)) in &byb_map {
        if !bin_map.contains_key(symbol) {
            eprintln!(
                "[{}][PositionSync] WARNING: {} only on Bybit (amt={:.4}, entry={:.6}) with no Binance counterpart — CLOSE MANUALLY",
                Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                symbol, byb_amt, byb_entry
            );
        }
    }

    eprintln!(
        "[{}][PositionSync] Sync complete: {} hedged position(s) recovered",
        Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        synced.len()
    );

    synced
}
