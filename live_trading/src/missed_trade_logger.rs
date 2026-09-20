use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Mutex;
use std::time::Instant;

use crate::config::{MISSED_TRADES_JSONL_PATH, MISSED_TRADES_LOG_PATH};
use crate::price_store::Exchange;

/// Record of an arbitrage opportunity where the spread exceeded the entry threshold,
/// but a live trade was NOT executed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissedTradeRecord {
    pub timestamp: DateTime<Utc>,
    pub coin: String,
    pub spread_pct: f64,
    pub threshold_pct: f64,
    pub buy_exchange: Exchange,
    pub sell_exchange: Exchange,
    pub buy_price: f64,
    pub sell_price: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub book_spread_pct: Option<f64>,
    pub reason: String,
    pub binance_balance: f64,
    pub bybit_balance: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency: Option<crate::latency::TradeLatency>,
}

/// Cooldown tracker to prevent writing duplicate logs every 100ms
/// for the same coin and same reason category.
static THROTTLE_MAP: Mutex<Option<HashMap<String, (String, Instant)>>> = Mutex::new(None);

/// Minimum seconds between logging the same coin for the same reason category.
const THROTTLE_SECS: u64 = 5;

lazy_static! {
    static ref LOG_FILE_TXT: Mutex<std::fs::File> = Mutex::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(MISSED_TRADES_LOG_PATH)
            .unwrap_or_else(|_| {
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("missed_trades.log")
                    .expect("Failed to open missed_trades.log")
            })
    );

    static ref LOG_FILE_JSON: Mutex<std::fs::File> = Mutex::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(MISSED_TRADES_JSONL_PATH)
            .unwrap_or_else(|_| {
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("missed_trades.jsonl")
                    .expect("Failed to open missed_trades.jsonl")
            })
    );
}

/// Extract reason category (e.g. "COOLDOWN" from "COOLDOWN: remaining 15s")
fn reason_category(reason: &str) -> &str {
    reason.split([':', '(']).next().unwrap_or(reason).trim()
}

/// Returns true if this record should be logged (not throttled).
fn should_log(coin: &str, reason: &str) -> bool {
    let cat = reason_category(reason);

    // Never throttle execution-level failures (orders that were actually sent to exchanges)
    if cat == "LEG_FILL_FAILED" || cat == "BOTH_LEGS_FAILED" || cat == "ONE_LEG_FAILED" {
        return true;
    }

    let mut guard = match THROTTLE_MAP.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let map = guard.get_or_insert_with(HashMap::new);
    let now = Instant::now();

    if let Some((prev_cat, last_time)) = map.get(coin) {
        if prev_cat == cat && now.duration_since(*last_time).as_secs() < THROTTLE_SECS {
            return false;
        }
    }

    map.insert(coin.to_string(), (cat.to_string(), now));
    true
}

/// Log a missed trade to both the human-readable text log and the structured JSONL log.
pub fn log_missed_trade(record: &MissedTradeRecord) {
    if !should_log(&record.coin, &record.reason) {
        return;
    }

    let time_str = record.timestamp.format("%Y-%m-%d %H:%M:%S UTC").to_string();
    let book_str = match record.book_spread_pct {
        Some(bs) => format!("{:.3}%", bs),
        None => "N/A".to_string(),
    };

    let latency_str = if let Some(ref lat) = record.latency {
        let mut parts = Vec::new();
        if let Some(ack) = lat.send_to_ack_ms {
            let buy_str = lat
                .buy_leg_rtt_ms
                .map(|ms| format!("{}ms", ms))
                .unwrap_or_else(|| "?".to_string());
            let sell_str = lat
                .sell_leg_rtt_ms
                .map(|ms| format!("{}ms", ms))
                .unwrap_or_else(|| "?".to_string());
            parts.push(format!(
                "RTT: send→ack={}ms (BuyLeg: {}, SellLeg: {})",
                ack, buy_str, sell_str
            ));
        } else if lat.buy_leg_rtt_ms.is_some() || lat.sell_leg_rtt_ms.is_some() {
            let buy_str = lat
                .buy_leg_rtt_ms
                .map(|ms| format!("{}ms", ms))
                .unwrap_or_else(|| "?".to_string());
            let sell_str = lat
                .sell_leg_rtt_ms
                .map(|ms| format!("{}ms", ms))
                .unwrap_or_else(|| "?".to_string());
            parts.push(format!("RTT: (BuyLeg: {}, SellLeg: {})", buy_str, sell_str));
        }
        if let Some(rev) = lat.reversal_rtt_ms {
            parts.push(format!("Reversal: {}ms", rev));
        }
        if let Some(b2s) = lat.book_to_send_ms {
            parts.push(format!("book→send: {}ms", b2s));
        }
        if let Some(d2s) = lat.detection_to_send_ms {
            parts.push(format!("det→send: {}ms", d2s));
        }
        if let Some(total) = lat.total_pipeline_ms {
            parts.push(format!("total: {}ms", total));
        }
        if let Some(ref diag) = lat.latency_diagnosis {
            parts.push(format!("diag: {}", diag));
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!(" | LATENCY: {}", parts.join(" | "))
        }
    } else {
        String::new()
    };

    // 1. Human-readable text log format
    let text_line = format!(
        "[{}] MISSED | COIN: {:<8} | Spread: {:>6.3}% (Min: {:.2}%) | BUY: {:<7} @ {:<10.6} | SELL: {:<7} @ {:<10.6} | BookSpread: {:>6} | REASON: {} | Balances: Binance=${:.2}, Bybit=${:.2}{}\n",
        time_str,
        record.coin,
        record.spread_pct,
        record.threshold_pct,
        record.buy_exchange.to_string(),
        record.buy_price,
        record.sell_exchange.to_string(),
        record.sell_price,
        book_str,
        record.reason,
        record.binance_balance,
        record.bybit_balance,
        latency_str,
    );

    // Open human-readable log (with local fallback if absolute path fails)
    if let Ok(mut file) = LOG_FILE_TXT.lock() {
        let _ = file.write_all(text_line.as_bytes());
    }

    // 2. Machine-readable JSONL format
    if let Ok(json) = serde_json::to_string(record) {
        if let Ok(mut file) = LOG_FILE_JSON.lock() {
            let _ = writeln!(file, "{}", json);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reason_category() {
        assert_eq!(reason_category("COOLDOWN: 15s remaining"), "COOLDOWN");
        assert_eq!(
            reason_category("MAX_POSITIONS_REACHED (1/1 active)"),
            "MAX_POSITIONS_REACHED"
        );
        assert_eq!(
            reason_category("INSUFFICIENT_BALANCE"),
            "INSUFFICIENT_BALANCE"
        );
        assert_eq!(
            reason_category("LEG_FILL_FAILED: Sell on Bybit failed"),
            "LEG_FILL_FAILED"
        );
    }

    #[test]
    fn test_record_json_serialization() {
        let mut record = MissedTradeRecord {
            timestamp: Utc::now(),
            coin: "FORM".to_string(),
            spread_pct: 1.45,
            threshold_pct: 1.3,
            buy_exchange: Exchange::Binance,
            sell_exchange: Exchange::Bybit,
            buy_price: 0.3059,
            sell_price: 0.3103,
            book_spread_pct: Some(1.438),
            reason: "MAX_POSITIONS_REACHED: Currently 1/1 open positions".to_string(),
            binance_balance: 15.2,
            bybit_balance: 14.8,
            latency: None,
        };

        let json = serde_json::to_string(&record).expect("must serialize");
        assert!(json.contains("\"coin\":\"FORM\""));
        assert!(json.contains("\"spread_pct\":1.45"));
        assert!(json.contains("MAX_POSITIONS_REACHED"));
        assert!(!json.contains("\"latency\""));

        // With latency populated
        let mut lat = crate::latency::TradeLatency::new(Some(1000));
        lat.order_send_ms = Some(1010);
        lat.exchange_ack_ms = Some(1250);
        lat.buy_leg_rtt_ms = Some(65);
        lat.sell_leg_rtt_ms = Some(240);
        lat.compute_derived();
        record.latency = Some(lat);

        let json_with_lat = serde_json::to_string(&record).expect("must serialize");
        assert!(json_with_lat.contains("\"latency\""));
        assert!(json_with_lat.contains("\"buy_leg_rtt_ms\":65"));
        assert!(json_with_lat.contains("\"sell_leg_rtt_ms\":240"));
        assert!(json_with_lat.contains("\"send_to_ack_ms\":240"));
    }
}
