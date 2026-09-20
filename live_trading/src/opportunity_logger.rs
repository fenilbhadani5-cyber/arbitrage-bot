use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Mutex;
use std::time::Instant;

use crate::rejection::RejectionReason;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunityRecord {
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub direction: String, // "Binance->Bybit" or "Bybit->Binance"
    pub raw_bid: f64,
    pub raw_ask: f64,
    pub effective_buy_price: f64,
    pub effective_sell_price: f64,
    pub gross_spread_pct: f64,
    pub effective_spread_pct: f64,
    pub baseline_spread_pct: f64,
    pub spread_volatility_pct: f64,
    pub dynamic_entry_threshold_pct: f64,
    pub dynamic_exit_threshold_pct: f64,
    pub z_score: f64,
    pub spread_velocity: f64,
    pub expected_slippage_pct: f64,
    pub buy_fee_pct: f64,
    pub sell_fee_pct: f64,
    pub net_edge_pct: f64,
    pub signal_age_ms: u64,
    pub liquidity_ok: bool,
    pub data_fresh_ok: bool,
    pub entry_decision: bool,
    pub reject_reason: Option<RejectionReason>,
    pub exit_trigger_condition: Option<String>,
}

static OPP_THROTTLE: Mutex<Option<HashMap<String, (Option<RejectionReason>, Instant)>>> =
    Mutex::new(None);
const THROTTLE_SECS: u64 = 1;

lazy_static! {
    static ref LOG_FILE: Mutex<std::fs::File> = Mutex::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open("opportunities.jsonl")
            .expect("Failed to open opportunities.jsonl")
    );
}

pub fn log_opportunity(record: &OpportunityRecord) {
    // Throttle duplicates (same coin, same reason)
    {
        let mut guard = match OPP_THROTTLE.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let map = guard.get_or_insert_with(HashMap::new);
        let now = Instant::now();
        let key = format!("{}_{}", record.symbol, record.direction);

        if let Some((prev_reason, last_time)) = map.get(&key) {
            if prev_reason == &record.reject_reason
                && now.duration_since(*last_time).as_secs() < THROTTLE_SECS
            {
                return;
            }
        }
        map.insert(key, (record.reject_reason.clone(), now));
    }

    if let Ok(json) = serde_json::to_string(record) {
        if let Ok(mut file) = LOG_FILE.lock() {
            let _ = writeln!(file, "{}", json);
        }
    }
}
