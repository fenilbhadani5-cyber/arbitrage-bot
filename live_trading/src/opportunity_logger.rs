//! Opportunity Logger
//!
//! Opportunity logging is disabled entirely.
//! Actual trades are saved in `live_trades.jsonl` and missed trades in `missed_trades.jsonl` / `missed_trades.log`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::rejection::RejectionReason;

/// Global toggle to enable/disable opportunity logging.
pub const ENABLE_OPPORTUNITY_LOGGING: bool = false;

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

/// Disables opportunity logging entirely. Does not open or write to opportunities.jsonl.
#[inline(always)]
#[allow(unused_variables)]
pub fn log_opportunity(record: &OpportunityRecord) {
    // Disabled entirely: no-op, avoiding file I/O and mutex contention
}

