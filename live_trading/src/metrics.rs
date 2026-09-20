use dashmap::DashMap;
use lazy_static::lazy_static;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default, Debug)]
pub struct Counters {
    pub entry_count: AtomicU64,
    pub exit_count: AtomicU64,
    pub partial_fill_count: AtomicU64,
    pub leg_failure_count: AtomicU64,
}

#[derive(Default, Debug)]
pub struct RejectionCounters {
    pub counts: DashMap<String, AtomicU64>,
}

#[derive(Default, Debug)]
pub struct SymbolGauges {
    // Values stored as bits (f64::to_bits) for atomic f64
    pub dynamic_entry_threshold: AtomicU64,
    pub dynamic_exit_threshold: AtomicU64,
    pub gross_spread: AtomicU64,
    pub effective_spread: AtomicU64,
    pub net_edge: AtomicU64,
    pub z_score: AtomicU64,
    pub signal_age_ms: AtomicU64,
}

lazy_static! {
    pub static ref METRICS_COUNTERS: Counters = Counters::default();
    pub static ref METRICS_REJECTIONS: RejectionCounters = RejectionCounters::default();
    pub static ref METRICS_GAUGES: DashMap<String, SymbolGauges> = DashMap::new();
}

pub fn increment_rejection(reason: &str) {
    METRICS_REJECTIONS
        .counts
        .entry(reason.to_string())
        .or_insert_with(|| AtomicU64::new(0))
        .fetch_add(1, Ordering::Relaxed);
}

pub fn set_gauge(symbol: &str, field: fn(&SymbolGauges) -> &AtomicU64, value: f64) {
    let gauges = METRICS_GAUGES.entry(symbol.to_string()).or_default();
    field(&gauges).store(value.to_bits(), Ordering::Relaxed);
}

pub fn get_gauge(symbol: &str, field: fn(&SymbolGauges) -> &AtomicU64) -> Option<f64> {
    METRICS_GAUGES
        .get(symbol)
        .map(|g| f64::from_bits(field(&g).load(Ordering::Relaxed)))
}
