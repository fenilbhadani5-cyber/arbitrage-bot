use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::oneshot;

/// Fill data pushed from the private WS to the waiting order function.
#[derive(Debug, Clone)]
pub struct FillEvent {
    pub order_id:    String,
    pub avg_price:   f64,
    pub filled_qty:  f64,
    pub quote_qty:   f64,
    pub commission:  f64,
    pub timestamp:   u64,
    /// True when the order expired/was canceled with 0 fill.
    /// Allows execute_order_with_fill to fast-fail in ~20ms instead of hitting the 500ms WS timeout.
    pub is_expired:  bool,
}

/// Registry of pending orders waiting for a fill event.
/// Key = order_id (String), Value = oneshot sender that delivers the FillEvent.
/// Inserted when an order is placed; removed (and fired) when the WS receives the fill.
pub type PendingFillMap = Arc<DashMap<String, oneshot::Sender<FillEvent>>>;

/// Create a new empty pending fill map.
pub fn new_pending_fill_map() -> PendingFillMap {
    Arc::new(DashMap::new())
}

/// Live USDT balance updated in real-time by the private WS ACCOUNT_UPDATE / wallet event.
/// Uses an RwLock so the trading engine can read it without blocking.
pub type LiveBalance = Arc<tokio::sync::RwLock<f64>>;

/// Create a new live balance cell, initially 0.0.
pub fn new_live_balance() -> LiveBalance {
    Arc::new(tokio::sync::RwLock::new(0.0))
}
