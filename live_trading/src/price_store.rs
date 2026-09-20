use dashmap::DashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;

/// Shared store mapping coin symbol → minimum funding interval hours across exchanges.
pub type FundingStore = Arc<DashMap<String, u32>>;

pub fn new_funding_store() -> FundingStore {
    Arc::new(DashMap::new())
}

/// Best bid/ask snapshot from an exchange's order book.
#[derive(Debug, Clone, Default)]
pub struct OrderBookEntry {
    pub best_bid: Option<f64>,
    pub best_bid_qty: Option<f64>,
    pub best_ask: Option<f64>,
    pub best_ask_qty: Option<f64>,
}

/// Holds the latest price from each exchange for a single coin.
#[derive(Debug, Clone, Default)]
pub struct CoinPrices {
    pub binance: Option<f64>,
    pub bybit: Option<f64>,
    /// Per-exchange last-update timestamps
    pub binance_updated: Option<Instant>,
    pub bybit_updated: Option<Instant>,
    /// Per-exchange order book snapshots (best bid/ask + quantities)
    pub binance_book: OrderBookEntry,
    pub bybit_book: OrderBookEntry,
    /// When WebSocket last updated binance_book (used to prevent REST from overwriting fresh WS data)
    pub binance_book_updated: Option<Instant>,
    /// When WebSocket last updated bybit_book
    pub bybit_book_updated: Option<Instant>,
    /// Wall-clock epoch milliseconds (UTC) when binance_book was last updated by WS.
    /// Used for latency profiling — Instant cannot be serialized or compared across logs.
    pub binance_book_epoch_ms: Option<i64>,
    /// Wall-clock epoch milliseconds (UTC) when bybit_book was last updated by WS.
    pub bybit_book_epoch_ms: Option<i64>,
}

/// Shared concurrent price store.
pub type PriceStore = Arc<DashMap<String, CoinPrices>>;

/// Tracks connection health for each exchange.
pub struct ExchangeStatus {
    pub binance_updates: AtomicU64,
    pub bybit_updates: AtomicU64,
    pub binance_connected: std::sync::atomic::AtomicBool,
    pub bybit_connected: std::sync::atomic::AtomicBool,
    pub binance_enabled: std::sync::atomic::AtomicBool,
    pub bybit_enabled: std::sync::atomic::AtomicBool,
    /// Live trading toggle — when true, the engine will execute real trades
    pub trading_enabled: std::sync::atomic::AtomicBool,
    /// Session trade limit: max total trades to execute (0 = unlimited, default 1)
    pub trade_limit: std::sync::atomic::AtomicU32,
    /// Number of trades opened during this session
    pub session_trades_taken: std::sync::atomic::AtomicU32,
}

impl ExchangeStatus {
    pub fn new() -> Self {
        ExchangeStatus {
            binance_updates: AtomicU64::new(0),
            bybit_updates: AtomicU64::new(0),
            binance_connected: std::sync::atomic::AtomicBool::new(false),
            bybit_connected: std::sync::atomic::AtomicBool::new(false),
            binance_enabled: std::sync::atomic::AtomicBool::new(true),
            bybit_enabled: std::sync::atomic::AtomicBool::new(true),
            trading_enabled: std::sync::atomic::AtomicBool::new(false),
            trade_limit: std::sync::atomic::AtomicU32::new(1),
            session_trades_taken: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

pub type SharedStatus = Arc<ExchangeStatus>;

pub fn new_store() -> PriceStore {
    Arc::new(DashMap::new())
}

pub fn new_status() -> SharedStatus {
    Arc::new(ExchangeStatus::new())
}

/// Normalize a symbol string to a common key (strip USDT suffix).
pub fn normalize_symbol(raw: &str) -> String {
    let s = raw.to_uppercase();
    if let Some(base) = s.strip_suffix("USDT") {
        if base.is_empty() {
            "USDT".to_string()
        } else {
            base.to_string()
        }
    } else {
        s
    }
}

/// Returns None if price is older than MAX_PRICE_AGE_SECS — prevents stale prices from
/// a dead/disconnecting WebSocket from triggering real trades on fake spreads.
const MAX_PRICE_AGE_SECS: u64 = 2;

#[inline(always)]
fn fresh_price(price: Option<f64>, updated: Option<Instant>) -> Option<f64> {
    match (price, updated) {
        (Some(p), Some(ts)) if ts.elapsed().as_secs() <= MAX_PRICE_AGE_SECS => Some(p),
        _ => None, // Missing or stale — treat as unavailable
    }
}

/// Compute spread % directly from fresh prices.
#[inline]
pub fn compute_spread_from_fresh(p1: Option<f64>, p2: Option<f64>) -> Option<f64> {
    match (p1, p2) {
        (Some(a), Some(b)) if a > 0.0 && b > 0.0 => {
            let min = a.min(b);
            let max = a.max(b);
            let spread = ((max - min) / min) * 100.0;
            // Sanity guard: reject spreads > 100% (ticker collisions or corrupted data)
            if spread > 100.0 {
                None
            } else {
                Some(spread)
            }
        }
        _ => None,
    }
}

/// Return a view of prices where stale per-exchange values are replaced with None.
#[inline]
pub fn fresh_prices(prices: &CoinPrices) -> (Option<f64>, Option<f64>) {
    (
        fresh_price(prices.binance, prices.binance_updated),
        fresh_price(prices.bybit, prices.bybit_updated),
    )
}

/// Exchange identifier — only Binance and Bybit for live trading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Exchange {
    Binance,
    Bybit,
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Exchange::Binance => write!(f, "Binance"),
            Exchange::Bybit => write!(f, "Bybit"),
        }
    }
}

/// Get the order book entry for a specific exchange from CoinPrices.
pub fn get_order_book(prices: &CoinPrices, exchange: Exchange) -> &OrderBookEntry {
    match exchange {
        Exchange::Binance => &prices.binance_book,
        Exchange::Bybit => &prices.bybit_book,
    }
}

/// Get when the order book was last updated for a specific exchange.
#[allow(dead_code)]
pub fn get_order_book_updated(prices: &CoinPrices, exchange: Exchange) -> Option<Instant> {
    match exchange {
        Exchange::Binance => prices.binance_book_updated,
        Exchange::Bybit => prices.bybit_book_updated,
    }
}

/// Returns the order book snapshot if it exists and was updated within max_age_millis.
#[allow(dead_code)]
#[inline]
pub fn fresh_order_book(
    book: &OrderBookEntry,
    updated: Option<Instant>,
    max_age_millis: u128,
) -> Option<OrderBookEntry> {
    match updated {
        Some(ts) if ts.elapsed().as_millis() <= max_age_millis => {
            if book.best_bid.is_some() && book.best_ask.is_some() {
                Some(book.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Get the last price for a specific exchange from CoinPrices.
pub fn get_price(prices: &CoinPrices, exchange: Exchange) -> Option<f64> {
    match exchange {
        Exchange::Binance => prices.binance,
        Exchange::Bybit => prices.bybit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_fresh_order_book() {
        let book = OrderBookEntry {
            best_bid: Some(0.005680),
            best_bid_qty: Some(10000.0),
            best_ask: Some(0.005685),
            best_ask_qty: Some(15000.0),
        };

        // Fresh update
        let now = Instant::now();
        assert!(fresh_order_book(&book, Some(now), 2000).is_some());

        // Stale update (e.g. 3 seconds ago)
        let stale = now - Duration::from_millis(3000);
        assert!(fresh_order_book(&book, Some(stale), 2000).is_none());

        // Missing bid
        let incomplete = OrderBookEntry {
            best_bid: None,
            best_bid_qty: None,
            best_ask: Some(0.005685),
            best_ask_qty: Some(15000.0),
        };
        assert!(fresh_order_book(&incomplete, Some(now), 2000).is_none());
    }

    #[test]
    fn test_executable_book_spread() {
        // Buy Binance @ 0.005638, Sell Bybit @ 0.005698 -> +1.064%
        let bin_ask = 0.005638;
        let byb_bid = 0.005698;
        let spread = ((byb_bid - bin_ask) / bin_ask) * 100.0;
        assert!(spread > 1.0);

        // Real KAT case where Bybit bid was 0.005687 and Binance filled @ 0.005693 -> negative spread
        let real_buy = 0.005693;
        let real_sell = 0.005687;
        let real_spread = ((real_sell - real_buy) / real_buy) * 100.0;
        assert!(real_spread < 0.0);
    }
}
