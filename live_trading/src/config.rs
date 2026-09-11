/// Trading configuration constants for the live arbitrage bot.
/// Adjust these values to control risk and trade behavior.

/// Target trade size in USDT per leg (actual size may be reduced based on balance + leverage).
pub const TRADE_SIZE_USDT: f64 = 6.0;

/// Fixed leverage used on both exchanges for all trades.
/// This is intentionally hardcoded to avoid the 150–300ms set_leverage API call
/// on every cache miss. After the first trade on any symbol, leverage is cached
/// and never re-sent to the exchange.
pub const FIXED_LEVERAGE: u32 = 10;

/// Minimum USDT balance required on each exchange to allow trading.
/// If either exchange is below this threshold, trades are skipped entirely.
pub const MIN_BALANCE_USDT: f64 = 1.0;

/// Minimum spread % to OPEN a new arbitrage position.
pub const ENTRY_SPREAD_THRESHOLD: f64 = 1.0;

/// Maximum sanity spread % to OPEN a new arbitrage position.
/// Any spread > 10% is guaranteed to be a ticker collision (e.g. stock vs crypto)
/// or delisting anomaly — never genuine live arbitrage.
pub const MAX_SPREAD_THRESHOLD: f64 = 10.0;

/// Maximum allowed orderbook age in milliseconds before a quote is considered stale.
/// 2000ms ensures quotes are fresh and prevents executing on dormant/stale quotes.
pub const MAX_BOOK_AGE_MILLIS: u128 = 2000;

/// Maximum acceptable slippage % between quoted orderbook price and executed fill price.
pub const MAX_ALLOWED_SLIPPAGE_PCT: f64 = 0.35;

/// Spread % at which to CLOSE an open position (spread has converged).
pub const EXIT_SPREAD_THRESHOLD: f64 = 0.1;

/// Cooldown between opening trades on the same coin (seconds).
pub const TRADE_COOLDOWN_SECS: i64 = 5;



/// Maximum seconds to hold a position before force-closing.
/// Arbitrage spreads converge in 1-5 seconds. Holding longer exposes to directional risk.
pub const MAX_HOLD_SECS: i64 = 10;

/// Maximum number of concurrent open arbitrage positions.
/// Set to 1 for safe real-money testing — only 1 position open at a time.
pub const MAX_OPEN_POSITIONS: usize = 3;

/// Minutes before/after funding time to PAUSE trading on a coin.
/// Coins near their funding timestamp are extremely volatile.
pub const FUNDING_PAUSE_MINUTES: i64 = 5;

/// Stop trading completely on coins with 1-hour funding intervals because they are too volatile.
/// Set to true to skip any coin with a 1h funding interval.
pub const SKIP_1H_FUNDING_COINS: bool = true;

/// Taker fee rates per exchange (VIP-0, market order).
pub fn taker_fee(exchange: crate::price_store::Exchange) -> f64 {
    match exchange {
        crate::price_store::Exchange::Binance => 0.0005,  // 0.05%
        crate::price_store::Exchange::Bybit   => 0.00055, // 0.055%
    }
}

/// Path for the live trade journal file.
pub const LIVE_TRADES_LOG_PATH: &str = "live_trades.jsonl";

/// Path for the missed trades log file (human-readable text log).
pub const MISSED_TRADES_LOG_PATH: &str = "missed_trades.log";

/// Path for the missed trades JSONL file (machine-readable structured JSON lines).
pub const MISSED_TRADES_JSONL_PATH: &str = "missed_trades.jsonl";
