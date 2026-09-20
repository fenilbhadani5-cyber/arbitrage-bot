use crate::latency::TradeLatency;
use crate::price_store::Exchange;
use serde::{Deserialize, Serialize};
use std::io::Write;

/// Whether this record represents an OPEN or CLOSE of an arbitrage position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeType {
    Open,
    Close,
}

impl std::fmt::Display for TradeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TradeType::Open => write!(f, "OPEN"),
            TradeType::Close => write!(f, "CLOSE"),
        }
    }
}

/// A single trade record with full execution details from REAL exchanges.
/// All prices, fees, and quantities come from actual exchange API responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub id: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub coin: String,
    pub exchange_buy: Exchange,
    pub exchange_sell: Exchange,

    /// Whether this is an OPEN or CLOSE trade.
    #[serde(default = "default_trade_type")]
    pub trade_type: TradeType,

    // ── Real exchange order IDs ──
    /// Order ID on the buy exchange
    pub buy_order_id: String,
    /// Order ID on the sell exchange
    pub sell_order_id: String,

    // ── Real fill data from exchange API ──
    /// Actual average fill price on buy side (from exchange)
    pub buy_fill_price: f64,
    /// Actual average fill price on sell side (from exchange)
    pub sell_fill_price: f64,
    /// Actual filled quantity in coin units (from exchange)
    pub buy_filled_qty: f64,
    pub sell_filled_qty: f64,
    /// Actual USDT value of the fills
    pub buy_quote_value: f64,
    pub sell_quote_value: f64,

    // ── Real fees from exchange API ──
    /// Actual commission charged by buy exchange
    pub buy_commission: f64,
    pub buy_commission_asset: String,
    /// Actual commission charged by sell exchange
    pub sell_commission: f64,
    pub sell_commission_asset: String,
    /// Total fees across both exchanges (buy_commission + sell_commission)
    pub total_fee: f64,

    // ── Spread data ──
    /// Spread % at the time of signal (before trade)
    pub spread_before: f64,
    /// Spread % after trade / at exit
    pub spread_after: f64,

    // ── PnL (only meaningful on CLOSE records) ──
    /// Gross PnL before fees
    pub pnl_gross: f64,
    /// Net PnL after all fees
    pub pnl_net: f64,
    /// Realized PnL reported by exchange (if available)
    pub exchange_realized_pnl: f64,

    // ── Order book snapshot at trade time ──
    pub buy_book_bid: f64,
    pub buy_book_ask: f64,
    pub buy_book_bid_qty: Option<f64>,
    pub buy_book_ask_qty: Option<f64>,
    pub sell_book_bid: f64,
    pub sell_book_ask: f64,
    pub sell_book_bid_qty: Option<f64>,
    pub sell_book_ask_qty: Option<f64>,

    // ── Close-specific fields (only present on CLOSE records) ──
    /// Price at which the close-leg buy was filled
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_buy_price: Option<f64>,
    /// Price at which the close-leg sell was filled
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_sell_price: Option<f64>,
    /// Commission on close-leg buy
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_buy_commission: Option<f64>,
    /// Commission on close-leg sell
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_sell_commission: Option<f64>,
    /// Close-leg order IDs
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_buy_order_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_sell_order_id: Option<String>,
    /// Total fees on open leg
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_open_fees: Option<f64>,
    /// Total fees on close leg
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_close_fees: Option<f64>,
    /// How long position was held (seconds)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_duration_secs: Option<i64>,
    /// Spread at entry
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_spread: Option<f64>,
    /// Spread at exit
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_spread: Option<f64>,

    // ── Funding info ──
    /// Funding interval hours for this coin
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub funding_interval_hours: Option<u32>,

    // ── Full latency profile (every pipeline stage timestamp + derived metrics) ──
    /// Complete latency breakdown from book update through WS fill.
    /// Contains: book_update_ms, opportunity_detected_ms, pre_flight_check_ms,
    /// order_send_ms, exchange_ack_ms, ws_fill_ms, detection_to_send_ms,
    /// send_to_ack_ms, ack_to_fill_ms, book_to_send_ms, total_pipeline_ms,
    /// detected/preflight quote snapshots, and latency_diagnosis string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency: Option<TradeLatency>,

    // ── Real account data from exchange (not calculated locally) ──
    /// Real entry price from exchange position API (buy side).
    /// Fetched from /fapi/v2/positionRisk (Binance) or /v5/position/list (Bybit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub real_account_entry_buy: Option<f64>,
    /// Real entry price from exchange position API (sell side).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub real_account_entry_sell: Option<f64>,
    /// Real unrealized PnL from exchange at time of open record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub real_unrealized_pnl: Option<f64>,
    /// Order fill status from exchange ("FILLED", "PARTIALLY_FILLED", etc.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buy_order_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sell_order_status: Option<String>,
}

fn default_trade_type() -> TradeType {
    TradeType::Close
}

/// Save a single trade record to the journal file (append as JSON line).
pub fn save_trade(record: &TradeRecord, path: &str) -> std::io::Result<()> {
    let json = serde_json::to_string(record)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    writeln!(file, "{}", json)?;
    Ok(())
}

/// Load all historical trades from the journal file.
pub fn load_trades(path: &str) -> Vec<TradeRecord> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str::<TradeRecord>(line).ok())
        .collect()
}

/// Aggregate statistics for the trade journal.
#[derive(Debug, Clone, Default)]
pub struct TradeSummary {
    pub total_trades: u64,
    pub winning_trades: u64,
    pub losing_trades: u64,
    pub win_rate: f64,
    pub total_pnl_gross: f64,
    pub total_pnl_net: f64,
    pub total_fees: f64,
    pub avg_spread_captured: f64,
    pub best_trade_pnl: f64,
    pub worst_trade_pnl: f64,
    pub closed_trades: u64,
    pub open_trades: u64,
    pub exchange_stats: std::collections::HashMap<Exchange, ExchangeTradeStats>,
}

/// Per-exchange trade statistics.
#[derive(Debug, Clone, Default)]
pub struct ExchangeTradeStats {
    pub trades_as_buy: u64,
    pub trades_as_sell: u64,
    pub total_fees_paid: f64,
    pub total_pnl_contribution: f64,
}

/// Compute aggregate summary from a list of trades.
pub fn compute_summary(trades: &[TradeRecord]) -> TradeSummary {
    let mut summary = TradeSummary::default();

    if trades.is_empty() {
        return summary;
    }

    summary.total_trades = trades.len() as u64;
    summary.best_trade_pnl = f64::NEG_INFINITY;
    summary.worst_trade_pnl = f64::INFINITY;

    let mut total_spread = 0.0f64;
    let mut closed_count = 0u64;

    for trade in trades {
        match trade.trade_type {
            TradeType::Open => {
                summary.open_trades += 1;
                summary.total_fees += trade.total_fee;
            }
            TradeType::Close => {
                if summary.open_trades > 0 {
                    summary.open_trades -= 1;
                }
                summary.closed_trades += 1;
                closed_count += 1;

                if trade.pnl_net > 0.0 {
                    summary.winning_trades += 1;
                } else {
                    summary.losing_trades += 1;
                }

                summary.total_pnl_gross += trade.pnl_gross;
                summary.total_pnl_net += trade.pnl_net;
                summary.total_fees += trade.total_fee;

                if let Some(entry_s) = trade.entry_spread {
                    total_spread += entry_s;
                } else {
                    total_spread += trade.spread_before;
                }

                if trade.pnl_net > summary.best_trade_pnl {
                    summary.best_trade_pnl = trade.pnl_net;
                }
                if trade.pnl_net < summary.worst_trade_pnl {
                    summary.worst_trade_pnl = trade.pnl_net;
                }
            }
        }

        // Per-exchange stats
        let buy_stats = summary
            .exchange_stats
            .entry(trade.exchange_buy)
            .or_default();
        buy_stats.trades_as_buy += 1;
        buy_stats.total_fees_paid += trade.buy_commission;
        if trade.trade_type == TradeType::Close {
            buy_stats.total_pnl_contribution += trade.pnl_net / 2.0;
        }

        let sell_stats = summary
            .exchange_stats
            .entry(trade.exchange_sell)
            .or_default();
        sell_stats.trades_as_sell += 1;
        sell_stats.total_fees_paid += trade.sell_commission;
        if trade.trade_type == TradeType::Close {
            sell_stats.total_pnl_contribution += trade.pnl_net / 2.0;
        }
    }

    if closed_count > 0 {
        summary.win_rate = (summary.winning_trades as f64 / closed_count as f64) * 100.0;
        summary.avg_spread_captured = total_spread / closed_count as f64;
    }

    if summary.best_trade_pnl == f64::NEG_INFINITY {
        summary.best_trade_pnl = 0.0;
    }
    if summary.worst_trade_pnl == f64::INFINITY {
        summary.worst_trade_pnl = 0.0;
    }

    summary
}
