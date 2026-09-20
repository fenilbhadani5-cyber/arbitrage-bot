use crate::config::*;
use crate::exchanges::binance_api::BinanceClient;
use crate::exchanges::bybit_api::BybitClient;
use crate::exchanges::exchange_info::ExchangeInfoCache;
use crate::funding;
use crate::latency::TradeLatency;
use crate::price_store::{
    get_order_book, get_price, Exchange, FundingStore, OrderBookEntry, PriceStore,
};
use crate::trade_journal::{save_trade, TradeRecord, TradeType};
use chrono::Utc;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::Mutex;

/// An open arbitrage position: bought on one exchange, sold on another,
/// waiting for spread to converge so we can close profitably.
#[derive(Debug, Clone)]
pub struct OpenPosition {
    #[allow(dead_code)]
    pub coin: String,
    /// Exchange where we BOUGHT (the cheap side at entry)
    pub buy_exchange: Exchange,
    /// Exchange where we SOLD (the expensive side at entry)
    pub sell_exchange: Exchange,
    /// Actual fill price from exchange API (buy side)
    pub entry_buy_price: f64,
    /// Actual fill price from exchange API (sell side)
    pub entry_sell_price: f64,
    /// Actual filled quantity from exchange (buy side)
    pub buy_filled_qty: f64,
    /// Actual filled quantity from exchange (sell side)
    pub sell_filled_qty: f64,
    /// USDT value of trades
    pub buy_quote_value: f64,
    pub sell_quote_value: f64,
    /// Real commission from exchange
    pub entry_buy_commission: f64,
    pub entry_sell_commission: f64,
    /// Exchange order IDs
    pub buy_order_id: String,
    pub sell_order_id: String,
    /// Spread % at the time of entry signal
    pub entry_spread: f64,
    /// When the position was opened
    pub open_time: chrono::DateTime<Utc>,
    /// Order book snapshot at entry (buy side)
    pub entry_buy_book_bid: f64,
    pub entry_buy_book_ask: f64,
    pub entry_buy_book_bid_qty: Option<f64>,
    pub entry_buy_book_ask_qty: Option<f64>,
    /// Order book snapshot at entry (sell side)
    pub entry_sell_book_bid: f64,
    pub entry_sell_book_ask: f64,
    pub entry_sell_book_bid_qty: Option<f64>,
    pub entry_sell_book_ask_qty: Option<f64>,
    /// Funding interval for this coin
    pub funding_hours: u32,
}

impl OpenPosition {
    /// Compute unrealized PnL at current prices.
    #[inline]
    pub fn unrealized_pnl(&self, current_buy_price: f64, current_sell_price: f64) -> f64 {
        let long_pnl = (current_buy_price - self.entry_buy_price) * self.buy_filled_qty;
        let short_pnl = (self.entry_sell_price - current_sell_price) * self.sell_filled_qty;
        let entry_fees = self.entry_buy_commission + self.entry_sell_commission;
        long_pnl + short_pnl - entry_fees
    }
}

/// The core live trading engine with real exchange execution.
pub struct LiveTradingEngine {
    pub binance_client: BinanceClient,
    pub bybit_client: BybitClient,
    pub trade_count: u64,
    pub total_pnl: f64,
    /// Real balances from exchanges
    pub binance_balance: f64,
    pub bybit_balance: f64,
    /// Total real fees paid
    pub total_fees_paid: f64,
    /// Cooldown tracker: coin -> last trade timestamp
    pub last_trade_time: HashMap<String, chrono::DateTime<Utc>>,
    /// Currently open arbitrage positions (keyed by coin)
    pub open_positions: HashMap<String, OpenPosition>,
    /// Trade history (in-memory, also persisted to file) — VecDeque for O(1) pop_front
    pub recent_trades: VecDeque<TradeRecord>,
    /// Max recent trades to keep in memory
    pub max_recent: usize,
    /// Trade log file path
    pub log_path: String,
    /// Count of closed (completed) round-trip trades
    pub closed_count: u64,
    pub winning_trades: u64,
    pub losing_trades: u64,
    /// Cache of last-set leverage per symbol: symbol -> (binance_lev, bybit_lev)
    /// Skips redundant set_leverage() API calls (saves 150-300ms per trade)
    pub leverage_cache: HashMap<String, (u32, u32)>,
    /// Emergency halt flag: set to true when a one-leg open fails AND the reversal also fails,
    /// leaving unknown exchange exposure. Blocks all new opens until manually cleared.
    /// Reset by pressing 'R' in the terminal UI.
    pub emergency_halt: bool,
    /// Exchange symbol metadata cache (tick sizes, step sizes, min notional).
    pub exchange_info: ExchangeInfoCache,
}

impl LiveTradingEngine {
    pub fn new(
        binance_client: BinanceClient,
        bybit_client: BybitClient,
        log_path: String,
        exchange_info: ExchangeInfoCache,
    ) -> Self {
        LiveTradingEngine {
            binance_client,
            bybit_client,
            trade_count: 0,
            total_pnl: 0.0,
            binance_balance: 0.0,
            bybit_balance: 0.0,
            total_fees_paid: 0.0,
            last_trade_time: HashMap::new(),
            open_positions: HashMap::new(),
            recent_trades: VecDeque::new(),
            max_recent: 100,
            log_path,
            closed_count: 0,
            winning_trades: 0,
            losing_trades: 0,
            leverage_cache: HashMap::new(),
            emergency_halt: false,
            exchange_info,
        }
    }

    /// Pings the REST APIs to keep the HTTP connection pool warm.
    /// Eliminates the TCP/TLS handshake latency for the next trade.
    pub async fn ping_keepalives(&self) {
        let bin_fut = self.binance_client.ping_keepalive();
        let byb_fut = self.bybit_client.ping_keepalive();
        tokio::join!(bin_fut, byb_fut);
    }

    /// Helper to safely round quantities based on price magnitude to avoid exchange lot size errors.
    /// Falls back to heuristic if exchange metadata is not available.
    pub fn round_quantity(quantity: f64, price: f64) -> f64 {
        if price < 0.2 {
            quantity.trunc() // Integer quantities for very cheap coins
        } else if price < 2.0 {
            (quantity * 10.0).trunc() / 10.0 // 1 decimal place
        } else if price < 50.0 {
            (quantity * 100.0).trunc() / 100.0 // 2 decimal places
        } else if price < 1000.0 {
            (quantity * 1000.0).trunc() / 1000.0 // 3 decimal places
        } else {
            (quantity * 10000.0).trunc() / 10000.0 // 4 decimal places for BTC/ETH
        }
    }

    /// Round quantity using exchange-specific step size if available, else heuristic.
    pub async fn round_quantity_exact(
        &self,
        symbol: &str,
        exchange: Exchange,
        quantity: f64,
        price: f64,
    ) -> f64 {
        let info = match exchange {
            Exchange::Binance => self.exchange_info.get_binance(symbol).await,
            Exchange::Bybit => self.exchange_info.get_bybit(symbol).await,
        };
        match info {
            Some(si) => ExchangeInfoCache::round_qty_down(quantity, si.step_size),
            None => Self::round_quantity(quantity, price),
        }
    }

    /// Round a limit price to the exchange's tick size.
    /// For BUY: round UP (we're willing to pay up to this price).
    /// For SELL: round DOWN (we're willing to sell at least at this price).
    pub async fn round_price(
        &self,
        symbol: &str,
        exchange: Exchange,
        price: f64,
        is_buy: bool,
    ) -> f64 {
        let info = match exchange {
            Exchange::Binance => self.exchange_info.get_binance(symbol).await,
            Exchange::Bybit => self.exchange_info.get_bybit(symbol).await,
        };
        match info {
            Some(si) => {
                if is_buy {
                    ExchangeInfoCache::round_price_up(price, si.tick_size)
                } else {
                    ExchangeInfoCache::round_price_down(price, si.tick_size)
                }
            }
            None => price, // No rounding if no metadata
        }
    }

    /// Get min notional for a symbol on a given exchange.
    pub async fn get_min_notional(&self, symbol: &str, exchange: Exchange) -> f64 {
        let info = match exchange {
            Exchange::Binance => self.exchange_info.get_binance(symbol).await,
            Exchange::Bybit => self.exchange_info.get_bybit(symbol).await,
        };
        info.map(|si| si.min_notional).unwrap_or(5.0)
    }

    /// Get count of open positions.
    #[inline]
    pub fn open_position_count(&self) -> usize {
        self.open_positions.len()
    }

    /// Compute total unrealized PnL across all open positions.
    pub fn total_unrealized_pnl(&self, store: &PriceStore) -> f64 {
        let mut total = 0.0;
        for (coin, pos) in &self.open_positions {
            if let Some(prices) = store.get(coin) {
                let buy_price = get_price(&prices, pos.buy_exchange).unwrap_or(pos.entry_buy_price);
                let sell_price =
                    get_price(&prices, pos.sell_exchange).unwrap_or(pos.entry_sell_price);
                total += pos.unrealized_pnl(buy_price, sell_price);
            }
        }
        total
    }

    /// Check if a coin is in cooldown period.
    #[inline]
    fn is_on_cooldown(&self, coin: &str) -> bool {
        if let Some(last_time) = self.last_trade_time.get(coin) {
            let elapsed = Utc::now().signed_duration_since(*last_time).num_seconds();
            elapsed < TRADE_COOLDOWN_SECS
        } else {
            false
        }
    }

    /// Remaining cooldown seconds for a coin (if any).
    #[inline]
    fn cooldown_remaining_secs(&self, coin: &str) -> Option<i64> {
        if let Some(last_time) = self.last_trade_time.get(coin) {
            let elapsed = Utc::now().signed_duration_since(*last_time).num_seconds();
            if elapsed < TRADE_COOLDOWN_SECS {
                Some(TRADE_COOLDOWN_SECS - elapsed)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Helper to log a missed trade opportunity when spread was above threshold
    /// but the trade could not be taken.
    pub fn log_missed(
        &self,
        coin: &str,
        buy_exchange: Exchange,
        sell_exchange: Exchange,
        buy_price: f64,
        sell_price: f64,
        spread: f64,
        book_spread: Option<f64>,
        reason: String,
    ) {
        self.log_missed_with_latency(
            coin,
            buy_exchange,
            sell_exchange,
            buy_price,
            sell_price,
            spread,
            book_spread,
            reason,
            None,
        );
    }

    /// Helper to log a missed trade opportunity with full round-trip and pipeline latency data.
    pub fn log_missed_with_latency(
        &self,
        coin: &str,
        buy_exchange: Exchange,
        sell_exchange: Exchange,
        buy_price: f64,
        sell_price: f64,
        spread: f64,
        book_spread: Option<f64>,
        reason: String,
        latency: Option<crate::latency::TradeLatency>,
    ) {
        crate::missed_trade_logger::log_missed_trade(
            &crate::missed_trade_logger::MissedTradeRecord {
                timestamp: Utc::now(),
                coin: coin.to_string(),
                spread_pct: spread,
                threshold_pct: ENTRY_SPREAD_THRESHOLD,
                buy_exchange,
                sell_exchange,
                buy_price,
                sell_price,
                book_spread_pct: book_spread,
                reason,
                binance_balance: self.binance_balance,
                bybit_balance: self.bybit_balance,
                latency,
            },
        );
    }

    /// Update real balances from both exchanges.
    /// Reads from the WS-pushed LiveBalance — zero REST calls, zero latency.
    /// The private WS ACCOUNT_UPDATE / wallet events keep these up-to-date in real-time.
    /// Falls back to REST only at startup when the WS hasn't pushed a balance yet (value == 0.0).
    pub async fn refresh_balances(&mut self) {
        let bin_ws = self.binance_client.get_live_balance().await;
        let byb_ws = self.bybit_client.get_live_balance().await;

        if bin_ws > 0.0 {
            self.binance_balance = bin_ws;
        } else {
            // WS not yet warmed up — fall back to REST once
            match self.binance_client.get_balance().await {
                Ok(bal) => {
                    self.binance_balance = bal;
                }
                Err(e) => {
                    eprintln!("[LiveTrading] Failed to fetch Binance balance: {}", e);
                }
            }
        }

        if byb_ws > 0.0 {
            self.bybit_balance = byb_ws;
        } else {
            match self.bybit_client.get_balance().await {
                Ok(bal) => {
                    self.bybit_balance = bal;
                }
                Err(e) => {
                    eprintln!("[LiveTrading] Failed to fetch Bybit balance: {}", e);
                }
            }
        }
    }

    /// Get balance for a specific exchange.
    #[inline]
    pub fn get_balance(&self, exchange: Exchange) -> f64 {
        match exchange {
            Exchange::Binance => self.binance_balance,
            Exchange::Bybit => self.bybit_balance,
        }
    }

    /// Calculate trade parameters using fixed leverage.
    ///
    /// Returns `(trade_usdt, leverage)` or `None` if balance is too low.
    /// Both sides always use FIXED_LEVERAGE (10x) — no dynamic calculation,
    /// so the leverage cache always hits after the first trade on any symbol.
    fn calculate_trade_params(
        &self,
        buy_exchange: Exchange,
        sell_exchange: Exchange,
    ) -> Option<(f64, u32)> {
        let buy_balance = self.get_balance(buy_exchange);
        let sell_balance = self.get_balance(sell_exchange);

        // Minimum balance safety check
        if buy_balance < MIN_BALANCE_USDT || sell_balance < MIN_BALANCE_USDT {
            eprintln!(
                "[LiveTrading] Balance too low — {} ${:.2}, {} ${:.2} (min: ${:.2})",
                buy_exchange, buy_balance, sell_exchange, sell_balance, MIN_BALANCE_USDT
            );
            return None;
        }

        // Trade size = configured target, capped by what 10x leverage can support on each side
        let buy_max_notional = buy_balance * (FIXED_LEVERAGE as f64);
        let sell_max_notional = sell_balance * (FIXED_LEVERAGE as f64);
        let target_usdt = TRADE_SIZE_USDT.min(buy_max_notional).min(sell_max_notional);

        // Skip micro-trades
        if target_usdt < 5.0 {
            eprintln!(
                "[LiveTrading] Trade size ${:.2} too small (balance too low for {}x leverage)",
                target_usdt, FIXED_LEVERAGE
            );
            return None;
        }

        Some((target_usdt, FIXED_LEVERAGE))
    }

    /// Push a trade record to recent_trades with O(1) eviction.
    #[inline]
    fn push_recent_trade(&mut self, record: TradeRecord) {
        self.recent_trades.push_back(record);
        if self.recent_trades.len() > self.max_recent {
            self.recent_trades.pop_front();
        }
    }

    /// Try to OPEN a new arbitrage position.
    /// Places real MARKET orders on both exchanges CONCURRENTLY.
    /// If one leg fills but the other fails, automatically reverses the filled leg.
    pub async fn try_open_position(
        &mut self,
        coin: &str,
        buy_exchange: Exchange,
        sell_exchange: Exchange,
        buy_book: &OrderBookEntry,
        sell_book: &OrderBookEntry,
        buy_last_price: f64,
        sell_last_price: f64,
        spread: f64,
        dynamic_entry: f64,
        spread_velocity: f64,
        funding_store: &FundingStore,
        price_store: &PriceStore,
        mut latency: TradeLatency,
    ) -> bool {
        // Already have a position on this coin
        if self.open_positions.contains_key(coin) {
            self.log_missed(
                coin,
                buy_exchange,
                sell_exchange,
                buy_last_price,
                sell_last_price,
                spread,
                None,
                "ALREADY_OPEN: Position already open on this coin".to_string(),
            );
            return false;
        }

        // Emergency halt: a previous one-leg open had a failed reversal
        if self.emergency_halt {
            eprintln!("[LiveTrading] 🚨 EMERGENCY HALT active — skipping new opens until cleared (press R to reset)");
            self.log_missed(
                coin, buy_exchange, sell_exchange, buy_last_price, sell_last_price, spread, None,
                "EMERGENCY_HALT: Emergency halt active from previous failed reversal — press R to reset".to_string(),
            );
            return false;
        }

        // Check cooldown
        if self.is_on_cooldown(coin) {
            let rem = self.cooldown_remaining_secs(coin).unwrap_or(0);
            self.log_missed(
                coin,
                buy_exchange,
                sell_exchange,
                buy_last_price,
                sell_last_price,
                spread,
                None,
                format!(
                    "COOLDOWN: Coin in cooldown ({}s remaining of {}s)",
                    rem, TRADE_COOLDOWN_SECS
                ),
            );
            return false;
        }

        // Spread must meet entry threshold and not exceed sanity ceiling
        if spread < ENTRY_SPREAD_THRESHOLD || spread > MAX_SPREAD_THRESHOLD {
            return false;
        }

        // Check max open positions
        if self.open_positions.len() >= MAX_OPEN_POSITIONS {
            let holding = self
                .open_positions
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            self.log_missed(
                coin,
                buy_exchange,
                sell_exchange,
                buy_last_price,
                sell_last_price,
                spread,
                None,
                format!(
                    "MAX_POSITIONS_REACHED: Currently {}/{} open positions (holding: {})",
                    self.open_positions.len(),
                    MAX_OPEN_POSITIONS,
                    holding
                ),
            );
            return false;
        }

        let funding_hours = funding_store.get(coin).map(|r| *r.value()).unwrap_or(8);

        // ── 1H FUNDING EXCLUSION: Skip coins with 1h funding interval due to extreme volatility ──
        if SKIP_1H_FUNDING_COINS && funding_hours <= 1 {
            eprintln!(
                "[LiveTrading] SKIP {}: 1h funding interval coins disabled due to high volatility",
                coin
            );
            self.log_missed(
                coin,
                buy_exchange,
                sell_exchange,
                buy_last_price,
                sell_last_price,
                spread,
                None,
                format!(
                    "1H_FUNDING_COIN: Skipped coin with {}h funding interval (high volatility)",
                    funding_hours
                ),
            );
            return false;
        }

        // ── FUNDING PAUSE: Skip if near funding time ──
        if funding::is_near_funding(coin, funding_store) {
            let countdown = funding::funding_countdown(coin, funding_store);
            eprintln!(
                "[LiveTrading] SKIP {}: Near funding time (next in {})",
                coin, countdown
            );
            self.log_missed(
                coin,
                buy_exchange,
                sell_exchange,
                buy_last_price,
                sell_last_price,
                spread,
                None,
                format!(
                    "NEAR_FUNDING: Within {}m funding pause window (next funding in {})",
                    FUNDING_PAUSE_MINUTES, countdown
                ),
            );
            return false;
        }

        // Strict executable spread check from order book — NEVER fall back to last_price!
        let buy_ask = match buy_book.best_ask {
            Some(a) if a > 0.0 => a,
            _ => {
                self.log_missed(
                    coin,
                    buy_exchange,
                    sell_exchange,
                    buy_last_price,
                    sell_last_price,
                    spread,
                    None,
                    format!(
                        "ORDERBOOK_EMPTY: Missing best ask in {} order book",
                        buy_exchange
                    ),
                );
                return false;
            }
        };
        let sell_bid = match sell_book.best_bid {
            Some(b) if b > 0.0 => b,
            _ => {
                self.log_missed(
                    coin,
                    buy_exchange,
                    sell_exchange,
                    buy_last_price,
                    sell_last_price,
                    spread,
                    None,
                    format!(
                        "ORDERBOOK_EMPTY: Missing best bid in {} order book",
                        sell_exchange
                    ),
                );
                return false;
            }
        };

        if buy_ask >= sell_bid {
            let raw_book_spread = ((sell_bid - buy_ask) / buy_ask) * 100.0;
            self.log_missed(
                coin,
                buy_exchange,
                sell_exchange,
                buy_ask,
                sell_bid,
                spread,
                Some(raw_book_spread),
                format!(
                    "ORDERBOOK_CROSSED: Buy ask ({:.6}) >= Sell bid ({:.6})",
                    buy_ask, sell_bid
                ),
            );
            return false;
        }
        let book_spread_pct = ((sell_bid - buy_ask) / buy_ask) * 100.0;
        if book_spread_pct < ENTRY_SPREAD_THRESHOLD {
            self.log_missed(
                coin, buy_exchange, sell_exchange, buy_ask, sell_bid, spread, Some(book_spread_pct),
                format!("BOOK_SPREAD_BELOW_MIN: Real orderbook spread {:.3}% < required entry threshold {:.2}%", book_spread_pct, ENTRY_SPREAD_THRESHOLD),
            );
            return false;
        }
        if book_spread_pct > MAX_SPREAD_THRESHOLD {
            self.log_missed(
                coin,
                buy_exchange,
                sell_exchange,
                buy_ask,
                sell_bid,
                spread,
                Some(book_spread_pct),
                format!(
                    "BOOK_SPREAD_TOO_HIGH: Orderbook spread {:.2}% > max sanity {:.2}%",
                    book_spread_pct, MAX_SPREAD_THRESHOLD
                ),
            );
            return false; // Orderbook spread exceeds sanity ceiling
        }

        // Estimate total fees for all 4 legs
        let total_fee_rate = taker_fee(buy_exchange)
            + taker_fee(sell_exchange)
            + taker_fee(buy_exchange)
            + taker_fee(sell_exchange);
        let total_fee_pct = total_fee_rate * 100.0;

        let min_profitable_spread = total_fee_pct + EXIT_SPREAD_THRESHOLD + 0.1;
        if book_spread_pct < min_profitable_spread {
            self.log_missed(
                coin, buy_exchange, sell_exchange, buy_ask, sell_bid, spread, Some(book_spread_pct),
                format!("BOOK_SPREAD_NOT_PROFITABLE: Book spread {:.3}% < min required {:.3}% (fees {:.3}% + exit {:.2}% + 0.1% buffer)", book_spread_pct, min_profitable_spread, total_fee_pct, EXIT_SPREAD_THRESHOLD),
            );
            return false;
        }

        // ── FIXED LEVERAGE TRADE SIZING ──
        // Always uses FIXED_LEVERAGE (10x) — no recalculation per trade.
        let (mut trade_usdt, leverage) = match self
            .calculate_trade_params(buy_exchange, sell_exchange)
        {
            Some(params) => params,
            None => {
                self.log_missed(
                    coin, buy_exchange, sell_exchange, buy_ask, sell_bid, spread, Some(book_spread_pct),
                    format!("INSUFFICIENT_BALANCE: Balance below minimum ${:.2} (Binance: ${:.2}, Bybit: ${:.2})", MIN_BALANCE_USDT, self.binance_balance, self.bybit_balance),
                );
                return false; // Insufficient balance
            }
        };

        // ── STRICT LIQUIDITY CHECK ──
        // Ensure both order books have depth quantities available.
        let buy_ask_qty = match buy_book.best_ask_qty {
            Some(q) if q > 0.0 => q,
            _ => {
                self.log_missed(
                    coin,
                    buy_exchange,
                    sell_exchange,
                    buy_ask,
                    sell_bid,
                    spread,
                    Some(book_spread_pct),
                    format!(
                        "ORDERBOOK_EMPTY: Missing best ask qty in {} order book",
                        buy_exchange
                    ),
                );
                return false;
            }
        };

        let sell_bid_qty = match sell_book.best_bid_qty {
            Some(q) if q > 0.0 => q,
            _ => {
                self.log_missed(
                    coin,
                    buy_exchange,
                    sell_exchange,
                    buy_ask,
                    sell_bid,
                    spread,
                    Some(book_spread_pct),
                    format!(
                        "ORDERBOOK_EMPTY: Missing best bid qty in {} order book",
                        sell_exchange
                    ),
                );
                return false;
            }
        };

        // Calculate available liquidity at the best prices
        let buy_liquidity = buy_ask * buy_ask_qty;
        let sell_liquidity = sell_bid * sell_bid_qty;

        // Cap our trade size to the MINIMUM available liquidity on either side
        // to ensure we don't eat into the book and cause massive slippage.
        if buy_liquidity < trade_usdt {
            trade_usdt = buy_liquidity;
        }
        if sell_liquidity < trade_usdt {
            trade_usdt = sell_liquidity;
        }

        // Skip micro-trades after liquidity adjustment
        if trade_usdt < 5.0 {
            self.log_missed(
                coin, buy_exchange, sell_exchange, buy_ask, sell_bid, spread, Some(book_spread_pct),
                format!("LOW_LIQUIDITY: Orderbook depth allows only ${:.2} trade size (< $5.00 min). BuyLiq: ${:.2}, SellLiq: ${:.2}", trade_usdt, buy_liquidity, sell_liquidity),
            );
            return false;
        }

        // Calculate quantities in coin units — calculated PER EXCHANGE using that side's price.
        // BUG FIX: Using one shared quantity (trade_usdt / buy_ask) causes mismatch because
        // the sell exchange's price is always HIGHER, meaning it fills FEWER coins for the same USDT.
        let buy_quantity = trade_usdt / buy_ask;
        let sell_quantity = trade_usdt / sell_bid;
        // Use the MINIMUM of the two as the conservative hedged quantity for both sides.
        let raw_quantity = buy_quantity.min(sell_quantity);

        // Build the symbol string with USDT suffix (needed for exchange_info lookup)
        let symbol = format!("{}USDT", coin);

        // Use exchange-specific step size for precise rounding (falls back to heuristic).
        // Use the stricter (larger step) of the two exchanges to satisfy both.
        let qty_binance = self
            .round_quantity_exact(&symbol, Exchange::Binance, raw_quantity, buy_ask)
            .await;
        let qty_bybit = self
            .round_quantity_exact(&symbol, Exchange::Bybit, raw_quantity, buy_ask)
            .await;
        let quantity = qty_binance.min(qty_bybit);

        if quantity <= 0.0 {
            self.log_missed(
                coin,
                buy_exchange,
                sell_exchange,
                buy_ask,
                sell_bid,
                spread,
                Some(book_spread_pct),
                format!(
                    "QTY_ZERO: Calculated quantity rounded to 0.0 (price=${:.4})",
                    buy_ask
                ),
            );
            return false;
        }

        // ── PRE-NOTIONAL VALIDATION: Prevent Binance "notional must be no smaller than 5" ──
        let buy_notional = quantity * buy_ask;
        let sell_notional = quantity * sell_bid;
        let min_notional = self.get_min_notional(&symbol, Exchange::Binance).await;
        if buy_notional < min_notional * 1.05 || sell_notional < min_notional * 1.05 {
            self.log_missed(
                coin, buy_exchange, sell_exchange, buy_ask, sell_bid, spread, Some(book_spread_pct),
                format!("BELOW_MIN_NOTIONAL: Notional ${:.2}/{:.2} < min ${:.2} (qty={:.4}, prices={:.6}/{:.6})", 
                    buy_notional, sell_notional, min_notional, quantity, buy_ask, sell_bid),
            );
            return false;
        }

        eprintln!(
            "[LiveTrading] OPENING {} | BUY on {} ({}x) | SELL on {} ({}x) | Spread: {:.4}% | Book: {:.4}% | Size: ${:.2} | BuyQty: {:.2} SellQty: {:.2} → Qty: {:.2}",
            coin, buy_exchange, leverage, sell_exchange, leverage, spread, book_spread_pct, trade_usdt,
            buy_quantity, sell_quantity, quantity
        );

        // ── PRE-TRADE BALANCE SANITY CHECK (cached — no REST call) ──
        // Balance is kept accurate by:
        //   • startup refresh_balances()
        //   • post-trade refresh after every open/close
        //   • periodic refresh every 60 seconds
        // No need to hit the exchange REST API again here — that costs 50–150ms.
        let buy_balance = self.get_balance(buy_exchange);
        let sell_balance = self.get_balance(sell_exchange);

        // Each side needs at least (trade_usdt / 10x) as margin, +10% buffer for fees and slippage.
        let required_margin = (trade_usdt / FIXED_LEVERAGE as f64) * 1.10;

        if buy_balance < required_margin {
            eprintln!(
                "[LiveTrading] ABORT {}: {} balance ${:.2} too low — need ${:.2} margin for ${:.0} trade at {}x",
                coin, buy_exchange, buy_balance, required_margin, trade_usdt, FIXED_LEVERAGE
            );
            self.log_missed(
                coin, buy_exchange, sell_exchange, buy_ask, sell_bid, spread, Some(book_spread_pct),
                format!("INSUFFICIENT_MARGIN_BUY: {} balance ${:.2} < required margin ${:.2} for ${:.0} trade at {}x", buy_exchange, buy_balance, required_margin, trade_usdt, FIXED_LEVERAGE),
            );
            return false;
        }
        if sell_balance < required_margin {
            eprintln!(
                "[LiveTrading] ABORT {}: {} balance ${:.2} too low — need ${:.2} margin for ${:.0} trade at {}x",
                coin, sell_exchange, sell_balance, required_margin, trade_usdt, FIXED_LEVERAGE
            );
            self.log_missed(
                coin, buy_exchange, sell_exchange, buy_ask, sell_bid, spread, Some(book_spread_pct),
                format!("INSUFFICIENT_MARGIN_SELL: {} balance ${:.2} < required margin ${:.2} for ${:.0} trade at {}x", sell_exchange, sell_balance, required_margin, trade_usdt, FIXED_LEVERAGE),
            );
            return false;
        }

        // ── SET LEVERAGE (cached — same 10x on both exchanges, skipped after first trade per symbol) ──
        let cached = self.leverage_cache.get(&symbol);
        let skip_leverage = cached
            .map(|c| c.0 == leverage && c.1 == leverage)
            .unwrap_or(false);

        if !skip_leverage {
            eprintln!(
                "[LiveTrading] Setting leverage {}x on both exchanges for {} (backgrounding)",
                leverage, symbol
            );

            let bin_cli = self.binance_client.clone();
            let byb_cli = self.bybit_client.clone();
            let sym = symbol.clone();

            tokio::spawn(async move {
                let (bin_result, byb_result) = tokio::join!(
                    bin_cli.set_leverage(&sym, leverage),
                    byb_cli.set_leverage(&sym, leverage)
                );
                if let Err(e) = bin_result {
                    eprintln!(
                        "[LiveTrading] WARNING: Failed to set leverage on Binance for {}: {}",
                        sym, e
                    );
                }
                if let Err(e) = byb_result {
                    eprintln!(
                        "[LiveTrading] WARNING: Failed to set leverage on Bybit for {}: {}",
                        sym, e
                    );
                }
            });

            self.leverage_cache
                .insert(symbol.clone(), (leverage, leverage));
        }

        // ── Unified fill struct for normalizing data from both exchanges ──
        struct UnifiedFill {
            order_id: String,
            avg_price: f64,
            filled_qty: f64,
            quote_qty: f64,
            commission: f64,
        }

        // Helper to convert Binance fill
        fn from_binance_fill(f: &crate::exchanges::binance_api::OrderFill) -> UnifiedFill {
            UnifiedFill {
                order_id: f.order_id.to_string(),
                avg_price: f.avg_price,
                filled_qty: f.filled_qty,
                quote_qty: f.quote_qty,
                commission: f.commission,
            }
        }

        // Helper to convert Bybit fill
        fn from_bybit_fill(f: &crate::exchanges::bybit_api::OrderFill) -> UnifiedFill {
            UnifiedFill {
                order_id: f.order_id.clone(),
                avg_price: f.avg_price,
                filled_qty: f.filled_qty,
                quote_qty: f.quote_qty,
                commission: f.commission,
            }
        }

        // ── PRE-FLIGHT SPREAD RE-CHECK: verify fresh quotes right before firing orders ──
        if let Some(fresh_entry) = price_store.get(coin) {
            let cur_buy_book = get_order_book(&fresh_entry, buy_exchange);
            let cur_sell_book = get_order_book(&fresh_entry, sell_exchange);
            if let (Some(cur_ask), Some(cur_bid)) = (cur_buy_book.best_ask, cur_sell_book.best_bid)
            {
                if cur_ask <= 0.0 || cur_bid <= 0.0 || cur_ask >= cur_bid {
                    eprintln!(
                        "[{}][LiveTrading] ABORT {}: Pre-flight order book crossed or empty (ask={:.6}, bid={:.6})",
                        Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"), coin, cur_ask, cur_bid
                    );
                    self.log_missed_with_latency(
                        coin, buy_exchange, sell_exchange, cur_ask, cur_bid, spread, None,
                        format!("PRE_FLIGHT_BOOK_INVALID: Pre-flight check failed — Ask ({:.6}) >= Bid ({:.6})", cur_ask, cur_bid),
                        Some(latency.clone()),
                    );
                    return false;
                }
                let cur_spread = ((cur_bid - cur_ask) / cur_ask) * 100.0;
                if cur_spread < ENTRY_SPREAD_THRESHOLD {
                    eprintln!(
                        "[{}][LiveTrading] ABORT {}: Spread slipped from {:.3}% to {:.3}% right before execution (< {:.2}%)",
                        Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"), coin, book_spread_pct, cur_spread, ENTRY_SPREAD_THRESHOLD
                    );
                    latency.mark_pre_flight(cur_ask, cur_bid, cur_spread);
                    latency.compute_derived();
                    self.log_missed_with_latency(
                        coin, buy_exchange, sell_exchange, cur_ask, cur_bid, spread, Some(cur_spread),
                        format!("PRE_FLIGHT_SPREAD_COLLAPSED: Spread dropped to {:.3}% during pre-trade checks (< {:.2}%)", cur_spread, ENTRY_SPREAD_THRESHOLD),
                        Some(latency.clone()),
                    );
                    return false;
                }
                // ✔ Pre-flight passed — mark timestamp + quote snapshot
                latency.mark_pre_flight(cur_ask, cur_bid, cur_spread);
            }
        }

        // Adaptive slippage cap: Allow more slippage if the spread is wider, up to 0.35%
        let slip_pct = if spread > 1.2 {
            MAX_ALLOWED_SLIPPAGE_PCT / 100.0
        } else {
            0.002
        };
        let raw_buy_price = buy_ask * (1.0 + slip_pct);
        let raw_sell_price = sell_bid * (1.0 - slip_pct);
        // Round limit prices to exchange tick size to prevent "Price not increased by tick size" errors
        let worst_buy_price = Some(
            self.round_price(&symbol, buy_exchange, raw_buy_price, true)
                .await,
        );
        let worst_sell_price = Some(
            self.round_price(&symbol, sell_exchange, raw_sell_price, false)
                .await,
        );

        let (buy_unified, sell_unified) = match (buy_exchange, sell_exchange) {
            (Exchange::Binance, Exchange::Bybit) => {
                latency.mark_order_send();
                let buy_fut = async {
                    let t0 = std::time::Instant::now();
                    let res = self
                        .binance_client
                        .execute_order_with_fill(&symbol, "BUY", quantity, false, worst_buy_price)
                        .await;
                    (res, t0.elapsed().as_millis() as i64)
                };
                let sell_fut = async {
                    let t0 = std::time::Instant::now();
                    let res = self
                        .bybit_client
                        .execute_order_with_fill(&symbol, "Sell", quantity, false, worst_sell_price)
                        .await;
                    (res, t0.elapsed().as_millis() as i64)
                };
                let ((buy_result, buy_rtt_ms), (sell_result, sell_rtt_ms)) =
                    tokio::join!(buy_fut, sell_fut);
                latency.mark_exchange_ack();
                latency.buy_leg_rtt_ms = Some(buy_rtt_ms);
                latency.sell_leg_rtt_ms = Some(sell_rtt_ms);
                latency.compute_derived();

                match (buy_result, sell_result) {
                    (Ok(bf), Ok(sf)) => (from_binance_fill(&bf), from_bybit_fill(&sf)),
                    (Ok(bf), Err(e)) => {
                        // BUY filled on Binance but SELL failed on Bybit — REVERSE BUY
                        let rev_t0 = std::time::Instant::now();
                        let rev_result = self
                            .binance_client
                            .execute_order_with_fill(&symbol, "SELL", bf.filled_qty, true, None)
                            .await;
                        let rev_rtt_ms = rev_t0.elapsed().as_millis() as i64;
                        latency.reversal_rtt_ms = Some(rev_rtt_ms);
                        latency.compute_derived();

                        latency.print_leg_failure_audit(
                            coin,
                            buy_exchange,
                            sell_exchange,
                            true,
                            false,
                            buy_rtt_ms,
                            sell_rtt_ms,
                            &format!("Sell on Bybit failed: {}", e),
                            Some(("Binance", rev_rtt_ms, rev_result.is_ok())),
                        );

                        self.log_missed_with_latency(
                            coin, buy_exchange, sell_exchange, buy_ask, sell_bid, spread, Some(book_spread_pct),
                            format!("LEG_FILL_FAILED: Sell on Bybit failed ({}) [RTT: Bybit={}ms, Binance={}ms] — reversed Binance Buy (rev: {}ms)", e, sell_rtt_ms, buy_rtt_ms, rev_rtt_ms),
                            Some(latency.clone()),
                        );

                        match rev_result {
                            Ok(_) => eprintln!("[LiveTrading] ONE-LEG RECOVERY: Successfully reversed Binance BUY for {} (took {}ms)", coin, rev_rtt_ms),
                            Err(rev_err) => {
                                eprintln!("[LiveTrading] 🚨 REVERSAL ALSO FAILED for {}: {} — Binance BUY still open! CLOSE MANUALLY!", coin, rev_err);
                                self.emergency_halt = true;
                            }
                        }
                        self.last_trade_time.insert(coin.to_string(), Utc::now());
                        return false;
                    }
                    (Err(e), Ok(sf)) => {
                        // SELL filled on Bybit but BUY failed on Binance — REVERSE SELL
                        let rev_t0 = std::time::Instant::now();
                        let rev_result = self
                            .bybit_client
                            .execute_order_with_fill(&symbol, "Buy", sf.filled_qty, true, None)
                            .await;
                        let rev_rtt_ms = rev_t0.elapsed().as_millis() as i64;
                        latency.reversal_rtt_ms = Some(rev_rtt_ms);
                        latency.compute_derived();

                        latency.print_leg_failure_audit(
                            coin,
                            buy_exchange,
                            sell_exchange,
                            false,
                            true,
                            buy_rtt_ms,
                            sell_rtt_ms,
                            &format!("Buy on Binance failed: {}", e),
                            Some(("Bybit", rev_rtt_ms, rev_result.is_ok())),
                        );

                        self.log_missed_with_latency(
                            coin, buy_exchange, sell_exchange, buy_ask, sell_bid, spread, Some(book_spread_pct),
                            format!("LEG_FILL_FAILED: Buy on Binance failed ({}) [RTT: Binance={}ms, Bybit={}ms] — reversed Bybit Sell (rev: {}ms)", e, buy_rtt_ms, sell_rtt_ms, rev_rtt_ms),
                            Some(latency.clone()),
                        );

                        match rev_result {
                            Ok(_) => eprintln!("[LiveTrading] ONE-LEG RECOVERY: Successfully reversed Bybit SELL for {} (took {}ms)", coin, rev_rtt_ms),
                            Err(rev_err) => {
                                eprintln!("[LiveTrading] 🚨 REVERSAL ALSO FAILED for {}: {} — Bybit SELL still open! CLOSE MANUALLY!", coin, rev_err);
                                self.emergency_halt = true;
                            }
                        }
                        self.last_trade_time.insert(coin.to_string(), Utc::now());
                        return false;
                    }
                    (Err(e1), Err(e2)) => {
                        latency.compute_derived();
                        latency.print_leg_failure_audit(
                            coin,
                            buy_exchange,
                            sell_exchange,
                            false,
                            false,
                            buy_rtt_ms,
                            sell_rtt_ms,
                            &format!("Buy err: {}, Sell err: {}", e1, e2),
                            None,
                        );

                        self.log_missed_with_latency(
                            coin,
                            buy_exchange,
                            sell_exchange,
                            buy_ask,
                            sell_bid,
                            spread,
                            Some(book_spread_pct),
                            format!(
                                "BOTH_LEGS_FAILED: Buy err: {} ({}ms), Sell err: {} ({}ms)",
                                e1, buy_rtt_ms, e2, sell_rtt_ms
                            ),
                            Some(latency.clone()),
                        );
                        self.emergency_halt = true;
                        self.last_trade_time.insert(coin.to_string(), Utc::now());
                        return false;
                    }
                }
            }
            (Exchange::Bybit, Exchange::Binance) => {
                latency.mark_order_send();
                let buy_fut = async {
                    let t0 = std::time::Instant::now();
                    let res = self
                        .bybit_client
                        .execute_order_with_fill(&symbol, "Buy", quantity, false, worst_buy_price)
                        .await;
                    (res, t0.elapsed().as_millis() as i64)
                };
                let sell_fut = async {
                    let t0 = std::time::Instant::now();
                    let res = self
                        .binance_client
                        .execute_order_with_fill(&symbol, "SELL", quantity, false, worst_sell_price)
                        .await;
                    (res, t0.elapsed().as_millis() as i64)
                };
                let ((buy_result, buy_rtt_ms), (sell_result, sell_rtt_ms)) =
                    tokio::join!(buy_fut, sell_fut);
                latency.mark_exchange_ack();
                latency.buy_leg_rtt_ms = Some(buy_rtt_ms);
                latency.sell_leg_rtt_ms = Some(sell_rtt_ms);
                latency.compute_derived();

                match (buy_result, sell_result) {
                    (Ok(bf), Ok(sf)) => (from_bybit_fill(&bf), from_binance_fill(&sf)),
                    (Ok(bf), Err(e)) => {
                        // BUY filled on Bybit but SELL failed on Binance — REVERSE BUY
                        let rev_t0 = std::time::Instant::now();
                        let rev_result = self
                            .bybit_client
                            .execute_order_with_fill(&symbol, "Sell", bf.filled_qty, true, None)
                            .await;
                        let rev_rtt_ms = rev_t0.elapsed().as_millis() as i64;
                        latency.reversal_rtt_ms = Some(rev_rtt_ms);
                        latency.compute_derived();

                        latency.print_leg_failure_audit(
                            coin,
                            buy_exchange,
                            sell_exchange,
                            true,
                            false,
                            buy_rtt_ms,
                            sell_rtt_ms,
                            &format!("Sell on Binance failed: {}", e),
                            Some(("Bybit", rev_rtt_ms, rev_result.is_ok())),
                        );

                        self.log_missed_with_latency(
                            coin, buy_exchange, sell_exchange, buy_ask, sell_bid, spread, Some(book_spread_pct),
                            format!("LEG_FILL_FAILED: Sell on Binance failed ({}) [RTT: Binance={}ms, Bybit={}ms] — reversed Bybit Buy (rev: {}ms)", e, sell_rtt_ms, buy_rtt_ms, rev_rtt_ms),
                            Some(latency.clone()),
                        );

                        match rev_result {
                            Ok(_) => eprintln!("[LiveTrading] ONE-LEG RECOVERY: Successfully reversed Bybit BUY for {} (took {}ms)", coin, rev_rtt_ms),
                            Err(rev_err) => {
                                eprintln!("[LiveTrading] 🚨 REVERSAL ALSO FAILED for {}: {} — Bybit BUY still open! CLOSE MANUALLY!", coin, rev_err);
                                self.emergency_halt = true;
                            }
                        }
                        self.last_trade_time.insert(coin.to_string(), Utc::now());
                        return false;
                    }
                    (Err(e), Ok(sf)) => {
                        // SELL filled on Binance but BUY failed on Bybit — REVERSE SELL
                        let rev_t0 = std::time::Instant::now();
                        let rev_result = self
                            .binance_client
                            .execute_order_with_fill(&symbol, "BUY", sf.filled_qty, true, None)
                            .await;
                        let rev_rtt_ms = rev_t0.elapsed().as_millis() as i64;
                        latency.reversal_rtt_ms = Some(rev_rtt_ms);
                        latency.compute_derived();

                        latency.print_leg_failure_audit(
                            coin,
                            buy_exchange,
                            sell_exchange,
                            false,
                            true,
                            buy_rtt_ms,
                            sell_rtt_ms,
                            &format!("Buy on Bybit failed: {}", e),
                            Some(("Binance", rev_rtt_ms, rev_result.is_ok())),
                        );

                        self.log_missed_with_latency(
                            coin, buy_exchange, sell_exchange, buy_ask, sell_bid, spread, Some(book_spread_pct),
                            format!("LEG_FILL_FAILED: Buy on Bybit failed ({}) [RTT: Bybit={}ms, Binance={}ms] — reversed Binance Sell (rev: {}ms)", e, buy_rtt_ms, sell_rtt_ms, rev_rtt_ms),
                            Some(latency.clone()),
                        );

                        match rev_result {
                            Ok(_) => eprintln!("[LiveTrading] ONE-LEG RECOVERY: Successfully reversed Binance SELL for {} (took {}ms)", coin, rev_rtt_ms),
                            Err(rev_err) => {
                                eprintln!("[LiveTrading] 🚨 REVERSAL ALSO FAILED for {}: {} — Binance SELL still open! CLOSE MANUALLY!", coin, rev_err);
                                self.emergency_halt = true;
                            }
                        }
                        self.last_trade_time.insert(coin.to_string(), Utc::now());
                        return false;
                    }
                    (Err(e1), Err(e2)) => {
                        latency.compute_derived();
                        latency.print_leg_failure_audit(
                            coin,
                            buy_exchange,
                            sell_exchange,
                            false,
                            false,
                            buy_rtt_ms,
                            sell_rtt_ms,
                            &format!("Bybit Buy err: {}, Binance Sell err: {}", e1, e2),
                            None,
                        );

                        self.log_missed_with_latency(
                            coin, buy_exchange, sell_exchange, buy_ask, sell_bid, spread, Some(book_spread_pct),
                            format!("BOTH_LEGS_FAILED: Buy (Bybit): {} [{}ms], Sell (Binance): {} [{}ms]", e1, buy_rtt_ms, e2, sell_rtt_ms),
                            Some(latency.clone()),
                        );
                        self.emergency_halt = true;
                        self.last_trade_time.insert(coin.to_string(), Utc::now());
                        return false;
                    }
                }
            }
            _ => {
                eprintln!(
                    "[LiveTrading] Unsupported exchange pair: {} -> {}",
                    buy_exchange, sell_exchange
                );
                return false;
            }
        };

        let buy_order_id = buy_unified.order_id;
        let buy_fill_price = buy_unified.avg_price;
        let buy_filled_qty = buy_unified.filled_qty;
        let buy_quote_value = buy_unified.quote_qty;
        let buy_commission = buy_unified.commission;

        let sell_order_id = sell_unified.order_id;
        let sell_fill_price = sell_unified.avg_price;
        let sell_filled_qty = sell_unified.filled_qty;
        let sell_quote_value = sell_unified.quote_qty;
        let sell_commission = sell_unified.commission;

        // ── POST-FILL SLIPPAGE AUDIT ──
        let actual_entry_spread = if buy_fill_price > 0.0 {
            ((sell_fill_price - buy_fill_price) / buy_fill_price) * 100.0
        } else {
            0.0
        };
        let buy_slippage = if buy_ask > 0.0 {
            ((buy_fill_price - buy_ask) / buy_ask) * 100.0
        } else {
            0.0
        };
        let sell_slippage = if sell_bid > 0.0 {
            ((sell_bid - sell_fill_price) / sell_bid) * 100.0
        } else {
            0.0
        };

        // ✔ Fill confirmed — mark WS fill timestamp and compute all derived latency metrics
        latency.mark_ws_fill();
        latency.log_summary(coin);

        eprintln!(
            "[{}][LiveTrading] FILL AUDIT for {}: Quoted Book: {:.3}% → Real Executed: {:.3}% | Buy: {:.6} (slip: {:+.3}%) | Sell: {:.6} (slip: {:+.3}%)",
            Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            coin, book_spread_pct, actual_entry_spread, buy_fill_price, buy_slippage, sell_fill_price, sell_slippage
        );
        if actual_entry_spread < 0.0 {
            eprintln!(
                "[{}][LiveTrading] ⚠️ WARNING: NEGATIVE EXECUTED SPREAD ({:.3}%) on {} — bought @ {:.6}, sold @ {:.6}",
                Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                actual_entry_spread, coin, buy_fill_price, sell_fill_price
            );
        }
        if buy_slippage.abs() > MAX_ALLOWED_SLIPPAGE_PCT
            || sell_slippage.abs() > MAX_ALLOWED_SLIPPAGE_PCT
        {
            eprintln!(
                "[{}][LiveTrading] ⚠️ HIGH SLIPPAGE ALERT on {}: Buy slip={:+.3}%, Sell slip={:+.3}% (threshold: {:.2}%)",
                Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                coin, buy_slippage, sell_slippage, MAX_ALLOWED_SLIPPAGE_PCT
            );
        }

        // ── SAFETY CHECK 1: Filled quantity mismatch & Hedge Alignment ──
        // Market/IOC orders can get partial fills. If buy_qty ≠ sell_qty, the difference
        // is unhedged directional exposure. We immediately execute a MARKET order to close the excess.
        let mut final_buy_filled_qty = buy_filled_qty;
        let mut final_sell_filled_qty = sell_filled_qty;

        if final_buy_filled_qty > 0.0 && final_sell_filled_qty > 0.0 {
            let mismatch_pct = (final_buy_filled_qty - final_sell_filled_qty).abs()
                / final_buy_filled_qty.max(final_sell_filled_qty)
                * 100.0;

            if mismatch_pct > 0.1 {
                eprintln!(
                    "[LiveTrading] ⚠️ QTY MISMATCH on {} — BUY: {:.6}, SELL: {:.6}, diff: {:.3}%. Aligning to minimum...",
                    coin, final_buy_filled_qty, final_sell_filled_qty, mismatch_pct
                );

                let min_qty = final_buy_filled_qty.min(final_sell_filled_qty);

                if final_buy_filled_qty > min_qty {
                    let mut excess =
                        Self::round_quantity(final_buy_filled_qty - min_qty, buy_fill_price);
                    let mut attempts = 0;
                    while excess > 0.0 && attempts < 2 {
                        eprintln!(
                            "[LiveTrading] ⚖️ ALIGNMENT {}/2: Selling excess {} on {}",
                            attempts + 1,
                            excess,
                            buy_exchange
                        );
                        let slip_pct = if attempts == 0 { 0.998 } else { 0.995 }; // 0.2% then 0.5% slippage allowance
                        let worst_align_price =
                            Some(buy_book.best_bid.unwrap_or(buy_fill_price) * slip_pct);

                        let fill_res: Result<f64, String> = match buy_exchange {
                            Exchange::Binance => self
                                .binance_client
                                .execute_order_with_fill(
                                    &symbol,
                                    "SELL",
                                    excess,
                                    true,
                                    worst_align_price,
                                )
                                .await
                                .map(|f| f.filled_qty),
                            Exchange::Bybit => self
                                .bybit_client
                                .execute_order_with_fill(
                                    &symbol,
                                    "Sell",
                                    excess,
                                    true,
                                    worst_align_price,
                                )
                                .await
                                .map(|f| f.filled_qty),
                        };

                        if let Ok(align_qty) = fill_res {
                            if align_qty > 0.0 {
                                final_buy_filled_qty -= align_qty;
                                excess = Self::round_quantity(
                                    final_buy_filled_qty - min_qty,
                                    buy_fill_price,
                                );
                            }
                        }

                        if excess > 0.0 && attempts == 0 {
                            eprintln!("[LiveTrading] ⚠️ ALIGNMENT 1/2 PARTIAL/FAILED. Retrying with wider limit...");
                        } else if excess > 0.0 {
                            eprintln!("[LiveTrading] ⚠️ ALIGNMENT GAVE UP: Retained {} unhedged coins on {}. Will carry residual to exit.", excess, buy_exchange);
                        }
                        attempts += 1;
                    }
                } else if final_sell_filled_qty > min_qty {
                    let mut excess =
                        Self::round_quantity(final_sell_filled_qty - min_qty, sell_fill_price);
                    let mut attempts = 0;
                    while excess > 0.0 && attempts < 2 {
                        eprintln!(
                            "[LiveTrading] ⚖️ ALIGNMENT {}/2: Buying excess {} on {}",
                            attempts + 1,
                            excess,
                            sell_exchange
                        );
                        let slip_pct = if attempts == 0 { 1.002 } else { 1.005 }; // 0.2% then 0.5% slippage allowance
                        let worst_align_price =
                            Some(sell_book.best_ask.unwrap_or(sell_fill_price) * slip_pct);

                        let fill_res: Result<f64, String> = match sell_exchange {
                            Exchange::Binance => self
                                .binance_client
                                .execute_order_with_fill(
                                    &symbol,
                                    "BUY",
                                    excess,
                                    true,
                                    worst_align_price,
                                )
                                .await
                                .map(|f| f.filled_qty),
                            Exchange::Bybit => self
                                .bybit_client
                                .execute_order_with_fill(
                                    &symbol,
                                    "Buy",
                                    excess,
                                    true,
                                    worst_align_price,
                                )
                                .await
                                .map(|f| f.filled_qty),
                        };

                        if let Ok(align_qty) = fill_res {
                            if align_qty > 0.0 {
                                final_sell_filled_qty -= align_qty;
                                excess = Self::round_quantity(
                                    final_sell_filled_qty - min_qty,
                                    sell_fill_price,
                                );
                            }
                        }

                        if excess > 0.0 && attempts == 0 {
                            eprintln!("[LiveTrading] ⚠️ ALIGNMENT 1/2 PARTIAL/FAILED. Retrying with wider limit...");
                        } else if excess > 0.0 {
                            eprintln!("[LiveTrading] ⚠️ ALIGNMENT GAVE UP: Retained {} unhedged coins on {}. Will carry residual to exit.", excess, sell_exchange);
                        }
                        attempts += 1;
                    }
                }
            }
        }

        // ── SAFETY CHECK 2: Post-trade balance refresh ──
        // Immediately re-fetch real balances from both exchanges so the engine
        // uses accurate margin numbers for the next trade opportunity.
        // Without this, the engine sees stale balances for up to 60 seconds,
        // potentially sizing the next trade incorrectly.
        self.refresh_balances().await;
        eprintln!(
            "[LiveTrading] Post-trade balances — Binance: ${:.2} | Bybit: ${:.2}",
            self.binance_balance, self.bybit_balance
        );

        let total_entry_fee = buy_commission + sell_commission;
        self.total_fees_paid += total_entry_fee;
        self.trade_count += 1;
        self.last_trade_time.insert(coin.to_string(), Utc::now());

        let funding_hours = funding_store.get(coin).map(|r| *r.value()).unwrap_or(8);

        // Store open position with real fill data
        let position = OpenPosition {
            coin: coin.to_string(),
            buy_exchange,
            sell_exchange,
            entry_buy_price: buy_fill_price,
            entry_sell_price: sell_fill_price,
            buy_filled_qty: final_buy_filled_qty,
            sell_filled_qty: final_sell_filled_qty,
            buy_quote_value,
            sell_quote_value,
            entry_buy_commission: buy_commission,
            entry_sell_commission: sell_commission,
            buy_order_id: buy_order_id.clone(),
            sell_order_id: sell_order_id.clone(),
            entry_spread: actual_entry_spread,
            open_time: Utc::now(),
            entry_buy_book_bid: buy_book.best_bid.unwrap_or(0.0),
            entry_buy_book_ask: buy_book.best_ask.unwrap_or(0.0),
            entry_buy_book_bid_qty: buy_book.best_bid_qty,
            entry_buy_book_ask_qty: buy_book.best_ask_qty,
            entry_sell_book_bid: sell_book.best_bid.unwrap_or(0.0),
            entry_sell_book_ask: sell_book.best_ask.unwrap_or(0.0),
            entry_sell_book_bid_qty: sell_book.best_bid_qty,
            entry_sell_book_ask_qty: sell_book.best_ask_qty,
            funding_hours,
        };
        self.open_positions.insert(coin.to_string(), position);

        // Save OPEN trade record with real exchange fill details
        let open_record = TradeRecord {
            id: self.trade_count,
            timestamp: Utc::now(),
            coin: coin.to_string(),
            exchange_buy: buy_exchange,
            exchange_sell: sell_exchange,
            trade_type: TradeType::Open,
            buy_order_id: buy_order_id.clone(),
            sell_order_id: sell_order_id.clone(),
            buy_fill_price,
            sell_fill_price,
            buy_filled_qty: final_buy_filled_qty,
            sell_filled_qty: final_sell_filled_qty,
            buy_quote_value,
            sell_quote_value,
            buy_commission,
            buy_commission_asset: "USDT".to_string(),
            sell_commission,
            sell_commission_asset: "USDT".to_string(),
            total_fee: total_entry_fee,
            spread_before: book_spread_pct,
            spread_after: actual_entry_spread,
            pnl_gross: 0.0,
            pnl_net: -total_entry_fee,
            exchange_realized_pnl: 0.0,
            buy_book_bid: buy_book.best_bid.unwrap_or(0.0),
            buy_book_ask: buy_book.best_ask.unwrap_or(0.0),
            buy_book_bid_qty: buy_book.best_bid_qty,
            buy_book_ask_qty: buy_book.best_ask_qty,
            sell_book_bid: sell_book.best_bid.unwrap_or(0.0),
            sell_book_ask: sell_book.best_ask.unwrap_or(0.0),
            sell_book_bid_qty: sell_book.best_bid_qty,
            sell_book_ask_qty: sell_book.best_ask_qty,
            close_buy_price: None,
            close_sell_price: None,
            close_buy_commission: None,
            close_sell_commission: None,
            close_buy_order_id: None,
            close_sell_order_id: None,
            total_open_fees: Some(total_entry_fee),
            total_close_fees: None,
            hold_duration_secs: None,
            entry_spread: Some(actual_entry_spread),
            exit_spread: None,
            funding_interval_hours: Some(funding_hours),
            latency: Some(latency),
            real_account_entry_buy: None,
            real_account_entry_sell: None,
            real_unrealized_pnl: None,
            buy_order_status: Some("FILLED".to_string()),
            sell_order_status: Some("FILLED".to_string()),
        };

        if let Err(e) = save_trade(&open_record, &self.log_path) {
            eprintln!(
                "[{}][LiveTrading] Failed to save OPEN trade: {}",
                Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                e
            );
        }

        self.push_recent_trade(open_record);

        eprintln!(
            "[{}][LiveTrading] OPEN #{}: {} | BUY {} @ {:.6} (id: {}) | SELL {} @ {:.6} (id: {}) | Fees: ${:.4} | DynEntry: {:.3}% | Vel: {:.3}%/s",
            Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            self.trade_count, coin,
            buy_exchange, buy_fill_price, buy_order_id,
            sell_exchange, sell_fill_price, sell_order_id,
            total_entry_fee, dynamic_entry, spread_velocity
        );

        true
    }

    pub async fn try_close_position(
        &mut self,
        coin: &str,
        current_spread: f64,
        dynamic_exit: f64,
        store: &crate::price_store::PriceStore,
    ) -> Option<TradeRecord> {
        let position = self.open_positions.get(coin)?.clone();
        let hold_secs = Utc::now()
            .signed_duration_since(position.open_time)
            .num_seconds();

        let buy_exchange = position.buy_exchange;
        let sell_exchange = position.sell_exchange;
        let symbol = format!("{}USDT", coin);

        // Close EXACTLY the filled quantity on each leg.
        let close_buy_qty = Self::round_quantity(position.buy_filled_qty, position.entry_buy_price);
        let close_sell_qty =
            Self::round_quantity(position.sell_filled_qty, position.entry_sell_price);

        if close_buy_qty <= 0.0 || close_sell_qty <= 0.0 {
            eprintln!("[LiveTrading] CLOSING {} FAILED: close_qty rounded to <= 0.0. Manual intervention needed.", coin);
            return None;
        }

        // Fetch current book prices to calculate close slippage relative to CURRENT market, NOT entry.
        // If we calculate relative to entry, any price movement > 0.35% will cause close orders to fail!
        let cur_sell_ask = store
            .get(coin)
            .and_then(|p| match sell_exchange {
                Exchange::Binance => Some(p.value().binance_book.clone()),
                Exchange::Bybit => Some(p.value().bybit_book.clone()),
            })
            .and_then(|b| b.best_ask)
            .unwrap_or(position.entry_sell_price);

        let cur_buy_bid = store
            .get(coin)
            .and_then(|p| match buy_exchange {
                Exchange::Binance => Some(p.value().binance_book.clone()),
                Exchange::Bybit => Some(p.value().bybit_book.clone()),
            })
            .and_then(|b| b.best_bid)
            .unwrap_or(position.entry_buy_price);

        // Close slippage protection (0.50% from current market)
        let worst_buy_close = Some(
            self.round_price(&symbol, sell_exchange, cur_sell_ask * 1.005, true)
                .await,
        );
        let worst_sell_close = Some(
            self.round_price(&symbol, buy_exchange, cur_buy_bid * 0.995, false)
                .await,
        );

        eprintln!(
            "[LiveTrading] CLOSING {} | SELL on {} | BUY on {} | Held {}s | Spread: {:.4}%",
            coin, buy_exchange, sell_exchange, hold_secs, current_spread
        );

        // ── Unified fill struct for close leg ──
        struct CloseFill {
            order_id: String,
            avg_price: f64,
            commission: f64,
        }

        fn close_from_binance(f: &crate::exchanges::binance_api::OrderFill) -> CloseFill {
            CloseFill {
                order_id: f.order_id.to_string(),
                avg_price: f.avg_price,
                commission: f.commission,
            }
        }

        fn close_from_bybit(f: &crate::exchanges::bybit_api::OrderFill) -> CloseFill {
            CloseFill {
                order_id: f.order_id.clone(),
                avg_price: f.avg_price,
                commission: f.commission,
            }
        }

        // ── Place REAL MARKET orders to CLOSE — CONCURRENTLY ──
        // Close long: SELL on buy_exchange (reduce_only=true — close only, never open)
        // Close short: BUY on sell_exchange (reduce_only=true — close only, never open)
        //
        // PARTIAL CLOSE HANDLING:
        // If one leg fills but the other fails, we retry ONLY the failed leg (up to 3x).
        // We NEVER retry the successful leg — doing so would create a new position on that
        // exchange, which is exactly what caused the naked Bybit long the user experienced.
        // If all retries fail, the position is force-removed from open_positions and a
        // critical alert is logged for manual intervention.
        const MAX_CLOSE_RETRIES: u32 = 3;

        let (close_sell_unified, close_buy_unified) = match (buy_exchange, sell_exchange) {
            (Exchange::Binance, Exchange::Bybit) => {
                let (sell_result, buy_result) = tokio::join!(
                    self.binance_client.execute_order_with_fill(
                        &symbol,
                        "SELL",
                        close_buy_qty,
                        true,
                        worst_sell_close
                    ),
                    self.bybit_client.execute_order_with_fill(
                        &symbol,
                        "Buy",
                        close_sell_qty,
                        true,
                        worst_buy_close
                    )
                );
                match (sell_result, buy_result) {
                    (Ok(sf), Ok(bf)) => (close_from_binance(&sf), close_from_bybit(&bf)),

                    // ── Binance SELL OK, Bybit BUY failed → retry Bybit only ──
                    (Ok(sf), Err(first_err)) => {
                        eprintln!("[LiveTrading] ⚠️ PARTIAL CLOSE {}: Binance SELL filled, Bybit BUY failed: {} — retrying Bybit only", coin, first_err);
                        let mut recovered: Option<CloseFill> = None;
                        for attempt in 1..=MAX_CLOSE_RETRIES {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                300 * attempt as u64,
                            ))
                            .await;
                            eprintln!(
                                "[LiveTrading] Retry {}/{}: BUY {} on Bybit",
                                attempt, MAX_CLOSE_RETRIES, coin
                            );
                            match self
                                .bybit_client
                                .execute_order_with_fill(&symbol, "Buy", close_sell_qty, true, None)
                                .await
                            {
                                Ok(bf) => {
                                    recovered = Some(close_from_bybit(&bf));
                                    break;
                                }
                                Err(e) => {
                                    eprintln!("[LiveTrading] Retry {} failed: {}", attempt, e)
                                }
                            }
                        }
                        match recovered {
                            Some(bf) => (close_from_binance(&sf), bf),
                            None => {
                                eprintln!("[LiveTrading] 🚨 CRITICAL: {} — Bybit BUY failed after {} retries. Binance long CLOSED. Bybit short STILL OPEN — CLOSE MANUALLY!", coin, MAX_CLOSE_RETRIES);
                                // Force-remove so the bot doesn't keep trying to re-close Binance
                                self.open_positions.remove(coin);
                                self.last_trade_time.insert(coin.to_string(), Utc::now());
                                return None;
                            }
                        }
                    }

                    // ── Bybit BUY OK, Binance SELL failed → retry Binance only ──
                    (Err(first_err), Ok(bf)) => {
                        eprintln!("[LiveTrading] ⚠️ PARTIAL CLOSE {}: Bybit BUY filled, Binance SELL failed: {} — retrying Binance only", coin, first_err);
                        let mut recovered: Option<CloseFill> = None;
                        for attempt in 1..=MAX_CLOSE_RETRIES {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                300 * attempt as u64,
                            ))
                            .await;
                            eprintln!(
                                "[LiveTrading] Retry {}/{}: SELL {} on Binance",
                                attempt, MAX_CLOSE_RETRIES, coin
                            );
                            match self
                                .binance_client
                                .execute_order_with_fill(&symbol, "SELL", close_buy_qty, true, None)
                                .await
                            {
                                Ok(sf) => {
                                    recovered = Some(close_from_binance(&sf));
                                    break;
                                }
                                Err(e) => {
                                    eprintln!("[LiveTrading] Retry {} failed: {}", attempt, e)
                                }
                            }
                        }
                        match recovered {
                            Some(sf) => (sf, close_from_bybit(&bf)),
                            None => {
                                eprintln!("[LiveTrading] 🚨 CRITICAL: {} — Binance SELL failed after {} retries. Bybit short CLOSED. Binance long STILL OPEN — CLOSE MANUALLY!", coin, MAX_CLOSE_RETRIES);
                                // Force-remove so the bot doesn't loop and create naked Bybit long
                                self.open_positions.remove(coin);
                                self.last_trade_time.insert(coin.to_string(), Utc::now());
                                return None;
                            }
                        }
                    }

                    (Err(e1), Err(e2)) => {
                        eprintln!("[LiveTrading] CLOSE BOTH legs FAILED: SELL={}, BUY={} — will retry next tick", e1, e2);
                        return None;
                    }
                }
            }

            (Exchange::Bybit, Exchange::Binance) => {
                let (sell_result, buy_result) = tokio::join!(
                    self.bybit_client.execute_order_with_fill(
                        &symbol,
                        "Sell",
                        close_buy_qty,
                        true,
                        worst_sell_close
                    ),
                    self.binance_client.execute_order_with_fill(
                        &symbol,
                        "BUY",
                        close_sell_qty,
                        true,
                        worst_buy_close
                    )
                );
                match (sell_result, buy_result) {
                    (Ok(sf), Ok(bf)) => (close_from_bybit(&sf), close_from_binance(&bf)),

                    // ── Bybit SELL OK, Binance BUY failed → retry Binance only ──
                    (Ok(sf), Err(first_err)) => {
                        eprintln!("[LiveTrading] ⚠️ PARTIAL CLOSE {}: Bybit SELL filled, Binance BUY failed: {} — retrying Binance only", coin, first_err);
                        let mut recovered: Option<CloseFill> = None;
                        for attempt in 1..=MAX_CLOSE_RETRIES {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                300 * attempt as u64,
                            ))
                            .await;
                            eprintln!(
                                "[LiveTrading] Retry {}/{}: BUY {} on Binance",
                                attempt, MAX_CLOSE_RETRIES, coin
                            );
                            match self
                                .binance_client
                                .execute_order_with_fill(&symbol, "BUY", close_sell_qty, true, None)
                                .await
                            {
                                Ok(bf) => {
                                    recovered = Some(close_from_binance(&bf));
                                    break;
                                }
                                Err(e) => {
                                    eprintln!("[LiveTrading] Retry {} failed: {}", attempt, e)
                                }
                            }
                        }
                        match recovered {
                            Some(bf) => (close_from_bybit(&sf), bf),
                            None => {
                                eprintln!("[LiveTrading] 🚨 CRITICAL: {} — Binance BUY failed after {} retries. Bybit long CLOSED. Binance short STILL OPEN — CLOSE MANUALLY!", coin, MAX_CLOSE_RETRIES);
                                self.open_positions.remove(coin);
                                self.last_trade_time.insert(coin.to_string(), Utc::now());
                                return None;
                            }
                        }
                    }

                    // ── Binance BUY OK, Bybit SELL failed → retry Bybit only ──
                    (Err(first_err), Ok(bf)) => {
                        eprintln!("[LiveTrading] ⚠️ PARTIAL CLOSE {}: Binance BUY filled, Bybit SELL failed: {} — retrying Bybit only", coin, first_err);
                        let mut recovered: Option<CloseFill> = None;
                        for attempt in 1..=MAX_CLOSE_RETRIES {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                300 * attempt as u64,
                            ))
                            .await;
                            eprintln!(
                                "[LiveTrading] Retry {}/{}: SELL {} on Bybit",
                                attempt, MAX_CLOSE_RETRIES, coin
                            );
                            match self
                                .bybit_client
                                .execute_order_with_fill(&symbol, "Sell", close_buy_qty, true, None)
                                .await
                            {
                                Ok(sf) => {
                                    recovered = Some(close_from_bybit(&sf));
                                    break;
                                }
                                Err(e) => {
                                    eprintln!("[LiveTrading] Retry {} failed: {}", attempt, e)
                                }
                            }
                        }
                        match recovered {
                            Some(sf) => (sf, close_from_binance(&bf)),
                            None => {
                                eprintln!("[LiveTrading] 🚨 CRITICAL: {} — Bybit SELL failed after {} retries. Binance short CLOSED. Bybit long STILL OPEN — CLOSE MANUALLY!", coin, MAX_CLOSE_RETRIES);
                                self.open_positions.remove(coin);
                                self.last_trade_time.insert(coin.to_string(), Utc::now());
                                return None;
                            }
                        }
                    }

                    (Err(e1), Err(e2)) => {
                        eprintln!("[LiveTrading] CLOSE BOTH legs FAILED: SELL={}, BUY={} — will retry next tick", e1, e2);
                        return None;
                    }
                }
            }
            _ => {
                eprintln!("[LiveTrading] Unsupported exchange pair for close");
                return None;
            }
        };

        let close_sell_price = close_sell_unified.avg_price;
        let close_buy_price = close_buy_unified.avg_price;
        let close_sell_commission = close_sell_unified.commission;
        let close_buy_commission = close_buy_unified.commission;
        let close_sell_order_id = close_sell_unified.order_id;
        let close_buy_order_id = close_buy_unified.order_id;

        let total_close_fee = close_sell_commission + close_buy_commission;
        let total_open_fee = position.entry_buy_commission + position.entry_sell_commission;
        let total_all_fees = total_open_fee + total_close_fee;

        // Calculate PnL from real fill prices
        let long_pnl = (close_sell_price - position.entry_buy_price) * position.buy_filled_qty;
        let short_pnl = (position.entry_sell_price - close_buy_price) * position.sell_filled_qty;
        let gross_pnl = long_pnl + short_pnl;
        let net_pnl = gross_pnl - total_all_fees;

        // Update engine stats
        self.trade_count += 1;
        self.total_pnl += net_pnl;
        self.total_fees_paid += total_close_fee;
        self.closed_count += 1;
        if net_pnl > 0.0 {
            self.winning_trades += 1;
        } else {
            self.losing_trades += 1;
        }

        // Remove from open positions
        self.open_positions.remove(coin);
        self.last_trade_time.insert(coin.to_string(), Utc::now());

        // Build CLOSE trade record with all real exchange data
        let record = TradeRecord {
            id: self.trade_count,
            timestamp: Utc::now(),
            coin: coin.to_string(),
            exchange_buy: buy_exchange,
            exchange_sell: sell_exchange,
            trade_type: TradeType::Close,
            buy_order_id: position.buy_order_id.clone(),
            sell_order_id: position.sell_order_id.clone(),
            buy_fill_price: position.entry_buy_price,
            sell_fill_price: position.entry_sell_price,
            buy_filled_qty: position.buy_filled_qty,
            sell_filled_qty: position.sell_filled_qty,
            buy_quote_value: position.buy_quote_value,
            sell_quote_value: position.sell_quote_value,
            buy_commission: position.entry_buy_commission,
            buy_commission_asset: "USDT".to_string(),
            sell_commission: position.entry_sell_commission,
            sell_commission_asset: "USDT".to_string(),
            total_fee: total_all_fees,
            spread_before: position.entry_spread,
            spread_after: current_spread,
            pnl_gross: gross_pnl,
            pnl_net: net_pnl,
            exchange_realized_pnl: 0.0,
            buy_book_bid: position.entry_buy_book_bid,
            buy_book_ask: position.entry_buy_book_ask,
            buy_book_bid_qty: position.entry_buy_book_bid_qty,
            buy_book_ask_qty: position.entry_buy_book_ask_qty,
            sell_book_bid: position.entry_sell_book_bid,
            sell_book_ask: position.entry_sell_book_ask,
            sell_book_bid_qty: position.entry_sell_book_bid_qty,
            sell_book_ask_qty: position.entry_sell_book_ask_qty,
            close_buy_price: Some(close_buy_price),
            close_sell_price: Some(close_sell_price),
            close_buy_commission: Some(close_buy_commission),
            close_sell_commission: Some(close_sell_commission),
            close_buy_order_id: Some(close_buy_order_id),
            close_sell_order_id: Some(close_sell_order_id),
            total_open_fees: Some(total_open_fee),
            total_close_fees: Some(total_close_fee),
            hold_duration_secs: Some(hold_secs),
            entry_spread: Some(position.entry_spread),
            exit_spread: Some(current_spread),
            funding_interval_hours: Some(position.funding_hours),
            latency: None, // close leg latency not tracked (fast path)
            real_account_entry_buy: None,
            real_account_entry_sell: None,
            real_unrealized_pnl: None,
            buy_order_status: Some("FILLED".to_string()),
            sell_order_status: Some("FILLED".to_string()),
        };

        // Save to file
        if let Err(e) = save_trade(&record, &self.log_path) {
            eprintln!(
                "[{}][LiveTrading] Failed to save CLOSE trade: {}",
                Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                e
            );
        }

        // Keep in memory
        self.push_recent_trade(record.clone());

        eprintln!(
            "[{}][LiveTrading] CLOSE #{}: {} | Held {}s | Entry: {:.4}% → Exit: {:.4}% | DynExit: {:.3}% | Gross: ${:.4} | Fees: ${:.4} | Net: ${:.4}",
            Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            self.trade_count, coin, hold_secs,
            position.entry_spread, current_spread, dynamic_exit,
            gross_pnl, total_all_fees, net_pnl
        );

        // Refresh balances after close
        self.refresh_balances().await;

        Some(record)
    }
}

pub type SharedTradingEngine = Arc<Mutex<LiveTradingEngine>>;

pub fn new_trading_engine(
    binance_client: BinanceClient,
    bybit_client: BybitClient,
    log_path: String,
    exchange_info: ExchangeInfoCache,
) -> SharedTradingEngine {
    Arc::new(Mutex::new(LiveTradingEngine::new(
        binance_client,
        bybit_client,
        log_path,
        exchange_info,
    )))
}

/// Candidate for opening a new position — collected outside the lock.
struct OpenCandidate {
    coin: String,
    buy_ex: Exchange,
    sell_ex: Exchange,
    buy_book: OrderBookEntry,
    sell_book: OrderBookEntry,
    buy_price: f64,
    sell_price: f64,
    spread: f64,
    dynamic_entry: f64,
    spread_velocity: f64,
    /// Latency profiler pre-populated with book_update_ms and detected_ms at scan time.
    latency: TradeLatency,
}

/// Background task that scans for arbitrage opportunities and manages positions.
/// Same logic as paper trading but executes REAL orders.
/// BUG-5 FIX: Batch mutex access — collect candidates first, lock once for execution.
pub async fn run_trading_loop(
    store: PriceStore,
    status: crate::price_store::SharedStatus,
    engine: SharedTradingEngine,
    funding_store: FundingStore,
) {
    use crate::price_store::fresh_prices;
    use std::sync::atomic::Ordering;

    // Reduced from 100ms to 2ms for near-instant reaction to spreads (<5ms latency target)
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(2));

    // Dynamic Spread-Threshold Config and Stats Maps
    let dynamic_config = crate::config::DynamicSpreadConfig::default();
    let startup_time_ms = chrono::Utc::now().timestamp_millis() as u64;
    let mut stats_dir_a: std::collections::HashMap<String, crate::stats::RollingStats> =
        std::collections::HashMap::new();
    let mut stats_dir_b: std::collections::HashMap<String, crate::stats::RollingStats> =
        std::collections::HashMap::new();

    // Keep HTTP connection pool warm by pinging REST endpoints every 10 seconds
    let engine_for_keepalive = engine.clone();
    tokio::spawn(async move {
        let mut keepalive_interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            keepalive_interval.tick().await;
            let eng = engine_for_keepalive.lock().await;
            eng.ping_keepalives().await;
        }
    });

    // Refresh balances on startup
    {
        let mut eng = engine.lock().await;
        eng.refresh_balances().await;
        let bin_bal = eng.binance_balance;
        let byb_bal = eng.bybit_balance;
        // Log startup balances vs. fixed 10x leverage sizing
        eprintln!(
            "[LiveTrading] Balances — Binance: ${:.2} | Bybit: ${:.2} | Fixed Leverage: {}x | Trade Size: ${:.0}",
            bin_bal, byb_bal, FIXED_LEVERAGE, TRADE_SIZE_USDT
        );
    }

    // Periodic balance refresh counter
    let mut loop_count: u64 = 0;

    loop {
        interval.tick().await;
        loop_count += 1;

        let trading_on = status.trading_enabled.load(Ordering::Relaxed);
        let bin_e = status.binance_enabled.load(Ordering::Relaxed);
        let byb_e = status.bybit_enabled.load(Ordering::Relaxed);



        // Refresh balances every ~60 seconds (600 iterations * 100ms)
        if loop_count % 600 == 0 {
            let mut eng = engine.lock().await;
            eng.refresh_balances().await;
        }

        // ── Pass 1: Collect open positions info outside the lock, then process closes ──
        let open_coins: Vec<(String, Exchange, Exchange)> = {
            let eng = engine.lock().await;
            eng.open_positions
                .iter()
                .map(|(coin, pos)| (coin.clone(), pos.buy_exchange, pos.sell_exchange))
                .collect()
        };

        // Collect close candidates
        let mut close_candidates: Vec<(String, f64, f64)> = Vec::new();
        let mut force_close_candidates: Vec<String> = Vec::new();

        for (coin, buy_ex, sell_ex) in &open_coins {
            let prices = match store.get(coin) {
                Some(entry) => entry.value().clone(),
                None => continue,
            };

            let (bin_p, byb_p) = fresh_prices(&prices);
            let bin_f = if bin_e { bin_p } else { None };
            let byb_f = if byb_e { byb_p } else { None };

            let buy_ex_price = match buy_ex {
                Exchange::Binance => bin_f,
                Exchange::Bybit => byb_f,
            };
            let sell_ex_price = match sell_ex {
                Exchange::Binance => bin_f,
                Exchange::Bybit => byb_f,
            };

            // Grab the open position to check its age and entry spread
            let pos_opt = {
                let eng = engine.lock().await;
                eng.open_positions.get(coin).cloned()
            };

            if let Some(pos) = pos_opt {
                let hold_secs = Utc::now()
                    .signed_duration_since(pos.open_time)
                    .num_seconds();

                if let (Some(bp), Some(sp)) = (buy_ex_price, sell_ex_price) {
                    if bp > 0.0 && sp > 0.0 {
                        // Original exit spread calculation for the position
                        let current_spread = ((sp - bp) / bp) * 100.0;

                        // Dynamic Exit Phase 6
                        let current_time_ms = chrono::Utc::now().timestamp_millis() as u64;
                        let open_time_ms = pos.open_time.timestamp_millis() as u64;

                        let bin_ask = match prices.binance_book.best_ask {
                            Some(a) if a > 0.0 => a,
                            _ => continue,
                        };
                        let bin_bid = match prices.binance_book.best_bid {
                            Some(b) if b > 0.0 => b,
                            _ => continue,
                        };
                        let byb_ask = match prices.bybit_book.best_ask {
                            Some(a) if a > 0.0 => a,
                            _ => continue,
                        };
                        let byb_bid = match prices.bybit_book.best_bid {
                            Some(b) if b > 0.0 => b,
                            _ => continue,
                        };

                        let bin_asks = [crate::stats::OrderBookLevel {
                            price: bin_ask,
                            qty: prices.binance_book.best_ask_qty.unwrap_or(0.0),
                        }];
                        let bin_bids = [crate::stats::OrderBookLevel {
                            price: bin_bid,
                            qty: prices.binance_book.best_bid_qty.unwrap_or(0.0),
                        }];
                        let byb_asks = [crate::stats::OrderBookLevel {
                            price: byb_ask,
                            qty: prices.bybit_book.best_ask_qty.unwrap_or(0.0),
                        }];
                        let byb_bids = [crate::stats::OrderBookLevel {
                            price: byb_bid,
                            qty: prices.bybit_book.best_bid_qty.unwrap_or(0.0),
                        }];

                        let (should_close, dynamic_exit) = if *buy_ex == Exchange::Binance {
                            // Opened on Dir A (Bought Binance, Sold Bybit)
                            // Exit path uses Dir A stats: compute effective spread (sell Binance, buy Bybit)
                            let target_qty = TRADE_SIZE_USDT / bin_bid; // roughly base quantity
                            if let Ok(exit_spread) = crate::stats::compute_effective_spread(
                                &bin_bids,
                                &byb_asks,
                                target_qty,
                                dynamic_config.max_depth_levels,
                                dynamic_config.max_slippage_pct,
                            ) {
                                if let Some(stats_a) = stats_dir_a.get_mut(coin) {
                                    (
                                        stats_a.evaluate_exit(
                                            exit_spread,
                                            current_time_ms,
                                            open_time_ms,
                                        ),
                                        stats_a.get_exit_threshold(current_time_ms).unwrap_or(0.0),
                                    )
                                } else {
                                    (false, 0.0)
                                }
                            } else {
                                (false, 0.0)
                            }
                        } else {
                            // Opened on Dir B (Bought Bybit, Sold Binance)
                            // Exit path uses Dir B stats: compute effective spread (sell Bybit, buy Binance)
                            let target_qty = TRADE_SIZE_USDT / byb_bid;
                            if let Ok(exit_spread) = crate::stats::compute_effective_spread(
                                &byb_bids,
                                &bin_asks,
                                target_qty,
                                dynamic_config.max_depth_levels,
                                dynamic_config.max_slippage_pct,
                            ) {
                                if let Some(stats_b) = stats_dir_b.get_mut(coin) {
                                    (
                                        stats_b.evaluate_exit(
                                            exit_spread,
                                            current_time_ms,
                                            open_time_ms,
                                        ),
                                        stats_b.get_exit_threshold(current_time_ms).unwrap_or(0.0),
                                    )
                                } else {
                                    (false, 0.0)
                                }
                            } else {
                                (false, 0.0)
                            }
                        };

                        if should_close {
                            close_candidates.push((coin.clone(), current_spread, dynamic_exit));
                            let timeout_exceeded = hold_secs
                                >= (dynamic_config.max_arbitrage_hold_time_ms as i64 / 1000);
                            if timeout_exceeded {
                                eprintln!("[LiveTrading] MAX HOLD EXCEEDED: {} held for {}s (spread {:.3}%)", coin, hold_secs, current_spread);
                            }
                        }
                    }
                }

                // ── FUNDING PAUSE: Force close if near funding time ──
                if funding::is_near_funding(coin, &funding_store) {
                    if buy_ex_price.is_some() && sell_ex_price.is_some() {
                        force_close_candidates.push(coin.clone());
                    }
                }
            }
        }

        // Execute closes (single lock acquisition for all close operations)
        if !close_candidates.is_empty() || !force_close_candidates.is_empty() {
            let mut eng = engine.lock().await;
            for (coin, spread, dynamic_exit) in &close_candidates {
                eng.try_close_position(coin, *spread, *dynamic_exit, &store)
                    .await;
            }
            for coin in &force_close_candidates {
                if eng.open_positions.contains_key(coin.as_str()) {
                    if let Some(pos) = eng.open_positions.get(coin.as_str()).cloned() {
                        let hold_secs = Utc::now()
                            .signed_duration_since(pos.open_time)
                            .num_seconds();
                        if hold_secs >= 10 {
                            eprintln!(
                                "[LiveTrading] FUNDING PAUSE: Force closing {} position",
                                coin
                            );
                            eng.try_close_position(coin, -999.0, 0.0, &store).await;
                        }
                    }
                }
            }
        }

        // ── Pass 2: Scan all coins for new OPEN signals — collect candidates outside lock ──
        let mut open_candidates: Vec<OpenCandidate> = Vec::new();

        for entry in store.iter() {
            let coin = entry.key().clone();
            let prices = entry.value().clone();

            if !bin_e || !byb_e {
                continue;
            }

            // Freshness check for order books on both exchanges
            let bin_fresh = prices
                .binance_book_updated
                .map(|ts| ts.elapsed().as_millis() <= MAX_BOOK_AGE_MILLIS)
                .unwrap_or(false);
            let byb_fresh = prices
                .bybit_book_updated
                .map(|ts| ts.elapsed().as_millis() <= MAX_BOOK_AGE_MILLIS)
                .unwrap_or(false);
            if !bin_fresh || !byb_fresh {
                continue;
            }

            let bin_ask = match prices.binance_book.best_ask {
                Some(a) if a > 0.0 => a,
                _ => continue,
            };
            let bin_bid = match prices.binance_book.best_bid {
                Some(b) if b > 0.0 => b,
                _ => continue,
            };
            let byb_ask = match prices.bybit_book.best_ask {
                Some(a) if a > 0.0 => a,
                _ => continue,
            };
            let byb_bid = match prices.bybit_book.best_bid {
                Some(b) if b > 0.0 => b,
                _ => continue,
            };

            // Dynamic Threshold & Stats Engine Path (Phase 5)
            let current_time_ms = chrono::Utc::now().timestamp_millis() as u64;

            let bin_ask_qty = prices.binance_book.best_ask_qty.unwrap_or(0.0);
            let bin_bid_qty = prices.binance_book.best_bid_qty.unwrap_or(0.0);
            let byb_ask_qty = prices.bybit_book.best_ask_qty.unwrap_or(0.0);
            let byb_bid_qty = prices.bybit_book.best_bid_qty.unwrap_or(0.0);

            let bin_asks = [crate::stats::OrderBookLevel {
                price: bin_ask,
                qty: bin_ask_qty,
            }];
            let bin_bids = [crate::stats::OrderBookLevel {
                price: bin_bid,
                qty: bin_bid_qty,
            }];
            let byb_asks = [crate::stats::OrderBookLevel {
                price: byb_ask,
                qty: byb_ask_qty,
            }];
            let byb_bids = [crate::stats::OrderBookLevel {
                price: byb_bid,
                qty: byb_bid_qty,
            }];

            // Direction A: Buy Binance, Sell Bybit
            let target_qty_a = TRADE_SIZE_USDT / bin_ask;
            let mut candidate_opt = None;
            let mut reject_a = None;
            let mut eff_spread_a = 0.0;

            let stats_a = stats_dir_a.entry(coin.clone()).or_insert_with(|| {
                crate::stats::RollingStats::new(dynamic_config.clone(), startup_time_ms)
            });

            match crate::stats::compute_effective_spread(
                &bin_asks,
                &byb_bids,
                target_qty_a,
                dynamic_config.max_depth_levels,
                dynamic_config.max_slippage_pct,
            ) {
                Ok(effective_spread_a) => {
                    eff_spread_a = effective_spread_a;
                    stats_a.update(effective_spread_a, current_time_ms);
                    
                    if effective_spread_a <= MAX_SPREAD_THRESHOLD {
                        if let Err(e) = stats_a.evaluate_entry(effective_spread_a, current_time_ms) {
                            reject_a = Some(e);
                        } else {
                            candidate_opt = Some((
                                Exchange::Binance,
                                Exchange::Bybit,
                                bin_ask,
                                byb_bid,
                                effective_spread_a,
                                stats_a.get_entry_threshold(current_time_ms).unwrap_or(0.0),
                                stats_a.spread_velocity,
                            ));
                        }
                    } else {
                        reject_a = Some(crate::rejection::RejectionReason::RISK_LIMIT);
                    }
                }
                Err(_) => {
                    reject_a = Some(crate::rejection::RejectionReason::INSUFFICIENT_LIQUIDITY);
                }
            }

            crate::opportunity_logger::log_opportunity(&crate::opportunity_logger::OpportunityRecord {
                timestamp: chrono::Utc::now(),
                symbol: coin.clone(),
                direction: "Binance->Bybit".to_string(),
                raw_bid: bin_bid,
                raw_ask: bin_ask,
                effective_buy_price: bin_ask,
                effective_sell_price: byb_bid,
                gross_spread_pct: ((byb_bid - bin_ask) / bin_ask) * 100.0,
                effective_spread_pct: eff_spread_a,
                baseline_spread_pct: stats_a.rolling_median,
                spread_volatility_pct: stats_a.get_spread_volatility(),
                dynamic_entry_threshold_pct: stats_a.get_entry_threshold(current_time_ms).unwrap_or(0.0),
                dynamic_exit_threshold_pct: stats_a.get_exit_threshold(current_time_ms).unwrap_or(0.0),
                z_score: if stats_a.get_spread_volatility() > 0.0 { (eff_spread_a - stats_a.rolling_median) / stats_a.get_spread_volatility() } else { 0.0 },
                spread_velocity: stats_a.spread_velocity,
                expected_slippage_pct: 0.0,
                buy_fee_pct: 0.0,
                sell_fee_pct: 0.0,
                net_edge_pct: eff_spread_a,
                signal_age_ms: 0,
                liquidity_ok: reject_a != Some(crate::rejection::RejectionReason::INSUFFICIENT_LIQUIDITY),
                data_fresh_ok: bin_fresh && byb_fresh,
                entry_decision: candidate_opt.is_some(),
                reject_reason: reject_a.clone(),
                exit_trigger_condition: None,
            });

            // Direction B: Buy Bybit, Sell Binance
            if candidate_opt.is_none() {
                let target_qty_b = TRADE_SIZE_USDT / byb_ask;
                let mut reject_b = None;
                let mut eff_spread_b = 0.0;
                let stats_b = stats_dir_b.entry(coin.clone()).or_insert_with(|| {
                    crate::stats::RollingStats::new(dynamic_config.clone(), startup_time_ms)
                });

                match crate::stats::compute_effective_spread(
                    &byb_asks,
                    &bin_bids,
                    target_qty_b,
                    dynamic_config.max_depth_levels,
                    dynamic_config.max_slippage_pct,
                ) {
                    Ok(effective_spread_b) => {
                        eff_spread_b = effective_spread_b;
                        stats_b.update(effective_spread_b, current_time_ms);
                        
                        if effective_spread_b <= MAX_SPREAD_THRESHOLD {
                            if let Err(e) = stats_b.evaluate_entry(effective_spread_b, current_time_ms) {
                                reject_b = Some(e);
                            } else {
                                candidate_opt = Some((
                                    Exchange::Bybit,
                                    Exchange::Binance,
                                    byb_ask,
                                    bin_bid,
                                    effective_spread_b,
                                    stats_b.get_entry_threshold(current_time_ms).unwrap_or(0.0),
                                    stats_b.spread_velocity,
                                ));
                            }
                        } else {
                            reject_b = Some(crate::rejection::RejectionReason::RISK_LIMIT);
                        }
                    }
                    Err(_) => {
                        reject_b = Some(crate::rejection::RejectionReason::INSUFFICIENT_LIQUIDITY);
                    }
                }

                crate::opportunity_logger::log_opportunity(&crate::opportunity_logger::OpportunityRecord {
                    timestamp: chrono::Utc::now(),
                    symbol: coin.clone(),
                    direction: "Bybit->Binance".to_string(),
                    raw_bid: byb_bid,
                    raw_ask: byb_ask,
                    effective_buy_price: byb_ask,
                    effective_sell_price: bin_bid,
                    gross_spread_pct: ((bin_bid - byb_ask) / byb_ask) * 100.0,
                    effective_spread_pct: eff_spread_b,
                    baseline_spread_pct: stats_b.rolling_median,
                    spread_volatility_pct: stats_b.get_spread_volatility(),
                    dynamic_entry_threshold_pct: stats_b.get_entry_threshold(current_time_ms).unwrap_or(0.0),
                    dynamic_exit_threshold_pct: stats_b.get_exit_threshold(current_time_ms).unwrap_or(0.0),
                    z_score: if stats_b.get_spread_volatility() > 0.0 { (eff_spread_b - stats_b.rolling_median) / stats_b.get_spread_volatility() } else { 0.0 },
                    spread_velocity: stats_b.spread_velocity,
                    expected_slippage_pct: 0.0,
                    buy_fee_pct: 0.0,
                    sell_fee_pct: 0.0,
                    net_edge_pct: eff_spread_b,
                    signal_age_ms: 0,
                    liquidity_ok: reject_b != Some(crate::rejection::RejectionReason::INSUFFICIENT_LIQUIDITY),
                    data_fresh_ok: bin_fresh && byb_fresh,
                    entry_decision: candidate_opt.is_some(),
                    reject_reason: reject_b.clone(),
                    exit_trigger_condition: None,
                });
            }

            if let Some((buy_ex, sell_ex, buy_price_val, sell_price_val, s, dyn_entry, vel)) =
                candidate_opt
            {
                let buy_book = get_order_book(&prices, buy_ex).clone();
                let sell_book = get_order_book(&prices, sell_ex).clone();

                // Build latency profiler — capture book update epoch ms and detection time now
                let book_epoch_ms = match buy_ex {
                    Exchange::Binance => prices.binance_book_epoch_ms,
                    Exchange::Bybit => prices.bybit_book_epoch_ms,
                };
                let mut lat = TradeLatency::new(book_epoch_ms);
                lat.mark_detected(buy_price_val, sell_price_val, s);

                open_candidates.push(OpenCandidate {
                    coin,
                    buy_ex,
                    sell_ex,
                    buy_book,
                    sell_book,
                    buy_price: buy_price_val,
                    sell_price: sell_price_val,
                    spread: s,
                    dynamic_entry: dyn_entry,
                    spread_velocity: vel,
                    latency: lat,
                });
            }
        }

        // Execute opens (single lock acquisition for all open candidates)
        if !open_candidates.is_empty() {
            // Sort by spread descending — prioritize the best opportunity
            open_candidates.sort_by(|a, b| {
                b.spread
                    .partial_cmp(&a.spread)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let mut eng = engine.lock().await;
            for candidate in &open_candidates {
                let cur_limit = status.trade_limit.load(Ordering::Relaxed);
                let cur_taken = status.session_trades_taken.load(Ordering::Relaxed);
                if cur_limit > 0 && cur_taken >= cur_limit {
                    eng.log_missed(
                        &candidate.coin,
                        candidate.buy_ex,
                        candidate.sell_ex,
                        candidate.buy_price,
                        candidate.sell_price,
                        candidate.spread,
                        None,
                        format!(
                            "SESSION_LIMIT_REACHED: Session limit of {} trades reached ({}/{})",
                            cur_limit, cur_taken, cur_limit
                        ),
                    );
                    continue;
                }

                if !trading_on {
                    eng.log_missed(
                        &candidate.coin,
                        candidate.buy_ex,
                        candidate.sell_ex,
                        candidate.buy_price,
                        candidate.sell_price,
                        candidate.spread,
                        None,
                        "TRADING_PAUSED: Trading is disabled in TUI (press T to enable)".to_string(),
                    );
                    continue;
                }

                // Stop if max concurrent positions reached
                if eng.open_positions.len() >= MAX_OPEN_POSITIONS {
                    let holding = eng
                        .open_positions
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ");
                    eng.log_missed(
                        &candidate.coin,
                        candidate.buy_ex,
                        candidate.sell_ex,
                        candidate.buy_price,
                        candidate.sell_price,
                        candidate.spread,
                        None,
                        format!(
                            "MAX_POSITIONS_REACHED: Currently {}/{} open positions (holding: {})",
                            eng.open_positions.len(),
                            MAX_OPEN_POSITIONS,
                            holding
                        ),
                    );
                    continue;
                }

                let opened = eng
                    .try_open_position(
                        &candidate.coin,
                        candidate.buy_ex,
                        candidate.sell_ex,
                        &candidate.buy_book,
                        &candidate.sell_book,
                        candidate.buy_price,
                        candidate.sell_price,
                        candidate.spread,
                        candidate.dynamic_entry,
                        candidate.spread_velocity,
                        &funding_store,
                        &store,
                        candidate.latency.clone(),
                    )
                    .await;

                if opened {
                    let new_taken = status.session_trades_taken.fetch_add(1, Ordering::Relaxed) + 1;
                    if cur_limit > 0 && new_taken >= cur_limit {
                        eprintln!(
                            "[LiveTrading] 🛑 Session trade limit reached ({}/{}). No more trades will be opened.",
                            new_taken, cur_limit
                        );
                    }
                }
            }
        }
    }
}
