use crate::funding;
use crate::live_trading::SharedTradingEngine;
use crate::price_store::{compute_spread_from_fresh, fresh_prices, PriceStore, SharedStatus};
use crossterm::{
    cursor,
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    terminal, ExecutableCommand,
};
use std::fmt::Write as FmtWrite;
use std::io::{self, Write};
use std::sync::atomic::Ordering;
use std::time::Duration;

const COL_COIN: usize = 20;
const COL_PRICE: usize = 18;
const COL_SPREAD: usize = 12;

#[inline]
fn write_price_cell(buf: &mut String, p: Option<f64>, is_min: bool, is_max: bool) {
    match p {
        None => {
            let _ = write!(buf, "\x1b[90m│\x1b[90m{:<w$}\x1b[0m", "—", w = COL_PRICE);
        }
        Some(v) => {
            let color = if is_min {
                "32"
            } else if is_max {
                "31"
            } else {
                "37"
            };
            if v >= 1000.0 {
                let _ = write!(
                    buf,
                    "\x1b[90m│\x1b[{}m{:<w$.2}\x1b[0m",
                    color,
                    v,
                    w = COL_PRICE
                );
            } else if v >= 1.0 {
                let _ = write!(
                    buf,
                    "\x1b[90m│\x1b[{}m{:<w$.4}\x1b[0m",
                    color,
                    v,
                    w = COL_PRICE
                );
            } else if v >= 0.01 {
                let _ = write!(
                    buf,
                    "\x1b[90m│\x1b[{}m{:<w$.6}\x1b[0m",
                    color,
                    v,
                    w = COL_PRICE
                );
            } else if v >= 0.0001 {
                let _ = write!(
                    buf,
                    "\x1b[90m│\x1b[{}m{:<w$.8}\x1b[0m",
                    color,
                    v,
                    w = COL_PRICE
                );
            } else {
                let _ = write!(
                    buf,
                    "\x1b[90m│\x1b[{}m{:<w$.10}\x1b[0m",
                    color,
                    v,
                    w = COL_PRICE
                );
            }
        }
    }
}

#[inline]
fn write_spread_cell(buf: &mut String, spread: Option<f64>) {
    match spread {
        None => {
            let _ = write!(buf, "\x1b[90m│\x1b[90m{:<w$}\x1b[0m", "N/A", w = COL_SPREAD);
        }
        Some(s) => {
            let color = if s >= crate::config::ENTRY_SPREAD_THRESHOLD {
                "33;1"
            } else if s >= 0.3 {
                "33"
            } else if s >= 0.1 {
                "93"
            } else {
                "37"
            };
            let s_text = format!("{:.4}%", s);
            let _ = write!(
                buf,
                "\x1b[90m│\x1b[{}m{:<w$}\x1b[0m",
                color,
                s_text,
                w = COL_SPREAD
            );
        }
    }
}

#[inline]
fn format_price(v: f64) -> String {
    if v >= 1000.0 {
        format!("{:.2}", v)
    } else if v >= 1.0 {
        format!("{:.4}", v)
    } else if v >= 0.01 {
        format!("{:.6}", v)
    } else if v >= 0.0001 {
        format!("{:.8}", v)
    } else {
        format!("{:.10}", v)
    }
}

#[inline]
fn min_max_indices(binance: Option<f64>, bybit: Option<f64>) -> (Option<usize>, Option<usize>) {
    match (binance, bybit) {
        (Some(a), Some(b)) if a < b => (Some(0), Some(1)),
        (Some(a), Some(b)) if b < a => (Some(1), Some(0)),
        _ => (None, None),
    }
}

struct TableRow {
    coin: String,
    binance: Option<f64>,
    bybit: Option<f64>,
    spread: Option<f64>,
    has_open_position: bool,
    funding_hours: u32,
    near_funding: bool,
}

struct PositionDetail {
    coin: String,
    buy_exchange: String,
    sell_exchange: String,
    entry_buy_price: f64,
    entry_sell_price: f64,
    buy_qty: f64,
    sell_qty: f64,
    entry_spread: f64,
    hold_secs: i64,
    unrealized_pnl: f64,
}

#[inline]
fn status_indicator(connected: bool, enabled: bool, updates: u64) -> String {
    if !enabled {
        "⚪OFF".to_string()
    } else if connected {
        format!("🟢{}", updates)
    } else {
        "🔴".to_string()
    }
}

/// Run the flicker-free terminal table renderer for live trading.
pub async fn run(
    store: PriceStore,
    status: SharedStatus,
    engine: SharedTradingEngine,
    funding_store: crate::price_store::FundingStore,
) {
    let mut stdout = io::stdout();

    let _ = stdout.execute(terminal::EnterAlternateScreen);
    let _ = stdout.execute(terminal::Clear(terminal::ClearType::All));
    let _ = stdout.execute(cursor::Hide);
    let _ = terminal::enable_raw_mode();
    let _ = write!(stdout, "\x1b[2J\x1b[H\x1b[?7l");
    let _ = stdout.flush();

    let mut scroll_offset: usize = 0;
    let mut buf = String::with_capacity(32768);
    let mut rows: Vec<TableRow> = Vec::with_capacity(512);
    let mut search_query = String::new();
    let mut search_mode = false;
    let mut last_input_time = std::time::Instant::now();
    // Cached live balances — updated every 500ms to avoid per-frame async lock contention
    let mut cached_bin_bal: f64 = 0.0;
    let mut cached_byb_bal: f64 = 0.0;
    let mut last_balance_refresh = std::time::Instant::now();
    // Render interval: 800ms (~1.2 FPS) prevents terminal buffer bloat on remote servers
    // while keeping screen readable. Keystrokes are polled every 30ms for instant response.
    let render_interval_ms: u64 = 800;
    let mut last_render_time = std::time::Instant::now() - Duration::from_secs(5); // render 1st frame immediately

    loop {
        let mut key_pressed = false;

        let key_event = tokio::task::spawn_blocking(|| {
            if event::poll(Duration::from_millis(30)).unwrap_or(false) {
                if let Ok(Event::Key(k)) = event::read() {
                    return Some(k);
                }
            }
            None
        })
        .await
        .unwrap_or(None);

        if let Some(KeyEvent {
            code,
            modifiers,
            kind,
            ..
        }) = key_event
        {
            if kind == crossterm::event::KeyEventKind::Press
                || kind == crossterm::event::KeyEventKind::Repeat
            {
                last_input_time = std::time::Instant::now();
                key_pressed = true; // force immediate screen update on keystroke

                if search_mode {
                    match code {
                        KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => break,
                        KeyCode::Esc => {
                            search_mode = false;
                            search_query.clear();
                            scroll_offset = 0;
                        }
                        KeyCode::Enter => {
                            search_mode = false;
                            scroll_offset = 0;
                        }
                        KeyCode::Backspace => {
                            search_query.pop();
                            scroll_offset = 0;
                        }
                        KeyCode::Char(c) => {
                            search_query.push(c.to_ascii_uppercase());
                            scroll_offset = 0;
                        }
                        _ => {}
                    }
                } else {
                    match code {
                        KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => break,
                        KeyCode::Char('q') => break,
                        KeyCode::Char('/') => {
                            search_mode = true;
                            search_query.clear();
                        }
                        KeyCode::Esc => {
                            search_query.clear();
                            scroll_offset = 0;
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            scroll_offset = scroll_offset.saturating_sub(1)
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            scroll_offset = scroll_offset.saturating_add(1)
                        }
                        KeyCode::PageUp => scroll_offset = scroll_offset.saturating_sub(20),
                        KeyCode::PageDown => scroll_offset = scroll_offset.saturating_add(20),
                        KeyCode::Home => scroll_offset = 0,
                        KeyCode::End => scroll_offset = usize::MAX,
                        KeyCode::Char('1') => {
                            let curr = status.binance_enabled.load(Ordering::Relaxed);
                            status.binance_enabled.store(!curr, Ordering::Relaxed);
                        }
                        KeyCode::Char('2') => {
                            let curr = status.bybit_enabled.load(Ordering::Relaxed);
                            status.bybit_enabled.store(!curr, Ordering::Relaxed);
                        }
                        // T key: Toggle LIVE trading on/off
                        KeyCode::Char('t') | KeyCode::Char('T') => {
                            let curr = status.trading_enabled.load(Ordering::Relaxed);
                            status.trading_enabled.store(!curr, Ordering::Relaxed);
                        }
                        // R key: Reset emergency halt flag
                        KeyCode::Char('r') | KeyCode::Char('R') => {
                            if let Ok(mut eng) = engine.try_lock() {
                                if eng.emergency_halt {
                                    eng.emergency_halt = false;
                                    eprintln!("[Renderer] Emergency halt cleared by user.");
                                }
                            }
                        }
                        // L key: Cycle trade limit: 1 -> 2 -> 3 -> 5 -> 10 -> 0 (Unlimited) -> 1
                        KeyCode::Char('l') | KeyCode::Char('L') => {
                            let curr = status.trade_limit.load(Ordering::Relaxed);
                            let (next, reset_counter) = match curr {
                                1 => (2, false),
                                2 => (3, false),
                                3 => (5, false),
                                5 => (10, false),
                                10 => (0, false),
                                _ => (1, true),
                            };
                            status.trade_limit.store(next, Ordering::Relaxed);
                            if reset_counter {
                                status.session_trades_taken.store(0, Ordering::Relaxed);
                            }
                        }
                        _ => {}
                    }
                }
            }
        } else if !search_mode
            && scroll_offset > 0
            && last_input_time.elapsed() > Duration::from_secs(10)
        {
            scroll_offset = 0;
        }

        // Throttle full screen repaints to 800ms unless a key was pressed.
        // This avoids clogging the terminal buffer over remote SSH/Web consoles.
        if !key_pressed && last_render_time.elapsed() < Duration::from_millis(render_interval_ms) {
            continue;
        }
        last_render_time = std::time::Instant::now();

        let bin_e = status.binance_enabled.load(Ordering::Relaxed);
        let byb_e = status.bybit_enabled.load(Ordering::Relaxed);
        let trading_on = status.trading_enabled.load(Ordering::Relaxed);

        // Collect rows
        rows.clear();
        for entry in store.iter() {
            let key = entry.key();

            if !search_query.is_empty() && !key.contains(&search_query) {
                continue;
            }

            let mut bin_val = entry.value().binance;
            let mut byb_val = entry.value().bybit;
            if !bin_e {
                bin_val = None;
            }
            if !byb_e {
                byb_val = None;
            }

            // Only skip if BOTH exchanges have never received a price
            if bin_val.is_none() && byb_val.is_none() {
                continue;
            }

            let spread = compute_spread_from_fresh(bin_val, byb_val);

            // BUG FIX: Show open position marker regardless of trading_on.
            // A position is real even if trading is paused (T was toggled OFF after opening).
            let has_open = engine
                .try_lock()
                .map(|eng| eng.open_positions.contains_key(key.as_str()))
                .unwrap_or(false);

            let funding_hours = funding_store
                .get(key.as_str())
                .map(|r| *r.value())
                .unwrap_or(8);
            let near_funding = funding::is_near_funding(key.as_str(), &funding_store);

            rows.push(TableRow {
                coin: key.clone(),
                binance: bin_val,
                bybit: byb_val,
                spread,
                has_open_position: has_open,
                funding_hours,
                near_funding,
            });
        }

        // Sort: always keep open positions at the top, then coarse bucketing (0.25% steps).
        // Coarse bucketing prevents minor price noise from constantly swapping rows (UI juggling).
        rows.sort_by(|a, b| {
            match b.has_open_position.cmp(&a.has_open_position) {
                std::cmp::Ordering::Equal => {
                    // Round to nearest 0.25% bucket (by multiplying by 4.0)
                    let sa = (a.spread.unwrap_or(-1.0) * 4.0).round() as i64;
                    let sb = (b.spread.unwrap_or(-1.0) * 4.0).round() as i64;
                    match sb.cmp(&sa) {
                        std::cmp::Ordering::Equal => a.coin.cmp(&b.coin), // stable: alphabetical tie-break
                        other => other,
                    }
                }
                other => other,
            }
        });

        // ── Collect open position details for the detail panel ──
        let position_details: Vec<PositionDetail> = if let Ok(eng) = engine.try_lock() {
            eng.open_positions
                .iter()
                .map(|(coin, pos)| {
                    let hold_secs = chrono::Utc::now()
                        .signed_duration_since(pos.open_time)
                        .num_seconds();
                    // Compute unrealized PnL using current price from price store
                    let (cur_buy, cur_sell) = if let Some(prices) = store.get(coin) {
                        let (bin_p, byb_p) = fresh_prices(prices.value());
                        let bp = match pos.buy_exchange {
                            crate::price_store::Exchange::Binance => bin_p,
                            crate::price_store::Exchange::Bybit => byb_p,
                        };
                        let sp = match pos.sell_exchange {
                            crate::price_store::Exchange::Binance => bin_p,
                            crate::price_store::Exchange::Bybit => byb_p,
                        };
                        (
                            bp.unwrap_or(pos.entry_buy_price),
                            sp.unwrap_or(pos.entry_sell_price),
                        )
                    } else {
                        (pos.entry_buy_price, pos.entry_sell_price)
                    };
                    let long_pnl = (cur_buy - pos.entry_buy_price) * pos.buy_filled_qty;
                    let short_pnl = (pos.entry_sell_price - cur_sell) * pos.sell_filled_qty;
                    let unrealized_pnl =
                        long_pnl + short_pnl - pos.entry_buy_commission - pos.entry_sell_commission;
                    PositionDetail {
                        coin: coin.clone(),
                        buy_exchange: format!("{}", pos.buy_exchange),
                        sell_exchange: format!("{}", pos.sell_exchange),
                        entry_buy_price: pos.entry_buy_price,
                        entry_sell_price: pos.entry_sell_price,
                        buy_qty: pos.buy_filled_qty,
                        sell_qty: pos.sell_filled_qty,
                        entry_spread: pos.entry_spread,
                        hold_secs,
                        unrealized_pnl,
                    }
                })
                .collect()
        } else {
            vec![]
        };
        let pos_panel_lines = if position_details.is_empty() {
            0
        } else {
            position_details.len() + 2
        };

        let (_term_width, term_height) = terminal::size().unwrap_or((120, 30));
        let header_lines: usize = 4 + pos_panel_lines;
        let footer_lines: usize = 2;
        let available_rows = (term_height as usize).saturating_sub(header_lines + footer_lines);
        let total_rows = rows.len();

        if total_rows > available_rows {
            let max_off = total_rows.saturating_sub(available_rows);
            if scroll_offset > max_off {
                scroll_offset = max_off;
            }
        } else {
            scroll_offset = 0;
        }

        buf.clear();
        let _ = write!(buf, "\x1b[H");

        let mut row_idx = 1;

        // ── Status bar ──
        let bin_s = status_indicator(
            status.binance_connected.load(Ordering::Relaxed),
            bin_e,
            status.binance_updates.load(Ordering::Relaxed),
        );
        let byb_s = status_indicator(
            status.bybit_connected.load(Ordering::Relaxed),
            byb_e,
            status.bybit_updates.load(Ordering::Relaxed),
        );

        let search_display = if !search_query.is_empty() {
            format!("│ 🔍 {}", search_query)
        } else {
            String::new()
        };

        let trade_limit = status.trade_limit.load(Ordering::Relaxed);
        let trades_taken = status.session_trades_taken.load(Ordering::Relaxed);
        let limit_reached = trade_limit > 0 && trades_taken >= trade_limit;

        let trade_indicator = if !trading_on {
            "\x1b[37m⚪ LIVE:OFF (Press T)\x1b[30;46;1m"
        } else if limit_reached {
            "\x1b[33;1m⏹ LIMIT REACHED\x1b[30;43;1m"
        } else {
            "\x1b[32;1m🔴 LIVE:ON\x1b[30;41;1m"
        };

        let status_bg = if !trading_on {
            "\x1b[30;46;1m"
        } else if limit_reached {
            "\x1b[30;43;1m"
        } else {
            "\x1b[30;41;1m"
        };

        let status_line = format!(
            " ⚡ LIVE ARB │ {} pairs │ {} │ Bin⚡:{} Byb⚡:{} │ {} {}",
            total_rows,
            chrono::Local::now().format("%H:%M:%S"),
            bin_s,
            byb_s,
            trade_indicator,
            search_display,
        );
        let _ = write!(
            buf,
            "\x1b[{row_idx};1H{}{}\x1b[K\x1b[0m",
            status_bg, status_line
        );
        row_idx += 1;

        // ── Trading info bar (always shown: shows live balances and trade limit) ──
        {
            // Refresh cached balances every 500ms — avoids an async .await in the hot render path
            if last_balance_refresh.elapsed() >= Duration::from_millis(500) {
                if let Ok(eng) = engine.try_lock() {
                    let bin_ws = eng.binance_client.get_live_balance().await;
                    let byb_ws = eng.bybit_client.get_live_balance().await;
                    cached_bin_bal = if bin_ws > 0.0 {
                        bin_ws
                    } else {
                        eng.binance_balance
                    };
                    cached_byb_bal = if byb_ws > 0.0 {
                        byb_ws
                    } else {
                        eng.bybit_balance
                    };
                    last_balance_refresh = std::time::Instant::now();
                }
            }

            let engine_guard = engine.try_lock();
            match engine_guard {
                Ok(eng) => {
                    let bin_bal = if cached_bin_bal > 0.0 {
                        cached_bin_bal
                    } else {
                        eng.binance_balance
                    };
                    let byb_bal = if cached_byb_bal > 0.0 {
                        cached_byb_bal
                    } else {
                        eng.bybit_balance
                    };
                    let pnl_color = if eng.total_pnl > 0.0 {
                        "32"
                    } else if eng.total_pnl < 0.0 {
                        "31"
                    } else {
                        "37"
                    };
                    let open_count = eng.open_position_count();
                    let closed_count = eng.closed_count;
                    let unrealized = eng.total_unrealized_pnl(&store);
                    let unr_color = if unrealized > 0.0 {
                        "32"
                    } else if unrealized < 0.0 {
                        "31"
                    } else {
                        "37"
                    };
                    let halt_badge = if eng.emergency_halt {
                        " \x1b[31;1m[🚨HALT-R to reset]\x1b[0m\x1b[100m"
                    } else {
                        ""
                    };

                    let limit_badge = if trade_limit == 0 {
                        format!("Limit:∞ (taken:{})", trades_taken)
                    } else if limit_reached {
                        format!(
                            "\x1b[33;1mLimit:{}/{} [DONE]\x1b[0m\x1b[100m",
                            trades_taken, trade_limit
                        )
                    } else {
                        format!("Limit:{}/{}", trades_taken, trade_limit)
                    };

                    let last_trade = eng
                        .recent_trades
                        .back()
                        .map(|t| {
                            let type_icon = match t.trade_type {
                                crate::trade_journal::TradeType::Open => "📈",
                                crate::trade_journal::TradeType::Close => "📉",
                            };
                            format!(
                                "│ {} {} {}->{} \x1b[{}m${:.4}\x1b[0m\x1b[100m",
                                type_icon,
                                t.coin,
                                t.exchange_buy,
                                t.exchange_sell,
                                if t.pnl_net > 0.0 {
                                    "32"
                                } else if t.pnl_net < 0.0 {
                                    "31"
                                } else {
                                    "37"
                                },
                                t.pnl_net
                            )
                        })
                        .unwrap_or_default();

                    let trade_bar = format!(
                        " 💰 Bin:\x1b[33;1m${:.2}\x1b[0m\x1b[100m  Byb:\x1b[33;1m${:.2}\x1b[0m\x1b[100m │ \x1b[33mOpen:{}\x1b[0m\x1b[100m Closed:{} W:{} L:{} \x1b[36;1m[{}]\x1b[0m\x1b[100m │ PnL:\x1b[{}m${:.2}\x1b[0m\x1b[100m │ Unrl:\x1b[{}m${:.2}\x1b[0m\x1b[100m │ Fees:${:.2}{} {}",
                        bin_bal, byb_bal,
                        open_count, closed_count, eng.winning_trades, eng.losing_trades,
                        limit_badge,
                        pnl_color, eng.total_pnl,
                        unr_color, unrealized,
                        eng.total_fees_paid,
                        halt_badge,
                        last_trade,
                    );
                    let _ = write!(
                        buf,
                        "\x1b[{row_idx};1H\x1b[37;100m{}\x1b[K\x1b[0m",
                        trade_bar
                    );
                }
                Err(_) => {
                    let _ = write!(
                        buf,
                        "\x1b[{row_idx};1H\x1b[37;100m 💰 Loading balances...\x1b[K\x1b[0m"
                    );
                }
            }
            row_idx += 1;
        }

        // ── Open Positions Detail Panel (shown when positions exist) ──
        if !position_details.is_empty() {
            let _ = write!(
                buf,
                "\x1b[{row_idx};1H\x1b[30;43;1m {:<8} {:<7} {:<14} {:<7} {:<14} {:<10} {:<10} {:<8} {:<12} {:<14}\x1b[K\x1b[0m",
                "COIN", "BUY@", "ENTRY BUY", "SELL@", "ENTRY SELL",
                "BUY QTY", "SELL QTY", "SPREAD%", "HOLD", "UNREAL PnL"
            );
            row_idx += 1;
            for pd in &position_details {
                let pnl_color = if pd.unrealized_pnl > 0.0 {
                    "32;1"
                } else if pd.unrealized_pnl < 0.0 {
                    "31;1"
                } else {
                    "37"
                };
                let hold_str = if pd.hold_secs >= 3600 {
                    format!("{}h{}m", pd.hold_secs / 3600, (pd.hold_secs % 3600) / 60)
                } else if pd.hold_secs >= 60 {
                    format!("{}m{}s", pd.hold_secs / 60, pd.hold_secs % 60)
                } else {
                    format!("{}s", pd.hold_secs)
                };
                let entry_buy_str = format_price(pd.entry_buy_price);
                let entry_sell_str = format_price(pd.entry_sell_price);
                let _ = write!(
                    buf,
                    "\x1b[{row_idx};1H\x1b[33m {:<8}\x1b[0m\x1b[32m {:<7}\x1b[0m\x1b[37m {:<14}\x1b[0m\x1b[31m {:<7}\x1b[0m\x1b[37m {:<14}\x1b[0m\x1b[36m {:<10}\x1b[0m\x1b[36m {:<10}\x1b[0m\x1b[33m {:.4}%\x1b[0m  \x1b[37m {:<8}\x1b[0m  \x1b[{}m ${:.4}\x1b[K\x1b[0m",
                    pd.coin, pd.buy_exchange, entry_buy_str,
                    pd.sell_exchange, entry_sell_str,
                    format!("{:.2}", pd.buy_qty),
                    format!("{:.2}", pd.sell_qty),
                    pd.entry_spread,
                    hold_str,
                    pnl_color, pd.unrealized_pnl,
                );
                row_idx += 1;
            }
            let _ = write!(
                buf,
                "\x1b[{row_idx};1H\x1b[90m{}\x1b[K\x1b[0m",
                "─".repeat(COL_COIN + COL_PRICE * 2 + COL_SPREAD)
            );
            row_idx += 1;
        }

        // ── Header ──
        let _ = write!(
            buf,
            "\x1b[{row_idx};1H\x1b[37;100;1m{:<cw$}│{:<pw$}│{:<pw$}│{:<sw$}\x1b[K\x1b[0m",
            " COIN",
            "BINANCE ⚡",
            "BYBIT ⚡",
            "SPREAD %",
            cw = COL_COIN,
            pw = COL_PRICE,
            sw = COL_SPREAD,
        );
        row_idx += 1;

        // ── Separator ──
        let _ = write!(
            buf,
            "\x1b[{row_idx};1H\x1b[90m{}┼{}┼{}┼{}\x1b[K\x1b[0m",
            "─".repeat(COL_COIN),
            "─".repeat(COL_PRICE),
            "─".repeat(COL_PRICE),
            "─".repeat(COL_SPREAD),
        );
        row_idx += 1;

        // ── Rows ──
        let end = (scroll_offset + available_rows).min(total_rows);
        for i in scroll_offset..end {
            let row = &rows[i];
            let (min_idx, max_idx) = min_max_indices(row.binance, row.bybit);

            let (cc, marker) = if row.has_open_position {
                ("33;1", "●") // Yellow bold + dot for open positions
            } else if row.near_funding {
                ("31", "⏸") // Red + pause for near-funding
            } else if i % 2 == 0 {
                ("36;1", " ")
            } else {
                ("36", " ")
            };
            let coin_display = format!("{}{}({}h)", marker, &row.coin, row.funding_hours);
            let _ = write!(
                buf,
                "\x1b[{row_idx};1H\x1b[{}m{:<w$}\x1b[0m",
                cc,
                coin_display,
                w = COL_COIN
            );

            write_price_cell(
                &mut buf,
                row.binance,
                min_idx == Some(0),
                max_idx == Some(0),
            );
            write_price_cell(&mut buf, row.bybit, min_idx == Some(1), max_idx == Some(1));
            write_spread_cell(&mut buf, row.spread);

            let _ = write!(buf, "\x1b[K");
            row_idx += 1;
        }

        for _ in end.saturating_sub(scroll_offset)..available_rows {
            let _ = write!(buf, "\x1b[{row_idx};1H\x1b[K");
            row_idx += 1;
        }

        // ── Footer ──
        let fy = term_height.max(1);
        let _ = write!(buf, "\x1b[{};1H", fy);

        if search_mode {
            let footer = format!(" 🔍 Search: {}▌  (Enter=confirm, Esc=cancel)", search_query);
            let _ = write!(buf, "\x1b[30;43;1m{}\x1b[K\x1b[0m", footer);
        } else {
            let limit_footer = if trade_limit == 0 {
                "Limit: ∞".to_string()
            } else if limit_reached {
                format!("Limit: {}/{} (DONE)", trades_taken, trade_limit)
            } else {
                format!("Limit: {}/{}", trades_taken, trade_limit)
            };

            let footer = format!(
                " 1-2: Toggle │ T: LIVE {} │ L: {} │ R: ClearHalt │ ↑↓ Scroll │ /: Search │ q: Quit │ {}-{}/{}",
                if trading_on { "ON" } else { "OFF" },
                limit_footer,
                if total_rows > 0 { scroll_offset + 1 } else { 0 }, end, total_rows,
            );
            let _ = write!(buf, "\x1b[37;100m{}\x1b[K\x1b[0m", footer);
        }

        let buf_str = buf.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let mut stdout = io::stdout();
            let _ = stdout.write_all(buf_str.as_bytes());
            let _ = stdout.flush();
        })
        .await;

        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    cleanup();
}

pub fn cleanup() {
    let mut stdout = io::stdout();
    use std::io::Write;
    let _ = write!(stdout, "\x1b[?7h");
    let _ = terminal::disable_raw_mode();
    let _ = stdout.execute(cursor::Show);
    let _ = stdout.execute(terminal::LeaveAlternateScreen);
    let _ = stdout.flush();
}
