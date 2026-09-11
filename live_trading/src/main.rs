mod config;
mod exchanges;
mod price_store;
mod live_trading;
mod trade_journal;
mod renderer;
mod funding;
mod latency;
mod position_sync;
pub mod missed_trade_logger;


use config::{LIVE_TRADES_LOG_PATH, MISSED_TRADES_LOG_PATH};
use exchanges::binance_api::BinanceClient;
use exchanges::bybit_api::BybitClient;
use price_store::{new_store, new_status, new_funding_store, Exchange};
use std::time::Duration;

/// Print a detailed trade summary to the terminal after exit.
fn print_trade_summary() {
    let trades = trade_journal::load_trades(LIVE_TRADES_LOG_PATH);

    println!("\n\x1b[31;1m{}\x1b[0m", "═".repeat(70));
    println!("\x1b[31;1m  📊 LIVE TRADING SUMMARY\x1b[0m");
    println!("\x1b[31;1m{}\x1b[0m\n", "═".repeat(70));

    if trades.is_empty() {
        println!("  No live trades recorded yet.");
        println!("  Press \x1b[33;1mT\x1b[0m while running to enable LIVE trading.\n");
        println!("  Trade log:        \x1b[90m{}\x1b[0m", LIVE_TRADES_LOG_PATH);
        println!("  Missed trade log: \x1b[90m{}\x1b[0m\n", MISSED_TRADES_LOG_PATH);
        return;
    }

    let summary = trade_journal::compute_summary(&trades);

    let pnl_color = if summary.total_pnl_net > 0.0 { "32;1" } else { "31;1" };
    println!("  \x1b[37;1mTotal Trades:\x1b[0m  {}", summary.total_trades);
    println!("  \x1b[32mWins:\x1b[0m          {}  │  \x1b[31mLosses:\x1b[0m  {}", summary.winning_trades, summary.losing_trades);
    println!("  \x1b[37;1mWin Rate:\x1b[0m      {:.1}%", summary.win_rate);
    println!();
    println!("  \x1b[37;1mGross PnL:\x1b[0m     \x1b[{}m${:.4}\x1b[0m", pnl_color, summary.total_pnl_gross);
    println!("  \x1b[37;1mTotal Fees:\x1b[0m     \x1b[33m-${:.4}\x1b[0m", summary.total_fees);
    println!("  \x1b[37;1mNet PnL:\x1b[0m       \x1b[{}m${:.4}\x1b[0m", pnl_color, summary.total_pnl_net);
    println!("  \x1b[37;1mAvg Spread:\x1b[0m    {:.4}%", summary.avg_spread_captured);
    println!("  \x1b[37;1mBest Trade:\x1b[0m    \x1b[32m${:.4}\x1b[0m", summary.best_trade_pnl);
    println!("  \x1b[37;1mWorst Trade:\x1b[0m   \x1b[31m${:.4}\x1b[0m", summary.worst_trade_pnl);

    // Per-exchange breakdown
    println!("\n  \x1b[31;1m── Exchange Breakdown ──\x1b[0m\n");
    println!("  \x1b[37;1m{:<12} {:>8} {:>8} {:>12} {:>12}\x1b[0m", "Exchange", "Buy", "Sell", "Fees Paid", "PnL");
    println!("  \x1b[90m{}\x1b[0m", "─".repeat(58));

    for ex in &[Exchange::Binance, Exchange::Bybit] {
        if let Some(stats) = summary.exchange_stats.get(ex) {
            let pnl_c = if stats.total_pnl_contribution > 0.0 { "32" } else { "31" };
            println!("  {:<12} {:>8} {:>8} \x1b[33m${:>10.4}\x1b[0m \x1b[{}m${:>10.4}\x1b[0m",
                ex, stats.trades_as_buy, stats.trades_as_sell,
                stats.total_fees_paid, pnl_c, stats.total_pnl_contribution);
        }
    }

    // Last 10 trades
    let recent_start = if trades.len() > 10 { trades.len() - 10 } else { 0 };
    let recent = &trades[recent_start..];

    println!("\n  \x1b[31;1m── Last {} Trades ──\x1b[0m\n", recent.len());
    println!("  \x1b[37;1m{:<6} {:<8} {:<10} {:<10} {:>10} {:>10} {:>8} {:>10}\x1b[0m",
        "#", "Coin", "Buy@", "Sell@", "Spread", "Fee", "Net PnL", "Time");
    println!("  \x1b[90m{}\x1b[0m", "─".repeat(80));

    for trade in recent {
        let pnl_c = if trade.pnl_net > 0.0 { "32" } else { "31" };
        let time = trade.timestamp.format("%H:%M:%S");
        println!("  {:<6} {:<8} {:<10} {:<10} {:>9.4}% \x1b[33m${:>8.4}\x1b[0m \x1b[{}m${:>8.4}\x1b[0m {:>10}",
            trade.id,
            if trade.coin.len() > 7 { &trade.coin[..7] } else { &trade.coin },
            format!("{}", trade.exchange_buy),
            format!("{}", trade.exchange_sell),
            trade.spread_before,
            trade.total_fee,
            pnl_c, trade.pnl_net,
            time);
    }

    println!("\n  \x1b[90mFull trade log:   {}\x1b[0m", LIVE_TRADES_LOG_PATH);
    println!("  \x1b[90mMissed trade log: {}\x1b[0m", MISSED_TRADES_LOG_PATH);
    println!();
}

/// Automatically try to load environment variables from keys.env or .env file
fn load_env_file() {
    let candidates = [
        "keys.env",
        "../keys.env",
        "d:\\Arbitrage\\keys.env",
        ".env",
        "../.env",
    ];
    for path in &candidates {
        if let Ok(content) = std::fs::read_to_string(path) {
            let mut loaded_count = 0;
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((k, v)) = line.split_once('=') {
                    let key = k.trim();
                    let val = v.trim().trim_matches('"').trim_matches('\'').trim();
                    if !val.is_empty() && !val.starts_with("paste-your-") {
                        if std::env::var(key).map(|existing| existing.is_empty()).unwrap_or(true) {
                            std::env::set_var(key, val);
                            loaded_count += 1;
                        }
                    }
                }
            }
            if loaded_count > 0 {
                println!("\x1b[32m[Config] Loaded {} API keys from {}\x1b[0m", loaded_count, path);
            }
            break;
        }
    }
}

#[tokio::main]
async fn main() {
    // ── Load keys from keys.env file if present ──
    load_env_file();

    // ── Load API keys from environment variables ──
    let binance_api_key = std::env::var("BINANCE_API_KEY").unwrap_or_else(|_| {
        eprintln!("\x1b[31;1m[ERROR] BINANCE_API_KEY not set in environment or keys.env!\x1b[0m");
        String::new()
    });
    let binance_api_secret = std::env::var("BINANCE_API_SECRET").unwrap_or_else(|_| {
        eprintln!("\x1b[31;1m[ERROR] BINANCE_API_SECRET not set in environment or keys.env!\x1b[0m");
        String::new()
    });
    let bybit_api_key = std::env::var("BYBIT_API_KEY").unwrap_or_else(|_| {
        eprintln!("\x1b[31;1m[ERROR] BYBIT_API_KEY not set in environment or keys.env!\x1b[0m");
        String::new()
    });
    let bybit_api_secret = std::env::var("BYBIT_API_SECRET").unwrap_or_else(|_| {
        eprintln!("\x1b[31;1m[ERROR] BYBIT_API_SECRET not set in environment or keys.env!\x1b[0m");
        String::new()
    });

    if binance_api_key.is_empty() || binance_api_secret.is_empty()
        || bybit_api_key.is_empty() || bybit_api_secret.is_empty()
    {
        eprintln!("\n\x1b[31;1m[FATAL] All 4 API keys must be set. Exiting.\x1b[0m");
        eprintln!("  Please open \x1b[33;1mkeys.env\x1b[0m in the project folder and paste your keys:");
        eprintln!("    BINANCE_API_KEY");
        eprintln!("    BINANCE_API_SECRET");
        eprintln!("    BYBIT_API_KEY");
        eprintln!("    BYBIT_API_SECRET");
        eprintln!("  (Or set them via PowerShell $env:)\n");
        std::process::exit(1);
    }

    eprintln!("\x1b[31;1m");
    eprintln!("  ╔═══════════════════════════════════════════╗");
    eprintln!("  ║  ⚠️  LIVE TRADING MODE — REAL MONEY ⚠️     ║");
    eprintln!("  ║  Binance + Bybit Futures Arbitrage        ║");
    eprintln!("  ║  Press T to enable trading, q to quit     ║");
    eprintln!("  ╚═══════════════════════════════════════════╝");
    eprintln!("\x1b[0m");

    use exchanges::fill_channel::{new_pending_fill_map, new_live_balance};

    // ── Create shared fill channels and live balances ──
    // These are written by the private WS and read by market_order_with_fill()
    let bin_fills   = new_pending_fill_map();
    let bin_balance = new_live_balance();
    let byb_fills   = new_pending_fill_map();
    let byb_balance = new_live_balance();

    // Create API clients (inject fill maps + live balance cells)
    let binance_client = BinanceClient::new(
        binance_api_key.clone(),
        binance_api_secret.clone(),
        bin_fills.clone(),
        bin_balance.clone(),
    );
    let bybit_client = BybitClient::new(
        bybit_api_key.clone(),
        bybit_api_secret.clone(),
        byb_fills.clone(),
        byb_balance.clone(),
    );

    // Verify API keys by fetching balances
    eprintln!("[Startup] Verifying API keys...");
    match binance_client.get_balance().await {
        Ok(bal) => {
            *bin_balance.write().await = bal;
            eprintln!("[Startup] Binance USDT balance: ${:.2}", bal);
        }
        Err(e) => {
            eprintln!("\x1b[31;1m[Startup] Binance API key verification FAILED: {}\x1b[0m", e);
            eprintln!("  Check your BINANCE_API_KEY and BINANCE_API_SECRET");
            std::process::exit(1);
        }
    }
    match bybit_client.get_balance().await {
        Ok(bal) => {
            *byb_balance.write().await = bal;
            eprintln!("[Startup] Bybit USDT balance: ${:.2}", bal);
        }
        Err(e) => {
            eprintln!("\x1b[31;1m[Startup] Bybit API key verification FAILED: {}\x1b[0m", e);
            eprintln!("  Check your BYBIT_API_KEY and BYBIT_API_SECRET");
            std::process::exit(1);
        }
    }

    // ── Load exchange symbol metadata (tick sizes, step sizes, min notional) ──
    let exchange_info = exchanges::exchange_info::ExchangeInfoCache::new();
    eprintln!("[Startup] Loading exchange symbol metadata...");
    let (bin_info_result, byb_info_result) = tokio::join!(
        exchanges::exchange_info::load_binance_info(&exchange_info),
        exchanges::exchange_info::load_bybit_info(&exchange_info)
    );
    match bin_info_result {
        Ok(count) => eprintln!("[Startup] Binance: loaded {} symbol specs (tick/step/notional)", count),
        Err(e) => eprintln!("[Startup] WARNING: Failed to load Binance exchangeInfo: {} — using heuristic rounding", e),
    }
    match byb_info_result {
        Ok(count) => eprintln!("[Startup] Bybit: loaded {} symbol specs (tick/step/notional)", count),
        Err(e) => eprintln!("[Startup] WARNING: Failed to load Bybit instruments-info: {} — using heuristic rounding", e),
    }

    let store = new_store();
    let status = new_status();
    let funding = new_funding_store();

    // Initialize live trading engine
    let engine = live_trading::new_trading_engine(
        binance_client.clone(),
        bybit_client.clone(),
        LIVE_TRADES_LOG_PATH.to_string(),
        exchange_info.clone(),
    );

    // ── Load historical trades & sync real account positions ──
    {
        let historical = trade_journal::load_trades(LIVE_TRADES_LOG_PATH);
        if !historical.is_empty() {
            let mut eng = engine.lock().await;
            eprintln!("[Startup] Loaded {} historical trades from {}", historical.len(), LIVE_TRADES_LOG_PATH);
            eng.trade_count = historical.len() as u64;
            let summary = trade_journal::compute_summary(&historical);
            eng.total_pnl = summary.total_pnl_net;
            eng.total_fees_paid = summary.total_fees;
            eng.winning_trades = summary.winning_trades;
            eng.losing_trades = summary.losing_trades;
            eng.closed_count = summary.closed_trades;
            let start = if historical.len() > 100 { historical.len() - 100 } else { 0 };
            eng.recent_trades = historical[start..].iter().cloned().collect();
        }
    }

    // ── Sync real open positions from exchange accounts ──
    // Fetches live positions from Binance+Bybit REST to recover any positions
    // that were open when the bot was last stopped. Overrides local JSONL state.
    eprintln!("[Startup] Syncing open positions from exchange accounts...");
    {
        let synced = position_sync::sync_open_positions(&binance_client, &bybit_client).await;
        if !synced.is_empty() {
            let mut eng = engine.lock().await;
            for pos in synced {
                let coin = pos.coin.clone();
                eprintln!(
                    "[Startup] Recovered open position: {} BUY {} @ {:.6} | SELL {} @ {:.6}",
                    coin, pos.buy_exchange, pos.entry_buy_price, pos.sell_exchange, pos.entry_sell_price
                );
                eng.open_positions.insert(coin, pos);
            }
        } else {
            eprintln!("[Startup] No open positions found on exchanges.");
        }
    }

    // ── Start price feeds (read-only, no auth needed) ──
    let bin_store = store.clone();
    let bin_status = status.clone();
    let bin_funding = funding.clone();
    tokio::spawn(async move {
        loop {
            exchanges::binance_feed::run(bin_store.clone(), bin_status.clone(), bin_funding.clone()).await;
            eprintln!("[Binance] Disconnected. Reconnecting in 2s...");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    let byb_store = store.clone();
    let byb_status = status.clone();
    let byb_funding = funding.clone();
    tokio::spawn(async move {
        loop {
            exchanges::bybit_feed::run(byb_store.clone(), byb_status.clone(), byb_funding.clone()).await;
            eprintln!("[Bybit] Disconnected. Reconnecting in 3s...");
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });

    // ── Private WebSocket feeds (authenticated — fills + balance) ──
    // Binance: USER_DATA stream (ORDER_TRADE_UPDATE + ACCOUNT_UPDATE)
    let ws_bin_bal = bin_balance.clone();
    tokio::spawn(async move {
        loop {
            exchanges::binance_private_ws::run(
                binance_api_key.clone(),
                bin_fills.clone(),
                ws_bin_bal.clone(),
            ).await;
            eprintln!("[BinancePrivateWS] Disconnected. Reconnecting in 3s...");
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });

    // Bybit: Private stream (execution + wallet)
    let ws_byb_bal = byb_balance.clone();
    tokio::spawn(async move {
        loop {
            exchanges::bybit_private_ws::run(
                bybit_api_key.clone(),
                bybit_api_secret.clone(),
                byb_fills.clone(),
                ws_byb_bal.clone(),
            ).await;
            eprintln!("[BybitPrivateWS] Disconnected. Reconnecting in 3s...");
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });


    // ── Continuous balance sync task (every 3 seconds) ──
    // Ensures any deposit/transfer made on Binance or Bybit website/app
    // immediately shows up on screen within 3 seconds, even if no trades are active.
    let poll_bin = binance_client.clone();
    let poll_byb = bybit_client.clone();
    let poll_bin_bal = bin_balance.clone();
    let poll_byb_bal = byb_balance.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        loop {
            interval.tick().await;
            if let Ok(b) = poll_bin.get_balance().await {
                *poll_bin_bal.write().await = b;
            }
            if let Ok(b) = poll_byb.get_balance().await {
                *poll_byb_bal.write().await = b;
            }
        }
    });

    // ── Live trading background loop ──
    let trade_store = store.clone();
    let trade_status = status.clone();
    let trade_engine = engine.clone();
    let trade_funding = funding.clone();
    tokio::spawn(async move {
        live_trading::run_trading_loop(trade_store, trade_status, trade_engine, trade_funding).await;
    });

    // ── Renderer runs until user presses 'q' or Ctrl+C ──
    let renderer_store = store.clone();
    let renderer_status = status.clone();
    let renderer_engine = engine.clone();
    let renderer_funding = funding.clone();
    let renderer_handle = tokio::spawn(async move {
        renderer::run(renderer_store, renderer_status, renderer_engine, renderer_funding).await;
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = renderer_handle => {}
    }

    renderer::cleanup();
    print_trade_summary();
    std::process::exit(0);
}
