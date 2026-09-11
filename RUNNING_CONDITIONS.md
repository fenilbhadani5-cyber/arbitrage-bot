# Live Arbitrage Bot — Running Conditions & Complete Reference

## Table of Contents
- [Prerequisites](#prerequisites)
- [Environment Variables (Required)](#environment-variables-required)
- [Building & Running](#building--running)
- [Configuration Parameters](#configuration-parameters)
- [Trading Logic & Conditions](#trading-logic--conditions)
- [Safety Mechanisms](#safety-mechanisms)
- [Exchange API Requirements](#exchange-api-requirements)
- [Network Requirements](#network-requirements)
- [Keyboard Controls](#keyboard-controls)
- [File Outputs](#file-outputs)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

| Requirement | Details |
|---|---|
| **Rust** | Edition 2021+ (stable toolchain) |
| **OS** | Windows (tested), Linux/macOS (should work) |
| **Internet** | Stable low-latency connection to Binance & Bybit |
| **Accounts** | Binance Futures + Bybit Unified Trading accounts |
| **API Keys** | 4 keys with Futures trading permissions |
| **Margin** | Sufficient USDT in both futures accounts |
| **Terminal** | Supports ANSI escape codes (Windows Terminal, iTerm2, etc.) |

---

## Environment Variables (Required)

All 4 must be set before running. The bot **will not start** if any are missing.

```powershell
# PowerShell (Windows)
$env:BINANCE_API_KEY = "your-binance-api-key"
$env:BINANCE_API_SECRET = "your-binance-api-secret"
$env:BYBIT_API_KEY = "your-bybit-api-key"
$env:BYBIT_API_SECRET = "your-bybit-api-secret"
```

```bash
# Bash (Linux/macOS)
export BINANCE_API_KEY="your-binance-api-key"
export BINANCE_API_SECRET="your-binance-api-secret"
export BYBIT_API_KEY="your-bybit-api-key"
export BYBIT_API_SECRET="your-bybit-api-secret"
```

### API Key Permissions Required

| Exchange | Permission | Required |
|---|---|---|
| **Binance** | Futures Trading | ✅ Yes |
| **Binance** | Read Account Info | ✅ Yes |
| **Binance** | IP Restriction | Recommended |
| **Bybit** | Contract Trade | ✅ Yes |
| **Bybit** | Read-Only (Account) | ✅ Yes |
| **Bybit** | Unified Trading Account | ✅ Yes |

---

## Building & Running

### Development Build (faster compile, slower execution)
```bash
cd d:\Arbitrage\live_trading
cargo run
```

### Release Build (slower compile, optimized execution — recommended for live)
```bash
cd d:\Arbitrage\live_trading
cargo build --release
.\target\release\live-arb.exe
```

### Release Profile Optimizations (Cargo.toml)
| Setting | Value | Purpose |
|---|---|---|
| `opt-level` | 3 | Maximum optimization |
| `lto` | "fat" | Full link-time optimization across all crates |
| `codegen-units` | 1 | Single codegen unit for best optimization |
| `panic` | "abort" | No unwinding overhead |
| `strip` | true | Remove debug symbols for smaller binary |

---

## Configuration Parameters

All trading parameters are in [`src/config.rs`](src/config.rs):

| Parameter | Default | Description |
|---|---|---|
| `TRADE_SIZE_USDT` | `20.0` | USDT value per trade leg |
| `ENTRY_SPREAD_THRESHOLD` | `1.0%` | Minimum spread to OPEN a new position |
| `EXIT_SPREAD_THRESHOLD` | `0.3%` | Spread at which to CLOSE (convergence target) |
| `TRADE_COOLDOWN_SECS` | `30s` | Cooldown between trades on the same coin |
| `MIN_HOLD_SECS` | `30s` | Minimum seconds to hold before allowing close |
| `MAX_OPEN_POSITIONS` | `5` | Maximum concurrent open arbitrage positions |
| `FUNDING_PAUSE_MINUTES` | `5 min` | Pause trading within ±5 minutes of funding time |
| `SKIP_1H_FUNDING_COINS` | `true` | Skip coins with 1-hour funding intervals |
| `LIVE_TRADES_LOG_PATH` | `d:\Arbitrage\live_trades.jsonl` | Trade journal file path |

### Fee Rates (Taker, VIP-0)
| Exchange | Fee Rate |
|---|---|
| Binance | 0.05% (0.0005) |
| Bybit | 0.055% (0.00055) |

---

## Trading Logic & Conditions

### Position OPEN Conditions (ALL must be true)

1. ✅ **Trading enabled** — User pressed `T` to enable live trading
2. ✅ **Both exchanges enabled** — Binance and Bybit feeds are active
3. ✅ **No existing position** on this coin
4. ✅ **Not in cooldown** — At least `TRADE_COOLDOWN_SECS` since last trade on this coin
5. ✅ **Spread ≥ ENTRY_SPREAD_THRESHOLD** — Price difference exceeds 1.0%
6. ✅ **Open positions < MAX_OPEN_POSITIONS** — Not at capacity (5)
7. ✅ **Not a 1h funding coin** — (if `SKIP_1H_FUNDING_COINS` is true)
8. ✅ **Not near funding time** — Outside ±5 minutes of funding timestamp
9. ✅ **Order book spread is profitable** — `book_spread > total_fees + exit_threshold + 0.1%`
10. ✅ **Sufficient liquidity** — Trade size adjusted to available orderbook depth
11. ✅ **Trade size ≥ $5** — Skip micro-trades below $5
12. ✅ **Buy exchange has balance** — `balance ≥ trade_size × 1.01`
13. ✅ **Sell exchange has margin** — `balance ≥ trade_size × 1.01`
14. ✅ **Buy ask < Sell bid** — Valid arbitrage direction confirmed from orderbook

### Position CLOSE Conditions (ALL must be true)

1. ✅ **Position exists** — There is an open position on this coin
2. ✅ **Spread ≤ EXIT_SPREAD_THRESHOLD** — Spread has converged to ≤0.3%
3. ✅ **Hold time ≥ MIN_HOLD_SECS** — Position held for at least 30 seconds

### Forced Close (Funding Pause)

1. ✅ **Position exists** on a coin that is within `FUNDING_PAUSE_MINUTES` of funding
2. ✅ **Hold time ≥ 10 seconds** — Minimum hold even for forced close
3. ⚠️ Uses spread = -999.0 to bypass normal spread threshold check

### Profitability Formula

```
Gross PnL = (close_sell_price - entry_buy_price) × buy_qty 
          + (entry_sell_price - close_buy_price) × sell_qty

Total Fees = entry_buy_fee + entry_sell_fee + close_buy_fee + close_sell_fee

Net PnL = Gross PnL - Total Fees

Minimum profitable spread = (2 × binance_fee + 2 × bybit_fee) × 100 
                           + EXIT_SPREAD_THRESHOLD + 0.1
                           = (2×0.05 + 2×0.055) + 0.3 + 0.1
                           = 0.21 + 0.3 + 0.1
                           = 0.61%
```

---

## Safety Mechanisms

### One-Legged Recovery
If one side of a concurrent order pair fills but the other fails:
- **OPEN**: The filled side is immediately reversed with a market order
- **CLOSE**: Critical alert is logged; position remains open for manual review

### Funding Protection
- Coins within ±5 minutes of funding are paused for new positions
- Existing positions are force-closed near funding time
- Coins with 1h funding intervals are completely excluded (too volatile)

### Balance Checks
- Both buy and sell exchange balances are verified before every trade
- Balances are refreshed every ~60 seconds from exchange APIs
- Balances are refreshed after every position close

### Cooldown System
- 30-second cooldown between trades on the same coin
- Prevents rapid-fire trading on the same pair

### Position Limits
- Maximum 5 concurrent open positions
- Each coin can only have one active position

---

## Exchange API Requirements

### Binance Futures API
| Endpoint | Purpose | Auth |
|---|---|---|
| `wss://fstream.binance.com/stream?streams=!bookTicker` | 100% Pure Real-time tick-by-tick bookTicker (best bid/ask, 0ms delay) WS | No |
| `GET /fapi/v1/exchangeInfo` | List perpetual contracts | No |
| `GET /fapi/v1/ticker/price` | Initial price snapshot | No |
| `GET /fapi/v1/ticker/bookTicker` | Backup orderbook top (REST, every 2s) | No |
| `GET /fapi/v2/balance` | Account balance | Yes (HMAC-SHA256) |
| `POST /fapi/v1/order` | Place market order | Yes (HMAC-SHA256) |
| `GET /fapi/v1/userTrades` | Get fill details | Yes (HMAC-SHA256) |

### Bybit V5 API
| Endpoint | Purpose | Auth |
|---|---|---|
| `wss://stream.bybit.com/v5/public/linear` | Real-time tickers & orderbook WS | No |
| `GET /v5/market/instruments-info` | List linear perpetuals | No |
| `GET /v5/market/tickers` | REST price refresh (every 3s) | No |
| `GET /v5/account/wallet-balance` | Account balance | Yes (HMAC-SHA256) |
| `POST /v5/order/create` | Place market order | Yes (HMAC-SHA256) |
| `GET /v5/order/realtime` | Get order fill details | Yes (HMAC-SHA256) |

---

## Network Requirements

| Connection | Protocol | Latency Sensitivity |
|---|---|---|
| Binance WS (prices) | WSS (TLS) | **High** — real-time price feed |
| Bybit WS (prices) | WSS (TLS) | **High** — real-time price feed |
| Binance REST (orders) | HTTPS | **Critical** — order execution (3s timeout) |
| Bybit REST (orders) | HTTPS | **Critical** — order execution (3s timeout) |
| Binance REST (balance) | HTTPS | Low — periodic refresh (8s timeout) |
| Bybit REST (balance) | HTTPS | Low — periodic refresh (8s timeout) |
| Binance REST (bookTicker) | HTTPS | Medium — orderbook refresh every 2s |
| Bybit REST (tickers) | HTTPS | Medium — price backup every 3s |

### Reconnection Behavior
- **Binance WS**: Auto-reconnect after 2s on disconnect
- **Bybit WS**: Auto-reconnect after 3s on disconnect
- **Dead connection**: Detected after 30s of no messages → force reconnect
- **Stale data**: Cleared immediately on disconnect to prevent false spreads

---

## Keyboard Controls

| Key | Action |
|---|---|
| `T` | Toggle LIVE trading ON/OFF (starts OFF by default) |
| `1` | Toggle Binance feed ON/OFF |
| `2` | Toggle Bybit feed ON/OFF |
| `q` | Quit (prints trade summary) |
| `Ctrl+C` | Force quit |
| `↑`/`k` | Scroll up |
| `↓`/`j` | Scroll down |
| `PgUp`/`PgDn` | Scroll by 20 rows |
| `Home`/`End` | Jump to top/bottom |
| `/` | Enter search mode (filter coins) |
| `Esc` | Clear search / reset scroll |

---

## File Outputs

### Trade Journal (`live_trades.jsonl`)
- Location: `d:\Arbitrage\live_trades.jsonl`
- Format: JSONL (one JSON object per line)
- Contains: Every OPEN and CLOSE trade record with full exchange fill data
- Fields include: order IDs, fill prices, quantities, commissions, spreads, PnL, funding info

### Log Output (stderr)
- All operational logs are written to stderr
- Includes: connection status, trade execution details, errors, warnings
- Can be redirected to a file: `live-arb.exe 2> trading.log`

---

## Troubleshooting

### "BINANCE_API_KEY environment variable not set!"
→ Set all 4 environment variables before running. See [Environment Variables](#environment-variables-required).

### "Binance API key verification FAILED"
→ Check that your API key has Futures trading permission and is not expired/restricted.

### "Bybit API error: 10003 - Invalid parameter"
→ Ensure your Bybit account is a Unified Trading Account (not Classic).

### "Insufficient balance on Binance/Bybit"
→ Transfer USDT to your futures/unified trading account. Need at least `TRADE_SIZE_USDT × 1.01` per exchange.

### "No messages for 30s — reconnecting"
→ Network issue. The bot will auto-reconnect. Check your internet connection.

### "ONE-LEG RECOVERY: Reversed..."
→ One side of a trade failed after the other filled. The filled side was automatically reversed. Check your exchange positions to confirm.

### "⚠️ CRITICAL: partial close!"
→ A close operation partially failed. **Check your exchange positions manually** — you may have an unhedged position.

### Prices showing but no trades executing
→ Make sure you pressed `T` to enable live trading (shown in status bar as "LIVE:ON").

### All spreads showing 0% or N/A
→ Both exchanges must be connected and enabled. Check status indicators in the header bar.

---

## Timing Summary

| Component | Interval |
|---|---|
| Trading loop scan | 100ms |
| Binance price updates (WS) | Real-time (~100-250ms) |
| Bybit price updates (WS) | Real-time (~100-250ms) |
| Binance orderbook (REST) | 2s |
| Bybit price backup (REST) | 3s |
| Balance refresh | ~60s |
| Renderer refresh | 80ms |
| Order HTTP timeout | 3s |
| Fill poll interval | 50ms (max 10 attempts = 500ms) |
| WebSocket ping | 20s |
| Dead connection timeout | 30s |
