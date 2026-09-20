use serde::Deserialize;
/// Exchange metadata cache: tick sizes, step sizes, and min notional per symbol.
/// Fetched once at startup from Binance exchangeInfo and Bybit instruments-info.
/// Used to properly round prices and quantities before sending orders.
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Per-symbol trading rules from an exchange.
#[derive(Debug, Clone)]
pub struct SymbolInfo {
    /// Minimum price increment (e.g., 0.0001 for BTCUSDT)
    pub tick_size: f64,
    /// Minimum quantity increment (e.g., 0.001 for BTCUSDT)
    pub step_size: f64,
    /// Minimum notional value per order (e.g., 5.0 USDT)
    pub min_notional: f64,
}

/// Shared cache of symbol metadata, keyed by symbol (e.g., "BTCUSDT").
/// Separate maps for each exchange since tick/step sizes can differ.
#[derive(Clone)]
pub struct ExchangeInfoCache {
    pub binance: Arc<RwLock<HashMap<String, SymbolInfo>>>,
    pub bybit: Arc<RwLock<HashMap<String, SymbolInfo>>>,
}

impl ExchangeInfoCache {
    pub fn new() -> Self {
        ExchangeInfoCache {
            binance: Arc::new(RwLock::new(HashMap::new())),
            bybit: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Round a price DOWN to the nearest tick size.
    /// For BUY limit orders: we want to buy at or below this price.
    pub fn round_price_down(price: f64, tick_size: f64) -> f64 {
        if tick_size <= 0.0 {
            return price;
        }
        (price / tick_size).floor() * tick_size
    }

    /// Round a price UP to the nearest tick size.
    /// For SELL limit orders: we want to sell at or above this price.
    pub fn round_price_up(price: f64, tick_size: f64) -> f64 {
        if tick_size <= 0.0 {
            return price;
        }
        (price / tick_size).ceil() * tick_size
    }

    /// Round a quantity DOWN to the nearest step size.
    pub fn round_qty_down(qty: f64, step_size: f64) -> f64 {
        if step_size <= 0.0 {
            return qty;
        }
        (qty / step_size).floor() * step_size
    }

    /// Get Binance symbol info (non-blocking read).
    pub async fn get_binance(&self, symbol: &str) -> Option<SymbolInfo> {
        self.binance.read().await.get(symbol).cloned()
    }

    /// Get Bybit symbol info (non-blocking read).
    pub async fn get_bybit(&self, symbol: &str) -> Option<SymbolInfo> {
        self.bybit.read().await.get(symbol).cloned()
    }
}

// ── Binance exchangeInfo parsing ──

#[derive(Debug, Deserialize)]
#[allow(non_snake_case, dead_code)]
struct BinanceExchangeInfo {
    symbols: Vec<BinanceSymbolInfo>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case, dead_code)]
struct BinanceSymbolInfo {
    symbol: String,
    filters: Vec<BinanceFilter>,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case, dead_code)]
struct BinanceFilter {
    filterType: String,
    #[serde(default)]
    tickSize: Option<String>,
    #[serde(default)]
    stepSize: Option<String>,
    #[serde(default)]
    notional: Option<String>,
    #[serde(default)]
    minNotional: Option<String>,
}

// ── Bybit instruments-info parsing ──

/// Fetch Binance futures exchangeInfo and populate the cache.
pub async fn load_binance_info(cache: &ExchangeInfoCache) -> Result<usize, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| format!("HTTP client error: {}", e))?;

    let resp = client
        .get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        .send()
        .await
        .map_err(|e| format!("Binance exchangeInfo request failed: {}", e))?;

    let text = resp
        .text()
        .await
        .map_err(|e| format!("Failed to read Binance exchangeInfo: {}", e))?;

    let info: BinanceExchangeInfo = serde_json::from_str(&text)
        .map_err(|e| format!("Failed to parse Binance exchangeInfo: {}", e))?;

    let mut map = cache.binance.write().await;
    let mut count = 0;

    for sym in &info.symbols {
        if !sym.symbol.ends_with("USDT") {
            continue;
        }

        let mut tick_size = 0.0;
        let mut step_size = 0.0;
        let mut min_notional = 5.0; // Binance default

        for filter in &sym.filters {
            match filter.filterType.as_str() {
                "PRICE_FILTER" => {
                    if let Some(ts) = &filter.tickSize {
                        tick_size = ts.parse::<f64>().unwrap_or(0.0);
                    }
                }
                "LOT_SIZE" | "MARKET_LOT_SIZE" => {
                    if let Some(ss) = &filter.stepSize {
                        let parsed = ss.parse::<f64>().unwrap_or(0.0);
                        if parsed > 0.0 && (step_size == 0.0 || parsed > step_size) {
                            step_size = parsed;
                        }
                    }
                }
                "MIN_NOTIONAL" => {
                    if let Some(n) = &filter.notional {
                        min_notional = n.parse::<f64>().unwrap_or(5.0);
                    } else if let Some(n) = &filter.minNotional {
                        min_notional = n.parse::<f64>().unwrap_or(5.0);
                    }
                }
                _ => {}
            }
        }

        if tick_size > 0.0 && step_size > 0.0 {
            map.insert(
                sym.symbol.clone(),
                SymbolInfo {
                    tick_size,
                    step_size,
                    min_notional,
                },
            );
            count += 1;
        }
    }

    Ok(count)
}

/// Fetch Bybit linear instruments info and populate the cache.
pub async fn load_bybit_info(cache: &ExchangeInfoCache) -> Result<usize, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| format!("HTTP client error: {}", e))?;

    // Bybit paginates with cursor — we need to loop
    let mut cursor = String::new();
    let mut total = 0;

    loop {
        let url = if cursor.is_empty() {
            "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
                .to_string()
        } else {
            format!("https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000&cursor={}", cursor)
        };

        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Bybit instruments-info request failed: {}", e))?;

        let text = resp
            .text()
            .await
            .map_err(|e| format!("Failed to read Bybit instruments-info: {}", e))?;

        let raw: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse Bybit instruments-info: {}", e))?;

        let ret_code = raw.get("retCode").and_then(|v| v.as_i64()).unwrap_or(-1);
        if ret_code != 0 {
            return Err(format!(
                "Bybit instruments-info API error: {}",
                raw.get("retMsg")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
            ));
        }

        let list = raw
            .get("result")
            .and_then(|r| r.get("list"))
            .and_then(|l| l.as_array())
            .cloned()
            .unwrap_or_default();

        if list.is_empty() {
            break;
        }

        let mut map = cache.bybit.write().await;

        for item in &list {
            let symbol = item.get("symbol").and_then(|s| s.as_str()).unwrap_or("");
            if !symbol.ends_with("USDT") {
                continue;
            }

            let tick_size = item
                .get("priceFilter")
                .and_then(|f| f.get("tickSize"))
                .and_then(|t| t.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);

            let step_size = item
                .get("lotSizeFilter")
                .and_then(|f| f.get("qtyStep"))
                .and_then(|t| t.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);

            let min_notional = 5.0;

            if tick_size > 0.0 && step_size > 0.0 {
                map.insert(
                    symbol.to_string(),
                    SymbolInfo {
                        tick_size,
                        step_size,
                        min_notional,
                    },
                );
                total += 1;
            }
        }

        // Check for next page cursor
        let next_cursor = raw
            .get("result")
            .and_then(|r| r.get("nextPageCursor"))
            .and_then(|c| c.as_str())
            .unwrap_or("");

        if next_cursor.is_empty() || next_cursor == cursor {
            break;
        }
        cursor = next_cursor.to_string();
    }

    Ok(total)
}
