use std::collections::VecDeque;

#[derive(Debug, PartialEq, Clone)]
pub enum VwapError {
    InsufficientDepth,
    MaxDepthExceeded,
    MaxSlippageExceeded,
}

#[derive(Debug, Clone)]
pub struct OrderBookLevel {
    pub price: f64,
    pub qty: f64,
}

/// Computes the VWAP for a given target quantity.
/// `levels`: Order book levels sorted best-to-worst (lowest price first for asks, highest first for bids).
/// `target_qty`: The total quantity to execute.
/// `max_depth_levels`: Maximum number of levels to walk.
/// `max_slippage_pct`: Maximum allowed slippage from the best price (1.0 = 1.0%).
/// `is_buy`: true if walking asks (buying), false if walking bids (selling).
pub fn compute_vwap(
    levels: &[OrderBookLevel],
    target_qty: f64,
    max_depth_levels: usize,
    max_slippage_pct: f64,
    is_buy: bool,
) -> Result<f64, VwapError> {
    if levels.is_empty() || target_qty <= 0.0 {
        return Err(VwapError::InsufficientDepth);
    }
    let best_price = levels[0].price;
    let mut remaining_qty = target_qty;
    let mut total_cost = 0.0;

    for (i, level) in levels.iter().enumerate() {
        if i >= max_depth_levels {
            return Err(VwapError::MaxDepthExceeded);
        }

        let slippage_pct = if is_buy {
            ((level.price - best_price) / best_price) * 100.0
        } else {
            ((best_price - level.price) / best_price) * 100.0
        };

        if slippage_pct > max_slippage_pct {
            return Err(VwapError::MaxSlippageExceeded);
        }

        let take_qty = remaining_qty.min(level.qty);
        total_cost += take_qty * level.price;
        remaining_qty -= take_qty;

        if remaining_qty <= 0.0 {
            return Ok(total_cost / target_qty);
        }
    }

    Err(VwapError::InsufficientDepth)
}

/// Compute effective spread using executable VWAP for both legs.
/// buy_levels: e.g. Binance asks (we buy from Binance).
/// sell_levels: e.g. Bybit bids (we sell to Bybit).
pub fn compute_effective_spread(
    buy_levels: &[OrderBookLevel],
    sell_levels: &[OrderBookLevel],
    target_qty: f64,
    max_depth_levels: usize,
    max_slippage_pct: f64,
) -> Result<f64, VwapError> {
    let effective_buy_price = compute_vwap(
        buy_levels,
        target_qty,
        max_depth_levels,
        max_slippage_pct,
        true,
    )?;
    let effective_sell_price = compute_vwap(
        sell_levels,
        target_qty,
        max_depth_levels,
        max_slippage_pct,
        false,
    )?;

    Ok(((effective_sell_price - effective_buy_price) / effective_buy_price) * 100.0)
}

fn compute_median(data: &mut [f64]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    data.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = data.len() / 2;
    if data.len().is_multiple_of(2) {
        (data[mid - 1] + data[mid]) / 2.0
    } else {
        data[mid]
    }
}

#[derive(Debug, Clone)]
pub struct Bucket {
    pub timestamp: u64,
    pub values: Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct RollingStats {
    pub rolling_median: f64,
    pub rolling_mean: f64,
    pub rolling_std: f64,
    pub rolling_mad: f64,
    pub recent_min: f64,
    pub recent_max: f64,
    pub sample_count: usize,
    pub last_spread: f64,
    pub last_observation_ts: u64,
    pub entry_threshold_crossing_ts: u64,
    pub spread_velocity: f64,

    pub config: crate::config::DynamicSpreadConfig,
    pub startup_time_ms: u64,

    current_bucket: Bucket,
    historical_buckets: VecDeque<f64>,
    recent_ticks: VecDeque<(u64, f64)>,
}

impl RollingStats {
    pub fn new(config: crate::config::DynamicSpreadConfig, startup_time_ms: u64) -> Self {
        Self {
            rolling_median: 0.0,
            rolling_mean: 0.0,
            rolling_std: 0.0,
            rolling_mad: 0.0,
            recent_min: 0.0,
            recent_max: 0.0,
            sample_count: 0,
            last_spread: 0.0,
            last_observation_ts: 0,
            entry_threshold_crossing_ts: 0,
            spread_velocity: 0.0,

            config,
            startup_time_ms,

            current_bucket: Bucket {
                timestamp: 0,
                values: Vec::new(),
            },
            historical_buckets: VecDeque::new(),
            recent_ticks: VecDeque::new(),
        }
    }

    /// Return the raw effective spread for the *signal path*.
    /// The *statistics path* will use the capped version internally.
    pub fn get_spread_volatility(&self) -> f64 {
        // robust_std = 1.4826 * MAD
        1.4826 * self.rolling_mad
    }

    pub fn update(&mut self, raw_spread: f64, timestamp_ms: u64) {
        self.last_spread = raw_spread;
        self.last_observation_ts = timestamp_ms;

        let mut stats_value = raw_spread;
        if self.sample_count > 0 && self.rolling_mad > 0.0 {
            let cap = self.config.outlier_cap_multiplier * self.rolling_mad;
            let min_val = self.rolling_median - cap;
            let max_val = self.rolling_median + cap;
            stats_value = stats_value.clamp(min_val, max_val);
        }

        // --- Spread Velocity Calculation ---
        self.recent_ticks.push_back((timestamp_ms, raw_spread));

        let target_ts = timestamp_ms.saturating_sub(self.config.velocity_window_ms);
        let tolerance = (self.config.velocity_window_ms as f64 * 0.20) as u64;

        let mut best_sample = None;
        let mut min_diff = u64::MAX;

        for &(ts, val) in &self.recent_ticks {
            let diff = ts.abs_diff(target_ts);
            if diff <= tolerance && diff < min_diff {
                min_diff = diff;
                best_sample = Some((ts, val));
            }
        }

        if let Some((_, old_spread)) = best_sample {
            let dt_sec = self.config.velocity_window_ms as f64 / 1000.0;
            if dt_sec > 0.0 {
                self.spread_velocity = (raw_spread - old_spread) / dt_sec;
            } else {
                self.spread_velocity = 0.0;
            }
        } else {
            self.spread_velocity = 0.0;
        }

        // Trim recent_ticks buffer
        let cutoff = timestamp_ms.saturating_sub(self.config.velocity_window_ms * 2);
        while let Some(&(ts, _)) = self.recent_ticks.front() {
            if ts < cutoff {
                self.recent_ticks.pop_front();
            } else {
                break;
            }
        }
        // -----------------------------------

        if self.current_bucket.timestamp == 0 {
            self.current_bucket.timestamp = timestamp_ms;
        }

        // Convert bucket size from seconds to ms
        let bucket_ms = self.config.stats_bucket_seconds * 1000;

        if timestamp_ms >= self.current_bucket.timestamp + bucket_ms {
            self.close_current_bucket();
            // Start new bucket
            self.current_bucket.timestamp = timestamp_ms;
        }

        self.current_bucket.values.push(stats_value);
        self.sample_count += 1;
    }

    fn close_current_bucket(&mut self) {
        if self.current_bucket.values.is_empty() {
            return;
        }

        let bucket_val = compute_median(&mut self.current_bucket.values);
        self.historical_buckets.push_back(bucket_val);

        let max_buckets =
            (self.config.stats_window_seconds / self.config.stats_bucket_seconds) as usize;
        while self.historical_buckets.len() > max_buckets {
            self.historical_buckets.pop_front();
        }

        self.current_bucket.values.clear();
        self.recompute_stats();
    }

    fn recompute_stats(&mut self) {
        if self.historical_buckets.is_empty() {
            return;
        }

        let mut values: Vec<f64> = self.historical_buckets.iter().copied().collect();
        self.rolling_median = compute_median(&mut values);
        self.rolling_mean = values.iter().sum::<f64>() / values.len() as f64;

        let variance = values
            .iter()
            .map(|v| (v - self.rolling_mean).powi(2))
            .sum::<f64>()
            / values.len() as f64;
        self.rolling_std = variance.sqrt();

        let mut abs_devs: Vec<f64> = values
            .iter()
            .map(|v| (v - self.rolling_median).abs())
            .collect();
        self.rolling_mad = compute_median(&mut abs_devs);

        self.recent_min = values.iter().copied().fold(f64::INFINITY, f64::min);
        self.recent_max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    }

    /// Checks if trading is permitted according to warmup and safety guards.
    pub fn can_trade(&self, current_time_ms: u64) -> Result<(), crate::rejection::RejectionReason> {
        if self.sample_count < self.config.minimum_samples {
            return Err(crate::rejection::RejectionReason::INSUFFICIENT_HISTORY);
        }
        if current_time_ms < self.startup_time_ms + (self.config.warmup_seconds * 1000) {
            return Err(crate::rejection::RejectionReason::WARMUP);
        }
        if self.get_spread_volatility() < self.config.min_volatility_floor_pct {
            return Err(crate::rejection::RejectionReason::INSUFFICIENT_LIQUIDITY);
            // actually maybe something else, let's use Z_SCORE_TOO_LOW or just WARMUP
            // Wait, min_volatility_floor_pct is used for z-score division floor, it shouldn't reject by itself,
            // but the original code returned false. Let's return WARMUP for now or INSUFFICIENT_HISTORY
        }
        Ok(())
    }

    /// Returns the dynamic entry threshold if trading is permitted.
    pub fn get_entry_threshold(&self, current_time_ms: u64) -> Option<f64> {
        if self.can_trade(current_time_ms).is_err() {
            return None;
        }
        let dynamic_entry =
            self.rolling_median + (self.config.k_entry * self.get_spread_volatility());
        let final_entry = dynamic_entry.max(self.config.minimum_entry_threshold_pct);
        Some(final_entry)
    }

    /// Returns the dynamic exit threshold if trading is permitted.
    pub fn get_exit_threshold(&self, current_time_ms: u64) -> Option<f64> {
        if self.can_trade(current_time_ms).is_err() {
            return None;
        }
        let dynamic_exit =
            self.rolling_median + (self.config.k_exit * self.get_spread_volatility());
        let final_exit = dynamic_exit.clamp(
            self.config.minimum_exit_threshold_pct,
            self.config.maximum_exit_threshold_pct,
        );
        Some(final_exit)
    }

    /// Evaluates if the current state triggers an ENTRY signal.
    pub fn evaluate_entry(
        &mut self,
        current_spread: f64,
        current_time_ms: u64,
    ) -> Result<(), crate::rejection::RejectionReason> {
        self.can_trade(current_time_ms)?;

        let dynamic_entry = match self.get_entry_threshold(current_time_ms) {
            Some(t) => t,
            None => return Err(crate::rejection::RejectionReason::INSUFFICIENT_HISTORY),
        };

        // Condition 2: raw, current effective spread crosses ABOVE dynamic entry threshold
        if current_spread <= dynamic_entry {
            return Err(crate::rejection::RejectionReason::SPREAD_BELOW_DYNAMIC_THRESHOLD);
        }

        let z_score = if self.get_spread_volatility() > 0.0 {
            (current_spread - self.rolling_median)
                / self
                    .get_spread_volatility()
                    .max(self.config.min_volatility_floor_pct)
        } else {
            0.0
        };

        if z_score < self.config.minimum_entry_z_score {
            return Err(crate::rejection::RejectionReason::Z_SCORE_TOO_LOW);
        }

        // Collapse detection
        if self.spread_velocity < -self.config.collapse_rate_threshold_pct_per_sec {
            return Err(crate::rejection::RejectionReason::SPREAD_COLLAPSING);
        }

        let signal_age_ms = if self.entry_threshold_crossing_ts > 0
            && current_time_ms >= self.entry_threshold_crossing_ts
        {
            current_time_ms - self.entry_threshold_crossing_ts
        } else {
            0
        };

        if signal_age_ms > self.config.max_signal_age_ms {
            return Err(crate::rejection::RejectionReason::SIGNAL_TOO_OLD);
        }

        // Condition 3: raw, current effective spread is LESS THAN max_slippage_pct away from threshold (we can drop this or map to INVALID_ORDERBOOK)
        if (current_spread - dynamic_entry) >= self.config.max_slippage_pct {
            return Err(crate::rejection::RejectionReason::INVALID_ORDERBOOK);
        }

        // All conditions met!
        self.entry_threshold_crossing_ts = current_time_ms;
        Ok(())
    }

    /// Evaluates if the current state triggers an EXIT (close) signal for an open position.
    pub fn evaluate_exit(
        &mut self,
        current_spread: f64,
        current_time_ms: u64,
        open_time_ms: u64,
    ) -> bool {
        // Condition 3: time since open exceeds force_close_minutes
        let hold_ms = current_time_ms.saturating_sub(open_time_ms);
        let force_close_ms = self.config.max_arbitrage_hold_time_ms;
        if hold_ms > force_close_ms {
            return true;
        }

        if self.can_trade(current_time_ms).is_err() {
            return false;
        }

        // Condition 2: below hard-floor safety catch
        if current_spread < self.config.minimum_exit_threshold_pct {
            return true;
        }

        // Condition 1: below dynamic exit threshold
        if let Some(dynamic_exit) = self.get_exit_threshold(current_time_ms) {
            if current_spread < dynamic_exit {
                return true;
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_median() {
        let mut data = vec![1.0, 5.0, 3.0, 4.0, 2.0];
        assert_eq!(compute_median(&mut data), 3.0);

        let mut data2 = vec![1.0, 4.0, 2.0, 3.0];
        assert_eq!(compute_median(&mut data2), 2.5);
    }

    #[test]
    fn test_vwap_walk() {
        let asks = vec![
            OrderBookLevel {
                price: 100.0,
                qty: 10.0,
            },
            OrderBookLevel {
                price: 101.0,
                qty: 10.0,
            },
            OrderBookLevel {
                price: 102.0,
                qty: 10.0,
            },
        ];

        // Target qty 15: takes 10 @ 100, 5 @ 101 => total cost 1000 + 505 = 1505. VWAP = 1505/15 = 100.333
        let vwap = compute_vwap(&asks, 15.0, 10, 5.0, true).unwrap();
        assert!((vwap - 100.333333).abs() < 1e-5);
    }

    #[test]
    fn test_vwap_invalid_depth() {
        let asks = vec![OrderBookLevel {
            price: 100.0,
            qty: 10.0,
        }];

        // Need 15, only 10 available
        let result = compute_vwap(&asks, 15.0, 10, 5.0, true);
        assert_eq!(result, Err(VwapError::InsufficientDepth));
    }

    #[test]
    fn test_vwap_invalid_max_levels() {
        let asks = vec![
            OrderBookLevel {
                price: 100.0,
                qty: 1.0,
            },
            OrderBookLevel {
                price: 101.0,
                qty: 1.0,
            },
            OrderBookLevel {
                price: 102.0,
                qty: 10.0,
            },
        ];

        // Need 5. We restrict to max_depth_levels = 2. But the first 2 levels only give 2.0 qty.
        let result = compute_vwap(&asks, 5.0, 2, 5.0, true);
        assert_eq!(result, Err(VwapError::MaxDepthExceeded));
    }

    #[test]
    fn test_vwap_invalid_slippage() {
        let asks = vec![
            OrderBookLevel {
                price: 100.0,
                qty: 5.0,
            },
            OrderBookLevel {
                price: 105.0,
                qty: 10.0,
            }, // 5% slippage
        ];

        // Max slippage pct is 4.0%
        let result = compute_vwap(&asks, 10.0, 10, 4.0, true);
        assert_eq!(result, Err(VwapError::MaxSlippageExceeded));
    }

    #[test]
    fn test_effective_spread_asymmetric() {
        // Direction A: buy Binance, sell Bybit
        let binance_asks = vec![
            OrderBookLevel {
                price: 100.0,
                qty: 10.0,
            },
            OrderBookLevel {
                price: 101.0,
                qty: 10.0,
            },
        ];
        let bybit_bids = vec![
            OrderBookLevel {
                price: 105.0,
                qty: 10.0,
            },
            OrderBookLevel {
                price: 104.0,
                qty: 10.0,
            },
        ];

        // Target qty 15
        // Buy Binance VWAP: 15 qty -> 10@100, 5@101 -> 1505/15 = 100.333
        // Sell Bybit VWAP: 15 qty -> 10@105, 5@104 -> 1570/15 = 104.666
        // Spread = (104.666 - 100.333) / 100.333 * 100 = 4.318%

        let spread_a = compute_effective_spread(&binance_asks, &bybit_bids, 15.0, 5, 5.0).unwrap();
        assert!((spread_a - 4.3189).abs() < 1e-3);

        // Direction B: buy Bybit, sell Binance (different book)
        let bybit_asks = vec![OrderBookLevel {
            price: 95.0,
            qty: 20.0,
        }];
        let binance_bids = vec![OrderBookLevel {
            price: 98.0,
            qty: 20.0,
        }];
        let spread_b = compute_effective_spread(&bybit_asks, &binance_bids, 15.0, 5, 5.0).unwrap();

        // Spread = (98 - 95)/95 = 3.157%
        assert!((spread_b - 3.1578).abs() < 1e-3);

        assert!(spread_a != spread_b); // non-symmetric
    }

    #[test]
    fn test_rolling_stats_winsorization_divergence() {
        let mut config = crate::config::DynamicSpreadConfig::default();
        config.stats_window_seconds = 14400;
        config.stats_bucket_seconds = 30;
        config.outlier_cap_multiplier = 8.0;
        let mut stats = RollingStats::new(config, 0);

        let mut time = 1000;
        let medians = vec![0.20, 0.21, 0.19, 0.22, 0.18, 0.20, 0.21, 0.19, 0.22, 0.18];
        for &m in &medians {
            stats.update(m, time);
            stats.update(m + 0.01, time + 10);
            stats.update(m - 0.01, time + 20);

            // Advance by 30 seconds to close bucket
            time += 30000;
            // Dummy update to force bucket closure and start new bucket
            stats.update(m, time);
        }

        let median_before = stats.rolling_median;
        let mad_before = stats.rolling_mad;
        assert!(mad_before > 0.0, "mad_before was {}", mad_before);

        // The *signal path* sees the raw outlier directly.
        let raw_outlier = 4.0;

        // The *statistics path* cap is median + (8 * MAD)
        let cap = median_before + (8.0 * mad_before);

        // Assert that the raw outlier is strictly larger than the cap
        assert!(
            raw_outlier > cap,
            "Raw outlier {} is not > cap {}",
            raw_outlier,
            cap
        );

        let expected_capped_value = cap;

        // We push the outlier
        stats.update(raw_outlier, time + 10);

        // Verify that the internally stored value was capped
        let last_val_in_bucket = *stats.current_bucket.values.last().unwrap();
        assert!((last_val_in_bucket - expected_capped_value).abs() < 1e-7);

        // Numeric difference assertion between signal path and stats path
        let difference = raw_outlier - last_val_in_bucket;
        assert!(
            difference > 0.1,
            "Numeric difference between signal and stats paths is {}",
            difference
        );
    }

    #[test]
    fn test_threshold_math_happy_path() {
        let mut config = crate::config::DynamicSpreadConfig::default();
        config.warmup_seconds = 10;
        config.minimum_samples = 5;
        config.k_entry = 3.0;
        config.k_exit = 1.0;
        config.minimum_entry_threshold_pct = 0.5;
        config.minimum_exit_threshold_pct = 0.1;
        config.maximum_exit_threshold_pct = 0.6;
        config.min_volatility_floor_pct = 0.01;

        let startup_time = 1000;
        let mut stats = RollingStats::new(config, startup_time);

        // Seed some data to populate median and mad
        let mut time = startup_time;
        for &m in &[0.20, 0.22, 0.18, 0.20, 0.22, 0.18] {
            stats.update(m, time);
            time += 30000;
        }

        // Ensure constraints are met
        assert!(stats.sample_count >= 5);
        assert!(time >= startup_time + 10000);

        let volatility = stats.get_spread_volatility();
        assert!(volatility >= 0.01);

        let entry = stats.get_entry_threshold(time).unwrap();
        let exit = stats.get_exit_threshold(time).unwrap();

        // dynamic_entry = 0.20 + 3 * vol
        let expected_dynamic_entry = stats.rolling_median + 3.0 * volatility;
        assert!((entry - expected_dynamic_entry.max(0.5)).abs() < 1e-7);

        // dynamic_exit = 0.20 + 1 * vol
        let expected_dynamic_exit = stats.rolling_median + 1.0 * volatility;
        let expected_clamped_exit = expected_dynamic_exit.clamp(0.1, 0.6);
        assert!((exit - expected_clamped_exit).abs() < 1e-7);
    }

    #[test]
    fn test_threshold_math_volatility_floor_rejection() {
        let mut config = crate::config::DynamicSpreadConfig::default();
        config.warmup_seconds = 10;
        config.minimum_samples = 5;
        config.min_volatility_floor_pct = 1.0; // Extremely high floor to force rejection

        let startup_time = 1000;
        let mut stats = RollingStats::new(config, startup_time);

        let mut time = startup_time;
        for _ in 0..6 {
            stats.update(0.20, time); // All same values, MAD = 0, volatility = 0
            time += 30000;
        }

        assert_eq!(stats.get_spread_volatility(), 0.0);

        // Rejected because volatility 0.0 < floor 1.0
        assert_eq!(stats.get_entry_threshold(time), None);
        assert_eq!(stats.get_exit_threshold(time), None);
        assert!(stats.can_trade(time).is_err());
    }

    #[test]
    fn test_threshold_math_minimum_entry_floor() {
        let mut config = crate::config::DynamicSpreadConfig::default();
        config.warmup_seconds = 10;
        config.minimum_samples = 5;
        config.k_entry = 3.0;
        config.minimum_entry_threshold_pct = 10.0; // High minimum entry
        config.min_volatility_floor_pct = 0.01;

        let startup_time = 1000;
        let mut stats = RollingStats::new(config, startup_time);

        let mut time = startup_time;
        for &m in &[0.20, 0.21, 0.19, 0.20, 0.21, 0.19] {
            stats.update(m, time);
            time += 30000;
        }

        // dynamic_entry should be around 0.20 + 3 * ~0.015 = ~0.245
        // but minimum_entry_threshold_pct is 10.0
        let entry = stats.get_entry_threshold(time).unwrap();
        assert_eq!(entry, 10.0);
    }

    #[test]
    fn test_threshold_math_exit_clamps() {
        let mut config = crate::config::DynamicSpreadConfig::default();
        config.warmup_seconds = 10;
        config.minimum_samples = 5;
        config.min_volatility_floor_pct = 0.01;

        let startup_time = 1000;
        let mut stats = RollingStats::new(config.clone(), startup_time);

        let mut time = startup_time;
        for &m in &[0.20, 0.21, 0.19, 0.20, 0.21, 0.19] {
            stats.update(m, time);
            time += 30000;
        }

        let _dynamic_exit =
            stats.rolling_median + (stats.config.k_exit * stats.get_spread_volatility());

        // Case 1: clamp at maximum
        stats.config.maximum_exit_threshold_pct = 0.1; // dynamic_exit (~0.21) > maximum (0.1)
        stats.config.minimum_exit_threshold_pct = 0.05;
        assert_eq!(stats.get_exit_threshold(time).unwrap(), 0.1);

        // Case 2: clamp at minimum
        stats.config.maximum_exit_threshold_pct = 0.5;
        stats.config.minimum_exit_threshold_pct = 0.4; // dynamic_exit (~0.21) < minimum (0.4)
        assert_eq!(stats.get_exit_threshold(time).unwrap(), 0.4);
    }

    #[test]
    fn test_spread_velocity_calculation() {
        let mut config = crate::config::DynamicSpreadConfig::default();
        config.velocity_window_ms = 400; // 0.4 seconds
        let mut stats = RollingStats::new(config, 0);

        stats.update(1.0, 1000);

        // At 1200, lookback target is 800. Tolerance is 80 (20% of 400).
        // Best sample is 1000, difference 200 > 80. So velocity should be 0.
        stats.update(1.2, 1200);
        assert_eq!(stats.spread_velocity, 0.0);

        // At 1400, target is 1000. Sample 1000 is available (difference 0 <= 80).
        // Spread changed from 1.0 (at 1000) to 1.8.
        // Velocity = (1.8 - 1.0) / 0.4 = 2.0
        stats.update(1.8, 1400);
        assert!((stats.spread_velocity - 2.0).abs() < 1e-7);

        // Test negative velocity
        // At 1800, target is 1400. Spread changed from 1.8 to 1.6
        // Velocity = (1.6 - 1.8) / 0.4 = -0.5
        stats.update(1.6, 1800);
        assert!((stats.spread_velocity - -0.5).abs() < 1e-7);
    }

    #[test]
    fn test_evaluate_entry() {
        let mut config = crate::config::DynamicSpreadConfig::default();
        config.warmup_seconds = 10;
        config.minimum_samples = 5;
        config.k_entry = 3.0;
        config.minimum_entry_threshold_pct = 0.5;
        config.min_volatility_floor_pct = 0.01;
        config.velocity_window_ms = 400;
        config.max_slippage_pct = 0.35;

        let startup_time = 1000;
        let mut stats = RollingStats::new(config, startup_time);

        // Seed normal data
        let mut time = startup_time;
        for &m in &[0.20, 0.22, 0.18, 0.20, 0.22, 0.18] {
            stats.update(m, time);
            time += 30000;
        }

        let _threshold = stats.get_entry_threshold(time).unwrap();
        // threshold is around 0.5 (minimum floor kicks in, or slightly above)
        // Let's assume threshold = 0.50.

        // Fail: Below threshold
        stats.update(0.40, time);
        assert!(stats.evaluate_entry(0.40, time).is_err());

        time += 400;

        // Fail: Above threshold, but negative velocity (0.40 -> 0.55) wait, that's positive velocity.
        // Let's make a positive velocity but way above slippage
        stats.update(0.95, time); // 0.95 - 0.50 = 0.45 > 0.35
        assert!(stats.evaluate_entry(0.95, time).is_err(), "Should fail max slippage");

        time += 400;

        // Fail: Negative velocity. From 0.95 to 0.60
        stats.update(0.60, time);
        assert!(stats.evaluate_entry(0.60, time).is_err(), "Should fail negative velocity");

        time += 400;

        // Pass: Positive velocity, above threshold, within slippage. From 0.60 to 0.70.
        stats.update(0.70, time);
        let velocity = stats.spread_velocity;
        assert!(velocity > 0.0);
        let current = 0.70;
        let entry_th = stats.get_entry_threshold(time).unwrap();
        assert!(current > entry_th);
        assert!(current - entry_th < 0.35);

        assert!(stats.evaluate_entry(0.70, time).is_ok(), "Should pass all conditions");
    }

    #[test]
    fn test_evaluate_exit_conditions() {
        let mut config = crate::config::DynamicSpreadConfig::default();
        config.warmup_seconds = 10;
        config.minimum_samples = 5;
        config.k_exit = 1.0;
        config.minimum_exit_threshold_pct = 0.1;
        config.maximum_exit_threshold_pct = 0.5;
        config.max_arbitrage_hold_time_ms = 60 * 60 * 1000;
        config.min_volatility_floor_pct = 0.01;

        let startup_time = 10_000_000;
        let mut stats = RollingStats::new(config, startup_time);

        let mut time = startup_time;
        for &m in &[0.30, 0.32, 0.28, 0.30, 0.32, 0.28] {
            stats.update(m, time);
            time += 30000;
        }

        let dynamic_exit = stats.get_exit_threshold(time).unwrap();
        assert!(dynamic_exit > 0.1 && dynamic_exit < 0.5);

        let open_time = time - 1000;

        assert_eq!(stats.evaluate_exit(0.40, time, open_time), false);
        assert_eq!(
            stats.evaluate_exit(dynamic_exit - 0.01, time, open_time),
            true
        );
        assert_eq!(stats.evaluate_exit(0.05, time, open_time), true);

        let old_open_time = time - (61 * 60 * 1000);
        assert_eq!(stats.evaluate_exit(0.40, time, old_open_time), true);
    }
}
