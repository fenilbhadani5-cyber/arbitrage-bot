#[derive(Debug, Clone)]
pub struct DynamicSpreadConfig {
    /// Warmup time in seconds before trading is allowed.
    pub warmup_seconds: u64,
    /// Minimum number of samples required before trading is allowed.
    pub minimum_samples: usize,
    /// Rolling window size in seconds for computing median and MAD.
    pub stats_window_seconds: u64,
    /// Bucket size in seconds for the rolling window.
    pub stats_bucket_seconds: u64,
    /// Entry multiplier for spread volatility.
    pub k_entry: f64,
    /// Exit multiplier for spread volatility (must be < k_entry).
    pub k_exit: f64,
    /// Minimum entry threshold (1.0 means 1.0%).
    pub minimum_entry_threshold_pct: f64,
    /// Minimum exit threshold (operator-set).
    pub minimum_exit_threshold_pct: f64,
    /// Maximum exit threshold (operator-set).
    pub maximum_exit_threshold_pct: f64,
    /// Minimum z-score required for entry.
    pub minimum_entry_z_score: f64,
    /// Minimum volatility floor (1.0 means 1.0%).
    pub min_volatility_floor_pct: f64,
    /// Minimum net edge percentage (1.0 means 1.0%).
    pub minimum_net_edge_pct: f64,
    /// Base safety buffer percentage (operator-set).
    pub base_safety_buffer_pct: f64,
    /// Maximum safety buffer percentage (operator-set).
    pub max_safety_buffer_pct: f64,
    /// Maximum age of a signal in milliseconds.
    pub max_signal_age_ms: u64,
    /// Window for velocity calculation in milliseconds (must be <= max_signal_age_ms).
    pub velocity_window_ms: u64,
    /// Depth of crossing history to retain.
    pub crossing_history_depth: usize,
    /// Multiplier for winsorization cap.
    pub outlier_cap_multiplier: f64,
    /// Maximum age of market data in milliseconds (operator-set).
    pub max_data_age_ms: u64,
    /// Maximum hold time in milliseconds (operator-set).
    pub max_arbitrage_hold_time_ms: u64,
    /// Maximum slippage percentage (operator-set).
    pub max_slippage_pct: f64,
    /// Maximum depth levels to walk in the order book (operator-set).
    pub max_depth_levels: usize,
    /// Minimum liquidity (operator-set).
    pub minimum_liquidity: f64,
    /// Cooldown between trades in milliseconds (operator-set).
    pub cooldown_ms: u64,
    /// Spread collapse rate threshold in percentage per second (operator-set).
    pub collapse_rate_threshold_pct_per_sec: f64,
}

impl Default for DynamicSpreadConfig {
    fn default() -> Self {
        Self {
            warmup_seconds: 45,
            minimum_samples: 60,
            stats_window_seconds: 14400,
            stats_bucket_seconds: 30,
            k_entry: 3.0,
            k_exit: 1.0,
            minimum_entry_threshold_pct: 0.60,
            // Operator-set parameters below, provided with sensible defaults
            minimum_exit_threshold_pct: 0.10,
            maximum_exit_threshold_pct: 0.50,
            minimum_entry_z_score: 3.0,
            min_volatility_floor_pct: 0.005,
            minimum_net_edge_pct: 0.25,
            base_safety_buffer_pct: 0.05,
            max_safety_buffer_pct: 0.25,
            max_signal_age_ms: 400,
            velocity_window_ms: 400, // Reduced from 500 to 400 to satisfy invariant: velocity_window_ms <= max_signal_age_ms
            crossing_history_depth: 5,
            outlier_cap_multiplier: 8.0,
            max_data_age_ms: 2000,
            max_arbitrage_hold_time_ms: 600000, // 10 minutes
            max_slippage_pct: 0.35,
            max_depth_levels: 10,
            minimum_liquidity: 100.0,
            cooldown_ms: 5000,
            collapse_rate_threshold_pct_per_sec: 0.10,
        }
    }
}

impl DynamicSpreadConfig {
    /// Load-time validation to ensure configuration invariants are met.
    /// Fails fast by panicking with a clear error message.
    pub fn validate(&self) {
        if self.k_exit >= self.k_entry {
            panic!("Configuration Error: k_exit ({}) must be < k_entry ({})", self.k_exit, self.k_entry);
        }
        if self.minimum_entry_threshold_pct <= 0.0 {
            panic!("Configuration Error: minimum_entry_threshold_pct ({}) must be > 0", self.minimum_entry_threshold_pct);
        }
        if self.velocity_window_ms > self.max_signal_age_ms {
            panic!("Configuration Error: velocity_window_ms ({}) must be <= max_signal_age_ms ({})", self.velocity_window_ms, self.max_signal_age_ms);
        }
        if self.minimum_exit_threshold_pct > self.maximum_exit_threshold_pct {
            panic!("Configuration Error: minimum_exit_threshold_pct ({}) must be <= maximum_exit_threshold_pct ({})", self.minimum_exit_threshold_pct, self.maximum_exit_threshold_pct);
        }
        if self.minimum_samples == 0 || self.warmup_seconds == 0 {
            panic!("Configuration Error: minimum_samples ({}) and warmup_seconds ({}) must both be > 0", self.minimum_samples, self.warmup_seconds);
        }
        
        // Unit convention checks: 1.0 means 1.0%. Assert plausibility for percentage-like values.
        let binance_fee = taker_fee(crate::price_store::Exchange::Binance) * 100.0;
        if binance_fee >= 2.0 {
            panic!("Configuration Error: Binance fee appears to be >= 2.0% ({}%). Verify unit convention (1.0 = 1.0%).", binance_fee);
        }
        if self.max_slippage_pct >= 5.0 {
            panic!("Configuration Error: max_slippage_pct ({}) appears implausibly high (>= 5.0%). Expected unit convention is 1.0 = 1.0%.", self.max_slippage_pct);
        }
        if self.minimum_exit_threshold_pct >= 5.0 {
            panic!("Configuration Error: minimum_exit_threshold_pct ({}) appears implausibly high (>= 5.0%). Expected unit convention is 1.0 = 1.0%.", self.minimum_exit_threshold_pct);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dynamic_config_happy_path() {
        let config = DynamicSpreadConfig::default();
        config.validate(); // Should not panic
    }

    #[test]
    #[should_panic(expected = "must be < k_entry")]
    fn test_dynamic_config_k_exit_ge_k_entry() {
        let mut config = DynamicSpreadConfig::default();
        config.k_exit = 4.0;
        config.k_entry = 3.0;
        config.validate();
    }

    #[test]
    #[should_panic(expected = "must be > 0")]
    fn test_dynamic_config_minimum_entry_threshold_pct_le_zero() {
        let mut config = DynamicSpreadConfig::default();
        config.minimum_entry_threshold_pct = 0.0;
        config.validate();
    }

    #[test]
    #[should_panic(expected = "must be <= max_signal_age_ms")]
    fn test_dynamic_config_velocity_window_ms_gt_max_signal_age_ms() {
        let mut config = DynamicSpreadConfig::default();
        config.velocity_window_ms = 500;
        config.max_signal_age_ms = 400;
        config.validate();
    }

    #[test]
    #[should_panic(expected = "must be <= maximum_exit_threshold_pct")]
    fn test_dynamic_config_min_exit_gt_max_exit() {
        let mut config = DynamicSpreadConfig::default();
        config.minimum_exit_threshold_pct = 0.60;
        config.maximum_exit_threshold_pct = 0.50;
        config.validate();
    }

    #[test]
    #[should_panic(expected = "must both be > 0")]
    fn test_dynamic_config_minimum_samples_zero() {
        let mut config = DynamicSpreadConfig::default();
        config.minimum_samples = 0;
        config.validate();
    }

    #[test]
    #[should_panic(expected = "must both be > 0")]
    fn test_dynamic_config_warmup_seconds_zero() {
        let mut config = DynamicSpreadConfig::default();
        config.warmup_seconds = 0;
        config.validate();
    }

    #[test]
    #[should_panic(expected = "appears implausibly high")]
    fn test_dynamic_config_plausibility_slippage() {
        let mut config = DynamicSpreadConfig::default();
        config.max_slippage_pct = 6.0; // Over the 5.0% cap
        config.validate();
    }

    // Since taker_fee is hardcoded to 0.0005 (0.05%), we can't easily change it in this test without modifying the function.
    // The validation function reads it dynamically. We will just trust that the function works, or we can mock it.
    // For now, testing slippage covers the plausibility of the unit convention.
}
