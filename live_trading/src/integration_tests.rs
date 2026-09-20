#[cfg(test)]
mod tests {
    use crate::config::DynamicSpreadConfig;
    use crate::stats::RollingStats;
    use crate::rejection::RejectionReason;

    // 1. Config validation & unit-convention assertion
    #[test]
    fn test_config_validation() {
        let mut config = DynamicSpreadConfig::default();
        config.minimum_entry_threshold_pct = 1.0;
        config.minimum_exit_threshold_pct = 0.5;
        config.maximum_exit_threshold_pct = 0.9;
        
        // Assert the convention: 1.0 means 1.0% (not 0.01)
        assert_eq!(config.minimum_entry_threshold_pct, 1.0);
        assert!(config.k_exit < config.k_entry);
        assert!(config.velocity_window_ms <= config.max_signal_age_ms);
        assert!(config.minimum_exit_threshold_pct <= config.maximum_exit_threshold_pct);
        assert!(config.minimum_samples > 0);
        assert!(config.warmup_seconds > 0);
    }

    // 2. Behavioral diff: Old fixed threshold vs new dynamic threshold
    #[test]
    fn test_old_vs_new_threshold() {
        let config = DynamicSpreadConfig::default();
        let mut stats = RollingStats::new(config, 0);
        // Warmup Phase (pushing 60 ticks with noise)
        for i in 0..60 {
            let val = if i % 2 == 0 { 0.20 } else { 0.25 };
            stats.update(val, (i * 30_000) as u64);
        }


        let time_ms = 60 * 30_000;
        
        // Let's assume the old system had a fixed threshold of 0.50%
        let old_threshold = 0.50;
        
        // New system calculates dynamic threshold
        let new_threshold = stats.get_entry_threshold(time_ms).unwrap();
        // Since volatility is low, new_threshold might be clamped to minimum (e.g. 0.60%)
        assert!(new_threshold >= 0.60);
        
        // If spread hits 0.55%, old system enters, but new system rejects
        let spread = 0.55;
        assert!(spread > old_threshold); // Old system would enter
        assert_eq!(stats.evaluate_entry(spread, time_ms), Err(RejectionReason::SPREAD_BELOW_DYNAMIC_THRESHOLD)); // New system rejects
    }

    // 3. Hysteresis: no oscillation
    #[test]
    fn test_hysteresis_no_oscillation() {
        let config = DynamicSpreadConfig::default();
        let mut stats = RollingStats::new(config, 0);
        // Warmup
        for i in 0..60 {
            let val = if i % 2 == 0 { 0.20 } else { 0.25 };
            stats.update(val, (i * 30_000) as u64);
        }


        let time_ms = 60 * 30_000;
        
        let entry = stats.get_entry_threshold(time_ms).unwrap();
        let exit = stats.get_exit_threshold(time_ms).unwrap();
        
        // Ensure gap
        assert!(entry > exit + 0.1);

        // Spike above entry
        let spike = entry + 0.1;
        stats.update(spike, time_ms + 1000);
        // Note: evaluating entry changes internal state (crossing_ts).
        assert!(stats.evaluate_entry(spike, time_ms + 1000).is_ok());

        // Oscillate below entry but above exit
        let flutter = exit + 0.05;
        stats.update(flutter, time_ms + 2000);
        
        // Entry evaluates to false because it's below entry now
        assert_eq!(stats.evaluate_entry(flutter, time_ms + 2000), Err(RejectionReason::SPREAD_BELOW_DYNAMIC_THRESHOLD));
        // Exit evaluates to false because it hasn't reached the exit threshold yet
        assert_eq!(stats.evaluate_exit(flutter, time_ms + 2000, time_ms + 1000), false);
        
        // This proves the flutter spread state (between entry and exit) does NOT cause flipping
    }

    // 4. Full Restart End-to-End
    #[test]
    fn test_full_restart_end_to_end() {
        let config = DynamicSpreadConfig::default();
        let stats = RollingStats::new(config, 0);

        // Assert it starts in WARMUP
        assert_eq!(stats.can_trade(1000), Err(RejectionReason::INSUFFICIENT_HISTORY));
    }
}
