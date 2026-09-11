use crate::config::FUNDING_PAUSE_MINUTES;
use crate::price_store::FundingStore;
use chrono::{Timelike, Utc};

/// Check if a coin is near its funding time and should be paused.
///
/// Funding times are at fixed UTC intervals:
/// - 1h coins: every hour (00:00, 01:00, 02:00, ...) — MOST VOLATILE
/// - 4h coins: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00
/// - 8h coins: 00:00, 08:00, 16:00
///
/// Returns true if current time is within ±FUNDING_PAUSE_MINUTES of any
/// funding time for this coin's interval.
#[inline]
pub fn is_near_funding(coin: &str, funding_store: &FundingStore) -> bool {
    let funding_hours = funding_store.get(coin).map(|r| *r.value()).unwrap_or(8);
    is_near_funding_with_interval(funding_hours)
}

/// Check if current time is near a funding timestamp given the interval in hours.
#[inline]
fn is_near_funding_with_interval(interval_hours: u32) -> bool {
    let now = Utc::now();
    let current_minute_of_day = (now.hour() as i64) * 60 + (now.minute() as i64);

    let interval_minutes = (interval_hours as i64) * 60;

    if interval_minutes <= 0 {
        return false;
    }

    // Find the nearest funding time (in minutes since midnight UTC)
    // Funding times: 0, interval, 2*interval, ...
    let current_slot = current_minute_of_day / interval_minutes;
    let prev_funding = current_slot * interval_minutes;
    let next_funding = (current_slot + 1) * interval_minutes;

    // Distance to nearest funding boundary
    let dist_prev = current_minute_of_day - prev_funding;
    let dist_next = next_funding - current_minute_of_day;
    let min_distance = dist_prev.min(dist_next);

    min_distance <= FUNDING_PAUSE_MINUTES
}

/// Get time remaining until next funding (in seconds) for display purposes.
pub fn time_to_next_funding(coin: &str, funding_store: &FundingStore) -> i64 {
    let funding_hours = funding_store.get(coin).map(|r| *r.value()).unwrap_or(8);
    let now = Utc::now();
    let current_seconds_of_day =
        (now.hour() as i64) * 3600 + (now.minute() as i64) * 60 + (now.second() as i64);

    let interval_seconds = (funding_hours as i64) * 3600;
    if interval_seconds <= 0 {
        return 0;
    }

    let current_slot = current_seconds_of_day / interval_seconds;
    let next_funding = (current_slot + 1) * interval_seconds;
    next_funding - current_seconds_of_day
}

/// Return a human-readable string like "4m 32s" for the time to next funding.
pub fn funding_countdown(coin: &str, funding_store: &FundingStore) -> String {
    let secs = time_to_next_funding(coin, funding_store);
    if secs <= 0 {
        return "NOW".to_string();
    }
    let mins = secs / 60;
    let s = secs % 60;
    if mins > 60 {
        let hours = mins / 60;
        let m = mins % 60;
        format!("{}h {}m", hours, m)
    } else {
        format!("{}m {}s", mins, s)
    }
}

/// Check if ANY coin that's in a currently open position is near funding.
/// Used as a global safety check.
#[allow(dead_code)]
pub fn is_any_position_near_funding(
    open_coins: &[String],
    funding_store: &FundingStore,
) -> Vec<String> {
    let mut near_funding = Vec::new();
    for coin in open_coins {
        if is_near_funding(coin, funding_store) {
            near_funding.push(coin.clone());
        }
    }
    near_funding
}
