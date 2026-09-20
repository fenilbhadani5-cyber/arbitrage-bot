/// Latency profiling for the arbitrage pipeline.
///
/// Captures wall-clock timestamps at every stage from WebSocket book update
/// through order send, exchange ACK, and WS fill confirmation.
/// All timestamps are epoch milliseconds (UTC) for easy logging and analysis.
///
/// Pipeline stages:
///   book_update_ms  → WS delivered a fresh book quote
///   detected_ms     → Spread scan found this opportunity
///   pre_flight_ms   → All checks passed (spread, balance, book freshness)
///   order_send_ms   → tokio::join! fired (both legs sent simultaneously)
///   exchange_ack_ms → REST response received from both exchanges
///   ws_fill_ms      → WS fill event received (or REST fallback timeout)
use chrono::Utc;
use serde::{Deserialize, Serialize};

/// Captures epoch-millisecond timestamps and derived metrics for one trade's
/// full pipeline journey from book update to fill confirmation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TradeLatency {
    // ── Stage timestamps (epoch ms UTC) ──
    /// When the book update that triggered this opportunity was written to the store.
    /// Set from price_store::CoinPrices.{binance,bybit}_book_update_epoch_ms.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub book_update_ms: Option<i64>,

    /// When the trading loop scan found spread >= threshold.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub opportunity_detected_ms: Option<i64>,

    /// When all pre-trade checks passed (orderbook, spread, balance, margins).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pre_flight_check_ms: Option<i64>,

    /// Immediately before tokio::join! fires both orders (both legs sent simultaneously).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_send_ms: Option<i64>,

    /// When the REST response was received from both exchanges (join! returned).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange_ack_ms: Option<i64>,

    /// When the WS fill event was received (or REST fallback completed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ws_fill_ms: Option<i64>,

    // ── Derived latency metrics (milliseconds) ──
    /// detection_to_send: Time from opportunity detection to order fire.
    /// Measures: processing overhead, lock contention, pre-flight checks.
    /// Target: < 5ms. If > 20ms → processing bottleneck.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detection_to_send_ms: Option<i64>,

    /// send_to_ack: REST round-trip to both exchanges.
    /// Measures: network latency + exchange processing time.
    /// Target: < 150ms. If > 300ms → network/exchange issue.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send_to_ack_ms: Option<i64>,

    /// ack_to_fill: Time from REST ACK to WS fill confirmation.
    /// Measures: exchange matching + WS delivery delay.
    /// Target: < 50ms. If > 200ms → WS feed lagging.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ack_to_fill_ms: Option<i64>,

    /// book_to_send: Staleness of the book quote when the order was sent.
    /// Measures: how old the opportunity signal was when we acted.
    /// Target: < 50ms. If > 100ms → buffer drain / processing delay.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub book_to_send_ms: Option<i64>,

    /// total_pipeline_ms: End-to-end from book update to fill.
    /// = ws_fill_ms - book_update_ms (or exchange_ack_ms - book_update_ms if leg failed).
    /// Target: < 500ms. This is the total latency "budget" consumed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_pipeline_ms: Option<i64>,

    // ── Per-leg round-trip latencies (milliseconds) ──
    /// Round-trip time (in ms) for the buy leg order send -> fill/error response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buy_leg_rtt_ms: Option<i64>,

    /// Round-trip time (in ms) for the sell leg order send -> fill/error response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sell_leg_rtt_ms: Option<i64>,

    /// Round-trip time (in ms) for an emergency reversal order if one leg failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reversal_rtt_ms: Option<i64>,

    // ── Quote snapshots at each stage ──
    // Used to determine if market moved or if we had stale data.
    /// Best ask on the buy exchange at opportunity detection time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detected_buy_ask: Option<f64>,
    /// Best bid on the sell exchange at opportunity detection time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detected_sell_bid: Option<f64>,
    /// Computed spread at detection time (%).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detected_spread_pct: Option<f64>,

    /// Best ask on the buy exchange at pre-flight check time (re-read from store).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preflight_buy_ask: Option<f64>,
    /// Best bid on the sell exchange at pre-flight check time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preflight_sell_bid: Option<f64>,
    /// Computed spread at pre-flight time (%).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preflight_spread_pct: Option<f64>,

    // fill prices come from the fill itself (buy_fill_price, sell_fill_price in TradeRecord)

    // ── Latency diagnosis ──
    /// Human-readable diagnosis of the latency profile.
    /// e.g. "OK", "STALE_BOOK", "SLOW_REST", "SLOW_WS_FILL", "PROCESSING_DELAY", "SLOW_BUY_LEG", "SLOW_SELL_LEG"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_diagnosis: Option<String>,
}

impl TradeLatency {
    /// Create a new TradeLatency with only the book_update_ms set.
    pub fn new(book_update_epoch_ms: Option<i64>) -> Self {
        TradeLatency {
            book_update_ms: book_update_epoch_ms,
            ..Default::default()
        }
    }

    /// Get the current wall-clock time as epoch milliseconds (UTC).
    #[inline]
    pub fn now_ms() -> i64 {
        Utc::now().timestamp_millis()
    }

    /// Mark the opportunity detection timestamp.
    #[inline]
    pub fn mark_detected(&mut self, buy_ask: f64, sell_bid: f64, spread_pct: f64) {
        let ms = Self::now_ms();
        self.opportunity_detected_ms = Some(ms);
        self.detected_buy_ask = Some(buy_ask);
        self.detected_sell_bid = Some(sell_bid);
        self.detected_spread_pct = Some(spread_pct);
    }

    /// Mark the pre-flight check completion timestamp.
    #[inline]
    pub fn mark_pre_flight(&mut self, buy_ask: f64, sell_bid: f64, spread_pct: f64) {
        let ms = Self::now_ms();
        self.pre_flight_check_ms = Some(ms);
        self.preflight_buy_ask = Some(buy_ask);
        self.preflight_sell_bid = Some(sell_bid);
        self.preflight_spread_pct = Some(spread_pct);
    }

    /// Mark the order send timestamp (immediately before tokio::join!).
    #[inline]
    pub fn mark_order_send(&mut self) {
        self.order_send_ms = Some(Self::now_ms());
    }

    /// Mark the exchange ACK timestamp (after tokio::join! returns).
    #[inline]
    pub fn mark_exchange_ack(&mut self) {
        self.exchange_ack_ms = Some(Self::now_ms());
    }

    /// Mark the WS fill timestamp and compute all derived metrics.
    pub fn mark_ws_fill(&mut self) {
        let fill_ms = Self::now_ms();
        self.ws_fill_ms = Some(fill_ms);
        self.compute_derived();
    }

    /// Compute all derived latency metrics from the recorded timestamps.
    pub fn compute_derived(&mut self) {
        if let (Some(det), Some(send)) = (self.opportunity_detected_ms, self.order_send_ms) {
            self.detection_to_send_ms = Some(send - det);
        }
        if let (Some(send), Some(ack)) = (self.order_send_ms, self.exchange_ack_ms) {
            self.send_to_ack_ms = Some(ack - send);
        }
        if let (Some(ack), Some(fill)) = (self.exchange_ack_ms, self.ws_fill_ms) {
            self.ack_to_fill_ms = Some(fill - ack);
        }
        if let (Some(book), Some(send)) = (self.book_update_ms, self.order_send_ms) {
            self.book_to_send_ms = Some(send - book);
        }
        if let (Some(book), Some(fill)) = (self.book_update_ms, self.ws_fill_ms) {
            self.total_pipeline_ms = Some(fill - book);
        } else if self.total_pipeline_ms.is_none() {
            // If WS fill didn't happen (e.g. leg failure), compute total pipeline up to ACK
            if let (Some(book), Some(ack)) = (self.book_update_ms, self.exchange_ack_ms) {
                self.total_pipeline_ms = Some(ack - book);
            } else if let (Some(det), Some(ack)) =
                (self.opportunity_detected_ms, self.exchange_ack_ms)
            {
                self.total_pipeline_ms = Some(ack - det);
            }
        }

        // Diagnose the latency profile
        self.latency_diagnosis = Some(self.diagnose());
    }

    /// Produce a human-readable diagnosis string.
    fn diagnose(&self) -> String {
        let mut issues: Vec<&str> = Vec::new();

        if self.book_to_send_ms.unwrap_or(0) > 200 {
            issues.push("STALE_BOOK(>200ms)");
        } else if self.book_to_send_ms.unwrap_or(0) > 100 {
            issues.push("STALE_BOOK(>100ms)");
        }

        if self.detection_to_send_ms.unwrap_or(0) > 50 {
            issues.push("PROCESSING_DELAY(>50ms)");
        } else if self.detection_to_send_ms.unwrap_or(0) > 20 {
            issues.push("PROCESSING_DELAY(>20ms)");
        }

        if self.send_to_ack_ms.unwrap_or(0) > 500 {
            issues.push("SLOW_REST(>500ms)");
        } else if self.send_to_ack_ms.unwrap_or(0) > 200 {
            issues.push("SLOW_REST(>200ms)");
        }

        if let Some(buy_rtt) = self.buy_leg_rtt_ms {
            if buy_rtt > 400 {
                issues.push("SLOW_BUY_LEG(>400ms)");
            }
        }

        if let Some(sell_rtt) = self.sell_leg_rtt_ms {
            if sell_rtt > 400 {
                issues.push("SLOW_SELL_LEG(>400ms)");
            }
        }

        if self.ack_to_fill_ms.unwrap_or(0) > 200 {
            issues.push("SLOW_WS_FILL(>200ms)");
        } else if self.ack_to_fill_ms.unwrap_or(0) > 50 {
            issues.push("SLOW_WS_FILL(>50ms)");
        }

        if let (Some(det_ask), Some(fill_ask)) = (self.detected_buy_ask, self.preflight_buy_ask) {
            if det_ask > 0.0 {
                let slip = ((fill_ask - det_ask) / det_ask).abs() * 100.0;
                if slip > 0.2 {
                    issues.push("MARKET_MOVED(buy_ask)");
                }
            }
        }

        if issues.is_empty() {
            "OK".to_string()
        } else {
            issues.join("|")
        }
    }

    /// Log a formatted latency summary to stderr with UTC timestamp.
    pub fn log_summary(&self, coin: &str) {
        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let leg_rtt_str = match (self.buy_leg_rtt_ms, self.sell_leg_rtt_ms) {
            (Some(b), Some(s)) => format!(" (BuyLeg: {}ms, SellLeg: {}ms)", b, s),
            (Some(b), None) => format!(" (BuyLeg: {}ms)", b),
            (None, Some(s)) => format!(" (SellLeg: {}ms)", s),
            (None, None) => String::new(),
        };

        eprintln!(
            "[{}][Latency] {} | book→send: {}ms | send→ack: {}ms{} | ack→fill: {}ms | total: {}ms | diag: {}",
            ts,
            coin,
            self.book_to_send_ms.unwrap_or(-1),
            self.send_to_ack_ms.unwrap_or(-1),
            leg_rtt_str,
            self.ack_to_fill_ms.unwrap_or(-1),
            self.total_pipeline_ms.unwrap_or(-1),
            self.latency_diagnosis.as_deref().unwrap_or("?"),
        );
        eprintln!(
            "[{}][Latency] {} | detected: ask={:.6}/bid={:.6} spread={:.3}% | preflight: ask={:.6}/bid={:.6} spread={:.3}%",
            ts,
            coin,
            self.detected_buy_ask.unwrap_or(0.0),
            self.detected_sell_bid.unwrap_or(0.0),
            self.detected_spread_pct.unwrap_or(0.0),
            self.preflight_buy_ask.unwrap_or(0.0),
            self.preflight_sell_bid.unwrap_or(0.0),
            self.preflight_spread_pct.unwrap_or(0.0),
        );
    }

    /// Print a comprehensive, high-visibility latency audit to stderr when one leg fails.
    pub fn print_leg_failure_audit(
        &self,
        coin: &str,
        buy_exchange: crate::price_store::Exchange,
        sell_exchange: crate::price_store::Exchange,
        buy_ok: bool,
        sell_ok: bool,
        buy_rtt_ms: i64,
        sell_rtt_ms: i64,
        error_msg: &str,
        reversal_info: Option<(&str, i64, bool)>,
    ) {
        let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let buy_status = if buy_ok { "FILLED" } else { "FAILED" };
        let sell_status = if sell_ok { "FILLED" } else { "FAILED" };

        eprintln!("\n{}", "═".repeat(70));
        eprintln!(
            "[{}][LiveTrading] 🚨 ONE-LEG FAILURE LATENCY AUDIT: {}",
            ts, coin
        );
        eprintln!("{}", "═".repeat(70));
        eprintln!("  Failure Reason: {}", error_msg);
        eprintln!("  Round-Trip Timings:");
        eprintln!(
            "    • Combined Send→ACK RTT: {}ms",
            self.send_to_ack_ms.unwrap_or(buy_rtt_ms.max(sell_rtt_ms))
        );
        eprintln!(
            "    • Buy Leg  ({:<7}):   {:>4}ms [{}]",
            buy_exchange.to_string(),
            buy_rtt_ms,
            buy_status
        );
        eprintln!(
            "    • Sell Leg ({:<7}):  {:>4}ms [{}]",
            sell_exchange.to_string(),
            sell_rtt_ms,
            sell_status
        );
        if let Some((rev_ex, rev_ms, rev_ok)) = reversal_info {
            let rev_stat = if rev_ok {
                "SUCCESS"
            } else {
                "FAILED - MANUAL ACTION REQUIRED"
            };
            eprintln!(
                "    • Reversal ({:<7}):  {:>4}ms [{}]",
                rev_ex, rev_ms, rev_stat
            );
        }
        eprintln!("  Pipeline Latency:");
        eprintln!(
            "    • Book Update → Send:    {:>4}ms",
            self.book_to_send_ms.unwrap_or(-1)
        );
        eprintln!(
            "    • Detection → Send:      {:>4}ms",
            self.detection_to_send_ms.unwrap_or(-1)
        );
        eprintln!(
            "    • Total Pipeline Time:   {:>4}ms",
            self.total_pipeline_ms.unwrap_or(-1)
        );
        eprintln!(
            "    • Latency Diagnosis:     {}",
            self.latency_diagnosis.as_deref().unwrap_or("?")
        );
        eprintln!("  Quote & Spread Drift:");
        eprintln!(
            "    • Detected:  Ask={:.6} / Bid={:.6} (Spread: {:.3}%)",
            self.detected_buy_ask.unwrap_or(0.0),
            self.detected_sell_bid.unwrap_or(0.0),
            self.detected_spread_pct.unwrap_or(0.0),
        );
        eprintln!(
            "    • Preflight: Ask={:.6} / Bid={:.6} (Spread: {:.3}%)",
            self.preflight_buy_ask.unwrap_or(0.0),
            self.preflight_sell_bid.unwrap_or(0.0),
            self.preflight_spread_pct.unwrap_or(0.0),
        );
        eprintln!("{}\n", "═".repeat(70));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_latency_failure_derivation() {
        let mut latency = TradeLatency::new(Some(1000));
        latency.opportunity_detected_ms = Some(1005);
        latency.order_send_ms = Some(1010);
        latency.exchange_ack_ms = Some(1250);
        latency.buy_leg_rtt_ms = Some(70);
        latency.sell_leg_rtt_ms = Some(240);
        latency.reversal_rtt_ms = Some(80);
        latency.compute_derived();

        assert_eq!(latency.detection_to_send_ms, Some(5));
        assert_eq!(latency.send_to_ack_ms, Some(240));
        assert_eq!(latency.book_to_send_ms, Some(10));
        assert_eq!(latency.total_pipeline_ms, Some(250));
        assert_eq!(latency.buy_leg_rtt_ms, Some(70));
        assert_eq!(latency.sell_leg_rtt_ms, Some(240));
        assert_eq!(latency.reversal_rtt_ms, Some(80));

        let json = serde_json::to_string(&latency).expect("must serialize");
        assert!(json.contains("\"buy_leg_rtt_ms\":70"));
        assert!(json.contains("\"sell_leg_rtt_ms\":240"));
        assert!(json.contains("\"reversal_rtt_ms\":80"));
    }
}
