use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[allow(non_camel_case_types)]
pub enum RejectionReason {
    WARMUP,
    STALE_DATA,
    INSUFFICIENT_HISTORY,
    INSUFFICIENT_LIQUIDITY,
    SPREAD_BELOW_DYNAMIC_THRESHOLD,
    Z_SCORE_TOO_LOW,
    NET_EDGE_TOO_LOW,
    SIGNAL_TOO_OLD,
    SPREAD_COLLAPSING,
    RISK_LIMIT,
    COOLDOWN,
    INVALID_ORDERBOOK,
    DUPLICATE_POSITION,
    EXCHANGE_UNHEALTHY,
}

impl std::fmt::Display for RejectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
