//! Observability. Deliberately off the hot path: the streaming loop itself
//! writes no logs and no per-token metrics.

pub mod logging;
pub mod metrics;

pub use logging::{RequestSpan, StreamSummary};
pub use metrics::{Metrics, RejectionKind, SharedMetrics};
