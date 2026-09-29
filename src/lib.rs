//! A governed, multi-tenant streaming LLM gateway.
//!
//! One SSE front door (`POST /v1/chat/stream`) routing to Groq, OpenRouter and
//! NVIDIA NIM. Layering, outermost first:
//!
//! ```text
//!   api          HTTP surface, SSE framing, per-request accounting
//!   governance   auth -> policy -> limits, all pre-flight
//!   router       model resolution, parameter merge, cost estimation
//!   providers    per-vendor adapters hiding their quirks
//! ```
//!
//! Design rules that the rest of the code depends on:
//!
//! * **No per-token work on the hot path.** No logging, no per-token metrics,
//!   no allocation-heavy bookkeeping. One summary line per stream.
//! * **Governance runs before the first byte**, never mid-stream, so a
//!   rejection is a real HTTP status instead of an error hidden in a 200.
//! * **The model catalogue is config, not code.** No model id is hardcoded.
//! * **Versioned contract.** The normalized frame shapes are frozen under
//!   [`api::chat::API_VERSION`].

pub mod api;
pub mod bootstrap;
pub mod config;
pub mod governance;
pub mod observability;
pub mod providers;
pub mod router;
pub mod state;

pub use config::Settings;
pub use state::AppState;

/// Wire contract version, surfaced in every response header.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
