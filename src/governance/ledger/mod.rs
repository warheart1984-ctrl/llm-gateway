//! The spend ledger: where reservations, settlements and daily spend live.
//!
//! Two backends behind one trait:
//!
//! * [`MemoryLedger`]: process memory. Exact and fast, but a restart forgets
//!   it and replicas do not share it. The default, for a single instance.
//! * [`PostgresLedger`]: one durable, transactional store shared by every
//!   gateway process. Survives restarts; replicas draw on one budget.
//!
//! Every change to money is one conditional state transition, so each
//! outcome is decided by stored state plus the request, never by timing:
//!
//! * admission is a single check-and-reserve (`spent < budget AND spent +
//!   amount <= budget`) that either commits the reservation or refuses;
//! * closing is `open -> settled | abandoned | released | committed`, applied
//!   only if the reservation is still `open`, so a duplicate close is inert;
//! * an idempotency key is claimed in the same step as the budget, so there
//!   is never a charge without a key record or a key record without a charge.
//!
//! The provider is contacted only after [`Ledger::try_reserve`] returns, i.e.
//! after the reservation is durable. "Called but unrecorded" cannot happen.

mod memory;
pub mod postgres;
pub mod sealed;

use std::{sync::Arc, time::SystemTime};

use uuid::Uuid;

pub use memory::MemoryLedger;
#[cfg(test)]
pub(crate) use memory::SpendLedger;
pub use postgres::PostgresLedger;

/// A request to put money on hold for one gateway request.
#[derive(Debug, Clone)]
pub struct NewReservation<'a> {
    /// The request id. Unique per attempt; the handle every closing uses.
    pub id: Uuid,
    pub tenant_id: &'a str,
    /// Wall clock for the memory backend's day bucket. The Postgres backend
    /// ignores it and asks the database, so replicas cannot disagree about
    /// which day it is.
    pub now: SystemTime,
    pub amount_nano_usd: u64,
    pub prompt_nano_usd: u64,
    /// `0` means no ceiling.
    pub budget_nano_usd: u64,
    pub idempotency: Option<IdempotencyClaim<'a>>,
}

/// A client-supplied idempotency key and the fingerprint of the request it
/// was sent with. The same key with a different fingerprint is a client bug,
/// not a retry, and is refused rather than guessed at.
#[derive(Debug, Clone)]
pub struct IdempotencyClaim<'a> {
    pub key: &'a str,
    pub fingerprint: [u8; 32],
}

/// How a reservation was closed. The first closing decides the bill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Settled,
    Abandoned,
    Released,
    Committed,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Settled => "settled",
            Outcome::Abandoned => "abandoned",
            Outcome::Released => "released",
            Outcome::Committed => "committed",
        }
    }
}

/// A closing, applied at most once per reservation.
#[derive(Debug, Clone)]
pub struct Closing {
    pub id: Uuid,
    pub tenant_id: Arc<str>,
    /// The day the reservation was charged to.
    pub bucket: u64,
    /// Correction relative to the reservation: actual minus reserved.
    pub delta_nano_usd: i128,
    pub outcome: Outcome,
    /// Wall clock for the memory backend's rollover rule. The Postgres
    /// backend uses the database clock when it applies the closing.
    pub at: SystemTime,
    /// The answer, kept so a replay under the same idempotency key can be
    /// served without calling the provider again. Completions only.
    pub response: Option<String>,
}

/// Why the ledger refused a reservation. Each is a final answer for this
/// attempt; none of them reached the provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerRefusal {
    BudgetExhausted { spent: u64, budget: u64 },
    BudgetWouldBeExceeded { estimate: u64, remaining: u64 },
    /// Same key, different request.
    IdempotencyKeyReused,
    /// Same key, and the first attempt has not finished. `original` is
    /// `None` when two first attempts raced and the other one won the claim.
    InProgress { original: Option<Uuid> },
    /// Same key, and the first attempt finished and was billed.
    Duplicate {
        original: Uuid,
        billed_nano_usd: u64,
        response: Option<String>,
    },
    /// The ledger could not be reached or did not answer in time. The
    /// request is refused: admitting without a ledger is failing open.
    Unavailable(String),
}

#[async_trait::async_trait]
pub trait Ledger: Send + Sync + std::fmt::Debug {
    /// Check the budget, claim the idempotency key if any, and reserve, as
    /// one atomic step. Returns the day bucket charged.
    async fn try_reserve(&self, r: NewReservation<'_>) -> Result<u64, LedgerRefusal>;

    /// Close a reservation. Synchronous because it is called from `Drop`:
    /// the memory backend applies it at once, the Postgres backend queues it
    /// for its writer (see [`Ledger::flush`]).
    fn close(&self, c: Closing);

    /// `(day bucket, spent)` for the tenant's current day.
    async fn snapshot(&self, tenant_id: &str, now: SystemTime) -> Result<(u64, u64), LedgerRefusal>;

    /// Wait until every closing accepted so far is durable.
    async fn flush(&self);

    /// Whether the ledger can serve admissions right now. Readiness uses it.
    async fn healthy(&self) -> bool;

    /// Backend name, for logs and readiness.
    fn backend(&self) -> &'static str;
}

/// SHA-256 over a canonical rendering of the request: object keys sorted at
/// every level, so the same request sent with its keys in another order is
/// the same request. `scope` separates endpoints: one key used for a stream
/// and then a completion is two different requests.
pub fn fingerprint(scope: &str, request: &serde_json::Value) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut canonical = String::with_capacity(256);
    canonical.push_str(scope);
    canonical.push('\n');
    write_canonical(request, &mut canonical);
    Sha256::digest(canonical.as_bytes()).into()
}

fn write_canonical(value: &serde_json::Value, out: &mut String) {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).unwrap_or_default());
                out.push(':');
                write_canonical(&map[key], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

/// Validate a client's `Idempotency-Key`: 1 to 255 visible ASCII characters.
pub fn valid_idempotency_key(key: &str) -> bool {
    !key.is_empty() && key.len() <= 255 && key.bytes().all(|b| (0x21..=0x7e).contains(&b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_fingerprint_ignores_key_order_but_not_content_or_endpoint() {
        let a = json!({ "model": "m", "params": { "max_tokens": 5, "temperature": 0.1 } });
        let b = json!({ "params": { "temperature": 0.1, "max_tokens": 5 }, "model": "m" });
        let c = json!({ "model": "m", "params": { "max_tokens": 6, "temperature": 0.1 } });
        assert_eq!(fingerprint("stream", &a), fingerprint("stream", &b));
        assert_ne!(fingerprint("stream", &a), fingerprint("stream", &c));
        assert_ne!(fingerprint("stream", &a), fingerprint("complete", &a));
    }

    #[test]
    fn idempotency_keys_are_bounded_visible_ascii() {
        assert!(valid_idempotency_key("order-2026-09-29/42"));
        assert!(!valid_idempotency_key(""));
        assert!(!valid_idempotency_key("has space"));
        assert!(!valid_idempotency_key(&"k".repeat(256)));
        assert!(!valid_idempotency_key("naïve"));
    }
}
