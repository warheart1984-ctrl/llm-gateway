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

pub mod decisions;
mod memory;
pub mod postgres;
pub mod sealed;
pub mod sqlite;

use std::{sync::Arc, time::SystemTime};

use uuid::Uuid;

pub use memory::MemoryLedger;
#[cfg(test)]
pub(crate) use memory::SpendLedger;
pub use postgres::PostgresLedger;
pub use sqlite::SqliteLedger;

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
    /// The encoding stored for a new record: keyed, with the current key.
    pub fingerprint: Vec<u8>,
    /// Other encodings of this same request that a stored record may carry:
    /// under an older fingerprint key still configured, or the unkeyed form
    /// used before keys existed. Lets records outlive a key rotation.
    pub also_matches: Vec<Vec<u8>>,
}

impl IdempotencyClaim<'_> {
    /// Whether a stored fingerprint is this request's.
    pub fn matches(&self, stored: &[u8]) -> bool {
        stored == self.fingerprint.as_slice() || self.also_matches.iter().any(|f| f.as_slice() == stored)
    }
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

    /// Record a routine refusal or failure. Never blocks and never fails the
    /// caller: if it cannot be kept, it is dropped and counted.
    fn record_decision(&self, decision: decisions::Decision);

    /// Record an operator action before it takes effect. An error means the
    /// action must not happen.
    async fn record_decision_durably(&self, decision: decisions::Decision) -> Result<(), LedgerRefusal>;

    /// Recorded decisions, newest first.
    async fn decisions(&self, query: decisions::DecisionQuery)
    -> Result<Vec<decisions::DecisionRecord>, LedgerRefusal>;

    /// Routine records dropped since the process started.
    fn decisions_dropped(&self) -> u64;
}

/// The request, rendered canonically: object keys sorted at every level, so
/// the same request sent with its keys in another order is the same request.
/// `scope` separates endpoints: one key used for a stream and then a
/// completion is two different requests.
fn canonical(scope: &str, request: &serde_json::Value) -> String {
    let mut out = String::with_capacity(256);
    out.push_str(scope);
    out.push('\n');
    write_canonical(request, &mut out);
    out
}

/// Unkeyed SHA-256 of the canonical request: the form stored before
/// fingerprint keys existed, still accepted when comparing.
pub fn fingerprint(scope: &str, request: &serde_json::Value) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(canonical(scope, request).as_bytes()).into()
}

/// Keyed request fingerprints.
///
/// A fingerprint stored in the ledger identifies a request. Unkeyed, it is a
/// plain hash of the prompt: anyone who can read the table can confirm a
/// guess ("did this tenant send X?") by hashing it. With a key it is an
/// HMAC-SHA256, and confirming a guess needs the key, which lives in the
/// environment, not the database.
///
/// Stored form, keyed: `b"h1"`, one length byte, the key id, then the 32-byte
/// MAC. Unkeyed: the 32 raw digest bytes. The lengths never collide.
/// Keys: `kid:base64[,kid:base64...]`, each at least 32 bytes; the first key
/// fingerprints new records, every key matches old ones.
#[derive(Clone)]
pub struct Fingerprinter {
    keys: Vec<(String, Vec<u8>)>,
}

impl std::fmt::Debug for Fingerprinter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ids: Vec<&str> = self.keys.iter().map(|(id, _)| id.as_str()).collect();
        f.debug_struct("Fingerprinter").field("key_ids", &ids).finish()
    }
}

impl Fingerprinter {
    /// Plain SHA-256 fingerprints: the behaviour without a configured key.
    pub fn unkeyed() -> Self {
        Self { keys: Vec::new() }
    }

    pub fn from_keys(spec: &str) -> Result<Self, String> {
        use base64::{Engine, engine::general_purpose::STANDARD};
        let mut keys: Vec<(String, Vec<u8>)> = Vec::new();
        for (position, entry) in spec.split(',').map(str::trim).filter(|e| !e.is_empty()).enumerate() {
            let (id, encoded) = entry
                .split_once(':')
                .ok_or_else(|| format!("fingerprint key #{} must be `kid:base64key`", position + 1))?;
            let valid_id = !id.is_empty()
                && id.len() <= 32
                && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
            if !valid_id {
                return Err(format!(
                    "fingerprint key #{} has an invalid id: use 1 to 32 letters, digits, `_` or `-`",
                    position + 1
                ));
            }
            let bytes = STANDARD
                .decode(encoded)
                .map_err(|_| format!("fingerprint key `{id}` is not valid base64"))?;
            if bytes.len() < 32 {
                return Err(format!("fingerprint key `{id}` must be at least 32 bytes, got {}", bytes.len()));
            }
            if keys.iter().any(|(existing, _)| existing == id) {
                return Err(format!("fingerprint key id `{id}` appears twice"));
            }
            keys.push((id.to_string(), bytes));
        }
        if keys.is_empty() {
            return Err("no fingerprint keys given".into());
        }
        Ok(Self { keys })
    }

    pub fn is_keyed(&self) -> bool {
        !self.keys.is_empty()
    }

    fn keyed(id: &str, key: &[u8], canonical: &str) -> Vec<u8> {
        use hmac::{Hmac, Mac};
        let mut mac = <Hmac<sha2::Sha256> as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
        mac.update(canonical.as_bytes());
        let mut out = Vec::with_capacity(3 + id.len() + 32);
        out.extend_from_slice(b"h1");
        out.push(id.len() as u8);
        out.extend_from_slice(id.as_bytes());
        out.extend_from_slice(&mac.finalize().into_bytes());
        out
    }

    /// The claim for this request: fingerprinted with the current key, and
    /// matching every encoding a stored record of the same request may carry.
    pub fn claim<'a>(&self, key: &'a str, scope: &str, request: &serde_json::Value) -> IdempotencyClaim<'a> {
        let canonical = canonical(scope, request);
        let plain = fingerprint(scope, request).to_vec();
        let mut encodings: Vec<Vec<u8>> =
            self.keys.iter().map(|(id, k)| Self::keyed(id, k, &canonical)).collect();
        encodings.push(plain);
        let fingerprint = encodings.remove(0);
        IdempotencyClaim { key, fingerprint, also_matches: encodings }
    }
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

    const K1: &str = "k1:AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";
    const K2: &str = "k2:ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8=";

    #[test]
    fn a_keyed_fingerprint_does_not_reveal_the_request_to_a_guesser() {
        let request = json!({ "messages": [{ "role": "user", "content": "yes" }] });
        let claim = Fingerprinter::from_keys(K1).unwrap().claim("idem", "stream", &request);
        // Someone reading the table hashes a guess the unkeyed way: no match.
        let guess = fingerprint("stream", &request);
        assert_ne!(claim.fingerprint.as_slice(), &guess[..]);
        assert!(claim.fingerprint.starts_with(b"h1"));
        // The same request, keys reordered, still has the same fingerprint.
        let reordered: serde_json::Value = serde_json::from_str(&request.to_string()).unwrap();
        let again = Fingerprinter::from_keys(K1).unwrap().claim("idem", "stream", &reordered);
        assert_eq!(claim.fingerprint, again.fingerprint);
    }

    #[test]
    fn records_made_before_keys_or_before_a_rotation_still_match() {
        let request = json!({ "model": "m", "messages": [] });
        let unkeyed = Fingerprinter::unkeyed().claim("idem", "stream", &request);
        let under_k1 = Fingerprinter::from_keys(K1).unwrap().claim("idem", "stream", &request);
        let rotated = Fingerprinter::from_keys(&format!("{K2},{K1}")).unwrap().claim("idem", "stream", &request);
        assert!(rotated.fingerprint.starts_with(b"h1\x02k2"), "the first key fingerprints new records");
        assert!(rotated.matches(&unkeyed.fingerprint), "a record from before keys existed");
        assert!(rotated.matches(&under_k1.fingerprint), "a record from before the rotation");
        let other = Fingerprinter::from_keys(K1).unwrap().claim("idem", "stream", &json!({ "model": "other" }));
        assert!(!rotated.matches(&other.fingerprint), "a different request never matches");
        let retired = Fingerprinter::from_keys(K2).unwrap().claim("idem", "stream", &request);
        assert!(!retired.matches(&under_k1.fingerprint), "a retired key's records no longer match");
    }

    #[test]
    fn malformed_fingerprint_keys_are_refused_without_echoing_them() {
        for (spec, fragment) in [
            ("", "no fingerprint keys"),
            ("k1", "kid:base64key"),
            ("k1:not-base64!!", "not valid base64"),
            ("k1:AAAA", "at least 32 bytes"),
            (&format!("{K1},{K1}"), "twice"),
        ] {
            let err = Fingerprinter::from_keys(spec).unwrap_err();
            assert!(err.contains(fragment), "{spec:?} -> {err}");
            assert!(!err.contains("AQIDBAUG"), "key material in error: {err}");
        }
        assert!(!format!("{:?}", Fingerprinter::from_keys(K1).unwrap()).contains("AQIDBAUG"));
    }
}
