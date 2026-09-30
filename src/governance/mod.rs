//! Governance: everything that decides whether a request is allowed, and for
//! how much, *before* a single upstream byte is requested.
//!
//! Ordering on the hot path is deliberate and fixed:
//! 1. [`auth`]  — who is calling?
//! 2. [`policy`] — may this tenant call this model, at this size?
//! 3. [`limits`] — is there quota left, and may we reserve the cost?
//!
//! None of these sit inside the streaming loop, and none of them emit per-token
//! work.

pub mod auth;
pub mod ledger;
pub mod limits;
pub mod policy;

pub use auth::{
    ApiKeyAuthenticator, AuthError, Authenticator, JwtAuthenticator, KeyRecord, Principal, Scope,
    SCOPE_ADMIN, SCOPE_CHAT_STREAM, SCOPE_MODELS_READ,
};
pub use limits::{AdmitRequest, QuotaSplit, BudgetSnapshot, CostEstimate, LimitEngine, LimitError, Reservation, TenantUsage};
pub use policy::{AuthorizeError, Decision, PolicyEngine, Tenant, TenantRegistry};

use crate::config::LimitProfile;
use std::time::SystemTime;

/// UTC calendar day, used as the budget bucket key. Deliberately coarse: a
/// daily budget that rolls over at 00:00 UTC is predictable, which matters when
/// someone has to explain a spend graph to a customer.
pub fn budget_bucket(now: SystemTime) -> u64 {
    let secs = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    secs / 86_400
}

pub fn budget_bucket_label(bucket: u64) -> String {
    let (y, m, d) = civil_from_days(bucket as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Howard Hinnant's `civil_from_days`: days since 1970-01-01 -> (y, m, d).
/// Avoids pulling a date crate in for one metrics label.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

impl PolicyEngine {
    /// Effective limits for a tenant, falling back to the configured defaults.
    pub fn limits_for(&self, tenant: &Tenant) -> LimitProfile {
        tenant
            .limits
            .clone()
            .unwrap_or_default()
            .merged_with_defaults(&self.default_limits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn budget_bucket_is_a_utc_day() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        assert_eq!(budget_bucket(now), 19_675);
        assert_eq!(budget_bucket_label(19_675), "2023-11-14");
    }

    #[test]
    fn epoch_day_is_1970_01_01() {
        assert_eq!(budget_bucket_label(0), "1970-01-01");
    }

    #[test]
    fn leap_day_round_trips() {
        // 2024-02-29 is day 19782.
        assert_eq!(budget_bucket_label(19_782), "2024-02-29");
    }
}
