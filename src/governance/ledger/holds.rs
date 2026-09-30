//! HOLD: the decision between GO and NO-GO.
//!
//! A tenant's policy can say that some requests wait for a person: requests
//! for certain models, or whose worst-case cost is above a threshold. Such a
//! request is not executed and not charged. It becomes a *hold*, and the
//! caller gets `202` with the hold's id.
//!
//! ```text
//! requested -> pending -> approved -> consumed      (executed once)
//!                     \-> denied                    (never executed)
//!                     \-> expired                   (nobody decided in time)
//!              approved -> denied                   (approval revoked before use)
//!              approved -> expired                  (approval not used in time)
//! ```
//!
//! * **What is kept.** The request's keyed fingerprint, model, output cap and
//!   worst-case cost. Never the prompt: the client keeps its request and
//!   sends it again with `Hold-Id` once approved, and the fingerprint is how
//!   the gateway knows it is the same request.
//! * **Who decides.** A key with the `approve:holds` scope, granted
//!   explicitly: neither `admin` nor a wildcard grant implies it. The key
//!   that made the request can never decide its own hold.
//! * **Once.** The approval is consumed in the admission transaction, with
//!   the budget. Everything else is checked again at that moment: the key,
//!   the tenant, the model policy, the budget, the rate and concurrency
//!   limits. If the provider then refuses before accepting anything, the
//!   reservation is released and the approval restored, like an idempotency
//!   key; once anything was executed, it stays consumed.
//! * **Recorded.** Every transition goes on the decision record in the same
//!   transaction as the transition itself. If the record cannot be written,
//!   the transition does not happen.
//! * **No money is held while waiting.** The budget is charged when the
//!   approved request is executed, not when it is held: a pending hold that
//!   reserved money would let a flood of requests nobody approved exhaust a
//!   tenant's budget. The cost of that choice: an approved request can still
//!   be refused for budget, and is, with `402`.

use std::time::Duration;

use serde::Serialize;
use uuid::Uuid;

use super::LedgerRefusal;

/// Decision codes for hold transitions.
pub const CODE_REQUESTED: &str = "hold_requested";
pub const CODE_APPROVED: &str = "hold_approved";
pub const CODE_DENIED: &str = "hold_denied";
pub const CODE_EXPIRED: &str = "hold_expired";
pub const CODE_CONSUMED: &str = "hold_consumed";

/// Reasons recorded when the sweeper expires a hold.
pub const EXPIRED_UNDECIDED: &str = "nobody decided before the hold expired";
pub const EXPIRED_UNUSED: &str = "approved, but not used before the approval lapsed";

/// The decision record's endpoint for approvals and denials.
pub const DECIDE_ENDPOINT: &str = "admin/holds";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HoldState {
    Pending,
    Approved,
    Denied,
    Expired,
    Consumed,
}

impl HoldState {
    pub fn as_str(self) -> &'static str {
        match self {
            HoldState::Pending => "pending",
            HoldState::Approved => "approved",
            HoldState::Denied => "denied",
            HoldState::Expired => "expired",
            HoldState::Consumed => "consumed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(HoldState::Pending),
            "approved" => Some(HoldState::Approved),
            "denied" => Some(HoldState::Denied),
            "expired" => Some(HoldState::Expired),
            "consumed" => Some(HoldState::Consumed),
            _ => None,
        }
    }

    /// The state as callers see it: a pending hold or an approval whose time
    /// has run out is expired, whether or not the sweeper has marked it yet.
    pub fn effective(self, lapsed: bool) -> Self {
        match self {
            HoldState::Pending | HoldState::Approved if lapsed => HoldState::Expired,
            other => other,
        }
    }
}

/// A request to hold, made instead of a reservation.
#[derive(Debug, Clone)]
pub struct NewHold {
    pub id: Uuid,
    pub tenant_id: String,
    pub requested_by: String,
    /// `stream` or `complete`.
    pub endpoint: String,
    /// The registry model id.
    pub model: String,
    pub max_output_tokens: u32,
    /// The worst-case cost the approver is asked to accept.
    pub exposure_nano_usd: u64,
    /// Which rule matched, in a sentence.
    pub reason: String,
    /// The request's fingerprint under the current key.
    pub fingerprint: Vec<u8>,
    pub expires_in: Duration,
    pub approval_valid_for: Duration,
    /// Refuse a new hold when this many are already pending for the tenant.
    pub max_pending: u32,
}

/// A hold as callers and approvers see it. No fingerprint, no prompt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HoldRecord {
    pub id: Uuid,
    pub tenant_id: String,
    pub requested_by: String,
    pub endpoint: String,
    pub model: String,
    pub max_output_tokens: u32,
    pub exposure_nano_usd: u64,
    pub reason: String,
    pub state: HoldState,
    /// Unix seconds.
    pub created_at: i64,
    /// Pending: when it stops waiting. Approved: when the approval lapses.
    pub expires_at: i64,
    pub decided_by: Option<String>,
    pub decided_at: Option<i64>,
    pub note: Option<String>,
    /// The request that used the approval.
    pub reservation_id: Option<Uuid>,
}

/// An approved hold, presented with the request it was approved for.
#[derive(Debug, Clone)]
pub struct HoldClaim {
    pub id: Uuid,
    /// Every encoding of this request's fingerprint that a stored hold may
    /// carry: under the current key, older keys, or none.
    pub fingerprints: Vec<Vec<u8>>,
    pub model: String,
    /// This request's worst-case cost now. Prices or policy may have changed
    /// since approval; an approval covers at most what the approver saw.
    pub exposure_nano_usd: u64,
    /// The key presenting it, for the decision record.
    pub key_id: String,
}

impl HoldClaim {
    pub fn matches(&self, stored: &[u8]) -> bool {
        self.fingerprints.iter().any(|f| f.as_slice() == stored)
    }

    /// Whether a hold in `state` (effective) with these bindings can be used
    /// by this request. `Ok` only for an approved hold for this exact request.
    pub fn check(&self, state: HoldState, fingerprint: &[u8], model: &str, approved_exposure: u64) -> Result<(), HoldProblem> {
        match state {
            HoldState::Pending => Err(HoldProblem::Pending),
            HoldState::Denied => Err(HoldProblem::Denied),
            HoldState::Expired => Err(HoldProblem::Expired),
            HoldState::Consumed => Err(HoldProblem::Consumed),
            HoldState::Approved => {
                if self.matches(fingerprint) && self.model == model && self.exposure_nano_usd <= approved_exposure {
                    Ok(())
                } else {
                    Err(HoldProblem::Mismatch)
                }
            }
        }
    }
}

/// Why a presented hold cannot be used. Each is final for this attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldProblem {
    /// No such hold for this tenant.
    NotFound,
    Pending,
    Denied,
    Expired,
    Consumed,
    /// Not the request that was approved, or it would now cost more.
    Mismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Approve,
    Deny,
}

#[derive(Debug, Clone)]
pub struct HoldDecision {
    pub id: Uuid,
    pub approver_tenant: String,
    pub approver_key: String,
    pub verdict: Verdict,
    /// Kept on the hold and the decision record. Bounded by the caller.
    pub note: String,
}

impl HoldDecision {
    /// How the approver is named on the hold.
    pub fn decided_by(&self) -> String {
        format!("{}/{}", self.approver_tenant, self.approver_key)
    }

    pub fn code(&self) -> &'static str {
        match self.verdict {
            Verdict::Approve => CODE_APPROVED,
            Verdict::Deny => CODE_DENIED,
        }
    }

    /// The reason on the decision record: the approver's note, or a default.
    pub fn reason(&self) -> String {
        if !self.note.is_empty() {
            return self.note.clone();
        }
        match self.verdict {
            Verdict::Approve => "approved".to_string(),
            Verdict::Deny => "denied".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HoldQuery {
    pub tenant_id: Option<String>,
    /// Effective state.
    pub state: Option<HoldState>,
    pub limit: u32,
}

/// What a decision does to a hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    /// Already in the state asked for: a repeated approval or denial is
    /// answered with the hold as it is, and records nothing new.
    Unchanged,
    To(HoldState),
}

/// The transition rules, shared by every backend so they cannot drift.
/// `current` is the hold with its effective state.
pub fn transition(current: &HoldRecord, d: &HoldDecision) -> Result<Transition, LedgerRefusal> {
    if d.approver_tenant == current.tenant_id && d.approver_key == current.requested_by {
        return Err(LedgerRefusal::SelfApproval);
    }
    match (current.state, d.verdict) {
        (HoldState::Pending, Verdict::Approve) => Ok(Transition::To(HoldState::Approved)),
        (HoldState::Pending | HoldState::Approved, Verdict::Deny) => Ok(Transition::To(HoldState::Denied)),
        (HoldState::Approved, Verdict::Approve) | (HoldState::Denied, Verdict::Deny) => Ok(Transition::Unchanged),
        (state, _) => Err(LedgerRefusal::HoldNotDecidable { state }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(state: HoldState) -> HoldRecord {
        HoldRecord {
            id: Uuid::nil(),
            tenant_id: "acme".into(),
            requested_by: "ak_app".into(),
            endpoint: "stream".into(),
            model: "m".into(),
            max_output_tokens: 10,
            exposure_nano_usd: 100,
            reason: "r".into(),
            state,
            created_at: 0,
            expires_at: 0,
            decided_by: None,
            decided_at: None,
            note: None,
            reservation_id: None,
        }
    }

    fn by(tenant: &str, key: &str, verdict: Verdict) -> HoldDecision {
        HoldDecision {
            id: Uuid::nil(),
            approver_tenant: tenant.into(),
            approver_key: key.into(),
            verdict,
            note: String::new(),
        }
    }

    #[test]
    fn the_transition_table() {
        use HoldState::*;
        use Verdict::*;
        let cases = [
            (Pending, Approve, Ok(Transition::To(Approved))),
            (Pending, Deny, Ok(Transition::To(Denied))),
            (Approved, Approve, Ok(Transition::Unchanged)),
            (Approved, Deny, Ok(Transition::To(Denied))),
            (Denied, Deny, Ok(Transition::Unchanged)),
            (Denied, Approve, Err(LedgerRefusal::HoldNotDecidable { state: Denied })),
            (Expired, Approve, Err(LedgerRefusal::HoldNotDecidable { state: Expired })),
            (Expired, Deny, Err(LedgerRefusal::HoldNotDecidable { state: Expired })),
            (Consumed, Approve, Err(LedgerRefusal::HoldNotDecidable { state: Consumed })),
            (Consumed, Deny, Err(LedgerRefusal::HoldNotDecidable { state: Consumed })),
        ];
        for (state, verdict, expected) in cases {
            assert_eq!(transition(&record(state), &by("ops", "ak_ops", verdict)), expected, "{state:?} {verdict:?}");
        }
    }

    #[test]
    fn nobody_decides_their_own_hold() {
        for verdict in [Verdict::Approve, Verdict::Deny] {
            assert_eq!(
                transition(&record(HoldState::Pending), &by("acme", "ak_app", verdict)),
                Err(LedgerRefusal::SelfApproval)
            );
        }
        // The same key id in another tenant is another key.
        assert!(transition(&record(HoldState::Pending), &by("ops", "ak_app", Verdict::Approve)).is_ok());
    }

    #[test]
    fn a_lapsed_hold_is_expired_whatever_is_stored() {
        assert_eq!(HoldState::Pending.effective(true), HoldState::Expired);
        assert_eq!(HoldState::Approved.effective(true), HoldState::Expired);
        assert_eq!(HoldState::Consumed.effective(true), HoldState::Consumed);
        assert_eq!(HoldState::Denied.effective(true), HoldState::Denied);
        assert_eq!(HoldState::Approved.effective(false), HoldState::Approved);
    }

    #[test]
    fn only_the_approved_request_at_no_higher_cost_can_use_an_approval() {
        let claim = HoldClaim {
            id: Uuid::nil(),
            fingerprints: vec![b"fp".to_vec()],
            model: "m".into(),
            exposure_nano_usd: 100,
            key_id: "ak_app".into(),
        };
        assert_eq!(claim.check(HoldState::Approved, b"fp", "m", 100), Ok(()));
        assert_eq!(claim.check(HoldState::Approved, b"fp", "m", 150), Ok(()), "it may cost less than approved");
        assert_eq!(claim.check(HoldState::Approved, b"fp", "m", 99), Err(HoldProblem::Mismatch), "never more");
        assert_eq!(claim.check(HoldState::Approved, b"other", "m", 100), Err(HoldProblem::Mismatch));
        assert_eq!(claim.check(HoldState::Approved, b"fp", "other", 100), Err(HoldProblem::Mismatch));
        assert_eq!(claim.check(HoldState::Pending, b"fp", "m", 100), Err(HoldProblem::Pending));
        assert_eq!(claim.check(HoldState::Consumed, b"fp", "m", 100), Err(HoldProblem::Consumed));
    }
}
