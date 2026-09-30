//! The decision record: why work that was asked for did not happen.
//!
//! Reservations are the record of admitted, billable work. Decisions are the
//! record of everything else a known caller was told: a refusal (4xx), a
//! failure (5xx), or an operator action. Together they answer "what was
//! authorised, spent, refused, and why".
//!
//! What is kept, and what is not:
//!
//! * Only callers who passed authentication. Anonymous 401s stay in logs and
//!   metrics: persisting them would let anyone write to the database.
//! * No prompts, no answers. A reason is a code and a short sentence; for a
//!   malformed body it is a fixed sentence, because a parser's message can
//!   quote the offending value.
//!
//! Failure rules, by what the record is for:
//!
//! * **Routine refusals and failures** ([`Ledger::record_decision`]) go
//!   through a bounded queue. If the queue is full or the store fails, the
//!   record is dropped and counted (`gw_decisions_dropped_total`); the caller
//!   still gets the refusal. An audit outage must not become an outage.
//! * **Operator actions** ([`Ledger::record_decision_durably`]) are written
//!   before the action takes effect. If that write fails, the action does not
//!   happen: here the record is part of the authorization.
//!
//! [`Ledger::record_decision`]: super::Ledger::record_decision
//! [`Ledger::record_decision_durably`]: super::Ledger::record_decision_durably

use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use uuid::Uuid;

/// Routine records waiting to be written, at most. Beyond this they are
/// dropped and counted rather than queued without bound.
pub const DECISION_QUEUE: usize = 10_000;

/// Longest reason sentence kept.
const REASON_MAX: usize = 300;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionKind {
    /// Refused by governance: a 4xx the caller can act on.
    Refused,
    /// Could not be served: a 5xx, such as a provider or ledger failure.
    Failed,
    /// An operator changed something.
    AdminAction,
}

impl DecisionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            DecisionKind::Refused => "refused",
            DecisionKind::Failed => "failed",
            DecisionKind::AdminAction => "admin_action",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "refused" => Some(DecisionKind::Refused),
            "failed" => Some(DecisionKind::Failed),
            "admin_action" => Some(DecisionKind::AdminAction),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Decision {
    pub request_id: Uuid,
    pub tenant_id: String,
    pub key_id: String,
    pub kind: DecisionKind,
    /// `stream`, `complete`, or the admin route.
    pub endpoint: String,
    pub model: Option<String>,
    /// The stable error code the caller received.
    pub code: String,
    pub reason: String,
}

impl Decision {
    /// Bound the reason to a short sentence.
    pub fn with_reason(mut self, reason: &str) -> Self {
        self.reason = reason.chars().take(REASON_MAX).collect();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DecisionRecord {
    #[serde(flatten)]
    pub decision: Decision,
    /// Unix seconds.
    pub at: i64,
}

#[derive(Debug, Clone)]
pub struct DecisionQuery {
    pub tenant_id: Option<String>,
    /// Unix seconds; records at or after it.
    pub since: i64,
    pub limit: u32,
}

pub fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// The in-memory ledger's decisions: the most recent [`DECISION_QUEUE`],
/// gone with the process like everything else it holds.
#[derive(Debug, Default)]
pub struct DecisionRing {
    records: Mutex<VecDeque<DecisionRecord>>,
    dropped: AtomicU64,
}

impl DecisionRing {
    pub fn push(&self, decision: Decision) {
        let mut records = self.records.lock().unwrap_or_else(|p| p.into_inner());
        if records.len() >= DECISION_QUEUE {
            records.pop_front();
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        records.push_back(DecisionRecord { decision, at: unix_now() });
    }

    pub fn query(&self, q: &DecisionQuery) -> Vec<DecisionRecord> {
        let records = self.records.lock().unwrap_or_else(|p| p.into_inner());
        records
            .iter()
            .rev()
            .filter(|r| r.at >= q.since)
            .filter(|r| q.tenant_id.as_deref().is_none_or(|t| r.decision.tenant_id == t))
            .take(q.limit as usize)
            .cloned()
            .collect()
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}
