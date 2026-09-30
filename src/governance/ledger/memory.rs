//! The in-process ledger. Exact and fast, but it lives and dies with the
//! process, and each replica has its own. Right for a single instance.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use dashmap::{DashMap, mapref::entry::Entry};
use uuid::Uuid;

use super::{
    Closing, Ledger, LedgerRefusal, NewReservation, Outcome,
    decisions::{Decision, DecisionKind, DecisionQuery, DecisionRecord, DecisionRing, unix_now},
    holds::{self, HoldDecision, HoldProblem, HoldQuery, HoldRecord, HoldState, NewHold, Transition},
};
use crate::governance::budget_bucket;

/// Day-bucketed spend in nano-USD, guarded by one mutex so the bucket and the
/// counter flip together. Integer only: money is not a float.
#[derive(Debug, Default)]
pub(crate) struct SpendLedger {
    inner: Mutex<(u64, u64)>,
}

impl SpendLedger {
    /// Roll over at the UTC day boundary and run `f` on the pair. Called on
    /// every read/write, so the first request after midnight resets the
    /// counter without a timer. The pair must flip under one lock: two
    /// separate atomics let a write sneak between the bucket's CAS and the
    /// `spent` reset, and that write is silently lost — a reservation made one
    /// microsecond after the boundary vanishes from the budget graph.
    fn with_day<F, R>(&self, now: SystemTime, f: F) -> R
    where
        F: FnOnce(&mut (u64, u64)) -> R,
    {
        let today = budget_bucket(now);
        let mut pair = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pair.0 != today {
            pair.0 = today;
            pair.1 = 0;
        }
        f(&mut pair)
    }

    pub(crate) fn snapshot(&self, now: SystemTime) -> (u64, u64) {
        self.with_day(now, |pair| *pair)
    }

    #[cfg(test)]
    pub(crate) fn reserve(&self, now: SystemTime, amount: u64) -> u64 {
        self.with_day(now, |pair| {
            pair.1 = pair.1.saturating_add(amount);
            pair.1
        })
    }

    /// Check the budget and place the reservation under one lock. Checking in
    /// one critical section and reserving in another lets a concurrent
    /// settle overage land in between, and the reservation then overshoots
    /// the budget it was just checked against. `budget == 0` means no
    /// ceiling: spend is still recorded, so `/v1/usage` stays truthful.
    pub(crate) fn try_reserve(&self, now: SystemTime, amount: u64, budget: u64) -> Result<u64, LedgerRefusal> {
        self.with_day(now, |pair| {
            if budget > 0 {
                if pair.1 >= budget {
                    return Err(LedgerRefusal::BudgetExhausted {
                        spent: pair.1,
                        budget,
                    });
                }
                let remaining = budget - pair.1;
                if amount > remaining {
                    return Err(LedgerRefusal::BudgetWouldBeExceeded {
                        estimate: amount,
                        remaining,
                    });
                }
            }
            pair.1 = pair.1.saturating_add(amount);
            Ok(pair.0)
        })
    }

    /// Correct a reservation made in day `bucket` once real usage or
    /// abandonment is known. Clamps at zero so a refund larger than the
    /// reservation cannot wrap the counter.
    ///
    /// A stream reserved at 23:59 UTC and settled at 00:01 belongs to a day
    /// that has already rolled over. Its refund is dropped: the reservation
    /// was never on today's ledger, and refunding it there would hand today's
    /// budget yesterday's money. An overage is still charged, to today, since
    /// that spend is real and today is the only budget still open.
    pub(crate) fn correct(&self, now: SystemTime, bucket: u64, delta: i128) {
        self.with_day(now, |pair| {
            if delta < 0 {
                if pair.0 == bucket {
                    pair.1 = pair.1.saturating_sub(delta.unsigned_abs().min(u64::MAX as u128) as u64);
                }
            } else {
                pair.1 = pair.1.saturating_add(delta.min(u64::MAX as i128) as u64);
            }
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyState {
    /// The attempt holding the key has not closed.
    Open,
    /// Closed without executing anything; the key may be used again.
    Released,
    /// Closed and billed; a repeat is a duplicate.
    Billed,
}

#[derive(Debug)]
struct KeyRecord {
    id: Uuid,
    fingerprint: Vec<u8>,
    state: KeyState,
    reserved: u64,
    billed: u64,
    response: Option<String>,
    closed_at: Option<SystemTime>,
}

/// A hold as the memory ledger keeps it: the record, with its stored (not
/// effective) state, plus what is never shown.
#[derive(Debug)]
struct HeldRequest {
    record: HoldRecord,
    fingerprint: Vec<u8>,
    approval_valid_secs: i64,
}

impl HeldRequest {
    fn effective(&self, now: i64) -> HoldRecord {
        let mut record = self.record.clone();
        record.state = record.state.effective(now >= record.expires_at);
        record
    }
}

#[derive(Debug)]
pub struct MemoryLedger {
    spend: DashMap<String, Arc<SpendLedger>>,
    /// `(tenant, idempotency key)` -> the attempt that holds it.
    keys: DashMap<(String, String), KeyRecord>,
    /// Reservation id -> its key, for reservations that carry one.
    ///
    /// Lock order: `keys` may be held while touching `by_id`, never the
    /// other way round. `close` copies the key out of `by_id` and drops that
    /// guard before it locks `keys`, so the two cannot deadlock.
    by_id: DashMap<Uuid, (String, String)>,
    retention: Duration,
    decisions: DecisionRing,
    /// Lock order: `holds` may be held while reserving (which touches `keys`
    /// and `by_id`), never the other way round.
    holds: Mutex<HashMap<Uuid, HeldRequest>>,
    /// Reservation id -> the hold it consumed, to restore a released one.
    consumed: DashMap<Uuid, Uuid>,
}

impl Default for MemoryLedger {
    fn default() -> Self {
        Self::new(Duration::from_secs(86_400))
    }
}

impl MemoryLedger {
    pub fn new(idempotency_retention: Duration) -> Self {
        Self {
            spend: DashMap::new(),
            keys: DashMap::new(),
            by_id: DashMap::new(),
            retention: idempotency_retention,
            decisions: DecisionRing::default(),
            holds: Mutex::new(HashMap::new()),
            consumed: DashMap::new(),
        }
    }

    fn held(&self) -> std::sync::MutexGuard<'_, HashMap<Uuid, HeldRequest>> {
        self.holds.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// What the ledger would answer for this request's idempotency key
    /// without reserving anything: the refusal a repeat gets, if any. Lets a
    /// repeat of an executed request be answered as a repeat before its
    /// (consumed) hold is looked at, as the SQL ledgers do.
    fn repeat_answer(&self, r: &NewReservation<'_>) -> Option<LedgerRefusal> {
        let claim = r.idempotency.as_ref()?;
        let record = self.keys.get(&(r.tenant_id.to_string(), claim.key.to_string()))?;
        if self.expired(&record, r.now) {
            return None;
        }
        if !claim.matches(&record.fingerprint) {
            return Some(LedgerRefusal::IdempotencyKeyReused);
        }
        match record.state {
            KeyState::Open => Some(LedgerRefusal::InProgress { original: Some(record.id) }),
            KeyState::Billed => Some(LedgerRefusal::Duplicate {
                original: record.id,
                billed_nano_usd: record.billed,
                response: record.response.clone(),
            }),
            KeyState::Released => None,
        }
    }

    fn spend_for(&self, tenant_id: &str) -> Arc<SpendLedger> {
        if let Some(existing) = self.spend.get(tenant_id) {
            return Arc::clone(existing.value());
        }
        Arc::clone(self.spend.entry(tenant_id.to_string()).or_default().value())
    }

    fn expired(&self, record: &KeyRecord, now: SystemTime) -> bool {
        record.state == KeyState::Billed
            && record
                .closed_at
                .and_then(|at| now.duration_since(at).ok())
                .is_some_and(|age| age >= self.retention)
    }
}

impl MemoryLedger {
    /// Budget and idempotency key, as one step.
    fn reserve(&self, r: &NewReservation<'_>) -> Result<u64, LedgerRefusal> {
        let spend = self.spend_for(r.tenant_id);
        let Some(claim) = &r.idempotency else {
            return spend.try_reserve(r.now, r.amount_nano_usd, r.budget_nano_usd);
        };

        let key = (r.tenant_id.to_string(), claim.key.to_string());
        let fresh = |id| KeyRecord {
            id,
            fingerprint: claim.fingerprint.clone(),
            state: KeyState::Open,
            reserved: r.amount_nano_usd,
            billed: 0,
            response: None,
            closed_at: None,
        };
        // The entry guard locks this key's shard for the whole decision, so
        // two attempts with one key are decided one after the other.
        match self.keys.entry(key.clone()) {
            Entry::Vacant(slot) => {
                let bucket = spend.try_reserve(r.now, r.amount_nano_usd, r.budget_nano_usd)?;
                slot.insert(fresh(r.id));
                self.by_id.insert(r.id, key);
                Ok(bucket)
            }
            Entry::Occupied(mut held) => {
                let record = held.get();
                // An expired key is forgotten entirely. Otherwise the key is
                // bound to its first request for good: a different request
                // under it is refused whatever state the first one is in.
                if !self.expired(record, r.now) {
                    if !claim.matches(&record.fingerprint) {
                        return Err(LedgerRefusal::IdempotencyKeyReused);
                    }
                    match record.state {
                        KeyState::Open => {
                            return Err(LedgerRefusal::InProgress { original: Some(record.id) });
                        }
                        KeyState::Billed => {
                            return Err(LedgerRefusal::Duplicate {
                                original: record.id,
                                billed_nano_usd: record.billed,
                                response: record.response.clone(),
                            });
                        }
                        // Nothing was executed or billed: run it again.
                        KeyState::Released => {}
                    }
                }
                let bucket = spend.try_reserve(r.now, r.amount_nano_usd, r.budget_nano_usd)?;
                let previous = std::mem::replace(held.get_mut(), fresh(r.id));
                self.by_id.remove(&previous.id);
                self.by_id.insert(r.id, key);
                Ok(bucket)
            }
        }
    }
}

#[async_trait::async_trait]
impl Ledger for MemoryLedger {
    async fn try_reserve(&self, r: NewReservation<'_>) -> Result<u64, LedgerRefusal> {
        let Some(claim) = &r.hold else {
            return self.reserve(&r);
        };
        if let Some(refusal) = self.repeat_answer(&r) {
            return Err(refusal);
        }
        // The holds lock is kept for the whole admission, so two requests
        // presenting one approval are decided one after the other.
        let now = unix_now();
        let mut held = self.held();
        let hold = held
            .get_mut(&claim.id)
            .filter(|h| h.record.tenant_id == r.tenant_id)
            .ok_or(LedgerRefusal::Hold(HoldProblem::NotFound))?;
        let current = hold.effective(now);
        claim
            .check(current.state, &hold.fingerprint, &current.model, current.exposure_nano_usd)
            .map_err(LedgerRefusal::Hold)?;
        let bucket = self.reserve(&r)?;
        hold.record.state = HoldState::Consumed;
        hold.record.reservation_id = Some(r.id);
        self.consumed.insert(r.id, claim.id);
        self.decisions.push(
            Decision {
                request_id: claim.id,
                tenant_id: r.tenant_id.to_string(),
                key_id: claim.key_id.clone(),
                kind: DecisionKind::Hold,
                endpoint: hold.record.endpoint.clone(),
                model: Some(hold.record.model.clone()),
                code: holds::CODE_CONSUMED.to_string(),
                reason: String::new(),
            }
            .with_reason(&format!("executed as request {}", r.id)),
        );
        Ok(bucket)
    }

    fn close(&self, c: Closing) {
        // Nothing was executed: the approval can be used again.
        if c.outcome == Outcome::Released
            && let Some((_, hold_id)) = self.consumed.remove(&c.id)
            && let Some(hold) = self.held().get_mut(&hold_id)
            && hold.record.state == HoldState::Consumed
        {
            hold.record.state = HoldState::Approved;
            hold.record.reservation_id = None;
        }
        if c.delta_nano_usd != 0 {
            self.spend_for(&c.tenant_id).correct(c.at, c.bucket, c.delta_nano_usd);
        }
        let Some(key) = self.by_id.get(&c.id).map(|k| k.value().clone()) else {
            return;
        };
        if let Some(mut record) = self.keys.get_mut(&key)
            && record.id == c.id
            && record.state == KeyState::Open
        {
            record.state = if c.outcome == Outcome::Released {
                KeyState::Released
            } else {
                KeyState::Billed
            };
            record.billed = (record.reserved as i128 + c.delta_nano_usd).max(0) as u64;
            record.response = c.response;
            record.closed_at = Some(c.at);
        }
    }

    async fn snapshot(&self, tenant_id: &str, now: SystemTime) -> Result<(u64, u64), LedgerRefusal> {
        Ok(self
            .spend
            .get(tenant_id)
            .map(|s| s.snapshot(now))
            .unwrap_or((budget_bucket(now), 0)))
    }

    async fn flush(&self) {}

    async fn healthy(&self) -> bool {
        true
    }

    fn backend(&self) -> &'static str {
        "memory"
    }

    fn record_decision(&self, decision: Decision) {
        self.decisions.push(decision);
    }

    async fn record_decision_durably(&self, decision: Decision) -> Result<(), LedgerRefusal> {
        self.decisions.push(decision);
        Ok(())
    }

    async fn decisions(&self, query: DecisionQuery) -> Result<Vec<DecisionRecord>, LedgerRefusal> {
        Ok(self.decisions.query(&query))
    }

    fn decisions_dropped(&self) -> u64 {
        self.decisions.dropped()
    }

    async fn create_hold(&self, h: NewHold) -> Result<HoldRecord, LedgerRefusal> {
        let now = unix_now();
        let mut held = self.held();
        let pending = held
            .values()
            .filter(|x| x.record.tenant_id == h.tenant_id && x.effective(now).state == HoldState::Pending)
            .count();
        if pending >= h.max_pending as usize {
            return Err(LedgerRefusal::TooManyHolds { limit: h.max_pending });
        }
        let record = HoldRecord {
            id: h.id,
            tenant_id: h.tenant_id,
            requested_by: h.requested_by,
            endpoint: h.endpoint,
            model: h.model,
            max_output_tokens: h.max_output_tokens,
            exposure_nano_usd: h.exposure_nano_usd,
            reason: h.reason,
            state: HoldState::Pending,
            created_at: now,
            expires_at: now + h.expires_in.as_secs() as i64,
            decided_by: None,
            decided_at: None,
            note: None,
            reservation_id: None,
        };
        self.decisions.push(
            Decision {
                request_id: record.id,
                tenant_id: record.tenant_id.clone(),
                key_id: record.requested_by.clone(),
                kind: DecisionKind::Hold,
                endpoint: record.endpoint.clone(),
                model: Some(record.model.clone()),
                code: holds::CODE_REQUESTED.to_string(),
                reason: String::new(),
            }
            .with_reason(&record.reason),
        );
        held.insert(
            record.id,
            HeldRequest {
                record: record.clone(),
                fingerprint: h.fingerprint,
                approval_valid_secs: h.approval_valid_for.as_secs().max(1) as i64,
            },
        );
        Ok(record)
    }

    async fn hold(&self, id: Uuid) -> Result<Option<HoldRecord>, LedgerRefusal> {
        Ok(self.held().get(&id).map(|h| h.effective(unix_now())))
    }

    async fn holds(&self, q: HoldQuery) -> Result<Vec<HoldRecord>, LedgerRefusal> {
        let now = unix_now();
        let mut found: Vec<HoldRecord> = self
            .held()
            .values()
            .map(|h| h.effective(now))
            .filter(|h| q.tenant_id.as_deref().is_none_or(|t| h.tenant_id == t))
            .filter(|h| q.state.is_none_or(|s| h.state == s))
            .collect();
        found.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(b.id.cmp(&a.id)));
        found.truncate(q.limit as usize);
        Ok(found)
    }

    async fn decide_hold(&self, d: HoldDecision) -> Result<HoldRecord, LedgerRefusal> {
        let now = unix_now();
        let mut held = self.held();
        let hold = held.get_mut(&d.id).ok_or(LedgerRefusal::Hold(HoldProblem::NotFound))?;
        let current = hold.effective(now);
        let Transition::To(state) = holds::transition(&current, &d)? else {
            return Ok(current);
        };
        hold.record.state = state;
        hold.record.decided_by = Some(d.decided_by());
        hold.record.decided_at = Some(now);
        hold.record.note = (!d.note.is_empty()).then(|| d.note.clone());
        if state == HoldState::Approved {
            hold.record.expires_at = now + hold.approval_valid_secs;
        }
        self.decisions.push(
            Decision {
                request_id: d.id,
                tenant_id: hold.record.tenant_id.clone(),
                key_id: d.approver_key.clone(),
                kind: DecisionKind::Hold,
                endpoint: holds::DECIDE_ENDPOINT.to_string(),
                model: Some(hold.record.model.clone()),
                code: d.code().to_string(),
                reason: String::new(),
            }
            .with_reason(&d.reason()),
        );
        Ok(hold.effective(now))
    }
}
