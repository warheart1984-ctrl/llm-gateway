//! Quota enforcement: rate limits, token budgets, concurrency caps and spend.
//!
//! All four are pre-flight checks, with one twist: a *reservation* is taken
//! before the upstream connection opens and settled with real usage when the
//! stream ends. That is what makes a cost guardrail meaningful — a stream that
//! dies at 20% still pays for the prompt it already sent, and the over-reserve
//! is released on settle.
//!
//! Spend lives in a [`Ledger`]: in process memory by default, or in Postgres,
//! shared by every replica and surviving restarts. Rate windows and
//! concurrency counters are always per process.
//!
//! Locking discipline: window counters live behind `std::sync::Mutex`s that
//! are never held across an `await`, so `settle()` can stay synchronous and be
//! called straight from a stream `Drop`. Admission records the attempt in the
//! window, releases the lock, asks the ledger, and takes the attempt back out
//! if the ledger refuses.

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime},
};

use dashmap::DashMap;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use super::budget_bucket_label;
use super::ledger::{
    Closing, Fingerprinter, IdempotencyClaim, Ledger, LedgerRefusal, MemoryLedger, NewReservation, Outcome,
    SharedLimits,
};
use crate::{config::LimitProfile, providers::Usage, router::CostModel};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LimitError {
    #[error("rate limit exceeded: {done} requests in the last minute, limit is {limit}")]
    RateLimited { done: u32, limit: u32 },
    #[error("token rate limit exceeded: {done} tokens in the last minute (this request needs {requested} more), limit is {limit}")]
    TokenRateLimited {
        done: u32,
        requested: u32,
        limit: u32,
    },
    #[error("concurrent stream limit reached ({limit} in flight)")]
    ConcurrencyLimited { limit: u32 },
    #[error("daily budget exhausted: {spent} nano-USD spent, budget is {budget} nano-USD")]
    BudgetExhausted { spent: u64, budget: u64 },
    #[error("this request would cost about {estimate} nano-USD, more than the {remaining} nano-USD left in today's budget")]
    BudgetWouldBeExceeded { estimate: u64, remaining: u64 },
    #[error("output token limit exceeded: requested {requested}, tenant allows at most {limit}")]
    OutputTokensExceeded { requested: u32, limit: u32 },
    #[error("the spend ledger is unavailable, so the request was not executed; retry shortly")]
    LedgerUnavailable,
    #[error("this Idempotency-Key was already used for a different request")]
    IdempotencyKeyReused,
    #[error("a request with this Idempotency-Key is still in progress; retry shortly")]
    RequestInProgress { original: Option<Uuid> },
    #[error("a request with this Idempotency-Key already completed (request {original}, billed {billed_nano_usd} nano-USD)")]
    DuplicateRequest {
        original: Uuid,
        billed_nano_usd: u64,
        response: Option<String>,
    },
}

impl From<LedgerRefusal> for LimitError {
    fn from(refusal: LedgerRefusal) -> Self {
        match refusal {
            LedgerRefusal::BudgetExhausted { spent, budget } => LimitError::BudgetExhausted { spent, budget },
            LedgerRefusal::BudgetWouldBeExceeded { estimate, remaining } => {
                LimitError::BudgetWouldBeExceeded { estimate, remaining }
            }
            LedgerRefusal::IdempotencyKeyReused => LimitError::IdempotencyKeyReused,
            LedgerRefusal::InProgress { original } => LimitError::RequestInProgress { original },
            LedgerRefusal::Duplicate { original, billed_nano_usd, response } => {
                LimitError::DuplicateRequest { original, billed_nano_usd, response }
            }
            LedgerRefusal::Unavailable(_) => LimitError::LedgerUnavailable,
            LedgerRefusal::RateLimited { done, limit } => LimitError::RateLimited { done, limit },
            LedgerRefusal::TokenRateLimited { done, requested, limit } => {
                LimitError::TokenRateLimited { done, requested, limit }
            }
            LedgerRefusal::ConcurrencyLimited { limit } => LimitError::ConcurrencyLimited { limit },
        }
    }
}

const WINDOW: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Per-tenant state
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Window {
    request_times: VecDeque<Instant>,
    // Kept as a separate field so the struct is `Debug` without a `Debug` bound
    // on `Instant` in older toolchains.
    token_events: VecDeque<(Instant, u32)>,
}

impl Window {
    fn evict(&mut self, now: Instant) {
        while let Some(t) = self.request_times.front() {
            if now.saturating_duration_since(*t) > WINDOW {
                self.request_times.pop_front();
            } else {
                break;
            }
        }
        while let Some((t, _)) = self.token_events.front() {
            if now.saturating_duration_since(*t) > WINDOW {
                self.token_events.pop_front();
            } else {
                break;
            }
        }
    }

    fn tokens(&self) -> u32 {
        self.token_events.iter().map(|(_, t)| *t).sum()
    }

    /// Take back one attempt recorded at `at` whose admission was refused.
    /// Entries recorded at the same instant are interchangeable, so removing
    /// any one of them restores the counts exactly.
    fn withdraw(&mut self, at: Instant, prompt_tokens: u32) {
        if let Some(i) = self.request_times.iter().rposition(|t| *t == at) {
            self.request_times.remove(i);
        }
        if let Some(i) = self
            .token_events
            .iter()
            .rposition(|(t, n)| *t == at && *n == prompt_tokens)
        {
            self.token_events.remove(i);
        }
    }
}

#[derive(Debug, Default)]
struct TenantState {
    window: Mutex<Window>,
    /// Streams in flight, which is also the concurrency cap's counter. A
    /// semaphore sized at first sight would pin the cap to whatever the
    /// tenant file said when the tenant first connected; comparing against
    /// the limit on every admission lets a reload raise or lower it live.
    inflight: AtomicU64,
}

impl TenantState {
    fn window(&self) -> std::sync::MutexGuard<'_, Window> {
        self.window.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// One held concurrency slot. Releasing on `Drop` means every early return in
/// [`LimitEngine::admit`] gives the slot back without a rollback path.
#[derive(Debug)]
struct Slot(Arc<TenantState>);

impl Slot {
    fn try_acquire(state: &Arc<TenantState>, limit: u32) -> Option<Self> {
        let limit = u64::from(limit.max(1));
        state
            .inflight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| (n < limit).then_some(n + 1))
            .ok()
            .map(|_| Slot(Arc::clone(state)))
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

// ---------------------------------------------------------------------------
// Reservation
// ---------------------------------------------------------------------------

/// RAII admission token. Dropping it releases both concurrency slots.
///
/// Exactly one of the closing calls decides what the request costs; every
/// later call, and `Drop`, is a no-op on the ledger:
///
/// | call | when | billed |
/// |---|---|---|
/// | [`settle`](Self::settle) | usage is known | actual usage |
/// | [`abandon`](Self::abandon) | the stream died with no usage | prompt estimate |
/// | [`release`](Self::release) | the upstream never accepted the request | nothing |
/// | [`commit_reserved`](Self::commit_reserved) | usage is unobservable | the full reservation |
///
/// Unclosed, `Drop` behaves as `abandon`. `Debug` is derived so
/// `unwrap()`/`expect()` on a `Result<Reservation, _>` works in tests. No field
/// is a secret.
#[derive(Debug)]
pub struct Reservation {
    state: Arc<TenantState>,
    ledger: Arc<dyn Ledger>,
    id: Uuid,
    tenant_id: Arc<str>,
    _global_permit: Option<OwnedSemaphorePermit>,
    _slot: Option<Slot>,
    /// What admission put on the ledger: 0 when cost tracking is off, so a
    /// correction never refunds money that was never charged.
    reserved_nano_usd: u64,
    /// The prompt half of the reservation. Kept so an abandoned stream refunds
    /// only the completion half without needing the cost model at drop time.
    reserved_prompt_nano_usd: u64,
    /// The UTC day the reservation was charged to.
    bucket: u64,
    /// Whether money moves on this reservation (cost tracking on).
    tracked: bool,
    /// Whether the ledger holds a record of it: always when tracked, and
    /// also when untracked but carrying an idempotency key.
    recorded: bool,
    /// The answer, stored with the closing for idempotent replay.
    response: Option<String>,
    prompt_tokens: u32,
    max_output_tokens: u32,
    settled: bool,
}

impl Reservation {
    /// Commit actual usage.
    pub fn settle(&mut self, usage: Usage, cost: &CostModel) {
        if !self.close() {
            return;
        }
        let prompt = usage.prompt_tokens.max(self.prompt_tokens);
        let completion = usage.completion_tokens;
        let actual = nano_usd_for(prompt, usage.cached_prompt_tokens, completion, cost);
        self.finish(Outcome::Settled, actual as i128 - self.reserved_nano_usd as i128, completion);
        self.count_completion_tokens(completion);
    }

    /// Close without charging completion tokens — the stream ended without
    /// usage the gateway could see. The prompt estimate still stands, because
    /// the upstream already received and paid for it. Refunds the completion
    /// half *now*, rather than counting on `Drop`: a client disconnecting
    /// mid-stream reaches this from `Drop` for the stream state, and a `Drop`
    /// that merely marks itself settled would leave the over-reserve on the
    /// ledger.
    pub fn abandon(&mut self) {
        if !self.close() {
            return;
        }
        self.finish(Outcome::Abandoned, self.completion_refund(), 0);
    }

    /// Refund everything: the upstream refused the request or could not be
    /// reached, so no prompt was processed and nothing is owed. The request
    /// still counts against the per-minute request window — a rate limit
    /// counts attempts, not successes — and its idempotency key may be used
    /// again, since nothing was executed.
    pub fn release(&mut self) {
        if !self.close() {
            return;
        }
        self.finish(Outcome::Released, -(self.reserved_nano_usd as i128), 0);
    }

    /// Keep the whole reservation as the bill. For a stream whose usage the
    /// gateway cannot observe (passthrough framing): the reservation is the
    /// ceiling the tenant agreed to, and refunding any of it on a guess would
    /// make opting out of visibility a way to spend for free. The output cap is
    /// counted against the token-per-minute window for the same reason.
    pub fn commit_reserved(&mut self) {
        if !self.close() {
            return;
        }
        self.finish(Outcome::Committed, 0, self.max_output_tokens);
        self.count_completion_tokens(self.max_output_tokens);
    }

    /// Keep this answer with the closing, so a repeat under the same
    /// idempotency key can be served without calling the provider again.
    /// Must be called before the closing call.
    pub fn keep_response(&mut self, response: String) {
        self.response = Some(response);
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn reserved_nano_usd(&self) -> u64 {
        self.reserved_nano_usd
    }

    pub fn prompt_tokens(&self) -> u32 {
        self.prompt_tokens
    }

    /// Mark closed. Returns whether this call was the one that closed it.
    fn close(&mut self) -> bool {
        !std::mem::replace(&mut self.settled, true)
    }

    fn completion_refund(&self) -> i128 {
        (self.reserved_prompt_nano_usd as i128 - self.reserved_nano_usd as i128).min(0)
    }

    fn finish(&mut self, outcome: Outcome, delta: i128, completion_tokens: u32) {
        if !self.recorded {
            return;
        }
        self.ledger.close(Closing {
            id: self.id,
            tenant_id: Arc::clone(&self.tenant_id),
            bucket: self.bucket,
            delta_nano_usd: if self.tracked { delta } else { 0 },
            outcome,
            at: SystemTime::now(),
            response: self.response.take(),
            completion_tokens,
        });
    }

    fn count_completion_tokens(&self, completion: u32) {
        if completion == 0 {
            return;
        }
        let mut window = self.state.window();
        let now = Instant::now();
        window.evict(now);
        window.token_events.push_back((now, completion));
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        // A stream that died without reporting usage still consumed its
        // prompt. Release only the completion half of the reservation. The
        // slot and permit fields drop after this body, so the ledger is
        // corrected before the concurrency slot frees up.
        if self.close() {
            self.finish(Outcome::Abandoned, self.completion_refund(), 0);
        }
    }
}

fn nano_usd_for(
    prompt_tokens: u32,
    cached_prompt_tokens: Option<u32>,
    completion_tokens: u32,
    cost: &CostModel,
) -> u64 {
    let input_rate = cost.input_nano_usd_per_token();
    let cached_rate = cost.cached_input_nano_usd_per_token();
    let (fresh, cached) = match cached_prompt_tokens {
        Some(c) => (prompt_tokens.saturating_sub(c), c),
        None => (prompt_tokens, 0),
    };
    fresh as u64 * input_rate
        + cached as u64 * cached_rate
        + completion_tokens as u64 * cost.output_nano_usd_per_token()
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// Quota-split mode: this replica enforces its fixed share of every
/// tenant's budget, so `replicas` gateways that each hold their own durable
/// ledger can never together exceed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaSplit {
    pub replicas: u32,
    /// Share of the budget the replicas may use in total, 1 to 100.
    pub margin_percent: u32,
}

impl QuotaSplit {
    /// `budget * margin% / replicas`, rounded down, in integer arithmetic.
    /// Rounding down is what makes the bound exact: `replicas` shares sum to
    /// at most `budget`.
    pub fn share(self, budget: u64) -> u64 {
        (budget as u128 * u128::from(self.margin_percent.min(100)) / 100 / u128::from(self.replicas.max(1))) as u64
    }
}

/// Everything admission needs to know about one request.
#[derive(Debug, Clone)]
pub struct AdmitRequest<'a> {
    /// The request id, which becomes the reservation's id in the ledger.
    pub id: Uuid,
    pub tenant_id: &'a str,
    pub limits: &'a LimitProfile,
    pub prompt_tokens: u32,
    pub max_output_tokens: u32,
    pub estimate: CostEstimate,
    pub idempotency: Option<IdempotencyClaim<'a>>,
}

pub struct LimitEngine {
    tenants: DashMap<String, Arc<TenantState>>,
    global: Arc<Semaphore>,
    cost_tracking: bool,
    ledger: Arc<dyn Ledger>,
    quota_split: Option<QuotaSplit>,
    fingerprinter: Fingerprinter,
}

impl LimitEngine {
    /// An engine on the in-memory ledger.
    pub fn new(max_concurrent_global: usize, cost_tracking: bool) -> Arc<Self> {
        Self::with_ledger(max_concurrent_global, cost_tracking, Arc::new(MemoryLedger::default()))
    }

    pub fn with_ledger(max_concurrent_global: usize, cost_tracking: bool, ledger: Arc<dyn Ledger>) -> Arc<Self> {
        Self::with_ledger_split(max_concurrent_global, cost_tracking, ledger, None, Fingerprinter::unkeyed())
    }

    /// An engine that enforces this replica's share of every budget.
    pub fn with_ledger_split(
        max_concurrent_global: usize,
        cost_tracking: bool,
        ledger: Arc<dyn Ledger>,
        quota_split: Option<QuotaSplit>,
        fingerprinter: Fingerprinter,
    ) -> Arc<Self> {
        Arc::new(Self {
            tenants: DashMap::new(),
            global: Arc::new(Semaphore::new(max_concurrent_global.max(1))),
            cost_tracking,
            ledger,
            quota_split,
            fingerprinter,
        })
    }

    /// How idempotency claims fingerprint their requests.
    pub fn fingerprinter(&self) -> &Fingerprinter {
        &self.fingerprinter
    }

    pub fn quota_split(&self) -> Option<QuotaSplit> {
        self.quota_split
    }

    /// The budget this process enforces: the tenant's whole budget, or this
    /// replica's share of it. `0` still means no ceiling.
    fn enforced_budget(&self, budget: u64) -> u64 {
        match self.quota_split {
            Some(split) if budget > 0 => split.share(budget),
            _ => budget,
        }
    }

    /// A rate or concurrency limit as this process enforces it: whole, or
    /// this replica's share. `0` still means no ceiling; a real limit whose
    /// share rounds down to zero is reported as `Some(0)` so the caller can
    /// refuse rather than treat it as unlimited.
    fn enforced_limit(&self, limit: u32) -> u32 {
        match self.quota_split {
            Some(split) if limit > 0 => split.share(u64::from(limit)) as u32,
            _ => limit,
        }
    }

    pub fn ledger(&self) -> &Arc<dyn Ledger> {
        &self.ledger
    }

    fn state(&self, tenant_id: &str) -> Arc<TenantState> {
        if let Some(existing) = self.tenants.get(tenant_id) {
            return Arc::clone(existing.value());
        }
        let entry = self.tenants.entry(tenant_id.to_string()).or_default();
        Arc::clone(entry.value())
    }

    /// [`LimitEngine::admit_request`] with a fresh id and no idempotency key.
    pub async fn admit(
        self: &Arc<Self>,
        tenant_id: &str,
        limits: &LimitProfile,
        prompt_tokens: u32,
        max_output_tokens: u32,
        estimate: CostEstimate,
    ) -> Result<Reservation, LimitError> {
        self.admit_request(AdmitRequest {
            id: Uuid::new_v4(),
            tenant_id,
            limits,
            prompt_tokens,
            max_output_tokens,
            estimate,
            idempotency: None,
        })
        .await
    }

    /// Pre-flight gate. Every rejection names the ceiling that was hit, so a
    /// tenant can tell "slow down" apart from "you cannot afford this".
    pub async fn admit_request(self: &Arc<Self>, req: AdmitRequest<'_>) -> Result<Reservation, LimitError> {
        let limits = req.limits;
        let estimated_cost_nano_usd = req.estimate.total();
        // `0` means "no ceiling", matching the rest of `LimitProfile`.
        if limits.max_output_tokens > 0 && req.max_output_tokens > limits.max_output_tokens {
            return Err(LimitError::OutputTokensExceeded {
                requested: req.max_output_tokens,
                limit: limits.max_output_tokens,
            });
        }

        // Permits first: an early return simply drops them, so no counter
        // needs rolling back.
        let global_permit = Arc::clone(&self.global).try_acquire_owned().map_err(|_| {
            LimitError::ConcurrencyLimited {
                limit: u32::MAX,
            }
        })?;

        // The limits this process enforces: whole, or this replica's share.
        // A real limit whose share rounds down to zero refuses outright; as
        // `0` it would mean "no ceiling".
        let concurrent = self.enforced_limit(limits.max_concurrent_streams);
        let requests_per_minute = self.enforced_limit(limits.requests_per_minute);
        let tokens_per_minute = self.enforced_limit(limits.tokens_per_minute);
        if limits.max_concurrent_streams > 0 && concurrent == 0 {
            return Err(LimitError::ConcurrencyLimited { limit: 0 });
        }
        if limits.requests_per_minute > 0 && requests_per_minute == 0 {
            return Err(LimitError::RateLimited { done: 0, limit: 0 });
        }
        if limits.tokens_per_minute > 0 && tokens_per_minute == 0 {
            return Err(LimitError::TokenRateLimited { done: 0, requested: req.prompt_tokens, limit: 0 });
        }

        // A ledger shared across processes enforces rate and concurrency
        // limits in its admission transaction. The in-process window then
        // only records, for `/v1/usage`; deciding locally as well would
        // refuse on a sliding window what the shared fixed window allows.
        let shared = self.ledger.enforces_limits();

        let state = self.state(req.tenant_id);
        let slot = Slot::try_acquire(&state, concurrent).ok_or(
            LimitError::ConcurrencyLimited {
                limit: concurrent,
            },
        )?;

        // Phase 1, under the window lock: check the rate limits (when this
        // process decides them) and record this attempt, so concurrent
        // admissions see it.
        let now = Instant::now();
        {
            let mut window = state.window();
            window.evict(now);
            if !shared {
                if requests_per_minute > 0 && window.request_times.len() as u32 >= requests_per_minute {
                    return Err(LimitError::RateLimited {
                        done: window.request_times.len() as u32,
                        limit: requests_per_minute,
                    });
                }
                let used_tokens = window.tokens();
                if tokens_per_minute > 0 && used_tokens.saturating_add(req.prompt_tokens) > tokens_per_minute {
                    return Err(LimitError::TokenRateLimited {
                        done: used_tokens,
                        requested: req.prompt_tokens,
                        limit: tokens_per_minute,
                    });
                }
            }
            window.request_times.push_back(now);
            window.token_events.push_back((now, req.prompt_tokens));
            if window.token_events.len() > 8_192 {
                let drop_to = window.token_events.len() - 4_096;
                window.token_events.drain(..drop_to);
            }
        }

        // Phase 2, no lock held: the ledger decides, atomically, whether the
        // money, the idempotency key and (on a shared ledger) the rate and
        // concurrency headroom are available. A refusal takes the attempt
        // back out of the window: a request that was never admitted does not
        // consume rate limit.
        let tracked = self.cost_tracking;
        let recorded = tracked || req.idempotency.is_some() || shared;
        let budget = self.enforced_budget(limits.daily_budget_nano_usd);
        // A real budget whose share rounds down to zero must refuse, not
        // reach the ledger as `0`, which means "no ceiling": that would turn
        // the smallest budgets into unlimited ones.
        if tracked && limits.daily_budget_nano_usd > 0 && budget == 0 {
            state.window().withdraw(now, req.prompt_tokens);
            return Err(LimitError::BudgetWouldBeExceeded {
                estimate: estimated_cost_nano_usd,
                remaining: 0,
            });
        }
        let bucket = if recorded {
            let decision = self
                .ledger
                .try_reserve(NewReservation {
                    id: req.id,
                    tenant_id: req.tenant_id,
                    now: SystemTime::now(),
                    amount_nano_usd: if tracked { estimated_cost_nano_usd } else { 0 },
                    prompt_nano_usd: if tracked { req.estimate.prompt_nano_usd } else { 0 },
                    budget_nano_usd: if tracked { budget } else { 0 },
                    idempotency: req.idempotency.clone(),
                    limits: SharedLimits {
                        // The local slot treats 0 as 1; the ledger must agree.
                        max_concurrent: concurrent.max(1),
                        requests_per_minute,
                        tokens_per_minute,
                        prompt_tokens: req.prompt_tokens,
                    },
                })
                .await;
            match decision {
                Ok(bucket) => bucket,
                Err(refusal) => {
                    state.window().withdraw(now, req.prompt_tokens);
                    return Err(refusal.into());
                }
            }
        } else {
            super::budget_bucket(SystemTime::now())
        };
        let reserved = if tracked { estimated_cost_nano_usd } else { 0 };

        tracing::debug!(
            tenant = req.tenant_id,
            reservation = %req.id,
            prompt_tokens = req.prompt_tokens,
            max_output_tokens = req.max_output_tokens,
            reserved_nano_usd = reserved,
            ledger = self.ledger.backend(),
            "admitted"
        );

        Ok(Reservation {
            state: Arc::clone(&state),
            ledger: Arc::clone(&self.ledger),
            id: req.id,
            tenant_id: Arc::from(req.tenant_id),
            _global_permit: Some(global_permit),
            _slot: Some(slot),
            reserved_nano_usd: reserved,
            reserved_prompt_nano_usd: if tracked { req.estimate.prompt_nano_usd } else { 0 },
            bucket,
            tracked,
            recorded,
            response: None,
            prompt_tokens: req.prompt_tokens,
            max_output_tokens: req.max_output_tokens,
            settled: false,
        })
    }

    /// Current sliding-window usage, for `GET /v1/usage` and metrics.
    pub fn usage(&self, tenant_id: &str) -> TenantUsage {
        let Some(state) = self.tenants.get(tenant_id) else {
            return TenantUsage::default();
        };
        let mut window = state.window();
        window.evict(Instant::now());
        TenantUsage {
            requests_last_minute: window.request_times.len() as u32,
            tokens_last_minute: window.tokens(),
            in_flight: state.inflight.load(Ordering::Relaxed),
        }
    }

    /// Point-in-time budget view, read from the ledger.
    pub async fn snapshot(&self, tenant_id: &str, limits: &LimitProfile) -> Result<BudgetSnapshot, LimitError> {
        let (bucket, spent) = self.ledger.snapshot(tenant_id, SystemTime::now()).await?;
        let usage = self.usage(tenant_id);
        Ok(BudgetSnapshot {
            tenant_id: tenant_id.to_string(),
            requests_last_minute: usage.requests_last_minute,
            tokens_last_minute: usage.tokens_last_minute,
            in_flight: usage.in_flight,
            spent_nano_usd: spent,
            budget_nano_usd: self.enforced_budget(limits.daily_budget_nano_usd),
            budget_bucket: bucket,
            budget_label: budget_bucket_label(bucket),
        })
    }

    pub fn tracked_tenants(&self) -> usize {
        self.tenants.len()
    }
}

/// Pre-flight cost reservation, split so an abandoned stream can be refunded
/// the completion half without re-deriving rates.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CostEstimate {
    pub prompt_nano_usd: u64,
    pub completion_nano_usd: u64,
}

impl CostEstimate {
    pub fn total(&self) -> u64 {
        self.prompt_nano_usd
            .saturating_add(self.completion_nano_usd)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TenantUsage {
    pub requests_last_minute: u32,
    pub tokens_last_minute: u32,
    pub in_flight: u64,
}

#[derive(Debug, Clone)]
pub struct BudgetSnapshot {
    pub tenant_id: String,
    pub requests_last_minute: u32,
    pub tokens_last_minute: u32,
    pub in_flight: u64,
    pub spent_nano_usd: u64,
    pub budget_nano_usd: u64,
    pub budget_bucket: u64,
    pub budget_label: String,
}

impl BudgetSnapshot {
    pub fn remaining_nano_usd(&self) -> u64 {
        self.budget_nano_usd.saturating_sub(self.spent_nano_usd)
    }
}

/// Aggregate token/cost counters for one finished stream, published to
/// metrics. Kept as a plain struct so the API layer never touches atomics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UsageDelta {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub cost_nano_usd: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::ledger::SpendLedger;
    use futures_util::FutureExt as _;

    /// The in-memory ledger answers immediately, so its snapshot future is
    /// ready on first poll. Lets sync tests and threads read spend.
    fn snap(engine: &LimitEngine, tenant: &str, limits: &LimitProfile) -> BudgetSnapshot {
        engine
            .snapshot(tenant, limits)
            .now_or_never()
            .expect("the memory ledger answers immediately")
            .expect("the memory ledger never refuses a snapshot")
    }

    fn limits(requests: u32, tokens: u32, concurrent: u32, budget: u64) -> LimitProfile {
        LimitProfile {
            requests_per_minute: requests,
            tokens_per_minute: tokens,
            max_concurrent_streams: concurrent,
            max_output_tokens: 8_192,
            daily_budget_nano_usd: budget,
            max_messages: 100,
            max_prompt_chars: 1_000_000,
        }
    }

    fn cost(input: f64, output: f64) -> CostModel {
        CostModel {
            input_per_mtok_usd: input,
            output_per_mtok_usd: output,
            cached_input_per_mtok_usd: None,
        }
    }

    #[tokio::test]
    async fn rate_limit_trips_at_the_configured_ceiling() {
        let engine = LimitEngine::new(64, false);
        let l = limits(2, 0, 8, 0);
        for _ in 0..2 {
            engine.admit("t", &l, 10, 100, CostEstimate::default()).await.unwrap();
        }
        let err = engine.admit("t", &l, 10, 100, CostEstimate::default()).await.unwrap_err();
        assert!(matches!(err, LimitError::RateLimited { done: 2, limit: 2 }));
    }

    #[tokio::test]
    async fn token_rate_limit_counts_prompt_tokens() {
        let engine = LimitEngine::new(64, false);
        let l = limits(0, 100, 8, 0);
        engine.admit("t", &l, 60, 10, CostEstimate::default()).await.unwrap();
        let err = engine.admit("t", &l, 60, 10, CostEstimate::default()).await.unwrap_err();
        assert!(matches!(err, LimitError::TokenRateLimited { .. }));
    }

    #[tokio::test]
    async fn concurrency_is_capped_per_tenant() {
        let engine = LimitEngine::new(64, false);
        let l = limits(0, 0, 1, 0);
        let held = engine.admit("t", &l, 10, 10, CostEstimate::default()).await.unwrap();
        let err = engine.admit("t", &l, 10, 10, CostEstimate::default()).await.unwrap_err();
        assert_eq!(err, LimitError::ConcurrencyLimited { limit: 1 });
        drop(held);
        // Slot is released on drop.
        assert!(engine.admit("t", &l, 10, 10, CostEstimate::default()).await.is_ok());
    }

    #[tokio::test]
    async fn tenants_do_not_share_quota() {
        let engine = LimitEngine::new(64, false);
        let l = limits(1, 0, 8, 0);
        assert!(engine.admit("a", &l, 10, 10, CostEstimate::default()).await.is_ok());
        assert!(engine.admit("a", &l, 10, 10, CostEstimate::default()).await.is_err());
        assert!(engine.admit("b", &l, 10, 10, CostEstimate::default()).await.is_ok());
    }

    #[tokio::test]
    async fn output_token_ceiling_is_checked_before_anything_else() {
        let engine = LimitEngine::new(64, false);
        let l = limits(100, 100, 8, 0);
        let err = engine.admit("t", &l, 10, 9_000, CostEstimate::default()).await.unwrap_err();
        assert_eq!(
            err,
            LimitError::OutputTokensExceeded { requested: 9_000, limit: 8_192 }
        );
    }

    #[tokio::test]
    async fn settle_corrects_the_reservation_to_actual_usage() {
        let engine = LimitEngine::new(64, true);
        // $2/MTok in, $8/MTok out == 2,000 and 8,000 nano-USD per token.
        let c = cost(2.0, 8.0);
        let l = limits(0, 0, 8, 10_000_000_000);
        // The estimate is derived from the same rate table that settles it, so
        // the two cannot disagree. 10 prompt tokens = 20,000; reserving for 100
        // completion = 800,000; total 820,000.
        let estimate = CostEstimate {
            prompt_nano_usd: nano_usd_for(10, None, 0, &c),
            completion_nano_usd: nano_usd_for(0, None, 100, &c),
        };
        let mut r = engine.admit("t", &l, 10, 100, estimate).await.unwrap();
        assert_eq!(snap(&engine, "t", &l).spent_nano_usd, 820_000);
        r.settle(
            Usage { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15, ..Default::default() },
            &c,
        );
        // Actual: 10 * 2,000 + 5 * 8,000 = 60,000. The 760,000 over-reserve is
        // refunded.
        assert_eq!(snap(&engine, "t", &l).spent_nano_usd, 60_000);
    }

#[tokio::test]
    async fn abandoned_stream_keeps_prompt_cost_and_refunds_the_rest() {
        let engine = LimitEngine::new(64, true);
        let l = limits(0, 0, 8, 10_000_000);
        let mut r = engine
            .admit(
                "t",
                &l,
                10,
                100,
                CostEstimate { prompt_nano_usd: 20, completion_nano_usd: 800 },
            )
            .await
            .unwrap();
        assert_eq!(snap(&engine, "t", &l).spent_nano_usd, 820);
        r.abandon();
        // The completion reservation is refunded immediately, not deferred to
        // `Drop`; only the prompt half stands.
        assert_eq!(snap(&engine, "t", &l).spent_nano_usd, 20);
        drop(r);
        // Dropping an already-abandoned reservation refunds nothing twice.
        assert_eq!(snap(&engine, "t", &l).spent_nano_usd, 20);
    }

    #[tokio::test]
    async fn abandon_after_settle_is_a_no_op() {
        let engine = LimitEngine::new(64, true);
        let l = limits(0, 0, 8, 10_000_000);
        let c = cost(2.0, 8.0);
        let mut r = engine
            .admit("t", &l, 10, 100, CostEstimate { prompt_nano_usd: 20, completion_nano_usd: 800 })
            .await
            .unwrap();
        r.settle(
            Usage { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15, ..Default::default() },
            &c,
        );
        let billed = snap(&engine, "t", &l).spent_nano_usd;
        assert_eq!(billed, 60_000);
        r.abandon();
        // Real usage was already charged; abandoning afterwards must not refund
        // the completion half a second time.
        assert_eq!(snap(&engine, "t", &l).spent_nano_usd, billed);
    }

    #[test]
    fn spend_rolls_over_at_the_utc_day_boundary() {
        let ledger = SpendLedger::default();
        let day1 = SystemTime::UNIX_EPOCH + Duration::from_secs(3 * 86_400);
        let day2 = SystemTime::UNIX_EPOCH + Duration::from_secs(4 * 86_400);
        assert_eq!(ledger.snapshot(day1), (3, 0));
        ledger.reserve(day1, 820);
        ledger.reserve(day1, 1_000_000);
        assert_eq!(ledger.snapshot(day1), (3, 1_000_820));
        // First touch on a new day resets the pair to that day's bucket.
        assert_eq!(ledger.snapshot(day2), (4, 0));
    }

    /// Regression guard for the two-atomics reset, where a write landing
    /// between the bucket's CAS and the `spent` reset was silently lost.
    /// All reservations here are made *after* the day flip, so every one of
    /// them must survive: the total can never fall short of the count.
    #[test]
    fn day_rollover_cannot_lose_concurrent_spend() {
        let ledger = Arc::new(SpendLedger::default());
        let day1 = SystemTime::UNIX_EPOCH + Duration::from_secs(3 * 86_400);
        let day2 = SystemTime::UNIX_EPOCH + Duration::from_secs(4 * 86_400);
        assert_eq!(ledger.snapshot(day1), (3, 0));
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let ledger = Arc::clone(&ledger);
                std::thread::spawn(move || {
                    let _ = ledger.snapshot(day2);
                    for _ in 0..10_000 {
                        ledger.reserve(day2, 1);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().expect("worker must not panic");
        }
        assert_eq!(ledger.snapshot(day2), (4, 40_000));
    }

    #[tokio::test]
    async fn budget_would_be_exceeded_is_distinct_from_exhausted() {
        let engine = LimitEngine::new(64, true);
        let l = limits(0, 0, 8, 1_000);
        let err = engine.admit("t", &l, 10, 10, CostEstimate { prompt_nano_usd: 2_500, completion_nano_usd: 2_500 }).await.unwrap_err();
        assert!(matches!(err, LimitError::BudgetWouldBeExceeded { estimate: 5_000, remaining: 1_000 }));
    }

    #[tokio::test]
    async fn spent_exactly_hitting_the_budget_is_allowed_once() {
        let engine = LimitEngine::new(64, true);
        let l = limits(0, 0, 8, 1_000);
        // Reserving the full budget is allowed exactly once.
        let mut r = engine
            .admit("t", &l, 1, 1, CostEstimate { prompt_nano_usd: 500, completion_nano_usd: 500 })
            .await
            .unwrap();
        assert_eq!(snap(&engine, "t", &l).spent_nano_usd, 1_000);
        // Settling at the reserved amount leaves the budget exactly exhausted.
        // $0.001/MTok is 1 nano-USD per token, so 500 + 500 tokens bills
        // exactly the 1,000 nano-USD that was reserved.
        r.settle(
            Usage { prompt_tokens: 500, completion_tokens: 500, total_tokens: 1_000, ..Default::default() },
            &CostModel { input_per_mtok_usd: 0.001, output_per_mtok_usd: 0.001, cached_input_per_mtok_usd: None },
        );
        let err = engine
            .admit("t", &l, 1, 1, CostEstimate { prompt_nano_usd: 1, completion_nano_usd: 0 })
            .await
            .unwrap_err();
        assert!(matches!(err, LimitError::BudgetExhausted { spent: 1_000, budget: 1_000 }));
    }

    #[test]
    fn cached_prompt_tokens_are_billed_at_the_cache_rate() {
        let c = CostModel {
            input_per_mtok_usd: 2.0,
            output_per_mtok_usd: 8.0,
            cached_input_per_mtok_usd: Some(0.5),
        };
        // Rates are nano-USD per token: $2/MTok -> 2,000, $0.50/MTok -> 500,
        // $8/MTok -> 8,000. 800 of 1_000 prompt tokens were cached.
        let cost = nano_usd_for(1_000, Some(800), 100, &c);
        assert_eq!(cost, 200 * 2_000 + 800 * 500 + 100 * 8_000);
    }

    #[tokio::test]
    async fn sliding_window_evicts_old_entries() {
        let engine = LimitEngine::new(64, false);
        let l = limits(2, 0, 8, 0);
        drop(engine.admit("t", &l, 10, 10, CostEstimate::default()).await.unwrap());
        assert!(engine.usage("t").requests_last_minute >= 1);
        // Fast-forward is not available; assert the counter is at least the
        // number of admissions we made.
        drop(engine.admit("t", &l, 10, 10, CostEstimate::default()).await.unwrap());
        assert_eq!(engine.usage("t").requests_last_minute, 2);
    }

    // -----------------------------------------------------------------------
    // Money under concurrency and in every closing order
    // -----------------------------------------------------------------------

    /// $2/MTok in, $8/MTok out: 2,000 and 8,000 nano-USD per token.
    fn rates() -> CostModel {
        cost(2.0, 8.0)
    }

    fn estimate_for(prompt: u32, max_output: u32) -> CostEstimate {
        CostEstimate {
            prompt_nano_usd: nano_usd_for(prompt, None, 0, &rates()),
            completion_nano_usd: nano_usd_for(0, None, max_output, &rates()),
        }
    }

    fn usage(prompt: u32, completion: u32) -> Usage {
        Usage {
            prompt_tokens: prompt,
            completion_tokens: completion,
            total_tokens: prompt + completion,
            ..Default::default()
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum Close {
        Settle(u32),
        Abandon,
        Release,
        Commit,
        Drop,
    }

    impl Close {
        fn apply(self, r: &mut Option<Reservation>) {
            let Some(res) = r.as_mut() else { return };
            match self {
                Close::Settle(completion) => res.settle(usage(10, completion), &rates()),
                Close::Abandon => res.abandon(),
                Close::Release => res.release(),
                Close::Commit => res.commit_reserved(),
                Close::Drop => drop(r.take()),
            }
        }

        /// What a request with 10 prompt tokens and a 100-token ceiling owes
        /// when this is the first closing call.
        fn owed(self) -> u64 {
            match self {
                Close::Settle(completion) => nano_usd_for(10, None, completion, &rates()),
                Close::Abandon | Close::Drop => estimate_for(10, 100).prompt_nano_usd,
                Close::Release => 0,
                Close::Commit => estimate_for(10, 100).total(),
            }
        }
    }

    const CLOSES: [Close; 6] = [
        Close::Settle(5),
        Close::Settle(250),
        Close::Abandon,
        Close::Release,
        Close::Commit,
        Close::Drop,
    ];

    /// Every ordering of up to three closing calls, exhaustively: the first
    /// one decides the bill and nothing after it — a second settle, a stray
    /// abandon, the stream's `Drop` — moves the ledger again. Duplicated
    /// cleanup is where a refund gets paid twice.
    #[tokio::test]
    async fn the_first_closing_call_decides_the_bill_in_every_order() {
        let mut cases = 0;
        for a in CLOSES {
            for b in CLOSES {
                for c in CLOSES {
                    let engine = LimitEngine::new(64, true);
                    let l = limits(0, 0, 8, 10_000_000_000);
                    let mut r = Some(engine.admit("t", &l, 10, 100, estimate_for(10, 100)).await.unwrap());
                    for step in [a, b, c] {
                        step.apply(&mut r);
                    }
                    drop(r);
                    let snap = snap(&engine, "t", &l);
                    assert_eq!(snap.spent_nano_usd, a.owed(), "sequence {a:?} -> {b:?} -> {c:?}");
                    assert_eq!(snap.in_flight, 0, "sequence {a:?} -> {b:?} -> {c:?} leaked a slot");
                    cases += 1;
                }
            }
        }
        assert_eq!(cases, 216);
    }

    /// Tiny deterministic PRNG, so the property run needs no dependency and a
    /// failure reproduces from its seed.
    struct XorShift(u64);

    impl XorShift {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Property: however admissions and closings interleave across threads,
    /// once every reservation is closed the ledger holds exactly what each
    /// admitted request owes — no lost update, no double refund, no refund of
    /// a rejected request. The budget is tight enough that many requests are
    /// refused, so rejections race admissions and settlements too.
    #[test]
    fn concurrent_admit_and_close_leave_the_ledger_exact() {
        let engine = LimitEngine::new(1_024, true);
        let l = limits(0, 0, 64, 2_000_000_000);
        let threads: Vec<_> = (0..8u64)
            .map(|seed| {
                let engine = Arc::clone(&engine);
                let l = l.clone();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
                    let mut rng = XorShift(0x9E37_79B9_7F4A_7C15 ^ (seed + 1));
                    let mut owed = 0u64;
                    let mut admitted = 0u32;
                    let mut held: Vec<(Reservation, u32, u32)> = Vec::new();
                    for _ in 0..2_000 {
                        let prompt = 1 + rng.below(500) as u32;
                        let max_out = 1 + rng.below(2_000) as u32;
                        if let Ok(r) = rt.block_on(engine.admit("t", &l, prompt, max_out, estimate_for(prompt, max_out))) {
                            admitted += 1;
                            held.push((r, prompt, max_out));
                        }
                        // Close a random held reservation about half the time,
                        // so several are open at once.
                        if !held.is_empty() && rng.below(2) == 0 {
                            let i = rng.below(held.len() as u64) as usize;
                            let (mut r, prompt, max_out) = held.swap_remove(i);
                            owed += match rng.below(5) {
                                0 => {
                                    // Real usage may overshoot the estimate.
                                    let completion = rng.below(max_out as u64 * 2) as u32;
                                    r.settle(usage(prompt, completion), &rates());
                                    nano_usd_for(prompt, None, completion, &rates())
                                }
                                1 => {
                                    r.abandon();
                                    estimate_for(prompt, max_out).prompt_nano_usd
                                }
                                2 => {
                                    r.release();
                                    0
                                }
                                3 => {
                                    r.commit_reserved();
                                    estimate_for(prompt, max_out).total()
                                }
                                _ => estimate_for(prompt, max_out).prompt_nano_usd,
                            };
                            // A duplicate cleanup must be inert.
                            r.abandon();
                            r.release();
                        }
                    }
                    for (r, prompt, max_out) in held {
                        drop(r);
                        owed += estimate_for(prompt, max_out).prompt_nano_usd;
                    }
                    (owed, admitted)
                })
            })
            .collect();

        let (mut owed, mut admitted) = (0u64, 0u32);
        for t in threads {
            let (o, a) = t.join().expect("worker must not panic");
            owed += o;
            admitted += a;
        }
        let snap = snap(&engine, "t", &l);
        assert!(admitted > 0, "the property is vacuous if nothing was admitted");
        assert_eq!(snap.spent_nano_usd, owed, "{admitted} admitted");
        assert_eq!(snap.in_flight, 0);
    }

    /// Many streams racing for the last of a budget: exactly as many are
    /// admitted as fit, however the admissions interleave.
    #[test]
    fn concurrent_reservations_near_the_limit_admit_exactly_what_fits() {
        let per_request = estimate_for(10, 100).total();
        let l = limits(0, 0, 1_000, per_request * 10 + per_request / 2);
        for round in 0..20 {
            let engine = LimitEngine::new(1_024, true);
            let barrier = Arc::new(std::sync::Barrier::new(64));
            let threads: Vec<_> = (0..64)
                .map(|_| {
                    let engine = Arc::clone(&engine);
                    let barrier = Arc::clone(&barrier);
                    let l = l.clone();
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
                        barrier.wait();
                        rt.block_on(engine.admit("t", &l, 10, 100, estimate_for(10, 100)))
                    })
                })
                .collect();
            let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
            let admitted = results.iter().filter(|r| r.is_ok()).count();
            assert_eq!(admitted, 10, "round {round}");
            assert!(
                results.iter().filter_map(|r| r.as_ref().err()).all(|e| matches!(
                    e,
                    LimitError::BudgetWouldBeExceeded { .. } | LimitError::BudgetExhausted { .. }
                )),
                "round {round}: every refusal is a budget refusal"
            );
            assert_eq!(snap(&engine, "t", &l).spent_nano_usd, per_request * 10);
        }
    }

    /// One tenant exhausting its budget and hammering the gate must not move
    /// another tenant's ledger or starve its admissions.
    #[test]
    fn tenant_budgets_are_isolated_under_contention() {
        let engine = LimitEngine::new(1_024, true);
        let per_request = estimate_for(10, 100).total();
        let tight = limits(0, 0, 1_000, per_request * 3);
        let roomy = limits(0, 0, 1_000, per_request * 1_000);
        let threads: Vec<_> = (0..16)
            .map(|i| {
                let engine = Arc::clone(&engine);
                let (tight, roomy) = (tight.clone(), roomy.clone());
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
                    let (tenant, l) = if i % 2 == 0 { ("noisy", tight) } else { ("quiet", roomy) };
                    let mut held = Vec::new();
                    for _ in 0..50 {
                        if let Ok(r) = rt.block_on(engine.admit(tenant, &l, 10, 100, estimate_for(10, 100))) {
                            held.push(r);
                        }
                    }
                    held
                })
            })
            .collect();
        let held: Vec<Reservation> = threads.into_iter().flat_map(|t| t.join().unwrap()).collect();
        assert_eq!(snap(&engine, "noisy", &tight).spent_nano_usd, per_request * 3);
        assert_eq!(snap(&engine, "quiet", &roomy).spent_nano_usd, per_request * 400);
        assert_eq!(held.len(), 403);
    }

    #[tokio::test]
    async fn release_refunds_everything_but_still_counts_the_request() {
        let engine = LimitEngine::new(64, true);
        let l = limits(0, 0, 8, 10_000_000_000);
        let mut r = engine.admit("t", &l, 10, 100, estimate_for(10, 100)).await.unwrap();
        r.release();
        let snap = snap(&engine, "t", &l);
        assert_eq!(snap.spent_nano_usd, 0, "a refused upstream costs nothing");
        assert_eq!(snap.requests_last_minute, 1, "but it was an attempt");
    }

    #[tokio::test]
    async fn commit_reserved_bills_the_ceiling_and_counts_its_tokens() {
        let engine = LimitEngine::new(64, true);
        let l = limits(0, 0, 8, 10_000_000_000);
        let mut r = engine.admit("t", &l, 10, 100, estimate_for(10, 100)).await.unwrap();
        r.commit_reserved();
        let snap = snap(&engine, "t", &l);
        assert_eq!(snap.spent_nano_usd, estimate_for(10, 100).total());
        assert_eq!(snap.tokens_last_minute, 110, "prompt plus the whole output ceiling");
    }

    /// With cost tracking off nothing is charged, so no closing call may
    /// refund into the ledger either.
    #[tokio::test]
    async fn untracked_reservations_never_touch_the_ledger() {
        let engine = LimitEngine::new(64, false);
        let l = limits(0, 0, 8, 1);
        for close in CLOSES {
            let mut r = Some(engine.admit("t", &l, 10, 100, estimate_for(10, 100)).await.unwrap());
            close.apply(&mut r);
        }
        assert_eq!(snap(&engine, "t", &l).spent_nano_usd, 0);
    }

    /// A zero budget means no ceiling, but spend is still recorded so a
    /// tenant's `/v1/usage` is truthful. A refund used to clamp against a
    /// ledger that was never charged.
    #[tokio::test]
    async fn an_unlimited_budget_still_records_spend() {
        let engine = LimitEngine::new(64, true);
        let l = limits(0, 0, 8, 0);
        let mut r = engine.admit("t", &l, 10, 100, estimate_for(10, 100)).await.unwrap();
        r.settle(usage(10, 5), &rates());
        assert_eq!(snap(&engine, "t", &l).spent_nano_usd, nano_usd_for(10, None, 5, &rates()));
    }

    #[test]
    fn a_refund_after_rollover_never_reaches_the_new_day() {
        let ledger = SpendLedger::default();
        let day1 = SystemTime::UNIX_EPOCH + Duration::from_secs(3 * 86_400);
        let day2 = SystemTime::UNIX_EPOCH + Duration::from_secs(4 * 86_400);
        ledger.try_reserve(day1, 1_000, 0).unwrap();
        ledger.try_reserve(day2, 700, 0).unwrap();
        // Yesterday's stream settles today for less than it reserved.
        ledger.correct(day2, 3, -900);
        assert_eq!(ledger.snapshot(day2), (4, 700), "today keeps only today's spend");
        // Yesterday's stream settles today for more than it reserved: that
        // spend is real, so today pays it.
        ledger.correct(day2, 3, 50);
        assert_eq!(ledger.snapshot(day2), (4, 750));
        // Today's own corrections still apply.
        ledger.correct(day2, 4, -100);
        assert_eq!(ledger.snapshot(day2), (4, 650));
    }

    /// Rollover under contention: yesterday's refunds race today's
    /// reservations, and today's total must be exactly today's reservations.
    #[test]
    fn stale_refunds_racing_new_reservations_cannot_erase_todays_spend() {
        let ledger = Arc::new(SpendLedger::default());
        let day1 = SystemTime::UNIX_EPOCH + Duration::from_secs(3 * 86_400);
        let day2 = SystemTime::UNIX_EPOCH + Duration::from_secs(4 * 86_400);
        ledger.try_reserve(day1, 1_000_000, 0).unwrap();
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let ledger = Arc::clone(&ledger);
                std::thread::spawn(move || {
                    for _ in 0..5_000 {
                        if i % 2 == 0 {
                            ledger.try_reserve(day2, 3, 0).unwrap();
                        } else {
                            ledger.correct(day2, 3, -7);
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(ledger.snapshot(day2), (4, 4 * 5_000 * 3));
    }

    /// The concurrency cap is read on every admission, so a tenant reload
    /// that raises or lowers it takes effect on the next request.
    #[tokio::test]
    async fn the_concurrency_cap_follows_a_reloaded_limit() {
        let engine = LimitEngine::new(64, false);
        let one = limits(0, 0, 1, 0);
        let two = limits(0, 0, 2, 0);
        let a = engine.admit("t", &one, 1, 1, CostEstimate::default()).await.unwrap();
        assert!(engine.admit("t", &one, 1, 1, CostEstimate::default()).await.is_err());
        let b = engine.admit("t", &two, 1, 1, CostEstimate::default()).await.unwrap();
        assert!(engine.admit("t", &two, 1, 1, CostEstimate::default()).await.is_err());
        // Lowered while two are in flight: nothing new until below the cap.
        drop(a);
        assert_eq!(
            engine.admit("t", &one, 1, 1, CostEstimate::default()).await.unwrap_err(),
            LimitError::ConcurrencyLimited { limit: 1 }
        );
        drop(b);
        assert!(engine.admit("t", &one, 1, 1, CostEstimate::default()).await.is_ok());
    }

    /// Admission refused on a later check must hand its concurrency slot
    /// back: the slot is taken first, so every refusal path returns it.
    #[tokio::test]
    async fn every_refusal_returns_its_concurrency_slot() {
        let engine = LimitEngine::new(64, true);
        let rpm = limits(1, 0, 1, 0);
        drop(engine.admit("rpm", &rpm, 1, 1, CostEstimate::default()).await.unwrap());
        for _ in 0..5 {
            assert!(matches!(
                engine.admit("rpm", &rpm, 1, 1, CostEstimate::default()).await,
                Err(LimitError::RateLimited { .. })
            ));
        }
        assert_eq!(engine.usage("rpm").in_flight, 0);

        let broke = limits(0, 0, 1, 10);
        for _ in 0..5 {
            assert!(engine.admit("broke", &broke, 1, 1, estimate_for(10, 100)).await.is_err());
        }
        assert_eq!(engine.usage("broke").in_flight, 0);
        assert!(engine.admit("broke", &broke, 1, 1, CostEstimate { prompt_nano_usd: 10, completion_nano_usd: 0 }).await.is_ok());
    }

    // -----------------------------------------------------------------------
    // Quota split
    // -----------------------------------------------------------------------

    #[test]
    fn replica_shares_never_sum_past_the_budget() {
        // Exhaustive over awkward budgets, replica counts and margins: the
        // shares, rounded down, can never add up to more than the budget.
        for budget in [1u64, 2, 3, 7, 99, 1_000, 1_000_003, u64::MAX / 3] {
            for replicas in 1..=9u32 {
                for margin_percent in [1u32, 50, 95, 99, 100] {
                    let split = QuotaSplit { replicas, margin_percent };
                    let total = split.share(budget) as u128 * u128::from(replicas);
                    assert!(total <= budget as u128, "{budget} {replicas} {margin_percent}");
                }
            }
        }
    }

    #[tokio::test]
    async fn each_replica_enforces_only_its_share() {
        let per_request = estimate_for(10, 100).total();
        let split = QuotaSplit { replicas: 3, margin_percent: 100 };
        // Room for 6 requests in all, so 2 per replica.
        let l = limits(0, 0, 64, per_request * 6 + 5);
        let replica = LimitEngine::with_ledger_split(64, true, Arc::new(MemoryLedger::default()), Some(split), Fingerprinter::unkeyed());
        let mut held = Vec::new();
        while let Ok(r) = replica.admit("t", &l, 10, 100, estimate_for(10, 100)).await {
            held.push(r);
        }
        assert_eq!(held.len(), 2);
        assert_eq!(snap(&replica, "t", &l).budget_nano_usd, split.share(l.daily_budget_nano_usd));
    }

    /// The fail-open trap: a real budget whose share rounds down to zero
    /// would reach the ledger as `0`, which means "no ceiling".
    #[tokio::test]
    async fn a_share_that_rounds_to_zero_refuses_instead_of_unlimiting() {
        let split = QuotaSplit { replicas: 3, margin_percent: 100 };
        let l = limits(0, 0, 64, 2);
        let replica = LimitEngine::with_ledger_split(64, true, Arc::new(MemoryLedger::default()), Some(split), Fingerprinter::unkeyed());
        let err = replica
            .admit("t", &l, 1, 1, CostEstimate { prompt_nano_usd: 1, completion_nano_usd: 0 })
            .await
            .unwrap_err();
        assert!(matches!(err, LimitError::BudgetWouldBeExceeded { remaining: 0, .. }), "{err:?}");
        assert_eq!(snap(&replica, "t", &l).spent_nano_usd, 0);
        assert_eq!(replica.usage("t").requests_last_minute, 0, "a refusal consumes no rate limit");

        let unlimited = limits(0, 0, 64, 0);
        assert!(replica.admit("t", &unlimited, 1, 1, CostEstimate::default()).await.is_ok(), "0 is still no ceiling");
    }
}
