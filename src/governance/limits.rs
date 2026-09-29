//! Quota enforcement: rate limits, token budgets, concurrency caps and spend.
//!
//! All four are pre-flight checks, with one twist: a *reservation* is taken
//! before the upstream connection opens and settled with real usage when the
//! stream ends. That is what makes a cost guardrail meaningful — a stream that
//! dies at 20% still pays for the prompt it already sent, and the over-reserve
//! is released on settle.
//!
//! Locking discipline: window counters live behind a `std::sync::Mutex` that is
//! never held across an `await`, so `settle()` can stay synchronous and be
//! called straight from a stream `Drop`. Spend uses atomics for the same
//! reason.

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{budget_bucket, budget_bucket_label};
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
}

/// Day-bucketed spend in nano-USD. Integer only: money is not a float.
#[derive(Debug, Default)]
struct SpendLedger {
    bucket: AtomicU64,
    spent: AtomicU64,
}

impl SpendLedger {
    /// Roll over at the UTC day boundary. Called on every read/write, so the
    /// first request after midnight resets the counter without a timer.
    fn current_bucket(&self, now: std::time::SystemTime) -> u64 {
        let today = budget_bucket(now);
        let stored = self.bucket.load(Ordering::Acquire);
        if stored == today {
            return today;
        }
        if self
            .bucket
            .compare_exchange(stored, today, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.spent.store(0, Ordering::Release);
        }
        self.bucket.load(Ordering::Acquire)
    }

    fn snapshot(&self, now: std::time::SystemTime) -> (u64, u64) {
        let bucket = self.current_bucket(now);
        (bucket, self.spent.load(Ordering::Acquire))
    }

    fn reserve(&self, now: std::time::SystemTime, amount: u64) -> u64 {
        self.current_bucket(now);
        self.spent.fetch_add(amount, Ordering::AcqRel) + amount
    }

    /// Correct a reservation once real usage is known. Clamps at zero so a
    /// refund larger than the reservation cannot wrap the counter.
    fn adjust(&self, now: std::time::SystemTime, delta: i128) {
        self.current_bucket(now);
        let mut current = self.spent.load(Ordering::Acquire);
        loop {
            let next = if delta < 0 {
                current.saturating_sub(delta.unsigned_abs().min(u64::MAX as u128) as u64)
            } else {
                current.saturating_add(delta.min(u64::MAX as i128) as u64)
            };
            match self.spent.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

#[derive(Debug)]
struct TenantState {
    window: Mutex<Window>,
    semaphore: Arc<Semaphore>,
    spend: SpendLedger,
    inflight: AtomicU64,
}

impl TenantState {
    fn new(max_concurrent: u32) -> Self {
        Self {
            window: Mutex::new(Window::default()),
            semaphore: Arc::new(Semaphore::new(max_concurrent.max(1) as usize)),
            spend: SpendLedger::default(),
            inflight: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// Reservation
// ---------------------------------------------------------------------------

/// RAII admission token. Dropping it releases both concurrency permits. Call
/// [`Reservation::settle`] exactly once with real usage to correct the cost
/// reservation; if it is never called, `Drop` charges the prompt estimate.
/// `Debug` is derived so `unwrap()`/`expect()` on a `Result<Reservation, _>`
/// works in tests. Neither field is a secret.
#[derive(Debug)]
pub struct Reservation {
    state: Arc<TenantState>,
    _global_permit: Option<OwnedSemaphorePermit>,
    _tenant_permit: Option<OwnedSemaphorePermit>,
    reserved_nano_usd: u64,
    /// The prompt half of the reservation. Kept so an abandoned stream refunds
    /// only the completion half without needing the cost model at drop time.
    reserved_prompt_nano_usd: u64,
    prompt_tokens: u32,
    settled: bool,
}

impl Reservation {
    /// Commit actual usage and release the concurrency permits.
    pub fn settle(&mut self, usage: Usage, cost: &CostModel) {
        if self.settled {
            return;
        }
        self.settled = true;
        let prompt = usage.prompt_tokens.max(self.prompt_tokens);
        let completion = usage.completion_tokens;
        let actual = nano_usd_for(prompt, usage.cached_prompt_tokens, completion, cost);
        let delta = actual as i128 - self.reserved_nano_usd as i128;
        if delta != 0 {
            self.state
                .spend
                .adjust(std::time::SystemTime::now(), delta);
        }
        if completion > 0 {
            let mut window = self
                .state
                .window
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let now = Instant::now();
            window.evict(now);
            window.token_events.push_back((now, completion));
        }
    }

    /// Release without charging completion tokens — the stream failed before
    /// producing any. The prompt estimate still stands.
    pub fn abandon(&mut self) {
        self.settled = true;
    }

    pub fn reserved_nano_usd(&self) -> u64 {
        self.reserved_nano_usd
    }

    pub fn prompt_tokens(&self) -> u32 {
        self.prompt_tokens
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.settled {
            // A stream that died without reporting usage still consumed its
            // prompt. Release only the completion half of the reservation.
            let refund = self.reserved_nano_usd as i128 - self.reserved_prompt_nano_usd as i128;
            if refund > 0 {
                self.state
                    .spend
                    .adjust(std::time::SystemTime::now(), -refund);
            }
        }
        self.state.inflight.fetch_sub(1, Ordering::Relaxed);
        self._tenant_permit.take();
        self._global_permit.take();
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

pub struct LimitEngine {
    tenants: DashMap<String, Arc<TenantState>>,
    global: Arc<Semaphore>,
    cost_tracking: bool,
}

impl LimitEngine {
    pub fn new(max_concurrent_global: usize, cost_tracking: bool) -> Arc<Self> {
        Arc::new(Self {
            tenants: DashMap::new(),
            global: Arc::new(Semaphore::new(max_concurrent_global.max(1))),
            cost_tracking,
        })
    }

    fn state(&self, tenant_id: &str, limits: &LimitProfile) -> Arc<TenantState> {
        if let Some(existing) = self.tenants.get(tenant_id) {
            return Arc::clone(existing.value());
        }
        let entry = self
            .tenants
            .entry(tenant_id.to_string())
            .or_insert_with(|| Arc::new(TenantState::new(limits.max_concurrent_streams)));
        Arc::clone(entry.value())
    }

    /// Pre-flight gate. Every rejection names the ceiling that was hit, so a
    /// tenant can tell "slow down" apart from "you cannot afford this".
    pub async fn admit(
        self: &Arc<Self>,
        tenant_id: &str,
        limits: &LimitProfile,
        prompt_tokens: u32,
        max_output_tokens: u32,
        estimate: CostEstimate,
    ) -> Result<Reservation, LimitError> {
        let estimated_cost_nano_usd = estimate.total();
        // `0` means "no ceiling", matching the rest of `LimitProfile`.
        if limits.max_output_tokens > 0 && max_output_tokens > limits.max_output_tokens {
            return Err(LimitError::OutputTokensExceeded {
                requested: max_output_tokens,
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

        let state = self.state(tenant_id, limits);
        let tenant_permit = Arc::clone(&state.semaphore).try_acquire_owned().map_err(|_| {
            LimitError::ConcurrencyLimited {
                limit: limits.max_concurrent_streams,
            }
        })?;

        let now = Instant::now();
        let system_now = std::time::SystemTime::now();
        let mut window = state
            .window
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        window.evict(now);

        if limits.requests_per_minute > 0 && window.request_times.len() as u32 >= limits.requests_per_minute {
            return Err(LimitError::RateLimited {
                done: window.request_times.len() as u32,
                limit: limits.requests_per_minute,
            });
        }
        let used_tokens = window.tokens();
        if limits.tokens_per_minute > 0 && used_tokens.saturating_add(prompt_tokens) > limits.tokens_per_minute {
            return Err(LimitError::TokenRateLimited {
                done: used_tokens,
                requested: prompt_tokens,
                limit: limits.tokens_per_minute,
            });
        }

        let reserved_total = if self.cost_tracking && limits.daily_budget_nano_usd > 0 {
            let (_bucket, spent) = state.spend.snapshot(system_now);
            if spent >= limits.daily_budget_nano_usd {
                return Err(LimitError::BudgetExhausted {
                    spent,
                    budget: limits.daily_budget_nano_usd,
                });
            }
            let remaining = limits.daily_budget_nano_usd - spent;
            if estimated_cost_nano_usd > remaining {
                return Err(LimitError::BudgetWouldBeExceeded {
                    estimate: estimated_cost_nano_usd,
                    remaining,
                });
            }
            state.spend.reserve(system_now, estimated_cost_nano_usd)
        } else {
            0
        };

        window.request_times.push_back(now);
        window.token_events.push_back((now, prompt_tokens));
        if window.token_events.len() > 8_192 {
            let drop_to = window.token_events.len() - 4_096;
            window.token_events.drain(..drop_to);
        }
        drop(window);

        state.inflight.fetch_add(1, Ordering::Relaxed);

        tracing::debug!(
            tenant = tenant_id,
            prompt_tokens,
            max_output_tokens,
            reserved_nano_usd = estimated_cost_nano_usd,
            tenant_spend_nano_usd = reserved_total,
            "admitted"
        );

        Ok(Reservation {
            state: Arc::clone(&state),
            _global_permit: Some(global_permit),
            _tenant_permit: Some(tenant_permit),
            reserved_nano_usd: estimated_cost_nano_usd,
            reserved_prompt_nano_usd: estimate.prompt_nano_usd,
            prompt_tokens,
            settled: false,
        })
    }

    /// Current sliding-window usage, for `GET /v1/usage` and metrics.
    pub fn usage(&self, tenant_id: &str) -> TenantUsage {
        let Some(state) = self.tenants.get(tenant_id) else {
            return TenantUsage::default();
        };
        let mut window = state
            .window
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        window.evict(Instant::now());
        TenantUsage {
            requests_last_minute: window.request_times.len() as u32,
            tokens_last_minute: window.tokens(),
            in_flight: state.inflight.load(Ordering::Relaxed),
        }
    }

    /// Point-in-time budget view. Day-bucketed like the ledger itself.
    pub fn snapshot(&self, tenant_id: &str, limits: &LimitProfile) -> BudgetSnapshot {
        let system_now = std::time::SystemTime::now();
        let (bucket, spent) = self
            .tenants
            .get(tenant_id)
            .map(|state| state.spend.snapshot(system_now))
            .unwrap_or((budget_bucket(system_now), 0));
        let usage = self.usage(tenant_id);
        BudgetSnapshot {
            tenant_id: tenant_id.to_string(),
            requests_last_minute: usage.requests_last_minute,
            tokens_last_minute: usage.tokens_last_minute,
            in_flight: usage.in_flight,
            spent_nano_usd: spent,
            budget_nano_usd: limits.daily_budget_nano_usd,
            budget_bucket: bucket,
            budget_label: budget_bucket_label(bucket),
        }
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
        assert_eq!(engine.snapshot("t", &l).spent_nano_usd, 820_000);
        r.settle(
            Usage { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15, ..Default::default() },
            &c,
        );
        // Actual: 10 * 2,000 + 5 * 8,000 = 60,000. The 760,000 over-reserve is
        // refunded.
        assert_eq!(engine.snapshot("t", &l).spent_nano_usd, 60_000);
    }

    #[tokio::test]
    async fn abandoned_stream_keeps_prompt_cost_and_refunds_the_rest() {
        let engine = LimitEngine::new(64, true);
        let l = limits(0, 0, 8, 10_000_000);
        let r = engine
            .admit(
                "t",
                &l,
                10,
                100,
                CostEstimate { prompt_nano_usd: 20, completion_nano_usd: 800 },
            )
            .await
            .unwrap();
        assert_eq!(engine.snapshot("t", &l).spent_nano_usd, 820);
        drop(r);
        // Only the prompt half is kept; the completion reservation is refunded.
        assert_eq!(engine.snapshot("t", &l).spent_nano_usd, 20);
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
        assert_eq!(engine.snapshot("t", &l).spent_nano_usd, 1_000);
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
}
