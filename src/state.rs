//! Shared application state.

use std::sync::Arc;

use crate::{
    config::Settings,
    governance::{limits::LimitEngine, policy::PolicyEngine, Authenticator, TenantRegistry},
    observability::SharedMetrics,
    router::{ModelRegistry, ProviderPool as RouterProviderPool},
};

pub struct AppState {
    pub settings: Arc<Settings>,
    pub models: Arc<ModelRegistry>,
    pub tenants: Arc<TenantRegistry>,
    pub providers: RouterProviderPool,
    pub auth: Arc<dyn Authenticator>,
    pub policy: PolicyEngine,
    pub limits: Arc<LimitEngine>,
    pub metrics: SharedMetrics,
    pub started_at: std::time::Instant,
    /// Live stream count, incremented when a stream is constructed and
    /// decremented when it is dropped. Lets shutdown drain cleanly instead of
    /// guessing with a fixed sleep.
    pub inflight: Arc<std::sync::atomic::AtomicU64>,
}

impl AppState {
    pub fn uptime_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    pub fn inflight(&self) -> u64 {
        self.inflight.load(std::sync::atomic::Ordering::SeqCst)
    }
}
