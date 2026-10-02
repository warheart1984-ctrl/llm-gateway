//! Startup wiring: config -> registries -> providers -> governance -> state.
//!
//! Kept separate from `main` so the integration tests can build the exact same
//! object graph the binary does, and so every failure mode surfaces as a typed
//! error at boot rather than a 502 on the first request.

use std::sync::Arc;

use crate::{
    config::Settings,
    governance::{
        auth::build_authenticator, limits::LimitEngine, policy::{self, PolicyEngine, TenantRegistry},
    },
    providers::{
        ChatProvider, GroqAdapter, NvidiaAdapter, OpenRouterAdapter, ProviderPool, UpstreamTuning,
    },
    router::ModelRegistry,
    state::AppState,
};

#[derive(Debug, thiserror::Error)]
pub enum BootError {
    #[error("configuration: {0}")]
    Config(#[from] crate::config::ConfigError),
    #[error("model registry: {0}")]
    ModelRegistry(#[from] crate::router::RegistryError),
    #[error("tenant registry: {0}")]
    Policy(#[from] policy::PolicyError),
    #[error("authentication: {0}")]
    Auth(#[from] crate::governance::AuthError),
    #[error("no usable providers: {0}. Set the matching API key, or point a model at a local endpoint.")]
    NoProviders(String),
    #[error("registry validation failed:\n{}", .0.join("\n"))]
    InvalidRegistry(Vec<String>),
    /// A configured Postgres ledger that cannot be reached is fatal: falling
    /// back to process memory would silently drop the guarantees it exists
    /// to give (durability across restarts, one budget across replicas).
    #[error("ledger: {0}")]
    Ledger(String),
    #[error("could not build an upstream HTTP client: {0}")]
    Provider(String),
}

/// Build the full object graph, with the ledger `settings.ledger` names.
pub async fn build(settings: Settings) -> Result<Arc<AppState>, BootError> {
    let ledger = build_ledger(&settings.ledger).await?;
    build_with_ledger(settings, ledger).await
}

/// Build the full object graph on a ledger the caller already has: how two
/// gateway processes are pointed at one shared ledger in tests, and how an
/// embedder supplies its own.
pub async fn build_with_ledger(
    settings: Settings,
    ledger: Arc<dyn crate::governance::ledger::Ledger>,
) -> Result<Arc<AppState>, BootError> {
    let settings = Arc::new(settings);

    // Registries first: everything else is validated against them.
    let models = ModelRegistry::load(
        settings.registry.models_path.clone(),
        settings.registry.hot_reload,
        settings.registry.reload_interval_ms,
    )?;
    let tenants = TenantRegistry::load(settings.registry.tenants_path.clone())?;

    // Providers. Transport settings come from config so a single long-lived
    // `reqwest::Client` per provider honours the operator's timeouts and pool
    // sizes. A vendor with no credential still gets an adapter if the registry
    // routes to it through a local endpoint (NVIDIA NIM), so a self-hosted
    // deployment needs no cloud key at all.
    let tuning = UpstreamTuning::from(&settings.upstream);
    let mut adapters: Vec<Arc<dyn ChatProvider>> = Vec::new();

    let key_for = |var: &str| -> Option<String> {
        std::env::var(var).ok().filter(|v| !v.trim().is_empty()).map(|v| v.trim().to_string())
    };

    if let Some(key) = key_for("GROQ_API_KEY") {
        adapters.push(Arc::new(
            GroqAdapter::tuned(Some(key), &tuning).map_err(|e| BootError::Provider(e.to_string()))?,
        ));
    }
    if let Some(key) = key_for("OPENROUTER_API_KEY") {
        adapters.push(Arc::new(
            OpenRouterAdapter::tuned(Some(key), &tuning)
                .map_err(|e| BootError::Provider(e.to_string()))?,
        ));
    }
    // NVIDIA always gets an adapter. With a key it serves cloud NIM; without
    // one it serves self-hosted endpoints, which is a complete deployment on
    // its own.
    let nvidia_key = key_for("NVIDIA_API_KEY");
    adapters.push(Arc::new(
        NvidiaAdapter::tuned(nvidia_key.clone(), &tuning)
            .map_err(|e| BootError::Provider(e.to_string()))?,
    ));
    if nvidia_key.is_none() {
        tracing::info!(
            "NVIDIA_API_KEY is not set; NVIDIA serves self-hosted endpoints only"
        );
    }

    let providers = ProviderPool::new(adapters);
    if providers.is_empty() {
        return Err(BootError::NoProviders("no provider adapters constructed".into()));
    }

    // Cross-check registries before serving. Fatal problems stop the boot; a
    // tenant referencing a provider whose credential is not set yet is only
    // warned about, because refusing to start would turn a missing secret into
    // a full outage.
    let pool = providers.names();
    let validation = policy::validate(
        &tenants,
        settings.governance.require_model_allowlist,
        &|p| pool.contains(&p.to_ascii_lowercase()),
    );
    for warning in &validation.warnings {
        tracing::warn!(%warning, "tenant registry warning");
    }
    if !validation.errors.is_empty() {
        return Err(BootError::InvalidRegistry(validation.errors));
    }

    let auth = build_authenticator(&settings.auth, tenants.key_records())?;
    let policy = PolicyEngine::new(
        settings.governance.require_model_allowlist,
        settings.governance.default_limits.clone(),
    );
    let quota_split = quota_split(&settings.ledger, ledger.backend())?;
    // Holds live in the ledger, and under quota split each replica has its
    // own: a hold made on one replica is unknown to the others. That fails
    // closed (an unknown hold is refused), but approvals only work if a
    // tenant's requests, polls and approvals reach the same replica.
    if quota_split.is_some() {
        let held: Vec<&str> = tenants
            .tenant_ids()
            .into_iter()
            .filter(|id| tenants.get(id).is_some_and(|t| t.holds.is_some()))
            .collect();
        if !held.is_empty() {
            tracing::warn!(
                tenants = ?held,
                "these tenants hold requests for approval, and under quota split each replica keeps its own holds:                  route each tenant's requests, hold polls, approvals and resubmissions to one replica, or use postgres"
            );
        }
    }
    if ledger.backend() == "memory" {
        tracing::warn!(
            "THE IN-MEMORY LEDGER IS IN USE: a restart forgets all spend, idempotency keys and decisions, \
             and each replica enforces the full budget. For tests and development only; \
             use backend = \"sqlite\" or \"postgres\" in production"
        );
    }
    tracing::info!(
        backend = ledger.backend(),
        quota_split_replicas = quota_split.map(|s| s.replicas).unwrap_or(0),
        "spend ledger ready"
    );
    let fingerprinter = fingerprinter(&settings.ledger.fingerprint_keys_env, ledger.backend())?;
    let limits = LimitEngine::with_ledger_split(
        settings.server.max_concurrent_streams_global,
        settings.governance.cost_tracking_enabled,
        ledger,
        quota_split,
        fingerprinter,
    );

    // Report which catalogue entries cannot currently be served. Not fatal:
    // the gateway is a multi-provider front door, and one absent credential
    // should not stop the others from working.
    let snapshot = models.snapshot().await;
    let unavailable: Vec<&str> = snapshot
        .ids()
        .into_iter()
        .filter(|id| {
            snapshot
                .by_id
                .get(*id)
                .is_some_and(|cfg| !pool.contains(&cfg.provider.to_ascii_lowercase()))
        })
        .collect();
    if !unavailable.is_empty() {
        tracing::warn!(
            models = ?unavailable,
            "some registered models have no configured provider adapter and will return an error until a credential is set"
        );
    }

    let metrics = crate::observability::Metrics::new(std::time::Instant::now());

    Ok(Arc::new(AppState {
        settings,
        models,
        tenants: std::sync::RwLock::new(tenants),
        providers,
        auth: std::sync::RwLock::new(auth),
        policy,
        limits,
        metrics,
        started_at: std::time::Instant::now(),
        inflight: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    }))
}

async fn build_ledger(
    cfg: &crate::config::LedgerConfig,
) -> Result<Arc<dyn crate::governance::ledger::Ledger>, BootError> {
    use crate::{
        config::LedgerBackend,
        governance::ledger::{MemoryLedger, PostgresLedger, postgres::PostgresOptions},
    };
    use std::time::Duration;

    let retention = Duration::from_secs(cfg.idempotency_retention_secs);
    let decision_retention = Duration::from_secs(u64::from(cfg.decision_retention_days) * 86_400);
    let reservation_retention = Duration::from_secs(u64::from(cfg.reservation_retention_days) * 86_400);
    // A reservation deleted while its idempotency key should still be
    // remembered would let a repeat execute again.
    if !reservation_retention.is_zero() && reservation_retention < retention {
        return Err(BootError::Ledger(format!(
            "ledger.reservation_retention_days ({}) must cover ledger.idempotency_retention_secs ({})",
            cfg.reservation_retention_days, cfg.idempotency_retention_secs
        )));
    }
    match cfg.backend {
        LedgerBackend::Memory => Ok(Arc::new(MemoryLedger::new(retention))),
        LedgerBackend::Sqlite => {
            let ledger = crate::governance::ledger::SqliteLedger::connect(
                crate::governance::ledger::sqlite::SqliteOptions {
                    path: cfg.sqlite_path.clone(),
                    timeout: Duration::from_millis(cfg.timeout_ms),
                    lease: Duration::from_secs(cfg.lease_secs),
                    sweep_interval: Duration::from_secs(cfg.sweep_interval_secs),
                    idempotency_retention: retention,
                    sealer: response_sealer(&cfg.response_keys_env)?,
                    decision_retention,
                    max_pending_closings: cfg.max_pending_closings,
                    reservation_retention,
                },
            )
            .await
            .map_err(BootError::Ledger)?;
            Ok(ledger)
        }
        LedgerBackend::Postgres => {
            let url = std::env::var(&cfg.url_env).map_err(|_| {
                BootError::Ledger(format!("backend is postgres but `{}` is not set", cfg.url_env))
            })?;
            let sealer = response_sealer(&cfg.response_keys_env)?;
            let ledger = PostgresLedger::connect(PostgresOptions {
                url,
                schema: cfg.schema.clone(),
                max_connections: cfg.max_connections,
                timeout: Duration::from_millis(cfg.timeout_ms),
                lease: Duration::from_secs(cfg.lease_secs),
                sweep_interval: Duration::from_secs(cfg.sweep_interval_secs),
                idempotency_retention: retention,
                sealer,
                decision_retention,
                max_pending_closings: cfg.max_pending_closings,
                reservation_retention,
            })
            .await
            .map_err(BootError::Ledger)?;
            Ok(ledger)
        }
    }
}

/// The keys that seal stored answers. Absent: answers are not stored. Set
/// but malformed: fatal, because a typo must not silently turn encryption
/// off, and the error never repeats the key material.
fn response_sealer(
    env_name: &str,
) -> Result<Option<Arc<crate::governance::ledger::sealed::ResponseSealer>>, BootError> {
    match std::env::var(env_name) {
        Ok(spec) if !spec.trim().is_empty() => {
            let sealer = crate::governance::ledger::sealed::ResponseSealer::from_keys(&spec)
                .map_err(|e| BootError::Ledger(format!("`{env_name}`: {e}")))?;
            tracing::info!(key_id = sealer.current_key_id(), "stored answers are sealed with AES-256-GCM");
            Ok(Some(Arc::new(sealer)))
        }
        _ => {
            tracing::warn!(
                "`{env_name}` is not set: completions are not stored, so a repeated Idempotency-Key gets 409 rather than a replay"
            );
            Ok(None)
        }
    }
}

/// Quota-split mode, validated against the ledger actually in use. It is
/// only sound on a durable ledger that is private to this replica: on the
/// in-memory ledger a restart would hand this replica a fresh share (failing
/// open), and on Postgres the replicas already share one budget exactly, so
/// splitting would only strand it.
fn quota_split(
    cfg: &crate::config::LedgerConfig,
    backend: &str,
) -> Result<Option<crate::governance::QuotaSplit>, BootError> {
    if cfg.quota_split_replicas == 0 {
        return Ok(None);
    }
    if backend != "sqlite" {
        return Err(BootError::Ledger(format!(
            "quota_split_replicas needs backend = \"sqlite\" (a durable ledger private to each replica), not `{backend}`"
        )));
    }
    if !(1..=100).contains(&cfg.quota_split_margin_percent) {
        return Err(BootError::Ledger(format!(
            "quota_split_margin_percent must be 1 to 100, got {}",
            cfg.quota_split_margin_percent
        )));
    }
    Ok(Some(crate::governance::QuotaSplit {
        replicas: cfg.quota_split_replicas,
        margin_percent: cfg.quota_split_margin_percent,
    }))
}

/// The keys that fingerprint idempotent requests. Set but malformed: fatal.
/// Unset: plain SHA-256, with a warning when the ledger keeps them on disk.
fn fingerprinter(
    env_name: &str,
    backend: &str,
) -> Result<crate::governance::ledger::Fingerprinter, BootError> {
    use crate::governance::ledger::Fingerprinter;
    match std::env::var(env_name) {
        Ok(spec) if !spec.trim().is_empty() => {
            Fingerprinter::from_keys(&spec).map_err(|e| BootError::Ledger(format!("`{env_name}`: {e}")))
        }
        _ => {
            if backend != "memory" {
                tracing::warn!(
                    "`{env_name}` is not set: request fingerprints are stored unkeyed, so anyone who can read the ledger can test a guessed prompt against them"
                );
            }
            Ok(Fingerprinter::unkeyed())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{LedgerBackend, LedgerConfig};

    fn ledger_config(reservation_retention_days: u32, idempotency_retention_secs: u64) -> LedgerConfig {
        LedgerConfig {
            backend: LedgerBackend::Memory,
            reservation_retention_days,
            idempotency_retention_secs,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn reservations_must_outlive_the_keys_that_point_at_them() {
        // Deleting a reservation while its idempotency key is still honoured
        // would let a repeat execute again.
        let err = build_ledger(&ledger_config(1, 2 * 86_400)).await.unwrap_err();
        let message = err.to_string();
        assert!(message.contains("reservation_retention_days") && message.contains("idempotency_retention_secs"), "{message}");
        assert!(build_ledger(&ledger_config(0, 2 * 86_400)).await.is_ok(), "0 keeps reservations forever");
        assert!(build_ledger(&ledger_config(30, 86_400)).await.is_ok(), "the shipped defaults");
    }
}
