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
    tracing::info!(backend = ledger.backend(), "spend ledger ready");
    let limits = LimitEngine::with_ledger(
        settings.server.max_concurrent_streams_global,
        settings.governance.cost_tracking_enabled,
        ledger,
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
    match cfg.backend {
        LedgerBackend::Memory => Ok(Arc::new(MemoryLedger::new(retention))),
        LedgerBackend::Postgres => {
            let url = std::env::var(&cfg.url_env).map_err(|_| {
                BootError::Ledger(format!("backend is postgres but `{}` is not set", cfg.url_env))
            })?;
            let ledger = PostgresLedger::connect(PostgresOptions {
                url,
                schema: cfg.schema.clone(),
                max_connections: cfg.max_connections,
                timeout: Duration::from_millis(cfg.timeout_ms),
                sweep_after: Duration::from_secs(cfg.sweep_after_secs),
                sweep_interval: Duration::from_secs(cfg.sweep_interval_secs),
                idempotency_retention: retention,
            })
            .await
            .map_err(BootError::Ledger)?;
            Ok(ledger)
        }
    }
}
