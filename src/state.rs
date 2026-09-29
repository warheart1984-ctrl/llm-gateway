//! Shared application state.

use std::sync::{Arc, RwLock};

use crate::{
    config::Settings,
    governance::{
        auth::build_authenticator, limits::LimitEngine, policy::{self, PolicyEngine, TenantRegistry},
        Authenticator,
    },
    observability::SharedMetrics,
    router::{ModelRegistry, ProviderPool as RouterProviderPool},
};

pub struct AppState {
    pub settings: Arc<Settings>,
    pub models: Arc<ModelRegistry>,
    /// Live tenant registry. The `Arc` is swapped whole by
    /// [`AppState::reload_tenants`], so the request path never sees a
    /// half-loaded file and a failed reload keeps the previous generation
    /// serving. The hot path clones the `Arc` under a brief read lock and
    /// drops the guard before any `await`.
    pub tenants: RwLock<Arc<TenantRegistry>>,
    pub providers: RouterProviderPool,
    /// Swapped alongside `tenants` on reload, so a revoked or rotated key stops
    /// authenticating immediately rather than after the next restart.
    pub auth: RwLock<Arc<dyn Authenticator>>,
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

    /// The registry currently serving, cloned out from under the lock so the
    /// caller can hold it across `await`s without blocking a reload.
    pub fn current_tenants(&self) -> Arc<TenantRegistry> {
        self.tenants
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// The authenticator currently serving, cloned the same way.
    pub fn current_auth(&self) -> Arc<dyn Authenticator> {
        Arc::clone(
            &self
                .auth
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    /// Re-read `tenants.yaml` and atomically swap the registry and the
    /// authenticator derived from its key material. A failed load, a
    /// validation error, or a failed authenticator rebuild leaves both
    /// untouched — the swap is all-or-nothing and the old generation keeps
    /// serving. Returns the number of tenants now registered.
    pub fn reload_tenants(&self) -> Result<usize, String> {
        let fresh = TenantRegistry::load(self.settings.registry.tenants_path.clone())
            .map_err(|e| e.to_string())?;
        let providers = self.providers.names();
        let validation = policy::validate(
            &fresh,
            self.settings.governance.require_model_allowlist,
            &|p| providers.contains(&p.to_ascii_lowercase()),
        );
        if !validation.errors.is_empty() {
            return Err(validation.errors.join("\n"));
        }
        let auth_new = build_authenticator(&self.settings.auth, fresh.key_records())
            .map_err(|e| e.to_string())?;
        // Both guards are sync and short-lived; nothing else takes one while
        // holding the other, so this pair cannot deadlock.
        {
            let mut tenants = self
                .tenants
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *tenants = fresh;
            let mut auth = self
                .auth
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *auth = auth_new;
        }
        let current = self.current_tenants();
        Ok(current.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Settings,
        governance::{
            auth::{build_authenticator, SCOPE_CHAT_STREAM},
            limits::LimitEngine,
            policy::PolicyEngine,
        },
    };
    use axum::http::{HeaderMap, HeaderValue};
    use std::time::Instant;

    fn headers_with(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("llm-gateway-state-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_tenants(path: &std::path::Path, yaml: &str) {
        std::fs::write(path, yaml).unwrap();
    }

    const WHITELISTED: &str = "groq/gpt-oss-20b";

    fn build_state(dir: &std::path::Path) -> Arc<AppState> {
        let mut settings = Settings::default();
        settings.registry.tenants_path = dir.join("tenants.yaml");
        let settings = Arc::new(settings);
        let models = crate::router::ModelRegistry::load("config/models.yaml", false, 0).unwrap();
        let tenants = TenantRegistry::load(&settings.registry.tenants_path).unwrap();
        let auth = build_authenticator(&settings.auth, tenants.key_records()).unwrap();
        Arc::new(AppState {
            settings,
            models,
            tenants: RwLock::new(tenants),
            providers: crate::router::ProviderPool::new(vec![]),
            auth: RwLock::new(auth),
            policy: PolicyEngine::new(true, crate::config::DEFAULT_LIMITS),
            limits: LimitEngine::new(8, false),
            metrics: crate::observability::Metrics::new(Instant::now()),
            started_at: Instant::now(),
            inflight: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    fn one_tenant_yaml(secret: &str) -> String {
        format!(
            "tenants:\n  - tenant_id: alpha\n    enabled: true\n    credentials:\n      - key_id: k1\n        key: \"{secret}\"\n        scopes: [chat:stream]\n    allowed_models: [\"{WHITELISTED}\"]\n    default_model: {WHITELISTED}\n"
        )
    }

    #[tokio::test]
    async fn reload_swaps_keys_and_added_tenants_into_the_data_plane() {
        let dir = tmp_dir("reload-ok");
        let path = dir.join("tenants.yaml");
        write_tenants(&path, &one_tenant_yaml("first-secret"));
        let state = build_state(&dir);

        let ok = state
            .current_auth()
            .authenticate(&headers_with(&[("x-api-key", "first-secret")]))
            .await
            .expect("initial key must authenticate");
        assert_eq!(&*ok.tenant_id, "alpha");

        write_tenants(&path, &one_tenant_yaml("rotated-secret"));
        let count = state.reload_tenants().expect("a valid rotate must reload");
        assert_eq!(count, 1);

        let rotated = state
            .current_auth()
            .authenticate(&headers_with(&[("x-api-key", "rotated-secret")]))
            .await
            .expect("rotated key must authenticate immediately");
        assert_eq!(&*rotated.tenant_id, "alpha");
        assert!(rotated.has_scope(SCOPE_CHAT_STREAM));

        let revoked = state
            .current_auth()
            .authenticate(&headers_with(&[("x-api-key", "first-secret")]))
            .await;
        assert!(
            revoked.is_err(),
            "the old key must stop authenticating the moment the reload lands"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn failed_reload_keeps_the_previous_generation_serving() {
        let dir = tmp_dir("reload-keep");
        let path = dir.join("tenants.yaml");
        write_tenants(&path, &one_tenant_yaml("first-secret"));
        let state = build_state(&dir);

        // A tenant with no `allowed_models` is invalid under strict mode.
        write_tenants(
            &path,
            "tenants:\n  - tenant_id: alpha\n    enabled: true\n    credentials:\n      - key_id: k1\n        key: \"first-secret\"\n        scopes: [chat:stream]\n",
        );
        let err = state.reload_tenants().expect_err("empty allowlist must fail strict validation");
        assert!(
            err.contains("allowed_models"),
            "the failure must name the offending tenant config, got: {err}"
        );

        // Nothing was swapped: the old key still authenticates.
        let ok = state
            .current_auth()
            .authenticate(&headers_with(&[("x-api-key", "first-secret")]))
            .await
            .expect("the previous registry must keep serving");
        assert_eq!(&*ok.tenant_id, "alpha");
        assert_eq!(state.current_tenants().len(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A key held in a mounted secret file rotates on reload: replace the
    /// file, reload, and the old key stops working with no restart. An
    /// env-held key cannot do this — a running process never sees a changed
    /// environment.
    #[tokio::test]
    async fn a_key_file_rotates_on_reload() {
        let dir = tmp_dir("reload-key-file");
        let secret = dir.join("alpha.key");
        std::fs::write(&secret, "file-secret-1
").unwrap();
        let yaml = format!(
            "tenants:
  - tenant_id: alpha
    enabled: true
    credentials:
      - key_id: k1
        key_file: \"{}\"
        scopes: [chat:stream]
    allowed_models: [\"{WHITELISTED}\"]
    default_model: {WHITELISTED}
",
            secret.display().to_string().replace('\\', "/")
        );
        write_tenants(&dir.join("tenants.yaml"), &yaml);
        let state = build_state(&dir);
        let auth = |key: &'static str| {
            let state = Arc::clone(&state);
            async move { state.current_auth().authenticate(&headers_with(&[("x-api-key", key)])).await.is_ok() }
        };

        assert!(auth("file-secret-1").await, "the trailing newline is trimmed");
        std::fs::write(&secret, "file-secret-2").unwrap();
        assert!(auth("file-secret-1").await, "nothing changes until a reload");
        state.reload_tenants().expect("reload with the rotated file");
        assert!(auth("file-secret-2").await);
        assert!(!auth("file-secret-1").await, "the old key is revoked by the reload");

        std::fs::remove_dir_all(&dir).ok();
    }
}
