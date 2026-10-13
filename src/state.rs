//! Shared application state.

use std::sync::{Arc, RwLock};

use crate::{
    config::Settings,
    governance::{
        auth::build_authenticator, limits::LimitEngine, policy::{self, PolicyEngine, TenantRegistry},
        Authenticator,
    },
    observability::SharedMetrics,
    router::{ModelRegistry, ProviderPool as RouterProviderPool, RegistrySnapshot},
};

/// The configuration generation visible to request handlers. All three parts
/// are swapped together so authentication, tenant policy, and model routing
/// always describe the same on-disk generation.
pub struct RegistryGeneration {
    pub models: Arc<RegistrySnapshot>,
    pub tenants: Arc<TenantRegistry>,
    pub auth: Arc<dyn Authenticator>,
}

pub struct AppState {
    pub settings: Arc<Settings>,
    /// The model loader also owns reload metadata and the on-disk path. Request
    /// handlers read the immutable snapshot from `generation` instead.
    pub models: Arc<ModelRegistry>,
    /// Live configuration generation. The `Arc` is swapped whole after all
    /// reloadable files have parsed and validated.
    pub generation: RwLock<Arc<RegistryGeneration>>,
    pub providers: RouterProviderPool,
    pub policy: PolicyEngine,
    pub limits: Arc<LimitEngine>,
    pub metrics: SharedMetrics,
    pub started_at: std::time::Instant,
    /// Serializes administrative and hot reloads so a prepared generation
    /// cannot overtake another reload before it is published.
    pub reload_lock: tokio::sync::Mutex<()>,
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

    /// The complete configuration generation currently serving, cloned out
    /// from under the lock so callers can hold it across `await`s without
    /// blocking a reload.
    pub fn current_generation(&self) -> Arc<RegistryGeneration> {
        self.generation
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn current_tenants(&self) -> Arc<TenantRegistry> {
        Arc::clone(&self.current_generation().tenants)
    }

    /// The authenticator currently serving, cloned the same way.
    pub fn current_auth(&self) -> Arc<dyn Authenticator> {
        Arc::clone(&self.current_generation().auth)
    }

    /// Re-read both reloadable registries, validate them together, and publish
    /// one complete generation. A failed load, validation error, or failed
    /// authenticator rebuild leaves the previous generation untouched.
    pub async fn reload_all(&self) -> Result<(u64, usize), String> {
        let _reload = self.reload_lock.lock().await;
        let prepared_models = self
            .models
            .prepare_reload()
            .map_err(|e| format!("model registry reload failed: {e}"))?;
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
        let models = Arc::clone(&prepared_models.snapshot);
        // Install the model metadata before the single visible generation
        // swap. Request handlers only read through `generation`, so they keep
        // seeing the previous complete generation until this point.
        self.models.install_prepared(prepared_models).await;
        let tenants = fresh;
        let generation = Arc::new(RegistryGeneration {
            models,
            tenants: Arc::clone(&tenants),
            auth: auth_new,
        });
        let model_generation = generation.models.generation;
        let tenant_count = tenants.len();
        *self
            .generation
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = generation;
        Ok((model_generation, tenant_count))
    }

    /// Re-read only the tenant file for the file-specific hot-reload path.
    /// The tenant registry and authenticator still publish as one generation.
    pub async fn reload_tenants(&self) -> Result<usize, String> {
        let _reload = self.reload_lock.lock().await;
        let current = self.current_generation();
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
        let tenants = fresh;
        *self
            .generation
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::new(RegistryGeneration {
            models: Arc::clone(&current.models),
            tenants: Arc::clone(&tenants),
            auth: auth_new,
        });
        Ok(tenants.len())
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

    fn write_models(path: &std::path::Path, model_id: &str) {
        std::fs::write(
            path,
            format!(
                "schema_version: 1\nmodels:\n  \"{model_id}\":\n    provider: groq\n"
            ),
        )
        .unwrap();
    }

    const WHITELISTED: &str = "groq/gpt-oss-20b";

    async fn build_state(dir: &std::path::Path) -> Arc<AppState> {
        let mut settings = Settings::default();
        settings.registry.models_path = dir.join("models.yaml");
        settings.registry.tenants_path = dir.join("tenants.yaml");
        write_models(&settings.registry.models_path, WHITELISTED);
        let settings = Arc::new(settings);
        let models = crate::router::ModelRegistry::load(settings.registry.models_path.clone(), false, 0).unwrap();
        let tenants = TenantRegistry::load(&settings.registry.tenants_path).unwrap();
        let auth = build_authenticator(&settings.auth, tenants.key_records()).unwrap();
        let generation = Arc::new(RegistryGeneration {
            models: models.snapshot().await,
            tenants,
            auth,
        });
        Arc::new(AppState {
            settings,
            models,
            generation: RwLock::new(generation),
            providers: crate::router::ProviderPool::new(vec![]),
            policy: PolicyEngine::new(true, crate::config::DEFAULT_LIMITS),
            limits: LimitEngine::new(8, false),
            metrics: crate::observability::Metrics::new(Instant::now()),
            started_at: Instant::now(),
            reload_lock: tokio::sync::Mutex::new(()),
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
        let state = build_state(&dir).await;

        let ok = state
            .current_auth()
            .authenticate(&headers_with(&[("x-api-key", "first-secret")]))
            .await
            .expect("initial key must authenticate");
        assert_eq!(&*ok.tenant_id, "alpha");

        write_tenants(&path, &one_tenant_yaml("rotated-secret"));
        let count = state
            .reload_tenants()
            .await
            .expect("a valid rotate must reload");
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
        let state = build_state(&dir).await;

        // A tenant with no `allowed_models` is invalid under strict mode.
        write_tenants(
            &path,
            "tenants:\n  - tenant_id: alpha\n    enabled: true\n    credentials:\n      - key_id: k1\n        key: \"first-secret\"\n        scopes: [chat:stream]\n",
        );
        let err = state
            .reload_tenants()
            .await
            .expect_err("empty allowlist must fail strict validation");
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

    #[tokio::test]
    async fn failed_full_reload_does_not_publish_only_the_model_change() {
        let dir = tmp_dir("reload-atomic");
        let tenants_path = dir.join("tenants.yaml");
        write_tenants(&tenants_path, &one_tenant_yaml("first-secret"));
        let state = build_state(&dir).await;

        // The model edit parses successfully, but the tenant edit fails strict
        // validation. An all-or-nothing reload must publish neither edit.
        write_models(&dir.join("models.yaml"), "groq/new-model");
        write_tenants(
            &tenants_path,
            "tenants:\n  - tenant_id: alpha\n    enabled: true\n    credentials:\n      - key_id: k1\n        key: \"first-secret\"\n        scopes: [chat:stream]\n",
        );

        let err = state
            .reload_all()
            .await
            .expect_err("the invalid tenant generation must be rejected");
        assert!(err.contains("allowed_models"), "unexpected reload error: {err}");

        let generation = state.current_generation();
        assert!(generation.models.resolve(WHITELISTED).is_ok());
        assert!(generation.models.resolve("groq/new-model").is_err());
        generation
            .auth
            .authenticate(&headers_with(&[("x-api-key", "first-secret")]))
            .await
            .expect("the previous tenant and auth generation must still serve");

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
        let state = build_state(&dir).await;
        let auth = |key: &'static str| {
            let state = Arc::clone(&state);
            async move { state.current_auth().authenticate(&headers_with(&[("x-api-key", key)])).await.is_ok() }
        };

        assert!(auth("file-secret-1").await, "the trailing newline is trimmed");
        std::fs::write(&secret, "file-secret-2").unwrap();
        assert!(auth("file-secret-1").await, "nothing changes until a reload");
        state
            .reload_tenants()
            .await
            .expect("reload with the rotated file");
        assert!(auth("file-secret-2").await);
        assert!(!auth("file-secret-1").await, "the old key is revoked by the reload");

        std::fs::remove_dir_all(&dir).ok();
    }
}
