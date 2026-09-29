//! Tenant registry and authorization policy.
//!
//! The registry is data (`tenants.yaml`) so that adding a tenant is a control
//! plane action, not a deploy. Authorization answers two questions and nothing
//! else: *may this tenant call this model*, and *how large may this request be*.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    config::LimitProfile,
    governance::auth::{key_digest, KeyRecord, Scope, SCOPE_CHAT_STREAM},
};

#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("cannot read tenant registry at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot parse tenant registry at {path}: {message}")]
    Parse { path: PathBuf, message: String },
    #[error("tenant `{0}` is defined more than once")]
    DuplicateTenant(String),
    #[error("tenant `{tenant}` has no credentials")]
    TenantHasNoKeys { tenant: String },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TenantFile {
    #[serde(default)]
    pub tenants: Vec<Tenant>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Tenant {
    pub tenant_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    /// Suffix appended to every model id this tenant may call, e.g. a
    /// `:production` deployment slot. Empty = no suffix.
    #[serde(default)]
    pub model_suffix: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub credentials: Vec<Credential>,
    /// Model patterns this tenant may call. `groq/*` matches every Groq model.
    /// `*` matches everything. Empty means "no models" when
    /// `require_model_allowlist` is on.
    #[serde(default)]
    pub allowed_models: Vec<String>,
    /// Model ids denied even if `allowed_models` would match. Deny wins.
    #[serde(default)]
    pub denied_models: Vec<String>,
    #[serde(default)]
    pub default_model: Option<String>,
    #[serde(default)]
    pub limits: Option<LimitProfile>,
    /// Per-model parameter overrides, keyed by model id. Applied under the
    /// caller's own params, so a tenant policy can force `temperature: 0`.
    #[serde(default)]
    pub model_params: HashMap<String, serde_json::Map<String, serde_json::Value>>,
}

impl Tenant {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    /// Safe-to-log identifier.
    pub key_id: String,
    /// Literal key. Prefer `key_env` in any shared environment.
    #[serde(default)]
    pub key: Option<String>,
    /// Env var holding the key material. Read at boot, never stored raw.
    #[serde(default)]
    pub key_env: Option<String>,
    #[serde(default = "default_scopes")]
    pub scopes: Vec<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_scopes() -> Vec<String> {
    vec![SCOPE_CHAT_STREAM.to_string()]
}

fn default_true() -> bool {
    true
}

impl Credential {
    /// Resolve the raw secret. Errors are loud at boot rather than a runtime
    /// 401 that looks like a client bug.
    pub fn resolve(&self) -> Result<String, PolicyError> {
        if let Some(env_name) = &self.key_env {
            return std::env::var(env_name).map_err(|_| PolicyError::Parse {
                path: PathBuf::from(env_name),
                message: "credential key_env is not set".to_string(),
            });
        }
        self.key.clone().ok_or(PolicyError::Parse {
            path: PathBuf::from(self.key_id.clone()),
            message: "credential needs either `key` or `key_env`".to_string(),
        })
    }

    pub fn to_record(&self, tenant_id: &str) -> Result<KeyRecord, PolicyError> {
        let secret = self.resolve()?;
        if secret.trim().is_empty() {
            return Err(PolicyError::Parse {
                path: PathBuf::from(self.key_id.clone()),
                message: "credential resolved to an empty string".to_string(),
            });
        }
        Ok(KeyRecord {
            key_id: self.key_id.clone(),
            tenant_id: tenant_id.to_string(),
            digest: key_digest(secret.trim()),
            scopes: self.scopes.iter().map(Scope::exact).collect(),
            enabled: self.enabled,
        })
    }
}

/// Immutable view of who exists. Swapped on reload alongside the model
/// registry so the two never disagree about a generation.
pub struct TenantRegistry {
    by_id: HashMap<String, Arc<Tenant>>,
    /// Every key record, flattened, for the authenticator.
    key_records: Vec<KeyRecord>,
    pub path: PathBuf,
}

impl TenantRegistry {
    pub fn load(path: impl Into<PathBuf>) -> Result<Arc<Self>, PolicyError> {
        let path = path.into();
        let text = std::fs::read_to_string(&path).map_err(|source| PolicyError::Io {
            path: path.clone(),
            source,
        })?;
        let file: TenantFile = serde_yaml::from_str(&text).map_err(|e| PolicyError::Parse {
            path: path.clone(),
            message: e.to_string(),
        })?;
        let registry = Self::from_file(file, path)?;
        Ok(Arc::new(registry))
    }

    pub fn from_file(file: TenantFile, path: PathBuf) -> Result<Self, PolicyError> {
        let mut by_id: HashMap<String, Arc<Tenant>> = HashMap::with_capacity(file.tenants.len());
        let mut key_records = Vec::new();
        for tenant in file.tenants {
            if by_id.contains_key(&tenant.tenant_id) {
                return Err(PolicyError::DuplicateTenant(tenant.tenant_id));
            }
            if tenant.credentials.is_empty() {
                return Err(PolicyError::TenantHasNoKeys {
                    tenant: tenant.tenant_id,
                });
            }
            for cred in &tenant.credentials {
                key_records.push(cred.to_record(&tenant.tenant_id)?);
            }
            by_id.insert(tenant.tenant_id.clone(), Arc::new(tenant));
        }
        Ok(Self { by_id, key_records, path })
    }

    pub fn get(&self, tenant_id: &str) -> Option<Arc<Tenant>> {
        self.by_id.get(tenant_id).cloned()
    }

    pub fn tenant_ids(&self) -> Vec<&str> {
        let mut ids: Vec<&str> = self.by_id.keys().map(String::as_str).collect();
        ids.sort_unstable();
        ids
    }

    pub fn key_records(&self) -> Vec<KeyRecord> {
        self.key_records.clone()
    }

pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum AuthorizeError {
    #[error("tenant `{0}` is not provisioned on this gateway")]
    UnknownTenant(String),
    #[error("tenant `{tenant}` is disabled")]
    TenantDisabled { tenant: String },
    #[error("credential `{key_id}` is not authorised for tenant `{tenant}`")]
    TenantMismatch { tenant: String, key_id: String },
    #[error("this credential lacks the `{required}` scope")]
    MissingScope { required: String },
    #[error("model `{model}` is not in the allowed set for tenant `{tenant}`")]
    ModelNotAllowed { tenant: String, model: String },
    #[error("model `{model}` is explicitly denied for tenant `{tenant}`")]
    ModelDenied { tenant: String, model: String },
    #[error("request has {actual} messages, tenant `{tenant}` allows at most {limit}")]
    TooManyMessages {
        tenant: String,
        actual: usize,
        limit: usize,
    },
    #[error("prompt is {actual} characters, tenant `{tenant}` allows at most {limit}")]
    PromptTooLarge {
        tenant: String,
        actual: usize,
        limit: usize,
    },
}

/// Outcome of the pre-flight governance pass. Carries everything the limits
/// engine and the provider need, so the hot path does not re-resolve anything.
#[derive(Debug, Clone)]
pub struct Decision {
    pub tenant: Arc<Tenant>,
    pub model_id: String,
    /// `model_id` with the tenant's suffix applied. This is the string the
    /// registry actually resolves against.
    pub qualified_model: String,
    pub limits: LimitProfile,
}

impl Decision {
    pub fn tenant_id(&self) -> &str {
        &self.tenant.tenant_id
    }
}

pub struct PolicyEngine {
    require_allowlist: bool,
    pub(crate) default_limits: LimitProfile,
}

impl PolicyEngine {
    pub fn new(require_allowlist: bool, default_limits: LimitProfile) -> Self {
        Self { require_allowlist, default_limits }
    }

    /// `authorise(tenant, model)` + `check_limits(tenant, model, tokens)` in one
    /// pass, per the gateway's governance contract. Returns the effective
    /// limits so they are computed exactly once per request.
    #[allow(clippy::too_many_arguments)]
    pub fn authorize(
        &self,
        principal: &super::auth::Principal,
        registry: &TenantRegistry,
        requested_model: &str,
        message_count: usize,
        prompt_chars: usize,
    ) -> Result<Decision, AuthorizeError> {
        let tenant = registry.get(&principal.tenant_id).ok_or_else(|| {
            AuthorizeError::UnknownTenant(principal.tenant_id.to_string())
        })?;

        if !tenant.is_enabled() {
            return Err(AuthorizeError::TenantDisabled {
                tenant: tenant.tenant_id.clone(),
            });
        }
        if !principal.has_scope(SCOPE_CHAT_STREAM) {
            return Err(AuthorizeError::MissingScope {
                required: SCOPE_CHAT_STREAM.to_string(),
            });
        }
        if !principal.key_id.as_ref().is_empty()
            && !tenant
                .credentials
                .iter()
                .any(|c| c.key_id.as_str() == &*principal.key_id && c.enabled)
        {
            return Err(AuthorizeError::TenantMismatch {
                tenant: tenant.tenant_id.clone(),
                key_id: principal.key_id.to_string(),
            });
        }

        let qualified = qualify(&tenant.model_suffix, requested_model);
        check_model_access(&tenant, &qualified, self.require_allowlist)?;

        let limits = tenant
            .limits
            .clone()
            .unwrap_or_default()
            .merged_with_defaults(&self.default_limits);

        // `0` means "no ceiling" consistently, both for tenant overrides and for
        // the gateway defaults — otherwise a tenant that inherits an unset
        // default would be rejected for having a single message.
        if limits.max_messages > 0 && message_count > limits.max_messages {
            return Err(AuthorizeError::TooManyMessages {
                tenant: tenant.tenant_id.clone(),
                actual: message_count,
                limit: limits.max_messages,
            });
        }
        if limits.max_prompt_chars > 0 && prompt_chars > limits.max_prompt_chars {
            return Err(AuthorizeError::PromptTooLarge {
                tenant: tenant.tenant_id.clone(),
                actual: prompt_chars,
                limit: limits.max_prompt_chars,
            });
        }

        Ok(Decision {
            tenant,
            model_id: requested_model.to_string(),
            qualified_model: qualified,
            limits,
        })
    }
}

/// Apply the tenant's deployment suffix, if any.
pub fn qualify(suffix: &str, model: &str) -> String {
    // A suffix already present is not applied twice, so applying a tenant's
    // suffix to an id that already carries it is idempotent.
    if suffix.is_empty() || model.ends_with(suffix) {
        model.to_string()
    } else {
        format!("{model}{suffix}")
    }
}

fn check_model_access(
    tenant: &Tenant,
    qualified_model: &str,
    require_allowlist: bool,
) -> Result<(), AuthorizeError> {
    if tenant
        .denied_models
        .iter()
        .any(|p| glob_match(p, qualified_model))
    {
        return Err(AuthorizeError::ModelDenied {
            tenant: tenant.tenant_id.clone(),
            model: qualified_model.to_string(),
        });
    }
    if require_allowlist {
        // Fail closed. An empty allowlist means "no models", not "every model":
        // this is the strict mode, and the permissive reading turned a present-
        // but-empty `allowed_models` list into full catalogue access. A tenant
        // that genuinely wants everything writes `*`, which `glob_match`
        // already handles. A *misnamed* key is already rejected outright by
        // `deny_unknown_fields` on `Tenant`.
        let allowed = tenant
            .allowed_models
            .iter()
            .any(|p| glob_match(p, qualified_model));
        if !allowed {
            return Err(AuthorizeError::ModelNotAllowed {
                tenant: tenant.tenant_id.clone(),
                model: qualified_model.to_string(),
            });
        }
    }
    Ok(())
}

/// Minimal glob: `*` matches any run of characters, `?` matches one. Enough for
/// `groq/*` and `nvidia/nemotron-4-*` without pulling a regex engine onto the
/// request path.
pub fn glob_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    // Iterative backtracking: linear in the common case, no recursion depth
    // issues on adversarial patterns.
    let (mut pi, mut vi) = (0usize, 0usize);
    let (mut star, mut star_vi) = (usize::MAX, 0usize);
    while vi < v.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == v[vi]) {
            pi += 1;
            vi += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = pi;
            star_vi = vi;
            pi += 1;
        } else if star != usize::MAX {
            pi = star + 1;
            star_vi += 1;
            vi = star_vi;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// True if `value` matches any pattern in `patterns`. An empty pattern list
/// means "everything" for allowlists and "nothing" for denylists; the caller
/// decides which, because the two have opposite defaults.
pub fn glob_any(patterns: &[String], value: &str) -> bool {
    patterns.iter().any(|p| glob_match(p, value))
}

/// Outcome of registry cross-validation.
#[derive(Debug, Default)]
pub struct Validation {
    /// Fatal: a tenant is configured in a way that can never work.
    pub errors: Vec<String>,
    /// Worth knowing at boot, but not fatal: a tenant may be provisioned ahead
    /// of the credential for one of its providers.
    pub warnings: Vec<String>,
}

/// Cross-check the tenant registry against the live provider set.
///
/// An allowlist naming a provider with no adapter is a *warning*, not an
/// error: tenants are routinely provisioned before credentials land, and
/// refusing to boot would turn a missing secret into an outage. A
/// `default_model` that is not in the tenant's own allowlist *is* fatal, since
/// nothing about that deployment can ever succeed. So is a tenant with no
/// allowlist at all while `require_allowlist` is on, for the same reason.
pub fn validate(
    tenants: &TenantRegistry,
    require_allowlist: bool,
    provider_available: &dyn Fn(&str) -> bool,
) -> Validation {
    let mut v = Validation::default();
    for id in tenants.tenant_ids() {
        let Some(tenant) = tenants.get(id) else { continue };

        for pattern in &tenant.allowed_models {
            if let Some(provider) = pattern.split('/').next()
                && provider != "*"
                && !provider_available(provider)
            {
                v.warnings.push(format!(
                    "tenant `{id}` allowlist references provider `{provider}`, which has no configured adapter; those models will fail until a credential is set"
                ));
            }
        }

        // Fail closed, so this is now a load-time error rather than a request
        // that silently succeeds. Caught here because the request-time effect
        // is a 403 on every model, which looks like a policy problem rather
        // than a missing config key.
        if require_allowlist && tenant.allowed_models.is_empty() {
            v.errors.push(format!(
                "tenant `{id}` has no `allowed_models` while `require_model_allowlist` is on, so every model request will be refused; list the models it may call, or use `[\"*\"]` if unrestricted access is intended"
            ));
        }

        if let Some(model) = &tenant.default_model {
            let qualified = qualify(&tenant.model_suffix, model);
            // Only meaningful when an allowlist is in force for this tenant:
            // either the deployment requires one, or this tenant supplied one.
            // With `require_allowlist` off and no list, there is nothing to be
            // outside of.
            if (require_allowlist || !tenant.allowed_models.is_empty())
                && !glob_any(&tenant.allowed_models, &qualified)
            {
                v.errors.push(format!(
                    "tenant `{id}` default_model `{model}` is not in its own allowlist"
                ));
            }
            if glob_any(&tenant.denied_models, &qualified) {
                v.errors.push(format!(
                    "tenant `{id}` default_model `{model}` is in its own deny list"
                ));
            }
            // A default model whose provider has no credential is a warning,
            // not a fatal error. Turning a missing secret into a failed boot
            // means a routine credential rotation takes the whole gateway down;
            // the model simply errors until the key is set, and the readiness
            // probe reports it.
            let provider = model.split('/').next().unwrap_or_default();
            if provider != "*" && !provider_available(provider) {
                v.warnings.push(format!(
                    "tenant `{id}` default_model `{model}` needs provider `{provider}`, which has no configured adapter; it will error until a credential is set"
                ));
            }
        }
    }
    v
}

pub fn default_tenant_path(base: &Path) -> PathBuf {
    base.join("tenants.yaml")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::auth::Principal;
    use std::sync::Arc as StdArc;

    fn tenant(allowed: &[&str], denied: &[&str]) -> Tenant {
        Tenant {
            tenant_id: "acme".into(),
            display_name: None,
            model_suffix: String::new(),
            enabled: true,
            credentials: vec![Credential {
                key_id: "ak_1".into(),
                key: Some("k".into()),
                key_env: None,
                scopes: vec![SCOPE_CHAT_STREAM.into()],
                enabled: true,
            }],
            allowed_models: allowed.iter().map(|s| (*s).to_string()).collect(),
            denied_models: denied.iter().map(|s| (*s).to_string()).collect(),
            default_model: None,
            limits: None,
            model_params: HashMap::new(),
        }
    }

    fn registry(t: Tenant) -> TenantRegistry {
        let mut by_id = HashMap::new();
        by_id.insert(t.tenant_id.clone(), StdArc::new(t));
        TenantRegistry { by_id, key_records: vec![], path: PathBuf::from("t.yaml") }
    }

    fn principal() -> Principal {
        Principal {
            tenant_id: "acme".into(),
            key_id: "ak_1".into(),
            scopes: StdArc::new(vec![Scope::exact(SCOPE_CHAT_STREAM)]),
            scheme: "api_key".into(),
        }
    }

    fn engine() -> PolicyEngine {
        // Real defaults, not `LimitProfile::default()`: the latter is all zeros,
        // which `merged_with_defaults` reads as "inherit", and the engine's own
        // defaults are what a tenant inherits from.
        PolicyEngine::new(true, crate::config::DEFAULT_LIMITS)
    }


    #[test]
    fn glob_star_matches_provider_wide() {
        assert!(glob_match("groq/*", "groq/llama-3.1-70b-versatile"));
        assert!(!glob_match("groq/*", "nvidia/nemotron"));
        assert!(glob_match("*", "anything/at-all"));
        assert!(glob_match("nvidia/nemotron-4-*", "nvidia/nemotron-4-340b"));
    }

    #[test]
    fn glob_backtracks_correctly() {
        assert!(glob_match("a*b*c", "axxbyyc"));
        assert!(!glob_match("a*b*c", "axxbyy"));
        assert!(glob_match("groq/**", "groq/a/b"));
    }

    #[test]
    fn allowed_prefix_passes() {
        let d = engine()
            .authorize(&principal(), &registry(tenant(&["groq/*"], &[])), "groq/llama", 1, 10)
            .unwrap();
        assert_eq!(d.qualified_model, "groq/llama");
    }

    #[test]
    fn model_outside_allowlist_is_rejected() {
        let err = engine()
            .authorize(
                &principal(),
                &registry(tenant(&["groq/*"], &[])),
                "nvidia/nemotron",
                1,
                10,
            )
            .unwrap_err();
        assert!(matches!(err, AuthorizeError::ModelNotAllowed { .. }));
    }

    #[test]
    fn deny_list_beats_allow_list() {
        let err = engine()
            .authorize(
                &principal(),
                &registry(tenant(&["groq/*"], &["groq/secret-mixtral"])),
                "groq/secret-mixtral",
                1,
                10,
            )
            .unwrap_err();
        assert!(matches!(err, AuthorizeError::ModelDenied { .. }));
    }

    /// The strict mode is the one that must be strict. An empty allowlist used
    /// to short-circuit to "allowed", which handed a tenant with a missing or
    /// empty `allowed_models` the whole catalogue -- including the most
    /// expensive model in the registry. Fail closed instead.
    #[test]
    fn an_empty_allowlist_allows_nothing_when_required() {
        for model in [
            "groq/gpt-oss-20b",
            "openrouter/gpt-4.1-mini",
            "openrouter/claude-sonnet-4",
        ] {
            let err = engine()
                .authorize(&principal(), &registry(tenant(&[], &[])), model, 1, 10)
                .unwrap_err();
            assert!(
                matches!(err, AuthorizeError::ModelNotAllowed { .. }),
                "empty allowlist must refuse `{model}`, got {err:?}"
            );
        }
    }

    /// A tenant that genuinely wants every model still says so with `*`, which
    /// is why the empty case above can be closed without breaking anyone.
    #[test]
    fn a_star_allowlist_still_allows_everything() {
        for model in ["groq/gpt-oss-20b", "openrouter/claude-sonnet-4"] {
            engine()
                .authorize(&principal(), &registry(tenant(&["*"], &[])), model, 1, 10)
                .unwrap_or_else(|e| panic!("`*` must allow `{model}`, got {e:?}"));
        }
    }

    /// With the allowlist not required, an empty list is not an error: the
    /// deployment has opted out of allowlist enforcement entirely.
    #[test]
    fn an_empty_allowlist_is_permissive_when_not_required() {
        let engine = PolicyEngine::new(false, crate::config::DEFAULT_LIMITS);
        engine
            .authorize(&principal(), &registry(tenant(&[], &[])), "groq/anything", 1, 10)
            .expect("allowlist enforcement is off, so the empty list must not gate");
    }

    /// The request-time gate fails closed, so the empty case is also a
    /// load-time error: otherwise an operator sees a 403 on every model and has
    /// no reason to suspect a missing config key.
    #[test]
    fn validation_reports_an_empty_allowlist_as_fatal() {
        let reg = registry(tenant(&[], &[]));
        let v = validate(&reg, true, &|_| true);
        assert!(
            v.errors.iter().any(|e| e.contains("no `allowed_models`")),
            "expected a fatal error about the empty allowlist, got {:?}",
            v.errors
        );
        // Not fatal when allowlist enforcement is off, since it then has no
        // effect on the request path.
        let v = validate(&reg, false, &|_| true);
        assert!(
            !v.errors.iter().any(|e| e.contains("no `allowed_models`")),
            "must not be fatal when enforcement is off, got {:?}",
            v.errors
        );
    }

    /// The shipped roster must survive the stricter reading, and every tenant
    /// in it must have a real allowlist or a deliberate `*`.
    #[test]
    fn the_shipped_tenant_roster_validates() {
        // The loader hashes credential material from `key_env` vars at load
        // time, like boot does. The CI/test environment has none of them set.
        for var in [
            "GATEWAY_KEY_ACME_MAIN",
            "GATEWAY_KEY_ACME_CI",
            "GATEWAY_KEY_JARVIS_MAIN",
            "GATEWAY_KEY_SANDBOX",
        ] {
            // Rust 2024 edition: process env mutation is `unsafe`. This is a
            // single-threaded unit test, so it is sound here.
            unsafe { std::env::set_var(var, "test-material-not-a-real-key") };
        }
        let reg =
            TenantRegistry::load("config/tenants.yaml").unwrap_or_else(|e| panic!("{e:?}"));
        let v = validate(&reg, true, &|_| true);
        assert!(
            v.errors.is_empty(),
            "config/tenants.yaml must not fail strict validation: {:?}",
            v.errors
        );
        for id in reg.tenant_ids() {
            let t = reg.get(id).expect("tenant exists");
            assert!(
                !t.allowed_models.is_empty(),
                "shipped tenant `{id}` has no allowed_models, so under strict mode it can call nothing"
            );
        }
    }


    #[test]
    fn missing_scope_is_rejected() {
        let mut p = principal();
        p.scopes = StdArc::new(vec![Scope::exact("models:read")]);
        let err = engine()
            .authorize(&p, &registry(tenant(&["*"], &[])), "groq/llama", 1, 10)
            .unwrap_err();
        assert!(matches!(err, AuthorizeError::MissingScope { .. }));
    }

    #[test]
    fn prompt_size_ceiling_is_enforced() {
        let mut t = tenant(&["*"], &[]);
        t.limits = Some(LimitProfile {
            max_prompt_chars: 50,
            ..Default::default()
        });
        let err = engine()
            .authorize(&principal(), &registry(t), "groq/llama", 1, 5_000)
            .unwrap_err();
        assert!(matches!(err, AuthorizeError::PromptTooLarge { .. }));
    }

    #[test]
    fn model_suffix_is_applied_once() {
        let mut t = tenant(&["groq/llama*"], &[]);
        t.model_suffix = ":prod".into();
        let d = engine()
            .authorize(&principal(), &registry(t), "groq/llama", 1, 10)
            .unwrap();
        assert_eq!(d.qualified_model, "groq/llama:prod");
        let d2 = engine()
            .authorize(&principal(), &registry(tenant(&["*"], &[])), "groq/llama:prod", 1, 10)
            .unwrap();
        assert_eq!(d2.qualified_model, "groq/llama:prod");
    }

    #[test]
    fn unknown_tenant_is_rejected() {
        let mut p = principal();
        p.tenant_id = "ghost".into();
        let err = engine()
            .authorize(&p, &registry(tenant(&["*"], &[])), "groq/llama", 1, 10)
            .unwrap_err();
        assert!(matches!(err, AuthorizeError::UnknownTenant(_)));
    }

    #[test]
    fn key_from_another_tenant_is_rejected() {
        let mut p = principal();
        p.key_id = "ak_other".into();
        let err = engine()
            .authorize(&p, &registry(tenant(&["*"], &[])), "groq/llama", 1, 10)
            .unwrap_err();
        assert!(matches!(err, AuthorizeError::TenantMismatch { .. }));
    }

    #[test]
    fn message_count_ceiling_is_enforced() {
        let mut t = tenant(&["*"], &[]);
        t.limits = Some(LimitProfile {
            max_messages: 2,
            ..Default::default()
        });
        let err = engine()
            .authorize(&principal(), &registry(t), "groq/llama", 9, 10)
            .unwrap_err();
        assert!(matches!(err, AuthorizeError::TooManyMessages { .. }));
    }
}
