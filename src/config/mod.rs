//! Layered configuration: `default.toml` on disk, overridden by
//! `LLM_GATEWAY__NESTED__KEY` environment variables.
//!
//! Precedence (low -> high): built-in defaults -> config file -> environment.
//! Nothing about the *model catalogue* lives here; that is a separate, hot
//! reloadable file (see [`crate::router::ModelRegistry`]) so operators can add
//! providers without a restart.

use std::{
    collections::BTreeMap,
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};

/// Env var that points at the directory holding `default.toml`, `models.yaml`
/// and `tenants.yaml`. Relative paths elsewhere are resolved against it.
pub const ENV_CONFIG_DIR: &str = "LLM_GATEWAY_CONFIG_DIR";
/// Env var holding the full path of the main config file (wins over
/// `<config dir>/default.toml`).
pub const ENV_CONFIG_FILE: &str = "LLM_GATEWAY_CONFIG";
/// Common prefix for environment overrides.
pub const ENV_PREFIX: &str = "LLM_GATEWAY";

/// Top-level configuration. Every section implements `Default`, so this does
/// too, and `Settings::default()` is a complete working configuration.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Settings {
    pub server: ServerConfig,
    pub registry: RegistryConfig,
    pub auth: AuthConfig,
    pub governance: GovernanceConfig,
    pub upstream: UpstreamConfig,
    pub telemetry: TelemetryConfig,
    /// Absent in older config files, which get the in-memory ledger.
    #[serde(default)]
    pub ledger: LedgerConfig,
}

/// Where spend and reservations live. See `governance::ledger`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LedgerBackend {
    /// Process memory: exact, but a restart forgets it and replicas do not
    /// share it. Right for a single instance.
    #[default]
    Memory,
    /// A shared, durable Postgres ledger: survives restarts, and every
    /// replica draws on one budget.
    Postgres,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LedgerConfig {
    pub backend: LedgerBackend,
    /// Name of the env var holding the database URL. Never the URL itself.
    pub url_env: String,
    pub schema: String,
    pub max_connections: u32,
    /// Bound on each admission's ledger round trip. When it is exceeded, or
    /// the database is down, requests are refused with 503: there is no
    /// setting that admits without the ledger, because that fails open.
    pub timeout_ms: u64,
    /// An `open` reservation older than this is treated as orphaned by a
    /// crash and billed in full. Keep it well above the longest request.
    pub sweep_after_secs: u64,
    pub sweep_interval_secs: u64,
    /// How long a completed request's Idempotency-Key is remembered.
    pub idempotency_retention_secs: u64,
    /// Name of the env var holding the keys that seal answers kept for
    /// replay: `kid:base64key[,kid:base64key...]`, first key seals. Unset
    /// means answers are not stored, so completions are not replayed.
    pub response_keys_env: String,
}

impl Default for LedgerConfig {
    fn default() -> Self {
        Self {
            backend: LedgerBackend::Memory,
            url_env: "LLM_GATEWAY_DATABASE_URL".to_string(),
            schema: "public".to_string(),
            max_connections: 16,
            timeout_ms: 1_000,
            sweep_after_secs: 3_600,
            sweep_interval_secs: 60,
            idempotency_retention_secs: 86_400,
            response_keys_env: "LLM_GATEWAY_RESPONSE_KEYS".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub port: u16,
    /// Hard cap on request body size for the chat endpoints.
    pub request_body_limit_bytes: usize,
    /// SSE comment ping cadence. Keeps intermediaries from buffering.
    pub keep_alive_interval_ms: u64,
    /// Global ceiling across all tenants, independent of per-tenant limits.
    pub max_concurrent_streams_global: usize,
    pub shutdown_grace_ms: u64,
    /// Serve `GET /metrics`.
    pub metrics_enabled: bool,
    /// Separate listener for the operator surface. When set, `/metrics` and
    /// `/v1/admin/*` are served only here, unauthenticated for `/metrics`, and
    /// removed from the public port; bind it to an interface tenants cannot
    /// reach. When unset, both stay on the public port and `/metrics`
    /// requires the `admin` scope, because it names every tenant's spend.
    #[serde(default)]
    pub ops_port: Option<u16>,
    /// Interface for `ops_port`. Loopback by default, so turning the ops
    /// listener on never exposes it by accident.
    #[serde(default = "default_ops_bind_addr")]
    pub ops_bind_addr: String,
}

fn default_ops_bind_addr() -> String {
    "127.0.0.1".to_string()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0".to_string(),
            port: 8080,
            request_body_limit_bytes: 1 << 20,
            keep_alive_interval_ms: 15_000,
            max_concurrent_streams_global: 512,
            shutdown_grace_ms: 20_000,
            metrics_enabled: true,
            ops_port: None,
            ops_bind_addr: default_ops_bind_addr(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryConfig {
    pub models_path: PathBuf,
    pub tenants_path: PathBuf,
    /// Re-read the registry when its mtime changes. Keeps the data plane alive
    /// while the control plane edits YAML.
    pub hot_reload: bool,
    pub reload_interval_ms: u64,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            models_path: PathBuf::from("config/models.yaml"),
            tenants_path: PathBuf::from("config/tenants.yaml"),
            hot_reload: true,
            reload_interval_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    /// `Authorization: Bearer <key>` and/or `X-API-Key: <key>`.
    #[default]
    ApiKey,
    /// API keys *or* a signed HS256 JWT. See [`JwtConfig`].
    ApiKeyOrJwt,
    /// No authentication. Refused at startup unless explicitly allowed.
    Disabled,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    pub mode: AuthMode,
    pub api_key_header: String,
    /// Accept `Authorization: Bearer <key>` in addition to `api_key_header`.
    pub accept_bearer: bool,
    /// Escape hatch for local development only. Refused when `mode` is not
    /// `Disabled` *and* real keys are configured.
    pub allow_anonymous: bool,
    #[serde(default)]
    pub jwt: Option<JwtConfig>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::ApiKey,
            api_key_header: "x-api-key".to_string(),
            accept_bearer: true,
            allow_anonymous: false,
            jwt: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JwtConfig {
    pub issuer: String,
    pub audience: String,
    /// Name of the env var holding the HS256 secret. Never the secret itself.
    pub secret_env: String,
    #[serde(default = "default_jwt_leeway")]
    pub leeway_secs: u64,
    /// Scopes a token must carry (all of them) to stream.
    pub required_scopes: Vec<String>,
}

fn default_jwt_leeway() -> u64 {
    30
}

impl JwtConfig {
    pub fn secret(&self) -> Result<String, ConfigError> {
        env::var(&self.secret_env).map_err(|_| {
            ConfigError::MissingSecret {
                env_var: self.secret_env.clone(),
            }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OverflowMode {
    /// Silently reduce `max_tokens` to the permitted ceiling.
    #[default]
    Clamp,
    /// Reject the request with 400.
    Reject,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernanceConfig {
    pub require_model_allowlist: bool,
    /// What to do when a request asks for more output tokens than allowed.
    pub on_max_tokens_exceeded: OverflowMode,
    /// Count spend against the tenant's daily budget.
    pub cost_tracking_enabled: bool,
    /// Applied when a tenant omits a limit. A `0` field here is *not* a
    /// ceiling — it means "inherit whatever the tenant set", which is why
    /// [`LimitProfile::default`] (all zeros) is deliberately different from
    /// this config's default.
    #[serde(default = "default_limits")]
    pub default_limits: LimitProfile,
}

fn default_limits() -> LimitProfile {
    DEFAULT_LIMITS
}

impl Default for GovernanceConfig {
    fn default() -> Self {
        Self {
            require_model_allowlist: true,
            on_max_tokens_exceeded: OverflowMode::Clamp,
            cost_tracking_enabled: true,
            default_limits: DEFAULT_LIMITS,
        }
    }
}

/// Per-tenant ceilings. `0` / unset means "unlimited" for numeric limits.
/// All fields optional so a tenant can override just the ceiling it cares
/// about; `0` means "inherit the gateway default" (see [`LimitProfile::merged_with_defaults`]).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LimitProfile {
    #[serde(default)]
    pub requests_per_minute: u32,
    #[serde(default)]
    pub tokens_per_minute: u32,
    #[serde(default)]
    pub max_concurrent_streams: u32,
    #[serde(default)]
    pub max_output_tokens: u32,
    /// Daily spend ceiling in nano-USD (1 USD = 1_000_000_000).
    #[serde(default)]
    pub daily_budget_nano_usd: u64,
    #[serde(default)]
    pub max_messages: usize,
    #[serde(default)]
    pub max_prompt_chars: usize,
}

/// Gateway-wide defaults, used when a tenant omits a limit entirely.
pub const DEFAULT_LIMITS: LimitProfile = LimitProfile {
    requests_per_minute: 120,
    tokens_per_minute: 200_000,
    max_concurrent_streams: 16,
    max_output_tokens: 8_192,
    daily_budget_nano_usd: 5_000_000_000,
    max_messages: 512,
    max_prompt_chars: 200_000,
};

impl LimitProfile {
    /// Tenant overrides are sparse: a missing key means "inherit the default".
    pub fn merged_with_defaults(&self, defaults: &LimitProfile) -> LimitProfile {
        macro_rules! pick {
            ($field:ident) => {
                match self.$field {
                    0 => defaults.$field,
                    v => v,
                }
            };
        }
        LimitProfile {
            requests_per_minute: pick!(requests_per_minute),
            tokens_per_minute: pick!(tokens_per_minute),
            max_concurrent_streams: pick!(max_concurrent_streams),
            max_output_tokens: pick!(max_output_tokens),
            daily_budget_nano_usd: pick!(daily_budget_nano_usd),
            max_messages: pick!(max_messages),
            max_prompt_chars: pick!(max_prompt_chars),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamConfig {
    pub connect_timeout_ms: u64,
    /// Per-chunk read timeout for streaming responses.
    pub stream_read_timeout_ms: u64,
    pub tcp_keepalive_secs: u64,
    pub pool_max_idle_per_host: usize,
    pub pool_idle_timeout_ms: u64,
    pub user_agent: String,
    /// Send `stream_options.include_usage` so we can bill accurately.
    pub request_usage: bool,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 5_000,
            stream_read_timeout_ms: 120_000,
            tcp_keepalive_secs: 75,
            pool_max_idle_per_host: 64,
            pool_idle_timeout_ms: 90_000,
            user_agent: concat!("llm-gateway/", env!("CARGO_PKG_VERSION")).to_string(),
            request_usage: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    #[default]
    Pretty,
    Json,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    pub service_name: String,
    pub service_version: String,
    pub log_format: LogFormat,
    /// `RUST_LOG`-style filter applied when no env filter is present.
    pub log_filter: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            service_name: "llm-gateway".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            log_format: LogFormat::Json,
            log_filter: "info,llm_gateway=debug".to_string(),
        }
    }
}

impl Settings {
    /// Load from the default discovery order.
    pub fn load() -> Result<Self, ConfigError> {
        let dir = config_dir();
        let file = env::var(ENV_CONFIG_FILE)
            .map(PathBuf::from)
            .unwrap_or_else(|_| dir.join("default.toml"));
        Self::load_file(&file, &dir)
    }

    /// Load an explicit file. Relative `models_path` / `tenants_path` are
    /// resolved against `base_dir` (normally the config directory) so the
    /// binary can be started from any working directory.
    pub fn load_file(path: &Path, base_dir: &Path) -> Result<Self, ConfigError> {
        let file = config::File::from(path.to_path_buf()).required(true);
        Self::from_sources(file, base_dir)
    }

    /// Load purely from environment variables (no config file). Used by the
    /// integration tests and by container entrypoints that pass everything as
    /// env.
    pub fn load_from_env(base_dir: impl AsRef<Path>) -> Result<Self, ConfigError> {
        // An optional source, so a missing file is not an error; the struct's
        // own `Default`s and the env layer supply the values.
        let file = config::File::from(Path::new("/nonexistent/llm-gateway.toml")).required(false);
        Self::from_sources(file, base_dir.as_ref())
    }

    fn from_sources(
        file: config::File<config::FileSourceFile, config::FileFormat>,
        base_dir: &Path,
    ) -> Result<Self, ConfigError> {
        let env_source = config::Environment::with_prefix(ENV_PREFIX)
            .prefix_separator("__")
            .separator("__")
            .try_parsing(true);

        let settings: Settings = config::Config::builder()
            .add_source(file)
            .add_source(env_source)
            .build()
            .map_err(|e| ConfigError::Parse(e.to_string()))?
            .try_deserialize()
            .map_err(|e| ConfigError::Parse(e.to_string()))?;

        Ok(settings.with_resolved_paths(base_dir))
    }

    fn with_resolved_paths(mut self, base_dir: &Path) -> Self {
        if self.registry.models_path.is_relative() {
            self.registry.models_path = base_dir.join(&self.registry.models_path);
        }
        if self.registry.tenants_path.is_relative() {
            self.registry.tenants_path = base_dir.join(&self.registry.tenants_path);
        }
        self
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, ConfigError> {
        let addr = format!("{}:{}", self.server.bind_addr, self.server.port);
        addr.parse()
            .map_err(|e| ConfigError::Parse(format!("invalid bind address {addr}: {e}")))
    }

    /// The ops listener's address, when one is configured.
    pub fn ops_socket_addr(&self) -> Result<Option<SocketAddr>, ConfigError> {
        let Some(port) = self.server.ops_port else {
            return Ok(None);
        };
        let addr = format!("{}:{}", self.server.ops_bind_addr, port);
        addr.parse()
            .map(Some)
            .map_err(|e| ConfigError::Parse(format!("invalid ops bind address {addr}: {e}")))
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.upstream.connect_timeout_ms)
    }

    pub fn stream_read_timeout(&self) -> Duration {
        Duration::from_millis(self.upstream.stream_read_timeout_ms)
    }

    pub fn keep_alive_interval(&self) -> Duration {
        Duration::from_millis(self.server.keep_alive_interval_ms)
    }

    pub fn shutdown_grace(&self) -> Duration {
        Duration::from_millis(self.server.shutdown_grace_ms)
    }

    pub fn reload_interval(&self) -> Duration {
        Duration::from_millis(self.registry.reload_interval_ms)
    }
}

pub fn config_dir() -> PathBuf {
    if let Ok(dir) = env::var(ENV_CONFIG_DIR) {
        return PathBuf::from(dir);
    }
    if let Ok(file) = env::var(ENV_CONFIG_FILE)
        && let Some(parent) = Path::new(&file).parent()
        && !parent.as_os_str().is_empty()
    {
        return parent.to_path_buf();
    }
    PathBuf::from("config")
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to parse configuration: {0}")]
    Parse(String),
    #[error("required secret environment variable `{env_var}` is not set")]
    MissingSecret { env_var: String },
}

/// Serialise the effective settings for the startup log. Secrets are
/// referenced by env var name only, never inlined, so this is safe to print.
pub fn redacted_settings(settings: &Settings) -> BTreeMap<&'static str, String> {
    let mut out = BTreeMap::new();
    out.insert("bind", format!("{}:{}", settings.server.bind_addr, settings.server.port));
    out.insert("models_path", settings.registry.models_path.display().to_string());
    out.insert("tenants_path", settings.registry.tenants_path.display().to_string());
    out.insert("auth_mode", format!("{:?}", settings.auth.mode));
    out.insert("jwt_issuer", settings.auth.jwt.as_ref().map(|j| j.issuer.clone()).unwrap_or_else(|| "-".into()));
    out.insert("metrics_enabled", settings.server.metrics_enabled.to_string());
    out.insert("ledger", format!("{:?}", settings.ledger.backend).to_ascii_lowercase());
    out.insert(
        "ops_bind",
        settings
            .server
            .ops_port
            .map(|p| format!("{}:{p}", settings.server.ops_bind_addr))
            .unwrap_or_else(|| "-".into()),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// Walking `cargo test --lib` from the crate root, so the shipped file is
    /// reachable the same way the binary reaches it when started from the repo
    /// root. With every config struct `deny_unknown_fields`, this is the
    /// regression guard for the dead-key removal: a stale key revived in
    /// `default.toml` makes boot fail loudly instead of being silently ignored.
    #[test]
    fn the_shipped_default_config_loads() {
        Settings::load_file(
            Path::new("config/default.toml"),
            Path::new("config"),
        )
        .unwrap_or_else(|e| panic!("shipped config/default.toml must parse: {e:?}"));
    }

    /// A config file that still carries the removed knobs is now a hard parse
    /// error. Grabs each section's required fields so the failure can only
    /// come from the stale key in `[server]`.
    #[test]
    fn removed_keys_are_rejected_not_ignored() {
        let toml = r#"
[server]
bind_addr = "0.0.0.0"
port = 8080
request_body_limit_bytes = 1048576
keep_alive_interval_ms = 15000
max_concurrent_streams_global = 512
shutdown_grace_ms = 20000
metrics_enabled = true
request_timeout_ms = 30000

[registry]
models_path = "models.yaml"
tenants_path = "tenants.yaml"
hot_reload = false
reload_interval_ms = 5000

[auth]
mode = "api_key"
api_key_header = "x-api-key"
accept_bearer = true
allow_anonymous = false

[governance]
require_model_allowlist = true
on_max_tokens_exceeded = "clamp"
cost_tracking_enabled = true

[upstream]
connect_timeout_ms = 5000
stream_read_timeout_ms = 120000
tcp_keepalive_secs = 75
pool_max_idle_per_host = 64
pool_idle_timeout_ms = 90000
user_agent = "test/0.0.0"
request_usage = true

[telemetry]
service_name = "test"
service_version = "0.0.0"
log_format = "json"
log_filter = "info"
"#;
        let dir = std::env::temp_dir().join("llm-gateway-config-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stale.toml");
        std::fs::write(&path, toml).unwrap();
        let err = Settings::load_file(&path, &dir).unwrap_err();
        let _ = std::fs::remove_dir_all(&dir);
        let rendered = err.to_string();
        assert!(
            rendered.contains("request_timeout_ms"),
            "expected the stale key to be named in the error, got: {rendered}"
        );
    }
}
