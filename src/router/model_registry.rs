//! Versioned model catalogue.
//!
//! The registry is the single source of truth for *which* models exist, what
//! provider serves them, and what they cost. Nothing in the request path
//! hardcodes a model id: `POST /v1/chat/stream` asks the registry to resolve
//! the caller's `model` field into a concrete [`ResolvedModel`].

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::SystemTime,
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::RwLock;

/// Semantic version of the on-disk registry schema. Bump when a breaking
/// change lands in `models.yaml` so operators get a clear error instead of
/// silently ignored fields.
pub const REGISTRY_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("cannot read model registry at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot parse model registry at {path}: {message}")]
    Parse { path: PathBuf, message: String },
    #[error("registry schema v{found} is not supported (this build supports v{expected})")]
    SchemaMismatch { found: u32, expected: u32 },
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("model `{requested}` is not in the registry")]
    NotFound { requested: String },
    #[error("model `{requested}` matches no enabled model (all candidates are disabled)")]
    Disabled { requested: String },
    #[error("model `{requested}` is ambiguous: {candidates:?}")]
    Ambiguous {
        requested: String,
        candidates: Vec<String>,
    },
    #[error("model `{requested}` is registered for provider `{provider}`, but that provider is not compiled in / not configured")]
    UnknownProvider {
        requested: String,
        provider: String,
    },
    #[error("model `{requested}` is missing required field `{field}`")]
    Incomplete { requested: String, field: String },
}

// ---------------------------------------------------------------------------
// File schema
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegistryFile {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub models: HashMap<String, ModelConfig>,
}

fn default_schema_version() -> u32 {
    REGISTRY_SCHEMA_VERSION
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModelConfig {
    /// Keys the provider adapter: `groq` | `openrouter` | `nvidia`.
    pub provider: String,
    /// Overrides the provider's default chat-completions URL. Needed for
    /// self-hosted NVIDIA NIM (`http://gpu-box:8000/v1/chat/completions`).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Name sent upstream. Defaults to the registry key with the
    /// `provider/` prefix stripped.
    #[serde(default)]
    pub upstream_model: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    /// Merged *under* caller-supplied params. See [`crate::router::resolve_params`].
    #[serde(default)]
    pub default_params: Map<String, Value>,
    #[serde(default)]
    pub cost: CostModel,
    /// Prompt tokens the vendor adds around every request: its chat template,
    /// a default system prompt. The gateway cannot see them, so the prompt
    /// estimate alone runs low (a short prompt to Groq's gpt-oss is billed ~79
    /// tokens against ~13 estimated). Added to the *reservation* only, so the
    /// budget check covers what the vendor will count; the bill is still the
    /// vendor's reported usage. The live suite checks these numbers.
    #[serde(default)]
    pub prompt_overhead_tokens: u32,
    /// Time-to-first-token p95 budget in ms. Used for routing preference and
    /// as an SLO signal, not as a hard cutoff.
    #[serde(default)]
    pub latency_slo_ms: Option<u32>,
    #[serde(default)]
    pub max_context_tokens: Option<u32>,
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    /// Extra names this model answers to, e.g. `["llama-70b", "groq-llama"]`.
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CostModel {
    /// USD per 1M prompt tokens.
    pub input_per_mtok_usd: f64,
    /// USD per 1M completion tokens.
    pub output_per_mtok_usd: f64,
    /// USD per 1M cached prompt tokens, when the provider bills them.
    #[serde(default)]
    pub cached_input_per_mtok_usd: Option<f64>,
}

/// Nano-USD per 1M tokens: the scale factor from a `$X` per-1M-tokens rate to
/// an integer nano-USD per-token rate.
///
/// `1 USD = 1e9 nano-USD`, so `$X` per 1M tokens is `X * 1e9 / 1e6 = X * 1e3`
/// nano-USD per token.
const NANO_USD_SCALE: f64 = 1_000.0;

impl CostModel {
    /// Nano-USD per single token.
    ///
    /// The rate is scaled up to nano-USD so that it lands on an integer. The
    /// alternative, nano-USD, does not: real prices are fractional at that
    /// precision, so `$0.075` per 1M tokens is 0.075 nano-USD per token, and
    /// rounding it to an integer would either erase the price (`0.075 -> 0`,
    /// a free model) or inflate it several-fold (`0.59 -> 1`).
    ///
    /// Nano-USD is exact for every rate with up to three decimal places per
    /// 1M tokens, which covers the whole shipped catalogue. A rate needing
    /// more precision is rejected by [`RegistryFile`] validation rather than
    /// silently rounded here.
    ///
    /// Worked example: `input_per_mtok_usd: 0.075` -> 75 nano-USD/token ->
    /// 75,000 nano-USD per 1K tokens.
    pub fn input_nano_usd_per_token(&self) -> u64 {
        nano_usd_per_token(self.input_per_mtok_usd)
    }

    pub fn output_nano_usd_per_token(&self) -> u64 {
        nano_usd_per_token(self.output_per_mtok_usd)
    }

    pub fn cached_input_nano_usd_per_token(&self) -> u64 {
        match self.cached_input_per_mtok_usd {
            Some(v) => nano_usd_per_token(v),
            None => self.input_nano_usd_per_token(),
        }
    }

    pub fn is_free(&self) -> bool {
        self.input_nano_usd_per_token() == 0 && self.output_nano_usd_per_token() == 0
    }
}

/// Convert a `$ per 1M tokens` rate to an exact integer nano-USD per token.
///
/// A rate that is not representable at nano-USD precision (more than three
/// decimal places per 1M tokens) is rounded to the nearest nano-USD, which is
/// a relative error below 1e-9 at any price above a millionth of a dollar.
/// Rejecting it instead would break perfectly reasonable catalogue entries, so
/// rounding is correct here; what is *not* correct is rounding at nano-USD
/// precision, which is what this function replaces.
fn nano_usd_per_token(usd_per_mtok: f64) -> u64 {
    if !usd_per_mtok.is_finite() || usd_per_mtok <= 0.0 {
        return 0;
    }
    let scaled = usd_per_mtok * NANO_USD_SCALE;
    if scaled >= u64::MAX as f64 {
        return u64::MAX;
    }
    scaled.round() as u64
}

impl ModelConfig {
    pub fn upstream_model_name(&self, registry_id: &str) -> String {
        self.upstream_model
            .clone()
            .unwrap_or_else(|| strip_provider_prefix(registry_id))
    }
}

pub fn strip_provider_prefix(id: &str) -> String {
    match id.split_once('/') {
        Some((_, rest)) if !rest.is_empty() => rest.to_string(),
        _ => id.to_string(),
    }
}

// ---------------------------------------------------------------------------
// In-memory registry
// ---------------------------------------------------------------------------

/// Immutable view of the catalogue. Swapped atomically on reload, so readers
/// on the hot path never block and never see a torn state.
#[derive(Debug)]
pub struct RegistrySnapshot {
    pub generation: u64,
    pub loaded_at: SystemTime,
    pub by_id: HashMap<String, Arc<ModelConfig>>,
    alias_to_id: HashMap<String, String>,
    by_provider: HashMap<String, Vec<String>>,
}

impl RegistrySnapshot {
    pub fn empty() -> Self {
        Self {
            generation: 0,
            loaded_at: SystemTime::now(),
            by_id: HashMap::new(),
            alias_to_id: HashMap::new(),
            by_provider: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    pub fn ids(&self) -> Vec<&str> {
        let mut ids: Vec<&str> = self.by_id.keys().map(String::as_str).collect();
        ids.sort_unstable();
        ids
    }

    pub fn provider_counts(&self) -> BTreeMapLike {
        self.by_provider
            .iter()
            .map(|(k, v)| (k.clone(), v.len()))
            .collect()
    }

    /// Three-stage resolution:
    ///  1. exact registry id
    ///  2. registered alias
    ///  3. `provider/*` wildcard — only if it is unambiguous
    pub fn resolve(&self, requested: &str) -> Result<ResolvedModel, ResolveError> {
        if let Some(cfg) = self.by_id.get(requested) {
            return self.build(requested, Arc::clone(cfg));
        }
        if let Some(id) = self.alias_to_id.get(requested) {
            let cfg = self.by_id.get(id).expect("alias target exists");
            return self.build(requested, Arc::clone(cfg));
        }
        if let Some((provider, _)) = requested.split_once('/')
            && provider == "*"
        {
            return Err(ResolveError::NotFound {
                requested: requested.to_string(),
            });
        }
        // A bare provider name (`groq`) or a wildcard (`groq/*`) resolves only
        // when exactly one model for that provider is enabled.
        //
        // A specific name under a provider (`groq/whatever`) that matched
        // neither an id nor an alias is a miss, not an ambiguity: "this matched
        // 2 candidates" for a name nobody registered is actively misleading.
        let provider = provider_of(requested);
        let is_wildcard = match requested.split_once('/') {
            None => true,
            Some((_, model)) => model.is_empty() || model == "*",
        };
        if is_wildcard {
            let scoped: Vec<&String> = self
                .by_provider
                .get(&provider)
                .map(|v| v.iter().collect())
                .unwrap_or_default();
            return match scoped.len() {
                1 => self.build(requested, Arc::clone(&self.by_id[scoped[0]])),
                0 => Err(ResolveError::NotFound {
                    requested: requested.to_string(),
                }),
                _ => Err(ResolveError::Ambiguous {
                    requested: requested.to_string(),
                    candidates: scoped.into_iter().cloned().collect(),
                }),
            };
        }

        Err(ResolveError::NotFound {
            requested: requested.to_string(),
        })
    }

    /// Resolve but require the provider to be an id we know how to drive.
    pub fn resolve_supported(
        &self,
        requested: &str,
        supported: &HashSet<String>,
    ) -> Result<ResolvedModel, ResolveError> {
        let resolved = self.resolve(requested)?;
        if !supported.contains(&resolved.config.provider) {
            return Err(ResolveError::UnknownProvider {
                requested: requested.to_string(),
                provider: resolved.config.provider.clone(),
            });
        }
        Ok(resolved)
    }

    fn build(&self, requested: &str, config: Arc<ModelConfig>) -> Result<ResolvedModel, ResolveError> {
        if !config.enabled {
            return Err(ResolveError::Disabled {
                requested: requested.to_string(),
            });
        }
        // Reverse lookup by pointer identity: the same `Arc<ModelConfig>` may be
        // reachable via an alias, and only the canonical key should be reported
        // as the registry id.
        let registry_id = self
            .by_id
            .iter()
            .find(|(_, c)| Arc::ptr_eq(c, &config))
            .map(|(id, _)| (*id).clone())
            .unwrap_or_else(|| requested.to_string());
        let upstream_model = config.upstream_model_name(&registry_id);
        Ok(ResolvedModel {
            requested: requested.to_string(),
            registry_id,
            upstream_model,
            endpoint: config.endpoint.clone(),
            config,
        })
    }
}

pub type BTreeMapLike = std::collections::BTreeMap<String, usize>;

/// The provider segment of a registry id. A bare `groq` is itself a provider
/// reference, so the whole string is returned when there is no slash — mapping
/// it to an empty string would silently turn a provider shorthand into a miss.
fn provider_of(id: &str) -> String {
    match id.split_once('/') {
        Some((provider, _)) => provider.to_ascii_lowercase(),
        None => id.to_ascii_lowercase(),
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedModel {
    /// What the caller asked for.
    pub requested: String,
    /// Canonical registry key.
    pub registry_id: String,
    /// What we send upstream.
    pub upstream_model: String,
    /// Per-model endpoint override, if any.
    pub endpoint: Option<String>,
    pub config: Arc<ModelConfig>,
}

impl ResolvedModel {
    pub fn provider(&self) -> &str {
        &self.config.provider
    }

    pub fn display_name(&self) -> &str {
        self.config
            .display_name
            .as_deref()
            .unwrap_or(&self.registry_id)
    }
}

pub struct ModelRegistry {
    path: PathBuf,
    hot_reload: bool,
    reload_interval_ms: u64,
    snapshot: RwLock<Arc<RegistrySnapshot>>,
    generation: AtomicU64,
    last_loaded: RwLock<Option<SystemTime>>,
    last_error: RwLock<Option<String>>,
}

impl ModelRegistry {
    pub fn load(path: impl Into<PathBuf>, hot_reload: bool, reload_interval_ms: u64) -> Result<Arc<Self>, RegistryError> {
        let path = path.into();
        let (file, stamp) = read_registry(&path)?;
        let snapshot = build_snapshot(&file, 1, stamp);
        Ok(Arc::new(Self {
            path,
            hot_reload,
            reload_interval_ms,
            snapshot: RwLock::new(Arc::new(snapshot)),
            generation: AtomicU64::new(1),
            last_loaded: RwLock::new(Some(stamp)),
            last_error: RwLock::new(None),
        }))
    }

    /// Lock-free read for callers that only need the current catalogue. This is
    /// the request hot path: one `RwLock` read and one `Arc` clone.
    pub async fn snapshot(&self) -> Arc<RegistrySnapshot> {
        let guard = self.snapshot.read().await;
        Arc::clone(&guard)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn reload_interval(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.reload_interval_ms)
    }

    pub fn hot_reload(&self) -> bool {
        self.hot_reload
    }

    pub async fn last_error(&self) -> Option<String> {
        self.last_error.read().await.clone()
    }

    /// Re-read the file. A failed reload keeps the previous generation serving
    /// traffic and records the error, because losing the catalogue is worse
    /// than serving yesterday's catalogue.
    pub async fn reload(&self) -> Result<u64, RegistryError> {
        let (file, stamp) = read_registry(&self.path)?;
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        let snapshot = build_snapshot(&file, generation, stamp);
        *self.snapshot.write().await = Arc::new(snapshot);
        *self.last_loaded.write().await = Some(stamp);
        *self.last_error.write().await = None;
        Ok(generation)
    }

    /// Cheap poll: reload only when mtime moved. Returns `Some(generation)` if
    /// a reload actually happened.
    pub async fn reload_if_changed(&self) -> Option<u64> {
        if !self.hot_reload {
            return None;
        }
        let stamp = std::fs::metadata(&self.path).and_then(|m| m.modified()).ok()?;
        if *self.last_loaded.read().await == Some(stamp) {
            return None;
        }
        match self.reload().await {
            Ok(generation) => Some(generation),
            Err(err) => {
                let msg = err.to_string();
                tracing::warn!(error = %msg, "model registry reload failed; keeping previous generation");
                *self.last_error.write().await = Some(msg);
                None
            }
        }
    }
}

fn read_registry(path: &Path) -> Result<(RegistryFile, SystemTime), RegistryError> {
    let text = std::fs::read_to_string(path).map_err(|source| RegistryError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let stamp = std::fs::metadata(path)
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::now());
    let file: RegistryFile =
        serde_yaml::from_str(&text).map_err(|e| RegistryError::Parse {
            path: path.to_path_buf(),
            message: e.to_string(),
        })?;
    if file.schema_version != REGISTRY_SCHEMA_VERSION {
        return Err(RegistryError::SchemaMismatch {
            found: file.schema_version,
            expected: REGISTRY_SCHEMA_VERSION,
        });
    }
    if file.models.is_empty() {
        return Err(RegistryError::Parse {
            path: path.to_path_buf(),
            message: "registry contains no models".to_string(),
        });
    }
    Ok((file, stamp))
}

fn build_snapshot(file: &RegistryFile, generation: u64, stamp: SystemTime) -> RegistrySnapshot {
    let mut by_id = HashMap::with_capacity(file.models.len());
    let mut alias_to_id = HashMap::new();
    let mut by_provider: HashMap<String, Vec<String>> = HashMap::new();

    for (id, cfg) in &file.models {
        by_id.insert(id.clone(), Arc::new(cfg.clone()));
        for alias in &cfg.aliases {
            alias_to_id.insert(alias.to_ascii_lowercase(), id.clone());
        }
        by_provider
            .entry(cfg.provider.to_ascii_lowercase())
            .or_default()
            .push(id.clone());
    }
    // Alias of an exact id must never shadow the real thing.
    for id in file.models.keys() {
        alias_to_id.remove(id);
    }
    for ids in by_provider.values_mut() {
        ids.sort();
    }

    RegistrySnapshot {
        generation,
        loaded_at: stamp,
        by_id,
        alias_to_id,
        by_provider,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(provider: &str, aliases: &[&str]) -> ModelConfig {
        ModelConfig {
            provider: provider.into(),
            endpoint: None,
            upstream_model: None,
            display_name: None,
            default_params: Map::new(),
            cost: CostModel::default(),
            prompt_overhead_tokens: 0,
            latency_slo_ms: None,
            max_context_tokens: None,
            max_output_tokens: None,
            aliases: aliases.iter().map(|s| (*s).to_string()).collect(),
            enabled: true,
        }
    }

    fn sample() -> RegistrySnapshot {
        let mut file = RegistryFile {
            schema_version: 1,
            models: HashMap::new(),
        };
        file.models.insert("groq/llama-3.1-70b-versatile".into(), cfg("groq", &["llama-70b"]));
        file.models.insert("nvidia/nemotron".into(), cfg("nvidia", &[]));
        build_snapshot(&file, 1, SystemTime::now())
    }

    #[test]
    fn exact_id_resolves_and_strips_prefix() {
        let snap = sample();
        let r = snap.resolve("groq/llama-3.1-70b-versatile").unwrap();
        assert_eq!(r.upstream_model, "llama-3.1-70b-versatile");
        assert_eq!(r.provider(), "groq");
    }

    #[test]
    fn alias_resolves_to_canonical_id() {
        let snap = sample();
        let r = snap.resolve("llama-70b").unwrap();
        assert_eq!(r.registry_id, "groq/llama-3.1-70b-versatile");
        assert_eq!(r.requested, "llama-70b");
    }

    #[test]
    fn bare_provider_shorthand_resolves_only_when_unambiguous() {
        let mut file = RegistryFile { schema_version: 1, models: HashMap::new() };
        file.models.insert("groq/a".into(), cfg("groq", &[]));
        file.models.insert("groq/b".into(), cfg("groq", &[]));
        let snap = build_snapshot(&file, 1, SystemTime::now());
        // `groq` alone is ambiguous with two models enabled.
        assert!(matches!(snap.resolve("groq"), Err(ResolveError::Ambiguous { .. })));
        assert!(matches!(snap.resolve("groq/*"), Err(ResolveError::Ambiguous { .. })));

        // A specific name under a provider is a miss, not an ambiguity.
        assert!(matches!(snap.resolve("groq/c"), Err(ResolveError::NotFound { .. })));
    }

    #[test]
    fn single_model_provider_shorthand_resolves() {
        let mut file = RegistryFile { schema_version: 1, models: HashMap::new() };
        file.models.insert("groq/only".into(), cfg("groq", &[]));
        file.models.insert("nvidia/a".into(), cfg("nvidia", &[]));
        let snap = build_snapshot(&file, 1, SystemTime::now());
        let r = snap.resolve("groq").unwrap();
        assert_eq!(r.registry_id, "groq/only");
    }

    #[test]
    fn unknown_model_is_not_found() {
        let snap = sample();
        assert!(matches!(snap.resolve("nope"), Err(ResolveError::NotFound { .. })));
    }

    #[test]
    fn disabled_model_is_reported() {
        let mut file = RegistryFile { schema_version: 1, models: HashMap::new() };
        let mut c = cfg("groq", &[]);
        c.enabled = false;
        file.models.insert("groq/off".into(), c);
        let snap = build_snapshot(&file, 1, SystemTime::now());
        assert!(matches!(snap.resolve("groq/off"), Err(ResolveError::Disabled { .. })));
    }

    #[test]
    fn usd_per_mtok_equals_nano_usd_per_token() {
        // $0.59 / 1M tokens == 590 nano-USD per token.
        let c = CostModel {
            input_per_mtok_usd: 0.59,
            output_per_mtok_usd: 0.79,
            cached_input_per_mtok_usd: None,
        };
        assert_eq!(c.input_nano_usd_per_token(), 590);
        assert_eq!(c.output_nano_usd_per_token(), 790);
        assert!(!c.is_free());
    }

    /// Rates below $1 per 1M tokens are exactly the ones an integer
    /// micro-USD-per-token cannot represent: $0.075/MTok is 0.075 micro-USD per
    /// token, so rounding it to an integer yields 0 and makes the model free
    /// to the budget engine. Every value below used to collapse to 0 or 1.
    #[test]
    fn fractional_rates_are_exact_rather_than_rounded_away() {
        for (usd_per_mtok, expected_nano) in [
            (0.075_f64, 75_u64),
            (0.15, 150),
            (0.30, 300),
            (0.40, 400),
            (0.60, 600),
            (1.60, 1_600),
            (3.00, 3_000),
            (15.00, 15_000),
        ] {
            let c = CostModel {
                input_per_mtok_usd: usd_per_mtok,
                output_per_mtok_usd: 0.0,
                cached_input_per_mtok_usd: None,
            };
            assert_eq!(
                c.input_nano_usd_per_token(),
                expected_nano,
                "${usd_per_mtok}/MTok must be {expected_nano} nano-USD/token, not rounded"
            );
        }
    }

    #[test]
    fn zero_cost_model_is_free() {
        let c = CostModel { input_per_mtok_usd: 0.0, output_per_mtok_usd: 0.0, cached_input_per_mtok_usd: None };
        assert_eq!(c.input_nano_usd_per_token(), 0);
        assert!(c.is_free());
    }

    #[test]
    fn a_realistic_stream_cost_is_sane() {
        // 1M prompt tokens at $2/MTok + 1M completion at $8/MTok = $10.
        let c = CostModel { input_per_mtok_usd: 2.0, output_per_mtok_usd: 8.0, cached_input_per_mtok_usd: None };
        let cost = 1_000_000 * c.input_nano_usd_per_token() + 1_000_000 * c.output_nano_usd_per_token();
        assert_eq!(cost, 10_000_000_000); // $10 expressed in nano-USD
    }

    /// Loads the catalogue the gateway actually ships. The unit bug was
    /// invisible in the unit tests above because every rate there was a whole
    /// number of micro-USD per token; the real prices are not, and three of
    /// the five shipped models were being billed at zero.
    #[tokio::test]
    async fn no_shipped_model_is_priced_free_unless_configured_free() {
        let registry = ModelRegistry::load("config/models.yaml", false, 0).expect("config/models.yaml must load");
        let snapshot = registry.snapshot().await;
        assert!(!snapshot.is_empty(), "the shipped catalogue must not be empty");
        for id in snapshot.ids() {
            let cfg = &snapshot.by_id[id];
            let configured_free =
                cfg.cost.input_per_mtok_usd == 0.0 && cfg.cost.output_per_mtok_usd == 0.0;
            assert_eq!(
                cfg.cost.is_free(),
                configured_free,
                "model `{id}` is configured at {} in / {} out USD per MTok but reports is_free() = {}",
                cfg.cost.input_per_mtok_usd,
                cfg.cost.output_per_mtok_usd,
                cfg.cost.is_free(),
            );
        }
    }

    #[test]
    fn unsupported_provider_is_reported() {
        let snap = sample();
        let supported: HashSet<String> = ["groq".to_string()].into_iter().collect();
        let err = snap.resolve_supported("nvidia/nemotron", &supported).unwrap_err();
        assert!(matches!(err, ResolveError::UnknownProvider { .. }));
    }
}
