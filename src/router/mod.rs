//! Model routing: turn a caller's `model` string into a provider, a set of
//! effective parameters, and a cost estimate.

pub mod model_registry;

pub use model_registry::{
    CostModel, ModelConfig, ModelRegistry, RegistryError, RegistryFile, RegistrySnapshot,
    ResolveError, ResolvedModel, REGISTRY_SCHEMA_VERSION,
};

use std::sync::Arc;

use serde_json::{Map, Value};

pub use crate::providers::ProviderPool;
use crate::providers::{ChatProvider, ResolvedParams, MAX_COMPLETION_TOKENS_MODELS};

/// Keys a caller may not set directly because the gateway owns them.
pub const RESERVED_PARAM_KEYS: &[&str] = &[
    "model",
    "messages",
    "stream",
    "stream_options",
];

/// Precedence for effective parameters, lowest to highest:
///  1. registry `default_params`
///  2. caller's `params`
///  3. governance clamps (applied by the caller of [`merge_params`])
pub fn merge_params(defaults: &Map<String, Value>, requested: Option<&Map<String, Value>>) -> ResolvedParams {
    let mut merged = ResolvedParams::default();
    for (k, v) in defaults {
        if RESERVED_PARAM_KEYS.contains(&k.as_str()) {
            continue;
        }
        merged.set_from_json(k, v);
    }
    if let Some(requested) = requested {
        for (k, v) in requested {
            if RESERVED_PARAM_KEYS.contains(&k.as_str()) {
                continue;
            }
            merged.set_from_json(k, v);
        }
    }
    merged
}

/// Reject values a provider will certainly reject, before we spend a
/// connection on them. Cheap, and keeps upstream 400s off our error budget.
pub fn validate_params(params: &ResolvedParams) -> Result<(), ParamViolation> {
    if let Some(t) = params.temperature {
        if !(0.0..=2.0).contains(&t) {
            return Err(ParamViolation::OutOfRange {
                field: "temperature".into(),
                value: t,
                allowed: "0.0..=2.0".into(),
            });
        }
    }
    if let Some(p) = params.top_p {
        if !(0.0..=1.0).contains(&p) {
            return Err(ParamViolation::OutOfRange {
                field: "top_p".into(),
                value: p,
                allowed: "0.0..=1.0".into(),
            });
        }
    }
    if let Some(m) = params.max_tokens {
        if m == 0 {
            return Err(ParamViolation::ZeroMaxTokens);
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ParamViolation {
    #[error("parameter `{field}` = {value} is outside the allowed range {allowed}")]
    OutOfRange {
        field: String,
        value: f64,
        allowed: String,
    },
    #[error("`max_tokens` must be greater than zero")]
    ZeroMaxTokens,
}

/// Look up the adapter responsible for a resolved model.
pub fn provider_for<'a>(
    resolved: &ResolvedModel,
    pool: &'a ProviderPool,
) -> Result<&'a Arc<dyn ChatProvider>, ResolveError> {
    pool.get(resolved.provider())
        .ok_or_else(|| ResolveError::UnknownProvider {
            requested: resolved.requested.clone(),
            provider: resolved.provider().to_string(),
        })
}

/// Cheap prompt-size estimate for pre-flight cost reservation: ~4 characters
/// per token. Deliberately an over-estimate so the budget check fails closed.
pub fn estimate_prompt_tokens(messages: &[crate::providers::ChatMessage]) -> u32 {
    let mut chars = 0usize;
    for m in messages {
        chars += m.approx_chars();
        chars += 8; // role / framing overhead per message
    }
    ((chars as f64) / 3.5).ceil() as u32
}

/// Micro-USD a request is expected to cost, given the effective output cap.
/// Reserved before the stream opens, settled with real usage afterwards.
pub fn estimate_cost_micro_usd(cost: &CostModel, prompt_tokens: u32, max_output_tokens: u32) -> u64 {
    prompt_tokens as u64 * cost.input_micro_usd_per_token()
        + max_output_tokens as u64 * cost.output_micro_usd_per_token()
}

/// True when the model must be addressed with `max_completion_tokens` instead
/// of the legacy `max_tokens` (o1-style reasoning models reject the latter).
pub fn needs_completion_token_field(upstream_model: &str) -> bool {
    MAX_COMPLETION_TOKENS_MODELS
        .iter()
        .any(|prefix| upstream_model.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn map(pairs: &[(&str, Value)]) -> Map<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    /// Tenants set `model_params` to express a governance constraint (a pinned
    /// temperature, a forced response format), so it is applied last. A
    /// governance constraint a caller can override is not a constraint.
    #[test]
    fn tenant_policy_overrides_both_registry_defaults_and_request_params() {
        let mut defaults = Map::new();
        defaults.insert("temperature".into(), json!(0.7));
        let mut requested = Map::new();
        requested.insert("temperature".into(), json!(0.9));

        let mut merged = merge_params(&defaults, Some(&requested));
        assert_eq!(merged.temperature, Some(0.9), "request beats registry default");

        // What `api::chat` does after merging.
        let policy = [("temperature".to_string(), json!(0.1))];
        for (key, value) in &policy {
            merged.set_from_json(key, value);
        }
        assert_eq!(merged.temperature, Some(0.1), "policy wins overall");
    }

    #[test]
    fn request_params_override_registry_defaults() {
        let defaults = map(&[("temperature", json!(0.7)), ("max_tokens", json!(2048))]);
        let requested = map(&[("temperature", json!(0.1))]);
        let merged = merge_params(&defaults, Some(&requested));
        assert_eq!(merged.temperature, Some(0.1));
        assert_eq!(merged.max_tokens, Some(2048));
    }

    #[test]
    fn reserved_keys_are_never_forwarded() {
        let requested = map(&[("stream", json!(false)), ("model", json!("evil")), ("top_k", json!(40))]);
        let merged = merge_params(&Map::new(), Some(&requested));
        assert!(!merged.extra.contains_key("stream"));
        assert!(!merged.extra.contains_key("model"));
        assert_eq!(merged.extra.get("top_k"), Some(&json!(40)));
    }

    #[test]
    fn out_of_range_temperature_is_rejected() {
        let p = ResolvedParams { temperature: Some(9.0), ..Default::default() };
        assert!(validate_params(&p).is_err());
    }

    #[test]
    fn reasoning_models_use_completion_token_field() {
        assert!(needs_completion_token_field("o1-mini"));
        assert!(needs_completion_token_field("o3-2025-04-16"));
        assert!(!needs_completion_token_field("gpt-4.1-mini"));
    }
}
