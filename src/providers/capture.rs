//! Opt-in capture of the JSON body a non-streaming completion forwards upstream.
//!
//! Off unless `LLM_GATEWAY_CAPTURE_FORWARDED` is `1` or `true` and both
//! `LLM_GATEWAY_CAPTURE_FORWARDED_PATH` and `LLM_GATEWAY_CAPTURE_FORWARDED_TENANT`
//! are set. The bench turns it on for the folded tenant. A summarizer call
//! shares that tenant id and is left out: its `key_id` is `summarizer`, and
//! the body under study is the one the tenant's own completion forwarded.
//!
//! The bytes written under `body` are the bytes `serde_json` would hand to
//! reqwest for that map, not a second rendering. The provider's `usage`
//! object supplies prompt tokens, completion tokens, and `cost` when the
//! provider sent one. Nothing here is added to the request.

use std::{
    collections::HashMap,
    env, fs,
    io::{self, Write},
    path::PathBuf,
    sync::{LazyLock, Mutex, OnceLock},
};

use serde_json::{Map, Value};
use uuid::Uuid;

use super::ProviderRequest;

static STAGED: LazyLock<Mutex<HashMap<Uuid, Vec<u8>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static WRITE_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug)]
pub(crate) struct CaptureSettings {
    path: PathBuf,
    tenant: String,
}

pub(crate) fn config_from(
    flag: Option<&str>,
    path: Option<&str>,
    tenant: Option<&str>,
) -> Option<CaptureSettings> {
    if !flag_on(flag) {
        return None;
    }
    let path = path.filter(|value| !value.is_empty())?;
    let tenant = tenant.filter(|value| !value.is_empty())?;
    Some(CaptureSettings {
        path: PathBuf::from(path),
        tenant: tenant.to_string(),
    })
}

fn flag_on(flag: Option<&str>) -> bool {
    let Some(flag) = flag else {
        return false;
    };
    flag == "1" || flag.eq_ignore_ascii_case("true")
}

fn installed() -> Option<&'static CaptureSettings> {
    static CONFIG: OnceLock<Option<CaptureSettings>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            config_from(
                env::var("LLM_GATEWAY_CAPTURE_FORWARDED").ok().as_deref(),
                env::var("LLM_GATEWAY_CAPTURE_FORWARDED_PATH")
                    .ok()
                    .as_deref(),
                env::var("LLM_GATEWAY_CAPTURE_FORWARDED_TENANT")
                    .ok()
                    .as_deref(),
            )
        })
        .as_ref()
}

/// The exact JSON to send, staged so the completion can pair it with usage.
/// `None` leaves the caller on the ordinary `.json(&body)` path: the flag is
/// off, this call is not the named tenant's own completion, or it is a stream.
pub(crate) fn owned_body(
    req: &ProviderRequest,
    body: &Map<String, Value>,
    streaming: bool,
) -> Option<Vec<u8>> {
    owned_body_with(installed(), req, body, streaming)
}

fn owned_body_with(
    settings: Option<&CaptureSettings>,
    req: &ProviderRequest,
    body: &Map<String, Value>,
    streaming: bool,
) -> Option<Vec<u8>> {
    let settings = settings?;
    if streaming || !settings.matches(req) {
        return None;
    }
    let bytes = serde_json::to_vec(body).ok()?;
    stage(req.request_id, bytes.clone());
    Some(bytes)
}

fn stage(id: Uuid, bytes: Vec<u8>) {
    STAGED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(id, bytes);
}

fn take(id: &Uuid) -> Option<Vec<u8>> {
    STAGED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(id)
}

/// Drop a staged body when the send itself failed, so a later call cannot
/// attach it to the wrong response.
pub(crate) fn discard(request_id: &Uuid) {
    if installed().is_none() {
        return;
    }
    take(request_id);
}

/// Write the staged body and the provider's reported usage. A missing stage
/// (capture off, or not this tenant) writes nothing.
pub(crate) fn finish(req: &ProviderRequest, response: Option<&Value>) {
    let Some(settings) = installed() else {
        return;
    };
    finish_with(settings, req, response);
}

fn finish_with(settings: &CaptureSettings, req: &ProviderRequest, response: Option<&Value>) {
    let Some(body) = take(&req.request_id) else {
        return;
    };
    if let Err(err) = settings.write(req, &body, response) {
        tracing::warn!(
            error = %err,
            path = %settings.path.display(),
            "forwarded-body capture failed"
        );
    }
}

impl CaptureSettings {
    fn matches(&self, req: &ProviderRequest) -> bool {
        req.key_id.as_ref() != "summarizer" && req.tenant_id.as_ref() == self.tenant
    }

    fn write(
        &self,
        req: &ProviderRequest,
        body: &[u8],
        response: Option<&Value>,
    ) -> io::Result<()> {
        let usage = serde_json::to_vec(&reported_usage(response))
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let mut line = Vec::with_capacity(body.len() + usage.len() + 96);
        line.extend_from_slice(br#"{"request_id":"#);
        push_json_string(&mut line, &req.request_id.to_string())?;
        line.extend_from_slice(br#","tenant_id":"#);
        push_json_string(&mut line, req.tenant_id.as_ref())?;
        line.extend_from_slice(br#","key_id":"#);
        push_json_string(&mut line, req.key_id.as_ref())?;
        line.extend_from_slice(br#","body":"#);
        line.extend_from_slice(body);
        line.extend_from_slice(br#","usage":"#);
        line.extend_from_slice(&usage);
        line.extend_from_slice(b"}\n");

        let _guard = WRITE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(&line)?;
        file.flush()
    }
}

fn push_json_string(out: &mut Vec<u8>, value: &str) -> io::Result<()> {
    let encoded = serde_json::to_vec(&value)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    out.extend_from_slice(&encoded);
    Ok(())
}

/// Prompt tokens, completion tokens, and cost, as the provider reported them.
/// A field the provider left out stays null. `reported` is the usage object
/// unchanged, so a cost the provider put somewhere else is still in the file.
fn reported_usage(response: Option<&Value>) -> Value {
    let usage = response.and_then(|value| value.get("usage"));
    let prompt_tokens = usage
        .and_then(|value| value.get("prompt_tokens"))
        .cloned()
        .unwrap_or(Value::Null);
    let completion_tokens = usage
        .and_then(|value| value.get("completion_tokens"))
        .cloned()
        .unwrap_or(Value::Null);
    let cost = usage.and_then(reported_cost).unwrap_or(Value::Null);
    serde_json::json!({
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "cost": cost,
        "reported": usage.cloned().unwrap_or(Value::Null),
    })
}

fn reported_cost(usage: &Value) -> Option<Value> {
    match usage.get("cost")? {
        Value::Number(number) => Some(Value::Number(number.clone())),
        Value::String(text) => {
            let number = text.parse::<f64>().ok().filter(|value| value.is_finite())?;
            serde_json::Number::from_f64(number).map(Value::Number)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{config_from, finish_with, owned_body_with, reported_usage};
    use crate::providers::{ProviderRequest, ResolvedParams};

    fn request(id: Uuid, tenant: &str, key: &str) -> ProviderRequest {
        ProviderRequest {
            request_id: id,
            tenant_id: Arc::from(tenant),
            key_id: Arc::from(key),
            upstream_model: "m".into(),
            endpoint: None,
            messages: Arc::new(vec![]),
            params: ResolvedParams::default(),
            include_usage: true,
        }
    }

    #[test]
    fn capture_stays_off_unless_the_flag_path_and_tenant_are_all_set() {
        let path = "benchmark_results/capture.jsonl";
        assert!(config_from(None, Some(path), Some("folded")).is_none());
        assert!(config_from(Some(""), Some(path), Some("folded")).is_none());
        assert!(config_from(Some("0"), Some(path), Some("folded")).is_none());
        assert!(config_from(Some("false"), Some(path), Some("folded")).is_none());
        assert!(config_from(Some("1"), None, Some("folded")).is_none());
        assert!(config_from(Some("1"), Some(""), Some("folded")).is_none());
        assert!(config_from(Some("1"), Some(path), None).is_none());
        assert!(config_from(Some("1"), Some(path), Some("")).is_none());
        assert!(config_from(Some("true"), Some(path), Some("folded")).is_some());
        assert!(config_from(Some("TRUE"), Some(path), Some("folded")).is_some());
    }

    #[test]
    fn capture_off_does_not_write_or_replace_the_body() {
        let dir = std::env::temp_dir().join(format!("gw-capture-off-{}", Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("forwarded.jsonl");
        let settings = config_from(None, Some(path.to_str().unwrap()), Some("folded"));
        let req = request(Uuid::new_v4(), "folded", "ak_bench_folded");
        let body = json!({"model": "m", "messages": [], "stream": false});
        let Value::Object(map) = body.clone() else {
            unreachable!()
        };
        let sent = owned_body_with(settings.as_ref(), &req, &map, false);
        assert!(sent.is_none());
        assert!(!path.exists(), "an off capture must not create a sidecar");
        assert_eq!(Value::Object(map), body);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_enabled_capture_writes_the_exact_body_and_the_provider_usage() {
        let dir = std::env::temp_dir().join(format!("gw-capture-on-{}", Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("forwarded.jsonl");
        let settings =
            config_from(Some("1"), Some(path.to_str().unwrap()), Some("folded")).unwrap();
        let body = json!({
            "model": "openai/gpt-4.1-mini",
            "messages": [{"role": "user", "content": "line \"39\""}],
            "stream": false
        });
        let Value::Object(map) = body.clone() else {
            unreachable!()
        };
        let exact = serde_json::to_vec(&map).unwrap();

        let other = request(Uuid::new_v4(), "plain", "ak_bench_plain");
        assert!(owned_body_with(Some(&settings), &other, &map, false).is_none());
        let summarizer = request(Uuid::new_v4(), "folded", "summarizer");
        assert!(owned_body_with(Some(&settings), &summarizer, &map, false).is_none());
        let streamed = request(Uuid::new_v4(), "folded", "ak_bench_folded");
        assert!(owned_body_with(Some(&settings), &streamed, &map, true).is_none());

        let folded = request(Uuid::new_v4(), "folded", "ak_bench_folded");
        let sent = owned_body_with(Some(&settings), &folded, &map, false).unwrap();
        assert_eq!(sent, exact);
        let response = json!({
            "usage": {"prompt_tokens": 1722, "completion_tokens": 12, "cost": 0.000812, "total_tokens": 1734}
        });
        finish_with(&settings, &folded, Some(&response));

        let missing = request(Uuid::new_v4(), "folded", "ak_bench_folded");
        let sent = owned_body_with(Some(&settings), &missing, &map, false).unwrap();
        finish_with(
            &settings,
            &missing,
            Some(&json!({"usage": {"prompt_tokens": 3, "completion_tokens": 1}})),
        );

        let text = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines.len(),
            2,
            "plain, summarizer, and streams are not captured"
        );
        assert!(
            lines[0]
                .as_bytes()
                .windows(exact.len())
                .any(|window| window == exact.as_slice())
        );
        let record: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(record["request_id"], folded.request_id.to_string());
        assert_eq!(record["tenant_id"], "folded");
        assert_eq!(record["key_id"], "ak_bench_folded");
        assert_eq!(record["body"], body);
        assert_eq!(record["usage"]["prompt_tokens"], 1722);
        assert_eq!(record["usage"]["completion_tokens"], 12);
        assert_eq!(record["usage"]["cost"], serde_json::json!(0.000812));
        assert_eq!(record["usage"]["reported"]["total_tokens"], 1734);
        let second: Value = serde_json::from_str(lines[1]).unwrap();
        assert!(second["usage"]["cost"].is_null());
        assert_eq!(sent, exact);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_numeric_cost_string_is_kept_and_anything_else_is_null() {
        let with_string =
            json!({"usage": {"prompt_tokens": 1, "completion_tokens": 2, "cost": "0.25"}});
        let usage = reported_usage(Some(&with_string));
        assert_eq!(usage["cost"], serde_json::json!(0.25));
        let with_object = json!({"usage": {"cost": {"total": 1}}});
        assert!(reported_usage(Some(&with_object))["cost"].is_null());
        assert!(reported_usage(None)["prompt_tokens"].is_null());
        assert!(reported_usage(Some(&json!({})))["reported"].is_null());
    }
}
