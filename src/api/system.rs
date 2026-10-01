//! Operational endpoints: model catalogue, quota visibility, health, metrics,
//! and a control-plane reload.
//!
//! The tenant-facing ones require the same API key as chat, and `/v1/usage`
//! is scoped to the caller's own tenant. `/metrics` is cross-tenant by
//! nature, so it is either admin-scoped on the public port or served only on
//! the ops listener; see [`crate::api::ops_router`]. Health probes are open.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{header::CONTENT_TYPE, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use crate::{governance::auth::Principal, state::AppState};

/// A handler's failure, described rather than pre-rendered.
///
/// The error variant is a small enum rather than a `Response` (~128 bytes), and
/// a real `Response` is built only on the way out. These handlers run per
/// request, and the failure path should not be the expensive one.
#[derive(Debug)]
pub enum ApiError {
    /// No or invalid credentials.
    Unauthorized,
    /// Authenticated, but not permitted.
    Forbidden(&'static str),
    /// The caller's tenant is not provisioned.
    TenantUnknown,
    /// A control-plane operation failed.
    ReloadFailed(String),
    /// The spend ledger could not be read.
    LedgerUnavailable,
    /// Any other refusal, with its own status and stable code.
    Refused {
        status: StatusCode,
        code: &'static str,
        message: String,
    },
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, kind, message) = match &self {
            ApiError::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "authentication_error",
                "valid credentials are required".to_string(),
            ),
            ApiError::Forbidden(message) => (
                StatusCode::FORBIDDEN,
                "forbidden",
                "permission_error",
                (*message).to_string(),
            ),
            ApiError::TenantUnknown => (
                StatusCode::NOT_FOUND,
                "tenant_not_provisioned",
                "not_found_error",
                "this tenant is not provisioned on the gateway".to_string(),
            ),
            ApiError::ReloadFailed(message) => (
                StatusCode::BAD_GATEWAY,
                "reload_failed",
                "api_error",
                message.clone(),
            ),
            ApiError::LedgerUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "ledger_unavailable",
                "api_error",
                "the spend ledger is unavailable; retry shortly".to_string(),
            ),
            ApiError::Refused { status, code, message } => (
                *status,
                *code,
                match *status {
                    StatusCode::FORBIDDEN => "permission_error",
                    StatusCode::NOT_FOUND => "not_found_error",
                    StatusCode::CONFLICT => "conflict_error",
                    _ => "invalid_request_error",
                },
                message.clone(),
            ),
        };
        (
            status,
            Json(json!({
                "error": { "code": code, "type": kind, "message": message }
            })),
        )
            .into_response()
    }
}

/// Resolve the caller, or 401. Every operational route goes through this.
async fn caller(state: &Arc<AppState>, headers: &axum::http::HeaderMap) -> Result<Principal, ApiError> {
    state.current_auth().authenticate(headers).await.map_err(|_| ApiError::Unauthorized)
}

/// `GET /v1/models` — the catalogue, filtered to what this tenant may call.
/// Clients should never have to guess which ids are permitted.
pub async fn list_models(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    let principal = caller(&state, &headers).await?;
    let tenants = state.current_tenants();
    let tenant = tenants
        .get(&principal.tenant_id)
        .ok_or(ApiError::TenantUnknown)?;
    let snapshot = state.models.snapshot().await;

    let mut models: Vec<_> = snapshot
        .ids()
        .into_iter()
        .filter_map(|id| {
            let cfg = snapshot.by_id.get(id)?;
            let qualified = crate::governance::policy::qualify(&tenant.model_suffix, id);
            if !crate::governance::policy::glob_any(&tenant.allowed_models, &qualified)
                || crate::governance::policy::glob_any(&tenant.denied_models, &qualified)
            {
                return None;
            }
            Some(json!({
                "id": id,
                "display_name": cfg.display_name,
                "provider": cfg.provider,
                "upstream_model": cfg.upstream_model_name(id),
                "aliases": cfg.aliases,
                "latency_slo_ms": cfg.latency_slo_ms,
                "max_context_tokens": cfg.max_context_tokens,
                "max_output_tokens": cfg.max_output_tokens,
                "input_per_mtok_usd": cfg.cost.input_per_mtok_usd,
                "output_per_mtok_usd": cfg.cost.output_per_mtok_usd,
            }))
        })
        .collect();
    models.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));

    Ok(Json(json!({
        "object": "list",
        "data": models,
        "registry_generation": snapshot.generation,
    }))
    .into_response())
}

/// `GET /v1/usage` — this tenant's quota and spend. Governed tenants need to
/// be able to see their own consumption without asking an operator.
pub async fn get_usage(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    let principal = caller(&state, &headers).await?;
    let tenants = state.current_tenants();
    let tenant = tenants
        .get(&principal.tenant_id)
        .ok_or(ApiError::TenantUnknown)?;
    let limits = state.policy.limits_for(&tenant);
    let snapshot = state
        .limits
        .snapshot(&principal.tenant_id, &limits)
        .await
        .map_err(|_| ApiError::LedgerUnavailable)?;

    Ok(Json(json!({
        "tenant_id": snapshot.tenant_id,
        "requests_last_minute": snapshot.requests_last_minute,
        "tokens_last_minute": snapshot.tokens_last_minute,
        "streams_in_flight": snapshot.in_flight,
        "spent_nano_usd": snapshot.spent_nano_usd,
        "remaining_nano_usd": snapshot.remaining_nano_usd(),
        "daily_budget_nano_usd": snapshot.budget_nano_usd,
        // In quota-split mode the budget above is this replica's share, and
        // `spent` is what this replica has spent of it.
        "budget_scope": match state.limits.quota_split() {
            None => json!("tenant"),
            Some(split) => json!({
                "replica_share": {
                    "replicas": split.replicas,
                    "margin_percent": split.margin_percent,
                    "tenant_budget_nano_usd": limits.daily_budget_nano_usd,
                }
            }),
        },
        "budget_day": snapshot.budget_label,
        "limits": {
            "requests_per_minute": limits.requests_per_minute,
            "tokens_per_minute": limits.tokens_per_minute,
            "max_concurrent_streams": limits.max_concurrent_streams,
            "max_output_tokens": limits.max_output_tokens,
        }
    }))
    .into_response())
}

/// `POST /v1/admin/registry/reload` — re-read `models.yaml` and `tenants.yaml`
/// without a restart. Requires the `admin` scope. A failed reload keeps the
/// previous generation serving traffic.
pub async fn reload_registry(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    let principal = caller(&state, &headers).await?;
    if !principal.has_scope(crate::governance::auth::SCOPE_ADMIN) {
        return Err(ApiError::Forbidden("this operation requires the `admin` scope"));
    }

    // An operator action is recorded before it takes effect. If the record
    // cannot be written, the action does not happen.
    let action = |code: &str, kind, reason: &str| {
        crate::governance::ledger::decisions::Decision {
            request_id: uuid::Uuid::new_v4(),
            tenant_id: principal.tenant_id.to_string(),
            key_id: principal.key_id.to_string(),
            kind,
            endpoint: "admin/registry/reload".to_string(),
            model: None,
            code: code.to_string(),
            reason: String::new(),
        }
        .with_reason(reason)
    };
    use crate::governance::ledger::decisions::DecisionKind;
    state
        .limits
        .ledger()
        .record_decision_durably(action("registry_reload", DecisionKind::AdminAction, "requested"))
        .await
        .map_err(|_| ApiError::LedgerUnavailable)?;

    let reloaded = async {
        let models = state
            .models
            .reload()
            .await
            .map_err(|e| format!("model registry reload failed: {e}"))?;
        let tenants = state
            .reload_tenants()
            .map_err(|e| format!("tenant registry reload failed: {e}"))?;
        Ok::<_, String>((models, tenants))
    }
    .await;
    let (models, tenants) = match reloaded {
        Ok(counts) => counts,
        Err(message) => {
            state
                .limits
                .ledger()
                .record_decision(action("registry_reload_failed", DecisionKind::Failed, &message));
            return Err(ApiError::ReloadFailed(message));
        }
    };

    tracing::info!(
        actor = %principal.key_id,
        tenant = %principal.tenant_id,
        generation = models,
        tenants,
        "registry reloaded"
    );
    Ok(Json(json!({
        "status": "reloaded",
        "model_generation": models,
        "models": state.models.snapshot().await.len(),
        "tenants": tenants,
    }))
    .into_response())
}

pub async fn live() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Readiness reflects the things a request actually depends on: a loaded
/// registry, live providers, and a resolvable default model. A gateway that
/// cannot route should be pulled from the load balancer, not left to fail
/// requests.
/// What the ledger guarantees, spelled out: a bare backend name invites
/// mistaking a per-instance ledger for a shared one.
fn ledger_scope(state: &AppState) -> serde_json::Value {
    let ledger = state.limits.ledger();
    let (durability, replica_mode) = match ledger.backend() {
        "memory" => ("ephemeral", json!("single_process")),
        "postgres" => ("persistent", json!("shared")),
        _ => (
            "persistent",
            match state.limits.quota_split() {
                Some(split) => json!({
                    "quota_split": { "replicas": split.replicas, "margin_percent": split.margin_percent }
                }),
                None => json!("single_instance_only"),
            },
        ),
    };
    json!({ "backend": ledger.backend(), "durability": durability, "replica_mode": replica_mode })
}

pub async fn ready(State(state): State<Arc<AppState>>) -> Response {
    let snapshot = state.models.snapshot().await;
    let mut issues: Vec<String> = Vec::new();

    if snapshot.is_empty() {
        issues.push("model registry is empty".into());
    }
    if state.providers.is_empty() {
        issues.push("no providers are configured".into());
    }
    // Every admission goes through the ledger, and an unreachable ledger
    // refuses them all. Pull this replica from the load balancer instead.
    if !state.limits.ledger().healthy().await {
        issues.push(format!("the {} spend ledger is unreachable", state.limits.ledger().backend()));
    }
    for id in snapshot.ids() {
        if let Some(cfg) = snapshot.by_id.get(id)
            && !state.providers.names().contains(&cfg.provider.to_ascii_lowercase())
        {
            issues.push(format!("model `{id}` names unknown provider `{}`", cfg.provider));
        }
    }

    if issues.is_empty() {
        (
            StatusCode::OK,
            Json(json!({
                "status": "ready",
                "uptime_seconds": state.uptime_secs(),
                "models": snapshot.len(),
                "providers": state.providers.names().len(),
                "registry_generation": snapshot.generation,
                "ledger": ledger_scope(&state),
            })),
        )
            .into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "not_ready", "issues": issues, "ledger": ledger_scope(&state) })),
        )
            .into_response()
    }
}

/// `/metrics` on the public port. The exposition carries every tenant's
/// spend and error counts, so on a port tenants can reach it is an admin
/// operation. A Prometheus scrape config sends the key as a bearer token.
pub async fn metrics_for_admins(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    let principal = caller(&state, &headers).await?;
    if !principal.has_scope(crate::governance::auth::SCOPE_ADMIN) {
        return Err(ApiError::Forbidden("`/metrics` requires the `admin` scope"));
    }
    Ok(metrics(State(state)).await)
}

/// Prometheus text exposition. Refreshes the uptime gauge, then renders.
pub async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    state.metrics.refresh_uptime(state.started_at);
    let mut body = state.metrics.render();
    use std::fmt::Write as _;
    let _ = writeln!(
        body,
        "# HELP gw_decisions_dropped_total Routine decision records dropped because they could not be kept.\n# TYPE gw_decisions_dropped_total counter\ngw_decisions_dropped_total {}",
        state.limits.ledger().decisions_dropped()
    );
    let upkeep = state.limits.ledger().upkeep();
    let _ = writeln!(
        body,
        "# HELP gw_ledger_sweep_failures_total Ledger sweeper steps that failed; while they fail, lapsed reservations stay open.
# TYPE gw_ledger_sweep_failures_total counter
gw_ledger_sweep_failures_total {}
         # HELP gw_ledger_swept_reservations_total Reservations closed by the sweeper after their lease lapsed, each billed its full reservation.
# TYPE gw_ledger_swept_reservations_total counter
gw_ledger_swept_reservations_total {}
         # HELP gw_ledger_late_commits_released_total Reservations that committed after their admission timed out, released at no charge.
# TYPE gw_ledger_late_commits_released_total counter
gw_ledger_late_commits_released_total {}",
        upkeep.sweep_failures, upkeep.swept, upkeep.late_commits_released
    );
    let mut response = body.into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    response
}

#[derive(Debug, serde::Deserialize)]
pub struct DecisionParams {
    tenant: Option<String>,
    /// Unix seconds. Defaults to the last 24 hours.
    since: Option<i64>,
    /// Default 100, at most 1000.
    limit: Option<u32>,
}

/// `GET /v1/admin/decisions` — the decision record, newest first: refusals,
/// failures and operator actions for authenticated callers. Admin scope,
/// like `/metrics`: it spans tenants. Served on the ops listener when one is
/// configured.
pub async fn list_decisions(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Query(params): Query<DecisionParams>,
) -> Result<Response, ApiError> {
    let principal = caller(&state, &headers).await?;
    if !principal.has_scope(crate::governance::auth::SCOPE_ADMIN) {
        return Err(ApiError::Forbidden("this operation requires the `admin` scope"));
    }
    let ledger = state.limits.ledger();
    let records = ledger
        .decisions(crate::governance::ledger::decisions::DecisionQuery {
            tenant_id: params.tenant,
            since: params
                .since
                .unwrap_or_else(|| crate::governance::ledger::decisions::unix_now() - 86_400),
            limit: params.limit.unwrap_or(100).clamp(1, 1_000),
        })
        .await
        .map_err(|_| ApiError::LedgerUnavailable)?;
    Ok(Json(json!({
        "decisions": records,
        "dropped_since_start": ledger.decisions_dropped(),
        "ledger": ledger.backend(),
    }))
    .into_response())
}

fn hold_not_found() -> ApiError {
    ApiError::Refused {
        status: StatusCode::NOT_FOUND,
        code: "hold_not_found",
        message: "no such hold".to_string(),
    }
}

/// `GET /v1/holds/{id}`: a held request's state, for the tenant that made
/// it. Poll this until the hold is approved, then send the request again with
/// `Hold-Id`. Another tenant's hold is not found, not forbidden: its
/// existence is not this caller's business.
pub async fn get_hold(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let principal = caller(&state, &headers).await?;
    let id = uuid::Uuid::parse_str(&id).map_err(|_| hold_not_found())?;
    let hold = state
        .limits
        .ledger()
        .hold(id)
        .await
        .map_err(|_| ApiError::LedgerUnavailable)?
        .filter(|h| h.tenant_id == *principal.tenant_id)
        .ok_or_else(hold_not_found)?;
    Ok(Json(json!({ "hold": hold })).into_response())
}

#[derive(Debug, serde::Deserialize)]
pub struct HoldParams {
    tenant: Option<String>,
    /// `pending`, `approved`, `denied`, `expired` or `consumed`.
    state: Option<String>,
    /// Default 100, at most 1000.
    limit: Option<u32>,
}

/// `GET /v1/admin/holds`: holds across tenants, newest first, for whoever
/// decides them. `?state=pending` is the approval queue. Needs
/// `approve:holds`, or `admin` to look without deciding.
pub async fn list_holds(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Query(params): Query<HoldParams>,
) -> Result<Response, ApiError> {
    use crate::governance::{
        auth::{SCOPE_ADMIN, SCOPE_APPROVE_HOLDS},
        ledger::holds::{HoldQuery, HoldState},
    };
    let principal = caller(&state, &headers).await?;
    if !principal.has_explicit_scope(SCOPE_APPROVE_HOLDS) && !principal.has_scope(SCOPE_ADMIN) {
        return Err(ApiError::Forbidden("listing holds requires the `approve:holds` or `admin` scope"));
    }
    let wanted = match params.state.as_deref() {
        None => None,
        Some(name) => Some(HoldState::parse(name).ok_or_else(|| ApiError::Refused {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_parameter",
            message: "`state` must be pending, approved, denied, expired or consumed".to_string(),
        })?),
    };
    let holds = state
        .limits
        .ledger()
        .holds(HoldQuery {
            tenant_id: params.tenant,
            state: wanted,
            limit: params.limit.unwrap_or(100).clamp(1, 1_000),
        })
        .await
        .map_err(|_| ApiError::LedgerUnavailable)?;
    Ok(Json(json!({ "holds": holds })).into_response())
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DecideBody {
    /// Kept on the hold and the decision record.
    #[serde(default)]
    note: Option<String>,
}

/// Longest approver note kept.
const NOTE_MAX: usize = 300;

/// `POST /v1/admin/holds/{id}/approve`, optionally with `{"note": "..."}`.
pub async fn approve_hold(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    body: bytes::Bytes,
) -> Result<Response, ApiError> {
    decide_hold(state, headers, id, body, crate::governance::ledger::holds::Verdict::Approve).await
}

/// `POST /v1/admin/holds/{id}/deny`. Also revokes an approval not yet used.
pub async fn deny_hold(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    body: bytes::Bytes,
) -> Result<Response, ApiError> {
    decide_hold(state, headers, id, body, crate::governance::ledger::holds::Verdict::Deny).await
}

/// Approve or deny. Needs `approve:holds`, granted by name: `admin` does not
/// imply it, and neither does a wildcard grant. The decision is recorded in
/// the same step as the transition; if it cannot be recorded, nothing
/// changes and the approver gets `503`.
async fn decide_hold(
    state: Arc<AppState>,
    headers: axum::http::HeaderMap,
    id: String,
    body: bytes::Bytes,
    verdict: crate::governance::ledger::holds::Verdict,
) -> Result<Response, ApiError> {
    use crate::governance::{
        auth::SCOPE_APPROVE_HOLDS,
        ledger::{
            LedgerRefusal,
            holds::{HoldDecision, HoldProblem},
        },
    };
    let principal = caller(&state, &headers).await?;
    if !principal.has_explicit_scope(SCOPE_APPROVE_HOLDS) {
        return Err(ApiError::Forbidden(
            "deciding a hold requires the `approve:holds` scope, granted by name; `admin` does not imply it",
        ));
    }
    let id = uuid::Uuid::parse_str(&id).map_err(|_| hold_not_found())?;
    let note = if body.iter().all(u8::is_ascii_whitespace) {
        String::new()
    } else {
        let parsed: DecideBody = serde_json::from_slice(&body).map_err(|_| ApiError::Refused {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_json",
            message: "the body must be empty or `{\"note\": \"...\"}`".to_string(),
        })?;
        parsed.note.unwrap_or_default().trim().chars().take(NOTE_MAX).collect()
    };
    let decision = HoldDecision {
        id,
        approver_tenant: principal.tenant_id.to_string(),
        approver_key: principal.key_id.to_string(),
        verdict,
        note,
    };
    let hold = match state.limits.ledger().decide_hold(decision).await {
        Ok(hold) => hold,
        Err(LedgerRefusal::Hold(HoldProblem::NotFound)) => return Err(hold_not_found()),
        Err(LedgerRefusal::HoldNotDecidable { state }) => {
            return Err(ApiError::Refused {
                status: StatusCode::CONFLICT,
                code: "hold_not_decidable",
                message: format!("this hold is {} and can no longer be decided", state.as_str()),
            });
        }
        Err(LedgerRefusal::SelfApproval) => {
            return Err(ApiError::Refused {
                status: StatusCode::FORBIDDEN,
                code: "self_approval",
                message: "the key that made a request cannot approve or deny it".to_string(),
            });
        }
        Err(_) => return Err(ApiError::LedgerUnavailable),
    };
    tracing::info!(
        hold = %hold.id,
        tenant = %hold.tenant_id,
        actor = %principal.key_id,
        actor_tenant = %principal.tenant_id,
        state = hold.state.as_str(),
        "hold decided"
    );
    Ok(Json(json!({ "hold": hold })).into_response())
}

pub async fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": { "code": "not_found", "type": "not_found_error",
            "message": "no route matches this path" } })),
    )
        .into_response()
}



