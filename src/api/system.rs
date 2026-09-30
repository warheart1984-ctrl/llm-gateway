//! Operational endpoints: model catalogue, quota visibility, health, metrics,
//! and a control-plane reload.
//!
//! The tenant-facing ones require the same API key as chat, and `/v1/usage`
//! is scoped to the caller's own tenant. `/metrics` is cross-tenant by
//! nature, so it is either admin-scoped on the public port or served only on
//! the ops listener; see [`crate::api::ops_router`]. Health probes are open.

use std::sync::Arc;

use axum::{
    extract::State,
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

    let models = state
        .models
        .reload()
        .await
        .map_err(|e| ApiError::ReloadFailed(format!("model registry reload failed: {e}")))?;
    let tenants = state
        .reload_tenants()
        .map_err(|e| ApiError::ReloadFailed(format!("tenant registry reload failed: {e}")))?;

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
            })),
        )
            .into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "not_ready", "issues": issues })),
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
    let body = state.metrics.render();
    let mut response = body.into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    response
}

pub async fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": { "code": "not_found", "type": "not_found_error",
            "message": "no route matches this path" } })),
    )
        .into_response()
}



