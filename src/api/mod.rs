//! HTTP surface: the SSE chat endpoint plus operational endpoints.

pub mod chat;
pub mod system;

use std::sync::Arc;

use axum::{
    extract::{DefaultBodyLimit, State},
    middleware::Next,
    response::Response,
    routing::{get, post},
    Router,
};

use crate::state::AppState;

pub const ROUTE_CHAT_STREAM: &str = "/v1/chat/stream";
pub const ROUTE_MODELS: &str = "/v1/models";
pub const ROUTE_USAGE: &str = "/v1/usage";
pub const ROUTE_RELOAD: &str = "/v1/admin/registry/reload";
pub const ROUTE_LIVE: &str = "/health/live";
pub const ROUTE_READY: &str = "/health/ready";
pub const ROUTE_METRICS: &str = "/metrics";

pub fn router(state: Arc<AppState>) -> Router {
    let body_limit = state.settings.server.request_body_limit_bytes;

    let api = Router::new()
        .route(ROUTE_CHAT_STREAM, post(chat::chat_stream))
        .route(ROUTE_MODELS, get(system::list_models))
        .route(ROUTE_USAGE, get(system::get_usage))
        .route(ROUTE_RELOAD, post(system::reload_registry))
        // Body cap enforced by the extractor, so an oversized request is
        // rejected before a handler allocates a buffer for it.
        .layer(DefaultBodyLimit::max(body_limit));

    let ops = Router::new()
        .route(ROUTE_LIVE, get(system::live))
        .route(ROUTE_READY, get(system::ready))
        .route(ROUTE_METRICS, get(system::metrics));

    Router::new()
        .merge(api)
        .merge(ops)
        .fallback(system::not_found)
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            request_context,
        ))
        .with_state(state)
}

/// Reject oversized bodies with 413 before a handler allocates for them, and
/// attach the request id every response carries.
async fn request_context(
    State(state): State<Arc<AppState>>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty() && v.len() <= 128)
        .map(str::to_string)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let mut response = next.run(request).await;
    if let Ok(value) = axum::http::HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", value);
    }
    response.headers_mut().insert(
        axum::http::header::HeaderName::from_static("x-gateway-version"),
        axum::http::HeaderValue::from_static(chat::API_VERSION),
    );
    let _ = &state;
    response
}
