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

    let mut ops = Router::new()
        .route(ROUTE_LIVE, get(system::live))
        .route(ROUTE_READY, get(system::ready));
    if state.settings.server.metrics_enabled {
        ops = ops.route(ROUTE_METRICS, get(system::metrics));
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn build_state(metrics_enabled: bool) -> Arc<AppState> {
        let dir = std::env::temp_dir().join(format!("llm-gateway-router-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let tenants_path = dir.join("tenants.yaml");
        std::fs::write(
            &tenants_path,
            "tenants:\n  - tenant_id: alpha\n    enabled: true\n    credentials:\n      - key_id: k1\n        key: \"secret\"\n        scopes: [chat:stream]\n    allowed_models: [\"*\"]\n    default_model: groq/gpt-oss-20b\n",
        )
        .unwrap();

        let mut settings = crate::config::Settings::default();
        settings.server.metrics_enabled = metrics_enabled;
        settings.registry.tenants_path = tenants_path;
        let settings = Arc::new(settings);

        let models = crate::router::ModelRegistry::load("config/models.yaml", false, 0).unwrap();
        let tenants =
            crate::governance::policy::TenantRegistry::load(&settings.registry.tenants_path)
                .unwrap();
        let auth =
            crate::governance::auth::build_authenticator(&settings.auth, tenants.key_records())
                .unwrap();
        let _ = std::fs::remove_dir_all(&dir);

        Arc::new(AppState {
            settings,
            models,
            tenants: std::sync::RwLock::new(tenants),
            auth: std::sync::RwLock::new(auth),
            policy: crate::governance::policy::PolicyEngine::new(
                true,
                crate::config::DEFAULT_LIMITS,
            ),
            providers: crate::router::ProviderPool::new(vec![]),
            limits: crate::governance::limits::LimitEngine::new(8, false),
            metrics: crate::observability::Metrics::new(std::time::Instant::now()),
            started_at: std::time::Instant::now(),
            inflight: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    /// `metrics_enabled` is a deployment decision about whether the scrape
    /// endpoint exists at all. Disabled means the route is not mounted — a 404,
    /// not a 200 with an empty body that a scraper then has to interpret.
    #[tokio::test]
    async fn metrics_route_is_mounted_only_when_enabled() {
        let request = || {
            Request::builder()
                .uri(ROUTE_METRICS)
                .body(Body::empty())
                .unwrap()
        };

        let on = router(build_state(true));
        let response = on.oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("gw_requests_total"),
            "a scraped /metrics must carry the gateway counters"
        );

        let off = router(build_state(false));
        let response = off.oneshot(request()).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "metrics disabled means no such route"
        );
    }
}
