//! HTTP surface: the SSE chat endpoint plus operational endpoints.

pub mod chat;
pub mod system;

use std::sync::Arc;

use axum::{
    Json,
    extract::{DefaultBodyLimit, State},
    http::{HeaderName, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use serde_json::json;

use crate::state::AppState;

pub const ROUTE_CHAT_STREAM: &str = "/v1/chat/stream";
pub const ROUTE_CHAT_COMPLETE: &str = "/v1/chat/complete";
pub const ROUTE_MODELS: &str = "/v1/models";
pub const ROUTE_USAGE: &str = "/v1/usage";
pub const ROUTE_RELOAD: &str = "/v1/admin/registry/reload";
pub const ROUTE_DECISIONS: &str = "/v1/admin/decisions";
pub const ROUTE_LIVE: &str = "/health/live";
pub const ROUTE_READY: &str = "/health/ready";
pub const ROUTE_METRICS: &str = "/metrics";

/// The public listener: what tenants call.
///
/// With no `ops_port`, the operator surface shares this port: `/metrics`
/// requires the `admin` scope, since it names every tenant's spend, and the
/// admin reload is here too. With an `ops_port`, both move to
/// [`ops_router`] and do not exist on this port at all.
pub fn router(state: Arc<AppState>) -> Router {
    let body_limit = state.settings.server.request_body_limit_bytes;
    let separate_ops = state.settings.server.ops_port.is_some();

    let mut api = Router::new()
        .route(ROUTE_CHAT_STREAM, post(chat::chat_stream))
        .route(ROUTE_CHAT_COMPLETE, post(chat::chat_complete))
        .route(ROUTE_MODELS, get(system::list_models))
        .route(ROUTE_USAGE, get(system::get_usage));
    if !separate_ops {
        api = api
            .route(ROUTE_RELOAD, post(system::reload_registry))
            .route(ROUTE_DECISIONS, get(system::list_decisions));
    }
    // Body cap enforced by the extractor, so an oversized request is rejected
    // before a handler allocates a buffer for it.
    let api = api.layer(DefaultBodyLimit::max(body_limit));

    let mut ops = health_routes();
    if state.settings.server.metrics_enabled && !separate_ops {
        ops = ops.route(ROUTE_METRICS, get(system::metrics_for_admins));
    }

    finish(Router::new().merge(api).merge(ops), state)
}

/// The ops listener, served only when `server.ops_port` is set: health,
/// an unauthenticated `/metrics` for the scraper, and the admin reload (which
/// still requires the `admin` scope). Network placement is the access control
/// for the scrape, so bind it where tenants cannot reach.
pub fn ops_router(state: Arc<AppState>) -> Router {
    let mut ops = health_routes()
        .route(ROUTE_RELOAD, post(system::reload_registry))
        .route(ROUTE_DECISIONS, get(system::list_decisions));
    if state.settings.server.metrics_enabled {
        ops = ops.route(ROUTE_METRICS, get(system::metrics));
    }
    finish(ops, state)
}

fn health_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(ROUTE_LIVE, get(system::live))
        .route(ROUTE_READY, get(system::ready))
}

fn finish(routes: Router<Arc<AppState>>, state: Arc<AppState>) -> Router {
    routes
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

    // Reject an oversized *declared* body before any handler starts reading it:
    // a client can declare 2 MiB in `content-length` and ship nothing, and
    // neither the `DefaultBodyLimit` layer nor the dispatch backstop can answer
    // 413 until bytes actually arrive. The length header alone decides it, so
    // the connection is closed instead of waiting on data we will never accept.
    let declared_too_large = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|len| len > state.settings.server.request_body_limit_bytes as u64);

    if declared_too_large {
        let mut response: Response = (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({
                "error": {
                    "code": "payload_too_large",
                    "type": "request_error",
                    "message": "request body exceeds the configured limit",
                    "retryable": false,
                },
                "request_id": request_id,
            })),
        )
            .into_response();
        if let Ok(value) = HeaderValue::from_str(&request_id) {
            response.headers_mut().insert("x-request-id", value);
        }
        response.headers_mut().insert(
            HeaderName::from_static("x-gateway-version"),
            HeaderValue::from_static(chat::API_VERSION),
        );
        return response;
    }

    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", value);
    }
    response.headers_mut().insert(
        HeaderName::from_static("x-gateway-version"),
        HeaderValue::from_static(chat::API_VERSION),
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
        // Each call gets its own directory: the helper used one
        // `llm-gateway-router-{pid}` dir for every test, and parallel tests
        // raced the trailing `remove_dir_all` against the next `fs::write`.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "llm-gateway-router-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
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

        let on = ops_router(build_state(true));
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
        let off = ops_router(build_state(false));
        let response = off.oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// On the public port `/metrics` names every tenant's spend, so it is an
    /// admin operation: no key is a 401, a tenant key without `admin` a 403.
    #[tokio::test]
    async fn public_metrics_require_the_admin_scope() {
        let app = router(build_state(true));
        let anonymous = Request::builder().uri(ROUTE_METRICS).body(Body::empty()).unwrap();
        let response = app.clone().oneshot(anonymous).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let tenant = Request::builder()
            .uri(ROUTE_METRICS)
            .header("x-api-key", "secret")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(tenant).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    /// With an ops listener configured, the operator routes leave the public
    /// port entirely: not a 401 to probe, a 404.
    #[tokio::test]
    async fn an_ops_port_removes_operator_routes_from_the_public_port() {
        let state = build_state(true);
        let mut settings = (*state.settings).clone();
        settings.server.ops_port = Some(0);
        let state = Arc::new(AppState {
            settings: Arc::new(settings),
            ..Arc::try_unwrap(state).ok().expect("sole owner")
        });
        let public = router(Arc::clone(&state));
        for (method, uri) in [("GET", ROUTE_METRICS), ("POST", ROUTE_RELOAD)] {
            let request = Request::builder().method(method).uri(uri).body(Body::empty()).unwrap();
            let response = public.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {uri}");
        }
        let ops = ops_router(state);
        let request = Request::builder().method("POST").uri(ROUTE_RELOAD).body(Body::empty()).unwrap();
        let response = ops.oneshot(request).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "the ops listener still requires an admin key to reload"
        );
    }

    /// An oversized declared `content-length` must be a 413 without the body
    /// ever being read. The raw-socket integration test for this used to hang:
    /// nothing in the extractor path answered until bytes actually arrived.
    #[tokio::test]
    async fn an_oversized_declared_body_is_rejected_without_reading() {
        let app = router(build_state(true));
        let request = Request::builder()
            .method("POST")
            .uri(ROUTE_CHAT_STREAM)
            .header("content-type", "application/json")
            .header("content-length", (2 * 1024 * 1024).to_string())
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    /// A body under the limit still reaches the handler: the middleware must
    /// not intercept legitimately-sized requests.
    #[tokio::test]
    async fn a_normal_sized_body_still_reaches_the_handler() {
        let app = router(build_state(true));
        let payload = r#"{"model":"groq/gpt-oss-20b","messages":[{"role":"user","content":"hi"}],"max_tokens":16}"#;
        let request = Request::builder()
            .method("POST")
            .uri(ROUTE_CHAT_STREAM)
            .header("content-type", "application/json")
            .header("content-length", payload.len().to_string())
            .body(Body::from(payload))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert!(
            response.status() != StatusCode::PAYLOAD_TOO_LARGE,
            "an in-limit body must not be pre-rejected"
        );
    }
}
