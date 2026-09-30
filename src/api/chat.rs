//! `POST /v1/chat/stream` — the hot path.
//!
//! Two framings, both SSE, chosen per request by the `x-gateway-framing`
//! header or a `framing` body field:
//!
//! * **`normalized`** (default) — provider frames are decoded and re-emitted
//!   under the gateway's own versioned contract. This is what makes behaviour
//!   identical across providers, keeps reasoning out of `token`, lets the
//!   gateway bill per tenant, and insulates clients from vendor wire changes.
//!   Cost: one decode + re-serialize per frame. `benches/` measures it.
//!
//! * **`passthrough`** — upstream bytes are relayed verbatim, never parsed.
//!   Chunk boundaries are transport boundaries, so only this mode guarantees
//!   the gateway cannot alter framing. Cost: nothing per frame. Trade: the
//!   gateway cannot see inside the stream, so per-token budgets cannot be
//!   enforced mid-stream and cost is settled from the pre-flight reservation.
//!
//! Both run identical governance *before* the first byte is written, and both
//! propagate client cancellation upstream (dropping the body drops the reqwest
//! response, which closes the connection).
//!
//! `POST /v1/chat/complete` is the non-streaming sibling: the same request
//! body, the same governance, one JSON answer. See [`chat_complete`].

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use axum::{
    body::Body,
    extract::State,
    http::{
        header::{CACHE_CONTROL, CONTENT_TYPE},
        HeaderMap, HeaderName, HeaderValue, StatusCode,
    },
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Json,
};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::{
    config::OverflowMode,
    governance::{
        auth::Principal, ledger, limits::AdmitRequest, limits::CostEstimate, limits::Reservation,
        AuthorizeError, LimitError,
    },
    observability::{logging, metrics::RejectionKind, RequestSpan, StreamSummary},
    providers::{
        ChatMessage, ChatProvider, Completion, ProviderError, ProviderRequest, StreamEvent, Usage,
    },
    router::{self, estimate_cost_nano_usd, estimate_prompt_tokens, merge_params, validate_params},
    state::AppState,
};

/// Bumped only for a breaking change to the normalized frame contract.
pub const API_VERSION: &str = "v1";

/// Which endpoint a request arrived on. Governance is identical; only the
/// transport of the answer differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Stream,
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Framing {
    Normalized,
    Passthrough,
}

impl Framing {
    fn parse(value: Option<&str>) -> Framing {
        match value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("passthrough" | "raw" | "relay") => Framing::Passthrough,
            _ => Framing::Normalized,
        }
    }
}

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: Option<bool>,
    /// Known parameters plus arbitrary vendor passthrough. Unknown keys are
    /// forwarded upstream, so a new provider parameter needs no gateway release.
    #[serde(default)]
    pub params: Option<Map<String, Value>>,
    #[serde(default)]
    pub framing: Option<String>,
    /// Echoed in the `start` event, never interpreted.
    #[serde(default)]
    pub metadata: Option<Map<String, Value>>,
}

#[derive(Debug, thiserror::Error)]
enum RequestError {
    #[error("`model` is required")]
    MissingModel,
    #[error("`messages` must contain at least one message")]
    NoMessages,
    #[error("`stream` must be true on /v1/chat/stream")]
    StreamRequired,
    #[error("`stream` must be false or absent on /v1/chat/complete; use /v1/chat/stream to stream")]
    StreamNotAllowed,
    #[error("`params.max_tokens` is required so the gateway can reserve a cost budget")]
    MaxTokensRequired,
    #[error("request body is not valid JSON: {0}")]
    BadJson(String),
    #[error("`Idempotency-Key` must be 1 to 255 visible ASCII characters")]
    InvalidIdempotencyKey,
    #[error("this tenant requires an `Idempotency-Key` header on every request")]
    IdempotencyKeyRequired,
    /// Body exceeded `server.request_body_limit_bytes`.
    #[error("request body is too large")]
    PayloadTooLarge,
    #[error("{0}")]
    Auth(#[from] crate::governance::AuthError),
    #[error("{0}")]
    Policy(#[from] AuthorizeError),
    #[error("{0}")]
    Limit(#[from] LimitError),
    #[error("{0}")]
    Model(#[from] router::ResolveError),
    #[error("{0}")]
    Param(#[from] router::ParamViolation),
    #[error("{0}")]
    Provider(#[from] ProviderError),
}

impl RequestError {
    fn status(&self) -> StatusCode {
        match self {
            RequestError::MissingModel
            | RequestError::NoMessages
            | RequestError::StreamRequired
            | RequestError::StreamNotAllowed
            | RequestError::MaxTokensRequired
            | RequestError::BadJson(_)
            | RequestError::InvalidIdempotencyKey
            | RequestError::IdempotencyKeyRequired
            | RequestError::Param(_) => StatusCode::BAD_REQUEST,
            RequestError::Limit(LimitError::LedgerUnavailable) => StatusCode::SERVICE_UNAVAILABLE,
            RequestError::Limit(LimitError::IdempotencyKeyReused) => StatusCode::UNPROCESSABLE_ENTITY,
            RequestError::Limit(LimitError::RequestInProgress { .. } | LimitError::DuplicateRequest { .. }) => {
                StatusCode::CONFLICT
            }
            RequestError::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            RequestError::Auth(_) => StatusCode::UNAUTHORIZED,
            RequestError::Policy(_) => StatusCode::FORBIDDEN,
            RequestError::Limit(
                LimitError::RateLimited { .. }
                | LimitError::TokenRateLimited { .. }
                | LimitError::ConcurrencyLimited { .. },
            ) => StatusCode::TOO_MANY_REQUESTS,
            RequestError::Limit(
                LimitError::BudgetExhausted { .. } | LimitError::BudgetWouldBeExceeded { .. },
            ) => StatusCode::PAYMENT_REQUIRED,
            RequestError::Limit(_) => StatusCode::BAD_REQUEST,
            RequestError::Model(router::ResolveError::NotFound { .. } | router::ResolveError::UnknownProvider { .. }) => {
                StatusCode::NOT_FOUND
            }
            RequestError::Model(_) => StatusCode::BAD_REQUEST,
            RequestError::Provider(_) => StatusCode::BAD_GATEWAY,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            RequestError::MissingModel => "missing_model",
            RequestError::NoMessages => "no_messages",
            RequestError::StreamRequired => "stream_required",
            RequestError::StreamNotAllowed => "stream_not_allowed",
            RequestError::MaxTokensRequired => "max_tokens_required",
            RequestError::BadJson(_) => "invalid_json",
            RequestError::InvalidIdempotencyKey => "invalid_idempotency_key",
            RequestError::IdempotencyKeyRequired => "idempotency_key_required",
            RequestError::Limit(LimitError::LedgerUnavailable) => "ledger_unavailable",
            RequestError::Limit(LimitError::IdempotencyKeyReused) => "idempotency_key_reused",
            RequestError::Limit(LimitError::RequestInProgress { .. }) => "request_in_progress",
            RequestError::Limit(LimitError::DuplicateRequest { .. }) => "duplicate_request",
            RequestError::PayloadTooLarge => "payload_too_large",
            RequestError::Auth(_) => "unauthorized",
            RequestError::Policy(
                AuthorizeError::ModelNotAllowed { .. } | AuthorizeError::ModelDenied { .. },
            ) => "model_not_allowed",
            RequestError::Policy(_) => "forbidden",
            RequestError::Limit(LimitError::RateLimited { .. } | LimitError::TokenRateLimited { .. }) => {
                "rate_limited"
            }
            RequestError::Limit(LimitError::ConcurrencyLimited { .. }) => "concurrency_limited",
            RequestError::Limit(
                LimitError::BudgetExhausted { .. } | LimitError::BudgetWouldBeExceeded { .. },
            ) => "budget_exhausted",
            RequestError::Limit(_) => "limit_exceeded",
            RequestError::Model(_) => "invalid_model",
            RequestError::Param(_) => "invalid_parameter",
            RequestError::Provider(e) => e.code(),
        }
    }

    fn rejection_kind(&self) -> Option<RejectionKind> {
        match self {
            RequestError::Auth(_) => Some(RejectionKind::Auth),
            RequestError::Policy(_) => Some(RejectionKind::Policy),
            RequestError::Limit(LimitError::RateLimited { .. } | LimitError::TokenRateLimited { .. }) => {
                Some(RejectionKind::RateLimit)
            }
            RequestError::Limit(LimitError::ConcurrencyLimited { .. }) => Some(RejectionKind::Concurrency),
            RequestError::Limit(
                LimitError::BudgetExhausted { .. } | LimitError::BudgetWouldBeExceeded { .. },
            ) => Some(RejectionKind::Budget),
            RequestError::Model(_) => Some(RejectionKind::Model),
            _ => None,
        }
    }

    /// Whether a client retry could plausibly succeed.
    fn retryable(&self) -> bool {
        match self {
            RequestError::Provider(e) => e.retryable(),
            RequestError::Limit(LimitError::LedgerUnavailable | LimitError::RequestInProgress { .. }) => true,
            _ => false,
        }
    }

    /// Seconds a client should wait before retrying, when there is a known
    /// answer.
    fn retry_after(&self) -> Option<&'static str> {
        match self {
            RequestError::Limit(LimitError::RateLimited { .. } | LimitError::TokenRateLimited { .. }) => Some("60"),
            RequestError::Limit(LimitError::LedgerUnavailable) => Some("5"),
            RequestError::Limit(LimitError::RequestInProgress { .. }) => Some("1"),
            _ => None,
        }
    }

    fn error_type(&self) -> &'static str {
        match self.status() {
            StatusCode::BAD_REQUEST => "invalid_request_error",
            StatusCode::UNAUTHORIZED => "authentication_error",
            StatusCode::FORBIDDEN => "permission_error",
            StatusCode::NOT_FOUND => "not_found_error",
            StatusCode::TOO_MANY_REQUESTS => "rate_limit_error",
            StatusCode::PAYMENT_REQUIRED => "billing_error",
            StatusCode::CONFLICT => "conflict_error",
            StatusCode::UNPROCESSABLE_ENTITY => "invalid_request_error",
            _ => "api_error",
        }
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `POST /v1/chat/stream`. Extractors run in order, so the header map is
/// taken before the body.
#[axum::debug_handler]
pub async fn chat_stream(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle(state, headers, body, Mode::Stream).await
}

/// `POST /v1/chat/complete`: the same request body and the same governance
/// as the stream endpoint, answered with one JSON document. `framing` is
/// ignored here; there is no stream to frame.
///
/// Billing follows the stream path's rules, with one difference forced by
/// the transport: a client that disconnects while waiting is billed the full
/// reservation, because a non-streaming upstream typically finishes, and
/// bills, the whole answer regardless, and the gateway never sees its usage.
#[axum::debug_handler]
pub async fn chat_complete(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle(state, headers, body, Mode::Complete).await
}

async fn handle(state: Arc<AppState>, headers: HeaderMap, body: Bytes, mode: Mode) -> Response {
    let request_id = Uuid::new_v4().to_string();
    let mut span = RequestSpan::new(request_id.clone());
    state.metrics.request_observed();

    // Returns `Response` rather than `Result<Response, RequestError>` so every
    // rejection is a fully-formed response with its own headers, built in one
    // place below.
    let outcome = match admit(&state, &headers, &body, &mut span, &request_id, mode).await {
        Err(err) => Err(err),
        Ok(admitted) => match (mode, admitted.framing) {
            (Mode::Complete, _) => complete_response(&state, admitted, &span).await,
            (Mode::Stream, Framing::Passthrough) => passthrough_response(&state, admitted, &span).await,
            (Mode::Stream, Framing::Normalized) => normalized_response(&state, admitted, &mut span).await,
        },
    };
    match outcome {
        Ok(response) => response,
        // A repeat of a completed completion is answered from the ledger:
        // the stored answer, with no provider call and no new charge.
        Err(RequestError::Limit(LimitError::DuplicateRequest {
            original,
            response: Some(stored),
            ..
        })) if mode == Mode::Complete => {
            tracing::info!(
                request_id = %request_id,
                tenant = %span.tenant_id,
                original = %original,
                "idempotent replay served from the ledger"
            );
            let mut response = (StatusCode::OK, stored).into_response();
            let h = response.headers_mut();
            h.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            h.insert("x-request-id", header_value(&request_id));
            h.insert("idempotent-replayed", HeaderValue::from_static("true"));
            h.insert("x-original-request-id", header_value(&original.to_string()));
            response
        }
        Err(err) => {
            if let Some(kind) = err.rejection_kind() {
                state.metrics.rejection(kind);
            }
            logging::log_rejected(&request_id, &span.tenant_id, err.code(), &err.to_string());
            record_decision(&state, &span, &request_id, mode, &err);

            let status = err.status();
            let mut body = json!({
                "error": {
                    "code": err.code(),
                    "type": err.error_type(),
                    "message": err.to_string(),
                    "retryable": err.retryable(),
                },
                "request_id": request_id,
            });
            // Point a client at the attempt that holds its idempotency key.
            match &err {
                RequestError::Limit(LimitError::DuplicateRequest { original, billed_nano_usd, .. }) => {
                    body["original_request_id"] = json!(original.to_string());
                    body["billed_nano_usd"] = json!(billed_nano_usd);
                }
                RequestError::Limit(LimitError::RequestInProgress { original: Some(original) }) => {
                    body["original_request_id"] = json!(original.to_string());
                }
                _ => {}
            }
            let mut response = (status, Json(body)).into_response();
            let h = response.headers_mut();
            h.insert("x-request-id", header_value(&request_id));
            h.insert(
                "x-error-retryable",
                HeaderValue::from_static(if err.retryable() { "true" } else { "false" }),
            );
            if let Some(seconds) = err.retry_after() {
                h.insert("retry-after", HeaderValue::from_static(seconds));
            }
            response
        }
    }
}

/// Keep a record of why this known caller's request did not happen. Only
/// after authentication: `span.tenant_id` is empty until then, and anonymous
/// refusals are never persisted.
fn record_decision(state: &AppState, span: &RequestSpan, request_id: &str, mode: Mode, err: &RequestError) {
    use crate::governance::ledger::decisions::{Decision, DecisionKind};
    if span.tenant_id.is_empty() || matches!(err, RequestError::Auth(_)) {
        return;
    }
    // A parser's message can quote the offending value, which may be prompt
    // text; the record keeps a fixed sentence instead.
    let reason = match err {
        RequestError::BadJson(_) => "request body is not valid JSON".to_string(),
        other => other.to_string(),
    };
    let decision = Decision {
        request_id: Uuid::parse_str(request_id).unwrap_or_else(|_| Uuid::nil()),
        tenant_id: span.tenant_id.clone(),
        key_id: span.key_id.clone(),
        kind: if err.status().is_client_error() {
            DecisionKind::Refused
        } else {
            DecisionKind::Failed
        },
        endpoint: match mode {
            Mode::Stream => "stream",
            Mode::Complete => "complete",
        }
        .to_string(),
        model: (!span.model.is_empty()).then(|| span.model.clone()),
        code: err.code().to_string(),
        reason: String::new(),
    }
    .with_reason(&reason);
    state.limits.ledger().record_decision(decision);
}

/// The request's `Idempotency-Key`, if it sent one.
fn idempotency_key(headers: &HeaderMap) -> Result<Option<&str>, RequestError> {
    let Some(raw) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let key = raw.to_str().map_err(|_| RequestError::InvalidIdempotencyKey)?;
    if !ledger::valid_idempotency_key(key) {
        return Err(RequestError::InvalidIdempotencyKey);
    }
    Ok(Some(key))
}

/// Everything the stream needs after governance has admitted the request.
struct Admitted {
    principal: Principal,
    resolved: router::ResolvedModel,
    provider: Arc<dyn ChatProvider>,
    request: ProviderRequest,
    reservation: Reservation,
    prompt_tokens: u32,
    max_output_tokens: u32,
    framing: Framing,
    metadata: Option<Map<String, Value>>,
}

/// Everything before the upstream connection: authenticate, parse, authorize,
/// resolve, merge parameters, and reserve cost. Shared by both endpoints, so
/// a governance rule cannot apply to one and not the other.
async fn admit(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    body: &Bytes,
    span: &mut RequestSpan,
    request_id: &str,
    mode: Mode,
) -> Result<Admitted, RequestError> {
    // 0. Body cap, checked before any parsing or authentication work. The
    //    extractor's own limit returns 413 without reaching the handler, so
    //    this is the backstop for a limit configured below the buffer size.
    if body.len() > state.settings.server.request_body_limit_bytes {
        return Err(RequestError::PayloadTooLarge);
    }

    // 1. Authenticate.
    let principal = state.current_auth().authenticate(headers).await?;
    span.tenant_id = principal.tenant_id.to_string();
    span.key_id = principal.key_id.to_string();
    span.scheme = principal.scheme.to_string();

    // 2. Parse. Bounded upstream by the body-limit layer.
    let req: ChatRequest = serde_json::from_slice(body)
        .map_err(|e| RequestError::BadJson(e.to_string()))?;
    if req.model.trim().is_empty() {
        return Err(RequestError::MissingModel);
    }
    if req.messages.is_empty() {
        return Err(RequestError::NoMessages);
    }
    match (mode, req.stream) {
        (Mode::Stream, Some(false)) => return Err(RequestError::StreamRequired),
        (Mode::Complete, Some(true)) => return Err(RequestError::StreamNotAllowed),
        _ => {}
    }
    let idempotency_key = idempotency_key(headers)?;

    // 3. Authorize: may this tenant call this model, at this size?
    let prompt_chars: usize = req.messages.iter().map(ChatMessage::approx_chars).sum();
    let tenants = state.current_tenants();
    let decision = state.policy.authorize(
        &principal,
        &tenants,
        &req.model,
        req.messages.len(),
        prompt_chars,
    )?;

    // 4. Resolve against the allowlisted registry. A client-supplied endpoint is
    //    never honoured; the URL comes from `models.yaml`.
    let snapshot = state.models.snapshot().await;
    let resolved =
        snapshot.resolve_supported(&decision.qualified_model, &state.providers.names())?;
    span.model = resolved.registry_id.clone();
    span.provider = resolved.provider().to_string();
    span.upstream_model = resolved.upstream_model.clone();

    // 5. Merge parameters. Precedence, lowest to highest:
    //    registry defaults < caller request < tenant policy.
    //    Policy sits on top deliberately: a tenant that pins
    //    `temperature: 0.2` in `tenants.yaml` is expressing a governance
    //    constraint, and a governance constraint a caller can override is not
    //    one. Use a `default_model` or a registry `default_params` for a
    //    preference rather than a policy.
    let mut params = merge_params(&resolved.config.default_params, req.params.as_ref());
    if let Some(policy_params) = decision.tenant.model_params.get(&resolved.registry_id) {
        for (key, value) in policy_params {
            params.set_from_json(key, value);
        }
    }

    // A model may set `max_tokens` in the registry, and a tenant policy may
    // too, so the effective value is not simply "did the caller send one".
    // If nothing at any layer set it, refuse: an uncapped request cannot have
    // its cost reserved.
    let requested_output = params.max_tokens.ok_or(RequestError::MaxTokensRequired)?;
    // `0` means the tenant set no ceiling, so nothing to clamp to.
    let ceiling = decision.limits.max_output_tokens;
    let max_output_tokens = if ceiling > 0 && requested_output > ceiling {
        match state.settings.governance.on_max_tokens_exceeded {
            OverflowMode::Clamp => {
                tracing::warn!(
                    request_id = %request_id,
                    tenant = %principal.tenant_id,
                    requested = requested_output,
                    ceiling,
                    "clamping max_tokens to the tenant ceiling"
                );
                ceiling
            }
            OverflowMode::Reject => {
                return Err(LimitError::OutputTokensExceeded {
                    requested: requested_output,
                    limit: ceiling,
                }
                .into());
            }
        }
    } else {
        requested_output
    };
    params.max_tokens = Some(max_output_tokens);
    validate_params(&params)?;

    // 6. Admit against quotas, reserving cost before the connection opens so a
    //    tenant cannot overspend by opening many streams at once.
    //    The provider is resolved first: a routing failure is a config error,
    //    and must not cost the tenant anything.
    let provider = router::provider_for(&resolved, &state.providers)?.clone();
    let prompt_tokens = estimate_prompt_tokens(&req.messages);
    let cost = CostEstimate {
        prompt_nano_usd: estimate_cost_nano_usd(&resolved.config.cost, prompt_tokens, 0),
        completion_nano_usd: estimate_cost_nano_usd(&resolved.config.cost, 0, max_output_tokens),
    };
    //    A repeated Idempotency-Key is decided here too, by the ledger, in the
    //    same step as the budget: a duplicate never reaches the provider.
    if decision.tenant.require_idempotency_key && idempotency_key.is_none() {
        return Err(RequestError::IdempotencyKeyRequired);
    }
    let idempotency = idempotency_key.map(|key| {
        let scope = match mode {
            Mode::Stream => "stream",
            Mode::Complete => "complete",
        };
        // The body already parsed as a `ChatRequest`, so it is valid JSON.
        let value: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        state.limits.fingerprinter().claim(key, scope, &value)
    });
    let reservation = state
        .limits
        .admit_request(AdmitRequest {
            id: Uuid::parse_str(request_id).unwrap_or_else(|_| Uuid::new_v4()),
            tenant_id: &principal.tenant_id,
            limits: &decision.limits,
            prompt_tokens,
            max_output_tokens,
            estimate: cost,
            idempotency,
        })
        .await?;

    let framing = match &req.framing {
        Some(explicit) => Framing::parse(Some(explicit)),
        None => Framing::parse(headers.get("x-gateway-framing").and_then(|v| v.to_str().ok())),
    };

    let request = ProviderRequest {
        request_id: Uuid::parse_str(request_id).unwrap_or_else(|_| Uuid::new_v4()),
        tenant_id: Arc::clone(&principal.tenant_id),
        key_id: Arc::clone(&principal.key_id),
        upstream_model: resolved.upstream_model.clone(),
        endpoint: resolved.endpoint.clone(),
        messages: Arc::new(req.messages),
        params,
        include_usage: state.settings.upstream.request_usage,
    };

    Ok(Admitted {
        principal,
        resolved,
        provider,
        request,
        reservation,
        prompt_tokens,
        max_output_tokens,
        framing,
        metadata: req.metadata,
    })
}

// ---------------------------------------------------------------------------
// Normalized framing
// ---------------------------------------------------------------------------

async fn normalized_response(
    state: &Arc<AppState>,
    mut admitted: Admitted,
    span: &mut RequestSpan,
) -> Result<Response, RequestError> {
    let provider = admitted.resolved.provider().to_string();
    let model = admitted.resolved.registry_id.clone();

    // The upstream must accept the request before we count a stream: an
    // attempt that 401s produced no tokens and is not a stream.
    let upstream = match admitted.provider.stream_chat(admitted.request.clone()).await {
        Ok(stream) => {
            state.metrics.stream_started(&provider, &model);
            state
                .inflight
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            stream
        }
        Err(err) => {
            // No 2xx, so the upstream processed nothing and bills nothing.
            admitted.reservation.release();
            state.metrics.stream_failed_to_start(&provider, &model);
            return Err(err.into());
        }
    };

    let headers = response_headers(span, &admitted.resolved, &admitted.principal, &admitted.reservation);
    let stream = normalized_stream(NormalizedState {
        inner: upstream,
        span: span.clone(),
        cost_model: admitted.resolved.config.cost.clone(),
        metrics: Arc::clone(&state.metrics),
        inflight: Arc::clone(&state.inflight),
        model: admitted.resolved.registry_id.clone(),
        tenant: admitted.principal.tenant_id.to_string(),
        request_id: span.request_id.clone(),
        metadata: admitted.metadata.clone(),
        max_output_tokens: admitted.max_output_tokens,
        reservation: admitted.reservation,
        summary: StreamSummary {
            prompt_tokens: admitted.prompt_tokens,
            ..Default::default()
        },
        observed_completion_tokens: 0,
        usage_reported: false,
        first_content: false,
        emitted_start: false,
        finished: false,
        upstream_done: false,
        pending: Vec::new().into_iter(),
    });

    // axum's KeepAlive writes an SSE comment on an idle interval, which every
    // conformant client ignores. Doing it here rather than by polling keeps the
    // stream a plain `Stream`, so there is no busy-wake on the hot path.
    let sse = Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(state.settings.keep_alive_interval())
            .text("ping"),
    );

    let mut response = sse.into_response();
    *response.headers_mut() = merge_headers(response.headers(), headers);
    Ok(response)
}

fn response_headers(
    span: &RequestSpan,
    resolved: &router::ResolvedModel,
    principal: &Principal,
    reservation: &Reservation,
) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream; charset=utf-8"));
    // `no-transform` forbids an intermediary from buffering or re-encoding.
    h.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache, no-store, no-transform"));
    // nginx buffers SSE by default; this is the off switch.
    h.insert(HeaderName::from_static("x-accel-buffering"), HeaderValue::from_static("no"));
    h.insert(HeaderName::from_static("x-request-id"), header_value(&span.request_id));
    h.insert(HeaderName::from_static("x-model"), header_value(&resolved.registry_id));
    h.insert(HeaderName::from_static("x-provider"), header_value(resolved.provider()));
    h.insert(HeaderName::from_static("x-upstream-model"), header_value(&resolved.upstream_model));
    h.insert(HeaderName::from_static("x-gateway-version"), HeaderValue::from_static(API_VERSION));
    h.insert(HeaderName::from_static("x-tenant"), header_value(&principal.tenant_id));
    h.insert(
        HeaderName::from_static("x-reserved-nano-usd"),
        header_value(&reservation.reserved_nano_usd().to_string()),
    );
    h
}

fn merge_headers(existing: &HeaderMap, ours: HeaderMap) -> HeaderMap {
    let mut out = existing.clone();
    for (name, value) in ours.iter() {
        out.insert(name.clone(), value.clone());
    }
    out
}

fn header_value(v: &str) -> HeaderValue {
    HeaderValue::from_str(v).unwrap_or_else(|_| HeaderValue::from_static("invalid"))
}

/// State for the normalized stream. Owned by `futures::stream::unfold`, which
/// threads it through every step, so a `Pending` from the upstream resumes with
/// all accumulated state intact.
struct NormalizedState {
    inner: crate::providers::ProviderStream,
    span: RequestSpan,
    reservation: Reservation,
    cost_model: router::CostModel,
    metrics: crate::observability::SharedMetrics,
    /// Decremented when the stream object is dropped, which lets shutdown
    /// drain by counting live streams instead of sleeping a fixed amount.
    inflight: std::sync::Arc<std::sync::atomic::AtomicU64>,
    model: String,
    tenant: String,
    request_id: String,
    metadata: Option<Map<String, Value>>,
    max_output_tokens: u32,
    summary: StreamSummary,
    /// Completion tokens estimated from the deltas actually delivered. Billed
    /// when the upstream never reports usage: a stream cut at 99% must not
    /// cost the same as one cut before its first token.
    observed_completion_tokens: u32,
    usage_reported: bool,
    first_content: bool,
    emitted_start: bool,
    finished: bool,
    /// Set once the terminal sequence is queued, so the upstream is never
    /// polled again. `unfold` panics if polled after returning `None`, and
    /// "more client frames to send" is not the same as "upstream has data".
    upstream_done: bool,
    /// Client frames produced by one upstream frame but not yet yielded. A
    /// single upstream chunk can carry content, reasoning and a tool call.
    pending: std::vec::IntoIter<Event>,
}

impl NormalizedState {
    /// Queue the closing sequence: an optional extra frame (an `error`, say),
    /// then `end`, then `[DONE]`. Idempotent, so a stream cannot emit two
    /// terminators.
    fn emit_terminal(&mut self, extra: Option<Event>) {
        if self.finished {
            return;
        }
        self.finished = true;

        let cost = self.settle();

        let mut out: Vec<Event> = Vec::with_capacity(3);
        if let Some(event) = extra {
            out.push(event);
        }
        out.push(end_event(
            &self.summary,
            cost,
            self.span.elapsed_ms(),
            self.span.ttft_ms(),
        ));
        out.push(Event::default().data("[DONE]"));
        self.pending = out.into_iter();
        self.emit_summary();
    }

    /// Settle the reservation against the best usage figure available and
    /// return the cost billed. Reported usage wins; without it, the
    /// completion is the estimate from delivered deltas, capped at the output
    /// ceiling the tenant reserved for.
    fn settle(&mut self) -> u64 {
        if !self.usage_reported {
            self.summary.completion_tokens =
                self.observed_completion_tokens.min(self.max_output_tokens);
        }
        let usage = self.current_usage();
        self.reservation.settle(usage, &self.cost_model);
        let cost = estimate_cost_nano_usd(
            &self.cost_model,
            self.summary.prompt_tokens,
            self.summary.completion_tokens,
        );
        self.summary.cost_nano_usd = cost;
        cost
    }

    fn observe_output(&mut self, text: &str) {
        self.observed_completion_tokens = self
            .observed_completion_tokens
            .saturating_add(estimate_output_tokens(text));
    }

    fn apply_usage(&mut self, usage: Usage) {
        self.usage_reported = true;
        self.summary.prompt_tokens = usage.prompt_tokens.max(self.summary.prompt_tokens);
        self.summary.completion_tokens = usage.completion_tokens;
        self.summary.reasoning_tokens = usage.reasoning_tokens.unwrap_or(0);
        self.summary.cached_prompt_tokens = usage.cached_prompt_tokens.unwrap_or(0);
    }

    fn current_usage(&self) -> Usage {
        Usage {
            prompt_tokens: self.summary.prompt_tokens,
            completion_tokens: self.summary.completion_tokens,
            total_tokens: self.summary.prompt_tokens + self.summary.completion_tokens,
            reasoning_tokens: None,
            cached_prompt_tokens: None,
        }
    }

    /// The only observability on the hot path: one log line and one metrics
    /// update for the whole stream.
    fn emit_summary(&self) {
        logging::log_stream_complete(&self.span, &self.summary);
        self.metrics.stream_finished(
            &self.span.provider,
            &self.span.model,
            &self.tenant,
            &self.summary,
            self.span.elapsed_ms() as u64,
            self.span.ttft_ms().map(|v| v as u64),
        );
    }
}

type Frame = std::result::Result<Event, Infallible>;

/// Advance the state machine to the next client frame, or `None` when the
/// terminal sequence is fully drained.
async fn next_frame(state: &mut NormalizedState) -> Option<Frame> {
    loop {
        // Anything buffered from a previous upstream frame goes first.
        if let Some(event) = state.pending.next() {
            return Some(Ok(event));
        }
        if state.upstream_done {
            return None;
        }

        // A synthetic `start` frame ahead of the upstream's first event, so a
        // client can render metadata without waiting on the model.
        if !state.emitted_start {
            state.emitted_start = true;
            return Some(Ok(start_event(
                &state.request_id,
                &state.model,
                &state.tenant,
                state.max_output_tokens,
                state.metadata.as_ref(),
            )));
        }

        match state.inner.next().await {
            // The upstream ended without a terminal event. Settle against
            // whatever usage was seen and emit the normal closing sequence, so
            // a client never has to distinguish "finished" from "closed".
            None => {
                state.upstream_done = true;
                state.emit_terminal(None);
            }
            Some(Err(err)) => {
                // A failure after streaming began cannot change the status
                // code, so it is reported in-band on its own event and the
                // stream still terminates cleanly.
                state.upstream_done = true;
                state.summary.error_code = Some(err.code());
                state.summary.error_retryable = err.retryable();
                state.emit_terminal(Some(
                    Event::default()
                        .event("error")
                        .data(error_payload(&err, &state.request_id)),
                ));
            }
            Some(Ok(event)) => {
                state.summary.events += 1;
                state.metrics.delta_events(1);

                if let StreamEvent::Delta { content, .. } = &event
                    && content.is_some()
                    && !state.first_content
                {
                    state.first_content = true;
                    state.span.mark_first_token();
                }

                // A terminal event becomes the gateway's own `end` + `[DONE]`
                // pair, after usage and cost are settled, so the client never
                // sees a duplicate finish.
                if event.is_terminal() {
                    if let StreamEvent::Finished { finish_reason, usage } = &event {
                        if let Some(usage) = usage {
                            state.apply_usage(*usage);
                        }
                        state.summary.finish_reason.clone_from(finish_reason);
                    }
                    state.upstream_done = true;
                    state.emit_terminal(None);
                    continue;
                }

                // One upstream frame can carry content, reasoning and a tool
                // call at once, so build the full set before yielding one.
                let mut out: Vec<Event> = Vec::with_capacity(2);
                match &event {
                    StreamEvent::Started { upstream_id, upstream_model } => {
                        state.summary.upstream_id.clone_from(upstream_id);
                        state.summary.upstream_model.clone_from(upstream_model);
                        out.push(
                            Event::default()
                                .event("meta")
                                .data(meta_payload(upstream_id, upstream_model)),
                        );
                    }
                    StreamEvent::Delta { content, reasoning } => {
                        for text in [content, reasoning].into_iter().flatten() {
                            state.observe_output(text);
                        }
                        if let Some(token) = content {
                            out.push(Event::default().data(token_json(token)));
                        }
                        if let Some(think) = reasoning {
                            out.push(
                                Event::default()
                                    .event("reasoning")
                                    .data(reasoning_json(think)),
                            );
                        }
                    }
                    StreamEvent::ToolCallDelta { index, id, name, arguments } => {
                        for text in [name, arguments].into_iter().flatten() {
                            state.observe_output(text);
                        }
                        out.push(
                            Event::default()
                                .event("tool")
                                .data(tool_json(*index, id, name, arguments)),
                        );
                    }
                    StreamEvent::Finished { .. } => unreachable!("handled above"),
                }

                if out.is_empty() {
                    // Nothing client-visible in this frame.
                    continue;
                }
                let first = out.remove(0);
                if !out.is_empty() {
                    state.pending = out.into_iter();
                }
                return Some(Ok(first));
            }
        }
    }
}

/// The normalized SSE stream.
///
/// `unfold` owns the state, so a client disconnect drops it mid-`await` and the
/// `Drop` below still logs exactly one summary line.
impl Drop for NormalizedState {
    fn drop(&mut self) {
        // Reached only when the stream is dropped before finishing, which in
        // practice means the client disconnected. Dropping `inner` closes the
        // upstream connection. What was delivered is billed; the rest of the
        // completion reservation is refunded.
        if !self.finished {
            self.summary.aborted_by_client = true;
            self.settle();
            self.emit_summary();
        }
        self.inflight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

fn normalized_stream(state: NormalizedState) -> impl Stream<Item = Frame> + Send {
    futures_util::stream::unfold(state, |mut state| async move {
        // None ends the fold and drops the state, which by then has emitted
        // its summary and settled its reservation.
        next_frame(&mut state).await.map(|frame| (frame, state))
    })
}

// ---------------------------------------------------------------------------
// Frame encoders — the frozen v1 wire shapes
// ---------------------------------------------------------------------------

/// The `start` frame, sent before the upstream's first event so a client can
/// render metadata without waiting on the model.
fn start_event(
    request_id: &str,
    model: &str,
    tenant: &str,
    max_output_tokens: u32,
    metadata: Option<&Map<String, Value>>,
) -> Event {
    let payload = json!({
        "request_id": request_id,
        "model": model,
        "tenant": tenant,
        "max_output_tokens": max_output_tokens,
        "metadata": metadata,
    });
    Event::default().event("start").data(payload.to_string())
}

/// Upstream correlation ids, forwarded so a client can quote them to support.
fn meta_payload(upstream_id: &Option<String>, upstream_model: &Option<String>) -> String {
    json!({ "upstream_id": upstream_id, "upstream_model": upstream_model }).to_string()
}

/// A mid-stream failure. Carries a stable `code` and whether a retry could work,
/// so a client can branch without string matching.
fn error_payload(err: &ProviderError, request_id: &str) -> String {
    json!({
        "error": {
            "code": err.code(),
            "message": err.to_string(),
            "retryable": err.retryable(),
        },
        "request_id": request_id,
    })
    .to_string()
}

fn tool_json(index: u32, id: &Option<String>, name: &Option<String>, arguments: &Option<String>) -> String {
    json!({ "index": index, "id": id, "name": name, "arguments": arguments }).to_string()
}

/// The terminal `end` frame, carrying the numbers needed to reconcile a bill.
/// Always followed by `[DONE]`.
fn end_event(
    summary: &StreamSummary,
    cost_nano_usd: u64,
    duration_ms: u128,
    ttft_ms: Option<u128>,
) -> Event {
    let payload = json!({
        "finish_reason": summary.finish_reason,
        "usage": {
            "prompt_tokens": summary.prompt_tokens,
            "completion_tokens": summary.completion_tokens,
            "reasoning_tokens": summary.reasoning_tokens,
            "total_tokens": summary.prompt_tokens + summary.completion_tokens,
        },
        "cost_nano_usd": cost_nano_usd,
        "duration_ms": duration_ms,
        "ttft_ms": ttft_ms,
        "upstream_id": summary.upstream_id,
    });
    Event::default().event("end").data(payload.to_string())
}

/// `data: {"token": "..."}` — the v1 content frame, the only unnamed event
/// type in the contract.
///
/// Hand-built rather than `json!`: this is the hottest allocation in the
/// gateway, and a macro would build a `Value` tree only to serialise and drop
/// it. The escaping itself is delegated to `serde_json` so the output is exactly
/// JSON-conformant — a raw newline here would break SSE framing.
fn token_json(token: &str) -> String {
    let mut out = String::with_capacity(token.len() + 12);
    out.push_str("{\"token\":");
    push_json_string(token, &mut out);
    out.push('}');
    out
}

/// `event: reasoning` — chain of thought, kept strictly separate from `token`
/// so a caller cannot accidentally render it.
fn reasoning_json(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 16);
    out.push_str("{\"reasoning\":");
    push_json_string(text, &mut out);
    out.push('}');
    out
}

/// Tokens in a delivered delta, by the same chars-per-token ratio the prompt
/// estimate uses. At least one: an upstream frame carrying text carried a
/// token.
fn estimate_output_tokens(text: &str) -> u32 {
    if text.is_empty() {
        return 0;
    }
    ((text.chars().count() as f64) / 3.5).ceil().max(1.0) as u32
}

fn push_json_string(value: &str, out: &mut String) {
    match serde_json::to_string(value) {
        Ok(encoded) => out.push_str(&encoded),
        // Unreachable: `str` always serialises. Never emit invalid JSON.
        Err(_) => out.push_str("\"\""),
    }
}

// ---------------------------------------------------------------------------
// Non-streaming completion
// ---------------------------------------------------------------------------

/// Owns the reservation while the upstream call is in flight, so every way
/// the request can end closes it exactly once. If the handler future is
/// dropped mid-await (the client disconnected), `Drop` bills the full
/// reservation and records a client abort.
struct CompletionGuard {
    reservation: Reservation,
    span: RequestSpan,
    metrics: crate::observability::SharedMetrics,
    inflight: Arc<std::sync::atomic::AtomicU64>,
    cost_model: router::CostModel,
    tenant: String,
    max_output_tokens: u32,
    summary: StreamSummary,
    closed: bool,
}

impl CompletionGuard {
    fn record(&self, accepted: bool) {
        if accepted {
            self.metrics.stream_started(&self.span.provider, &self.span.model);
        }
        logging::log_stream_complete(&self.span, &self.summary);
        self.metrics.stream_finished(
            &self.span.provider,
            &self.span.model,
            &self.tenant,
            &self.summary,
            self.span.elapsed_ms() as u64,
            None,
        );
    }
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        if !self.closed {
            self.closed = true;
            self.summary.aborted_by_client = true;
            self.reservation.commit_reserved();
            self.summary.cost_nano_usd = estimate_cost_nano_usd(
                &self.cost_model,
                self.summary.prompt_tokens,
                self.max_output_tokens,
            );
            self.record(true);
        }
        self.inflight.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

async fn complete_response(
    state: &Arc<AppState>,
    admitted: Admitted,
    span: &RequestSpan,
) -> Result<Response, RequestError> {
    let headers = response_headers(span, &admitted.resolved, &admitted.principal, &admitted.reservation);
    // Counted as in flight for the whole wait, so a graceful shutdown drains
    // a completion the same way it drains a stream.
    state.inflight.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let mut guard = CompletionGuard {
        reservation: admitted.reservation,
        span: span.clone(),
        metrics: Arc::clone(&state.metrics),
        inflight: Arc::clone(&state.inflight),
        cost_model: admitted.resolved.config.cost.clone(),
        tenant: admitted.principal.tenant_id.to_string(),
        max_output_tokens: admitted.max_output_tokens,
        summary: StreamSummary {
            prompt_tokens: admitted.prompt_tokens,
            ..Default::default()
        },
        closed: false,
    };

    let completion = match admitted.provider.complete(admitted.request).await {
        Ok(completion) => completion,
        Err(err) => {
            guard.closed = true;
            guard.summary.error_code = Some(err.code());
            guard.summary.error_retryable = err.retryable();
            if err.accepted_by_upstream() {
                // Accepted, then failed while answering: the prompt was
                // consumed, the answer never arrived.
                guard.reservation.abandon();
                guard.summary.cost_nano_usd =
                    estimate_cost_nano_usd(&guard.cost_model, guard.summary.prompt_tokens, 0);
                guard.record(true);
            } else {
                // Refused or unreachable: nothing was processed.
                guard.reservation.release();
                state.metrics.stream_failed_to_start(&guard.span.provider, &guard.span.model);
            }
            return Err(err.into());
        }
    };

    guard.closed = true;
    let usage = billable_usage(&completion, guard.summary.prompt_tokens, guard.max_output_tokens);
    let cost = estimate_cost_nano_usd(&guard.cost_model, usage.prompt_tokens, usage.completion_tokens);

    let s = &mut guard.summary;
    s.prompt_tokens = usage.prompt_tokens;
    s.completion_tokens = usage.completion_tokens;
    s.reasoning_tokens = usage.reasoning_tokens.unwrap_or(0);
    s.cost_nano_usd = cost;
    s.finish_reason.clone_from(&completion.finish_reason);
    s.upstream_id.clone_from(&completion.upstream_id);
    s.upstream_model.clone_from(&completion.upstream_model);
    s.events = 1;
    let payload = completion_payload(&completion, &usage, cost, &guard, admitted.metadata.as_ref());
    // The answer goes into the ledger with the settlement, so a repeat under
    // the same Idempotency-Key is served from there.
    guard.reservation.keep_response(payload.clone());
    guard.reservation.settle(usage, &guard.cost_model);
    guard.summary.bytes_out = payload.len() as u64;
    state.metrics.bytes_to_client(payload.len() as u64);
    guard.record(true);

    let mut response = (StatusCode::OK, payload).into_response();
    let mut headers = headers;
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.remove(HeaderName::from_static("x-accel-buffering"));
    *response.headers_mut() = headers;
    Ok(response)
}

/// Usage to bill: what the upstream reported, else an estimate from the
/// answer itself, capped at the ceiling that was reserved, by the same rule
/// as a stream that ends without usage. The prompt is never billed below the
/// pre-flight estimate the reservation was built on.
fn billable_usage(completion: &Completion, estimated_prompt: u32, max_output_tokens: u32) -> Usage {
    let reported = completion.usage.unwrap_or_default();
    let completion_tokens = match completion.usage {
        Some(u) => u.completion_tokens,
        None => {
            let texts = std::iter::once(completion.content.as_str())
                .chain(completion.reasoning.as_deref())
                .chain(completion.tool_calls.iter().flat_map(|t| {
                    [t.function.name.as_str(), t.function.arguments.as_str()]
                }));
            texts
                .map(estimate_output_tokens)
                .fold(0u32, u32::saturating_add)
                .min(max_output_tokens)
        }
    };
    let prompt_tokens = reported.prompt_tokens.max(estimated_prompt);
    Usage {
        prompt_tokens,
        completion_tokens,
        total_tokens: prompt_tokens.saturating_add(completion_tokens),
        reasoning_tokens: reported.reasoning_tokens,
        // Billed without the cache discount, like the stream path, so the
        // reported cost and the ledger always agree.
        cached_prompt_tokens: None,
    }
}

/// The v1 completion document: everything the stream's `start`, token,
/// `reasoning`, `tool` and `end` frames carry, in one object.
fn completion_payload(
    completion: &Completion,
    usage: &Usage,
    cost_nano_usd: u64,
    guard: &CompletionGuard,
    metadata: Option<&Map<String, Value>>,
) -> String {
    json!({
        "request_id": guard.span.request_id,
        "model": guard.span.model,
        "tenant": guard.tenant,
        "content": completion.content,
        "reasoning": completion.reasoning,
        "tool_calls": completion.tool_calls,
        "finish_reason": completion.finish_reason,
        "usage": {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "reasoning_tokens": usage.reasoning_tokens.unwrap_or(0),
            "total_tokens": usage.total_tokens,
        },
        "cost_nano_usd": cost_nano_usd,
        "duration_ms": guard.span.elapsed_ms(),
        "upstream_id": completion.upstream_id,
        "upstream_model": completion.upstream_model,
        "metadata": metadata,
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// Passthrough framing
// ---------------------------------------------------------------------------

/// Relay upstream bytes verbatim.
///
/// The property that matters: `bytes_stream()` is handed to `Body::from_stream`
/// with no text conversion, no SSE parse, no re-serialize. TCP chunk boundaries
/// are not SSE event boundaries, so this is the only mode in which the gateway
/// provably cannot alter framing. The gateway also cannot observe content,
/// which is why per-token budgeting is unavailable here.
async fn passthrough_response(
    state: &Arc<AppState>,
    mut admitted: Admitted,
    span: &RequestSpan,
) -> Result<Response, RequestError> {
    let provider_name = admitted.resolved.provider().to_string();
    let model_id = admitted.resolved.registry_id.clone();

    let stream = match admitted.provider.stream_chat_raw(admitted.request.clone()).await {
        Ok(stream) => {
            state.metrics.stream_started(&provider_name, &model_id);
            state
                .inflight
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            stream
        }
        Err(err) => {
            admitted.reservation.release();
            state.metrics.stream_failed_to_start(&provider_name, &model_id);
            return Err(err.into());
        }
    };

    let mut headers = response_headers(span, &admitted.resolved, &admitted.principal, &admitted.reservation);
    headers.insert(
        HeaderName::from_static("x-gateway-framing"),
        HeaderValue::from_static("passthrough"),
    );

    let body = Body::from_stream(
        RawStream {
            inner: stream,
            span: span.clone(),
            reservation: admitted.reservation,
            cost_model: admitted.resolved.config.cost.clone(),
            metrics: Arc::clone(&state.metrics),
            inflight: Arc::clone(&state.inflight),
            model: admitted.resolved.registry_id.clone(),
            provider: admitted.resolved.provider().to_string(),
            tenant: admitted.principal.tenant_id.to_string(),
            max_output_tokens: admitted.max_output_tokens,
            summary: StreamSummary {
                prompt_tokens: admitted.prompt_tokens,
                ..Default::default()
            },
            first: true,
            finished: false,
        }
        .map(|res| res.map_err(std::io::Error::other)),
    );

    let mut response = Response::new(body);
    *response.headers_mut() = headers;
    Ok(response)
}

struct RawStream {
    inner: crate::providers::RawStream,
    span: RequestSpan,
    reservation: Reservation,
    cost_model: router::CostModel,
    metrics: crate::observability::SharedMetrics,
    /// Decremented when the stream object is dropped, the same shutdown-drain
    /// counter the normalized path uses.
    inflight: std::sync::Arc<std::sync::atomic::AtomicU64>,
    model: String,
    provider: String,
    tenant: String,
    max_output_tokens: u32,
    summary: StreamSummary,
    first: bool,
    finished: bool,
}

impl Stream for RawStream {
    type Item = Result<Bytes, ProviderError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                if !this.finished {
                    this.finished = true;
                    this.reservation.commit_reserved();
                    this.emit();
                }
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(err))) => {
                if !this.finished {
                    this.finished = true;
                    this.summary.error_code = Some(err.code());
                    this.summary.error_retryable = err.retryable();
                    this.reservation.commit_reserved();
                    this.emit();
                }
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(Some(Ok(bytes))) => {
                if this.first {
                    this.first = false;
                    this.span.mark_first_token();
                }
                this.metrics.bytes_to_client(bytes.len() as u64);
                this.metrics.delta_events(1);
                this.summary.bytes_out += bytes.len() as u64;
                Poll::Ready(Some(Ok(bytes)))
            }
        }
    }
}

impl Drop for RawStream {
    fn drop(&mut self) {
        if !self.finished {
            self.summary.aborted_by_client = true;
            self.reservation.commit_reserved();
            self.emit();
        }
        self.inflight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl RawStream {
    fn emit(&self) {
        let mut summary = self.summary.clone();
        // Content is never parsed in this mode, so the bill is the full
        // reservation: prompt estimate plus the output ceiling.
        summary.cost_nano_usd = estimate_cost_nano_usd(
            &self.cost_model,
            summary.prompt_tokens,
            self.max_output_tokens,
        );
        logging::log_stream_complete(&self.span, &summary);
        self.metrics.stream_finished(
            &self.provider,
            &self.model,
            &self.tenant,
            &summary,
            self.span.elapsed_ms() as u64,
            self.span.ttft_ms().map(|v| v as u64),
        );
    }
}

use std::convert::Infallible;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_header_selects_passthrough() {
        assert_eq!(Framing::parse(Some("passthrough")), Framing::Passthrough);
        assert_eq!(Framing::parse(Some(" RAW ")), Framing::Passthrough);
        assert_eq!(Framing::parse(Some("relay")), Framing::Passthrough);
        assert_eq!(Framing::parse(Some("normalized")), Framing::Normalized);
        // Unknown or absent must never silently opt out of normalization.
        assert_eq!(Framing::parse(None), Framing::Normalized);
        assert_eq!(Framing::parse(Some("garbage")), Framing::Normalized);
    }

    #[test]
    fn token_frame_matches_the_v1_contract() {
        assert_eq!(token_json("Hel"), r#"{"token":"Hel"}"#);
        assert_eq!(token_json(""), r#"{"token":""}"#);
    }

    #[test]
    fn token_frame_escapes_control_characters_and_quotes() {
        assert_eq!(token_json("a\"b"), r#"{"token":"a\"b"}"#);
        assert_eq!(token_json("line\nbreak"), r#"{"token":"line\nbreak"}"#);
        assert_eq!(token_json("tab\there"), r#"{"token":"tab\there"}"#);
        // A raw newline would break SSE framing; it must stay escaped.
        assert!(!token_json("a\nb").contains('\n'));
    }

    #[test]
    fn token_frame_escapes_backslash_and_control_chars() {
        assert_eq!(token_json("back\\slash"), r#"{"token":"back\\slash"}"#);
        assert_eq!(token_json("\u{1}"), r#"{"token":"\u0001"}"#);
    }

    #[test]
    fn unicode_passes_through_unescaped() {
        assert_eq!(token_json("héllo 🌍"), "{\"token\":\"héllo 🌍\"}");
    }
}
