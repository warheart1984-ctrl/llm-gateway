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
    folding::{
        plan::{FoldOutcome, FoldPlan, PlanConfig, StoredSummary},
        store::EntryRef,
        summarizer, FoldAccounting, FoldAttempt, FoldFacts,
    },
    governance::{
        auth::Principal,
        ledger::{
            self,
            holds::{HoldClaim, HoldProblem, HoldRecord, NewHold},
        },
        limits::AdmitRequest,
        limits::CostEstimate,
        limits::Reservation,
        AuthorizeError, LimitError,
    },
    observability::{logging, metrics::RejectionKind, RequestSpan, StreamSummary},
    providers::{
        ChatMessage, ChatProvider, Completion, ProviderError, ProviderRequest, StreamEvent, Usage,
    },
    receipts::{self, OpenReceipt, RawReplyDigest, ReceiptError, ReplyDigest, Subject},
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

/// The wire format a request arrived in, and is answered in. Governance,
/// billing and receipts are the same for both; only parsing and the shape
/// of the answer differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dialect {
    /// `/v1/chat/stream` and `/v1/chat/complete`: the frozen v1 contract.
    Gateway,
    /// `/v1/chat/completions`: OpenAI's Chat Completions format, so an
    /// OpenAI SDK works by changing only its base URL.
    OpenAi,
}

/// What an OpenAI-format answer needs to label itself.
#[derive(Debug, Clone)]
struct OpenAiReply {
    /// `chatcmpl-<request id>`.
    id: String,
    /// Unix seconds when the request was admitted.
    created: u64,
    /// The registry model id that answered.
    model: String,
    /// `stream_options.include_usage`: send a final usage chunk.
    include_usage: bool,
}

impl OpenAiReply {
    fn chunk(&self, delta: Value, finish_reason: Option<&str>) -> Event {
        Event::default().data(
            json!({
                "id": self.id,
                "object": "chat.completion.chunk",
                "created": self.created,
                "model": self.model,
                "choices": [{ "index": 0, "delta": delta, "finish_reason": finish_reason }],
            })
            .to_string(),
        )
    }

    fn usage_chunk(&self, summary: &StreamSummary) -> Event {
        Event::default().data(
            json!({
                "id": self.id,
                "object": "chat.completion.chunk",
                "created": self.created,
                "model": self.model,
                "choices": [],
                "usage": {
                    "prompt_tokens": summary.prompt_tokens,
                    "completion_tokens": summary.completion_tokens,
                    "total_tokens": summary.prompt_tokens + summary.completion_tokens,
                },
            })
            .to_string(),
        )
    }

    /// OpenAI reports a failure inside a stream as a data line with an
    /// `error` object, which its SDKs raise.
    fn error(code: &str, message: &str) -> Event {
        Event::default().data(json!({ "error": { "message": message, "type": code, "code": code } }).to_string())
    }
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
    #[error("`Hold-Id` must be a hold's id, as returned when the request was held")]
    InvalidHoldId,
    /// Not a refusal: the request waits for approval. Answered with `202`.
    #[error("this request is held for approval: {}", .0.reason)]
    Held(Box<HoldRecord>),
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
    /// Receipts are on for this tenant and the chain could not take the entry.
    /// The reply is withheld.
    #[error("the receipt could not be written")]
    ReceiptUnavailable,
    /// A request field this gateway does not support.
    #[error("{0}")]
    Unsupported(&'static str),
    /// A request field of the wrong type or in the wrong combination.
    #[error("{0}")]
    InvalidField(&'static str),
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
            | RequestError::InvalidHoldId
            | RequestError::Unsupported(_)
            | RequestError::InvalidField(_)
            | RequestError::Param(_) => StatusCode::BAD_REQUEST,
            RequestError::Held(_) => StatusCode::ACCEPTED,
            RequestError::Limit(LimitError::Hold(problem)) => match problem {
                HoldProblem::NotFound => StatusCode::NOT_FOUND,
                HoldProblem::Pending | HoldProblem::Consumed => StatusCode::CONFLICT,
                HoldProblem::Denied => StatusCode::FORBIDDEN,
                HoldProblem::Expired => StatusCode::GONE,
                HoldProblem::Mismatch => StatusCode::UNPROCESSABLE_ENTITY,
            },
            RequestError::Limit(LimitError::TooManyHolds { .. }) => StatusCode::TOO_MANY_REQUESTS,
            RequestError::Limit(LimitError::HoldNotDecidable { .. }) => StatusCode::CONFLICT,
            RequestError::Limit(LimitError::SelfApproval) => StatusCode::FORBIDDEN,
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
            RequestError::ReceiptUnavailable => StatusCode::SERVICE_UNAVAILABLE,
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
            RequestError::InvalidHoldId => "invalid_hold_id",
            RequestError::Held(_) => "held",
            RequestError::Limit(LimitError::Hold(problem)) => match problem {
                HoldProblem::NotFound => "hold_not_found",
                HoldProblem::Pending => "hold_pending",
                HoldProblem::Denied => "hold_denied",
                HoldProblem::Expired => "hold_expired",
                HoldProblem::Consumed => "hold_consumed",
                HoldProblem::Mismatch => "hold_mismatch",
            },
            RequestError::Limit(LimitError::TooManyHolds { .. }) => "holds_pending_limit",
            RequestError::Limit(LimitError::HoldNotDecidable { .. }) => "hold_not_decidable",
            RequestError::Limit(LimitError::SelfApproval) => "self_approval",
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
            RequestError::ReceiptUnavailable => "receipt_unavailable",
            RequestError::Unsupported(_) => "unsupported_parameter",
            RequestError::InvalidField(_) => "invalid_parameter",
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
            RequestError::ReceiptUnavailable => true,
            RequestError::Limit(
                LimitError::LedgerUnavailable
                | LimitError::RequestInProgress { .. }
                | LimitError::Hold(HoldProblem::Pending),
            ) => true,
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
            RequestError::Limit(LimitError::Hold(HoldProblem::Pending)) => Some("5"),
            RequestError::ReceiptUnavailable => Some("1"),
            _ => None,
        }
    }

    /// The sentence the caller is shown, and the one the decision record
    /// keeps. A provider failure is a fixed sentence per class: the vendor's
    /// own words stay in the logs (see [`ProviderError::client_message`]).
    fn client_message(&self) -> String {
        match self {
            RequestError::Provider(e) => e.client_message(),
            other => other.to_string(),
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
            StatusCode::UNPROCESSABLE_ENTITY | StatusCode::GONE => "invalid_request_error",
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
    handle(state, headers, body, Mode::Stream, Dialect::Gateway).await
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
    handle(state, headers, body, Mode::Complete, Dialect::Gateway).await
}

/// `POST /v1/chat/completions`: OpenAI's Chat Completions format, answered
/// in that format, streamed when the request says `"stream": true`. Auth,
/// allowlists, limits, budgets, holds and receipts are the same code as the
/// gateway's own endpoints, so an OpenAI SDK pointed at this base URL is
/// governed exactly like a native client.
#[axum::debug_handler]
pub async fn openai_chat_completions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let stream = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v.get("stream").and_then(Value::as_bool))
        .unwrap_or(false);
    let mode = if stream { Mode::Stream } else { Mode::Complete };
    handle(state, headers, body, mode, Dialect::OpenAi).await
}

async fn handle(state: Arc<AppState>, headers: HeaderMap, body: Bytes, mode: Mode, dialect: Dialect) -> Response {
    let request_id = Uuid::new_v4().to_string();
    let mut span = RequestSpan::new(request_id.clone());
    state.metrics.request_observed();

    // Returns `Response` rather than `Result<Response, RequestError>` so every
    // rejection is a fully-formed response with its own headers, built in one
    // place below.
    let outcome = match admit(&state, &headers, &body, &mut span, &request_id, mode, dialect).await {
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
            let Stored { body, receipt, folded_turns, fold_summary, fold_receipt_id } = unwrap_stored(stored);
            let mut response = (StatusCode::OK, body).into_response();
            let h = response.headers_mut();
            h.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            h.insert("x-request-id", header_value(&request_id));
            h.insert("idempotent-replayed", HeaderValue::from_static("true"));
            h.insert("x-original-request-id", header_value(&original.to_string()));
            // The original receipt: a replay writes none of its own, so it
            // points at the one that covers this answer.
            if let Some((receipt_id, salt)) = receipt {
                h.insert(HeaderName::from_static("x-receipt-id"), header_value(&receipt_id));
                h.insert(HeaderName::from_static("x-receipt-salt"), header_value(&salt));
            }
            if folded_turns > 0 {
                h.insert(
                    HeaderName::from_static("x-gateway-folded"),
                    header_value(&folded_turns.to_string()),
                );
            }
            if let Some(summary) = fold_summary.as_deref() {
                h.insert(HeaderName::from_static("x-gateway-fold-summary"), fold_summary_header(summary));
            }
            if let Some(receipt_id) = fold_receipt_id.as_deref() {
                h.insert(HeaderName::from_static("x-gateway-fold-receipt-id"), header_value(receipt_id));
            }
            response
        }
        // Held for approval. Not a refusal, so neither a rejection metric nor
        // a routine decision: the hold was recorded when it was made.
        Err(RequestError::Held(hold)) => {
            tracing::info!(
                request_id = %request_id,
                tenant = %span.tenant_id,
                hold = %hold.id,
                model = %hold.model,
                reason = %hold.reason,
                "request held for approval"
            );
            let location = format!("/v1/holds/{}", hold.id);
            let message = format!(
                "this request needs approval before it runs ({}). Poll `GET {location}`; once it is approved, send \
                 the same request again with the header `Hold-Id: {}`.",
                hold.reason, hold.id
            );
            let mut response = (
                StatusCode::ACCEPTED,
                Json(json!({ "status": "held", "hold": hold, "message": message, "request_id": request_id })),
            )
                .into_response();
            let h = response.headers_mut();
            h.insert("x-request-id", header_value(&request_id));
            h.insert("hold-id", header_value(&hold.id.to_string()));
            h.insert(axum::http::header::LOCATION, header_value(&location));
            response
        }
        Err(err) => {
            let err = match ensure_deny_receipt(&state, &span, &request_id, &err).await {
                Ok(()) => err,
                Err(receipt_err) => receipt_err,
            };
            if let Some(kind) = err.rejection_kind() {
                state.metrics.rejection(kind);
            }
            logging::log_rejected(&request_id, &span.tenant_id, err.code(), &logging::redact_secrets(&err.to_string()));
            record_decision(&state, &span, &request_id, mode, &err);

            let status = err.status();
            let mut body = json!({
                "error": {
                    "code": err.code(),
                    "type": err.error_type(),
                    "message": err.client_message(),
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
    // text, and a vendor's can name the operator's account; the record keeps
    // a fixed sentence for both.
    let reason = match err {
        RequestError::BadJson(_) => "request body is not valid JSON".to_string(),
        other => other.client_message(),
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

/// The process chain, when this tenant has receipts on, for either
/// endpoint. Receipts off returns `None` and the decision queue keeps
/// today's best-effort behavior.
fn tenant_chain(
    state: &AppState,
    tenant_id: &str,
) -> Result<Option<std::sync::Arc<receipts::ReceiptChain>>, RequestError> {
    if tenant_id.is_empty() {
        return Ok(None);
    }
    let wants = state.current_tenants().get(tenant_id).is_some_and(|tenant| tenant.receipts);
    if !wants {
        return Ok(None);
    }
    let book = state.receipts.as_ref().ok_or(RequestError::ReceiptUnavailable)?;
    book.chain(tenant_id).map(Some).map_err(|_| RequestError::ReceiptUnavailable)
}

/// Governance refusals a receipt records as a kernel `deny`: out of budget
/// (402), not allowed (403), over a rate or concurrency limit (429). A
/// malformed or oversized body, a duplicate or a mismatched resubmission is
/// a bad request, not a decision, and leaves no receipt.
fn is_governance_refusal(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::PAYMENT_REQUIRED | StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS
    )
}

/// A governance refusal for a tenant with receipts on is a kernel `deny`,
/// written before the refusal is returned. A failed write replaces the
/// refusal: the reply is withheld. The decision queue is still best-effort
/// and is not this write.
async fn ensure_deny_receipt(
    state: &AppState,
    span: &RequestSpan,
    request_id: &str,
    err: &RequestError,
) -> Result<(), RequestError> {
    if matches!(
        err,
        RequestError::ReceiptUnavailable | RequestError::Auth(_) | RequestError::Held(_)
    ) || !is_governance_refusal(err.status())
    {
        return Ok(());
    }
    let Some(chain) = tenant_chain(state, &span.tenant_id)? else {
        return Ok(());
    };
    chain
        .deny(&Subject {
            request_id: request_id.to_string(),
            tenant_id: span.tenant_id.clone(),
            key_id: span.key_id.clone(),
            model: span.model.clone(),
            action: err.code().to_string(),
            call_digest: None,
        })
        .await
        .map(|_| ())
        .map_err(|_| RequestError::ReceiptUnavailable)
}

/// Nothing came back: close the allow with a `failed` outcome that has no
/// reply digest, and wait until it is durable.
async fn failed_outcome(open: &OpenReceipt) -> Result<(), RequestError> {
    open.chain
        .outcome(open, "failed", None)
        .await
        .map(|_| ())
        .map_err(|_| RequestError::ReceiptUnavailable)
}

/// Marks a stored answer that carries its receipt. Never a field of an
/// answer itself, so a stored answer without it is returned as it is.
const STORED_WITH_RECEIPT: &str = "llm_gateway_stored_with_receipt";
const STORED_WITH_FOLDING: &str = "llm_gateway_stored_with_folding";

/// The answer kept for idempotent replay, with the id and salt of the
/// receipt that covers it.
fn stored_with_receipt(
    payload: &str,
    open: &OpenReceipt,
    folded_turns: usize,
    fold_summary: Option<&str>,
    fold_receipt_id: Option<&str>,
) -> String {
    json!({
        STORED_WITH_RECEIPT: 1,
        "receipt_id": open.decision_receipt_id,
        "receipt_salt": open.salt.to_hex(),
        STORED_WITH_FOLDING: folded_turns,
        "fold_summary": fold_summary,
        "fold_receipt_id": fold_receipt_id,
        "body": payload,
    })
    .to_string()
}

fn stored_with_folding(payload: &str, folded_turns: usize) -> String {
    json!({
        STORED_WITH_FOLDING: folded_turns,
        "body": payload,
    })
    .to_string()
}

/// A stored answer and what is replayed with it.
#[derive(Default)]
struct Stored {
    body: String,
    /// The id and salt of the receipt that covers it.
    receipt: Option<(String, String)>,
    folded_turns: usize,
    /// A receipts tenant's `x-gateway-fold-summary` and
    /// `x-gateway-fold-receipt-id`.
    fold_summary: Option<String>,
    fold_receipt_id: Option<String>,
}

/// Read back a stored answer. Answers stored before receipts, or for tenants
/// without them, come back unchanged.
fn unwrap_stored(stored: String) -> Stored {
    if let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(&stored)
        && let Some(Value::String(body)) = fields.get("body")
    {
        let folded_turns = fields
            .get(STORED_WITH_FOLDING)
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize;
        if fields.get(STORED_WITH_RECEIPT) == Some(&json!(1))
            && let (Some(Value::String(id)), Some(Value::String(salt))) =
                (fields.get("receipt_id"), fields.get("receipt_salt"))
        {
            return Stored {
                body: body.clone(),
                receipt: Some((id.clone(), salt.clone())),
                folded_turns,
                fold_summary: fields.get("fold_summary").and_then(Value::as_str).map(str::to_string),
                fold_receipt_id: fields.get("fold_receipt_id").and_then(Value::as_str).map(str::to_string),
            };
        }
        if folded_turns > 0 {
            return Stored { body: body.clone(), folded_turns, ..Stored::default() };
        }
    }
    Stored { body: stored, ..Stored::default() }
}

/// `x-receipt-id` names the allow receipt; `x-receipt-salt` is the salt in
/// the request and reply digests. Only the caller gets the salt, so only
/// someone holding the request and reply can recompute either digest.
fn insert_receipt_headers(headers: &mut HeaderMap, receipt: Option<&OpenReceipt>) {
    if let Some(open) = receipt {
        headers.insert(HeaderName::from_static("x-receipt-id"), header_value(&open.decision_receipt_id));
        headers.insert(HeaderName::from_static("x-receipt-salt"), header_value(&open.salt.to_hex()));
    }
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

/// The request's `Hold-Id`, if it presents an approved hold.
fn hold_id(headers: &HeaderMap) -> Result<Option<Uuid>, RequestError> {
    let Some(raw) = headers.get("hold-id") else {
        return Ok(None);
    };
    raw.to_str()
        .ok()
        .and_then(|v| Uuid::parse_str(v.trim()).ok())
        .map(Some)
        .ok_or(RequestError::InvalidHoldId)
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
    /// Set for a stream whose tenant has receipts on, after the allow receipt
    /// is durable. Absent for completions and for tenants with receipts off.
    receipt: Option<OpenReceipt>,
    /// Set for `/v1/chat/completions`: answer in OpenAI's format.
    openai: Option<OpenAiReply>,
    /// Number of old turns replaced in the forwarded request.
    folded_turns: usize,
    /// The summary in the forwarded request, for a receipts tenant's
    /// `x-gateway-fold-summary`.
    fold_summary: Option<String>,
    /// The summarizer call's allow, when this request ran one.
    fold_receipt_id: Option<String>,
}

#[derive(Debug, Default)]
struct FoldResult {
    messages: Vec<ChatMessage>,
    folded_turns: usize,
    facts: Option<FoldFacts>,
    attempt: Option<FoldAttempt>,
    /// The summarizer call's own allow, when this request ran one for a
    /// receipts tenant: `x-gateway-fold-receipt-id`.
    fold_receipt_id: Option<String>,
}

/// What a receipts tenant's fold needs to write its own allow and outcome:
/// the tenant's chain, this request's salt, and its body digest.
struct FoldReceipts {
    chain: Arc<receipts::ReceiptChain>,
    salt: receipts::Salt,
    call_digest: String,
    request_id: String,
    tenant_id: String,
    key_id: String,
}

fn bare_fold(messages: Vec<ChatMessage>, outcome: &'static str, reason: &str, attempt: Option<FoldAttempt>) -> FoldResult {
    FoldResult {
        messages,
        folded_turns: 0,
        fold_receipt_id: None,
        facts: Some(FoldFacts {
            outcome,
            reason: reason.to_string(),
            turns_replaced: 0,
            turns_covered: 0,
            prompt_tokens_cut: 0,
            benefit_nano_usd: 0,
            overhead_nano_usd: 0,
            min_saving_nano_usd: 0,
            summarizer_prompt_tokens: attempt.map(|item| item.prompt_tokens).unwrap_or(0),
            summarizer_completion_tokens: attempt.map(|item| item.completion_tokens).unwrap_or(0),
            fold_count: 0,
            prefix_hash: String::new(),
        }),
        attempt,
    }
}

struct FoldStamp<'a> {
    outcome: &'static str,
    reason: &'a str,
    attempt: Option<FoldAttempt>,
    fold_count: u32,
    prefix_hash: Option<[u8; 32]>,
    folded_turns: usize,
}

fn describe_fold(
    original: &[ChatMessage],
    messages: Vec<ChatMessage>,
    plan: &FoldPlan,
    stamp: FoldStamp<'_>,
) -> FoldResult {
    let sent_cut = u64::from(
        estimate_prompt_tokens(original).saturating_sub(estimate_prompt_tokens(&messages)),
    );
    // A request that went out folded records the tokens actually removed.
    // A refused plan records the cost check's own estimate, which is what
    // the operator needs to see why it did not run.
    let prompt_tokens_cut = if stamp.folded_turns > 0 { sent_cut } else { u64::from(plan.prompt_tokens_cut) };
    FoldResult {
        messages,
        folded_turns: stamp.folded_turns,
        fold_receipt_id: None,
        facts: Some(FoldFacts {
            outcome: stamp.outcome,
            reason: stamp.reason.to_string(),
            turns_replaced: u64::try_from(stamp.folded_turns).unwrap_or(u64::MAX),
            turns_covered: u64::try_from(plan.turns_covered).unwrap_or(u64::MAX),
            prompt_tokens_cut,
            benefit_nano_usd: plan.benefit_nano_usd,
            overhead_nano_usd: plan.overhead_nano_usd,
            min_saving_nano_usd: plan.min_saving_nano_usd,
            summarizer_prompt_tokens: stamp.attempt.map(|item| item.prompt_tokens).unwrap_or(0),
            summarizer_completion_tokens: stamp.attempt.map(|item| item.completion_tokens).unwrap_or(0),
            fold_count: u64::from(stamp.fold_count),
            prefix_hash: stamp.prefix_hash.map(|hash| crate::folding::prefix_hex(&hash)).unwrap_or_default(),
        }),
        attempt: stamp.attempt,
    }
}

fn stored_summary(entry: &EntryRef) -> StoredSummary {
    let entry = entry.lock();
    StoredSummary {
        turns_covered: entry.turns_covered,
        prefix_hash: entry.prefix_hash,
        summary: entry.summary.clone(),
        fold_count: entry.fold_count,
        summarizer_model: entry.summarizer_model.clone(),
    }
}

/// The request as it goes out when no fold runs now: with the committed
/// summary and the turns it does not cover, or unchanged if there is none.
fn with_current_summary(
    messages: &[ChatMessage],
    protected: &[ChatMessage],
    prior: Option<&StoredSummary>,
    pending: &[ChatMessage],
    tail: &[ChatMessage],
) -> (Vec<ChatMessage>, usize) {
    match prior {
        Some(summary) => (
            crate::folding::plan::build_forwarded_messages(protected, Some(&summary.summary), pending, tail),
            summary.turns_covered,
        ),
        None => (messages.to_vec(), 0),
    }
}

/// Owns everything a fold needs after the request that planned it has moved on.
struct FoldWork {
    state: Arc<AppState>,
    tenant_id: String,
    limits: crate::config::LimitProfile,
    summarizer_model_id: String,
    summarizer_max_tokens: u32,
    upstream_model: String,
    endpoint: Option<String>,
    cost: crate::router::CostModel,
    provider: Arc<dyn ChatProvider>,
    key: [u8; 32],
    head: Vec<ChatMessage>,
    pending: Vec<ChatMessage>,
    covered: usize,
    prior_text: Option<String>,
    started_prefix: Option<[u8; 32]>,
    old_spent: u64,
    old_saved: u64,
    new_fold_count: u32,
    prompt_tokens_cut: u32,
    plan_config: PlanConfig,
}

enum AppliedFold {
    Committed {
        summary: String,
        prefix_hash: [u8; 32],
        prompt_tokens: u64,
        completion_tokens: u64,
    },
    Stopped {
        outcome: &'static str,
        reason: &'static str,
        accounting: FoldAccounting,
        prompt_tokens: u64,
        completion_tokens: u64,
    },
}

/// Clears the in-flight flag however the task ends, including cancellation.
struct EndFold {
    store: Arc<crate::folding::store::FoldingStore>,
    key: [u8; 32],
}

impl Drop for EndFold {
    fn drop(&mut self) {
        self.store.finish(&self.key);
    }
}

/// Run a fold and keep its backoff: a rejected fold holds off its
/// conversation until `backoff_turns` more turns arrive; a committed one
/// clears that.
async fn run_applied_fold(work: FoldWork) -> AppliedFold {
    let store = Arc::clone(&work.state.folding);
    let (key, head_turns) = (work.key, work.head.len());
    let result = run_summarizer_attempts(work).await;
    match &result {
        AppliedFold::Committed { .. } => store.clear_backoff(&key),
        AppliedFold::Stopped { accounting: FoldAccounting::Rejected, .. } => store.back_off(key, head_turns),
        AppliedFold::Stopped { .. } => {}
    }
    result
}

/// Run the summarizer. The store lock is never held across the provider call.
/// On shutdown the reservation is released, not abandoned.
async fn run_summarizer_attempts(work: FoldWork) -> AppliedFold {
    let mut shutdown = work.state.folding.subscribe_shutdown();
    let mut attempt_no = 0u8;
    let mut used_prompt = 0u64;
    let mut used_completion = 0u64;

    loop {
        if *shutdown.borrow() {
            return AppliedFold::Stopped {
                outcome: "failed",
                reason: "cancelled",
                accounting: FoldAccounting::Failed,
                prompt_tokens: used_prompt,
                completion_tokens: used_completion,
            };
        }
        attempt_no += 1;
        let budget = if attempt_no == 1 {
            work.summarizer_max_tokens
        } else {
            work.summarizer_max_tokens.saturating_mul(2)
        };
        let prompt_messages = summarizer::prompt_messages(&work.plan_config, work.prior_text.as_deref(), &work.pending);
        let prompt_tokens = estimate_prompt_tokens(&prompt_messages);
        let estimate = CostEstimate {
            prompt_nano_usd: estimate_cost_nano_usd(&work.cost, prompt_tokens, 0),
            completion_nano_usd: estimate_cost_nano_usd(&work.cost, 0, budget),
        };
        let Ok(mut reservation) = work
            .state
            .limits
            .admit_request(AdmitRequest {
                id: Uuid::new_v4(),
                tenant_id: &work.tenant_id,
                limits: &work.limits,
                prompt_tokens,
                max_output_tokens: budget,
                estimate,
                idempotency: None,
                hold: None,
            })
            .await
        else {
            return AppliedFold::Stopped {
                outcome: "refused",
                reason: "admission_refused",
                accounting: FoldAccounting::RefusedAdmission,
                prompt_tokens: used_prompt,
                completion_tokens: used_completion,
            };
        };
        if *shutdown.borrow() {
            reservation.release();
            return AppliedFold::Stopped {
                outcome: "failed",
                reason: "cancelled",
                accounting: FoldAccounting::Failed,
                prompt_tokens: used_prompt,
                completion_tokens: used_completion,
            };
        }

        let request = summarizer::build_request(
            &work.upstream_model,
            work.endpoint.clone(),
            &work.tenant_id,
            &Uuid::new_v4().to_string(),
            prompt_messages,
            budget,
        );
        let pending = work.provider.complete(request);
        tokio::pin!(pending);
        let completion = tokio::select! {
            biased;
            _ = shutdown.changed() => {
                reservation.release();
                return AppliedFold::Stopped {
                    outcome: "failed",
                    reason: "cancelled",
                    accounting: FoldAccounting::Failed,
                    prompt_tokens: used_prompt,
                    completion_tokens: used_completion,
                };
            }
            result = &mut pending => result,
        };
        let completion = match completion {
            Ok(completion) => completion,
            Err(error) => {
                if error.accepted_by_upstream() {
                    reservation.abandon();
                } else {
                    reservation.release();
                }
                return AppliedFold::Stopped {
                    outcome: "failed",
                    reason: "provider_error",
                    accounting: FoldAccounting::Failed,
                    prompt_tokens: used_prompt,
                    completion_tokens: used_completion,
                };
            }
        };
        // Billed like a main completion: a provider that reports no usage is
        // charged the gateway's estimate of the output, not zero.
        let usage = billable_usage(&completion, prompt_tokens, budget);
        used_prompt = used_prompt.saturating_add(u64::from(usage.prompt_tokens));
        used_completion = used_completion.saturating_add(u64::from(usage.completion_tokens));
        let content = summarizer::extract_content(&completion);
        let retry_empty = attempt_no == 1
            && content.trim().is_empty()
            && (completion.reasoning.as_deref().is_some_and(|text| !text.is_empty())
                || completion.finish_reason.as_deref() == Some("length"));
        reservation.settle(usage, &work.cost);
        if retry_empty {
            continue;
        }
        if content.trim().is_empty() || completion.finish_reason.as_deref() == Some("length") {
            let reason = if completion.finish_reason.as_deref() == Some("length") {
                "length"
            } else {
                "empty_reply"
            };
            return AppliedFold::Stopped {
                outcome: "refused",
                reason,
                accounting: FoldAccounting::Rejected,
                prompt_tokens: used_prompt,
                completion_tokens: used_completion,
            };
        }
        let Ok(summary) = summarizer::run_checks_with_prior(
            &content,
            work.prior_text.as_deref(),
            &work.pending,
            &work.plan_config,
        ) else {
            return AppliedFold::Stopped {
                outcome: "refused",
                reason: "checks_failed",
                accounting: FoldAccounting::Rejected,
                prompt_tokens: used_prompt,
                completion_tokens: used_completion,
            };
        };

        let new_covered = work.covered + work.pending.len();
        let prefix_hash = work.state.folding.prefix_hash(&work.tenant_id, &work.head[..new_covered]);
        let committed = work.state.folding.commit_if_current(
            work.key,
            work.started_prefix,
            crate::folding::store::SummaryEntry {
                turns_covered: new_covered,
                prefix_hash,
                summary: summary.clone(),
                fold_count: work.new_fold_count,
                tokens_spent: work.old_spent.saturating_add(used_prompt.saturating_add(used_completion)),
                tokens_saved: work.old_saved.saturating_add(u64::from(work.prompt_tokens_cut)),
                summarizer_model: work.summarizer_model_id.clone(),
                last_accessed: std::time::Instant::now(),
            },
        );
        if !committed {
            return AppliedFold::Stopped {
                outcome: "failed",
                reason: "prefix_moved",
                accounting: FoldAccounting::Failed,
                prompt_tokens: used_prompt,
                completion_tokens: used_completion,
            };
        }
        return AppliedFold::Committed {
            summary,
            prefix_hash,
            prompt_tokens: used_prompt,
            completion_tokens: used_completion,
        };
    }
}

/// Run one foreground fold. Every failure returns the client messages
/// unchanged; the main request is never made dependent on summarization.
async fn foreground_fold(
    state: &Arc<AppState>,
    generation: &crate::state::RegistryGeneration,
    decision: &crate::governance::policy::Decision,
    resolved: &router::ResolvedModel,
    key_id: &str,
    messages: &[ChatMessage],
    receipts: Option<&FoldReceipts>,
) -> FoldResult {
    let Some(folding) = decision.tenant.folding.as_ref().filter(|f| f.enabled) else {
        return FoldResult { messages: messages.to_vec(), ..FoldResult::default() };
    };

    let (protected, turns) = crate::folding::split_messages(messages);
    let Some(first_turn) = turns.first() else {
        return bare_fold(messages.to_vec(), "not_due", "no_turns", None);
    };
    let key = state
        .folding
        .conversation_key(&decision.tenant.tenant_id, key_id, &protected, Some(first_turn));

    let existing = state
        .folding
        .lookup(&key, &decision.tenant.tenant_id, &turns);
    let existing = existing.and_then(|entry| {
        let same_model = entry.lock().summarizer_model == folding.summarizer_model;
        if same_model {
            Some(entry)
        } else {
            state.folding.remove(&key);
            None
        }
    });
    let prior = existing.as_ref().map(stored_summary);
    let plan_config = PlanConfig::from_tenant(folding);

    // Resolved with the tenant's model suffix, as load-time validation checked
    // it and as the tenant's own requests are resolved.
    let summarizer_id = crate::governance::policy::qualify(&decision.tenant.model_suffix, &folding.summarizer_model);
    let Some(summarizer_model) = generation.models.resolve(&summarizer_id).ok() else {
        return bare_fold(
            messages.to_vec(),
            "failed",
            "summarizer_unavailable",
            Some(FoldAttempt { outcome: FoldAccounting::Failed, prompt_tokens: 0, completion_tokens: 0 }),
        );
    };
    let Ok(summarizer_provider) = router::provider_for(&summarizer_model, &state.providers) else {
        return bare_fold(
            messages.to_vec(),
            "failed",
            "summarizer_unavailable",
            Some(FoldAttempt { outcome: FoldAccounting::Failed, prompt_tokens: 0, completion_tokens: 0 }),
        );
    };
    let summarizer_provider = Arc::clone(summarizer_provider);

    let plan = crate::folding::plan::plan_fold(
        &plan_config,
        &resolved.config.cost,
        &summarizer_model.config.cost,
        &protected,
        &turns,
        prior.as_ref(),
    );
    if plan.outcome != FoldOutcome::Applied {
        let forwarded = if plan.forwarded_messages.is_empty() {
            messages.to_vec()
        } else {
            plan.forwarded_messages.clone()
        };
        let outcome = match plan.outcome {
            FoldOutcome::Applied => "applied",
            FoldOutcome::NotDue => "not_due",
            FoldOutcome::Skipped => "skipped",
            FoldOutcome::Refused => "refused",
            FoldOutcome::Failed => "failed",
        };
        let attempt = (plan.outcome == FoldOutcome::Refused).then_some(FoldAttempt {
            outcome: FoldAccounting::Unprofitable,
            prompt_tokens: 0,
            completion_tokens: 0,
        });
        return describe_fold(
            messages,
            forwarded,
            &plan,
            FoldStamp {
                outcome,
                reason: plan.reason.code(),
                attempt,
                fold_count: prior.as_ref().map(|summary| summary.fold_count).unwrap_or(0),
                prefix_hash: prior.as_ref().map(|summary| summary.prefix_hash),
                folded_turns: plan.turns_replaced,
            },
        );
    }

    let split = turns.len().saturating_sub(folding.keep_last);
    let head = turns[..split].to_vec();
    let tail = turns[split..].to_vec();
    let covered = prior.as_ref().map(|summary| summary.turns_covered.min(head.len())).unwrap_or(0);
    let pending = head[covered..].to_vec();

    // Checks that skip a fold before anything is admitted or paid. Each sends
    // the request with the summary it already has, or unfolded.
    let skip = |outcome: &'static str, reason: &'static str, accounting: Option<FoldAccounting>| {
        let (forwarded, folded_turns) = with_current_summary(messages, &protected, prior.as_ref(), &pending, &tail);
        describe_fold(
            messages,
            forwarded,
            &plan,
            FoldStamp {
                outcome,
                reason,
                attempt: accounting.map(|outcome| FoldAttempt { outcome, prompt_tokens: 0, completion_tokens: 0 }),
                fold_count: prior.as_ref().map(|summary| summary.fold_count).unwrap_or(0),
                prefix_hash: prior.as_ref().map(|summary| summary.prefix_hash),
                folded_turns,
            },
        )
    };
    if state.folding.shutting_down() {
        return skip("not_due", "shutdown", None);
    }
    if folding.backoff_turns > 0
        && let Some(rejected_at) = state.folding.backed_off_at(&key)
        && head.len() < rejected_at.saturating_add(folding.backoff_turns)
    {
        return skip("skipped", "backoff", Some(FoldAccounting::BackedOff));
    }
    let pinned = summarizer::pinned_terms(prior.as_ref().map(|summary| summary.summary.as_str()), &pending);
    if !summarizer::pinned_terms_fit(&pinned, plan_config.summary_chars) {
        return skip("skipped", "pinned_overflow", Some(FoldAccounting::PinnedOverflow));
    }

    // The summarizer is admitted like any request: it takes a rate slot and
    // prompt tokens from the tenant's window. Fold only when the window has
    // room for it and for the request it serves, so a fold never turns an
    // admissible request into a 429. Without room, the request goes out with
    // the summary it already has, or unfolded.
    let summarizer_prompt = estimate_prompt_tokens(&summarizer::prompt_messages(
        &plan_config,
        prior.as_ref().map(|summary| summary.summary.as_str()),
        &pending,
    ));
    if !state.limits.has_rate_headroom(
        &decision.tenant.tenant_id,
        &decision.limits,
        2,
        summarizer_prompt.saturating_add(estimate_prompt_tokens(messages)),
    ) {
        return skip("refused", "no_rate_headroom", Some(FoldAccounting::RefusedAdmission));
    }

    let (old_spent, old_saved) = existing
        .as_ref()
        .map(|entry| {
            let entry = entry.lock();
            (entry.tokens_spent, entry.tokens_saved)
        })
        .unwrap_or_default();
    let work = FoldWork {
        state: Arc::clone(state),
        tenant_id: decision.tenant.tenant_id.clone(),
        limits: decision.limits.clone(),
        summarizer_model_id: folding.summarizer_model.clone(),
        summarizer_max_tokens: folding.summarizer_max_tokens,
        upstream_model: summarizer_model.upstream_model.clone(),
        endpoint: summarizer_model.endpoint.clone(),
        cost: summarizer_model.config.cost.clone(),
        provider: summarizer_provider,
        key,
        head,
        pending: pending.clone(),
        covered,
        prior_text: prior.as_ref().map(|summary| summary.summary.clone()),
        started_prefix: prior.as_ref().map(|summary| summary.prefix_hash),
        old_spent,
        old_saved,
        new_fold_count: plan.new_fold_count,
        prompt_tokens_cut: plan.prompt_tokens_cut,
        plan_config,
    };

    if state.settings.folding.background {
        let (immediate, folded_turns) = with_current_summary(messages, &protected, prior.as_ref(), &pending, &tail);
        if state.folding.try_begin(key) {
            let tenant_id = work.tenant_id.clone();
            let metrics = Arc::clone(&state.metrics);
            let store = Arc::clone(&state.folding);
            tokio::spawn(async move {
                let _end = EndFold { store, key };
                let result = run_applied_fold(work).await;
                let (accounting, prompt_tokens, completion_tokens) = match result {
                    AppliedFold::Committed { prompt_tokens, completion_tokens, .. } => {
                        (FoldAccounting::Committed, prompt_tokens, completion_tokens)
                    }
                    AppliedFold::Stopped { accounting, prompt_tokens, completion_tokens, .. } => {
                        (accounting, prompt_tokens, completion_tokens)
                    }
                };
                metrics.record_fold(&tenant_id, accounting, prompt_tokens, completion_tokens);
            });
        }
        let sent_cut = u64::from(
            estimate_prompt_tokens(messages).saturating_sub(estimate_prompt_tokens(&immediate)),
        );
        return FoldResult {
            messages: immediate,
            folded_turns,
            fold_receipt_id: None,
            facts: Some(FoldFacts {
                outcome: "not_due",
                reason: "background".to_string(),
                turns_replaced: u64::try_from(folded_turns).unwrap_or(u64::MAX),
                turns_covered: u64::try_from(folded_turns).unwrap_or(u64::MAX),
                prompt_tokens_cut: if folded_turns > 0 { sent_cut } else { 0 },
                benefit_nano_usd: plan.benefit_nano_usd,
                overhead_nano_usd: plan.overhead_nano_usd,
                min_saving_nano_usd: plan.min_saving_nano_usd,
                summarizer_prompt_tokens: 0,
                summarizer_completion_tokens: 0,
                fold_count: u64::from(prior.as_ref().map(|summary| summary.fold_count).unwrap_or(0)),
                prefix_hash: prior
                    .as_ref()
                    .map(|summary| crate::folding::prefix_hex(&summary.prefix_hash))
                    .unwrap_or_default(),
            }),
            attempt: None,
        };
    }

    // One fold per conversation at a time, as in background mode. A request
    // that finds one running goes out with the summary it already has: two
    // concurrent folds would both be paid for and only one could commit.
    // `try_begin` also refuses once shutdown has begun; say which it was.
    if !state.folding.try_begin(key) {
        let reason = if state.folding.shutting_down() { "shutdown" } else { "fold_in_flight" };
        return skip("not_due", reason, None);
    }
    let _end = EndFold { store: Arc::clone(&state.folding), key };

    // A receipts tenant's summarizer call is a model call carrying its
    // content, so it gets its own allow first, under this request's salt. If
    // that cannot be written, nothing is folded: the chain has stopped, and
    // the request then meets the stopped chain like any other.
    let fold_receipt = match receipts {
        Some(receipts) => {
            let digest = receipts::fold_request_digest(
                &receipts.salt,
                &receipts.call_digest,
                covered,
                covered + pending.len(),
                prior.as_ref().map(|summary| summary.summary.as_str()).unwrap_or_default(),
            );
            let allowed = receipts
                .chain
                .allow(&Subject {
                    request_id: receipts.request_id.clone(),
                    tenant_id: receipts.tenant_id.clone(),
                    key_id: receipts.key_id.clone(),
                    model: summarizer_id.clone(),
                    action: "fold".into(),
                    call_digest: Some(digest.clone()),
                })
                .await;
            match allowed {
                Ok(decision_receipt_id) => Some(OpenReceipt {
                    chain: Arc::clone(&receipts.chain),
                    decision_receipt_id,
                    request_digest: digest,
                    salt: receipts.salt.clone(),
                }),
                Err(_) => {
                    return describe_fold(
                        messages,
                        messages.to_vec(),
                        &plan,
                        FoldStamp {
                            outcome: "failed",
                            reason: "receipt_unavailable",
                            attempt: Some(FoldAttempt { outcome: FoldAccounting::Failed, prompt_tokens: 0, completion_tokens: 0 }),
                            fold_count: prior.as_ref().map(|summary| summary.fold_count).unwrap_or(0),
                            prefix_hash: prior.as_ref().map(|summary| summary.prefix_hash),
                            folded_turns: 0,
                        },
                    );
                }
            }
        }
        None => None,
    };

    let applied = run_applied_fold(work).await;
    // The fold's outcome answers its allow: the summary it produced, or
    // `failed` without one. A write that fails stops the chain, and the
    // request's own allow then cannot be written, so it gets 503.
    if let Some(open) = &fold_receipt {
        let (status, reply) = match &applied {
            AppliedFold::Committed { summary, .. } => ("completed", Some(receipts::fold_reply_digest(&open.salt, summary))),
            AppliedFold::Stopped { .. } => ("failed", None),
        };
        let _ = open.chain.outcome(open, status, reply).await;
    }
    let fold_receipt_id = fold_receipt.map(|open| open.decision_receipt_id);

    let mut result = match applied {
        AppliedFold::Committed { summary, prefix_hash, prompt_tokens, completion_tokens } => {
            let forwarded = crate::folding::plan::build_forwarded_messages(&protected, Some(&summary), &[], &tail);
            describe_fold(
                messages,
                forwarded,
                &plan,
                FoldStamp {
                    outcome: "applied",
                    reason: plan.reason.code(),
                    attempt: Some(FoldAttempt {
                        outcome: FoldAccounting::Committed,
                        prompt_tokens,
                        completion_tokens,
                    }),
                    fold_count: plan.new_fold_count,
                    prefix_hash: Some(prefix_hash),
                    folded_turns: plan.turns_replaced,
                },
            )
        }
        AppliedFold::Stopped { outcome, reason, accounting, prompt_tokens, completion_tokens } => describe_fold(
            messages,
            messages.to_vec(),
            &plan,
            FoldStamp {
                outcome,
                reason,
                attempt: Some(FoldAttempt { outcome: accounting, prompt_tokens, completion_tokens }),
                fold_count: prior.as_ref().map(|summary| summary.fold_count).unwrap_or(0),
                prefix_hash: prior.as_ref().map(|summary| summary.prefix_hash),
                folded_turns: 0,
            },
        ),
    };
    result.fold_receipt_id = fold_receipt_id;
    result
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
    dialect: Dialect,
) -> Result<Admitted, RequestError> {
    // 0. Body cap, checked before any parsing or authentication work. The
    //    extractor's own limit returns 413 without reaching the handler, so
    //    this is the backstop for a limit configured below the buffer size.
    if body.len() > state.settings.server.request_body_limit_bytes {
        return Err(RequestError::PayloadTooLarge);
    }

    // 1. Authenticate. Keep the generation pinned for the whole admission so
    // auth, tenant policy, and model routing cannot straddle a reload.
    let generation = state.current_generation();
    let principal = generation.auth.authenticate(headers).await?;
    span.tenant_id = principal.tenant_id.to_string();
    span.key_id = principal.key_id.to_string();
    span.scheme = principal.scheme.to_string();

    // 2. Parse. Bounded upstream by the body-limit layer.
    let (mut req, include_usage) = match dialect {
        Dialect::Gateway => (
            serde_json::from_slice::<ChatRequest>(body).map_err(|e| RequestError::BadJson(e.to_string()))?,
            false,
        ),
        Dialect::OpenAi => openai_request(body)?,
    };
    if req.model.trim().is_empty() {
        return Err(RequestError::MissingModel);
    }
    if req.messages.is_empty() {
        return Err(RequestError::NoMessages);
    }
    if dialect == Dialect::Gateway {
        match (mode, req.stream) {
            (Mode::Stream, Some(false)) => return Err(RequestError::StreamRequired),
            (Mode::Complete, Some(true)) => return Err(RequestError::StreamNotAllowed),
            _ => {}
        }
    }
    let idempotency_key = idempotency_key(headers)?;
    let hold_id = hold_id(headers)?;

    // 3. Authorize: may this tenant call this model, at this size?
    let prompt_chars: usize = req.messages.iter().map(ChatMessage::approx_chars).sum();
    let decision = state.policy.authorize(
        &principal,
        &generation.tenants,
        &req.model,
        req.messages.len(),
        prompt_chars,
    )?;

    // 4. Resolve against the allowlisted registry. A client-supplied endpoint is
    //    never honoured; the URL comes from `models.yaml`.
    let snapshot = Arc::clone(&generation.models);
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
    if let Some(requested) = req.params.as_ref() {
        for (key, value) in requested {
            if matches!(key.as_str(), "max_tokens" | "max_completion_tokens")
                && let Some(raw) = value.as_u64()
                && raw > u32::MAX as u64
            {
                return Err(RequestError::Param(router::ParamViolation::MaxTokensOutOfRange {
                    value: raw,
                    max: u32::MAX as u64,
                }));
            }
        }
    }
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
    let mut prompt_tokens = estimate_prompt_tokens(&req.messages);
    // The reservation also covers what the vendor adds around the prompt.
    // `prompt_tokens` alone stays the billing floor and the rate-limit count:
    // the overhead is reserved, never billed unless the vendor reports it.
    let reserved_prompt_tokens = prompt_tokens.saturating_add(resolved.config.prompt_overhead_tokens);
    let mut cost = CostEstimate {
        prompt_nano_usd: estimate_cost_nano_usd(&resolved.config.cost, reserved_prompt_tokens, 0),
        completion_nano_usd: estimate_cost_nano_usd(&resolved.config.cost, 0, max_output_tokens),
    };
    let unfolded_cost = cost;
    //    A repeated Idempotency-Key is decided here too, by the ledger, in the
    //    same step as the budget: a duplicate never reaches the provider.
    if decision.tenant.require_idempotency_key && idempotency_key.is_none() {
        return Err(RequestError::IdempotencyKeyRequired);
    }
    // The idempotency and hold scope. Each dialect has its own, so a key
    // reused across formats never replays an answer in the wrong shape.
    let scope = match (dialect, mode) {
        (Dialect::Gateway, Mode::Stream) => "stream",
        (Dialect::Gateway, Mode::Complete) => "complete",
        (Dialect::OpenAi, Mode::Stream) => "openai-stream",
        (Dialect::OpenAi, Mode::Complete) => "openai-complete",
    };
    //    HOLD comes before admission. A request the tenant's policy says must
    //    wait is recorded as a hold instead: no reservation, no charge, no
    //    provider call. A request presenting an approved hold is admitted
    //    below, and the ledger consumes the approval with the budget.
    let hold_reason = match (&decision.tenant.holds, hold_id) {
        (Some(policy), None) => policy.trigger(&resolved.registry_id, cost.total()),
        _ => None,
    };
    // Parsed once, only if something fingerprints it. The body already
    // parsed as a `ChatRequest`, so it is valid JSON.
    let value: Value = if idempotency_key.is_some() || hold_id.is_some() || hold_reason.is_some() {
        serde_json::from_slice(body).unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    let chain = if decision.tenant.receipts {
        let book = state.receipts.as_ref().ok_or(RequestError::ReceiptUnavailable)?;
        Some(book.chain(&principal.tenant_id).map_err(|_| RequestError::ReceiptUnavailable)?)
    } else {
        None
    };
    // One salt per request, made before any fold so a summarizer's receipt
    // and the request's own share it, each digest under its own domain.
    let receipt_salt = match &chain {
        Some(_) => Some(receipts::Salt::generate().map_err(|_| RequestError::ReceiptUnavailable)?),
        None => None,
    };
    let call_digest = receipt_salt.as_ref().map(|salt| receipts::request_digest(salt, body));
    if let (Some(reason), Some(policy)) = (hold_reason, &decision.tenant.holds) {
        if let Some(chain) = &chain {
            chain
                .await_approval(&Subject {
                    request_id: request_id.to_string(),
                    tenant_id: principal.tenant_id.to_string(),
                    key_id: principal.key_id.to_string(),
                    model: resolved.registry_id.clone(),
                    action: "hold".into(),
                    // No salt reaches the client on a 202, so no digest: the
                    // approved resubmission gets its own allow with one.
                    call_digest: None,
                })
                .await
                .map_err(|_| RequestError::ReceiptUnavailable)?;
        }
        let fingerprint = state.limits.fingerprinter().encodings(scope, &value).remove(0);
        let hold = state
            .limits
            .ledger()
            .create_hold(NewHold {
                // The held request's id is the hold's id.
                id: Uuid::parse_str(request_id).unwrap_or_else(|_| Uuid::new_v4()),
                tenant_id: principal.tenant_id.to_string(),
                requested_by: principal.key_id.to_string(),
                endpoint: scope.to_string(),
                model: resolved.registry_id.clone(),
                max_output_tokens,
                exposure_nano_usd: cost.total(),
                reason,
                fingerprint,
                expires_in: std::time::Duration::from_secs(policy.expiry_secs),
                approval_valid_for: std::time::Duration::from_secs(policy.approval_valid_secs),
                max_pending: policy.max_pending,
            })
            .await
            .map_err(LimitError::from)?;
        return Err(RequestError::Held(Box::new(hold)));
    }

    // Holds are deliberately resolved before folding: a held request must not
    // spend a summarizer reservation, and idempotency still fingerprints the
    // original client body below.
    let fold = foreground_fold(
        state,
        &generation,
        &decision,
        &resolved,
        &principal.key_id,
        &req.messages,
        match (&chain, &receipt_salt, &call_digest) {
            (Some(chain), Some(salt), Some(call_digest)) => Some(FoldReceipts {
                chain: Arc::clone(chain),
                salt: salt.clone(),
                call_digest: call_digest.clone(),
                request_id: format!("{request_id}:fold"),
                tenant_id: principal.tenant_id.to_string(),
                key_id: principal.key_id.to_string(),
            }),
            _ => None,
        }
        .as_ref(),
    )
    .await;
    if let Some(facts) = &fold.facts {
        span.fold = Some(serde_json::to_string(facts).unwrap_or_else(|_| "{}".to_string()));
    }
    if let Some(attempt) = fold.attempt {
        state.metrics.record_fold(
            &decision.tenant.tenant_id,
            attempt.outcome,
            attempt.prompt_tokens,
            attempt.completion_tokens,
        );
    }
    if fold.folded_turns > 0 {
        let cut = fold.facts.as_ref().map(|facts| facts.prompt_tokens_cut).unwrap_or(0);
        state.metrics.record_folded_request(&decision.tenant.tenant_id, cut);
    }
    req.messages = fold.messages;
    let folded_turns = fold.folded_turns;
    let fold_receipt_id = fold.fold_receipt_id;
    // The summary actually forwarded, read back from the request itself, so a
    // receipt binds exactly what the model is sent.
    let fold_summary = if folded_turns > 0 { crate::folding::plan::sent_summary(&req.messages) } else { None };
    prompt_tokens = estimate_prompt_tokens(&req.messages);
    let reserved_prompt_tokens = prompt_tokens.saturating_add(resolved.config.prompt_overhead_tokens);
    cost = CostEstimate {
        prompt_nano_usd: estimate_cost_nano_usd(&resolved.config.cost, reserved_prompt_tokens, 0),
        completion_nano_usd: estimate_cost_nano_usd(&resolved.config.cost, 0, max_output_tokens),
    };
    let hold = hold_id.map(|id| HoldClaim {
        id,
        fingerprints: state.limits.fingerprinter().encodings(scope, &value),
        model: resolved.registry_id.clone(),
        exposure_nano_usd: unfolded_cost.total(),
        key_id: principal.key_id.to_string(),
    });
    let idempotency = idempotency_key.map(|key| state.limits.fingerprinter().claim(key, scope, &value));
    let mut reservation = state
        .limits
        .admit_request(AdmitRequest {
            id: Uuid::parse_str(request_id).unwrap_or_else(|_| Uuid::new_v4()),
            tenant_id: &principal.tenant_id,
            limits: &decision.limits,
            prompt_tokens,
            max_output_tokens,
            estimate: cost,
            idempotency,
            hold,
        })
        .await?;
    let receipt = if let Some(chain) = &chain {
        let written: Result<OpenReceipt, ReceiptError> = async {
            let salt = receipt_salt.clone().ok_or(ReceiptError::Unavailable)?;
            let call_digest = call_digest.clone().ok_or(ReceiptError::Unavailable)?;
            let decision_receipt_id = chain
                .allow(&Subject {
                    request_id: request_id.to_string(),
                    tenant_id: principal.tenant_id.to_string(),
                    key_id: principal.key_id.to_string(),
                    model: resolved.registry_id.clone(),
                    action: "reserve".into(),
                    call_digest: Some(call_digest.clone()),
                })
                .await?;
            // The allow binds what the client sent. The outcome binds what the
            // model was sent: the same digest for an unfolded call, and for a
            // folded one the client's digest, the turns replaced and the summary.
            let request_digest = match &fold_summary {
                Some(summary) => receipts::folded_request_digest(&salt, &call_digest, folded_turns, summary),
                None => call_digest,
            };
            Ok(OpenReceipt {
                chain: std::sync::Arc::clone(chain),
                decision_receipt_id,
                request_digest,
                salt,
            })
        }
        .await;
        match written {
            Ok(open) => Some(open),
            Err(_) => {
                reservation.release();
                return Err(RequestError::ReceiptUnavailable);
            }
        }
    } else {
        None
    };

    // OpenAI clients always get the normalized stream, re-labelled as
    // OpenAI chunks; passthrough would hand them a vendor's own dialect.
    let framing = match (dialect, &req.framing) {
        (Dialect::OpenAi, _) => Framing::Normalized,
        (Dialect::Gateway, Some(explicit)) => Framing::parse(Some(explicit)),
        (Dialect::Gateway, None) => Framing::parse(headers.get("x-gateway-framing").and_then(|v| v.to_str().ok())),
    };
    let openai = (dialect == Dialect::OpenAi).then(|| OpenAiReply {
        id: format!("chatcmpl-{request_id}"),
        created: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default(),
        model: resolved.registry_id.clone(),
        include_usage,
    });

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
        // Only a receipts tenant gets the summary back: it needs it to check
        // its outcome. For anyone else it would only make the response larger.
        fold_summary: if receipt.is_some() { fold_summary } else { None },
        fold_receipt_id,
        receipt,
        openai,
        folded_turns,
    })
}

/// Read an OpenAI Chat Completions request. `messages` already uses
/// OpenAI's shape. `stream_options.include_usage` decides whether a final
/// usage chunk is sent. `n` above 1 is refused, and `store` / `metadata`
/// (OpenAI's own storage options) are not forwarded. Every other field
/// becomes a parameter, so `max_tokens`, `temperature`, `tools` and the
/// rest are governed and forwarded exactly as on `/v1/chat/stream`.
fn openai_request(body: &[u8]) -> Result<(ChatRequest, bool), RequestError> {
    let value: Value = serde_json::from_slice(body).map_err(|e| RequestError::BadJson(e.to_string()))?;
    let Value::Object(mut fields) = value else {
        return Err(RequestError::BadJson("the request body must be a JSON object".into()));
    };
    let model = match fields.remove("model") {
        Some(Value::String(model)) => model,
        _ => String::new(),
    };
    let messages: Vec<ChatMessage> = match fields.remove("messages") {
        Some(messages) => serde_json::from_value(messages).map_err(|e| RequestError::BadJson(e.to_string()))?,
        None => Vec::new(),
    };
    // The fields that decide what kind of answer is sent are checked, not
    // coerced: a wrong type is a 400, as on OpenAI's API, never a guess.
    // `null` means "not set" there, and here.
    let stream = match fields.remove("stream") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(stream)) => Some(stream),
        Some(_) => return Err(RequestError::InvalidField("`stream` must be true or false")),
    };
    let include_usage = match fields.remove("stream_options") {
        None | Some(Value::Null) => false,
        Some(Value::Object(options)) => {
            if stream != Some(true) {
                return Err(RequestError::InvalidField("`stream_options` is only allowed when `stream` is true"));
            }
            match options.get("include_usage") {
                None | Some(Value::Null) => false,
                Some(Value::Bool(include)) => *include,
                Some(_) => {
                    return Err(RequestError::InvalidField("`stream_options.include_usage` must be true or false"));
                }
            }
        }
        Some(_) => return Err(RequestError::InvalidField("`stream_options` must be an object")),
    };
    match fields.remove("n") {
        None | Some(Value::Null) => {}
        Some(n) if n.as_u64() == Some(1) => {}
        Some(n) if n.as_u64().is_some() => return Err(RequestError::Unsupported("only `n: 1` is supported")),
        Some(_) => return Err(RequestError::InvalidField("`n` must be a whole number")),
    }
    fields.remove("store");
    fields.remove("metadata");
    let request = ChatRequest {
        model,
        messages,
        stream,
        params: Some(fields),
        framing: None,
        metadata: None,
    };
    Ok((request, include_usage))
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
            if let Some(open) = &admitted.receipt {
                failed_outcome(open).await?;
            }
            return Err(err.into());
        }
    };

    let mut headers = response_headers(
        span,
        &admitted.resolved,
        &admitted.principal,
        &admitted.reservation,
        admitted.folded_turns,
        admitted.fold_summary.as_deref(),
        admitted.fold_receipt_id.as_deref(),
    );
    insert_receipt_headers(&mut headers, admitted.receipt.as_ref());
    let reply_hash = admitted.receipt.as_ref().map(|open| ReplyDigest::new(open.salt.clone()));
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
        open_receipt: admitted.receipt,
        reply_hash,
        openai: admitted.openai,
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
    folded_turns: usize,
    fold_summary: Option<&str>,
    fold_receipt_id: Option<&str>,
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
    if folded_turns > 0 {
        h.insert(
            HeaderName::from_static("x-gateway-folded"),
            header_value(&folded_turns.to_string()),
        );
    }
    if let Some(summary) = fold_summary {
        h.insert(HeaderName::from_static("x-gateway-fold-summary"), fold_summary_header(summary));
    }
    if let Some(receipt_id) = fold_receipt_id {
        h.insert(HeaderName::from_static("x-gateway-fold-receipt-id"), header_value(receipt_id));
    }
    h
}

/// `x-gateway-fold-summary`: the summary's UTF-8 bytes, base64url without
/// padding, so any text survives as a header value.
fn fold_summary_header(summary: &str) -> HeaderValue {
    use base64::Engine as _;
    header_value(&base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(summary.as_bytes()))
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
    open_receipt: Option<OpenReceipt>,
    /// Running digest of the reply actually handed to the client. Absent
    /// when this stream has no receipt to close.
    reply_hash: Option<ReplyDigest>,
    /// Set when the client spoke OpenAI's format: frames go out as
    /// `chat.completion.chunk` data lines instead of the v1 events.
    openai: Option<OpenAiReply>,
}

impl NormalizedState {
    /// Queue the closing sequence: an optional extra frame (an `error`, say),
    /// then `end`, then `[DONE]`. Idempotent, so a stream cannot emit two
    /// terminators.
    async fn emit_terminal(&mut self, extra: Option<Event>) {
        if self.finished {
            return;
        }
        self.finished = true;

        let cost = self.settle();
        if let Some(open) = self.open_receipt.clone() {
            let failed = extra.is_some() || self.summary.error_code.is_some();
            let status = if failed { "failed" } else { "completed" };
            let response = self.reply_hash.as_ref().map(ReplyDigest::finish);
            // Waits on the receipt writer without holding a runtime thread.
            if open.chain.outcome(&open, status, response).await.is_err() {
                // The successful close is the reply. Withhold it.
                let event = match &self.openai {
                    Some(_) => OpenAiReply::error("receipt_unavailable", "the receipt could not be written"),
                    None => receipt_error_event(&self.request_id),
                };
                self.pending = vec![event].into_iter();
                self.emit_summary();
                return;
            }
        }

        let mut out: Vec<Event> = Vec::with_capacity(3);
        if let Some(openai) = &self.openai {
            match extra {
                Some(event) => out.push(event),
                None => {
                    let finish = self.summary.finish_reason.clone().unwrap_or_else(|| "stop".to_string());
                    out.push(openai.chunk(json!({}), Some(&finish)));
                }
            }
            if openai.include_usage {
                out.push(openai.usage_chunk(&self.summary));
            }
            out.push(Event::default().data("[DONE]"));
            self.pending = out.into_iter();
            self.emit_summary();
            return;
        }
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
            if let Some(openai) = &state.openai {
                return Some(Ok(openai.chunk(json!({ "role": "assistant", "content": "" }), None)));
            }
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
                state.emit_terminal(None).await;
            }
            Some(Err(err)) => {
                // A failure after streaming began cannot change the status
                // code, so it is reported in-band on its own event and the
                // stream still terminates cleanly.
                state.upstream_done = true;
                tracing::warn!(
                    request_id = %state.request_id,
                    tenant = %state.tenant,
                    code = err.code(),
                    detail = %logging::redact_secrets(&err.to_string()),
                    "upstream failed mid-stream"
                );
                state.summary.error_code = Some(err.code());
                state.summary.error_retryable = err.retryable();
                let event = match &state.openai {
                    Some(_) => OpenAiReply::error(err.code(), &err.client_message()),
                    None => Event::default()
                        .event("error")
                        .data(error_payload(&err, &state.request_id)),
                };
                state.emit_terminal(Some(event)).await;
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
                    state.emit_terminal(None).await;
                    continue;
                }

                // One upstream frame can carry content, reasoning and a tool
                // call at once, so build the full set before yielding one.
                let mut out: Vec<Event> = Vec::with_capacity(2);
                match &event {
                    StreamEvent::Started { upstream_id, upstream_model } => {
                        state.summary.upstream_id.clone_from(upstream_id);
                        state.summary.upstream_model.clone_from(upstream_model);
                        // OpenAI's format has no place for upstream ids.
                        if state.openai.is_none() {
                            out.push(
                                Event::default()
                                    .event("meta")
                                    .data(meta_payload(upstream_id, upstream_model)),
                            );
                        }
                    }
                    StreamEvent::Delta { content, reasoning } => {
                        for text in [content, reasoning].into_iter().flatten() {
                            state.observe_output(text);
                        }
                        if let Some(reply) = state.reply_hash.as_mut() {
                            if let Some(text) = content {
                                reply.content(text);
                            }
                            if let Some(text) = reasoning {
                                reply.reasoning(text);
                            }
                        }
                        if let Some(openai) = &state.openai {
                            // Reasoning goes in `reasoning_content`, the field
                            // OpenAI-compatible providers use for it.
                            if let Some(token) = content {
                                out.push(openai.chunk(json!({ "content": token }), None));
                            }
                            if let Some(think) = reasoning {
                                out.push(openai.chunk(json!({ "reasoning_content": think }), None));
                            }
                        } else {
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
                    }
                    StreamEvent::ToolCallDelta { index, id, name, arguments } => {
                        for text in [name, arguments].into_iter().flatten() {
                            state.observe_output(text);
                        }
                        if let Some(reply) = state.reply_hash.as_mut() {
                            reply.tool(*index, id.as_deref(), name.as_deref(), arguments.as_deref());
                        }
                        if let Some(openai) = &state.openai {
                            let mut call = json!({ "index": index, "function": {} });
                            if let Some(id) = id {
                                call["id"] = json!(id);
                                call["type"] = json!("function");
                            }
                            if let Some(name) = name {
                                call["function"]["name"] = json!(name);
                            }
                            if let Some(arguments) = arguments {
                                call["function"]["arguments"] = json!(arguments);
                            }
                            out.push(openai.chunk(json!({ "tool_calls": [call] }), None));
                        } else {
                            out.push(
                                Event::default()
                                    .event("tool")
                                    .data(tool_json(*index, id, name, arguments)),
                            );
                        }
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
            self.finished = true;
            self.summary.aborted_by_client = true;
            self.settle();
            if let Some(open) = &self.open_receipt {
                // Nobody is left to withhold from; the writer still records it.
                let response = self.reply_hash.as_ref().map(ReplyDigest::finish);
                open.chain.outcome_detached(open, "failed", response);
            }
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
/// so a client can branch without string matching. The message is the generic
/// one; the vendor's detail was logged where the failure was seen.
fn error_payload(err: &ProviderError, request_id: &str) -> String {
    json!({
        "error": {
            "code": err.code(),
            "message": err.client_message(),
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
fn receipt_error_event(request_id: &str) -> Event {
    Event::default().event("error").data(
        json!({
            "error": {
                "code": "receipt_unavailable",
                "message": "the receipt could not be written",
                "retryable": true,
            },
            "request_id": request_id,
        })
        .to_string(),
    )
}

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
    let mut headers = response_headers(
        span,
        &admitted.resolved,
        &admitted.principal,
        &admitted.reservation,
        admitted.folded_turns,
        admitted.fold_summary.as_deref(),
        admitted.fold_receipt_id.as_deref(),
    );
    insert_receipt_headers(&mut headers, admitted.receipt.as_ref());
    let receipt = admitted.receipt;
    // Counted as in flight for the whole wait, so a graceful shutdown drains
    // a completion the same way it drains a stream. A client that hangs up
    // while waiting leaves its allow without an outcome: whether the
    // upstream finished is unknown, and the log says exactly that.
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
            if let Some(open) = &receipt {
                failed_outcome(open).await?;
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
    let payload = match &admitted.openai {
        Some(openai) => openai_completion_payload(&completion, &usage, openai),
        None => completion_payload(&completion, &usage, cost, &guard, admitted.metadata.as_ref()),
    };
    // The outcome is durable before the answer goes anywhere: if it cannot
    // be written the answer is withheld, and it is not kept for an
    // idempotent replay either, so a retry cannot fetch what was withheld.
    // The provider did the work, so the usage is still billed.
    if let Some(open) = &receipt {
        let mut reply = ReplyDigest::new(open.salt.clone());
        reply.content(&completion.content);
        reply.reasoning(completion.reasoning.as_deref().unwrap_or_default());
        for (index, call) in completion.tool_calls.iter().enumerate() {
            reply.tool(
                u32::try_from(index).unwrap_or(u32::MAX),
                Some(&call.id),
                Some(&call.function.name),
                Some(&call.function.arguments),
            );
        }
        if open.chain.outcome(open, "completed", Some(reply.finish())).await.is_err() {
            guard.reservation.settle(usage, &guard.cost_model);
            guard.record(true);
            return Err(RequestError::ReceiptUnavailable);
        }
    }
    // The answer goes into the ledger with the settlement, so a repeat under
    // the same Idempotency-Key is served from there. With receipts on, the
    // receipt id and salt are kept with it (sealed at rest like the answer),
    // so the replay can name the receipt that covers it.
    let stored = match &receipt {
        Some(open) => stored_with_receipt(
            &payload,
            open,
            admitted.folded_turns,
            admitted.fold_summary.as_deref(),
            admitted.fold_receipt_id.as_deref(),
        ),
        None if admitted.folded_turns > 0 => stored_with_folding(&payload, admitted.folded_turns),
        None => payload.clone(),
    };
    guard.reservation.keep_response(stored);
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

/// The answer in OpenAI's `chat.completion` shape. `content` is null when
/// the model answered with tool calls only, as OpenAI's is; reasoning, when
/// there is any, is in `reasoning_content`.
fn openai_completion_payload(completion: &Completion, usage: &Usage, openai: &OpenAiReply) -> String {
    let mut message = json!({
        "role": "assistant",
        "content": if completion.content.is_empty() && !completion.tool_calls.is_empty() {
            Value::Null
        } else {
            json!(completion.content)
        },
    });
    if !completion.tool_calls.is_empty() {
        message["tool_calls"] = json!(completion.tool_calls);
    }
    if let Some(reasoning) = &completion.reasoning {
        message["reasoning_content"] = json!(reasoning);
    }
    json!({
        "id": openai.id,
        "object": "chat.completion",
        "created": openai.created,
        "model": openai.model,
        "choices": [{ "index": 0, "message": message, "finish_reason": completion.finish_reason }],
        "usage": {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens,
            "completion_tokens_details": { "reasoning_tokens": usage.reasoning_tokens.unwrap_or(0) },
        },
    })
    .to_string()
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
            if let Some(open) = &admitted.receipt {
                failed_outcome(open).await?;
            }
            return Err(err.into());
        }
    };

    let mut headers = response_headers(
        span,
        &admitted.resolved,
        &admitted.principal,
        &admitted.reservation,
        admitted.folded_turns,
        admitted.fold_summary.as_deref(),
        admitted.fold_receipt_id.as_deref(),
    );
    headers.insert(
        HeaderName::from_static("x-gateway-framing"),
        HeaderValue::from_static("passthrough"),
    );
    insert_receipt_headers(&mut headers, admitted.receipt.as_ref());

    let reply_hash = admitted.receipt.as_ref().map(|open| RawReplyDigest::new(&open.salt));
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
            open_receipt: admitted.receipt,
            reply_hash,
            closing: None,
            done: false,
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
    open_receipt: Option<OpenReceipt>,
    /// Digest of the raw bytes relayed. Passthrough never parses them.
    reply_hash: Option<RawReplyDigest>,
    /// The `completed` outcome on its way to disk. The stream ends only once
    /// it is durable; if it fails, the client gets an error instead of a
    /// clean end.
    closing: Option<tokio::sync::oneshot::Receiver<Result<String, ReceiptError>>>,
    /// Nothing more to yield: never poll `inner` again.
    done: bool,
}

impl Stream for RawStream {
    type Item = Result<Bytes, ProviderError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.closing.is_some() {
            return this.poll_closing(cx);
        }
        if this.done {
            return Poll::Ready(None);
        }
        match this.inner.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                if !this.finished {
                    this.finish(false);
                }
                if this.closing.is_some() {
                    return this.poll_closing(cx);
                }
                this.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(err))) => {
                if !this.finished {
                    tracing::warn!(
                        request_id = %this.span.request_id,
                        tenant = %this.tenant,
                        code = err.code(),
                        detail = %logging::redact_secrets(&err.to_string()),
                        "upstream failed mid-stream"
                    );
                    this.summary.error_code = Some(err.code());
                    this.summary.error_retryable = err.retryable();
                    this.finish(true);
                }
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(Some(Ok(bytes))) => {
                if this.first {
                    this.first = false;
                    this.span.mark_first_token();
                }
                if let Some(reply) = this.reply_hash.as_mut() {
                    reply.update(&bytes);
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
            self.finish(true);
        }
        self.inflight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl RawStream {
    /// Wait for the `completed` outcome, then end cleanly, or end with an
    /// error if it could not be written.
    fn poll_closing(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes, ProviderError>>> {
        let Some(closing) = self.closing.as_mut() else {
            return Poll::Ready(None);
        };
        match Pin::new(closing).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(answer) => {
                self.closing = None;
                self.done = true;
                if matches!(answer, Ok(Ok(_))) {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Err(ProviderError::Stream(
                        "the receipt could not be written".into(),
                    ))))
                }
            }
        }
    }

    fn finish(&mut self, failed: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.reservation.commit_reserved();
        if let Some(open) = &self.open_receipt {
            let failed = failed || self.summary.error_code.is_some() || self.summary.aborted_by_client;
            let response = self.reply_hash.as_ref().map(RawReplyDigest::finish);
            if failed {
                // Already ending in an error, or nobody is left: record it
                // without waiting.
                open.chain.outcome_detached(open, "failed", response);
            } else {
                self.closing = Some(open.chain.outcome_pending(open, "completed", response));
            }
        }
        self.emit();
    }

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
