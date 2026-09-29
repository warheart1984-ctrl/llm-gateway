//! A stand-in LLM provider for the demo: speaks the OpenAI streaming dialect
//! on `POST /v1/chat/completions`, one word per frame at a readable pace, and
//! reports usage on the terminal chunk the way the real vendors do.
//!
//! ```text
//! MOCK_ADDR=0.0.0.0:9000 cargo run --example mock_upstream
//! ```
//!
//! It never calls anything and costs nothing, so the gateway's governance can
//! be exercised end to end without a provider account.

use std::{convert::Infallible, net::SocketAddr, time::Duration};

use axum::{
    Json, Router,
    body::Body,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::{Value, json};

const ANSWER: &str = "The gateway reserved this answer's cost before the first byte, \
streamed it token by token, and will settle the bill from the usage on the final chunk.";

#[tokio::main]
async fn main() {
    let addr: SocketAddr = std::env::var("MOCK_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9000".into())
        .parse()
        .expect("MOCK_ADDR must be host:port");
    let delay = Duration::from_millis(
        std::env::var("MOCK_TOKEN_DELAY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(40),
    );

    let app = Router::new().route(
        "/v1/chat/completions",
        post(move |body: Json<Value>| completions(body, delay)),
    );
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind mock");
    println!("mock upstream listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.expect("serve mock");
}

async fn completions(Json(body): Json<Value>, delay: Duration) -> Response {
    let max_tokens = body["max_tokens"].as_u64().unwrap_or(64) as usize;
    let model = body["model"].as_str().unwrap_or("mock-model").to_string();
    // Rough prompt count, so the usage frame differs from the gateway's
    // estimate the way a real tokenizer's would.
    let prompt_tokens = body["messages"]
        .as_array()
        .map(|m| m.iter().map(|m| m["content"].as_str().unwrap_or("").len() / 4 + 4).sum())
        .unwrap_or(8);

    if body["stream"] != json!(true) {
        return (StatusCode::BAD_REQUEST, "this mock only streams").into_response();
    }

    let words: Vec<String> = ANSWER
        .split_inclusive(' ')
        .take(max_tokens)
        .map(str::to_string)
        .collect();
    let completion_tokens = words.len();
    let finish = if completion_tokens < ANSWER.split(' ').count() { "length" } else { "stop" };

    let mut frames: Vec<String> = words
        .into_iter()
        .map(|w| frame(json!({
            "id": "mock-1", "object": "chat.completion.chunk", "model": model,
            "choices": [{ "index": 0, "delta": { "content": w } }]
        })))
        .collect();
    frames.push(frame(json!({
        "id": "mock-1", "model": model,
        "choices": [{ "index": 0, "delta": {}, "finish_reason": finish }]
    })));
    frames.push(frame(json!({
        "id": "mock-1", "model": model, "choices": [],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens
        }
    })));
    frames.push("data: [DONE]\n\n".to_string());

    let stream = futures_util::stream::unfold(frames.into_iter(), move |mut frames| async move {
        let next = frames.next()?;
        tokio::time::sleep(delay).await;
        Some((Ok::<_, Infallible>(next), frames))
    });
    let mut response = Response::new(Body::from_stream(stream));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("text/event-stream"));
    response
}

fn frame(payload: Value) -> String {
    format!("data: {payload}\n\n")
}
