
use std::pin::Pin;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http_body_util::BodyExt;
use onwards::auth::PresentedKey;
use onwards::serving::BillingPrices;
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::crypto::ApiKeyHash;
use crate::prompt_cache::sse::SseBufferedStream;

use super::layer::{ContinuationState, RequestContext};
use super::metrics;
use super::render::{RenderError, RenderPrefix, RenderedPrefix};

pub type LegStream = Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>;

pub struct Leg {
    pub render: RenderedPrefix,
    pub stream: LegStream,
    pub deadline: tokio::time::Instant,
}

#[derive(Debug, thiserror::Error)]
pub enum LegError {
    #[error("render failed: {0}")]
    Render(#[from] RenderError),
    #[error("max_tokens is already exhausted by the partial generation")]
    MaxTokensReached(RenderedPrefix),
    #[error("the resume attempt exceeded its deadline")]
    Deadline,
    #[error("the resume leg returned {0}")]
    Upstream(StatusCode),
    #[error("the resume leg did not return a stream")]
    NotStreaming,
}

impl LegError {
    pub fn reason(&self) -> &'static str {
        match self {
            LegError::Render(_) => "render_failed",
            LegError::MaxTokensReached(_) => "max_tokens_reached",
            LegError::Deadline => "deadline",
            LegError::Upstream(_) => "leg_error",
            LegError::NotStreaming => "leg_not_streaming",
        }
    }
}

pub fn resume_path(original: &str) -> String {
    match original.strip_suffix("chat/completions") {
        Some(prefix) => format!("{prefix}completions"),
        None => original.to_string(),
    }
}

pub fn build_leg_body(ctx: &RequestContext, token_ids: &[u32], max_tokens: Option<u32>, priority: i32) -> Value {
    let mut body = json!({
        "model": ctx.model,
        "prompt": token_ids,
        "stream": true,
        "stream_options": {"include_usage": true},
        "nvext": {"agent_hints": {"priority": priority}},
    });
    if let Some(max_tokens) = max_tokens {
        body["max_tokens"] = json!(max_tokens);
    }
    for key in [
        "temperature",
        "top_p",
        "seed",
        "stop",
        "logit_bias",
        "frequency_penalty",
        "presence_penalty",
    ] {
        if let Some(value) = ctx.body.get(key).filter(|v| !v.is_null()) {
            body[key] = value.clone();
        }
    }
    body
}

fn leg_prices(original: BillingPrices) -> BillingPrices {
    BillingPrices {
        prompt: 0.0,
        cached: 0.0,
        output: original.output,
    }
}

fn build_leg_request(path: &str, key_hash: &ApiKeyHash, body: &Value, prices: Option<BillingPrices>) -> Request<Body> {
    let bytes = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(resume_path(path))
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "text/event-stream")
        .body(Body::from(bytes))
        .expect("a POST with static headers always builds");
    request
        .extensions_mut()
        .insert(PresentedKey(onwards::auth::KeyHash::from_bytes(*key_hash)));
    if let Some(prices) = prices {
        request.extensions_mut().insert(prices);
    }
    request
}

fn is_sse(response: &axum::response::Response) -> bool {
    response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|ct| ct.trim().eq_ignore_ascii_case("text/event-stream"))
}

pub async fn attempt(state: &ContinuationState, ctx: &RequestContext, continuation_text: &str, attempt_no: u32) -> Result<Leg, LegError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(state.cfg.resume_deadline_secs);

    let render_kwargs = ctx.render_kwargs();
    let prefix = RenderPrefix {
        virtual_model: &ctx.model,
        messages: ctx.messages(),
        tools: ctx.tools(),
        chat_template_kwargs: render_kwargs.as_ref(),
        continuation_text,
    };
    let render = match tokio::time::timeout_at(deadline, state.tokenizer.render(&prefix)).await {
        Err(_) => return Err(LegError::Deadline),
        Ok(result) => result?,
    };

    let max_tokens = match ctx.max_tokens() {
        Some(requested) => {
            let generated = render.continuation_tokens.unwrap_or(0);
            let remaining = requested.saturating_sub(generated);
            if remaining == 0 {
                return Err(LegError::MaxTokensReached(render));
            }
            Some(remaining)
        }
        None => None,
    };

    let body = build_leg_body(ctx, &render.token_ids, max_tokens, state.cfg.priority);
    let request = build_leg_request(&ctx.path, &state.key_hash, &body, ctx.prices.map(leg_prices));

    metrics::record_resume_leg(&ctx.model, attempt_no);
    metrics::record_eaten_prompt_tokens(&ctx.model, render.token_ids.len() as u64);

    let response = match tokio::time::timeout_at(deadline, state.resume_target.clone().oneshot(request)).await {
        Err(_) => return Err(LegError::Deadline),
        Ok(Err(_)) => return Err(LegError::NotStreaming),
        Ok(Ok(response)) => response,
    };

    if !response.status().is_success() {
        return Err(LegError::Upstream(response.status()));
    }
    if !is_sse(&response) {
        return Err(LegError::NotStreaming);
    }
    let body = BodyExt::into_data_stream(response.into_body()).map(|r| r.map_err(std::io::Error::other));
    Ok(Leg {
        render,
        stream: Box::pin(SseBufferedStream::with_limit(body, state.sse_buffer_limit)),
        deadline,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(body: Value) -> RequestContext {
        RequestContext {
            model: "dsv4-flash".to_string(),
            body,
            path: "/chat/completions".to_string(),
            response_id: None,
            route: crate::continuation::RouteInfo::default(),
            origin: "realtime",
            prices: None,
        }
    }

    #[test]
    fn resume_path_swaps_only_the_endpoint_suffix() {
        assert_eq!(resume_path("/chat/completions"), "/completions");
        assert_eq!(resume_path("/v1/chat/completions"), "/v1/completions");
        assert_eq!(resume_path("/ai/v1/chat/completions"), "/ai/v1/completions");
        assert_eq!(resume_path("/embeddings"), "/embeddings");
    }

    #[test]
    fn leg_body_is_token_ids_streaming_and_priority_bearing() {
        let body = build_leg_body(&ctx(json!({"model": "dsv4-flash", "stream": true})), &[1, 2, 3], Some(400), 100);
        assert_eq!(body["model"], "dsv4-flash");
        assert_eq!(body["prompt"], json!([1, 2, 3]), "a flat token-id array, never a string prompt");
        assert_eq!(body["stream"], true);
        assert_eq!(body["stream_options"]["include_usage"], true);
        assert_eq!(body["nvext"]["agent_hints"]["priority"], 100);
        assert!(body.get("priority").is_none(), "top-level priority is not a dynamo field");
        assert_eq!(body["max_tokens"], 400);
    }

    #[test]
    fn leg_body_passes_sampling_through_and_omits_absent_fields() {
        let original = json!({
            "model": "dsv4-flash", "stream": true,
            "temperature": 0.7, "top_p": 0.95, "seed": 42,
            "stop": ["\n\n"], "logit_bias": {"50256": -100},
            "frequency_penalty": 0.5, "presence_penalty": 0.25
        });
        let body = build_leg_body(&ctx(original), &[9], None, 100);
        assert_eq!(body["temperature"], 0.7);
        assert_eq!(body["top_p"], 0.95);
        assert_eq!(body["seed"], 42);
        assert_eq!(body["stop"], json!(["\n\n"]));
        assert_eq!(body["logit_bias"]["50256"], -100);
        assert_eq!(body["frequency_penalty"], 0.5);
        assert_eq!(body["presence_penalty"], 0.25);
        assert!(body.get("max_tokens").is_none(), "an unbounded request stays unbounded");
        assert!(body.get("messages").is_none());
    }

    #[test]
    fn leg_body_omits_null_sampling_fields() {
        let body = build_leg_body(
            &ctx(json!({"model": "dsv4-flash", "temperature": null, "frequency_penalty": null, "presence_penalty": null})),
            &[9],
            None,
            100,
        );
        for key in ["temperature", "frequency_penalty", "presence_penalty"] {
            assert!(body.get(key).is_none(), "{key} was sent as null and must not be forwarded");
        }
    }

    #[test]
    fn leg_request_carries_the_global_key_and_asks_for_a_stream() {
        let body = build_leg_body(&ctx(json!({})), &[1], None, 100);
        let key = crate::crypto::hash_api_key("dw-continuation-secret");
        let request = build_leg_request("/v1/chat/completions", &key, &body, None);
        assert_eq!(request.method(), Method::POST);
        assert_eq!(request.uri().path(), "/v1/completions");
        assert_eq!(
            request.extensions().get::<PresentedKey>(),
            Some(&PresentedKey(onwards::auth::KeyHash::from_bytes(key))),
            "the leg authenticates as the global continuation key, not as the customer"
        );
        assert!(request.headers().get(header::AUTHORIZATION).is_none(), "no secret on the leg");
        assert_eq!(request.headers().get(header::CONTENT_TYPE).unwrap(), "application/json");
        assert_eq!(request.headers().get(header::ACCEPT).unwrap(), "text/event-stream");
        assert!(request.extensions().get::<BillingPrices>().is_none());

        let prices = BillingPrices {
            prompt: 1e-6,
            cached: 1e-7,
            output: 3e-6,
        };
        let request = build_leg_request("/v1/chat/completions", &key, &body, Some(prices));
        assert_eq!(
            request.extensions().get::<BillingPrices>(),
            Some(&prices),
            "the leg is sent at the original request's prices, not the continuation key's"
        );
    }
}
