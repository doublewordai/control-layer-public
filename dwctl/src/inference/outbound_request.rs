
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde_json::Value;
use tracing::{debug, warn};

use super::serving_name::ServingName;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamTimeouts {
    pub first_chunk: Duration,
    pub chunk: Duration,
    pub body: Duration,
}

#[derive(Clone)]
pub struct OutboundConfig {
    pub timeouts: StreamTimeouts,
    pub key_policy: crate::sync::key_policy::KeyPolicyCache,
}

pub const STREAM_MARKER_KEY: &str = "stream";

pub const STREAM_MARKER_HEADER: &str = "x-fusillade-batch-stream";

impl StreamTimeouts {
    pub fn from_daemon_config(daemon: &crate::config::DaemonConfig) -> Self {
        Self {
            first_chunk: Duration::from_millis(daemon.first_chunk_timeout_ms),
            chunk: Duration::from_millis(daemon.chunk_timeout_ms),
            body: Duration::from_millis(daemon.body_timeout_ms),
        }
    }
}

fn should_force_stream(parts: &axum::http::request::Parts) -> bool {
    parts.headers.get(STREAM_MARKER_HEADER).and_then(|v| v.to_str().ok()) == Some("1")
}

pub async fn outbound_request_middleware(State(cfg): State<OutboundConfig>, request: Request, next: Next) -> Response {
    let (mut parts, body) = request.into_parts();

    let force_stream = should_force_stream(&parts);

    let path = parts.uri.path();
    let stream_usage = path.ends_with("/completions");
    if parts.method != axum::http::Method::POST {
        let response = next.run(Request::from_parts(parts, body)).await;
        return if force_stream {
            reassemble_stream(response, cfg.timeouts).await
        } else {
            response
        };
    }

    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => {
            let response = (StatusCode::BAD_REQUEST, "failed to read request body").into_response();
            return super::rejections::refuse(&parts, response, "body_read_failed", None, None);
        }
    };

    let (edited, model) = transform(&bytes, force_stream, stream_usage);
    if parts.extensions.get::<onwards::serving::BillingPrices>().is_none()
        && let Some(prices) = model.and_then(|model| unit_prices(&cfg, &parts, &model))
    {
        parts.extensions.insert(prices.to_billing());
    }
    let prices = parts.extensions.get::<onwards::serving::BillingPrices>().copied();

    let response = match edited {
        Some(edited) => {
            parts.headers.remove(axum::http::header::CONTENT_LENGTH);
            next.run(Request::from_parts(parts, Body::from(edited))).await
        }
        None => next.run(Request::from_parts(parts, Body::from(bytes))).await,
    };

    let mut response = if force_stream {
        reassemble_stream(response, cfg.timeouts).await
    } else {
        response
    };
    if let Some(prices) = prices {
        response.extensions_mut().insert(prices);
    }
    response
}

fn unit_prices(cfg: &OutboundConfig, parts: &axum::http::request::Parts, requested: &str) -> Option<crate::pricing::UnitPrices> {
    let value = |name: &str| parts.headers.get(name).and_then(|v| v.to_str().ok()).filter(|v| !v.is_empty());
    let bearer = value(header::AUTHORIZATION.as_str()).and_then(|v| v.strip_prefix("Bearer "));
    let key = crate::auth::internal_credential::presented_key_hash(&parts.extensions, bearer)?;
    let completion_window = value("x-fusillade-batch-completion-window");
    let batch_created_at = value("x-fusillade-batch-created-at").and_then(|v| v.parse::<chrono::DateTime<chrono::Utc>>().ok());
    let cache_reads = parts.extensions.get::<crate::prompt_cache::CacheReadSource>().copied();
    let name = ServingName::of(&parts.extensions, requested);
    let query = crate::pricing::PriceQuery {
        model: &name.model,
        completion_window,
        batch_created_at,
        now: chrono::Utc::now(),
        serving_class: name.class_label(),
        cache_reads,
    };
    cfg.key_policy.unit_prices(&key, &query)
}

fn transform(bytes: &Bytes, force_stream: bool, stream_usage: bool) -> (Option<Vec<u8>>, Option<String>) {
    let Some(mut value) = serde_json::from_slice::<Value>(bytes).ok() else {
        return (None, None);
    };
    let Some(obj) = value.as_object_mut() else {
        return (None, None);
    };
    let model = obj.get("model").and_then(Value::as_str).map(str::to_owned);
    let mut changed = false;

    if let Some(nvext) = obj.get_mut("nvext").and_then(Value::as_object_mut) {
        changed |= onwards::serving::scrub_router_targets(nvext);
        changed |= onwards::serving::scrub_billing(nvext);
    }

    let request_streaming = obj.get("stream").and_then(Value::as_bool) == Some(true) || force_stream;
    if stream_usage && request_streaming {
        if force_stream && obj.get("stream").and_then(Value::as_bool) != Some(true) {
            obj.insert("stream".to_string(), Value::Bool(true));
            changed = true;
        }
        let stream_options = obj.entry("stream_options").or_insert_with(|| Value::Object(Default::default()));
        if !stream_options.is_object() {
            *stream_options = Value::Object(Default::default());
            changed = true;
        }
        if let Some(so) = stream_options.as_object_mut()
            && so.get("include_usage").and_then(Value::as_bool) != Some(true)
        {
            so.insert("include_usage".to_string(), Value::Bool(true));
            changed = true;
        }
    }

    let edited = if changed { serde_json::to_vec(&value).ok() } else { None };
    (edited, model)
}

async fn reassemble_stream(response: Response, timeouts: StreamTimeouts) -> Response {
    use eventsource_stream::Eventsource;

    let (parts, body) = response.into_parts();
    let status = parts.status;

    let is_event_stream = parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("text/event-stream"));

    if !is_event_stream {
        return Response::from_parts(parts, body);
    }

    let reassemble = status.as_u16() < 400;
    let mut stream = body.into_data_stream().eventsource();

    let first_event = match tokio::time::timeout(timeouts.first_chunk, stream.next()).await {
        Err(_) => return timeout_response("first_chunk"),
        Ok(Some(Ok(event))) => Some(event),
        Ok(Some(Err(e))) => return sse_parse_error(&e.to_string()),
        Ok(None) => None,
    };

    let outcome = tokio::time::timeout(timeouts.body, async {
        let mut sink = Sink::new(reassemble);
        if let Some(event) = first_event {
            sink.absorb(&event);
        }
        loop {
            match tokio::time::timeout(timeouts.chunk, stream.next()).await {
                Ok(Some(Ok(event))) => sink.absorb(&event),
                Ok(Some(Err(e))) => return Collected::ParseError(e.to_string()),
                Ok(None) => break,
                Err(_) => return Collected::Stalled(sink.seen),
            }
        }
        Collected::Done(Box::new(sink))
    })
    .await;

    let sink = match outcome {
        Err(_) => return timeout_response("body"),
        Ok(Collected::Stalled(seen)) => {
            debug!(events_seen = seen, "fusillade stream stalled between events");
            return timeout_response("chunk");
        }
        Ok(Collected::ParseError(e)) => return sse_parse_error(&e),
        Ok(Collected::Done(sink)) => *sink,
    };

    if let Some(data) = &sink.embedded_error
        && let Ok(envelope) = serde_json::from_str::<EmbeddedErrorEnvelope>(data)
    {
        let code = envelope
            .error
            .code
            .as_ref()
            .and_then(|c| c.as_u64())
            .map(|c| c as u16)
            .filter(|c| (400..600).contains(c))
            .unwrap_or(500);

        warn!(
            embedded_status = code,
            "provider returned an error inside the SSE stream, reclassifying"
        );
        let status = StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        return json_body_response(parts, status, data.clone());
    }

    let status = if status.is_success() && sink.is_reasoning_without_answer() {
        warn!("upstream completed reasoning without an answer, reclassifying");
        StatusCode::BAD_GATEWAY
    } else {
        status
    };

    match sink.finish() {
        Ok(body) => json_body_response(parts, status, body),
        Err(e) => {
            warn!(error = %e, "failed to reassemble SSE stream into a response body");
            sse_parse_error(&e.to_string())
        }
    }
}

enum Collected {
    Done(Box<Sink>),
    Stalled(usize),
    ParseError(String),
}

struct Sink {
    reassembler: openai_reassembler::Reassembler,
    raw: String,
    embedded_error: Option<String>,
    seen: usize,
    reassemble: bool,
}

impl Sink {
    fn new(reassemble: bool) -> Self {
        Self {
            reassembler: openai_reassembler::Reassembler::new(),
            raw: String::new(),
            embedded_error: None,
            seen: 0,
            reassemble,
        }
    }

    fn absorb(&mut self, event: &eventsource_stream::Event) {
        self.seen += 1;

        if self.embedded_error.is_none() && event.data.starts_with("{\"error\"") {
            self.embedded_error = Some(event.data.clone());
        }

        if self.reassemble {
            self.reassembler.push(event);
        } else if !event.data.is_empty() && event.data != "[DONE]" {
            if !self.raw.is_empty() {
                self.raw.push('\n');
            }
            self.raw.push_str(&event.data);
        }
    }

    fn finish(self) -> anyhow::Result<String> {
        if self.reassemble { self.reassembler.finish() } else { Ok(self.raw) }
    }

    fn is_reasoning_without_answer(&self) -> bool {
        self.reassemble && self.reassembler.is_reasoning_without_answer()
    }
}

#[derive(serde::Deserialize)]
struct EmbeddedErrorEnvelope {
    error: EmbeddedError,
}

#[derive(serde::Deserialize)]
struct EmbeddedError {
    #[serde(default)]
    code: Option<Value>,
}

fn timeout_response(which: &str) -> Response {
    (StatusCode::GATEWAY_TIMEOUT, format!("upstream stream exceeded its {which} budget")).into_response()
}

fn sse_parse_error(detail: &str) -> Response {
    warn!(error = %detail, "SSE parse error while reassembling a fusillade stream");
    (StatusCode::BAD_GATEWAY, "failed to read upstream stream").into_response()
}

fn json_body_response(mut parts: axum::http::response::Parts, status: StatusCode, body: String) -> Response {
    parts.status = status;
    parts.headers.remove(header::CONTENT_LENGTH);
    parts.headers.remove(header::CONTENT_ENCODING);
    parts
        .headers
        .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));
    Response::from_parts(parts, Body::from(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::time::Duration;

    fn run(body: &serde_json::Value, fusillade: bool) -> Option<serde_json::Value> {
        let bytes = Bytes::from(serde_json::to_vec(body).unwrap());
        transform(&bytes, fusillade, true).0.map(|b| serde_json::from_slice(&b).unwrap())
    }

    #[test]
    fn injects_stream_options_when_streaming() {
        let body = serde_json::json!({"model": "gpt-4", "messages": [], "stream": true});
        let out = run(&body, false).expect("should transform");
        assert_eq!(out["stream_options"]["include_usage"], true);
    }

    #[test]
    fn injecting_usage_keeps_key_order() {
        let body = Bytes::from_static(
            br#"{"model":"m","stream":true,"response_format":{"type":"json_schema","json_schema":{"name":"a","schema":{"type":"object","properties":{"z":{"type":"string"},"a":{"type":"string"}}}}},"messages":[]}"#,
        );
        let out = transform(&body, false, true).0.expect("should transform");
        assert_eq!(
            String::from_utf8(out).unwrap(),
            r#"{"model":"m","stream":true,"response_format":{"type":"json_schema","json_schema":{"name":"a","schema":{"type":"object","properties":{"z":{"type":"string"},"a":{"type":"string"}}}}},"messages":[],"stream_options":{"include_usage":true}}"#
        );
    }

    #[test]
    fn skips_non_streaming() {
        let body = serde_json::json!({"model": "gpt-4", "messages": [], "stream": false});
        assert!(run(&body, false).is_none());
    }

    #[test]
    fn fusillade_header_forces_stream_and_usage() {
        let body = serde_json::json!({"model": "gpt-4", "messages": []});
        let out = run(&body, true).expect("should transform");
        assert_eq!(out["stream"], true);
        assert_eq!(out["stream_options"]["include_usage"], true);
    }

    #[test]
    fn non_object_stream_options_replaced() {
        for stream_options in [
            serde_json::Value::Null,
            serde_json::json!("x"),
            serde_json::json!([]),
            serde_json::json!(false),
        ] {
            let body = serde_json::json!({"model": "gpt-4", "messages": [], "stream": true, "stream_options": stream_options});
            let out = run(&body, false).expect("should transform");
            assert_eq!(out["stream_options"], serde_json::json!({"include_usage": true}));
        }
    }
    fn timeouts(first_chunk_ms: u64, chunk_ms: u64, body_ms: u64) -> StreamTimeouts {
        StreamTimeouts {
            first_chunk: Duration::from_millis(first_chunk_ms),
            chunk: Duration::from_millis(chunk_ms),
            body: Duration::from_millis(body_ms),
        }
    }

    fn sse_response(status: StatusCode, frames: &[&str]) -> Response {
        let body: String = frames.iter().map(|f| format!("data: {f}\n\n")).collect();
        let mut resp = Response::new(Body::from(body));
        *resp.status_mut() = status;
        resp.headers_mut()
            .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("text/event-stream"));
        resp
    }

    fn slow_sse_response(frames: Vec<(u64, String)>) -> Response {
        let stream = async_stream::stream! {
            for (delay_ms, frame) in frames {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                yield Ok::<Bytes, Infallible>(Bytes::from(format!("data: {frame}\n\n")));
            }
        };
        let mut resp = Response::new(Body::from_stream(stream));
        resp.headers_mut()
            .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("text/event-stream"));
        resp
    }

    async fn body_string(response: Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    fn chunk(content: &str) -> String {
        format!(
            r#"{{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{{"index":0,"delta":{{"content":"{content}"}},"finish_reason":null}}]}}"#
        )
    }

    #[tokio::test]
    async fn reassembles_sse_into_a_single_json_body() {
        let frames = [chunk("Hello"), chunk(" world"), "[DONE]".to_string()];
        let refs: Vec<&str> = frames.iter().map(String::as_str).collect();

        let out = reassemble_stream(sse_response(StatusCode::OK, &refs), timeouts(1000, 1000, 5000)).await;

        assert_eq!(out.status(), StatusCode::OK);
        assert_eq!(
            out.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json",
            "the caller asked for a completion, not a stream"
        );
        let body: serde_json::Value = serde_json::from_str(&body_string(out).await).unwrap();
        assert_eq!(body["choices"][0]["message"]["content"], "Hello world");
    }

    #[tokio::test]
    async fn reasoning_without_answer_is_reclassified_after_reassembly() {
        let frames = [
            r#"{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"reasoning_content":"thinking"}}]}"#,
            r#"{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
            "[DONE]",
        ];

        for upstream_status in [StatusCode::OK, StatusCode::CREATED] {
            let out = reassemble_stream(sse_response(upstream_status, &frames), timeouts(1000, 1000, 5000)).await;

            assert_eq!(out.status(), StatusCode::BAD_GATEWAY);
            let body: serde_json::Value = serde_json::from_str(&body_string(out).await).unwrap();
            assert_eq!(body["choices"][0]["message"]["reasoning_content"], "thinking");
        }
    }

    #[tokio::test]
    async fn keepalive_frames_do_not_change_the_assembled_body() {
        let keepalive = r#"{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":null}]}"#;

        let mut noisy = vec![chunk("Hello")];
        noisy.extend(std::iter::repeat_n(keepalive.to_string(), 500));
        noisy.push(chunk(" world"));
        let noisy_refs: Vec<&str> = noisy.iter().map(String::as_str).collect();
        let clean = [chunk("Hello"), chunk(" world")];
        let clean_refs: Vec<&str> = clean.iter().map(String::as_str).collect();

        let a = reassemble_stream(sse_response(StatusCode::OK, &noisy_refs), timeouts(1000, 1000, 5000)).await;
        let b = reassemble_stream(sse_response(StatusCode::OK, &clean_refs), timeouts(1000, 1000, 5000)).await;

        assert_eq!(body_string(a).await, body_string(b).await, "keepalives changed the assembled body");
    }

    #[tokio::test]
    async fn embedded_error_is_reclassified_to_a_real_status() {
        let frames = [r#"{"error":{"code":429,"message":"slow down"}}"#];

        let out = reassemble_stream(sse_response(StatusCode::OK, &frames), timeouts(1000, 1000, 5000)).await;

        assert_eq!(out.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(body_string(out).await.contains("slow down"));
    }

    #[tokio::test]
    async fn the_first_embedded_error_wins() {
        let frames = [
            r#"{"error":{"code":429,"message":"first"}}"#,
            r#"{"error":{"code":500,"message":"second"}}"#,
        ];

        let out = reassemble_stream(sse_response(StatusCode::OK, &frames), timeouts(1000, 1000, 5000)).await;

        assert_eq!(out.status(), StatusCode::TOO_MANY_REQUESTS);
        let body = body_string(out).await;
        assert!(body.contains("first"), "got: {body}");
        assert!(!body.contains("second"));
    }

    #[tokio::test]
    async fn error_status_keeps_raw_event_data() {
        let frames = [r#"{"detail":"bad request"}"#, "", "[DONE]"];

        let out = reassemble_stream(sse_response(StatusCode::BAD_REQUEST, &frames), timeouts(1000, 1000, 5000)).await;

        assert_eq!(out.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            body_string(out).await,
            r#"{"detail":"bad request"}"#,
            "empty and [DONE] frames are dropped, the rest is joined verbatim"
        );
    }

    #[tokio::test]
    async fn non_sse_response_passes_through_untouched() {
        let mut resp = Response::new(Body::from(r#"{"error":"no such model"}"#));
        *resp.status_mut() = StatusCode::NOT_FOUND;
        resp.headers_mut()
            .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));

        let out = reassemble_stream(resp, timeouts(1000, 1000, 5000)).await;

        assert_eq!(out.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_string(out).await, r#"{"error":"no such model"}"#);
    }

    #[tokio::test]
    async fn no_first_event_reports_a_first_chunk_timeout() {
        let response = slow_sse_response(vec![(400, chunk("late"))]);

        let out = reassemble_stream(response, timeouts(50, 5000, 5000)).await;

        assert_eq!(out.status(), StatusCode::GATEWAY_TIMEOUT);
        assert!(body_string(out).await.contains("first_chunk"));
    }

    #[tokio::test]
    async fn a_stalled_stream_reports_a_chunk_timeout() {
        let response = slow_sse_response(vec![(0, chunk("hi")), (400, chunk(" there"))]);

        let out = reassemble_stream(response, timeouts(1000, 50, 5000)).await;

        assert_eq!(out.status(), StatusCode::GATEWAY_TIMEOUT);
        assert!(
            body_string(out).await.contains("chunk"),
            "a stream that opens and then goes idle is a chunk timeout, not a body one"
        );
    }

    #[tokio::test]
    async fn a_long_stream_reports_a_body_timeout() {
        let frames: Vec<(u64, String)> = (0..20).map(|_| (30, chunk("x"))).collect();

        let out = reassemble_stream(slow_sse_response(frames), timeouts(1000, 1000, 120)).await;

        assert_eq!(out.status(), StatusCode::GATEWAY_TIMEOUT);
        assert!(
            body_string(out).await.contains("body"),
            "no single gap was long enough, so this must be the total budget"
        );
    }

    async fn content_type_through_the_edge(path: &str, headers: &[(&str, &str)]) -> String {
        use axum::{Router, routing::post};

        let upstream = || async {
            let frames = "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n";
            ([(header::CONTENT_TYPE, "text/event-stream")], frames)
        };

        let inner = Router::new()
            .route("/chat/completions", post(upstream))
            .route("/responses", post(upstream))
            .layer(axum::middleware::from_fn_with_state(config(), outbound_request_middleware));
        let app = Router::new().nest("/ai/v1", inner);

        let server = axum_test::TestServer::new(app).unwrap();
        let mut req = server.post(path).json(&serde_json::json!({"model": "m", "messages": []}));
        for (k, v) in headers {
            req = req.add_header(*k, *v);
        }
        let response = req.await;
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .map_or_else(String::new, |v| v.to_str().unwrap_or_default().to_string())
    }

    fn config() -> OutboundConfig {
        OutboundConfig {
            timeouts: timeouts(1000, 1000, 5000),
            key_policy: crate::sync::key_policy::KeyPolicyCache::empty(),
        }
    }

    async fn priced_through_the_edge(key: &str, headers: &[(&str, &str)], body: serde_json::Value) -> serde_json::Value {
        use crate::db::models::api_keys::ApiKeyPurpose;
        use crate::pricing::{PriceBook, TariffInfo};
        use crate::sync::key_policy::{KeyBilling, KeyPolicy, KeyPolicyCache};
        use axum::{Router, routing::post};
        use rust_decimal::Decimal;

        let tariff = |purpose, window: Option<&str>, input: i64, output: i64| TariffInfo {
            id: uuid::Uuid::new_v4(),
            purpose,
            effective_from: chrono::Utc::now() - chrono::Duration::hours(1),
            valid_until: None,
            input_price_per_token: Decimal::new(input, 6),
            output_price_per_token: Decimal::new(output, 6),
            completion_window: window.map(str::to_string),
            account: None,
            serving_class: None,
        };
        let account = uuid::Uuid::new_v4();
        let policy = |purpose| KeyPolicy {
            billing: Some(KeyBilling { account, purpose }),
            ..Default::default()
        };
        let key_policy = KeyPolicyCache::from_policies([
            ("sk-realtime".to_string(), policy(ApiKeyPurpose::Realtime)),
            ("sk-batch".to_string(), policy(ApiKeyPurpose::Batch)),
        ]);
        key_policy.replace_prices(PriceBook::from_rows(
            [account],
            [(
                "m".to_string(),
                vec![
                    tariff(ApiKeyPurpose::Realtime, None, 3, 9),
                    tariff(ApiKeyPurpose::Batch, Some("24h"), 1, 3),
                ],
            )],
            [],
        ));

        let next = |request: Request| async move {
            let prices = request.extensions().get::<onwards::serving::BillingPrices>().copied();
            let body = axum::body::to_bytes(request.into_body(), usize::MAX).await.unwrap();
            axum::Json(serde_json::json!({
                "prices": prices,
                "body": serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            }))
        };
        let config = OutboundConfig { key_policy, ..config() };
        let app = Router::new()
            .route("/chat/completions", post(next))
            .layer(axum::middleware::from_fn_with_state(config, outbound_request_middleware));
        let server = axum_test::TestServer::new(app).unwrap();
        let mut request = server
            .post("/chat/completions")
            .add_header("authorization", format!("Bearer {key}"))
            .json(&body);
        for (name, value) in headers {
            request = request.add_header(*name, *value);
        }
        request.await.json()
    }

    #[tokio::test]
    async fn requests_carry_the_prices_their_key_is_charged_at() {
        let body = serde_json::json!({"model": "m", "messages": []});
        let realtime = priced_through_the_edge("sk-realtime", &[], body.clone()).await;
        assert_eq!(
            realtime["prices"],
            serde_json::json!({"prompt": 3e-6, "cached": 3e-6, "output": 9e-6})
        );

        let batch = priced_through_the_edge(
            "sk-batch",
            &[
                ("x-fusillade-batch-completion-window", "24h"),
                ("x-fusillade-batch-created-at", &chrono::Utc::now().to_rfc3339()),
            ],
            body.clone(),
        )
        .await;
        assert_eq!(batch["prices"], serde_json::json!({"prompt": 1e-6, "cached": 1e-6, "output": 3e-6}));

        let unknown = priced_through_the_edge("sk-unknown", &[], body).await;
        assert!(unknown["prices"].is_null(), "a key outside the policy map is not priced");
    }

    #[tokio::test]
    async fn a_caller_cannot_state_its_own_prices() {
        let forged = serde_json::json!({
            "model": "m",
            "messages": [],
            "nvext": {"billing": {"prompt": 0, "cached": 0, "output": 0}, "cache_control": {"enabled": true}}
        });
        for key in ["sk-realtime", "sk-unknown"] {
            let out = priced_through_the_edge(key, &[], forged.clone()).await;
            assert_eq!(
                out["body"]["nvext"],
                serde_json::json!({"cache_control": {"enabled": true}}),
                "{key}"
            );
        }
    }

    #[test]
    fn the_budgets_are_read_from_the_daemons_own_configuration() {
        let mut daemon = crate::config::DaemonConfig::default();
        daemon.first_chunk_timeout_ms = 1;
        daemon.chunk_timeout_ms = 2;
        daemon.body_timeout_ms = 3;

        assert_eq!(
            StreamTimeouts::from_daemon_config(&daemon),
            StreamTimeouts {
                first_chunk: Duration::from_millis(1),
                chunk: Duration::from_millis(2),
                body: Duration::from_millis(3),
            }
        );
    }

    #[tokio::test]
    async fn daemon_traffic_is_reassembled() {
        let ct = content_type_through_the_edge(
            "/ai/v1/chat/completions",
            &[
                (STREAM_MARKER_HEADER, "1"),
                ("x-fusillade-request-id", "00000000-0000-0000-0000-000000000000"),
            ],
        )
        .await;
        assert!(ct.starts_with("application/json"), "expected an assembled body, got {ct:?}");
    }

    #[tokio::test]
    async fn client_traffic_still_streams() {
        let ct = content_type_through_the_edge(
            "/ai/v1/chat/completions",
            &[("x-fusillade-request-id", "00000000-0000-0000-0000-000000000000")],
        )
        .await;
        assert!(
            ct.starts_with("text/event-stream"),
            "a streaming client must not have its stream reassembled, got {ct:?}"
        );
    }

    #[tokio::test]
    async fn unmarked_traffic_still_streams() {
        let ct = content_type_through_the_edge("/ai/v1/chat/completions", &[]).await;
        assert!(ct.starts_with("text/event-stream"), "got {ct:?}");
    }

    #[tokio::test]
    async fn marked_responses_traffic_is_reassembled() {
        let ct = content_type_through_the_edge("/ai/v1/responses", &[(STREAM_MARKER_HEADER, "1")]).await;
        assert!(ct.starts_with("application/json"), "expected an assembled body, got {ct:?}");
    }
}
