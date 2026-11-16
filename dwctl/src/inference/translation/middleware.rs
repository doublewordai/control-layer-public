
use axum::{
    body::Body,
    extract::State,
    http::{Method, Request, StatusCode, header},
    middleware::Next,
    response::Response,
};
use bytes::Bytes;
use futures::StreamExt;
use tracing::{debug, warn};

use super::{ProtocolTranslator, TranslationError, TranslationRegistry};
use crate::inference::rejections;

pub async fn translation_middleware(State(registry): State<TranslationRegistry>, request: Request<Body>, next: Next) -> Response {
    let path = request.uri().path().to_string();

    let Some(translator) = registry.detect(&path, request.headers()) else {
        return next.run(request).await;
    };

    if translator.translates_request_body() && request.method() != Method::POST {
        return next.run(request).await;
    }
    debug!(translator = translator.name(), path = %path, "edge translation: request matched");

    let (mut parts, body) = request.into_parts();

    let body_bytes = match axum::body::to_bytes(body, registry.max_body_size()).await {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "edge translation: request body too large or unreadable");
            let response = error_response(translator.as_ref(), StatusCode::PAYLOAD_TOO_LARGE, "request body too large");
            return rejections::refuse(&parts, response, "request_too_large", None, None);
        }
    };

    let original_request = body_bytes.clone();

    let response_id = parts
        .headers
        .get("x-fusillade-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let translated = match translator.translate_request(&parts, body_bytes) {
        Ok(t) => t,
        Err(TranslationError::InvalidJson(msg)) => {
            let response = error_response(translator.as_ref(), StatusCode::BAD_REQUEST, &msg);
            return rejections::refuse(&parts, response, "invalid_json", None, None);
        }
        Err(TranslationError::SchemaMismatch { mismatch, message }) => {
            let param = mismatch.as_ref().and_then(|mismatch| mismatch.path.as_deref());
            let response = error_response_for_param(translator.as_ref(), StatusCode::BAD_REQUEST, &message, param);
            let model = rejections::model_of(&original_request);
            return rejections::refuse_mismatch(&parts, response, mismatch.as_ref(), model.as_deref());
        }
        Err(TranslationError::InvalidParameter { param, message }) => {
            let response = error_response_for_param(translator.as_ref(), StatusCode::BAD_REQUEST, &message, Some(param));
            let model = rejections::model_of(&original_request);
            return rejections::refuse(&parts, response, "invalid_parameter", Some(param), model.as_deref());
        }
        Err(TranslationError::UnsupportedParameter { param, message }) => {
            let (status, bytes) = translator.error_for_code(StatusCode::BAD_REQUEST, &message, Some(param), "unsupported_parameter");
            let model = rejections::model_of(&original_request);
            return rejections::refuse(
                &parts,
                json_response(status, bytes),
                "unsupported_parameter",
                Some(param),
                model.as_deref(),
            );
        }
        Err(TranslationError::Reasoning(error)) => {
            let status = StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::BAD_REQUEST);
            let (status, bytes) = translator.error_for_code(status, error.message(), error.param(), error.code());
            let model = rejections::model_of(&original_request);
            return rejections::refuse(&parts, json_response(status, bytes), error.code(), error.param(), model.as_deref());
        }
        Err(TranslationError::Internal(msg)) => {
            warn!(error = %msg, translator = translator.name(), "edge translation: request translate failed");
            return error_response(translator.as_ref(), StatusCode::INTERNAL_SERVER_ERROR, &msg);
        }
    };

    let rejection = rejections::Scope::of(&parts);
    parts.uri = translated.uri;
    parts.headers = translated.headers;
    let downstream_req = Request::from_parts(parts, Body::from(translated.body));

    let response = next.run(downstream_req).await;
    let extensions = response.extensions().clone();
    let mut translated = translate_response_back(translator.as_ref(), &original_request, response_id.as_deref(), rejection, response).await;
    translated.extensions_mut().extend(extensions);
    translated
}

async fn translate_response_back(
    translator: &dyn ProtocolTranslator,
    request: &Bytes,
    response_id: Option<&str>,
    rejection: rejections::Scope,
    response: Response,
) -> Response {
    let (parts, body) = response.into_parts();
    let status = parts.status;

    let is_sse = parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.starts_with("text/event-stream"))
        .unwrap_or(false);

    if is_sse {
        return reframe_sse(translator, request, response_id, status, body);
    }

    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "edge translation: failed to read upstream response body");
            return error_response(translator, StatusCode::BAD_GATEWAY, "failed to read upstream response");
        }
    };

    if status.is_success() {
        match translator.translate_response(request, response_id, body_bytes) {
            Ok(new_body) => json_response(status, new_body),
            Err(e) => {
                warn!(error = %e, translator = translator.name(), "edge translation: response translate failed");
                let response = error_response(translator, StatusCode::UNPROCESSABLE_ENTITY, "response translation failed");
                let model = rejections::model_of(request);
                rejection.refuse(response, "response_translation_failed", None, model.as_deref())
            }
        }
    } else {
        let (new_status, new_body) = translator.translate_error(status, body_bytes);
        json_response(new_status, new_body)
    }
}

fn json_response(status: StatusCode, body: Bytes) -> Response {
    let mut resp = Response::new(Body::from(body));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));
    resp
}

fn error_response(translator: &dyn ProtocolTranslator, status: StatusCode, message: &str) -> Response {
    let (status, bytes) = translator.error_from_message(status, message);
    json_response(status, bytes)
}

fn error_response_for_param(translator: &dyn ProtocolTranslator, status: StatusCode, message: &str, param: Option<&str>) -> Response {
    let (status, bytes) = translator.error_for_param(status, message, param);
    json_response(status, bytes)
}

fn reframe_sse(
    translator: &dyn ProtocolTranslator,
    request: &Bytes,
    response_id: Option<&str>,
    status: StatusCode,
    body: Body,
) -> Response {
    let mut reframer = translator.stream_reframer(request, response_id);
    let mut data = body.into_data_stream();

    let out = async_stream::stream! {
        let mut buf: Vec<u8> = Vec::new();
        while let Some(item) = data.next().await {
            let chunk = match item {
                Ok(c) => c,
                Err(e) => {
                    warn!(error = %e, "edge translation: upstream SSE transport error");
                    let ev = reframer.error("upstream stream error");
                    if !ev.is_empty() {
                        yield Ok::<Bytes, std::io::Error>(Bytes::from(ev));
                    }
                    return;
                }
            };
            buf.extend_from_slice(&chunk);
            while let Some(pos) = find_subsequence(&buf, b"\n\n") {
                let block: Vec<u8> = buf.drain(..pos + 2).collect();
                for line in String::from_utf8_lossy(&block).lines() {
                    let Some(data_part) = line.strip_prefix("data:") else { continue };
                    let data_part = data_part.trim();
                    if data_part.is_empty() || data_part == "[DONE]" {
                        continue;
                    }
                    match serde_json::from_str::<serde_json::Value>(data_part) {
                        Ok(val) => {
                            let emitted = reframer.push(&val);
                            if !emitted.is_empty() {
                                yield Ok::<Bytes, std::io::Error>(Bytes::from(emitted));
                            }
                        }
                        Err(e) => {
                            debug!(error = %e, "edge translation: dropping unparseable SSE data line");
                        }
                    }
                }
            }
        }
        let closing = reframer.finish();
        if !closing.is_empty() {
            yield Ok(Bytes::from(closing));
        }
    };

    let mut resp = Response::new(Body::from_stream(out));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("text/event-stream"));
    resp
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inference::translation::TranslationRegistry;
    use crate::inference::translation::anthropic::AnthropicMessages;
    use axum::{Router, extract::Request, routing::post};
    use std::sync::Arc;

    async fn fake_onwards_chat_completions(req: Request) -> Response {
        let body = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap();
        let received: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert!(
            received.get("system").is_none(),
            "system should be folded into messages, got: {received}"
        );
        assert_eq!(received["max_tokens"], 50);
        assert_eq!(received["messages"][0]["role"], "system");
        assert_eq!(received["messages"][1]["role"], "user");

        let resp = serde_json::json!({
            "id": "chatcmpl-1",
            "model": received["model"],
            "choices": [ { "message": { "role": "assistant", "content": "hi back" }, "finish_reason": "stop" } ],
            "usage": { "prompt_tokens": 4, "completion_tokens": 2 }
        });
        json_response(StatusCode::OK, Bytes::from(serde_json::to_vec(&resp).unwrap()))
    }

    fn test_app(inner: Router) -> axum_test::TestServer {
        let registry = TranslationRegistry::new(vec![Arc::new(AnthropicMessages::new(true))]);
        let inner = inner.layer(axum::middleware::from_fn_with_state(registry, translation_middleware));
        let app = Router::new().nest("/ai/v1", inner);
        axum_test::TestServer::new(app).expect("test server")
    }

    #[tokio::test]
    async fn messages_round_trips_via_alias_route() {
        let inner = Router::new().route("/messages", post(fake_onwards_chat_completions));
        let server = test_app(inner);

        let response = server
            .post("/ai/v1/messages")
            .add_header("x-api-key", "sk-test")
            .json(&serde_json::json!({
                "model": "claude-x",
                "max_tokens": 50,
                "system": "be brief",
                "messages": [ { "role": "user", "content": "hello" } ]
            }))
            .await;

        assert_eq!(response.status_code().as_u16(), 200);
        let body: serde_json::Value = response.json();
        assert_eq!(body["type"], "message");
        assert_eq!(body["role"], "assistant");
        assert_eq!(body["content"][0]["type"], "text");
        assert_eq!(body["content"][0]["text"], "hi back");
        assert_eq!(body["stop_reason"], "end_turn");
        assert_eq!(body["usage"]["input_tokens"], 4);
        assert_eq!(body["usage"]["output_tokens"], 2);
    }

    #[tokio::test]
    async fn server_side_tool_returns_anthropic_400_before_downstream() {
        async fn must_not_run(_req: Request) -> Response {
            panic!("unsupported server tool reached downstream")
        }

        let server = test_app(Router::new().route("/messages", post(must_not_run)));
        let response = server
            .post("/ai/v1/messages")
            .add_header("x-api-key", "sk-test")
            .json(&serde_json::json!({
                "model": "claude-x", "max_tokens": 50,
                "messages": [{"role": "user", "content": "hello"}],
                "tools": [{"type": "web_search_20250305", "name": "web_search"}]
            }))
            .await;

        assert_eq!(response.status_code().as_u16(), 400);
        let body: serde_json::Value = response.json();
        assert_eq!(body["type"], "error");
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert!(body["error"]["message"].as_str().unwrap().contains("web_search_20250305"));
    }

    #[tokio::test]
    async fn oversized_request_body_is_rejected_as_anthropic_error() {
        let inner = Router::new().route("/messages", post(fake_onwards_chat_completions));
        let registry = TranslationRegistry::new(vec![Arc::new(AnthropicMessages::new(true))]).with_max_body_size(16);
        let inner = inner.layer(axum::middleware::from_fn_with_state(registry, translation_middleware));
        let server = axum_test::TestServer::new(Router::new().nest("/ai/v1", inner)).expect("test server");

        let response = server
            .post("/ai/v1/messages")
            .add_header("x-api-key", "sk-test")
            .json(&serde_json::json!({
                "model": "claude-x", "max_tokens": 50,
                "messages": [ { "role": "user", "content": "this body is well over sixteen bytes" } ]
            }))
            .await;

        assert_eq!(response.status_code().as_u16(), 413);
        let body: serde_json::Value = response.json();
        assert_eq!(body["type"], "error");
        assert_eq!(body["error"]["type"], "request_too_large");
    }

    #[tokio::test]
    async fn untranslatable_success_body_becomes_anthropic_error() {
        async fn bad_handler(_req: Request) -> Response {
            json_response(StatusCode::OK, Bytes::from_static(b"not a chat completion"))
        }
        let server = test_app(Router::new().route("/messages", post(bad_handler)));

        let response = server
            .post("/ai/v1/messages")
            .add_header("x-api-key", "sk-test")
            .json(&serde_json::json!({
                "model": "claude-x", "max_tokens": 50,
                "messages": [ { "role": "user", "content": "hi" } ]
            }))
            .await;

        assert_eq!(response.status_code().as_u16(), 422);
        let body: serde_json::Value = response.json();
        assert_eq!(body["type"], "error");
    }

    #[test]
    fn counts_its_own_refusals_only() {
        use crate::inference::rejections::test_support;
        use tower::ServiceExt;

        async fn handler(req: Request) -> Response {
            let body = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap();
            if body.windows(6).any(|w| w == b"broken") {
                json_response(StatusCode::OK, Bytes::from_static(b"not a chat completion"))
            } else {
                json_response(StatusCode::BAD_REQUEST, Bytes::from_static(br#"{"error":{"message":"no"}}"#))
            }
        }
        let app = || {
            let registry = TranslationRegistry::new(vec![Arc::new(AnthropicMessages::new(true))]);
            Router::new()
                .route("/messages", post(handler))
                .layer(axum::middleware::from_fn_with_state(registry, translation_middleware))
                .layer(test_support::layer("claude-x"))
        };
        let message = |role: &str, content: &str| {
            serde_json::json!({
                "model": "claude-x", "max_tokens": 50,
                "messages": [ { "role": role, "content": content } ]
            })
            .to_string()
        };
        let (lines, statuses) = test_support::rejections(async move {
            let mut statuses = Vec::new();
            for body in [
                "{not json".to_string(),
                serde_json::json!({"model": "claude-x", "max_tokens": "lots", "messages": []}).to_string(),
                message("developer", "hi"),
                message("user", "broken"),
                message("user", "hi"),
                serde_json::json!({
                    "model": "claude-x", "max_tokens": 50, "thinking": {"type": "sometimes"},
                    "messages": [ { "role": "user", "content": "hi" } ]
                })
                .to_string(),
            ] {
                let request = axum::http::Request::post("/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap();
                statuses.push(app().oneshot(request).await.unwrap().status().as_u16());
            }
            statuses
        });

        assert_eq!(statuses, [400, 400, 400, 422, 400, 400]);
        assert_eq!(lines.len(), 4, "the downstream 400 is not counted: {lines:?}");
        let line = |code: &str| lines.iter().find(|l| l.contains(&format!("code=\"{code}\""))).unwrap();
        assert!(
            line("invalid_parameter").ends_with(" 2"),
            "the role and thinking refusals: {lines:?}"
        );
        assert!(line("invalid_json").contains("model=\"\""));
        assert!(line("schema_mismatch").contains("model=\"claude-x\""));
        assert!(line("response_translation_failed").contains("status=\"422\""));
        assert!(line("invalid_parameter").contains("model=\"claude-x\""));
    }

    #[tokio::test]
    async fn non_strict_catch_all_sees_normalized_path() {
        async fn echo_path(req: Request) -> Response {
            let path = req.uri().path().to_string();
            let resp = serde_json::json!({
                "id": "chatcmpl-1",
                "model": "m",
                "choices": [ { "message": { "role": "assistant", "content": path }, "finish_reason": "stop" } ],
                "usage": { "prompt_tokens": 0, "completion_tokens": 0 }
            });
            json_response(StatusCode::OK, Bytes::from(serde_json::to_vec(&resp).unwrap()))
        }

        let inner = Router::new().route("/{*rest}", post(echo_path));
        let server = test_app(inner);

        let response = server
            .post("/ai/v1/messages")
            .add_header("x-api-key", "sk-test")
            .json(&serde_json::json!({ "model": "claude-x", "max_tokens": 10, "messages": [ { "role": "user", "content": "hi" } ] }))
            .await;

        assert_eq!(response.status_code().as_u16(), 200);
        let body: serde_json::Value = response.json();
        let seen = body["content"][0]["text"].as_str().unwrap();
        assert!(seen.ends_with("/chat/completions"), "downstream saw: {seen}");
        assert!(!seen.contains("/messages"), "downstream saw: {seen}");
    }

    #[tokio::test]
    async fn non_post_request_is_not_intercepted() {
        async fn marker(_req: Request) -> Response {
            json_response(StatusCode::OK, Bytes::from_static(b"{\"handler\":\"reached\"}"))
        }

        let inner = Router::new().route("/messages", axum::routing::get(marker));
        let server = test_app(inner);

        let response = server.get("/ai/v1/messages").add_header("x-api-key", "sk-test").await;

        assert_eq!(
            response.status_code().as_u16(),
            200,
            "a GET must reach the route, not be claimed by the translator"
        );
        let body: serde_json::Value = response.json();
        assert_eq!(body["handler"], "reached");
    }

    #[tokio::test]
    async fn get_is_still_translated_for_header_only_translator() {
        async fn echo_auth(req: Request) -> Response {
            let seen = req
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("<none>")
                .to_string();
            let body = serde_json::json!({
                "object": "list",
                "data": [ { "id": seen, "object": "model", "created": 0, "owned_by": "test" } ]
            });
            json_response(StatusCode::OK, Bytes::from(serde_json::to_vec(&body).unwrap()))
        }

        let registry = TranslationRegistry::new(vec![Arc::new(crate::inference::translation::anthropic::models::AnthropicModels)]);
        let inner = Router::new()
            .route("/models", axum::routing::get(echo_auth))
            .layer(axum::middleware::from_fn_with_state(registry, translation_middleware));
        let app = Router::new().nest("/ai/v1", inner);
        let server = axum_test::TestServer::new(app).expect("test server");

        let response = server
            .get("/ai/v1/models")
            .add_header("x-api-key", "sk-test")
            .add_header("anthropic-version", "2023-06-01")
            .await;

        assert_eq!(response.status_code().as_u16(), 200);
        let body: serde_json::Value = response.json();
        assert_eq!(
            body["data"][0]["id"], "Bearer sk-test",
            "a header-only translator must still normalise auth on a GET"
        );
    }

    #[tokio::test]
    async fn streaming_request_is_reframed_to_anthropic_events() {
        async fn fake_sse(_req: Request) -> Response {
            let sse = concat!(
                "data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n",
                "data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n",
                "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n",
                "data: [DONE]\n\n",
            );
            let mut resp = Response::new(Body::from(sse));
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("text/event-stream"));
            resp
        }

        let inner = Router::new().route("/messages", post(fake_sse));
        let server = test_app(inner);

        let response = server
            .post("/ai/v1/messages")
            .add_header("x-api-key", "sk-test")
            .json(&serde_json::json!({ "model": "claude-x", "max_tokens": 10, "stream": true, "messages": [ { "role": "user", "content": "hi" } ] }))
            .await;

        assert_eq!(response.status_code().as_u16(), 200);
        let text = response.text();
        for ev in [
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ] {
            assert!(text.contains(&format!("event: {ev}")), "missing {ev} in:\n{text}");
        }
        assert!(text.contains(r#""text":"Hi""#));
        assert!(text.find("message_start").unwrap() < text.find("message_stop").unwrap());
    }

    #[tokio::test]
    async fn sse_event_split_across_chunks_is_reassembled() {
        async fn fake_split_sse(_req: Request) -> Response {
            let pieces: Vec<Result<Bytes, std::io::Error>> = vec![
                Ok(Bytes::from_static(b"data: {\"id\":\"c1\",\"model\":\"m\",\"cho")),
                Ok(Bytes::from_static(b"ices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n")),
                Ok(Bytes::from_static(
                    b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n",
                )),
                Ok(Bytes::from_static(b"\ndata: [DONE]\n\n")),
            ];
            let mut resp = Response::new(Body::from_stream(futures::stream::iter(pieces)));
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("text/event-stream"));
            resp
        }

        let inner = Router::new().route("/messages", post(fake_split_sse));
        let server = test_app(inner);

        let response = server
            .post("/ai/v1/messages")
            .add_header("x-api-key", "sk-test")
            .json(&serde_json::json!({ "model": "claude-x", "max_tokens": 10, "stream": true, "messages": [ { "role": "user", "content": "hi" } ] }))
            .await;

        assert_eq!(response.status_code().as_u16(), 200);
        let text = response.text();
        assert!(text.contains("event: message_start"), "{text}");
        assert!(text.contains(r#""text":"Hi""#), "{text}");
        assert!(text.contains(r#""stop_reason":"end_turn""#), "{text}");
        assert!(text.contains("event: message_stop"), "{text}");
    }

    #[tokio::test]
    async fn multibyte_char_split_across_chunks_is_intact() {
        async fn fake(_req: Request) -> Response {
            let full = "data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"delta\":{\"content\":\"caf\u{00e9}\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
            let bytes = full.as_bytes();
            let split = bytes.iter().position(|&b| b == 0xC3).unwrap() + 1;
            let pieces: Vec<Result<Bytes, std::io::Error>> = vec![
                Ok(Bytes::copy_from_slice(&bytes[..split])),
                Ok(Bytes::copy_from_slice(&bytes[split..])),
            ];
            let mut resp = Response::new(Body::from_stream(futures::stream::iter(pieces)));
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("text/event-stream"));
            resp
        }

        let inner = Router::new().route("/messages", post(fake));
        let server = test_app(inner);

        let response = server
            .post("/ai/v1/messages")
            .add_header("x-api-key", "sk-test")
            .json(&serde_json::json!({ "model": "m", "max_tokens": 1, "stream": true, "messages": [] }))
            .await;

        let text = response.text();
        assert!(text.contains("caf\u{00e9}"), "multibyte corrupted: {text}");
    }

    #[tokio::test]
    async fn error_responses_become_anthropic_error_envelope() {
        async fn status_echo(req: Request) -> Response {
            let code = req
                .headers()
                .get("x-test-status")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(200);
            let mut resp = Response::new(Body::from(r#"{"error":{"message":"boom"}}"#));
            *resp.status_mut() = StatusCode::from_u16(code).unwrap();
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));
            resp
        }

        let inner = Router::new().route("/messages", post(status_echo));
        let server = test_app(inner);

        for (code, ty) in [(400u16, "invalid_request_error"), (429, "rate_limit_error"), (500, "api_error")] {
            let cs = code.to_string();
            let response = server
                .post("/ai/v1/messages")
                .add_header("x-api-key", "sk-test")
                .add_header("x-test-status", cs.as_str())
                .json(&serde_json::json!({ "model": "m", "max_tokens": 1, "messages": [ { "role": "user", "content": "hi" } ] }))
                .await;

            assert_eq!(response.status_code().as_u16(), code);
            let body: serde_json::Value = response.json();
            assert_eq!(body["type"], "error");
            assert_eq!(body["error"]["type"], ty, "status {code}");
            assert_eq!(body["error"]["message"], "boom");
        }
    }

    #[tokio::test]
    async fn native_chat_completions_passes_through() {
        async fn echo(req: Request) -> Response {
            let body = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap();
            json_response(StatusCode::OK, body)
        }

        let inner = Router::new().route("/chat/completions", post(echo));
        let server = test_app(inner);

        let response = server
            .post("/ai/v1/chat/completions")
            .json(&serde_json::json!({ "model": "gpt-x", "messages": [] }))
            .await;

        assert_eq!(response.status_code().as_u16(), 200);
        let body: serde_json::Value = response.json();
        assert_eq!(body["model"], "gpt-x");
    }
}
