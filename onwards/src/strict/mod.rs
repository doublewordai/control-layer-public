
pub mod handlers;
pub mod schema_mismatch;
pub mod schemas;
pub mod stream_end;

use crate::AppState;
use crate::client::HttpClient;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use tracing::info;

pub use schemas::chat_completions::{ChatCompletionRequest, ChatCompletionResponse};
pub use schemas::responses::{ResponsesRequest, ResponsesResponse, ResponsesStreamingEvent};

pub fn build_strict_router<T: HttpClient + Clone + Send + Sync + 'static>(
    state: AppState<T>,
) -> Router {
    info!("Building strict router with schema validation");

    Router::new()
        .route("/models", get(handlers::models_handler::<T>))
        .route(
            "/chat/completions",
            post(handlers::chat_completions_handler::<T>),
        )
        .route("/messages", post(handlers::chat_completions_handler::<T>))
        .route("/completions", post(handlers::completions_handler::<T>))
        .route("/responses", post(handlers::responses_handler::<T>))
        .route("/embeddings", post(handlers::embeddings_handler::<T>))
        .layer(DefaultBodyLimit::max(state.body_limit))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::KeyHash;
    use crate::target::{Target, Targets};
    use crate::test_utils::MockHttpClient;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use dashmap::DashMap;
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn create_test_app_state() -> AppState<MockHttpClient> {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{"id":"chatcmpl-123","object":"chat.completion","choices":[{"message":{"role":"assistant","content":"Hello!"}}]}"#;
        AppState::with_client(targets, MockHttpClient::new(StatusCode::OK, mock_response))
    }

    #[tokio::test]
    async fn test_strict_router_rejects_unknown_paths() {
        let state = create_test_app_state();
        let router = build_strict_router(state);

        let request = Request::builder()
            .uri("/unknown/endpoint")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_strict_router_accepts_models_endpoint() {
        let state = create_test_app_state();
        let router = build_strict_router(state);

        let request = Request::builder()
            .uri("/models")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    fn padded_chat_completions_body(size: usize) -> String {
        let padding = "x".repeat(size);
        format!(r#"{{"model": "gpt-4", "messages": [{{"role": "user", "content": "{padding}"}}]}}"#)
    }

    #[tokio::test]
    async fn test_strict_router_accepts_body_over_axum_2mb_default() {
        let state = create_test_app_state();
        let router = build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(padded_chat_completions_body(3 * 1024 * 1024)))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_strict_router_messages_alias_routes_to_chat_completions() {
        let state = create_test_app_state();
        let router = build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/messages")
            .header("content-type", "application/json")
            .body(Body::from(padded_chat_completions_body(16)))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_responses_handler_dispatches_edge_translated_body_to_chat() {
        let state = create_test_app_state();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(padded_chat_completions_body(16)))
            .unwrap();

        let response = handlers::responses_handler(
            axum::extract::State(state),
            axum::http::HeaderMap::new(),
            request,
        )
        .await;

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "an edge-translated chat body on /responses must be served by the chat handler"
        );
    }

    #[tokio::test]
    async fn test_strict_router_accepts_base64_image_over_axum_2mb_default() {
        let state = create_test_app_state();
        let router = build_strict_router(state);

        let base64_data = "QUJD".repeat(3 * 1024 * 1024 / 4);
        let body = serde_json::json!({
            "model": "gpt-4",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "What is in this image?"},
                    {"type": "image_url", "image_url": {"url": format!("data:image/jpeg;base64,{base64_data}")}}
                ]
            }]
        });
        assert!(body.to_string().len() > 2 * 1024 * 1024);

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_strict_router_rejects_body_over_configured_limit() {
        let state = create_test_app_state().with_body_limit(1024);
        let router = build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(padded_chat_completions_body(2048)))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    fn create_reasoning_test_app_state(body: &str) -> AppState<MockHttpClient> {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        AppState::with_client(targets, MockHttpClient::new(StatusCode::OK, body))
    }

    #[tokio::test]
    async fn strict_chat_completions_rewrites_openrouter_reasoning_field() {
        let upstream = r#"{
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1234567890,
            "model": "gpt-4",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "42",
                    "reasoning": "let me think about this"
                },
                "finish_reason": "stop"
            }]
        }"#;

        let router = build_strict_router(create_reasoning_test_app_state(upstream));

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        let message = &json["choices"][0]["message"];

        assert_eq!(
            message["reasoning_content"], "let me think about this",
            "upstream `reasoning` should be rewritten onto `reasoning_content`"
        );
        assert!(
            message.get("reasoning").is_none(),
            "the OpenRouter spelling should not reach the caller"
        );
    }

    #[tokio::test]
    async fn strict_chat_completions_leaves_vllm_reasoning_content_alone() {
        let upstream = r#"{
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1234567890,
            "model": "gpt-4",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "42",
                    "reasoning_content": "vllm style reasoning"
                },
                "finish_reason": "stop"
            }]
        }"#;

        let router = build_strict_router(create_reasoning_test_app_state(upstream));

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        let message = &json["choices"][0]["message"];

        assert_eq!(message["reasoning_content"], "vllm style reasoning");
        assert!(message.get("reasoning").is_none());
    }

    #[tokio::test]
    async fn strict_streaming_chat_completions_rewrites_reasoning_field() {
        let chunks = concat!(
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,",
            "\"model\":\"gpt-4\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"first \"},",
            "\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,",
            "\"model\":\"gpt-4\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"second\"},",
            "\"finish_reason\":null}]}\n\n",
            "data: [DONE]\n\n",
        );

        let router = build_strict_router(create_reasoning_test_app_state(chunks));

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"stream":true}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body_text = std::str::from_utf8(&body_bytes).unwrap();

        assert!(
            body_text.contains("reasoning_content"),
            "streamed deltas should carry the canonical spelling; got: {body_text}"
        );
        assert!(
            !body_text.contains("\"reasoning\""),
            "the OpenRouter spelling should not survive in any chunk; got: {body_text}"
        );
    }

    #[tokio::test]
    async fn strict_completions_carry_caller_response_id() {
        let chat_stream = concat!(
            "data: {\"id\":\"chatcmpl-upstream\",\"object\":\"chat.completion.chunk\",\"created\":1,",
            "\"model\":\"gpt-4\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},",
            "\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl-upstream\",\"object\":\"chat.completion.chunk\",\"created\":1,",
            "\"model\":\"gpt-4\",\"choices\":[],\"usage\":{\"prompt_tokens\":3,",
            "\"completion_tokens\":1,\"total_tokens\":4}}\n\n",
            "data: [DONE]\n\n",
        );
        let completion_stream = concat!(
            "data: {\"id\":\"cmpl-upstream\",\"object\":\"text_completion\",\"created\":1,",
            "\"model\":\"gpt-4\",\"choices\":[{\"index\":0,\"text\":\"hi\",\"finish_reason\":null}]}\n\n",
            "data: [DONE]\n\n",
        );
        let completion = r#"{"id":"cmpl-upstream","object":"text_completion","created":1,"model":"gpt-4","choices":[{"index":0,"text":"hi","finish_reason":"stop"}]}"#;

        let cases = [
            (
                "/chat/completions",
                r#"{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"stream":true}"#,
                chat_stream,
            ),
            (
                "/completions",
                r#"{"model":"gpt-4","prompt":"hi","stream":true}"#,
                completion_stream,
            ),
            (
                "/completions",
                r#"{"model":"gpt-4","prompt":"hi"}"#,
                completion,
            ),
        ];

        for (path, request_body, upstream) in cases {
            let state = create_reasoning_test_app_state(upstream)
                .with_response_id_header("x-fusillade-request-id");
            let request = Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json")
                .header(
                    "x-fusillade-request-id",
                    "11111111-2222-3333-4444-555555555555",
                )
                .body(Body::from(request_body))
                .unwrap();

            let response = build_strict_router(state).oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path} {request_body}");
            let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
            let body_text = std::str::from_utf8(&body_bytes).unwrap();

            let ids: Vec<String> = if request_body.contains("\"stream\":true") {
                body_text
                    .split("\n\n")
                    .filter_map(|event| event.strip_prefix("data: "))
                    .filter(|data| *data != "[DONE]")
                    .map(|data| {
                        let chunk: serde_json::Value = serde_json::from_str(data).unwrap();
                        chunk["id"].as_str().unwrap().to_string()
                    })
                    .collect()
            } else {
                let body: serde_json::Value = serde_json::from_str(body_text).unwrap();
                vec![body["id"].as_str().unwrap().to_string()]
            };
            assert!(!ids.is_empty(), "{path}: no chunks in {body_text}");
            for id in ids {
                assert_eq!(
                    id, "resp_11111111-2222-3333-4444-555555555555",
                    "{path} {request_body}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_responses_forwarded_upstream_as_passthrough() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4o".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "resp_abc123",
            "object": "response",
            "created_at": 1234567890,
            "completed_at": 1234567900,
            "status": "completed",
            "incomplete_details": null,
            "model": "gpt-4o",
            "previous_response_id": null,
            "instructions": null,
            "output": [],
            "error": null,
            "tools": [],
            "tool_choice": "auto",
            "truncation": "disabled",
            "parallel_tool_calls": true,
            "text": {
                "format": {
                    "type": "text"
                }
            },
            "top_p": 1.0,
            "presence_penalty": 0.0,
            "frequency_penalty": 0.0,
            "top_logprobs": 0,
            "temperature": 1.0,
            "reasoning": null,
            "usage": null,
            "max_output_tokens": null,
            "max_tool_calls": null,
            "store": false,
            "background": false,
            "service_tier": "default",
            "metadata": null,
            "safety_identifier": null,
            "prompt_cache_key": null
        }"#;
        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client.clone());
        let router = build_strict_router(state);

        let request_body = r#"{
            "model": "gpt-4o",
            "input": "Hello"
        }"#;

        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 1);

        assert!(requests[0].uri.contains("/responses"));
    }

    fn create_completions_test_app_state() -> (AppState<MockHttpClient>, MockHttpClient) {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-3.5-turbo-instruct".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "cmpl-abc123",
            "object": "text_completion",
            "created": 1677652288,
            "model": "gpt-3.5-turbo-instruct",
            "choices": [{"text": "Hello!", "index": 0, "logprobs": null, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 7, "total_tokens": 12}
        }"#;
        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        (
            AppState::with_client(targets, mock_client.clone()),
            mock_client,
        )
    }

    #[tokio::test]
    async fn test_strict_router_accepts_completions_endpoint() {
        let (state, _) = create_completions_test_app_state();
        let router = build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Say hello"}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(body_json["object"], "text_completion");
        assert!(body_json["choices"].is_array());
        assert!(body_json["choices"][0]["text"].is_string());
    }

    #[tokio::test]
    async fn test_strict_router_rejects_reasoning_on_completions_endpoint() {
        let (state, mock_client) = create_completions_test_app_state();
        let router = build_strict_router(state);
        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Say hello","reasoning_effort":"low"}"#,
            ))
            .unwrap();

        let response = router.clone().oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["param"], "reasoning_effort");
        assert_eq!(body["error"]["code"], "unsupported_parameter");

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Say hello","thinking":false}"#,
            ))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["param"], "thinking");
        assert_eq!(body["error"]["code"], "unsupported_parameter");

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Say hello","thinking_token_budget":1024}"#,
            ))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["param"], "thinking_token_budget");
        assert_eq!(body["error"]["code"], "unsupported_parameter");
        assert!(mock_client.get_requests().is_empty());
    }

    #[tokio::test]
    async fn test_completions_proxied_to_upstream_completions() {
        let (state, mock_client) = create_completions_test_app_state();
        let router = build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Hello"}"#,
            ))
            .unwrap();

        router.oneshot(request).await.unwrap();

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0].uri.contains("completions"),
            "Should proxy to /completions, got: {}",
            requests[0].uri
        );
        assert!(
            !requests[0].uri.contains("chat"),
            "Must NOT proxy to /chat/completions"
        );
    }

    #[tokio::test]
    async fn test_completions_accepts_missing_prompt() {
        let (state, _) = create_completions_test_app_state();
        let router = build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"model":"gpt-3.5-turbo-instruct"}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_completions_streaming_response_format() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-3.5-turbo-instruct".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .build()
                .into_pool(),
        );
        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let chunks = vec![
            "data: {\"id\":\"cmpl-abc\",\"object\":\"text_completion\",\"created\":1677652288,\"model\":\"gpt-3.5-turbo-instruct\",\"choices\":[{\"text\":\"Hello\",\"index\":0,\"logprobs\":null,\"finish_reason\":null}]}\n\n".to_string(),
            "data: {\"id\":\"cmpl-abc\",\"object\":\"text_completion\",\"created\":1677652288,\"model\":\"gpt-3.5-turbo-instruct\",\"choices\":[{\"text\":\" world\",\"index\":0,\"logprobs\":null,\"finish_reason\":null}]}\n\n".to_string(),
            "data: {\"id\":\"cmpl-abc\",\"object\":\"text_completion\",\"created\":1677652288,\"model\":\"gpt-3.5-turbo-instruct\",\"choices\":[{\"text\":\"\",\"index\":0,\"logprobs\":null,\"finish_reason\":\"stop\"}]}\n\n".to_string(),
            "data: [DONE]\n\n".to_string(),
        ];
        let mock_client = MockHttpClient::new_streaming(StatusCode::OK, chunks);
        let state = AppState::with_client(targets, mock_client);
        let router = build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Hello","stream":true}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(content_type.contains("text/event-stream"));

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();

        assert!(body_str.contains("\"object\":\"text_completion\""));
        assert!(body_str.contains("\"text\":\"Hello\""));
        assert!(body_str.contains("\"text\":\" world\""));
        assert!(body_str.contains("[DONE]"));
    }


    fn app_with_key_purpose(purpose: Option<&str>) -> (AppState<MockHttpClient>, MockHttpClient) {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "dsv4-flash".to_string(),
            Target::builder()
                .url("https://api.example.com/v1/".parse().unwrap())
                .build()
                .into_pool(),
        );
        let key_labels = Arc::new(DashMap::new());
        if let Some(purpose) = purpose {
            key_labels.insert(
                KeyHash::of("sk-test"),
                std::collections::HashMap::from([("purpose".to_string(), purpose.to_string())]),
            );
        }

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels,
            key_ids: Default::default(),
            strict_mode: true,
            http_pool_config: None,
            accounts: std::sync::Arc::new(dashmap::DashMap::new()),
        };
        let mock_client = MockHttpClient::new(
            StatusCode::OK,
            r#"{"id":"cmpl-1","object":"text_completion","created":0,"model":"dsv4-flash","choices":[]}"#,
        );
        (
            AppState::with_client(targets, mock_client.clone()),
            mock_client,
        )
    }

    async fn forwarded_body(
        state: AppState<MockHttpClient>,
        mock_client: &MockHttpClient,
        uri: &str,
        body: &str,
    ) -> serde_json::Value {
        let request = Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .header("authorization", "Bearer sk-test")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = build_strict_router(state).oneshot(request).await.unwrap();
        assert!(
            response.status().is_success(),
            "{uri}: {}",
            response.status()
        );
        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 1, "{uri}");
        serde_json::from_slice(&requests[0].body).unwrap()
    }

    #[tokio::test]
    async fn scheduling_fields_forward_verbatim_for_every_key() {
        for purpose in [Some("realtime"), Some("continuation"), None] {
            let (state, mock_client) = app_with_key_purpose(purpose);
            let body = forwarded_body(
                state,
                &mock_client,
                "/completions",
                r#"{"model":"dsv4-flash","prompt":[1,2,3],"priority":100,"stream_options":{"include_usage":true}}"#,
            )
            .await;
            assert_eq!(
                body["priority"], 100,
                "purpose={purpose:?}: onwards must not eat the field"
            );
            assert_eq!(body["stream_options"]["include_usage"], true);
        }
    }

    #[tokio::test]
    async fn a_batch_keys_deadline_priority_survives_re_serialization() {
        for (uri, body) in [
            (
                "/chat/completions",
                r#"{"model":"dsv4-flash","messages":[{"role":"user","content":"hi"}],"priority":-1754812800}"#,
            ),
            (
                "/completions",
                r#"{"model":"dsv4-flash","prompt":[1,2,3],"priority":-1754812800}"#,
            ),
        ] {
            let (state, mock_client) = app_with_key_purpose(Some("batch"));
            let forwarded = forwarded_body(state, &mock_client, uri, body).await;
            assert_eq!(
                forwarded["priority"], -1754812800,
                "{uri}: a batch key's deadline-derived priority must reach the scheduler"
            );
        }
    }

    #[tokio::test]
    async fn unmodelled_fields_forward_verbatim_after_validation() {
        for (uri, body) in [
            (
                "/completions",
                r#"{"model":"dsv4-flash","prompt":[1,2],"ignore_eos":true,"repetition_penalty":1.1,"custom_knob":true}"#,
            ),
            (
                "/chat/completions",
                r#"{"model":"dsv4-flash","messages":[{"role":"user","content":"hi"}],"custom_knob":true}"#,
            ),
        ] {
            let (state, mock_client) = app_with_key_purpose(Some("realtime"));
            let forwarded = forwarded_body(state, &mock_client, uri, body).await;
            assert_eq!(
                forwarded["custom_knob"], true,
                "{uri}: original bytes forward untouched"
            );
        }
    }
}
