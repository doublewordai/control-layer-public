
use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::internal_credential::{InternalAuthState, carried_key_hash};
use crate::config::Config;
use crate::metrics::errors::component::ANALYTICS;
use crate::reference_cache::{Completion, ReferenceCache};
use crate::request_logging::batcher::{AnalyticsOutboxWriter, RawAnalyticsRecord};
use crate::request_logging::models::{AiResponse, ChatCompletionChunk};
use crate::request_logging::serializers::{Auth, UsageMetrics, parse_ai_response};
use crate::request_logging::stream_completion::client_disconnected;
use crate::request_logging::utils::{extract_header_as_string, extract_header_as_uuid};
use axum::http::StatusCode;
use onwards::AuthenticatedApiKeyId;
use onwards::strict::schemas::chat_completions::MessageContent;
use openai_reassembler::CompletionEvidence;
use outlet::{RequestData, RequestHandler, ResponseData};
use tracing::{Instrument, info_span};
use uuid::Uuid;

fn zdr_safe_parse_error(err: &(dyn std::error::Error + Send + Sync + 'static)) -> String {
    match err.downcast_ref::<serde_json::Error>() {
        Some(e) => format!("json parse error ({:?}) at line {} column {}", e.classify(), e.line(), e.column()),
        None => "parse error".to_string(),
    }
}

fn is_reasoning_without_answer(response: &AiResponse) -> bool {
    match response {
        AiResponse::ChatCompletions(response) => response.choices.iter().any(|choice| {
            let message = &choice.message;
            let has_reasoning = [message.reasoning_content.as_deref(), message.reasoning.as_deref()]
                .into_iter()
                .flatten()
                .any(|text| !text.is_empty());
            let has_answer = match &message.content {
                Some(MessageContent::Text(text)) => !text.is_empty(),
                Some(MessageContent::Parts(_)) => true,
                None => false,
            };
            let has_tool_call = message.tool_calls.as_ref().is_some_and(|calls| !calls.is_empty())
                || message
                    .extra
                    .as_ref()
                    .and_then(|extra| extra.get("function_call"))
                    .is_some_and(|call| !call.is_null());

            let mut evidence = CompletionEvidence::default();
            evidence.observe(choice.finish_reason.as_deref(), has_reasoning, has_answer, has_tool_call);
            evidence.is_reasoning_without_answer()
        }),
        AiResponse::ChatCompletionsStream(chunks) => {
            let mut choices: HashMap<u32, CompletionEvidence> = HashMap::new();
            for chunk in chunks {
                let ChatCompletionChunk::Chunk(chunk) = chunk else {
                    continue;
                };
                for choice in &chunk.choices {
                    let has_reasoning = [choice.delta.reasoning_content.as_deref(), choice.delta.reasoning.as_deref()]
                        .into_iter()
                        .flatten()
                        .any(|text| !text.is_empty());
                    let has_answer = choice.delta.content.as_deref().is_some_and(|text| !text.is_empty());
                    let has_tool_call = choice.delta.tool_calls.as_ref().is_some_and(|calls| !calls.is_empty());
                    choices.entry(choice.index).or_default().observe(
                        choice.finish_reason.as_deref(),
                        has_reasoning,
                        has_answer,
                        has_tool_call,
                    );
                }
            }
            choices.values().any(CompletionEvidence::is_reasoning_without_answer)
        }
        _ => false,
    }
}

const BATCH_CREATOR_USER_AGENT_HEADER: &str = "x-fusillade-batch-dw-user-agent";

fn resolve_user_agent(request_data: &RequestData) -> Option<String> {
    extract_header_as_string(request_data, BATCH_CREATOR_USER_AGENT_HEADER)
        .or_else(|| extract_header_as_string(request_data, "user-agent"))
        .map(|ua| ua.chars().take(256).collect())
}

pub struct AnalyticsHandler {
    outbox_writer: AnalyticsOutboxWriter,
    instance_id: Uuid,
    config: Config,
    reference_cache: Option<(Arc<ReferenceCache>, InternalAuthState)>,
}

impl AnalyticsHandler {
    pub fn new(outbox_writer: AnalyticsOutboxWriter, instance_id: Uuid, config: Config) -> Self {
        Self {
            outbox_writer,
            instance_id,
            config,
            reference_cache: None,
        }
    }

    pub fn with_reference_cache(mut self, reference_cache: Arc<ReferenceCache>, internal_auth: InternalAuthState) -> Self {
        self.reference_cache = Some((reference_cache, internal_auth));
        self
    }
}

impl RequestHandler for AnalyticsHandler {
    async fn handle_request(&self, _data: RequestData) {
    }

    async fn handle_response(&self, request_data: RequestData, mut response_data: ResponseData) {
        let correlation_id = request_data.correlation_id;
        let span = info_span!(
            "dwctl.analytics_handler",
            correlation_id = correlation_id,
            status = %response_data.status,
            x_request_id = tracing::field::Empty,
        );
        if let Some(x_request_id) = extract_header_as_string(&request_data, "x-request-id") {
            span.record("x_request_id", tracing::field::display(x_request_id));
        }

        async {
            let usage_bearing = is_usage_bearing_path(request_data.uri.path());

            let parsed = parse_ai_response(&request_data, &response_data);

            if usage_bearing
                && response_data.status.is_success()
                && let Err(e) = &parsed
            {
                crate::background_error!(
                    ANALYTICS, "parse_error", Error,
                    correlation_id = correlation_id,
                    uri = %request_data.uri,
                    error = %zdr_safe_parse_error(e.error.as_ref()),
                    "Failed to parse a successful generative response"
                );
            }

            let parsed = parsed.unwrap_or(AiResponse::Other(serde_json::Value::Null));

            if response_data.status.is_success() && is_reasoning_without_answer(&parsed) {
                response_data.status = StatusCode::BAD_GATEWAY;
            }

            let metrics = UsageMetrics::extract(self.instance_id, &request_data, &response_data, &parsed, &self.config);

            if (200..300).contains(&metrics.status_code) && metrics.total_tokens == 0 && usage_bearing {
                crate::background_error!(
                    ANALYTICS, "missing_usage", Error,
                    correlation_id = correlation_id,
                    uri = %request_data.uri,
                    response_type = %metrics.response_type,
                    request_model = metrics.request_model.as_deref(),
                    response_model = metrics.response_model.as_deref(),
                    client_disconnected = client_disconnected(&response_data),
                    "Successful generative response recorded zero tokens"
                );
            }

            let fusillade_batch_id = extract_header_as_uuid(&request_data, "x-fusillade-batch-id");
            let fusillade_request_id = extract_header_as_uuid(&request_data, "x-fusillade-request-id");
            let custom_id = extract_header_as_string(&request_data, "x-fusillade-custom-id");
            let batch_completion_window = extract_header_as_string(&request_data, "x-fusillade-batch-completion-window");
            let batch_request_source = extract_header_as_string(&request_data, "x-fusillade-batch-request-source").unwrap_or_default();

            let batch_created_at = extract_header_as_string(&request_data, "x-fusillade-batch-created-at")
                .and_then(|s| s.parse::<chrono::DateTime<chrono::Utc>>().ok());

            let api_key_id = response_data.extensions.get::<AuthenticatedApiKeyId>().map(|id| id.0);

            let reference_cached_tokens = match &self.reference_cache {
                Some((cache, internal_auth)) => {
                    let bearer_token = match Auth::from_request(&request_data, &self.config) {
                        Auth::ApiKey { bearer_token } => Some(bearer_token),
                        Auth::None => None,
                    };
                    let api_key = internal_auth.captured_key_hash(bearer_token.as_deref(), carried_key_hash(&response_data.extensions));
                    cache
                        .complete(Completion {
                            path: request_data.uri.path(),
                            success: (200..300).contains(&metrics.status_code),
                            api_key,
                            model: metrics.request_model.as_deref(),
                            body: request_data.body.as_ref(),
                            prompt_tokens: metrics.prompt_tokens,
                            arrival: chrono::DateTime::<chrono::Utc>::from(request_data.timestamp),
                        })
                        .await
                }
                None => None,
            };

            let record = RawAnalyticsRecord {
                instance_id: metrics.instance_id,
                correlation_id: metrics.correlation_id,
                timestamp: metrics.timestamp,
                method: metrics.method,
                uri: metrics.uri,
                request_model: metrics.request_model,
                response_model: metrics.response_model,
                status_code: metrics.status_code,
                duration_ms: metrics.duration_ms,
                duration_to_first_byte_ms: metrics.duration_to_first_byte_ms,
                prompt_tokens: metrics.prompt_tokens,
                completion_tokens: metrics.completion_tokens,
                reasoning_tokens: metrics.reasoning_tokens,
                total_tokens: metrics.total_tokens,
                cache_read_input_tokens: metrics.cache_read_input_tokens,
                cache_creation_5m_input_tokens: metrics.cache_creation_5m_input_tokens,
                cache_creation_1h_input_tokens: metrics.cache_creation_1h_input_tokens,
                cache_creation_24h_input_tokens: metrics.cache_creation_24h_input_tokens,
                response_type: metrics.response_type,
                finish_reason: metrics.finish_reason,
                user_agent: resolve_user_agent(&request_data),
                engine_cached_tokens: metrics.engine_cached_tokens,
                reference_cached_tokens,
                cache_read_source: metrics.cache_read_source.clone(),
                request_params: metrics.request_params,
                server_address: metrics.server_address,
                server_port: metrics.server_port,
                served_by: metrics.served_by,
                served_model: metrics.served_model,
                requested_serving_class: metrics.requested_serving_class,
                resolved_serving_class: metrics.resolved_serving_class,
                api_key_id,
                fusillade_batch_id,
                fusillade_request_id,
                custom_id,
                batch_completion_window,
                batch_created_at,
                batch_request_source,
                trace_id: request_data.trace_id.clone(),
                gateway_span_id: request_data.span_id.clone(),
                x_request_id: extract_header_as_uuid(&request_data, "x-request-id"),
            };

            if let Err(e) = self.outbox_writer.publish(record).await {
                crate::background_error!(
                    ANALYTICS, "outbox_publish", Error,
                    correlation_id = correlation_id,
                    error = %e,
                    "Failed to publish analytics record to the durable outbox"
                );
                metrics::counter!("dwctl_analytics_outbox_publish_failures_total").increment(1);
            }
        }
        .instrument(span)
        .await;
    }
}

fn is_usage_bearing_path(path: &str) -> bool {
    path.ends_with("/chat/completions")
        || path.ends_with("/completions")
        || path.ends_with("/embeddings")
        || path.ends_with("/responses")
        || path.ends_with("/messages")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Method, StatusCode, Uri};
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};
    use tokio::sync::mpsc;

    fn create_test_request_data() -> RequestData {
        RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: Uri::from_static("/ai/v1/chat/completions"),
            headers: HashMap::new(),
            body: None,
            trace_id: None,
            span_id: None,
        }
    }

    fn create_test_response_data() -> ResponseData {
        ResponseData {
            extensions: Default::default(),
            correlation_id: 123,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: None,
            duration_to_first_byte: Duration::from_millis(10),
            duration: Duration::from_millis(100),
        }
    }

    fn request_data_with_headers(headers: &[(&str, &str)]) -> RequestData {
        RequestData {
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_string(), vec![bytes::Bytes::from(value.to_string())]))
                .collect(),
            ..create_test_request_data()
        }
    }

    #[test]
    fn batch_creator_user_agent_is_used_when_the_wire_carries_none() {
        let data = request_data_with_headers(&[(BATCH_CREATOR_USER_AGENT_HEADER, "OpenAI/Python 1.2.3")]);
        assert_eq!(resolve_user_agent(&data).as_deref(), Some("OpenAI/Python 1.2.3"));
    }

    #[test]
    fn batch_creator_user_agent_beats_the_dispatching_daemons() {
        let data = request_data_with_headers(&[
            (BATCH_CREATOR_USER_AGENT_HEADER, "claude-cli/1.0.0"),
            ("user-agent", "fusillade/22.0.1"),
        ]);
        assert_eq!(resolve_user_agent(&data).as_deref(), Some("claude-cli/1.0.0"));
    }

    #[test]
    fn live_traffic_still_reports_the_wire_user_agent() {
        let data = request_data_with_headers(&[("user-agent", "curl/8.4.0")]);
        assert_eq!(resolve_user_agent(&data).as_deref(), Some("curl/8.4.0"));
        assert_eq!(resolve_user_agent(&create_test_request_data()), None);
    }

    #[test]
    fn a_pathological_user_agent_is_truncated_on_a_char_boundary() {
        let long = "é".repeat(400);
        let data = request_data_with_headers(&[(BATCH_CREATOR_USER_AGENT_HEADER, &long)]);
        let resolved = resolve_user_agent(&data).expect("a user agent");
        assert_eq!(resolved.chars().count(), 256);
        assert_eq!(resolved.len(), 512, "expected 2 bytes per char, i.e. no mid-codepoint cut");
    }

    #[test]
    fn test_analytics_handler_creation() {
        let (tx, _rx) = mpsc::channel::<RawAnalyticsRecord>(100);
        let config = Config::default();

        let _handler = AnalyticsHandler::new(AnalyticsOutboxWriter::for_test(tx), Uuid::new_v4(), config);
    }

    #[test]
    fn test_request_data_creation() {
        let data = create_test_request_data();
        assert_eq!(data.correlation_id, 123);
        assert_eq!(data.method, Method::POST);
    }

    #[test]
    fn test_response_data_creation() {
        let data = create_test_response_data();
        assert_eq!(data.correlation_id, 123);
        assert_eq!(data.status, StatusCode::OK);
    }

    #[test]
    fn detects_reasoning_without_an_answer_in_a_blocking_response() {
        let response: AiResponse = serde_json::from_value(serde_json::json!({
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "created": 1,
            "model": "model",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": null,
                    "reasoning_content": "thinking"
                },
                "finish_reason": "stop"
            }]
        }))
        .unwrap();

        assert!(is_reasoning_without_answer(&response));
    }

    #[test]
    fn detects_reasoning_without_an_answer_in_an_already_parsed_stream() {
        let response = AiResponse::ChatCompletionsStream(
            [
                serde_json::json!({
                    "id": "chatcmpl-1", "object": "chat.completion.chunk",
                    "created": 1, "model": "model",
                    "choices": [{"index": 0, "delta": {"reasoning_content": "thinking"}}]
                }),
                serde_json::json!({
                    "id": "chatcmpl-1", "object": "chat.completion.chunk",
                    "created": 1, "model": "model",
                    "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]
                }),
            ]
            .into_iter()
            .map(|value| ChatCompletionChunk::Chunk(serde_json::from_value(value).unwrap()))
            .collect(),
        );

        assert!(is_reasoning_without_answer(&response));
    }

    #[test]
    fn reasoning_with_a_tool_call_is_a_valid_completion() {
        let response: AiResponse = serde_json::from_value(serde_json::json!({
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "created": 1,
            "model": "model",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": null,
                    "reasoning_content": "thinking",
                    "tool_calls": [{
                        "id": "call-1",
                        "type": "function",
                        "function": {"name": "lookup", "arguments": "{}"}
                    }]
                },
                "finish_reason": "stop"
            }]
        }))
        .unwrap();

        assert!(!is_reasoning_without_answer(&response));
    }

    #[tokio::test]
    async fn test_handler_sends_to_channel() {
        let (tx, mut rx) = mpsc::channel::<RawAnalyticsRecord>(100);
        let config = Config::default();
        let handler = AnalyticsHandler::new(AnalyticsOutboxWriter::for_test(tx), Uuid::new_v4(), config);
        let api_key_id = Uuid::new_v4();

        let request_data = create_test_request_data();
        let mut response_data = create_test_response_data();
        response_data.extensions.insert(AuthenticatedApiKeyId(api_key_id));
        handler.handle_response(request_data, response_data).await;

        let record = rx.try_recv().expect("Should have received a record");
        assert_eq!(record.correlation_id, 123);
        assert_eq!(record.method, "POST");
        assert!(record.uri.contains("chat/completions"));
        assert_eq!(record.api_key_id, Some(api_key_id));
    }

    #[tokio::test]
    async fn a_dispatched_batch_request_reports_the_batchs_creator_as_its_client() {
        let (tx, mut rx) = mpsc::channel::<RawAnalyticsRecord>(100);
        let handler = AnalyticsHandler::new(AnalyticsOutboxWriter::for_test(tx), Uuid::new_v4(), Config::default());

        handler
            .handle_response(
                request_data_with_headers(&[(BATCH_CREATOR_USER_AGENT_HEADER, "OpenAI/Python 1.2.3")]),
                create_test_response_data(),
            )
            .await;

        let record = rx.try_recv().expect("Should have received a record");
        assert_eq!(
            record.user_agent.as_deref(),
            Some("OpenAI/Python 1.2.3"),
            "the batch creator's client must reach the analytics record"
        );
    }
}
