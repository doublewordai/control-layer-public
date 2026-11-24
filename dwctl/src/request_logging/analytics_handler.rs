
use std::collections::HashMap;

use crate::auth::internal_credential::InternalAuthState;
use crate::config::Config;
use crate::inference::rejections::api_key;
use crate::metrics::errors::component::ANALYTICS;
use crate::request_logging::batcher::{AnalyticsOutboxWriter, RawAnalyticsRecord};
use crate::request_logging::json_output::OutputContext;
use crate::request_logging::models::{AiResponse, ChatCompletionChunk};
use crate::request_logging::serializers::{UsageMetrics, parse_ai_response};
use crate::request_logging::stream_completion::{CLIENT_DISCONNECTED, client_disconnected};
use crate::request_logging::utils::{extract_header_as_string, extract_header_as_uuid, header_map};
use crate::request_logging::{structured_output, tool_calls};
use crate::telemetry::TraceParent;
use axum::http::StatusCode;
use onwards::AuthenticatedApiKeyId;
use onwards::auth::{API_KEY_ID_LABEL, KeyHash};
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

pub const STATUS_CLIENT_CLOSED_REQUEST: i32 = 499;

struct HeaderFields {
    fusillade_batch_id: Option<Uuid>,
    fusillade_request_id: Option<Uuid>,
    custom_id: Option<String>,
    batch_completion_window: Option<String>,
    batch_request_source: String,
    batch_created_at: Option<chrono::DateTime<chrono::Utc>>,
    user_agent: Option<String>,
    x_request_id: Option<Uuid>,
}

impl HeaderFields {
    fn read(request_data: &RequestData) -> Self {
        Self {
            fusillade_batch_id: extract_header_as_uuid(request_data, "x-fusillade-batch-id"),
            fusillade_request_id: extract_header_as_uuid(request_data, "x-fusillade-request-id"),
            custom_id: extract_header_as_string(request_data, "x-fusillade-custom-id"),
            batch_completion_window: extract_header_as_string(request_data, "x-fusillade-batch-completion-window"),
            batch_request_source: extract_header_as_string(request_data, "x-fusillade-batch-request-source").unwrap_or_default(),
            batch_created_at: extract_header_as_string(request_data, "x-fusillade-batch-created-at")
                .and_then(|s| s.parse::<chrono::DateTime<chrono::Utc>>().ok()),
            user_agent: resolve_user_agent(request_data),
            x_request_id: extract_header_as_uuid(request_data, "x-request-id"),
        }
    }
}

fn abandoned_record(
    instance_id: Uuid,
    request_data: &RequestData,
    api_key_id: Option<Uuid>,
    config: &Config,
    abandoned_at: std::time::SystemTime,
) -> RawAnalyticsRecord {
    let headers = HeaderFields::read(request_data);
    let duration_ms = abandoned_at.duration_since(request_data.timestamp).unwrap_or_default().as_millis();
    RawAnalyticsRecord {
        instance_id,
        correlation_id: request_data.correlation_id as i64,
        timestamp: chrono::DateTime::<chrono::Utc>::from(request_data.timestamp),
        method: request_data.method.to_string(),
        uri: request_data.uri.to_string(),
        request_model: extract_header_as_string(request_data, "x-onwards-model"),
        response_model: None,
        status_code: STATUS_CLIENT_CLOSED_REQUEST,
        duration_ms: i64::try_from(duration_ms).unwrap_or(i64::MAX),
        duration_to_first_byte_ms: None,
        prompt_tokens: 0,
        completion_tokens: 0,
        reasoning_tokens: 0,
        total_tokens: 0,
        cache_read_input_tokens: 0,
        cache_creation_5m_input_tokens: 0,
        cache_creation_1h_input_tokens: 0,
        cache_creation_24h_input_tokens: 0,
        response_type: "abandoned".to_string(),
        finish_reason: Some(CLIENT_DISCONNECTED.to_string()),
        user_agent: headers.user_agent,
        engine_cached_tokens: None,
        reference_cached_tokens: None,
        cache_read_source: None,
        request_params: Default::default(),
        server_address: config.host.clone(),
        server_port: config.port,
        served_by: None,
        served_model: None,
        compute_pool: None,
        requested_serving_class: None,
        resolved_serving_class: None,
        api_key_id,
        fusillade_batch_id: headers.fusillade_batch_id,
        fusillade_request_id: headers.fusillade_request_id,
        custom_id: headers.custom_id,
        batch_completion_window: headers.batch_completion_window,
        batch_created_at: headers.batch_created_at,
        batch_request_source: headers.batch_request_source,
        trace_id: request_data.trace_id.clone(),
        gateway_span_id: request_data.span_id.clone(),
        x_request_id: headers.x_request_id,
    }
}

pub struct AnalyticsHandler {
    outbox_writer: AnalyticsOutboxWriter,
    instance_id: Uuid,
    config: Config,
    key_identity: Option<InternalAuthState>,
}

impl AnalyticsHandler {
    pub fn new(outbox_writer: AnalyticsOutboxWriter, instance_id: Uuid, config: Config) -> Self {
        Self {
            outbox_writer,
            instance_id,
            config,
            key_identity: None,
        }
    }

    pub fn with_key_identity(mut self, internal_auth: InternalAuthState) -> Self {
        self.key_identity = Some(internal_auth);
        self
    }

    fn abandoned_api_key_id(&self, request_data: &RequestData) -> Option<Uuid> {
        let internal_auth = self.key_identity.as_ref()?;
        let headers = header_map(request_data);
        let hash = internal_auth.captured_key_hash(api_key(request_data.uri.path(), &headers), None)?;
        let labels = internal_auth.targets.key_labels.get(&KeyHash::from_bytes(hash))?;
        labels.get(API_KEY_ID_LABEL)?.parse().ok()
    }

    async fn publish(&self, record: RawAnalyticsRecord) {
        let correlation_id = record.correlation_id;
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

            let reasoning_without_answer = response_data.status.is_success() && is_reasoning_without_answer(&parsed);
            if reasoning_without_answer {
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

            let completed = ((200..300).contains(&metrics.status_code) || reasoning_without_answer) && !client_disconnected(&response_data);
            if completed {
                let served_by = metrics
                    .served_by
                    .as_deref()
                    .and_then(|url| url::Url::parse(url).ok())
                    .and_then(|url| url.host_str().map(str::to_owned));
                let reported = |tokens: i64| (metrics.total_tokens > 0).then_some(tokens);
                let context = OutputContext {
                    model: metrics.request_model.as_deref(),
                    served_by: served_by.as_deref(),
                    max_tokens: metrics.request_params.max_tokens,
                    output_tokens: reported(metrics.completion_tokens),
                    reasoning_tokens: reported(metrics.reasoning_tokens),
                    parent: response_data.extensions.get::<TraceParent>().map(|TraceParent(parent)| parent),
                };
                let tool_choice = metrics.request_params.tool_choice.as_deref();
                if let Some(report) = tool_calls::inspect(&parsed, tool_choice) {
                    tool_calls::record(&report, &context, tool_choice);
                }
                let format = metrics.request_params.response_format.as_deref();
                if let Some(format) = format.filter(|format| structured_output::expects_json(format))
                    && let Some(report) = structured_output::inspect(&parsed)
                {
                    structured_output::record(&report, &context, format);
                }
            }

            let headers = HeaderFields::read(&request_data);

            let api_key_id = response_data.extensions.get::<AuthenticatedApiKeyId>().map(|id| id.0);

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
                user_agent: headers.user_agent,
                engine_cached_tokens: metrics.engine_cached_tokens,
                reference_cached_tokens: metrics.reference_cached_tokens,
                cache_read_source: metrics.cache_read_source.clone(),
                request_params: metrics.request_params,
                server_address: metrics.server_address,
                server_port: metrics.server_port,
                served_by: metrics.served_by,
                served_model: metrics.served_model,
                compute_pool: metrics.compute_pool,
                requested_serving_class: metrics.requested_serving_class,
                resolved_serving_class: metrics.resolved_serving_class,
                api_key_id,
                fusillade_batch_id: headers.fusillade_batch_id,
                fusillade_request_id: headers.fusillade_request_id,
                custom_id: headers.custom_id,
                batch_completion_window: headers.batch_completion_window,
                batch_created_at: headers.batch_created_at,
                batch_request_source: headers.batch_request_source,
                trace_id: request_data.trace_id.clone(),
                gateway_span_id: request_data.span_id.clone(),
                x_request_id: headers.x_request_id,
            };

            self.publish(record).await;
        }
        .instrument(span)
        .await;
    }

    async fn handle_abandoned(&self, request_data: RequestData) {
        let span = info_span!(
            "dwctl.analytics_handler",
            correlation_id = request_data.correlation_id,
            status = STATUS_CLIENT_CLOSED_REQUEST,
            x_request_id = tracing::field::Empty,
        );
        if let Some(x_request_id) = extract_header_as_string(&request_data, "x-request-id") {
            span.record("x_request_id", tracing::field::display(x_request_id));
        }
        async {
            let api_key_id = self.abandoned_api_key_id(&request_data);
            let record = abandoned_record(
                self.instance_id,
                &request_data,
                api_key_id,
                &self.config,
                std::time::SystemTime::now(),
            );
            tracing::debug!(
                uri = %request_data.uri,
                request_model = record.request_model.as_deref(),
                duration_ms = record.duration_ms,
                "Request abandoned by the client before any response"
            );
            self.publish(record).await;
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

    fn key_identity(api_key_id: Uuid) -> InternalAuthState {
        let targets = onwards::target::Targets::from_config(
            serde_json::from_value(serde_json::json!({
                "targets": {"model-a": {"url": "https://upstream.example.com/v1/"}},
                "auth": {"global_keys": [], "key_definitions": {
                    "k": {"key": "sk-test", "labels": {"account": "acct-1", "api_key_id": api_key_id.to_string()}}
                }}
            }))
            .unwrap(),
        )
        .unwrap();
        InternalAuthState {
            credentials: crate::auth::internal_credential::InternalCredentials::generate(),
            targets,
        }
    }

    #[tokio::test]
    async fn an_abandoned_request_is_recorded_as_a_client_cancel() {
        let (tx, mut rx) = mpsc::channel::<RawAnalyticsRecord>(100);
        let api_key_id = Uuid::new_v4();
        let handler = AnalyticsHandler::new(AnalyticsOutboxWriter::for_test(tx), Uuid::new_v4(), Config::default())
            .with_key_identity(key_identity(api_key_id));
        let mut request = request_data_with_headers(&[
            ("authorization", "Bearer sk-test"),
            ("x-onwards-model", "model-a"),
            ("user-agent", "example-router/1.0"),
        ]);
        request.timestamp = SystemTime::now() - Duration::from_secs(300);

        handler.handle_abandoned(request).await;

        let record = rx.try_recv().expect("an abandoned request must produce a record");
        assert_eq!(record.status_code, STATUS_CLIENT_CLOSED_REQUEST);
        assert_eq!(record.finish_reason.as_deref(), Some(CLIENT_DISCONNECTED));
        assert_eq!(record.request_model.as_deref(), Some("model-a"));
        assert_eq!(record.api_key_id, Some(api_key_id));
        assert_eq!(record.user_agent.as_deref(), Some("example-router/1.0"));
        assert!(
            record.duration_ms >= 300_000,
            "duration is the time the caller waited: {}",
            record.duration_ms
        );
        assert_eq!(record.duration_to_first_byte_ms, None);
        assert_eq!((record.prompt_tokens, record.completion_tokens, record.total_tokens), (0, 0, 0));
        assert_eq!(record.response_type, "abandoned");
    }

    #[tokio::test]
    async fn an_abandoned_request_with_an_unknown_key_is_still_recorded() {
        let (tx, mut rx) = mpsc::channel::<RawAnalyticsRecord>(100);
        let handler = AnalyticsHandler::new(AnalyticsOutboxWriter::for_test(tx), Uuid::new_v4(), Config::default())
            .with_key_identity(key_identity(Uuid::new_v4()));
        handler
            .handle_abandoned(request_data_with_headers(&[("authorization", "Bearer sk-unknown")]))
            .await;
        let record = rx.try_recv().expect("record");
        assert_eq!(record.status_code, STATUS_CLIENT_CLOSED_REQUEST);
        assert_eq!(record.api_key_id, None);
        assert_eq!(record.request_model, None);
    }

    #[tokio::test]
    async fn an_abandoned_messages_request_is_attributed_by_its_x_api_key() {
        let (tx, mut rx) = mpsc::channel::<RawAnalyticsRecord>(100);
        let api_key_id = Uuid::new_v4();
        let handler = AnalyticsHandler::new(AnalyticsOutboxWriter::for_test(tx), Uuid::new_v4(), Config::default())
            .with_key_identity(key_identity(api_key_id));
        let request = RequestData {
            uri: Uri::from_static("/ai/v1/messages"),
            ..request_data_with_headers(&[("x-api-key", "sk-test")])
        };
        handler.handle_abandoned(request).await;
        assert_eq!(rx.try_recv().expect("record").api_key_id, Some(api_key_id));
    }

    fn metrics_after(request_body: &'static str, response_body: &'static str, ended: bool) -> String {
        let prometheus = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = prometheus.handle();
        metrics::with_local_recorder(&prometheus, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let (tx, _rx) = mpsc::channel::<RawAnalyticsRecord>(100);
                    let handler = AnalyticsHandler::new(AnalyticsOutboxWriter::for_test(tx), Uuid::new_v4(), Config::default());
                    let request = RequestData {
                        body: Some(bytes::Bytes::from(request_body)),
                        ..create_test_request_data()
                    };
                    let mut response = create_test_response_data();
                    response.body = Some(bytes::Bytes::from(response_body));
                    let completion = crate::request_logging::stream_completion::StreamCompletion::default();
                    if ended {
                        completion.mark_ended();
                    }
                    response.extensions.insert(completion);
                    handler.handle_response(request, response).await;
                })
        });
        handle.render()
    }

    #[test]
    fn tool_calls_are_checked_only_in_completed_responses() {
        let counted = |ended: bool| {
            metrics_after(
                r#"{"model":"model-a","messages":[{"role":"user","content":"u"}]}"#,
                r#"{"id":"c","object":"chat.completion","created":1,"model":"model-a","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"f","arguments":"{\"q\": "}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#,
                ended,
            )
        };
        let invalid = |rendered: &str| {
            crate::request_logging::json_output::testing::counter(
                rendered,
                "dwctl_tool_call_arguments_total",
                &[r#"model="model-a""#, r#"outcome="invalid_json""#],
            )
        };
        assert_eq!(invalid(&counted(true)), Some(1), "a completed response is checked");
        assert_eq!(invalid(&counted(false)), None, "a dropped response is not");
    }

    #[test]
    fn structured_output_is_checked_for_a_reasoning_only_answer() {
        const REASONING_ONLY: &str = r#"{"id":"c","object":"chat.completion","created":1,"model":"model-a","choices":[{"index":0,"message":{"role":"assistant","content":null,"reasoning_content":"r"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":200,"total_tokens":201}}"#;
        let rendered = metrics_after(
            r#"{"model":"model-a","messages":[{"role":"user","content":"u"}],"response_format":{"type":"json_schema","json_schema":{"name":"n","schema":{"type":"object"}}}}"#,
            REASONING_ONLY,
            true,
        );
        assert_eq!(
            crate::request_logging::json_output::testing::counter(
                &rendered,
                "dwctl_structured_output_total",
                &[
                    r#"model="model-a""#,
                    r#"response_format="json_schema""#,
                    r#"finish_reason="stop""#,
                    r#"outcome="missing""#
                ],
            ),
            Some(1),
            "{rendered}"
        );

        let plain = metrics_after(
            r#"{"model":"model-a","messages":[{"role":"user","content":"u"}]}"#,
            REASONING_ONLY,
            true,
        );
        assert!(!plain.contains("dwctl_structured_output_total"), "{plain}");
    }

    #[tokio::test]
    async fn outlet_reports_a_cancel_before_any_response_as_abandoned() {
        let (tx, mut rx) = mpsc::channel::<RawAnalyticsRecord>(100);
        let handler = AnalyticsHandler::new(AnalyticsOutboxWriter::for_test(tx), Uuid::new_v4(), Config::default());
        let config = outlet::RequestLoggerConfig {
            capture_request_body: true,
            capture_response_body: true,
            ..Default::default()
        };
        let app = axum::Router::new()
            .route(
                "/ai/v1/chat/completions",
                axum::routing::post(|| async {
                    std::future::pending::<()>().await;
                    "unreachable"
                }),
            )
            .layer(outlet::RequestLoggerLayer::new(config, handler));
        let request = axum::http::Request::post("/ai/v1/chat/completions")
            .header("x-onwards-model", "model-a")
            .body(axum::body::Body::from("{}"))
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_millis(100), tower::ServiceExt::oneshot(app, request)).await;
        assert!(outcome.is_err(), "the inner service must not answer");

        let record = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("outlet must report the abandoned request")
            .expect("record");
        assert_eq!(record.status_code, STATUS_CLIENT_CLOSED_REQUEST);
        assert_eq!(record.request_model.as_deref(), Some("model-a"));
        assert!(record.uri.ends_with("/chat/completions"));
    }
}
