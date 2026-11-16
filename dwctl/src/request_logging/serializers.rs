
use crate::config::Config;
use crate::prompt_cache::CacheBilling;
use crate::request_logging::models::{
    AiRequest, AiResponse, ChatCompletionChunk, CompletionChunk, MessagesRequest, ParsedAIRequest, ResponsesRequest,
};
use crate::request_logging::stream_completion::{CLIENT_DISCONNECTED, client_disconnected};
use outlet::{RequestData, ResponseData};
use outlet_postgres::SerializationError;
use serde_json::Value;
use std::fmt;
use std::str;
use tracing::{error, instrument};
use uuid::Uuid;

use super::utils;

#[derive(Clone)]
pub enum Auth {
    ApiKey { bearer_token: String },
    None,
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Auth::ApiKey { .. } => f.debug_struct("ApiKey").field("bearer_token", &"<redacted>").finish(),
            Auth::None => write!(f, "None"),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct HttpAnalyticsRow {
    pub instance_id: Uuid,
    pub correlation_id: i64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub method: String,
    pub uri: String,
    pub request_model: Option<String>,
    pub response_model: Option<String>,
    pub status_code: i32,
    pub duration_ms: i64,
    pub duration_to_first_byte_ms: Option<i64>,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub reasoning_tokens: i64,
    pub total_tokens: i64,
    pub response_type: String,
    pub user_id: Option<Uuid>,
    pub access_source: String,
    pub input_price_per_token: Option<rust_decimal::Decimal>,
    pub output_price_per_token: Option<rust_decimal::Decimal>,
    pub server_address: String,
    pub server_port: u16,
    pub provider_name: Option<String>,
    pub fusillade_batch_id: Option<Uuid>,
    pub fusillade_request_id: Option<Uuid>,
    pub custom_id: Option<String>,
    pub request_origin: String,
    pub batch_sla: String,
    pub batch_request_source: String,
    pub served_by: Option<String>,
    pub resolved_serving_class: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UsageMetrics {
    pub instance_id: Uuid,
    pub correlation_id: i64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub method: String,
    pub uri: String,
    pub request_model: Option<String>,
    pub response_model: Option<String>,
    pub status_code: i32,
    pub duration_ms: i64,
    pub duration_to_first_byte_ms: Option<i64>,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub reasoning_tokens: i64,
    pub total_tokens: i64,
    pub cache_read_input_tokens: i64,
    pub cache_creation_5m_input_tokens: i64,
    pub cache_creation_1h_input_tokens: i64,
    pub cache_creation_24h_input_tokens: i64,
    pub response_type: String,
    pub finish_reason: Option<String>,
    pub server_address: String,
    pub server_port: u16,
    pub served_by: Option<String>,
    pub served_model: Option<String>,
    pub engine_cached_tokens: Option<i64>,
    pub cache_read_source: Option<String>,
    pub request_params: RequestParams,
    pub requested_serving_class: Option<String>,
    pub resolved_serving_class: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RequestParams {
    pub stream: Option<bool>,
    pub max_tokens: Option<i64>,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub n: Option<i32>,
    pub tool_count: Option<i32>,
    pub message_count: Option<i32>,
}

impl RequestParams {
    pub fn from_parsed(parsed: &ParsedAIRequest) -> Self {
        if parsed.responses_request.is_some() {
            return Self::default();
        }
        if let Some(req) = &parsed.messages_request {
            return Self {
                stream: Some(req.stream.unwrap_or(false)),
                max_tokens: req.max_tokens,
                temperature: req.temperature,
                top_p: req.top_p,
                n: None,
                tool_count: Some(req.tool_count),
                message_count: req.message_count,
            };
        }
        match &parsed.request {
            AiRequest::ChatCompletions(req) => Self {
                stream: Some(req.stream.unwrap_or(false)),
                max_tokens: req.max_completion_tokens.or(req.max_tokens).map(i64::from),
                temperature: req.temperature,
                top_p: req.top_p,
                n: req.n.map(|n| i32::try_from(n).unwrap_or(i32::MAX)),
                tool_count: Some(req.tools.as_ref().map(|t| t.len()).unwrap_or(0) as i32),
                message_count: Some(req.messages.len() as i32),
            },
            AiRequest::Completions(req) => Self {
                stream: Some(req.stream.unwrap_or(false)),
                max_tokens: req.max_tokens.map(i64::from),
                temperature: req.temperature,
                top_p: req.top_p,
                n: req.n.map(|n| i32::try_from(n).unwrap_or(i32::MAX)),
                tool_count: None,
                message_count: None,
            },
            AiRequest::Embeddings(_) | AiRequest::Other(_) => Self::default(),
        }
    }
}

#[instrument(skip_all, name = "dwctl.parse_ai_request")]
pub fn parse_ai_request(request_data: &RequestData) -> Result<ParsedAIRequest, SerializationError> {
    let headers = request_data
        .headers
        .iter()
        .map(|(k, v)| (k.clone(), v.iter().map(|b| String::from_utf8_lossy(b).to_string()).collect()))
        .collect();

    let bytes = match &request_data.body {
        Some(body) => body.as_ref(),
        None => {
            return Ok(ParsedAIRequest {
                headers,
                request: AiRequest::Other(Value::Null),
                responses_request: None,
                messages_request: None,
            });
        }
    };

    let body_str = String::from_utf8_lossy(bytes);

    if body_str.trim().is_empty() {
        return Ok(ParsedAIRequest {
            headers,
            request: AiRequest::Other(Value::Null),
            responses_request: None,
            messages_request: None,
        });
    }

    let is_responses_path = request_data.uri.path().ends_with("/responses");
    if is_responses_path {
        return match serde_json::from_str::<Value>(&body_str) {
            Ok(value) => {
                let model = value.get("model").and_then(|v| v.as_str()).map(|s| s.to_string());
                let stream = value.get("stream").and_then(|v| v.as_bool());
                Ok(ParsedAIRequest {
                    headers,
                    request: AiRequest::Other(value),
                    responses_request: Some(ResponsesRequest { model, stream }),
                    messages_request: None,
                })
            }
            Err(e) => {
                let base64_encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes);
                Err(SerializationError {
                    fallback_data: format!("base64:{base64_encoded}"),
                    error: Box::new(e),
                })
            }
        };
    }

    if request_data.uri.path().ends_with("/messages") {
        return match serde_json::from_str::<Value>(&body_str) {
            Ok(value) => Ok(ParsedAIRequest {
                headers,
                messages_request: Some(MessagesRequest::from_value(&value)),
                request: AiRequest::Other(value),
                responses_request: None,
            }),
            Err(e) => {
                let base64_encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes);
                Err(SerializationError {
                    fallback_data: format!("base64:{base64_encoded}"),
                    error: Box::new(e),
                })
            }
        };
    }

    let path = request_data.uri.path();
    let known_endpoint = |value: Value| -> Option<AiRequest> {
        if path.ends_with("/chat/completions") {
            Some(serde_json::from_value(value.clone()).map_or(AiRequest::Other(value), AiRequest::ChatCompletions))
        } else if path.ends_with("/embeddings") {
            Some(serde_json::from_value(value.clone()).map_or(AiRequest::Other(value), AiRequest::Embeddings))
        } else if path.ends_with("/completions") {
            Some(serde_json::from_value(value.clone()).map_or(AiRequest::Other(value), AiRequest::Completions))
        } else {
            None
        }
    };

    match serde_json::from_str::<Value>(&body_str) {
        Ok(value) => {
            let request = match known_endpoint(value.clone()) {
                Some(request) => request,
                None => serde_json::from_value(value.clone()).unwrap_or(AiRequest::Other(value)),
            };
            Ok(ParsedAIRequest {
                headers,
                request,
                responses_request: None,
                messages_request: None,
            })
        }
        Err(e) => {
            let base64_encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes);
            Err(SerializationError {
                fallback_data: format!("base64:{base64_encoded}"),
                error: Box::new(e),
            })
        }
    }
}

fn looks_like_sse(body: &str) -> bool {
    body.lines()
        .find(|line| !line.trim().is_empty())
        .is_some_and(|line| line.starts_with("data:") || line.starts_with("event:"))
}

#[instrument(skip_all, name = "dwctl.parse_ai_response")]
pub fn parse_ai_response(request_data: &RequestData, response_data: &ResponseData) -> Result<AiResponse, SerializationError> {
    let bytes = match &response_data.body {
        Some(body) => body.as_ref(),
        None => return Ok(AiResponse::Other(Value::Null)),
    };

    if bytes.is_empty() {
        return Ok(AiResponse::Other(Value::Null));
    }

    let final_bytes = utils::decompress_response_if_needed(bytes, &response_data.headers)?;
    let body_str = String::from_utf8_lossy(&final_bytes);
    if body_str.trim().is_empty() {
        return Ok(AiResponse::Other(Value::Null));
    }

    let sse_body = looks_like_sse(&body_str);

    let result = if request_data.uri.path().ends_with("/messages") {
        let anthropic_stream = request_data
            .body
            .as_ref()
            .and_then(|b| serde_json::from_slice::<Value>(b).ok())
            .and_then(|v| v.get("stream").and_then(Value::as_bool))
            .unwrap_or(false)
            || sse_body;
        if anthropic_stream {
            utils::parse_anthropic_streaming_response(&body_str)
        } else {
            utils::parse_anthropic_non_streaming_response(&body_str).or_else(|_| utils::parse_non_streaming_response(&body_str))
        }
    } else {
        match parse_ai_request(request_data) {
            Ok(parsed_request) => {
                if let Some(responses_req) = &parsed_request.responses_request {
                    if responses_req.stream.unwrap_or(false) || sse_body {
                        utils::parse_responses_streaming_response(&body_str)
                    } else {
                        utils::parse_responses_non_streaming_response(&body_str).or_else(|_| utils::parse_non_streaming_response(&body_str))
                    }
                } else {
                    let is_streaming = sse_body
                        || match &parsed_request.request {
                            AiRequest::ChatCompletions(req) => req.stream.unwrap_or(false),
                            AiRequest::Completions(req) => req.stream.unwrap_or(false),
                            AiRequest::Other(value) => value.get("stream").and_then(Value::as_bool).unwrap_or(false),
                            AiRequest::Embeddings(_) => false,
                        };
                    match parsed_request.request {
                        AiRequest::ChatCompletions(_) if is_streaming => utils::parse_streaming_response(&body_str),
                        AiRequest::Completions(_) if is_streaming => utils::parse_completions_streaming_response(&body_str),
                        AiRequest::Other(_) if is_streaming => {
                            utils::parse_streaming_response(&body_str).or_else(|_| utils::parse_completions_streaming_response(&body_str))
                        }
                        _ => utils::parse_non_streaming_response(&body_str),
                    }
                }
            }
            _ => utils::parse_non_streaming_response(&body_str),
        }
    };

    result.map_err(|_| SerializationError {
        fallback_data: format!(
            "base64:{}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &final_bytes)
        ),
        error: "Failed to parse response as JSON or SSE".into(),
    })
}

impl UsageMetrics {
    #[instrument(skip_all, name = "dwctl.extract_usage_metrics")]
    pub fn extract(
        instance_id: Uuid,
        request_data: &RequestData,
        response_data: &ResponseData,
        parsed_response: &AiResponse,
        config: &Config,
    ) -> Self {
        let parsed_request = parse_ai_request(request_data).ok();
        let request_params = parsed_request.as_ref().map(RequestParams::from_parsed).unwrap_or_default();
        let request_model = match parsed_request {
            Some(parsed_request) => {
                if let Some(responses_req) = parsed_request.responses_request {
                    responses_req.model
                } else if let Some(messages_req) = parsed_request.messages_request {
                    messages_req.model
                } else {
                    match parsed_request.request {
                        AiRequest::ChatCompletions(req) => Some(req.model),
                        AiRequest::Completions(req) => Some(req.model),
                        AiRequest::Embeddings(req) => Some(req.model),
                        AiRequest::Other(ref value) => {
                            let model = value.get("model").and_then(|v| v.as_str()).map(String::from);
                            if model.is_some() {
                                error!(
                                    uri = %request_data.uri,
                                    "Request body has a model field but failed typed deserialization — \
                                     likely uses unsupported content types"
                                );
                            }
                            model
                        }
                    }
                }
            }
            None => None,
        };

        let mut metrics = TokenMetrics::from(parsed_response);

        if metrics.prompt_tokens == 0
            && metrics.completion_tokens == 0
            && metrics.total_tokens == 0
            && let Some(raw) = extract_from_last_usage(response_data, raw_usage_tokens)
        {
            metrics.prompt_tokens = raw.prompt;
            metrics.completion_tokens = raw.completion;
            metrics.reasoning_tokens = raw.reasoning;
            metrics.total_tokens = raw.total;
        }

        let cache_tokens = extract_cache_tokens(response_data);
        let engine_cached_tokens = response_data
            .extensions
            .get::<crate::prompt_cache::UpstreamCachedTokens>()
            .and_then(|c| c.get())
            .map(|v| i64::try_from(v).unwrap_or(i64::MAX))
            .or_else(|| extract_engine_cached_tokens(response_data));

        let upstream_status = response_data.status.as_u16() as i32;
        let status_code = if upstream_status < 400 && ai_response_stream_errored(parsed_response) {
            500
        } else {
            upstream_status
        };

        Self {
            instance_id,
            correlation_id: request_data.correlation_id as i64,
            timestamp: chrono::DateTime::<chrono::Utc>::from(request_data.timestamp),
            method: request_data.method.to_string(),
            uri: request_data.uri.to_string(),
            request_model,
            response_model: metrics.response_model,
            status_code,
            duration_ms: response_data.duration.as_millis() as i64,
            duration_to_first_byte_ms: Some(response_data.duration_to_first_byte.as_millis() as i64),
            prompt_tokens: metrics.prompt_tokens,
            completion_tokens: metrics.completion_tokens,
            reasoning_tokens: metrics.reasoning_tokens,
            total_tokens: metrics.total_tokens,
            cache_read_input_tokens: cache_tokens.read,
            cache_creation_5m_input_tokens: cache_tokens.creation_5m,
            cache_creation_1h_input_tokens: cache_tokens.creation_1h,
            cache_creation_24h_input_tokens: cache_tokens.creation_24h,
            response_type: metrics.response_type,
            finish_reason: extract_finish_reason(parsed_response)
                .or_else(|| client_disconnected(response_data).then(|| CLIENT_DISCONNECTED.to_string())),
            server_address: config.host.clone(),
            server_port: config.port,
            served_by: response_data.extensions.get::<onwards::ServedBy>().map(|s| s.url.clone()),
            served_model: response_data
                .extensions
                .get::<onwards::ServedBy>()
                .and_then(|s| s.onwards_model.clone()),
            engine_cached_tokens,
            cache_read_source: extract_cache_read_source(response_data),
            request_params,
            requested_serving_class: response_data
                .extensions
                .get::<onwards::ServingClassOutcome>()
                .and_then(|o| o.requested)
                .map(|c| c.as_str().to_string()),
            resolved_serving_class: response_data
                .extensions
                .get::<onwards::ServingClassOutcome>()
                .map(|o| o.resolved.as_str().to_string()),
        }
    }
}

fn ai_response_stream_errored(response: &AiResponse) -> bool {
    match response {
        AiResponse::ChatCompletionsStream(chunks) => chunks.iter().any(|c| matches!(c, ChatCompletionChunk::Error(_))),
        AiResponse::CompletionsStream(chunks) => chunks.iter().any(|c| matches!(c, CompletionChunk::Error(_))),
        AiResponse::AnthropicStream(events) => events.iter().any(|e| e.get("type").and_then(|v| v.as_str()) == Some("error")),
        AiResponse::ResponsesStream(events) => events.iter().any(|e| matches!(e.event_type.as_str(), "response.failed" | "error")),
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CacheTokens {
    pub read: i64,
    pub creation_5m: i64,
    pub creation_1h: i64,
    pub creation_24h: i64,
}

fn cache_tokens_from_usage(usage: &Value) -> CacheTokens {
    let read = usage.get("cache_read_input_tokens").and_then(Value::as_i64).unwrap_or(0).max(0);
    let tier = |k: &str| {
        usage
            .get("cache_creation")
            .and_then(|c| c.get(k))
            .and_then(Value::as_i64)
            .unwrap_or(0)
            .max(0)
    };
    CacheTokens {
        read,
        creation_5m: tier("ephemeral_5m_input_tokens"),
        creation_1h: tier("ephemeral_1h_input_tokens"),
        creation_24h: tier("ephemeral_24h_input_tokens"),
    }
}

pub(crate) fn extract_cache_tokens(response_data: &ResponseData) -> CacheTokens {
    if let Some(capture) = response_data.extensions.get::<CacheBilling>() {
        let stats = capture.get().unwrap_or_default().stats;
        let count = |value| i64::try_from(value).unwrap_or(i64::MAX);
        return CacheTokens {
            read: count(stats.read),
            creation_5m: count(stats.creation_5m),
            creation_1h: count(stats.creation_1h),
            creation_24h: count(stats.creation_24h),
        };
    }
    extract_from_last_usage(response_data, cache_tokens_from_usage)
}

pub(crate) fn extract_cache_read_source(response_data: &ResponseData) -> Option<String> {
    response_data
        .extensions
        .get::<CacheBilling>()
        .and_then(|capture| capture.get())
        .and_then(|billed| billed.read_source)
        .map(|source| source.as_str().to_string())
}

fn engine_cached_tokens_from_usage(usage: &Value) -> Option<i64> {
    usage
        .pointer("/prompt_tokens_details/cached_tokens")
        .and_then(Value::as_i64)
        .map(|v| v.max(0))
}

pub(crate) fn extract_engine_cached_tokens(response_data: &ResponseData) -> Option<i64> {
    extract_from_last_usage(response_data, engine_cached_tokens_from_usage)
}

pub(crate) struct RawUsageTokens {
    pub prompt: i64,
    pub completion: i64,
    pub reasoning: i64,
    pub total: i64,
}

pub(crate) fn raw_usage_tokens(usage: &Value) -> Option<RawUsageTokens> {
    let get = |k: &str| usage.get(k).and_then(Value::as_i64);
    let prompt = match get("prompt_tokens") {
        Some(p) => p,
        None if usage.get("input_tokens_details").is_some() => get("input_tokens")?,
        None => {
            get("input_tokens")?
                + get("cache_read_input_tokens").unwrap_or(0).max(0)
                + get("cache_creation_input_tokens").unwrap_or(0).max(0)
        }
    };
    let completion = get("completion_tokens").or_else(|| get("output_tokens")).unwrap_or(0);
    let reasoning = usage
        .pointer("/completion_tokens_details/reasoning_tokens")
        .or_else(|| usage.pointer("/output_tokens_details/reasoning_tokens"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let prompt = prompt.max(0);
    let completion = completion.max(0);
    let total = get("total_tokens").unwrap_or(prompt + completion).max(0);
    Some(RawUsageTokens {
        prompt,
        completion,
        reasoning: reasoning.max(0),
        total,
    })
}

pub(crate) fn extract_from_last_usage<T: Default>(response_data: &ResponseData, from_usage: impl Fn(&Value) -> T) -> T {
    let Some(body) = &response_data.body else {
        return T::default();
    };
    let bytes = match utils::decompress_response_if_needed(body.as_ref(), &response_data.headers) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "cache token extraction: response decompress failed, falling back to raw body");
            body.as_ref().to_vec()
        }
    };
    let body_str = String::from_utf8_lossy(&bytes);

    if let Ok(value) = serde_json::from_str::<Value>(body_str.trim())
        && let Some(usage) = value.get("usage").filter(|u| u.is_object())
    {
        return from_usage(usage);
    }

    let mut last = T::default();
    for line in body_str.lines() {
        if let Some(data) = line.strip_prefix("data:") {
            let data = data.strip_prefix(' ').unwrap_or(data);
            let trimmed = data.trim();
            if trimmed != "[DONE]"
                && let Ok(value) = serde_json::from_str::<Value>(trimmed)
                && let Some(usage) = value
                    .get("usage")
                    .or_else(|| match value.get("type").and_then(Value::as_str) {
                        Some("response.completed" | "response.incomplete" | "response.failed") => value.pointer("/response/usage"),
                        _ => None,
                    })
                    .filter(|u| u.is_object())
            {
                last = from_usage(usage);
            }
        }
    }
    last
}

impl Auth {
    #[instrument(skip_all, name = "dwctl.extract_auth")]
    pub fn from_request(request_data: &RequestData, _config: &Config) -> Self {
        if let Some(auth_header) = Self::get_header_value(request_data, "authorization")
            && let Some(bearer_token) = auth_header.strip_prefix("Bearer ")
        {
            return Auth::ApiKey {
                bearer_token: bearer_token.to_string(),
            };
        }

        Auth::None
    }

    fn get_header_value(request_data: &RequestData, header_name: &str) -> Option<String> {
        request_data
            .headers
            .get(header_name)
            .and_then(|values| values.first())
            .and_then(|bytes| str::from_utf8(bytes).ok())
            .map(|s| s.to_string())
    }
}

fn extract_finish_reason(response: &AiResponse) -> Option<String> {
    fn as_wire<T: serde::Serialize>(v: &T) -> Option<String> {
        match serde_json::to_value(v).ok()? {
            serde_json::Value::String(s) => Some(s),
            _ => None,
        }
    }
    match response {
        AiResponse::ChatCompletions(r) => r.choices.first()?.finish_reason.as_ref().and_then(as_wire),
        AiResponse::ChatCompletionsStream(chunks) => chunks.iter().rev().find_map(|c| match c {
            ChatCompletionChunk::Chunk(n) => n.choices.first()?.finish_reason.as_ref().and_then(as_wire),
            _ => None,
        }),
        AiResponse::Completions(r) => r.choices.first()?.finish_reason.as_ref().and_then(as_wire),
        AiResponse::CompletionsStream(chunks) => chunks.iter().rev().find_map(|c| match c {
            CompletionChunk::Chunk(n) => n.choices.first()?.finish_reason.as_ref().and_then(as_wire),
            _ => None,
        }),
        AiResponse::Responses(_) | AiResponse::ResponsesStream(_) => None,
        AiResponse::Anthropic(_) | AiResponse::AnthropicStream(_) => None,
        AiResponse::Embeddings(_) | AiResponse::Other(_) => None,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TokenMetrics {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub reasoning_tokens: i64,
    pub total_tokens: i64,
    pub response_type: String,
    pub response_model: Option<String>,
}

fn extract_completion_reasoning_tokens(usage: &onwards::strict::schemas::chat_completions::Usage) -> i64 {
    usage
        .completion_tokens_details
        .as_ref()
        .and_then(|d| d.get("reasoning_tokens"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0)
}

fn token_metrics_from_responses_usage(
    usage: Option<&crate::inference::translation::responses::types::ResponseUsage>,
    response_model: Option<String>,
    response_type: &str,
) -> TokenMetrics {
    match usage {
        Some(usage) => {
            let prompt_tokens = usage.input_tokens as i64;
            let completion_tokens = usage.output_tokens as i64;
            TokenMetrics {
                prompt_tokens,
                completion_tokens,
                reasoning_tokens: usage.output_tokens_details.reasoning_tokens as i64,
                total_tokens: if usage.total_tokens == 0 {
                    prompt_tokens + completion_tokens
                } else {
                    usage.total_tokens as i64
                },
                response_type: response_type.to_string(),
                response_model,
            }
        }
        None => TokenMetrics {
            prompt_tokens: 0,
            completion_tokens: 0,
            reasoning_tokens: 0,
            total_tokens: 0,
            response_type: response_type.to_string(),
            response_model,
        },
    }
}

impl From<&AiResponse> for TokenMetrics {
    fn from(response: &AiResponse) -> Self {
        match response {
            AiResponse::ChatCompletions(response) => {
                if let Some(usage) = &response.usage {
                    Self {
                        prompt_tokens: usage.prompt_tokens as i64,
                        completion_tokens: usage.completion_tokens as i64,
                        reasoning_tokens: extract_completion_reasoning_tokens(usage),
                        total_tokens: usage.total_tokens as i64,
                        response_type: "chat_completion".to_string(),
                        response_model: Some(response.model.clone()),
                    }
                } else {
                    Self {
                        prompt_tokens: 0,
                        completion_tokens: 0,
                        reasoning_tokens: 0,
                        total_tokens: 0,
                        response_type: "chat_completion".to_string(),
                        response_model: Some(response.model.clone()),
                    }
                }
            }
            AiResponse::ChatCompletionsStream(chunks) => {
                let last_normal_with_usage = chunks.iter().rev().find_map(|chunk| match chunk {
                    ChatCompletionChunk::Chunk(normal_chunk) if normal_chunk.usage.is_some() => Some(normal_chunk),
                    _ => None,
                });

                let model = chunks.iter().find_map(|chunk| match chunk {
                    ChatCompletionChunk::Chunk(c) => Some(c.model.clone()),
                    _ => None,
                });

                if let Some(chunk) = last_normal_with_usage {
                    if let Some(usage) = &chunk.usage {
                        Self {
                            prompt_tokens: usage.prompt_tokens as i64,
                            completion_tokens: usage.completion_tokens as i64,
                            reasoning_tokens: extract_completion_reasoning_tokens(usage),
                            total_tokens: usage.total_tokens as i64,
                            response_type: "chat_completion_stream".to_string(),
                            response_model: model,
                        }
                    } else {
                        Self {
                            prompt_tokens: 0,
                            completion_tokens: 0,
                            reasoning_tokens: 0,
                            total_tokens: 0,
                            response_type: "chat_completion_stream".to_string(),
                            response_model: model,
                        }
                    }
                } else {
                    Self {
                        prompt_tokens: 0,
                        completion_tokens: 0,
                        reasoning_tokens: 0,
                        total_tokens: 0,
                        response_type: "chat_completion_stream".to_string(),
                        response_model: model,
                    }
                }
            }
            AiResponse::CompletionsStream(chunks) => {
                let last_normal_with_usage = chunks.iter().rev().find_map(|chunk| match chunk {
                    CompletionChunk::Chunk(normal_chunk) if normal_chunk.usage.is_some() => Some(normal_chunk),
                    _ => None,
                });

                let model = chunks.iter().find_map(|chunk| match chunk {
                    CompletionChunk::Chunk(c) => Some(c.model.clone()),
                    _ => None,
                });

                if let Some(chunk) = last_normal_with_usage {
                    if let Some(usage) = &chunk.usage {
                        Self {
                            prompt_tokens: usage.prompt_tokens as i64,
                            completion_tokens: usage.completion_tokens as i64,
                            reasoning_tokens: extract_completion_reasoning_tokens(usage),
                            total_tokens: usage.total_tokens as i64,
                            response_type: "completion_stream".to_string(),
                            response_model: model,
                        }
                    } else {
                        Self {
                            prompt_tokens: 0,
                            completion_tokens: 0,
                            reasoning_tokens: 0,
                            total_tokens: 0,
                            response_type: "completion_stream".to_string(),
                            response_model: model,
                        }
                    }
                } else {
                    Self {
                        prompt_tokens: 0,
                        completion_tokens: 0,
                        reasoning_tokens: 0,
                        total_tokens: 0,
                        response_type: "completion_stream".to_string(),
                        response_model: model,
                    }
                }
            }
            AiResponse::Completions(response) => {
                if let Some(usage) = &response.usage {
                    Self {
                        prompt_tokens: usage.prompt_tokens as i64,
                        completion_tokens: usage.completion_tokens as i64,
                        reasoning_tokens: extract_completion_reasoning_tokens(usage),
                        total_tokens: usage.total_tokens as i64,
                        response_type: "completion".to_string(),
                        response_model: Some(response.model.clone()),
                    }
                } else {
                    Self {
                        prompt_tokens: 0,
                        completion_tokens: 0,
                        reasoning_tokens: 0,
                        total_tokens: 0,
                        response_type: "completion".to_string(),
                        response_model: Some(response.model.clone()),
                    }
                }
            }
            AiResponse::Embeddings(response) => {
                let usage = &response.usage;
                let base64 = response
                    .data
                    .first()
                    .is_some_and(|d| matches!(d.embedding, onwards::strict::schemas::embeddings::Embedding::Base64(_)));
                Self {
                    prompt_tokens: usage.prompt_tokens as i64,
                    completion_tokens: 0,
                    reasoning_tokens: 0,
                    total_tokens: usage.total_tokens as i64,
                    response_type: if base64 { "base64_embeddings" } else { "embeddings" }.to_string(),
                    response_model: Some(response.model.clone()),
                }
            }
            AiResponse::Responses(response) => {
                token_metrics_from_responses_usage(response.usage.as_ref(), Some(response.model.clone()), "response")
            }
            AiResponse::ResponsesStream(events) => {
                let snapshot = events.iter().rev().find_map(|e| e.response.as_ref().filter(|r| r.usage.is_some()));
                let model = events.iter().rev().find_map(|e| e.response.as_ref().map(|r| r.model.clone()));
                token_metrics_from_responses_usage(snapshot.and_then(|r| r.usage.as_ref()), model, "response_stream")
            }
            AiResponse::Anthropic(response) => {
                let usage = &response.usage;
                let prompt_tokens = (usage.input_tokens
                    + usage.cache_read_input_tokens.unwrap_or(0)
                    + usage.cache_creation_input_tokens.unwrap_or(0)) as i64;
                let completion_tokens = usage.output_tokens as i64;
                Self {
                    prompt_tokens,
                    completion_tokens,
                    reasoning_tokens: 0,
                    total_tokens: prompt_tokens + completion_tokens,
                    response_type: "anthropic_message".to_string(),
                    response_model: Some(response.model.clone()),
                }
            }
            AiResponse::AnthropicStream(events) => {
                let mut input = 0i64;
                let mut cached = 0i64;
                let mut output = 0i64;
                let mut model = None;
                let cache_total = |usage: &Value| -> Option<i64> {
                    let read = usage.get("cache_read_input_tokens").and_then(Value::as_i64);
                    let creation = usage.get("cache_creation_input_tokens").and_then(Value::as_i64);
                    (read.is_some() || creation.is_some()).then(|| read.unwrap_or(0).max(0) + creation.unwrap_or(0).max(0))
                };
                for event in events {
                    match event.get("type").and_then(|v| v.as_str()).unwrap_or_default() {
                        "message_start" => {
                            if let Some(m) = event.pointer("/message/model").and_then(|v| v.as_str()) {
                                model = Some(m.to_string());
                            }
                            if let Some(i) = event
                                .pointer("/message/usage/input_tokens")
                                .and_then(Value::as_i64)
                                .filter(|i| *i > 0)
                            {
                                input = i;
                            }
                            if let Some(c) = event.pointer("/message/usage").and_then(cache_total) {
                                cached = c;
                            }
                        }
                        "message_delta" => {
                            if let Some(usage) = event.get("usage").filter(|u| !u.is_null()) {
                                if let Some(i) = usage.get("input_tokens").and_then(Value::as_i64).filter(|i| *i > 0) {
                                    input = i;
                                }
                                if let Some(c) = cache_total(usage) {
                                    cached = c;
                                }
                                output = usage.get("output_tokens").and_then(Value::as_i64).unwrap_or(output);
                            }
                        }
                        _ => {}
                    }
                }
                let input = input + cached;
                Self {
                    prompt_tokens: input,
                    completion_tokens: output,
                    reasoning_tokens: 0,
                    total_tokens: input + output,
                    response_type: "anthropic_message_stream".to_string(),
                    response_model: model,
                }
            }
            AiResponse::Other(_) => Self {
                prompt_tokens: 0,
                completion_tokens: 0,
                reasoning_tokens: 0,
                total_tokens: 0,
                response_type: "other".to_string(),
                response_model: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RequestParams, UsageMetrics, extract_cache_read_source, extract_cache_tokens, extract_engine_cached_tokens, extract_finish_reason,
        parse_ai_request, parse_ai_response,
    };
    use crate::prompt_cache::{BilledCache, CacheBilling, CacheReadSource, CacheStats};
    use crate::request_logging::models::{AiRequest, AiResponse};
    use crate::request_logging::stream_completion::{CLIENT_DISCONNECTED, StreamCompletion};
    use axum::http::{Method, StatusCode, Uri};
    use bytes::Bytes;
    use onwards::strict::schemas::chat_completions::{ChatCompletionChunk as ChatChunk, ChatCompletionResponse, Usage as ChatUsage};
    use onwards::strict::schemas::completions::CompletionResponse;
    use onwards::strict::schemas::embeddings::{Embedding, EmbeddingData, EmbeddingsResponse, EmbeddingsUsage};
    use outlet::{RequestData, ResponseData};
    use serde_json::Value;
    use std::{
        collections::HashMap,
        time::{Duration, SystemTime},
    };
    use uuid::Uuid;

    #[test]
    fn test_parse_ai_request_no_body() {
        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: None,
            trace_id: None,
            span_id: None,
        };

        let result = parse_ai_request(&request_data).unwrap();

        match result.request {
            AiRequest::Other(value) => assert!(value.is_null()),
            _ => panic!("Expected AiRequest::Other(null)"),
        }
    }

    #[test]
    fn test_parse_ai_request_empty_bytes() {
        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::new()),
            trace_id: None,
            span_id: None,
        };

        let result = parse_ai_request(&request_data).unwrap();

        match result.request {
            AiRequest::Other(value) => assert!(value.is_null()),
            _ => panic!("Expected AiRequest::Other(null)"),
        }
    }

    #[test]
    fn test_parse_ai_request_invalid_json() {
        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from("invalid json")),
            trace_id: None,
            span_id: None,
        };

        let result = parse_ai_request(&request_data);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.fallback_data.starts_with("base64:"));
    }

    #[test]
    fn test_parse_ai_request_valid_json() {
        let json_body = r#"{"model": "gpt-4", "messages": [{"role": "user", "content": "hello"}]}"#;
        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(json_body)),
            trace_id: None,
            span_id: None,
        };

        let result = parse_ai_request(&request_data).unwrap();

        match result.request {
            AiRequest::ChatCompletions(req) => {
                assert_eq!(req.model, "gpt-4");
                assert_eq!(req.messages.len(), 1);
            }
            _ => panic!("Expected AiRequest::ChatCompletions"),
        }
    }

    #[test]
    fn test_parse_ai_request_completions() {
        let json_body = r#"{"model": "gpt-3.5-turbo-instruct", "prompt": "Say hello"}"#;
        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(json_body)),
            trace_id: None,
            span_id: None,
        };

        let result = parse_ai_request(&request_data).unwrap();

        match result.request {
            AiRequest::Completions(req) => {
                assert_eq!(req.model, "gpt-3.5-turbo-instruct");
            }
            _ => panic!("Expected AiRequest::Completions"),
        }
    }

    #[test]
    fn test_parse_ai_request_embeddings() {
        let json_body = r#"{"model": "text-embedding-ada-002", "input": "hello world"}"#;
        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(json_body)),
            trace_id: None,
            span_id: None,
        };

        let result = parse_ai_request(&request_data).unwrap();

        match result.request {
            AiRequest::Embeddings(req) => {
                assert_eq!(req.model, "text-embedding-ada-002");
            }
            _ => panic!("Expected AiRequest::Embeddings"),
        }
    }

    #[test]
    fn test_parse_ai_response_no_body() {
        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: None,
            trace_id: None,
            span_id: None,
        };

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 123,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: None,
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let result = parse_ai_response(&request_data, &response_data).unwrap();

        match result {
            AiResponse::Other(value) => assert!(value.is_null()),
            _ => panic!("Expected AiResponse::Other(null)"),
        }
    }

    #[test]
    fn test_parse_ai_response_empty_body() {
        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: None,
            trace_id: None,
            span_id: None,
        };

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 123,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: Some(Bytes::new()),
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let result = parse_ai_response(&request_data, &response_data).unwrap();

        match result {
            AiResponse::Other(value) => assert!(value.is_null()),
            _ => panic!("Expected AiResponse::Other(null)"),
        }
    }

    #[test]
    fn test_parse_ai_response_valid_json() {
        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: None,
            trace_id: None,
            span_id: None,
        };

        let json_response = r#"{
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1677652288,
            "model": "gpt-4",
            "choices": [],
            "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}
        }"#;

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 123,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: Some(Bytes::from(json_response)),
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let result = parse_ai_response(&request_data, &response_data).unwrap();

        match result {
            AiResponse::ChatCompletions(response) => {
                assert_eq!(response.model, "gpt-4");
                assert_eq!(response.id, "chatcmpl-123");
            }
            _ => panic!("Expected AiResponse::ChatCompletions"),
        }
    }

    #[test]
    fn test_parse_ai_response_streaming() {
        let request_json = r#"{"model": "gpt-4", "messages": [{"role": "user", "content": "hello"}], "stream": true}"#;
        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(request_json)),
            trace_id: None,
            span_id: None,
        };

        let sse_response = "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"created\":1677652288,\"model\":\"gpt-4\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\ndata: [DONE]\n\n";

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 123,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: Some(Bytes::from(sse_response)),
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let result = parse_ai_response(&request_data, &response_data).unwrap();

        match result {
            AiResponse::ChatCompletionsStream(chunks) => {
                assert!(!chunks.is_empty());
            }
            _ => panic!("Expected AiResponse::ChatCompletionsStream"),
        }
    }

    #[test]
    fn test_parse_ai_response_detects_stream_from_the_body() {
        let request_json = r#"{"model": "gpt-4", "messages": [{"role": "user", "content": "hello"}], "stream": false}"#;
        let headers = HashMap::new();
        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers,
            body: Some(Bytes::from(request_json)),
            trace_id: None,
            span_id: None,
        };

        let sse_response = "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"created\":1677652288,\"model\":\"gpt-4\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}],\"usage\":null}\n\ndata: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"created\":1677652288,\"model\":\"gpt-4\",\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\ndata: [DONE]\n\n";

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 123,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: Some(Bytes::from(sse_response)),
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let result = parse_ai_response(&request_data, &response_data).unwrap();

        match &result {
            AiResponse::ChatCompletionsStream(chunks) => {
                assert!(!chunks.is_empty(), "Expected parsed SSE chunks");
                let metrics = UsageMetrics::extract(
                    uuid::Uuid::nil(),
                    &request_data,
                    &response_data,
                    &result,
                    &crate::config::Config::default(),
                );
                assert_eq!(metrics.prompt_tokens, 10);
                assert_eq!(metrics.completion_tokens, 5);
                assert_eq!(metrics.total_tokens, 15);
            }
            other => panic!("Expected ChatCompletionsStream, got {:?}", std::mem::discriminant(other)),
        }
    }

    #[test]
    fn unrepresentable_finish_reason_still_bills_from_the_raw_usage() {
        let request_data = RequestData {
            correlation_id: 1,
            timestamp: SystemTime::now(),
            method: axum::http::Method::POST,
            uri: "/chat/completions".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#)),
            trace_id: None,
            span_id: None,
        };
        let body = r#"{"id":"resp_1","object":"chat.completion","created":1786498035,"model":"m","choices":[{"finish_reason":"error","index":0,"message":{"role":"assistant","refusal":null,"reasoning_details":[{"text":"...","type":"reasoning.text","index":0,"format":"unknown"}]}}],"usage":{"prompt_tokens":29366,"completion_tokens":97,"total_tokens":29463,"completion_tokens_details":{"reasoning_tokens":97},"cache_read_input_tokens":25351,"cache_creation_input_tokens":4015,"cache_creation":{"ephemeral_5m_input_tokens":4015,"ephemeral_1h_input_tokens":0,"ephemeral_24h_input_tokens":0}}}"#;
        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 1,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: Some(Bytes::from(body)),
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let parsed = parse_ai_response(&request_data, &response_data).unwrap();
        assert!(
            matches!(parsed, AiResponse::ChatCompletions(_)),
            "onwards represents an unrepresentable-by-enum finish_reason"
        );

        let metrics = UsageMetrics::extract(
            uuid::Uuid::nil(),
            &request_data,
            &response_data,
            &parsed,
            &crate::config::Config::default(),
        );
        assert_eq!(metrics.prompt_tokens, 29_366, "recovered from the raw usage object");
        assert_eq!(metrics.completion_tokens, 97);
        assert_eq!(metrics.reasoning_tokens, 97);
        assert_eq!(metrics.total_tokens, 29_463);
        assert_eq!(metrics.cache_read_input_tokens, 25_351, "split read from the raw body as before");
        assert_eq!(metrics.cache_creation_5m_input_tokens, 4_015);
    }

    #[test]
    fn null_usage_still_records_zero_on_the_live_path() {
        let request_data = RequestData {
            correlation_id: 1,
            timestamp: SystemTime::now(),
            method: axum::http::Method::POST,
            uri: "/chat/completions".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#)),
            trace_id: None,
            span_id: None,
        };
        let body = r#"{"id":"c","object":"chat.completion","created":1,"model":"m","choices":[{"finish_reason":"tool_calls","index":0,"message":{"role":"assistant","content":null}}],"usage":null}"#;
        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 1,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: Some(Bytes::from(body)),
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let parsed = parse_ai_response(&request_data, &response_data).unwrap();
        let metrics = UsageMetrics::extract(
            uuid::Uuid::nil(),
            &request_data,
            &response_data,
            &parsed,
            &crate::config::Config::default(),
        );
        assert_eq!(metrics.prompt_tokens, 0, "no usage anywhere → nothing invented");
        assert_eq!(metrics.completion_tokens, 0);
        assert_eq!(metrics.total_tokens, 0);
    }

    #[test]
    fn test_streamed_body_with_embedded_error_frame_reclassifies_to_500() {
        let request_json = r#"{"model": "moonshotai/Kimi-K2.6", "messages": [{"role": "user", "content": "hi"}], "stream": false}"#;
        let headers = HashMap::new();
        let request_data = RequestData {
            correlation_id: 999,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/v1/chat/completions".parse::<Uri>().unwrap(),
            headers,
            body: Some(Bytes::from(request_json)),
            trace_id: None,
            span_id: None,
        };

        let sse_response = concat!(
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"moonshotai/Kimi-K2.6\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"}}]}\n\n",
            "data: {\"error\":{\"message\":\"Engine was shut down during token generation\",\"type\":\"internal_server_error\",\"code\":500}}\n\n",
        );

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 999,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: Some(Bytes::from(sse_response)),
            duration: Duration::from_millis(335_000),
            duration_to_first_byte: Duration::from_millis(2_300),
        };

        let parsed = parse_ai_response(&request_data, &response_data).unwrap();
        let metrics = UsageMetrics::extract(
            uuid::Uuid::nil(),
            &request_data,
            &response_data,
            &parsed,
            &crate::config::Config::default(),
        );

        assert_eq!(
            metrics.status_code, 500,
            "200 OK with embedded SSE error frame must be reclassified to 500 so success-rate \
             metrics and credit-eligibility checks exclude this row, and so the analytics row \
             agrees with fusillade's HTTP-layer reclassification"
        );
        assert_eq!(metrics.total_tokens, 0);
        assert_eq!(metrics.response_type, "chat_completion_stream");
    }

    #[test]
    fn test_served_by_extension_flows_into_usage_metrics() {
        let request_json = r#"{"model": "zai-org/GLM-5.2-FP8", "messages": [{"role": "user", "content": "hi"}]}"#;
        let request_data = RequestData {
            correlation_id: 42,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/v1/chat/completions".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(request_json)),
            trace_id: None,
            span_id: None,
        };

        let mut extensions = axum::http::Extensions::new();
        extensions.insert(onwards::ServedBy {
            url: "https://router.requesty.ai/v1".to_string(),
            onwards_model: Some("policy/glm-5.2".to_string()),
        });
        let response_data = ResponseData {
            extensions,
            correlation_id: 42,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: None,
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let parsed = parse_ai_response(&request_data, &response_data).unwrap();
        let metrics = UsageMetrics::extract(
            uuid::Uuid::nil(),
            &request_data,
            &response_data,
            &parsed,
            &crate::config::Config::default(),
        );
        assert_eq!(metrics.served_by.as_deref(), Some("https://router.requesty.ai/v1"));
        assert_eq!(metrics.served_model.as_deref(), Some("policy/glm-5.2"));

        let response_data_no_ext = ResponseData {
            extensions: Default::default(),
            correlation_id: 42,
            timestamp: SystemTime::now(),
            status: StatusCode::BAD_GATEWAY,
            headers: HashMap::new(),
            body: None,
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        };
        let parsed = parse_ai_response(&request_data, &response_data_no_ext).unwrap();
        let metrics = UsageMetrics::extract(
            uuid::Uuid::nil(),
            &request_data,
            &response_data_no_ext,
            &parsed,
            &crate::config::Config::default(),
        );
        assert_eq!(metrics.served_by, None);
        assert_eq!(metrics.served_model, None);
    }

    #[test]
    fn test_streamed_body_with_real_error_status_is_preserved() {
        let request_json = r#"{"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}], "stream": false}"#;
        let headers = HashMap::new();
        let request_data = RequestData {
            correlation_id: 7,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/v1/chat/completions".parse::<Uri>().unwrap(),
            headers,
            body: Some(Bytes::from(request_json)),
            trace_id: None,
            span_id: None,
        };
        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 7,
            timestamp: SystemTime::now(),
            status: StatusCode::TOO_MANY_REQUESTS,
            headers: HashMap::new(),
            body: Some(Bytes::from(r#"{"error":{"message":"rate limit","type":"rate_limit"}}"#)),
            duration: Duration::from_millis(50),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let parsed = parse_ai_response(&request_data, &response_data).unwrap();
        let metrics = UsageMetrics::extract(
            uuid::Uuid::nil(),
            &request_data,
            &response_data,
            &parsed,
            &crate::config::Config::default(),
        );

        assert_eq!(metrics.status_code, 429);
    }

    #[test]
    fn test_parse_ai_response_fusillade_completions_stream() {
        let request_json = r#"{"model": "gpt-3.5-turbo-instruct", "prompt": "Hello", "stream": false}"#;
        let headers = HashMap::new();
        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers,
            body: Some(Bytes::from(request_json)),
            trace_id: None,
            span_id: None,
        };

        let sse_response = "data: {\"id\":\"cmpl-123\",\"object\":\"text_completion\",\"created\":1677652288,\"model\":\"gpt-3.5-turbo-instruct\",\"choices\":[{\"text\":\" world\",\"index\":0}]}\n\ndata: {\"id\":\"cmpl-123\",\"object\":\"text_completion\",\"created\":1677652288,\"model\":\"gpt-3.5-turbo-instruct\",\"choices\":[],\"usage\":{\"prompt_tokens\":8,\"completion_tokens\":12,\"total_tokens\":20}}\n\ndata: [DONE]\n\n";

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 123,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: Some(Bytes::from(sse_response)),
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let result = parse_ai_response(&request_data, &response_data).unwrap();

        match &result {
            AiResponse::CompletionsStream(chunks) => {
                assert!(!chunks.is_empty(), "Expected parsed SSE chunks");
                let metrics = UsageMetrics::extract(
                    uuid::Uuid::nil(),
                    &request_data,
                    &response_data,
                    &result,
                    &crate::config::Config::default(),
                );
                assert_eq!(metrics.prompt_tokens, 8);
                assert_eq!(metrics.completion_tokens, 12);
                assert_eq!(metrics.total_tokens, 20);
            }
            other => panic!("Expected CompletionsStream, got {:?}", std::mem::discriminant(other)),
        }
    }

    #[test]
    fn test_parse_ai_response_embeddings() {
        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: None,
            trace_id: None,
            span_id: None,
        };

        let embeddings_response = r#"{
            "object": "list",
            "data": [{"object": "embedding", "embedding": [0.1, 0.2], "index": 0}],
            "model": "text-embedding-ada-002",
            "usage": {"prompt_tokens": 5, "total_tokens": 5}
        }"#;

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 123,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: Some(Bytes::from(embeddings_response)),
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let result = parse_ai_response(&request_data, &response_data).unwrap();

        match result {
            AiResponse::Embeddings(response) => {
                assert_eq!(response.model, "text-embedding-ada-002");
                assert_eq!(response.object, "list");
            }
            _ => panic!("Expected AiResponse::Embeddings"),
        }
    }

    #[test]
    fn test_parse_ai_response_invalid_json() {
        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: None,
            trace_id: None,
            span_id: None,
        };

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 123,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: Some(Bytes::from("invalid json response")),
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let result = parse_ai_response(&request_data, &response_data);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.fallback_data.starts_with("base64:"));
    }

    #[test]
    fn test_analytics_metrics_extract_basic() {
        let instance_id = Uuid::new_v4();

        let request_data = RequestData {
            correlation_id: 12345,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/v1/chat/completions".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: None,
            trace_id: None,
            span_id: None,
        };

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 12345,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: None,
            duration: Duration::from_millis(250),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let parsed_response = AiResponse::Other(serde_json::Value::Null);

        let metrics = UsageMetrics::extract(
            instance_id,
            &request_data,
            &response_data,
            &parsed_response,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(metrics.instance_id, instance_id);
        assert_eq!(metrics.correlation_id, 12345);
        assert_eq!(metrics.method, "POST");
        assert_eq!(metrics.uri, "/v1/chat/completions");
        assert_eq!(metrics.request_model, None);
        assert_eq!(metrics.response_model, None);
        assert_eq!(metrics.status_code, 200);
        assert_eq!(metrics.duration_ms, 250);
        assert_eq!(metrics.duration_to_first_byte_ms, Some(50));
        assert_eq!(metrics.prompt_tokens, 0);
        assert_eq!(metrics.completion_tokens, 0);
        assert_eq!(metrics.total_tokens, 0);
        assert_eq!(metrics.response_type, "other");
    }

    #[test]
    fn test_analytics_metrics_extract_with_tokens() {
        let instance_id = Uuid::new_v4();

        let request_json = r#"{"model": "gpt-4", "messages": [{"role": "user", "content": "hello"}]}"#;
        let request_data = RequestData {
            correlation_id: 12345,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/v1/chat/completions".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(request_json)),
            trace_id: None,
            span_id: None,
        };

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 12345,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: None,
            duration: Duration::from_millis(500),
            duration_to_first_byte: Duration::from_millis(50),
        };

        #[allow(deprecated)]
        let chat_response = ChatCompletionResponse {
            id: "chatcmpl-123".to_string(),
            object: "chat.completion".to_string(),
            created: 1677652288,
            model: "gpt-5".to_string(),
            choices: vec![],
            usage: Some(ChatUsage {
                prompt_tokens: 15,
                completion_tokens: 25,
                total_tokens: 40,
                prompt_tokens_details: None,
                completion_tokens_details: None,
            }),
            system_fingerprint: None,
            service_tier: None,
        };

        let parsed_response = AiResponse::ChatCompletions(chat_response);

        let metrics = UsageMetrics::extract(
            instance_id,
            &request_data,
            &response_data,
            &parsed_response,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(metrics.instance_id, instance_id);
        assert_eq!(metrics.correlation_id, 12345);
        assert_eq!(metrics.method, "POST");
        assert_eq!(metrics.uri, "/v1/chat/completions");
        assert_eq!(metrics.request_model, Some("gpt-4".to_string()));
        assert_eq!(metrics.response_model, Some("gpt-5".to_string()));
        assert_eq!(metrics.status_code, 200);
        assert_eq!(metrics.duration_ms, 500);
        assert_eq!(metrics.prompt_tokens, 15);
        assert_eq!(metrics.completion_tokens, 25);
        assert_eq!(metrics.reasoning_tokens, 0);
        assert_eq!(metrics.total_tokens, 40);
        assert_eq!(metrics.response_type, "chat_completion");
    }

    #[test]
    fn test_analytics_metrics_extract_streaming_tokens() {
        let instance_id = Uuid::new_v4();

        let request_data = RequestData {
            correlation_id: 12345,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/v1/chat/completions".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: None,
            trace_id: None,
            span_id: None,
        };

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 12345,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: None,
            duration: Duration::from_millis(300),
            duration_to_first_byte: Duration::from_millis(50),
        };

        #[allow(deprecated)]
        let stream_chunk = ChatChunk {
            id: "chatcmpl-123".to_string(),
            object: "chat.completion.chunk".to_string(),
            created: 1677652288,
            model: "gpt-4".to_string(),
            choices: vec![],
            usage: Some(ChatUsage {
                prompt_tokens: 8,
                completion_tokens: 12,
                total_tokens: 20,
                prompt_tokens_details: None,
                completion_tokens_details: None,
            }),
            system_fingerprint: None,
            service_tier: None,
        };

        let parsed_response = AiResponse::ChatCompletionsStream(vec![crate::request_logging::models::ChatCompletionChunk::Chunk(
            Box::new(stream_chunk),
        )]);

        let metrics = UsageMetrics::extract(
            instance_id,
            &request_data,
            &response_data,
            &parsed_response,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(metrics.prompt_tokens, 8);
        assert_eq!(metrics.completion_tokens, 12);
        assert_eq!(metrics.reasoning_tokens, 0);
        assert_eq!(metrics.total_tokens, 20);
        assert_eq!(metrics.response_type, "chat_completion_stream");
    }

    #[test]
    fn test_analytics_metrics_extract_chat_reasoning_tokens() {
        let instance_id = Uuid::new_v4();
        let request_json = r#"{"model": "gpt-5", "messages": [{"role": "user", "content": "hello"}]}"#;
        let request_data = RequestData {
            correlation_id: 12345,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/v1/chat/completions".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(request_json)),
            trace_id: None,
            span_id: None,
        };

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 12345,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: None,
            duration: Duration::from_millis(500),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let chat_response: ChatCompletionResponse = serde_json::from_value(serde_json::json!({
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1677652288,
            "model": "gpt-5",
            "choices": [],
            "usage": {
                "prompt_tokens": 15,
                "completion_tokens": 25,
                "total_tokens": 40,
                "completion_tokens_details": {
                    "reasoning_tokens": 11
                }
            }
        }))
        .unwrap();

        let parsed_response = AiResponse::ChatCompletions(chat_response);
        let metrics = UsageMetrics::extract(
            instance_id,
            &request_data,
            &response_data,
            &parsed_response,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(metrics.reasoning_tokens, 11);
        assert_eq!(metrics.completion_tokens, 25);
        assert_eq!(metrics.total_tokens, 40);
    }

    #[test]
    fn test_analytics_metrics_extract_embeddings_tokens() {
        let instance_id = Uuid::new_v4();

        let request_data = RequestData {
            correlation_id: 12345,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/v1/embeddings".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: None,
            trace_id: None,
            span_id: None,
        };

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 12345,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: None,
            duration: Duration::from_millis(150),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let embeddings_response = EmbeddingsResponse {
            object: "list".to_string(),
            data: vec![],
            model: "text-embedding-ada-002".to_string(),
            usage: EmbeddingsUsage {
                prompt_tokens: 6,
                total_tokens: 6,
            },
        };

        let parsed_response = AiResponse::Embeddings(embeddings_response);

        let metrics = UsageMetrics::extract(
            instance_id,
            &request_data,
            &response_data,
            &parsed_response,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(metrics.prompt_tokens, 6);
        assert_eq!(metrics.completion_tokens, 0);
        assert_eq!(metrics.total_tokens, 6);
        assert_eq!(metrics.response_type, "embeddings");
    }

    #[test]
    fn test_analytics_metrics_extract_completions_tokens() {
        let instance_id = Uuid::new_v4();

        let request_data = RequestData {
            correlation_id: 12345,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/v1/completions".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: None,
            trace_id: None,
            span_id: None,
        };

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 12345,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: None,
            duration: Duration::from_millis(400),
            duration_to_first_byte: Duration::from_millis(50),
        };

        #[allow(deprecated)]
        let completions_response = CompletionResponse {
            id: "cmpl-123".to_string(),
            object: "text_completion".to_string(),
            created: 1677652288,
            model: "gpt-3.5-turbo-instruct".to_string(),
            choices: vec![],
            usage: Some(ChatUsage {
                prompt_tokens: 10,
                completion_tokens: 15,
                total_tokens: 25,
                prompt_tokens_details: None,
                completion_tokens_details: None,
            }),
            system_fingerprint: None,
        };

        let parsed_response = AiResponse::Completions(completions_response);

        let metrics = UsageMetrics::extract(
            instance_id,
            &request_data,
            &response_data,
            &parsed_response,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(metrics.prompt_tokens, 10);
        assert_eq!(metrics.completion_tokens, 15);
        assert_eq!(metrics.total_tokens, 25);
        assert_eq!(metrics.response_type, "completion");
    }

    #[test]
    fn test_analytics_metrics_extract_base64_embeddings_tokens() {
        let instance_id = Uuid::new_v4();

        let request_data = RequestData {
            correlation_id: 12345,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/v1/embeddings".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: None,
            trace_id: None,
            span_id: None,
        };

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 12345,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: None,
            duration: Duration::from_millis(200),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let base64_embeddings_response = EmbeddingsResponse {
            object: "list".to_string(),
            data: vec![EmbeddingData {
                object: "embedding".to_string(),
                embedding: Embedding::Base64("ZmFrZQ==".to_string()),
                index: 0,
            }],
            model: "text-embedding-3-large".to_string(),
            usage: EmbeddingsUsage {
                prompt_tokens: 4,
                total_tokens: 4,
            },
        };

        let parsed_response = AiResponse::Embeddings(base64_embeddings_response);

        let metrics = UsageMetrics::extract(
            instance_id,
            &request_data,
            &response_data,
            &parsed_response,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(metrics.prompt_tokens, 4);
        assert_eq!(metrics.completion_tokens, 0);
        assert_eq!(metrics.total_tokens, 4);
        assert_eq!(metrics.response_type, "base64_embeddings");
    }

    fn responses_api_body(usage: bool) -> String {
        let usage_json = if usage {
            r#","usage":{"input_tokens":15,"input_tokens_details":{"cached_tokens":0},"output_tokens":25,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":40}"#
        } else {
            ""
        };
        format!(
            r#"{{"id":"resp_123","object":"response","created_at":1234567890,"status":"completed","model":"gpt-4o","output":[],"tools":[],"tool_choice":"auto","truncation":"disabled","parallel_tool_calls":true,"text":{{"format":{{"type":"text"}}}},"top_p":1.0,"presence_penalty":0.0,"frequency_penalty":0.0,"top_logprobs":0,"temperature":1.0,"reasoning":null,"store":false,"background":false,"service_tier":"default"{usage_json}}}"#
        )
    }

    fn responses_request_data(stream: Option<bool>) -> RequestData {
        let stream_field = match stream {
            Some(true) => r#","stream":true"#,
            Some(false) => r#","stream":false"#,
            None => "",
        };
        let body = format!(r#"{{"model":"gpt-4o","input":"tell me a joke"{stream_field}}}"#);
        RequestData {
            correlation_id: 1,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/v1/responses".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(body)),
            trace_id: None,
            span_id: None,
        }
    }

    fn responses_response_data(body: String) -> ResponseData {
        ResponseData {
            extensions: Default::default(),
            correlation_id: 1,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: Some(Bytes::from(body)),
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        }
    }

    #[test]
    fn chat_request_with_reasoning_effort_max_is_not_downgraded_to_other() {
        let body = r#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"reasoning_effort":"max"}"#;
        let mut data = responses_request_data(None);
        data.uri = "/v1/chat/completions".parse::<Uri>().unwrap();
        data.body = Some(Bytes::from(body));

        match parse_ai_request(&data).unwrap().request {
            AiRequest::ChatCompletions(req) => {
                assert_eq!(req.model, "gpt-4o");
                assert_eq!(req.reasoning_effort, Some(serde_json::json!("max")));
            }
            other => panic!("expected ChatCompletions, got {other:?}"),
        }
    }

    #[test]
    fn streaming_response_is_parsed_even_when_request_type_is_unrecognised() {
        let body = r#"{"model":"gpt-4o","messages":42,"stream":true}"#;
        let mut req = responses_request_data(None);
        req.uri = "/v1/chat/completions".parse::<Uri>().unwrap();
        req.body = Some(Bytes::from(body));
        assert!(
            matches!(parse_ai_request(&req).unwrap().request, AiRequest::Other(_)),
            "fixture must produce an unrecognised request for this guard to mean anything"
        );

        let sse = concat!(
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",",
            "\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4o\",",
            "\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":7,\"total_tokens\":12}}\n\n",
            "data: [DONE]\n\n"
        );
        let parsed = parse_ai_response(&req, &responses_response_data(sse.to_string())).expect("streamed body should parse");

        match &parsed {
            AiResponse::ChatCompletionsStream(chunks) => assert!(!chunks.is_empty()),
            other => panic!("expected ChatCompletionsStream, got {other:?}"),
        }
        let metrics = crate::request_logging::serializers::TokenMetrics::from(&parsed);
        assert_eq!(metrics.completion_tokens, 7, "streamed request must not bill zero tokens");
        assert_eq!(metrics.prompt_tokens, 5);
    }

    #[test]
    fn test_parse_ai_request_responses_path_not_classified_as_embeddings() {
        let result = parse_ai_request(&responses_request_data(None)).unwrap();

        let rr = result.responses_request.expect("responses_request should be set");
        assert_eq!(rr.model, Some("gpt-4o".to_string()));
        assert_eq!(rr.stream, None);

        match result.request {
            AiRequest::Other(_) => {}
            _ => panic!("expected AiRequest::Other for /v1/responses, not Embeddings or ChatCompletions"),
        }
    }

    #[test]
    fn test_parse_ai_request_responses_path_stream_flag() {
        let result = parse_ai_request(&responses_request_data(Some(true))).unwrap();
        let rr = result.responses_request.unwrap();
        assert_eq!(rr.stream, Some(true));
    }

    #[test]
    fn test_parse_ai_response_responses_non_streaming() {
        let request_data = responses_request_data(None);
        let response_data = responses_response_data(responses_api_body(true));

        let result = parse_ai_response(&request_data, &response_data).unwrap();

        match result {
            AiResponse::Responses(resp) => {
                assert_eq!(resp.model, "gpt-4o");
                let usage = resp.usage.expect("usage should be present");
                assert_eq!(usage.input_tokens, 15);
                assert_eq!(usage.output_tokens, 25);
                assert_eq!(usage.output_tokens_details.reasoning_tokens, 0);
                assert_eq!(usage.total_tokens, 40);
            }
            _ => panic!("expected AiResponse::Responses"),
        }
    }

    #[test]
    fn test_analytics_metrics_extract_responses_reasoning_tokens() {
        let instance_id = Uuid::new_v4();
        let request_data = responses_request_data(None);
        let response_data = responses_response_data(
            r#"{"id":"resp_123","object":"response","created_at":1234567890,"status":"completed","model":"gpt-4o","output":[],"tools":[],"tool_choice":"auto","truncation":"disabled","parallel_tool_calls":true,"text":{"format":{"type":"text"}},"top_p":1.0,"presence_penalty":0.0,"frequency_penalty":0.0,"top_logprobs":0,"temperature":1.0,"reasoning":null,"store":false,"background":false,"service_tier":"default","usage":{"input_tokens":15,"input_tokens_details":{"cached_tokens":0},"output_tokens":25,"output_tokens_details":{"reasoning_tokens":9},"total_tokens":40}}"#
                .to_string(),
        );

        let parsed_response = parse_ai_response(&request_data, &response_data).unwrap();
        let metrics = UsageMetrics::extract(
            instance_id,
            &request_data,
            &response_data,
            &parsed_response,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(metrics.reasoning_tokens, 9);
        assert_eq!(metrics.completion_tokens, 25);
        assert_eq!(metrics.total_tokens, 40);
        assert_eq!(metrics.response_type, "response");
    }

    #[test]
    fn test_parse_ai_response_responses_error_body_falls_back_to_other() {
        let request_data = responses_request_data(None);
        let error_json = r#"{"error":{"message":"invalid request","type":"invalid_request_error","code":"model_not_found"}}"#;
        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 1,
            timestamp: SystemTime::now(),
            status: StatusCode::BAD_REQUEST,
            headers: HashMap::new(),
            body: Some(Bytes::from(error_json)),
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        };

        let result = parse_ai_response(&request_data, &response_data).unwrap();
        match result {
            AiResponse::Other(_) => {}
            _ => panic!("expected AiResponse::Other for error body, got something else"),
        }
    }

    #[test]
    fn test_parse_ai_response_responses_streaming() {
        let request_data = responses_request_data(Some(true));

        let completed_data = responses_api_body(true);
        let sse_body = format!(
            "data: {{\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"item_id\":\"item_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hello\"}}\n\ndata: {{\"type\":\"response.completed\",\"sequence_number\":5,\"response\":{completed_data}}}\n\n"
        );
        let response_data = responses_response_data(sse_body);

        let result = parse_ai_response(&request_data, &response_data).unwrap();

        match result {
            AiResponse::ResponsesStream(events) => {
                assert!(!events.is_empty(), "should have parsed at least the completed event");
                let has_completed = events.iter().any(|e| e.event_type == "response.completed");
                assert!(has_completed, "should contain a response.completed event");
            }
            _ => panic!("expected AiResponse::ResponsesStream"),
        }
    }

    #[test]
    fn test_analytics_metrics_extract_responses_tokens() {
        let instance_id = Uuid::new_v4();
        let request_data = responses_request_data(None);
        let response_data = responses_response_data(responses_api_body(true));

        let parsed_response = parse_ai_response(&request_data, &response_data).unwrap();

        let metrics = UsageMetrics::extract(
            instance_id,
            &request_data,
            &response_data,
            &parsed_response,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(metrics.request_model, Some("gpt-4o".to_string()));
        assert_eq!(metrics.response_model, Some("gpt-4o".to_string()));
        assert_eq!(metrics.prompt_tokens, 15);
        assert_eq!(metrics.completion_tokens, 25);
        assert_eq!(metrics.total_tokens, 40);
        assert_eq!(metrics.response_type, "response");
    }

    #[test]
    fn test_analytics_metrics_extract_responses_streaming_tokens() {
        let instance_id = Uuid::new_v4();
        let request_data = responses_request_data(Some(true));

        let completed_data = responses_api_body(true);
        let sse_body = format!("data: {{\"type\":\"response.completed\",\"sequence_number\":5,\"response\":{completed_data}}}\n\n");
        let response_data = responses_response_data(sse_body);

        let parsed_response = parse_ai_response(&request_data, &response_data).unwrap();

        let metrics = UsageMetrics::extract(
            instance_id,
            &request_data,
            &response_data,
            &parsed_response,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(metrics.request_model, Some("gpt-4o".to_string()));
        assert_eq!(metrics.response_model, Some("gpt-4o".to_string()));
        assert_eq!(metrics.prompt_tokens, 15);
        assert_eq!(metrics.completion_tokens, 25);
        assert_eq!(metrics.total_tokens, 40);
        assert_eq!(metrics.response_type, "response_stream");
    }


    fn messages_request_data(stream: bool) -> RequestData {
        let body =
            format!(r#"{{"model":"claude-3-5-sonnet","max_tokens":100,"messages":[{{"role":"user","content":"hi"}}],"stream":{stream}}}"#);
        RequestData {
            correlation_id: 1,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/v1/messages".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(body)),
            trace_id: None,
            span_id: None,
        }
    }

    fn ok_response(body: impl Into<Bytes>) -> ResponseData {
        ResponseData {
            extensions: Default::default(),
            correlation_id: 1,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: Some(body.into()),
            duration: Duration::from_millis(100),
            duration_to_first_byte: Duration::from_millis(50),
        }
    }

    fn agent_loop_messages_body() -> Value {
        serde_json::json!({
            "model": "claude-3-5-sonnet",
            "max_tokens": 4096,
            "temperature": 0.5,
            "thinking": {"type": "enabled", "budget_tokens": 1024},
            "system": [{"type": "text", "text": "You are a helpful agent."}],
            "tools": [
                {"name": "read_file", "description": "Read a file", "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}}},
                {"name": "list_dir", "description": "List a directory", "input_schema": {"type": "object", "properties": {}}}
            ],
            "messages": [
                {"role": "user", "content": "What is in README.md?"},
                {"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "I should read the file.", "signature": "sig"},
                    {"type": "tool_use", "id": "toolu_1", "name": "read_file", "input": {"path": "README.md"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_1", "content": "# Example project"}
                ]}
            ],
            "stream": true
        })
    }

    fn messages_request_with_body(body: &Value) -> RequestData {
        RequestData {
            body: Some(Bytes::from(body.to_string())),
            ..messages_request_data(false)
        }
    }

    #[test]
    fn test_parse_ai_request_messages_body_stored_as_sent() {
        let body = agent_loop_messages_body();
        let parsed = parse_ai_request(&messages_request_with_body(&body)).unwrap();

        match &parsed.request {
            AiRequest::Other(value) => assert_eq!(value, &body, "the body must be stored complete"),
            other => panic!("expected AiRequest::Other for /v1/messages, got {other:?}"),
        }
        assert_eq!(serde_json::to_value(&parsed.request).unwrap(), body);

        let mr = parsed.messages_request.expect("messages_request should be set");
        assert_eq!(mr.model.as_deref(), Some("claude-3-5-sonnet"));
        assert_eq!(mr.stream, Some(true));
        assert!(parsed.responses_request.is_none());
    }

    #[test]
    fn test_parse_ai_request_plain_messages_body_not_classified_as_chat() {
        let parsed = parse_ai_request(&messages_request_data(false)).unwrap();
        assert!(matches!(parsed.request, AiRequest::Other(_)));
        assert!(parsed.messages_request.is_some());
    }

    #[test]
    fn test_analytics_metrics_messages_request_params_and_model() {
        let request_data = messages_request_with_body(&agent_loop_messages_body());
        let sse = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-5-sonnet\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":12,\"output_tokens\":0}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":8}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
        let response_data = ok_response(sse);

        let parsed = parse_ai_response(&request_data, &response_data).unwrap();
        let metrics = UsageMetrics::extract(
            Uuid::new_v4(),
            &request_data,
            &response_data,
            &parsed,
            &crate::test::utils::create_test_config(),
        );
        assert_eq!(metrics.request_model.as_deref(), Some("claude-3-5-sonnet"));
        assert_eq!(
            metrics.request_params,
            RequestParams {
                stream: Some(true),
                max_tokens: Some(4096),
                temperature: Some(0.5),
                top_p: None,
                n: None,
                tool_count: Some(2),
                message_count: Some(3),
            }
        );
    }

    #[test]
    fn test_messages_request_params_defaults() {
        let parsed = parse_ai_request(&messages_request_with_body(
            &serde_json::json!({"model": "m", "max_tokens": 10, "messages": [{"role": "user", "content": "hi"}]}),
        ))
        .unwrap();
        let p = RequestParams::from_parsed(&parsed);
        assert_eq!(p.stream, Some(false));
        assert_eq!(p.tool_count, Some(0));
        assert_eq!(p.message_count, Some(1));
    }

    #[test]
    fn test_parse_ai_response_anthropic_blocking() {
        let request_data = messages_request_data(false);
        let body = r#"{"id":"msg_1","type":"message","role":"assistant","model":"claude-3-5-sonnet","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":12,"output_tokens":8}}"#;
        let response_data = ok_response(body);

        let result = parse_ai_response(&request_data, &response_data).unwrap();
        match result {
            AiResponse::Anthropic(resp) => {
                assert_eq!(resp.model, "claude-3-5-sonnet");
                assert_eq!(resp.usage.input_tokens, 12);
                assert_eq!(resp.usage.output_tokens, 8);
            }
            _ => panic!("expected AiResponse::Anthropic"),
        }
    }

    #[test]
    fn test_analytics_metrics_extract_anthropic_blocking_tokens() {
        let request_data = messages_request_data(false);
        let body = r#"{"id":"msg_1","type":"message","role":"assistant","model":"claude-3-5-sonnet","content":[],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":12,"output_tokens":8}}"#;
        let response_data = ok_response(body);

        let parsed = parse_ai_response(&request_data, &response_data).unwrap();
        let metrics = UsageMetrics::extract(
            Uuid::new_v4(),
            &request_data,
            &response_data,
            &parsed,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(metrics.response_model, Some("claude-3-5-sonnet".to_string()));
        assert_eq!(metrics.prompt_tokens, 12);
        assert_eq!(metrics.completion_tokens, 8);
        assert_eq!(metrics.total_tokens, 20);
        assert_eq!(metrics.reasoning_tokens, 0);
        assert_eq!(metrics.response_type, "anthropic_message");
        assert_eq!(metrics.status_code, 200);
    }

    #[test]
    fn anthropic_prompt_tokens_include_cached_and_match_chat_completions() {
        let anthropic_request = messages_request_data(false);
        let anthropic_response = ok_response(
            r#"{"id":"msg_1","type":"message","role":"assistant","model":"claude-3-5-sonnet","content":[],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":5000,"output_tokens":8,"cache_read_input_tokens":12000,"cache_creation_input_tokens":3000,"cache_creation":{"ephemeral_1h_input_tokens":3000}}}"#,
        );
        let anthropic_parsed = parse_ai_response(&anthropic_request, &anthropic_response).unwrap();
        let anthropic = UsageMetrics::extract(
            Uuid::new_v4(),
            &anthropic_request,
            &anthropic_response,
            &anthropic_parsed,
            &crate::test::utils::create_test_config(),
        );

        let chat_request = RequestData {
            correlation_id: 2,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/v1/chat/completions".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(
                r#"{"model":"claude-3-5-sonnet","messages":[{"role":"user","content":"hi"}]}"#,
            )),
            trace_id: None,
            span_id: None,
        };
        let chat_response = ok_response(
            r#"{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"claude-3-5-sonnet","choices":[{"index":0,"message":{"role":"assistant","content":""},"finish_reason":"stop"}],"usage":{"prompt_tokens":20000,"completion_tokens":8,"total_tokens":20008,"prompt_tokens_details":{"cached_tokens":12000},"cache_read_input_tokens":12000,"cache_creation_input_tokens":3000,"cache_creation":{"ephemeral_1h_input_tokens":3000}}}"#,
        );
        let chat_parsed = parse_ai_response(&chat_request, &chat_response).unwrap();
        let chat = UsageMetrics::extract(
            Uuid::new_v4(),
            &chat_request,
            &chat_response,
            &chat_parsed,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(
            anthropic.prompt_tokens, chat.prompt_tokens,
            "the same conversation must report the same prompt_tokens on both ingresses"
        );
        assert_eq!(
            anthropic.prompt_tokens, 20000,
            "prompt_tokens must be TOTAL input (5000 uncached + 12000 read + 3000 written)"
        );
        assert_eq!(anthropic.completion_tokens, chat.completion_tokens);
        assert_eq!(anthropic.total_tokens, 20008);
        assert_eq!(anthropic.cache_read_input_tokens, 12000);
        assert_eq!(anthropic.cache_read_input_tokens, chat.cache_read_input_tokens);
        assert_eq!(anthropic.cache_creation_1h_input_tokens, 3000);
        assert_eq!(anthropic.cache_creation_1h_input_tokens, chat.cache_creation_1h_input_tokens);
    }

    #[test]
    fn anthropic_stream_prompt_tokens_include_cached() {
        let request_data = messages_request_data(true);
        let sse = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-5-sonnet\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":5000,\"output_tokens\":0,\"cache_read_input_tokens\":12000,\"cache_creation_input_tokens\":3000}}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"input_tokens\":5000,\"output_tokens\":8,\"cache_read_input_tokens\":12000,\"cache_creation_input_tokens\":3000}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        let response_data = ok_response(sse);
        let parsed = parse_ai_response(&request_data, &response_data).unwrap();
        let metrics = UsageMetrics::extract(
            Uuid::new_v4(),
            &request_data,
            &response_data,
            &parsed,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(metrics.prompt_tokens, 20000, "streaming must fold the cache buckets in too");
        assert_eq!(metrics.completion_tokens, 8);
        assert_eq!(metrics.total_tokens, 20008);
    }

    #[test]
    fn anthropic_prompt_tokens_unchanged_without_caching() {
        let request_data = messages_request_data(false);
        let body = r#"{"id":"msg_1","type":"message","role":"assistant","model":"claude-3-5-sonnet","content":[],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":12,"output_tokens":8}}"#;
        let response_data = ok_response(body);
        let parsed = parse_ai_response(&request_data, &response_data).unwrap();
        let metrics = UsageMetrics::extract(
            Uuid::new_v4(),
            &request_data,
            &response_data,
            &parsed,
            &crate::test::utils::create_test_config(),
        );
        assert_eq!(metrics.prompt_tokens, 12);
        assert_eq!(metrics.total_tokens, 20);
    }

    #[test]
    fn test_analytics_metrics_extract_anthropic_streaming_tokens() {
        let request_data = messages_request_data(true);
        let sse = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-5-sonnet\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":12,\"output_tokens\":0}}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"input_tokens\":12,\"output_tokens\":8}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        let response_data = ok_response(sse);

        let parsed = parse_ai_response(&request_data, &response_data).unwrap();
        assert!(
            matches!(parsed, AiResponse::AnthropicStream(_)),
            "streaming /messages should parse as AnthropicStream"
        );

        let metrics = UsageMetrics::extract(
            Uuid::new_v4(),
            &request_data,
            &response_data,
            &parsed,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(metrics.response_model, Some("claude-3-5-sonnet".to_string()));
        assert_eq!(metrics.prompt_tokens, 12);
        assert_eq!(metrics.completion_tokens, 8);
        assert_eq!(metrics.total_tokens, 20);
        assert_eq!(metrics.response_type, "anthropic_message_stream");
        assert_eq!(metrics.status_code, 200);
    }

    #[test]
    fn test_anthropic_stream_error_event_reclassifies_to_500() {
        let request_data = messages_request_data(true);
        let sse = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-5-sonnet\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}\n\n",
            "event: error\n",
            "data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"overloaded\"}}\n\n",
        );
        let response_data = ok_response(sse);

        let parsed = parse_ai_response(&request_data, &response_data).unwrap();
        let metrics = UsageMetrics::extract(
            Uuid::new_v4(),
            &request_data,
            &response_data,
            &parsed,
            &crate::test::utils::create_test_config(),
        );

        assert_eq!(metrics.status_code, 500);
    }

    fn response_with_body(body: impl Into<Bytes>) -> ResponseData {
        ResponseData {
            extensions: Default::default(),
            correlation_id: 1,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: Some(body.into()),
            duration: Duration::from_millis(1),
            duration_to_first_byte: Duration::from_millis(1),
        }
    }

    #[test]
    fn extract_cache_tokens_non_streaming() {
        let body = serde_json::json!({
            "usage": {
                "prompt_tokens": 2000, "completion_tokens": 5,
                "cache_read_input_tokens": 1000,
                "cache_creation": {"ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": 500, "ephemeral_24h_input_tokens": 0},
                "prompt_tokens_details": {"cached_tokens": 1000}
            }
        })
        .to_string();
        let c = extract_cache_tokens(&response_with_body(body));
        assert_eq!(c.read, 1000);
        assert_eq!(c.creation_1h, 500);
        assert_eq!(c.creation_5m, 0);
        assert_eq!(c.creation_24h, 0);
    }

    #[test]
    fn extract_cache_tokens_streaming_terminal_frame() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n\
                   data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2000,\"cache_read_input_tokens\":1500,\"cache_creation\":{\"ephemeral_24h_input_tokens\":1500}}}\n\n\
                   data: [DONE]\n\n";
        let c = extract_cache_tokens(&response_with_body(sse));
        assert_eq!(c.read, 1500);
        assert_eq!(c.creation_24h, 1500);
        assert_eq!(c.creation_1h, 0);
    }

    #[test]
    fn responses_terminal_usage_preserves_cache_split_and_total_input() {
        let usage = serde_json::json!({
            "input_tokens": 2000, "output_tokens": 5, "total_tokens": 2005,
            "input_tokens_details": {"cached_tokens": 1000},
            "output_tokens_details": {"reasoning_tokens": 2},
            "cache_read_input_tokens": 1000, "cache_creation_input_tokens": 600,
            "cache_creation": {"ephemeral_5m_input_tokens": 100, "ephemeral_1h_input_tokens": 200, "ephemeral_24h_input_tokens": 300}
        });
        for event_type in ["response.completed", "response.incomplete", "response.failed"] {
            let event = serde_json::json!({"type": event_type, "response": {"usage": usage}});
            let body = format!("event: {event_type}\ndata: {event}\n\ndata: [DONE]\n\n");
            let response = response_with_body(body);
            let cache = extract_cache_tokens(&response);
            assert_eq!(
                (cache.read, cache.creation_5m, cache.creation_1h, cache.creation_24h),
                (1000, 100, 200, 300)
            );
            let tokens = super::extract_from_last_usage(&response, super::raw_usage_tokens).unwrap();
            assert_eq!(tokens.prompt, 2000, "Responses input already includes cached tokens");
            assert_eq!(tokens.completion, 5);
            assert_eq!(tokens.reasoning, 2);
        }
    }

    #[test]
    fn responses_standard_usage_bills_from_internal_metadata() {
        for streaming in [false, true] {
            let mut body: Value = serde_json::from_str(&responses_api_body(true)).unwrap();
            body["usage"] = serde_json::json!({
                "input_tokens": 2000, "output_tokens": 5, "total_tokens": 2005,
                "input_tokens_details": {"cached_tokens": 1000, "cache_write_tokens": 600},
                "output_tokens_details": {"reasoning_tokens": 2}
            });
            let body = if streaming {
                format!("data: {{\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{body}}}\n\n")
            } else {
                body.to_string()
            };
            let request = responses_request_data(Some(streaming));
            let mut response = responses_response_data(body);
            let capture = CacheBilling::default();
            response.extensions.insert(capture.clone());
            capture.set(BilledCache {
                stats: CacheStats {
                    read: 1000,
                    creation_5m: 100,
                    creation_1h: 200,
                    creation_24h: 300,
                    ..Default::default()
                },
                read_source: Some(CacheReadSource::Module),
            });
            let parsed = parse_ai_response(&request, &response).unwrap();
            let metrics = UsageMetrics::extract(
                Uuid::new_v4(),
                &request,
                &response,
                &parsed,
                &crate::test::utils::create_test_config(),
            );
            assert_eq!(metrics.prompt_tokens, 2000);
            assert_eq!(metrics.completion_tokens, 5);
            assert_eq!(metrics.reasoning_tokens, 2);
            assert_eq!(metrics.cache_read_input_tokens, 1000);
            assert_eq!(metrics.cache_creation_5m_input_tokens, 100);
            assert_eq!(metrics.cache_creation_1h_input_tokens, 200);
            assert_eq!(metrics.cache_creation_24h_input_tokens, 300);
        }
    }

    #[test]
    fn internal_cache_accounting_takes_precedence_over_public_usage() {
        let mut response = response_with_body(
            serde_json::json!({
                "usage": {"cache_read_input_tokens": 999, "cache_creation": {"ephemeral_1h_input_tokens": 999}}
            })
            .to_string(),
        );
        let capture = CacheBilling::default();
        response.extensions.insert(capture.clone());
        let absent = extract_cache_tokens(&response);
        assert_eq!(
            (absent.read, absent.creation_1h),
            (0, 0),
            "an unfilled cell must not trust provider counts"
        );
        assert_eq!(extract_cache_read_source(&response), None, "unfilled cell → no source");
        capture.set(BilledCache {
            stats: CacheStats {
                read: 10,
                creation_1h: 20,
                ..Default::default()
            },
            read_source: Some(CacheReadSource::Engine),
        });
        let captured = extract_cache_tokens(&response);
        assert_eq!((captured.read, captured.creation_1h), (10, 20));
        assert_eq!(
            extract_cache_read_source(&response).as_deref(),
            Some("engine"),
            "read provenance rides the same cell as the counts"
        );
    }

    #[test]
    fn engine_cached_tokens_read_from_prompt_tokens_details_and_not_billed() {
        let body = serde_json::json!({
            "usage": {"prompt_tokens": 3607, "completion_tokens": 607,
                      "prompt_tokens_details": {"cached_tokens": 3456}}
        })
        .to_string();
        let r = response_with_body(body);
        assert_eq!(extract_engine_cached_tokens(&r), Some(3456));
        assert_eq!(extract_cache_tokens(&r).read, 0, "engine hits are not dwctl cache reads");
    }

    #[test]
    fn engine_cached_tokens_streaming_terminal_frame() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n\
                   data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2000,\"completion_tokens\":5,\"prompt_tokens_details\":{\"cached_tokens\":1984}}}\n\n\
                   data: [DONE]\n\n";
        assert_eq!(extract_engine_cached_tokens(&response_with_body(sse)), Some(1984));
    }

    #[test]
    fn engine_cached_tokens_absent_is_none_and_negative_is_clamped() {
        let none = serde_json::json!({"usage": {"prompt_tokens": 10, "completion_tokens": 1}}).to_string();
        assert_eq!(extract_engine_cached_tokens(&response_with_body(none)), None);
        let neg = serde_json::json!({"usage": {"prompt_tokens": 10, "prompt_tokens_details": {"cached_tokens": -5}}}).to_string();
        assert_eq!(extract_engine_cached_tokens(&response_with_body(neg)), Some(0));
        assert_eq!(extract_engine_cached_tokens(&response_with_body("")), None);
    }

    #[test]
    fn engine_cached_tokens_prefers_the_cache_layers_stashed_value() {
        let scrubbed = serde_json::json!({
            "usage": {"prompt_tokens": 985, "completion_tokens": 2, "prompt_tokens_details": {"cached_tokens": 0}}
        })
        .to_string();
        let mut r = response_with_body(scrubbed);
        let cell = crate::prompt_cache::UpstreamCachedTokens::default();
        cell.set(687);
        r.extensions.insert(cell);
        let request_data = RequestData {
            correlation_id: 1,
            timestamp: SystemTime::now(),
            method: axum::http::Method::POST,
            uri: "/chat/completions".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#)),
            trace_id: None,
            span_id: None,
        };
        let parsed = parse_ai_response(&request_data, &r).unwrap();
        let m = UsageMetrics::extract(uuid::Uuid::nil(), &request_data, &r, &parsed, &crate::config::Config::default());
        assert_eq!(m.engine_cached_tokens, Some(687));

        let mut r = response_with_body(
            serde_json::json!({"usage": {"prompt_tokens": 1, "prompt_tokens_details": {"cached_tokens": 3}}}).to_string(),
        );
        r.extensions.insert(crate::prompt_cache::UpstreamCachedTokens::default());
        let parsed = parse_ai_response(&request_data, &r).unwrap();
        let m = UsageMetrics::extract(uuid::Uuid::nil(), &request_data, &r, &parsed, &crate::config::Config::default());
        assert_eq!(m.engine_cached_tokens, Some(3));
    }

    fn params_for(uri: &str, request_body: &str) -> RequestParams {
        let request_data = RequestData {
            correlation_id: 1,
            timestamp: SystemTime::now(),
            method: axum::http::Method::POST,
            uri: uri.parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(request_body.to_owned())),
            trace_id: None,
            span_id: None,
        };
        let response_data = response_with_body(
            r#"{"id":"r","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}"#,
        );
        let parsed = parse_ai_response(&request_data, &response_data).unwrap();
        UsageMetrics::extract(
            uuid::Uuid::nil(),
            &request_data,
            &response_data,
            &parsed,
            &crate::config::Config::default(),
        )
        .request_params
    }

    #[test]
    fn request_params_read_from_a_chat_request() {
        let body = serde_json::json!({
            "model": "m",
            "messages": [
                {"role": "system", "content": "s"},
                {"role": "user", "content": "u"},
                {"role": "assistant", "content": "a"}
            ],
            "tools": [{"type": "function", "function": {"name": "get_weather", "description": "d",
                       "parameters": {"type": "object", "properties": {}}}}],
            "stream": true,
            "max_tokens": 100,
            "max_completion_tokens": 512,
            "temperature": 0.7,
            "top_p": 0.9,
            "n": 1
        })
        .to_string();
        let p = params_for("/chat/completions", &body);
        assert_eq!(
            p,
            RequestParams {
                stream: Some(true),
                max_tokens: Some(512),
                temperature: Some(0.7),
                top_p: Some(0.9),
                n: Some(1),
                tool_count: Some(1),
                message_count: Some(3),
            }
        );
    }

    #[test]
    fn request_params_stream_defaults_false_and_untyped_bodies_yield_none() {
        let p = params_for("/chat/completions", r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#);
        assert_eq!(p.stream, Some(false));
        assert_eq!(p.max_tokens, None);
        assert_eq!(p.tool_count, Some(0));
        assert_eq!(p.message_count, Some(1));

        let e = params_for("/embeddings", r#"{"model":"m","input":"hello"}"#);
        assert_eq!(e, RequestParams::default());
    }

    #[test]
    fn request_params_read_from_a_completions_request() {
        let p = params_for(
            "/completions",
            r#"{"model":"m","prompt":"hi","max_tokens":64,"temperature":0.2,"stream":false}"#,
        );
        assert_eq!(p.stream, Some(false));
        assert_eq!(p.max_tokens, Some(64));
        assert_eq!(p.temperature, Some(0.2));
        assert_eq!(p.tool_count, None, "completions have no tools");
        assert_eq!(p.message_count, None, "completions have no messages");
    }

    #[test]
    fn extract_cache_tokens_ignores_provider_native_cached_tokens() {
        let body = serde_json::json!({
            "usage": {"prompt_tokens": 100, "completion_tokens": 2, "prompt_tokens_details": {"cached_tokens": 64}}
        })
        .to_string();
        let c = extract_cache_tokens(&response_with_body(body));
        assert_eq!(c.read, 0, "provider-native cached_tokens is not a dwctl cache read");
    }

    #[test]
    fn extract_cache_tokens_absent_is_zero() {
        let plain = serde_json::json!({"usage": {"prompt_tokens": 10, "completion_tokens": 2}}).to_string();
        let c = extract_cache_tokens(&response_with_body(plain));
        assert_eq!(c.read, 0);
        assert_eq!(c.creation_1h, 0);

        let err = serde_json::json!({"error": {"message": "bad"}}).to_string();
        let c = extract_cache_tokens(&response_with_body(err));
        assert_eq!(c.read, 0);
    }


    fn chat_pair(body: &'static str, stream: bool) -> (RequestData, ResponseData) {
        let request_json = if stream {
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}"#
        } else {
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#
        };
        let request_data = RequestData {
            correlation_id: 1,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/v1/chat/completions".parse::<Uri>().unwrap(),
            headers: HashMap::new(),
            body: Some(Bytes::from(request_json)),
            trace_id: None,
            span_id: None,
        };
        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 1,
            timestamp: SystemTime::now(),
            status: StatusCode::OK,
            headers: HashMap::new(),
            body: Some(Bytes::from(body)),
            duration: Duration::from_millis(10),
            duration_to_first_byte: Duration::from_millis(5),
        };
        (request_data, response_data)
    }

    fn finish_reason_of(body: &'static str, stream: bool) -> Option<String> {
        let (req, res) = chat_pair(body, stream);
        let parsed = parse_ai_response(&req, &res).unwrap_or(AiResponse::Other(serde_json::Value::Null));
        extract_finish_reason(&parsed)
    }

    #[test]
    fn finish_reason_from_non_streamed_chat_completion() {
        let body = r#"{"id":"c1","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"t1","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;
        assert_eq!(finish_reason_of(body, false).as_deref(), Some("tool_calls"));
    }

    #[test]
    fn finish_reason_survives_a_terminal_usage_chunk_with_no_choices() {
        let body = concat!(
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"tool_calls\"}],\"usage\":null}\n\n",
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n",
            "data: [DONE]\n\n",
        );
        assert_eq!(finish_reason_of(body, true).as_deref(), Some("tool_calls"));
    }

    #[test]
    fn finish_reason_present_when_no_chunk_carries_usage() {
        let body = concat!(
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n",
        );
        assert_eq!(finish_reason_of(body, true).as_deref(), Some("tool_calls"));
    }

    #[test]
    fn finish_reason_skips_done_and_error_frames() {
        let body = concat!(
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"length\"}]}\n\n",
            "data: {\"error\":{\"message\":\"boom\",\"type\":\"internal_server_error\",\"code\":500}}\n\n",
            "data: [DONE]\n\n",
        );
        assert_eq!(finish_reason_of(body, true).as_deref(), Some("length"));
    }

    #[test]
    fn finish_reason_none_when_stream_never_terminated() {
        let body = concat!(
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
        );
        assert_eq!(finish_reason_of(body, true), None);
    }

    fn recorded_finish_reason(body: &'static str, ended: bool) -> Option<String> {
        let (req, mut res) = chat_pair(body, true);
        let completion = StreamCompletion::default();
        if ended {
            completion.mark_ended();
        }
        res.extensions.insert(completion);
        let parsed = parse_ai_response(&req, &res).unwrap_or(AiResponse::Other(serde_json::Value::Null));
        UsageMetrics::extract(uuid::Uuid::nil(), &req, &res, &parsed, &crate::config::Config::default()).finish_reason
    }

    #[test]
    fn finish_reason_records_a_client_disconnect() {
        let body = concat!(
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
        );
        assert_eq!(recorded_finish_reason(body, false).as_deref(), Some(CLIENT_DISCONNECTED));
        assert_eq!(recorded_finish_reason(body, true), None);
    }

    #[test]
    fn finish_reason_prefers_the_model_reason_over_a_disconnect() {
        let body = concat!(
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        );
        assert_eq!(recorded_finish_reason(body, false).as_deref(), Some("stop"));
    }

    #[test]
    fn finish_reason_none_for_shapes_without_the_concept() {
        let embeddings = r#"{"object":"list","model":"m","data":[{"object":"embedding","index":0,"embedding":[0.1]}],"usage":{"prompt_tokens":1,"total_tokens":1}}"#;
        assert_eq!(finish_reason_of(embeddings, false), None);
        assert_eq!(finish_reason_of("not json at all", false), None);
    }

    #[test]
    fn finish_reason_round_trips_through_usage_metrics() {
        let body = r#"{"id":"c1","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"x"},"finish_reason":"length"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;
        let (req, res) = chat_pair(body, false);
        let parsed = parse_ai_response(&req, &res).unwrap();
        let metrics = UsageMetrics::extract(Uuid::nil(), &req, &res, &parsed, &crate::config::Config::default());
        assert_eq!(metrics.finish_reason.as_deref(), Some("length"));
    }
}
