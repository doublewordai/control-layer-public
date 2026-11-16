
use super::pagination::Pagination;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};

use crate::request_logging::{AiRequest, AiResponse};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum ApiAiRequest {
    ChatCompletions(serde_json::Value),
    Completions(serde_json::Value),
    Embeddings(serde_json::Value),
    Other(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum ApiAiResponse {
    ChatCompletions(serde_json::Value),
    ChatCompletionsStream(serde_json::Value),
    Completions(serde_json::Value),
    CompletionsStream(serde_json::Value),
    Embeddings(serde_json::Value),
    Responses(serde_json::Value),
    ResponsesStream(serde_json::Value),
    Anthropic(serde_json::Value),
    AnthropicStream(serde_json::Value),
    Other(serde_json::Value),
}

impl From<&AiRequest> for ApiAiRequest {
    fn from(ai_request: &AiRequest) -> Self {
        match ai_request {
            AiRequest::ChatCompletions(req) => ApiAiRequest::ChatCompletions(serde_json::to_value(req).unwrap_or_default()),
            AiRequest::Completions(req) => ApiAiRequest::Completions(serde_json::to_value(req).unwrap_or_default()),
            AiRequest::Embeddings(req) => ApiAiRequest::Embeddings(serde_json::to_value(req).unwrap_or_default()),
            AiRequest::Other(val) => ApiAiRequest::Other(val.clone()),
        }
    }
}

impl From<&AiResponse> for ApiAiResponse {
    fn from(ai_response: &AiResponse) -> Self {
        match ai_response {
            AiResponse::ChatCompletions(resp) => ApiAiResponse::ChatCompletions(serde_json::to_value(resp).unwrap_or_default()),
            AiResponse::ChatCompletionsStream(chunks) => {
                ApiAiResponse::ChatCompletionsStream(serde_json::to_value(chunks).unwrap_or_default())
            }
            AiResponse::Completions(resp) => ApiAiResponse::Completions(serde_json::to_value(resp).unwrap_or_default()),
            AiResponse::CompletionsStream(chunks) => ApiAiResponse::CompletionsStream(serde_json::to_value(chunks).unwrap_or_default()),
            AiResponse::Embeddings(resp) => ApiAiResponse::Embeddings(serde_json::to_value(resp).unwrap_or_default()),
            AiResponse::Responses(resp) => ApiAiResponse::Responses(serde_json::to_value(resp).unwrap_or_default()),
            AiResponse::ResponsesStream(events) => ApiAiResponse::ResponsesStream(serde_json::to_value(events).unwrap_or_default()),
            AiResponse::Anthropic(resp) => ApiAiResponse::Anthropic(serde_json::to_value(resp).unwrap_or_default()),
            AiResponse::AnthropicStream(events) => ApiAiResponse::AnthropicStream(serde_json::to_value(events).unwrap_or_default()),
            AiResponse::Other(val) => ApiAiResponse::Other(val.clone()),
        }
    }
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct AggregateRequestsQuery {
    pub model: Option<String>,

    pub timestamp_after: Option<DateTime<Utc>>,

    pub timestamp_before: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct ListRequestsQuery {
    #[serde(flatten)]
    #[param(inline)]
    pub pagination: Pagination,

    pub method: Option<String>,

    pub uri_pattern: Option<String>,

    pub status_code: Option<i32>,

    pub status_code_min: Option<i32>,

    pub status_code_max: Option<i32>,

    pub min_duration_ms: Option<i64>,

    pub max_duration_ms: Option<i64>,

    pub timestamp_after: Option<DateTime<Utc>>,

    pub timestamp_before: Option<DateTime<Utc>>,

    pub order_desc: Option<bool>,

    pub model: Option<String>,

    pub fusillade_batch_id: Option<uuid::Uuid>,

    pub custom_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HttpRequest {
    pub id: i64,
    pub timestamp: DateTime<Utc>,
    pub method: String,
    pub uri: String,
    pub headers: Value,
    pub body: Option<ApiAiRequest>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HttpResponse {
    pub id: i64,
    pub timestamp: DateTime<Utc>,
    pub status_code: i32,
    pub headers: Value,
    pub body: Option<ApiAiResponse>,
    pub duration_ms: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RequestResponsePair {
    pub request: HttpRequest,
    pub response: Option<HttpResponse>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListRequestsResponse {
    pub requests: Vec<RequestResponsePair>,
}


#[derive(Debug, Default)]
pub struct HttpAnalyticsFilter {
    pub method: Option<String>,
    pub uri_pattern: Option<String>,
    pub status_code: Option<i32>,
    pub status_code_min: Option<i32>,
    pub status_code_max: Option<i32>,
    pub min_duration_ms: Option<i64>,
    pub max_duration_ms: Option<i64>,
    pub timestamp_after: Option<DateTime<Utc>>,
    pub timestamp_before: Option<DateTime<Utc>>,
    pub model: Option<String>,
    pub fusillade_batch_id: Option<uuid::Uuid>,
    pub custom_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AnalyticsEntry {
    pub id: i64,
    pub timestamp: DateTime<Utc>,
    pub method: String,
    pub uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fusillade_batch_id: Option<uuid::Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_price_per_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_price_per_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListAnalyticsResponse {
    pub entries: Vec<AnalyticsEntry>,
}

impl Default for ListRequestsQuery {
    fn default() -> Self {
        Self {
            pagination: Pagination::default(),
            method: None,
            uri_pattern: None,
            status_code: None,
            status_code_min: None,
            status_code_max: None,
            min_duration_ms: None,
            max_duration_ms: None,
            timestamp_after: None,
            timestamp_before: None,
            order_desc: Some(true),
            model: None,
            fusillade_batch_id: None,
            custom_id: None,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct UsageDateQuery {
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    pub refresh: Option<bool>,
    #[param(value_type = Option<String>, format = "uuid")]
    #[schema(value_type = Option<String>, format = "uuid")]
    pub user_id: Option<crate::types::UserId>,
}


#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StatusCodeBreakdown {
    pub status: String,
    pub count: i64,
    pub percentage: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ModelUsage {
    pub model: String,
    pub count: i64,
    pub percentage: f64,
    pub avg_latency_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserUsage {
    pub user_id: Option<String>,
    pub user_email: Option<String>,
    pub request_count: i64,
    pub total_tokens: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_cost: Option<f64>,
    pub last_active_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ModelUserUsageResponse {
    pub model: String,
    pub start_date: DateTime<Utc>,
    pub end_date: DateTime<Utc>,
    pub total_requests: i64,
    pub total_tokens: i64,
    pub total_cost: Option<f64>,
    pub users: Vec<UserUsage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TimeSeriesPoint {
    pub timestamp: DateTime<Utc>,
    pub duration_minutes: i32,
    pub requests: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub avg_latency_ms: Option<f64>,
    pub p95_latency_ms: Option<f64>,
    pub p99_latency_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RequestsAggregateResponse {
    pub total_requests: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub status_codes: Vec<StatusCodeBreakdown>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub models: Option<Vec<ModelUsage>>,
    pub time_series: Vec<TimeSeriesPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ModelBreakdownEntry {
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cost: String,
    pub request_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserBatchUsageResponse {
    pub total_input_tokens: i64,
    pub total_output_tokens: i64,
    pub total_request_count: i64,
    pub total_batch_count: i64,
    pub avg_requests_per_batch: f64,
    pub total_cost: String,
    pub estimated_realtime_cost: String,
    pub by_model: Vec<ModelBreakdownEntry>,
}
