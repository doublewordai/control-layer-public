
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::{IntoParams, ToSchema};

use super::dwext::BatchDwExtResponse;
use crate::types::ApiKeyId;

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[schema(example = json!({
    "data": [{
        "code": "invalid_request",
        "line": 5,
        "message": "Invalid JSON on line 5"
    }]
}))]
pub struct BatchErrors {
    pub data: Vec<BatchError>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[schema(example = json!({
    "code": "invalid_request",
    "line": 5,
    "message": "Invalid JSON on line 5"
}))]
pub struct BatchError {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "invalid_request")]
    pub code: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 5)]
    pub line: Option<i32>,

    #[schema(example = "Invalid JSON on line 5")]
    pub message: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub param: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "input_file_id": "file-abc123",
    "endpoint": "/v1/chat/completions",
    "completion_window": "24h"
}))]
pub struct CreateBatchRequest {
    #[schema(example = "file-abc123")]
    pub input_file_id: String,

    #[schema(example = "/v1/chat/completions")]
    pub endpoint: String,

    #[schema(example = "24h")]
    pub completion_window: String,

    #[serde(default)]
    pub metadata: Option<HashMap<String, String>>,

    #[serde(default)]
    #[schema(value_type = Option<String>, format = "uuid")]
    pub api_key_id: Option<ApiKeyId>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[schema(example = json!({
    "request_ids": ["req-abc123", "req-def456"]
}))]
pub struct RetryRequestsRequest {
    pub request_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "id": "batch-abc123",
    "object": "batch",
    "endpoint": "/v1/chat/completions",
    "input_file_id": "file-abc123",
    "completion_window": "24h",
    "status": "completed",
    "output_file_id": "file-xyz789",
    "created_at": 1703187200,
    "completed_at": 1703190800,
    "request_counts": {
        "total": 100,
        "completed": 98,
        "failed": 2
    }
}))]
pub struct BatchResponse {
    #[schema(example = "batch-abc123")]
    pub id: String,

    #[serde(rename = "object")]
    pub object_type: BatchObjectType,

    #[schema(example = "/v1/chat/completions")]
    pub endpoint: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub errors: Option<BatchErrors>,

    #[schema(example = "file-abc123")]
    pub input_file_id: String,

    #[schema(example = "24h")]
    pub completion_window: String,

    #[schema(example = "completed")]
    pub status: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "file-xyz789")]
    pub output_file_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_file_id: Option<String>,

    #[schema(example = 1703187200)]
    pub created_at: i64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_progress_at: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalizing_at: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 1703190800)]
    pub completed_at: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub expired_at: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancelling_at: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancelled_at: Option<i64>,

    pub request_counts: RequestCounts,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(example = "Qwen/Qwen3-235B-A22B-Instruct-2507")]
    pub model: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub analytics: Option<BatchAnalytics>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dwext: Option<BatchDwExtResponse>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "total_requests": 100,
    "total_prompt_tokens": 50000,
    "total_completion_tokens": 25000,
    "total_tokens": 75000,
    "avg_duration_ms": 1250.5,
    "avg_ttfb_ms": 150.2,
    "total_cost": "0.75",
    "total_list_cost": "1.50"
}))]
pub struct BatchAnalytics {
    #[schema(example = 100)]
    pub total_requests: i64,

    #[schema(example = 50000)]
    pub total_prompt_tokens: i64,

    #[schema(example = 25000)]
    pub total_completion_tokens: i64,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 12000)]
    pub total_reasoning_tokens: Option<i64>,

    #[schema(example = 75000)]
    pub total_tokens: i64,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 1250.5)]
    pub avg_duration_ms: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 150.2)]
    pub avg_ttfb_ms: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "0.75")]
    pub total_cost: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "1.50")]
    pub total_list_cost: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum BatchObjectType {
    Batch,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "total": 100,
    "completed": 98,
    "failed": 2
}))]
pub struct RequestCounts {
    #[schema(example = 100)]
    pub total: i64,
    #[schema(example = 98)]
    pub completed: i64,
    #[schema(example = 2)]
    pub failed: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "object": "list",
    "data": [{
        "id": "batch-abc123",
        "object": "batch",
        "endpoint": "/v1/chat/completions",
        "input_file_id": "file-abc123",
        "completion_window": "24h",
        "status": "completed",
        "created_at": 1703187200,
        "request_counts": {"total": 100, "completed": 98, "failed": 2}
    }],
    "first_id": "batch-abc123",
    "last_id": "batch-abc123",
    "has_more": false
}))]
pub struct BatchListResponse {
    #[serde(rename = "object")]
    pub object_type: ListObjectType,

    pub data: Vec<BatchResponse>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "batch-abc123")]
    pub first_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "batch-abc123")]
    pub last_id: Option<String>,

    #[schema(example = false)]
    pub has_more: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ListObjectType {
    List,
}

use super::pagination::CursorPagination;
use crate::api::models::Pagination;

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ListBatchesQuery {
    #[serde(flatten)]
    #[param(inline)]
    pub pagination: CursorPagination,

    pub search: Option<String>,

    #[param(example = "analytics")]
    pub include: Option<String>,

    pub member_id: Option<uuid::Uuid>,

    pub status: Option<String>,

    #[param(example = "2025-01-01T00:00:00Z")]
    pub created_after: Option<chrono::DateTime<chrono::Utc>>,

    #[param(example = "2025-12-31T23:59:59Z")]
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,

    #[serde(default)]
    pub active_first: bool,

    #[param(example = "24h,1h")]
    pub completion_window: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct BatchResultsQuery {
    #[serde(flatten)]
    #[param(inline)]
    pub pagination: Pagination,

    pub search: Option<String>,

    pub status: Option<String>,
}
