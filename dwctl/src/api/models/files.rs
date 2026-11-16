
use crate::api::models::Pagination;

use super::pagination::CursorPagination;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ListFilesQuery {
    #[serde(flatten)]
    #[param(inline)]
    pub pagination: CursorPagination,

    #[serde(default = "default_order")]
    pub order: String,

    pub purpose: Option<String>,

    pub search: Option<String>,

    #[serde(default)]
    pub own: bool,

    pub member_id: Option<uuid::Uuid>,
}

fn default_order() -> String {
    "desc".to_string()
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct FileContentQuery {
    #[serde(flatten)]
    #[param(inline)]
    pub pagination: Pagination,

    pub search: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct FileCostEstimateQuery {
    pub completion_window: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "id": "file-abc123",
    "object": "file",
    "bytes": 12045,
    "created_at": 1703187200,
    "filename": "batch_requests.jsonl",
    "purpose": "batch"
}))]
pub struct FileResponse {
    #[schema(example = "file-abc123")]
    pub id: String,
    #[serde(rename = "object")]
    pub object_type: ObjectType,
    #[schema(example = 12045)]
    pub bytes: i64,
    #[schema(example = 1703187200)]
    pub created_at: i64,
    #[schema(example = "batch_requests.jsonl")]
    pub filename: String,
    pub purpose: Purpose,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by_email: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ObjectType {
    File,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    Batch,
    BatchOutput,
    BatchError,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "id": "file-abc123",
    "object": "file",
    "deleted": true
}))]
pub struct FileDeleteResponse {
    #[schema(example = "file-abc123")]
    pub id: String,
    #[serde(rename = "object")]
    pub object_type: ObjectType,
    #[schema(example = true)]
    pub deleted: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "object": "list",
    "data": [{
        "id": "file-abc123",
        "object": "file",
        "bytes": 12045,
        "created_at": 1703187200,
        "filename": "batch_requests.jsonl",
        "purpose": "batch"
    }],
    "first_id": "file-abc123",
    "last_id": "file-abc123",
    "has_more": false
}))]
pub struct FileListResponse {
    #[serde(rename = "object")]
    pub object_type: ListObject,
    pub data: Vec<FileResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "file-abc123")]
    pub first_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "file-abc123")]
    pub last_id: Option<String>,
    #[schema(example = false)]
    pub has_more: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ListObject {
    List,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "model": "Qwen/Qwen3-30B-A3B-FP8",
    "request_count": 100,
    "estimated_input_tokens": 50000,
    "estimated_output_tokens": 25000,
    "estimated_cost": "0.75"
}))]
pub struct ModelCostBreakdown {
    #[schema(example = "Qwen/Qwen3-30B-A3B-FP8")]
    pub model: String,
    #[schema(example = 100)]
    pub request_count: i64,
    #[schema(example = 50000)]
    pub estimated_input_tokens: i64,
    #[schema(example = 25000)]
    pub estimated_output_tokens: i64,
    #[schema(example = "0.75")]
    pub estimated_cost: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "file_id": "file-abc123",
    "total_requests": 100,
    "total_estimated_input_tokens": 50000,
    "total_estimated_output_tokens": 25000,
    "total_estimated_cost": "0.75",
    "models": [{
        "model": "Qwen/Qwen3-30B-A3B-FP8",
        "request_count": 100,
        "estimated_input_tokens": 50000,
        "estimated_output_tokens": 25000,
        "estimated_cost": "0.75"
    }]
}))]
pub struct FileCostEstimate {
    #[schema(example = "file-abc123")]
    pub file_id: String,
    #[schema(example = 100)]
    pub total_requests: i64,
    #[schema(example = 50000)]
    pub total_estimated_input_tokens: i64,
    #[schema(example = 25000)]
    pub total_estimated_output_tokens: i64,
    #[schema(example = "0.75")]
    pub total_estimated_cost: String,
    pub models: Vec<ModelCostBreakdown>,
}
