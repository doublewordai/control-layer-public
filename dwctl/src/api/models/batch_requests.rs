use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use super::pagination::Pagination;

#[derive(Debug, Default, Deserialize, IntoParams, ToSchema)]
pub struct ListBatchRequestsQuery {
    #[serde(flatten)]
    #[param(inline)]
    pub pagination: Pagination,

    pub status: Option<String>,

    pub model: Option<String>,

    pub member_id: Option<uuid::Uuid>,

    pub created_after: Option<DateTime<Utc>>,

    pub created_before: Option<DateTime<Utc>>,

    pub service_tiers: Option<String>,

    pub active_first: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ResponseSummary {
    pub id: Uuid,
    pub batch_id: Option<Uuid>,
    pub model: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<f64>,
    pub response_status: Option<i16>,
    pub service_tier: Option<String>,
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub cache_read_input_tokens: Option<i64>,
    pub cache_creation_input_tokens: Option<i64>,
    pub input_price_per_token: Option<f64>,
    pub output_price_per_token: Option<f64>,
    pub uncached_cost: Option<f64>,
    pub total_cost: Option<f64>,
    pub created_by_email: Option<String>,
    pub api_key_id: Option<Uuid>,
    pub api_key_name: Option<String>,
    pub api_key_holder_email: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ResponseDetail {
    pub id: Uuid,
    pub batch_id: Option<Uuid>,
    pub model: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<f64>,
    pub response_status: Option<i16>,
    pub service_tier: Option<String>,
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub cache_read_input_tokens: Option<i64>,
    pub cache_creation_input_tokens: Option<i64>,
    pub input_price_per_token: Option<f64>,
    pub output_price_per_token: Option<f64>,
    pub uncached_cost: Option<f64>,
    pub total_cost: Option<f64>,
    pub body: String,
    pub response_body: Option<String>,
    pub error: Option<String>,
    pub created_by: String,
    pub created_by_email: Option<String>,
    pub api_key_id: Option<Uuid>,
    pub api_key_name: Option<String>,
    pub api_key_holder_email: Option<String>,
}
