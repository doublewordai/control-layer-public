
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct Probe {
    #[schema(value_type = String, format = "uuid")]
    pub id: Uuid,
    pub name: String,
    #[schema(value_type = String, format = "uuid")]
    pub deployment_id: Uuid,
    pub interval_seconds: i32,
    pub active: bool,
    pub http_method: String,
    pub request_path: Option<String>,
    pub request_body: Option<serde_json::Value>,
    #[schema(value_type = String, format = "date-time")]
    pub created_at: DateTime<Utc>,
    #[schema(value_type = String, format = "date-time")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct ProbeResult {
    #[schema(value_type = String, format = "uuid")]
    pub id: Uuid,
    #[schema(value_type = String, format = "uuid")]
    pub probe_id: Uuid,
    #[schema(value_type = String, format = "date-time")]
    pub executed_at: DateTime<Utc>,
    pub success: bool,
    pub response_time_ms: Option<i32>,
    pub status_code: Option<i32>,
    pub error_message: Option<String>,
    pub response_data: Option<serde_json::Value>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeExecution {
    pub probe_id: Uuid,
    pub success: bool,
    pub response_time_ms: i32,
    pub status_code: Option<i32>,
    pub error_message: Option<String>,
    pub response_data: Option<serde_json::Value>,
    pub metadata: Option<serde_json::Value>,
}
