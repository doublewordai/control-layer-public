
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateProbe {
    pub name: String,
    #[schema(value_type = String, format = "uuid")]
    pub deployment_id: Uuid,
    pub interval_seconds: i32,
    #[serde(default = "default_http_method")]
    pub http_method: String,
    pub request_path: Option<String>,
    pub request_body: Option<serde_json::Value>,
}

fn default_http_method() -> String {
    "POST".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TestProbeRequest {
    pub http_method: Option<String>,
    pub request_path: Option<String>,
    pub request_body: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ProbesQuery {
    pub status: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ResultsQuery {
    #[param(value_type = Option<String>, format = "date-time")]
    #[schema(value_type = Option<String>, format = "date-time")]
    pub start_time: Option<DateTime<Utc>>,
    #[param(value_type = Option<String>, format = "date-time")]
    #[schema(value_type = Option<String>, format = "date-time")]
    pub end_time: Option<DateTime<Utc>>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct StatsQuery {
    #[param(value_type = Option<String>, format = "date-time")]
    #[schema(value_type = Option<String>, format = "date-time")]
    pub start_time: Option<DateTime<Utc>>,
    #[param(value_type = Option<String>, format = "date-time")]
    #[schema(value_type = Option<String>, format = "date-time")]
    pub end_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct UpdateProbeRequest {
    pub interval_seconds: Option<i32>,
    pub http_method: Option<String>,
    pub request_path: Option<String>,
    pub request_body: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProbeStatistics {
    pub total_executions: i64,
    pub successful_executions: i64,
    pub failed_executions: i64,
    pub success_rate: f64,
    pub avg_response_time_ms: Option<f64>,
    pub min_response_time_ms: Option<i32>,
    pub max_response_time_ms: Option<i32>,
    pub p50_response_time_ms: Option<f64>,
    pub p95_response_time_ms: Option<f64>,
    pub p99_response_time_ms: Option<f64>,
    #[schema(value_type = Option<String>, format = "date-time")]
    pub last_execution: Option<DateTime<Utc>>,
    #[schema(value_type = Option<String>, format = "date-time")]
    pub last_success: Option<DateTime<Utc>>,
    #[schema(value_type = Option<String>, format = "date-time")]
    pub last_failure: Option<DateTime<Utc>>,
}

impl Default for ProbeStatistics {
    fn default() -> Self {
        Self {
            total_executions: 0,
            successful_executions: 0,
            failed_executions: 0,
            success_rate: 0.0,
            avg_response_time_ms: None,
            min_response_time_ms: None,
            max_response_time_ms: None,
            p50_response_time_ms: None,
            p95_response_time_ms: None,
            p99_response_time_ms: None,
            last_execution: None,
            last_success: None,
            last_failure: None,
        }
    }
}
