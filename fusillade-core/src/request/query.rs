
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const DEFAULT_LIMIT: i64 = 50;

pub fn service_tier_from_completion_window(completion_window: &str) -> Option<&'static str> {
    match completion_window {
        "1h" => Some("flex"),
        "0s" => Some("priority"),
        _ => None,
    }
}

#[derive(Debug, Clone, Default)]
pub enum ServiceTierFilter {
    #[default]
    Any,
    Include(Vec<Option<String>>),
    Exclude(Vec<Option<String>>),
}

impl ServiceTierFilter {
    pub fn split(tiers: &[Option<String>]) -> (Vec<String>, bool) {
        let mut names = Vec::with_capacity(tiers.len());
        let mut has_null = false;
        for t in tiers {
            match t {
                Some(s) => names.push(s.clone()),
                None => has_null = true,
            }
        }
        (names, has_null)
    }
}

#[derive(Debug, Clone)]
pub struct ListRequestsFilter {
    pub created_by: Option<String>,
    pub status: Option<String>,
    pub models: Option<Vec<String>>,
    pub created_after: Option<DateTime<Utc>>,
    pub created_before: Option<DateTime<Utc>>,
    pub service_tiers: Option<Vec<String>>,
    pub active_first: bool,
    pub skip: i64,
    pub limit: i64,
}

impl Default for ListRequestsFilter {
    fn default() -> Self {
        Self {
            created_by: None,
            status: None,
            models: None,
            created_after: None,
            created_before: None,
            service_tiers: None,
            active_first: false,
            skip: 0,
            limit: DEFAULT_LIMIT,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "sqlx-postgres", derive(sqlx::FromRow))]
pub struct RequestSummary {
    pub id: Uuid,
    pub batch_id: Option<Uuid>,
    pub model: String,
    #[cfg_attr(feature = "sqlx-postgres", sqlx(rename = "state"))]
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<f64>,
    pub response_status: Option<i16>,
    pub service_tier: Option<String>,
    pub created_by: String,
}

#[allow(deprecated)]
mod deprecated_types {
    use super::{DateTime, Deserialize, RequestSummary, Serialize, Utc, Uuid};

    #[deprecated(
        since = "16.1.1",
        note = "no longer used internally; use RequestSummary and RequestListResult.total_count"
    )]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[cfg_attr(feature = "sqlx-postgres", derive(sqlx::FromRow))]
    pub struct RequestSummaryWithCount {
        pub id: Uuid,
        pub batch_id: Uuid,
        pub model: String,
        #[cfg_attr(feature = "sqlx-postgres", sqlx(rename = "state"))]
        pub status: String,
        pub created_at: DateTime<Utc>,
        pub completed_at: Option<DateTime<Utc>>,
        pub failed_at: Option<DateTime<Utc>>,
        pub duration_ms: Option<f64>,
        pub response_status: Option<i16>,
        pub service_tier: Option<String>,
        pub batch_created_by: String,
        pub total_count: i64,
    }

    impl From<RequestSummaryWithCount> for RequestSummary {
        fn from(r: RequestSummaryWithCount) -> Self {
            Self {
                id: r.id,
                batch_id: Some(r.batch_id),
                model: r.model,
                status: r.status,
                created_at: r.created_at,
                completed_at: r.completed_at,
                failed_at: r.failed_at,
                duration_ms: r.duration_ms,
                response_status: r.response_status,
                service_tier: r.service_tier,
                created_by: r.batch_created_by,
            }
        }
    }
}

#[allow(deprecated)]
pub use deprecated_types::RequestSummaryWithCount;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "sqlx-postgres", derive(sqlx::FromRow))]
pub struct RequestDetail {
    pub id: Uuid,
    pub batch_id: Option<Uuid>,
    pub model: String,
    #[cfg_attr(feature = "sqlx-postgres", sqlx(rename = "state"))]
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<f64>,
    pub response_status: Option<i16>,
    pub body: Option<String>,
    pub response_body: Option<String>,
    pub error: Option<String>,
    pub service_tier: Option<String>,
    pub created_by: String,
}

#[derive(Debug, Clone)]
pub struct CreateRealtimeInput {
    pub request_id: Uuid,
    pub body: String,
    pub model: String,
    pub endpoint: String,
    pub method: String,
    pub path: String,
    pub created_by: String,
}

#[derive(Debug, Clone)]
pub struct PersistCompletedRealtimeInput {
    pub request_id: Uuid,
    pub response_body: String,
    pub status_code: u16,
    pub request_body: String,
    pub model: String,
    pub endpoint: String,
    pub method: String,
    pub path: String,
    pub created_by: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct CreateFlexInput {
    pub request_id: Uuid,
    pub body: String,
    pub model: String,
    pub endpoint: String,
    pub method: String,
    pub path: String,
    pub api_key_id: Uuid,
    pub created_by: String,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct CreateBackgroundInput {
    pub request_id: Uuid,
    pub body: String,
    pub model: String,
    pub endpoint: String,
    pub method: String,
    pub path: String,
    pub api_key_id: Uuid,
    pub created_by: String,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestListResult {
    pub data: Vec<RequestSummary>,
    pub total_count: i64,
}

#[cfg(test)]
mod background_tests {
    use super::*;

    #[test]
    fn background_input_has_the_async_batchless_contract() {
        let request_id = Uuid::new_v4();
        let input = CreateBackgroundInput {
            request_id,
            body: r#"{"model":"model-a"}"#.to_string(),
            model: "model-a".to_string(),
            endpoint: "http://localhost:3001".to_string(),
            method: "POST".to_string(),
            path: "/v1/responses".to_string(),
            api_key_id: uuid::Uuid::new_v4(),
            created_by: "user-a".to_string(),
            metadata: None,
        };

        assert_eq!(input.request_id, request_id);
        assert_eq!(input.model, "model-a");
        assert_eq!(input.created_by, "user-a");
    }
}
