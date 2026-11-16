
use crate::api::models::api_keys::ApiKeyCreate;
use crate::types::{ApiKeyId, DeploymentId, UserId};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, sqlx::Type)]
#[sqlx(type_name = "VARCHAR", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum ApiKeyPurpose {
    Platform,
    Realtime,
    Batch,
    Playground,
    Continuation,
}

impl ApiKeyPurpose {
    pub fn is_customer_billing(&self) -> bool {
        matches!(self, Self::Realtime | Self::Batch | Self::Playground)
    }
}

pub fn is_inference_purpose(purpose: &str) -> bool {
    matches!(purpose, "realtime" | "batch" | "playground" | "continuation")
}

#[cfg(test)]
mod purpose_tests {
    use super::is_inference_purpose;

    #[test]
    fn inference_purposes_allowed_platform_rejected() {
        assert!(is_inference_purpose("realtime"));
        assert!(is_inference_purpose("batch"));
        assert!(is_inference_purpose("playground"));
        assert!(is_inference_purpose("continuation"));
        assert!(!is_inference_purpose("platform"));
        assert!(!is_inference_purpose("unknown"));
    }
}

#[derive(Debug, Clone)]
pub struct ApiKeyCreateDBRequest {
    pub user_id: UserId,
    pub name: String,
    pub description: Option<String>,
    pub purpose: ApiKeyPurpose,
    pub created_by: UserId,
    pub spend_limit: Option<Decimal>,
    pub spend_limit_interval: Option<String>,
}

impl ApiKeyCreateDBRequest {
    pub fn new(user_id: UserId, created_by: UserId, create: ApiKeyCreate) -> Self {
        Self {
            user_id,
            name: create.name,
            description: create.description,
            purpose: create.purpose,
            created_by,
            spend_limit: create.spend_limit,
            spend_limit_interval: create.spend_limit_interval,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ApiKeyUpdateDBRequest {
    pub name: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ApiKeyDBResponse {
    pub id: ApiKeyId,
    pub name: String,
    pub description: Option<String>,
    pub purpose: ApiKeyPurpose,
    pub user_id: UserId,
    pub created_by: UserId,
    pub created_at: DateTime<Utc>,
    pub last_used: Option<DateTime<Utc>>,
    pub model_access: Vec<DeploymentId>,
    pub spend_limit: Option<Decimal>,
    pub spend_limit_interval: Option<String>,
    pub parent_api_key_id: Option<ApiKeyId>,
    pub hidden: bool,
    pub secret_revealed_at: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct CreatedApiKey {
    pub key: ApiKeyDBResponse,
    pub secret: String,
}

impl std::ops::Deref for CreatedApiKey {
    type Target = ApiKeyDBResponse;
    fn deref(&self) -> &ApiKeyDBResponse {
        &self.key
    }
}

impl std::ops::DerefMut for CreatedApiKey {
    fn deref_mut(&mut self) -> &mut ApiKeyDBResponse {
        &mut self.key
    }
}

impl std::fmt::Debug for CreatedApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreatedApiKey")
            .field("key", &self.key)
            .field("secret", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ApiKeySpendState {
    pub spend: Option<Decimal>,
    pub total_spend: Option<Decimal>,
    pub resets_at: Option<DateTime<Utc>>,
}
