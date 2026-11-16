
use super::pagination::Pagination;
use crate::db::models::api_keys::{ApiKeyDBResponse, ApiKeyPurpose};
use crate::types::{ApiKeyId, DeploymentId, UserId};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

fn default_api_key_purpose() -> ApiKeyPurpose {
    ApiKeyPurpose::Realtime
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiKeyCreate {
    pub name: String,
    pub description: Option<String>,
    #[serde(default = "default_api_key_purpose")]
    pub purpose: ApiKeyPurpose,
    #[schema(value_type = Option<String>, format = "uuid")]
    pub member_id: Option<UserId>,
    #[serde(default)]
    #[schema(value_type = Option<String>)]
    pub spend_limit: Option<Decimal>,
    #[serde(default)]
    pub spend_limit_interval: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiKeyUpdate {
    pub name: Option<String>,
    pub description: Option<String>,
    #[serde(default, with = "::serde_with::rust::double_option", skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>)]
    pub spend_limit: Option<Option<Decimal>>,
    #[serde(default, with = "::serde_with::rust::double_option", skip_serializing_if = "Option::is_none")]
    pub spend_limit_interval: Option<Option<String>>,
    #[serde(default)]
    pub reset_window: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiKeyResponse {
    #[schema(value_type = String, format = "uuid")]
    pub id: ApiKeyId,
    pub name: String,
    pub description: Option<String>,
    pub key: String,
    pub purpose: ApiKeyPurpose,
    #[schema(value_type = String, format = "uuid")]
    pub user_id: UserId,
    #[schema(value_type = String, format = "uuid")]
    pub created_by: UserId,
    pub created_at: DateTime<Utc>,
    pub last_used: Option<DateTime<Utc>>,
    #[schema(value_type = Vec<String>)]
    pub model_access: Vec<DeploymentId>,
    pub secret_revealed_at: Option<DateTime<Utc>>,
    #[schema(value_type = Option<String>)]
    pub spend_limit: Option<Decimal>,
    pub spend_limit_interval: Option<String>,
    #[schema(value_type = Option<String>)]
    pub spend: Option<Decimal>,
    #[schema(value_type = Option<String>)]
    pub total_spend: Option<Decimal>,
    pub resets_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiKeyInfoResponse {
    #[schema(value_type = String, format = "uuid")]
    pub id: ApiKeyId,
    pub name: String,
    pub description: Option<String>,
    pub purpose: ApiKeyPurpose,
    #[schema(value_type = String, format = "uuid")]
    pub user_id: UserId,
    #[schema(value_type = String, format = "uuid")]
    pub created_by: UserId,
    pub created_at: DateTime<Utc>,
    pub last_used: Option<DateTime<Utc>>,
    #[schema(value_type = Vec<String>)]
    pub model_access: Vec<DeploymentId>,
    pub secret_revealed_at: Option<DateTime<Utc>>,
    #[schema(value_type = Option<String>)]
    pub spend_limit: Option<Decimal>,
    pub spend_limit_interval: Option<String>,
    #[schema(value_type = Option<String>)]
    pub spend: Option<Decimal>,
    #[schema(value_type = Option<String>)]
    pub total_spend: Option<Decimal>,
    pub resets_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiKeySecretResponse {
    pub key: String,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ListApiKeysQuery {
    #[serde(flatten)]
    #[param(inline)]
    pub pagination: Pagination,
    #[param(value_type = Option<String>, format = "uuid")]
    #[schema(value_type = Option<String>, format = "uuid")]
    pub created_by: Option<UserId>,
}

impl ApiKeyResponse {
    pub fn new(db: ApiKeyDBResponse, key: String) -> Self {
        Self {
            id: db.id,
            name: db.name,
            description: db.description,
            key,
            purpose: db.purpose,
            user_id: db.user_id,
            created_by: db.created_by,
            created_at: db.created_at,
            last_used: db.last_used,
            model_access: db.model_access,
            secret_revealed_at: db.secret_revealed_at,
            spend_limit: db.spend_limit,
            spend_limit_interval: db.spend_limit_interval,
            spend: None,
            total_spend: None,
            resets_at: None,
        }
    }

    pub fn with_spend_state(mut self, state: Option<&crate::db::models::api_keys::ApiKeySpendState>) -> Self {
        if let Some(state) = state {
            self.spend = state.spend;
            self.total_spend = state.total_spend;
            self.resets_at = state.resets_at;
        }
        self
    }
}

impl From<ApiKeyDBResponse> for ApiKeyInfoResponse {
    fn from(db: ApiKeyDBResponse) -> Self {
        Self {
            id: db.id,
            name: db.name,
            description: db.description,
            purpose: db.purpose,
            user_id: db.user_id,
            created_by: db.created_by,
            created_at: db.created_at,
            last_used: db.last_used,
            model_access: db.model_access,
            secret_revealed_at: db.secret_revealed_at,
            spend_limit: db.spend_limit,
            spend_limit_interval: db.spend_limit_interval,
            spend: None,
            total_spend: None,
            resets_at: None,
        }
    }
}

impl ApiKeyInfoResponse {
    pub fn with_spend_state(mut self, state: Option<&crate::db::models::api_keys::ApiKeySpendState>) -> Self {
        if let Some(state) = state {
            self.spend = state.spend;
            self.total_spend = state.total_spend;
            self.resets_at = state.resets_at;
        }
        self
    }
}
