
use crate::db::models::api_keys::ApiKeyPurpose;
use crate::types::DeploymentId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ModelTariff {
    pub id: Uuid,
    pub deployed_model_id: DeploymentId,
    pub name: String,
    pub input_price_per_token: Decimal,
    pub output_price_per_token: Decimal,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub api_key_purpose: Option<ApiKeyPurpose>,
    pub completion_window: Option<String>,
    pub user_id: Option<Uuid>,
    pub serving_class: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TariffCreateDBRequest {
    pub deployed_model_id: DeploymentId,
    pub name: String,
    pub input_price_per_token: Decimal,
    pub output_price_per_token: Decimal,
    pub api_key_purpose: Option<ApiKeyPurpose>,
    pub completion_window: Option<String>,
    pub valid_from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub user_id: Option<Uuid>,
}

pub type TariffDBResponse = ModelTariff;
