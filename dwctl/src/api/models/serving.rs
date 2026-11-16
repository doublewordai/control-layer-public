
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use super::tariffs::TariffResponse;
use crate::types::{DeploymentId, UserId};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OverlayResponse {
    #[schema(value_type = String, format = "uuid")]
    pub organization_id: UserId,
    pub organization_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organization_display_name: Option<String>,
    #[schema(value_type = String, format = "uuid")]
    pub deployed_model_id: DeploymentId,
    pub alias: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub targets: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_hosted_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provisioning_source: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OrganizationCacheTariffResponse {
    #[schema(value_type = String, format = "uuid")]
    pub deployed_model_id: DeploymentId,
    pub alias: String,
    #[schema(value_type = String)]
    pub write_multiplier_5m: Decimal,
    #[schema(value_type = String)]
    pub write_multiplier_1h: Decimal,
    #[schema(value_type = String)]
    pub write_multiplier_24h: Decimal,
    #[schema(value_type = String)]
    pub read_multiplier: Decimal,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serving_class: Option<String>,
    pub valid_from: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OrganizationServingResponse {
    #[schema(value_type = String, format = "uuid")]
    pub organization_id: UserId,
    pub self_hosted_only: bool,
    pub overlays: Vec<OverlayResponse>,
    pub tariffs: Vec<TariffResponse>,
    #[schema(value_type = std::collections::BTreeMap<String, String>)]
    pub model_aliases: std::collections::BTreeMap<DeploymentId, String>,
    pub cache_tariffs: Vec<OrganizationCacheTariffResponse>,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct OverlayRow {
    pub organization_id: Uuid,
    pub organization_name: String,
    pub organization_display_name: Option<String>,
    pub deployed_model_id: Uuid,
    pub alias: String,
    pub targets: Option<serde_json::Value>,
    pub self_hosted_only: Option<bool>,
    pub provisioning_source: Option<String>,
    pub updated_at: DateTime<Utc>,
}

impl From<OverlayRow> for OverlayResponse {
    fn from(row: OverlayRow) -> Self {
        Self {
            organization_id: row.organization_id,
            organization_name: row.organization_name,
            organization_display_name: row.organization_display_name,
            deployed_model_id: row.deployed_model_id,
            alias: row.alias,
            targets: row.targets,
            self_hosted_only: row.self_hosted_only,
            provisioning_source: row.provisioning_source,
            updated_at: row.updated_at,
        }
    }
}
