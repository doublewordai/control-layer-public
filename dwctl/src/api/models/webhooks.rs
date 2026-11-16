
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::db::models::webhooks::{Webhook, WebhookId};
use crate::types::{UserId, UserIdOrCurrent};

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct WebhookCreate {
    pub url: String,
    #[serde(default)]
    pub event_types: Option<Vec<String>>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default = "default_scope")]
    pub scope: String,
}

fn default_scope() -> String {
    "own".to_string()
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct WebhookUpdate {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub event_types: Option<Option<Vec<String>>>,
    #[serde(default)]
    pub description: Option<Option<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WebhookResponse {
    #[schema(value_type = String, format = "uuid")]
    pub id: WebhookId,
    #[schema(value_type = String, format = "uuid")]
    pub user_id: UserId,
    pub url: String,
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_types: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub scope: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disabled_at: Option<DateTime<Utc>>,
}

impl From<Webhook> for WebhookResponse {
    fn from(webhook: Webhook) -> Self {
        let event_types = webhook.event_types.and_then(|v| serde_json::from_value::<Vec<String>>(v).ok());

        Self {
            id: webhook.id,
            user_id: webhook.user_id,
            url: webhook.url,
            enabled: webhook.enabled,
            event_types,
            description: webhook.description,
            scope: webhook.scope,
            created_at: webhook.created_at,
            updated_at: webhook.updated_at,
            disabled_at: webhook.disabled_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WebhookWithSecretResponse {
    #[schema(value_type = String, format = "uuid")]
    pub id: WebhookId,
    #[schema(value_type = String, format = "uuid")]
    pub user_id: UserId,
    pub url: String,
    pub secret: String,
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_types: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub scope: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<Webhook> for WebhookWithSecretResponse {
    fn from(webhook: Webhook) -> Self {
        let event_types = webhook.event_types.and_then(|v| serde_json::from_value::<Vec<String>>(v).ok());

        Self {
            id: webhook.id,
            user_id: webhook.user_id,
            url: webhook.url,
            secret: webhook.secret,
            enabled: webhook.enabled,
            event_types,
            description: webhook.description,
            scope: webhook.scope,
            created_at: webhook.created_at,
            updated_at: webhook.updated_at,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebhookPathParams {
    pub user_id: UserIdOrCurrent,
    pub webhook_id: Uuid,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UserWebhookPathParams {
    pub user_id: UserIdOrCurrent,
}
