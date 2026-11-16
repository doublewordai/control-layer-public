
use chrono::{DateTime, Utc};
use sqlx::FromRow;
use uuid::Uuid;

use crate::types::UserId;
use crate::webhooks::WebhookEventType;

pub type WebhookId = Uuid;

pub type DeliveryId = Uuid;

#[derive(Debug, Clone, FromRow)]
pub struct Webhook {
    pub id: WebhookId,
    pub user_id: UserId,
    pub url: String,
    pub secret: String,
    pub enabled: bool,
    pub event_types: Option<serde_json::Value>,
    pub description: Option<String>,
    pub scope: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub consecutive_failures: i32,
    pub disabled_at: Option<DateTime<Utc>>,
}

impl Webhook {
    pub fn accepts_event(&self, event_type: WebhookEventType) -> bool {
        if !self.enabled {
            return false;
        }

        let webhook_scope = if self.scope == "platform" {
            crate::webhooks::WebhookScope::Platform
        } else {
            crate::webhooks::WebhookScope::Own
        };
        if event_type.scope() != webhook_scope {
            return false;
        }

        let Some(ref types) = self.event_types else {
            return true;
        };

        if let Some(arr) = types.as_array() {
            let event_str = event_type.to_string();
            arr.iter().any(|v| v.as_str() == Some(&event_str))
        } else {
            true
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    Failed,
    Exhausted,
}

impl DeliveryStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Failed => "failed",
            Self::Exhausted => "exhausted",
        }
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct WebhookDelivery {
    pub id: DeliveryId,
    pub webhook_id: WebhookId,
    pub event_id: Uuid,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub status: String,
    pub attempt_count: i32,
    pub next_attempt_at: DateTime<Utc>,
    pub resource_id: Option<Uuid>,
    pub last_status_code: Option<i32>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow)]
pub struct ClaimedDelivery {
    pub id: DeliveryId,
    pub webhook_id: WebhookId,
    pub event_id: Uuid,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub status: String,
    pub attempt_count: i32,
    pub next_attempt_at: DateTime<Utc>,
    pub resource_id: Option<Uuid>,
    pub last_status_code: Option<i32>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub webhook_url: Option<String>,
    pub webhook_secret: Option<String>,
    pub webhook_enabled: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct WebhookCreateDBRequest {
    pub user_id: UserId,
    pub url: String,
    pub secret: String,
    pub event_types: Option<Vec<String>>,
    pub description: Option<String>,
    pub scope: String,
}

#[derive(Debug, Clone, Default)]
pub struct WebhookUpdateDBRequest {
    pub url: Option<String>,
    pub enabled: Option<bool>,
    pub event_types: Option<Option<Vec<String>>>,
    pub description: Option<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct WebhookDeliveryCreateDBRequest {
    pub webhook_id: WebhookId,
    pub event_id: Uuid,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub resource_id: Option<Uuid>,
    pub next_attempt_at: Option<DateTime<Utc>>,
}
