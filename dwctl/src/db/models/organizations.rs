
use crate::types::UserId;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct OrganizationCreateDBRequest {
    pub name: String,
    pub email: String,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub created_by: UserId,
}

#[derive(Debug, Clone)]
pub struct OrganizationUpdateDBRequest {
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub email: Option<String>,
    pub batch_notifications_enabled: Option<bool>,
    pub low_balance_threshold: Option<Option<f32>>,
    pub zero_data_retention: Option<bool>,
    pub self_hosted_only: Option<bool>,
    pub toleration_preset: Option<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct OrganizationMemberDBResponse {
    pub id: UserId,
    pub user_id: Option<UserId>,
    pub organization_id: UserId,
    pub role: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub invite_email: Option<String>,
    pub invited_by: Option<UserId>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct PendingOrgEmailChangeDBResponse {
    pub id: uuid::Uuid,
    pub organization_id: UserId,
    pub new_email: String,
    pub requested_by: UserId,
    pub new_email_confirmed_at: Option<DateTime<Utc>>,
    pub old_email_confirmed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl PendingOrgEmailChangeDBResponse {
    pub fn is_fully_confirmed(&self) -> bool {
        self.new_email_confirmed_at.is_some() && self.old_email_confirmed_at.is_some()
    }
}
