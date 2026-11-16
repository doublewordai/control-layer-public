
use crate::api::models::users::{Role, UserCreate, UserUpdate};
use crate::types::UserId;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct UserCreateDBRequest {
    pub username: String,
    pub email: String,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub is_admin: bool,
    pub roles: Vec<Role>,
    pub auth_source: String,
    pub password_hash: Option<String>,
    pub external_user_id: Option<String>,
}

impl From<UserCreate> for UserCreateDBRequest {
    fn from(api: UserCreate) -> Self {
        Self {
            username: api.username,
            email: api.email,
            display_name: api.display_name,
            avatar_url: api.avatar_url,
            is_admin: false,
            roles: api.roles,
            auth_source: "proxy-header".to_string(),
            password_hash: None,
            external_user_id: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UserUpdateDBRequest {
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub roles: Option<Vec<Role>>,
    pub password_hash: Option<String>,
    pub batch_notifications_enabled: Option<bool>,
    pub low_balance_threshold: Option<Option<f32>>,
    pub auto_topup_amount: Option<Option<f32>>,
    pub auto_topup_threshold: Option<Option<f32>>,
    pub auto_topup_monthly_limit: Option<Option<f32>>,
    pub zero_data_retention: Option<bool>,
    pub self_hosted_only: Option<bool>,
    pub toleration_preset: Option<Option<String>>,
}

impl UserUpdateDBRequest {
    pub fn new(update: UserUpdate) -> Self {
        Self {
            display_name: update.display_name,
            avatar_url: update.avatar_url,
            roles: update.roles,
            password_hash: None,
            batch_notifications_enabled: update.batch_notifications_enabled,
            low_balance_threshold: update.low_balance_threshold,
            auto_topup_amount: update.auto_topup_amount,
            auto_topup_threshold: update.auto_topup_threshold,
            auto_topup_monthly_limit: update.auto_topup_monthly_limit,
            zero_data_retention: update.zero_data_retention,
            self_hosted_only: update.self_hosted_only,
            toleration_preset: update.toleration_preset,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UserDBResponse {
    pub id: UserId,
    pub username: String,
    pub email: String,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_login: Option<DateTime<Utc>>,
    pub auth_source: String,
    pub is_admin: bool,
    pub roles: Vec<Role>,
    pub password_hash: Option<String>,
    pub external_user_id: Option<String>,
    pub payment_provider_id: Option<String>,
    pub batch_notifications_enabled: bool,
    pub first_batch_email_sent: bool,
    pub low_balance_notification_sent: bool,
    pub low_balance_threshold: Option<f32>,
    pub auto_topup_amount: Option<f32>,
    pub auto_topup_threshold: Option<f32>,
    pub auto_topup_monthly_limit: Option<f32>,
    pub user_type: String,
    pub verified: bool,
    pub zero_data_retention: bool,
    pub invoicing_enabled: bool,
    pub self_hosted_only: bool,
    pub toleration_preset: Option<String>,
}
