
use chrono::{DateTime, Utc};
use sqlx::FromRow;
use uuid::Uuid;

use crate::types::UserId;

#[derive(Debug, Clone, FromRow)]
pub struct PasswordResetToken {
    pub id: Uuid,
    pub user_id: UserId,
    pub token_hash: String,
    pub expires_at: DateTime<Utc>,
    #[allow(dead_code)]
    pub created_at: DateTime<Utc>,
    pub used_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct PasswordResetTokenCreateRequest {
    pub user_id: UserId,
    pub raw_token: String,
    pub expires_at: DateTime<Utc>,
    pub argon2_params: crate::auth::password::Argon2Params,
}

#[derive(Debug, Clone)]
pub struct PasswordResetTokenUpdateRequest {
    pub used_at: Option<DateTime<Utc>>,
}

pub type PasswordResetTokenResponse = PasswordResetToken;

#[derive(Debug, Clone)]
pub struct PasswordResetTokenFilter {
    pub user_id: Option<UserId>,
    pub skip: i64,
    pub limit: i64,
}
