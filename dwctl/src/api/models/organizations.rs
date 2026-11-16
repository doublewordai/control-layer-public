
use crate::api::models::pagination::Pagination;
use crate::api::models::users::UserResponse;

use crate::types::UserId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::rust::double_option;
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OrganizationCreate {
    #[schema(example = "acme.com")]
    pub name: String,
    #[schema(example = "admin@acme.com")]
    pub email: String,
    #[schema(example = "Acme Corporation")]
    pub display_name: Option<String>,
    #[schema(value_type = Option<String>, format = "uuid")]
    pub owner_id: Option<UserId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OrganizationUpdate {
    pub display_name: Option<String>,
    pub email: Option<String>,
    pub batch_notifications_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub low_balance_threshold: Option<Option<f32>>,
    pub zero_data_retention: Option<bool>,
    pub self_hosted_only: Option<bool>,
    pub auto_join_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disabled_modalities: Option<Vec<crate::modalities::Modality>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub toleration_preset: Option<Option<String>>,
}

impl OrganizationUpdate {
    pub fn is_enable_zdr_only(&self) -> bool {
        matches!(
            self,
            Self {
                zero_data_retention: Some(true),
                display_name: None,
                email: None,
                batch_notifications_enabled: None,
                low_balance_threshold: None,
                self_hosted_only: None,
                auto_join_enabled: None,
                disabled_modalities: None,
                toleration_preset: None,
            }
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OrganizationResponse {
    #[serde(flatten)]
    pub user: UserResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_email_change: Option<PendingEmailChangeResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_join_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disabled_modalities: Option<Vec<crate::modalities::Modality>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub toleration_preset: Option<String>,
}

impl OrganizationResponse {
    pub fn from_user(user: UserResponse) -> Self {
        Self {
            user,
            member_count: None,
            pending_email_change: None,
            auto_join_enabled: None,
            disabled_modalities: None,
            toleration_preset: None,
        }
    }

    pub fn with_member_count(mut self, count: i64) -> Self {
        self.member_count = Some(count);
        self
    }

    pub fn with_pending_email_change(mut self, info: PendingEmailChangeResponse) -> Self {
        self.pending_email_change = Some(info);
        self
    }

    pub fn with_auto_join_enabled(mut self, enabled: bool) -> Self {
        self.auto_join_enabled = Some(enabled);
        self
    }

    pub fn with_disabled_modalities(mut self, disabled: crate::modalities::ModalitySet) -> Self {
        self.disabled_modalities = Some(disabled.to_vec());
        self
    }

    pub fn with_toleration_preset(mut self, preset: Option<String>) -> Self {
        self.toleration_preset = preset;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PendingEmailChangeResponse {
    pub new_email: String,
    pub expires_at: DateTime<Utc>,
    pub new_email_confirmed_at: Option<DateTime<Utc>>,
    pub old_email_confirmed_at: Option<DateTime<Utc>>,
}

impl From<crate::db::models::organizations::PendingOrgEmailChangeDBResponse> for PendingEmailChangeResponse {
    fn from(p: crate::db::models::organizations::PendingOrgEmailChangeDBResponse) -> Self {
        Self {
            new_email: p.new_email,
            expires_at: p.expires_at,
            new_email_confirmed_at: p.new_email_confirmed_at,
            old_email_confirmed_at: p.old_email_confirmed_at,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema, Default)]
pub struct ApproveJoinRequestRequest {
    #[serde(default)]
    pub role: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OrganizationMemberResponse {
    #[schema(value_type = String, format = "uuid")]
    pub id: uuid::Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<UserResponse>,
    pub role: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub can_manage_keys: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invite_email: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InviteMemberRequest {
    #[schema(example = "newuser@example.com")]
    pub email: String,
    pub role: Option<String>,
    pub can_manage_keys: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InviteMemberResponse {
    #[schema(value_type = String, format = "uuid")]
    pub id: uuid::Uuid,
    pub email: String,
    pub role: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InviteDetailsResponse {
    pub org_name: String,
    pub role: String,
    pub inviter_name: Option<String>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AddMemberRequest {
    #[schema(value_type = String, format = "uuid")]
    pub user_id: UserId,
    pub role: Option<String>,
    pub can_manage_keys: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateMemberRoleRequest {
    pub role: String,
    pub can_manage_keys: Option<bool>,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ListOrganizationsQuery {
    #[serde(flatten)]
    #[param(inline)]
    pub pagination: Pagination,

    pub search: Option<String>,

    pub include: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SetActiveOrganizationRequest {
    #[schema(value_type = Option<String>, format = "uuid")]
    pub organization_id: Option<UserId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SetActiveOrganizationResponse {
    #[schema(value_type = Option<String>, format = "uuid")]
    pub active_organization_id: Option<UserId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OrganizationSummary {
    #[schema(value_type = String, format = "uuid")]
    pub id: UserId,
    pub name: String,
    pub role: String,
    pub zero_data_retention: bool,
    pub can_manage_keys: bool,
    pub verified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PendingJoinRequestResponse {
    #[schema(value_type = String, format = "uuid")]
    pub id: uuid::Uuid,
    #[schema(value_type = String, format = "uuid")]
    pub organization_id: UserId,
    pub organization_name: String,
    pub requested_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PendingInvitationResponse {
    #[schema(value_type = String, format = "uuid")]
    pub id: uuid::Uuid,
    #[schema(value_type = String, format = "uuid")]
    pub organization_id: UserId,
    pub organization_name: String,
    pub role: String,
    pub inviter_name: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DomainMatchResponse {
    #[schema(value_type = String, format = "uuid")]
    pub organization_id: UserId,
    #[schema(example = "acme.com")]
    pub domain: String,
    pub auto_join_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DomainJoinOutcome {
    Joined,
    Requested,
    Invited,
    AlreadyMember,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DomainJoinResponse {
    pub outcome: DomainJoinOutcome,
    #[schema(value_type = String, format = "uuid")]
    pub organization_id: UserId,
    pub join_request: Option<PendingJoinRequestResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OnboardingContextResponse {
    pub invitation: Option<PendingInvitationResponse>,
    pub domain_match: Option<DomainMatchResponse>,
    pub join_request: Option<PendingJoinRequestResponse>,
}
