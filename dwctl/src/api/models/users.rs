
use super::pagination::Pagination;
use crate::api::models::groups::GroupResponse;
use crate::db::models::users::UserDBResponse;
use crate::types::{ApiKeyId, UserId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::rust::double_option;
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type, PartialEq, ToSchema)]
#[sqlx(type_name = "user_role", rename_all = "UPPERCASE")]
pub enum Role {
    PlatformManager,
    RequestViewer,
    StandardUser,
    BillingManager,
    BatchAPIUser,
    BackgroundInferenceUser,
    ConnectionsUser,
    QueueDemandReader,
    CatalogueReader,
    CatalogueReaderAll,
    OpenRouterPriceManager,
}

impl Role {
    pub fn is_service_role(&self) -> bool {
        matches!(
            self,
            Role::QueueDemandReader | Role::CatalogueReader | Role::CatalogueReaderAll | Role::OpenRouterPriceManager
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ToSchema)]
pub enum AssignableRole {
    PlatformManager,
    RequestViewer,
    StandardUser,
    BillingManager,
    BatchAPIUser,
    BackgroundInferenceUser,
    ConnectionsUser,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserCreate {
    #[schema(example = "jsmith")]
    pub username: String,
    #[schema(example = "john.smith@example.com")]
    pub email: String,
    #[schema(example = "John Smith")]
    pub display_name: Option<String>,
    #[schema(example = "https://example.com/avatars/jsmith.png")]
    pub avatar_url: Option<String>,
    #[schema(value_type = Vec<AssignableRole>)]
    pub roles: Vec<Role>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserUpdate {
    #[schema(example = "John Smith Jr.")]
    pub display_name: Option<String>,
    #[schema(example = "https://example.com/avatars/jsmith-new.png")]
    pub avatar_url: Option<String>,
    #[schema(value_type = Option<Vec<AssignableRole>>)]
    pub roles: Option<Vec<Role>>,
    pub batch_notifications_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub low_balance_threshold: Option<Option<f32>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub auto_topup_amount: Option<Option<f32>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub auto_topup_threshold: Option<Option<f32>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub auto_topup_monthly_limit: Option<Option<f32>>,
    pub zero_data_retention: Option<bool>,
    pub self_hosted_only: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub toleration_preset: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified: Option<bool>,
}

impl UserUpdate {
    pub fn is_enable_zdr_only(&self) -> bool {
        matches!(
            self,
            Self {
                zero_data_retention: Some(true),
                display_name: None,
                avatar_url: None,
                roles: None,
                batch_notifications_enabled: None,
                low_balance_threshold: None,
                auto_topup_amount: None,
                auto_topup_threshold: None,
                auto_topup_monthly_limit: None,
                self_hosted_only: None,
                toleration_preset: None,
                verified: None,
            }
        )
    }

    pub fn is_mark_verified_only(&self) -> bool {
        matches!(
            self,
            Self {
                verified: Some(true),
                zero_data_retention: None,
                display_name: None,
                avatar_url: None,
                roles: None,
                batch_notifications_enabled: None,
                low_balance_threshold: None,
                auto_topup_amount: None,
                auto_topup_threshold: None,
                auto_topup_monthly_limit: None,
                self_hosted_only: None,
                toleration_preset: None,
            }
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserResponse {
    #[schema(value_type = String, format = "uuid")]
    pub id: UserId,
    pub username: String,
    pub email: String,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub is_admin: bool,
    pub roles: Vec<Role>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_login: Option<DateTime<Utc>>,
    pub auth_source: String,
    pub external_user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(no_recursion)]
    pub groups: Option<Vec<GroupResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credit_balance: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_purchased: Option<bool>,
    pub has_payment_provider_id: bool,
    pub batch_notifications_enabled: bool,
    pub low_balance_threshold: Option<f32>,
    pub auto_topup_amount: Option<f32>,
    pub auto_topup_threshold: Option<f32>,
    pub has_auto_topup_payment_method: bool,
    pub auto_topup_monthly_limit: Option<f32>,
    pub user_type: String,
    pub verified: bool,
    pub zero_data_retention: bool,
    pub invoicing_enabled: bool,
    pub self_hosted_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub toleration_preset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organizations: Option<Vec<super::organizations::OrganizationSummary>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>, format = "uuid")]
    pub active_organization_id: Option<UserId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub onboarding_redirect_url: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ListUsersQuery {
    #[serde(flatten)]
    #[param(inline)]
    pub pagination: Pagination,

    pub include: Option<String>,

    pub search: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AuthMethod {
    #[default]
    NativeSession,
    ApiKey,
    PlatformApiKey,
    SystemApiKey,
    ServiceApiKey,
    TrustedSso,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CurrentUser {
    #[serde(skip)]
    pub auth_method: AuthMethod,
    #[schema(value_type = String, format = "uuid")]
    pub id: UserId,
    pub username: String,
    pub email: String,
    pub is_admin: bool,
    pub roles: Vec<Role>,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub payment_provider_id: Option<String>,
    pub organizations: Vec<UserOrganizationContext>,
    #[schema(value_type = Option<String>, format = "uuid")]
    pub active_organization: Option<UserId>,
    #[serde(skip)]
    pub api_key_id: Option<ApiKeyId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserOrganizationContext {
    #[schema(value_type = String, format = "uuid")]
    pub id: UserId,
    pub name: String,
    pub role: String,
    pub can_manage_keys: bool,
}

impl CurrentUser {
    pub fn with_auth_method(mut self, method: AuthMethod) -> Self {
        self.auth_method = method;
        if self.is_system() {
            return self;
        }
        if method == AuthMethod::ServiceApiKey {
            self.is_admin = false;
            self.roles.retain(Role::is_service_role);
            return self;
        }
        self.roles.retain(|role| !role.is_service_role());
        if method != AuthMethod::TrustedSso {
            self.is_admin = false;
            self.roles.retain(|role| match role {
                Role::PlatformManager => method == AuthMethod::PlatformApiKey,
                Role::BillingManager | Role::RequestViewer => false,
                _ => true,
            });
        }
        self
    }

    pub fn is_sso_platform_manager(&self) -> bool {
        self.auth_method == AuthMethod::TrustedSso && (self.is_admin || self.roles.contains(&Role::PlatformManager))
    }

    pub fn is_system(&self) -> bool {
        self.auth_method == AuthMethod::SystemApiKey && self.id.is_nil() && self.api_key_id == Some(uuid::Uuid::nil())
    }

    pub fn can_manage_platform(&self) -> bool {
        self.is_system()
            || self.is_sso_platform_manager()
            || (self.auth_method == AuthMethod::PlatformApiKey && self.roles.contains(&Role::PlatformManager))
    }

    #[cfg(test)]
    pub fn is_admin(&self) -> bool {
        self.is_admin
    }
}

impl From<UserDBResponse> for UserResponse {
    fn from(db: UserDBResponse) -> Self {
        Self {
            id: db.id,
            username: db.username,
            email: db.email,
            display_name: db.display_name,
            avatar_url: db.avatar_url,
            is_admin: db.is_admin,
            roles: db.roles,
            created_at: db.created_at,
            updated_at: db.updated_at,
            auth_source: db.auth_source,
            external_user_id: db.external_user_id,
            last_login: db.last_login,
            groups: None,
            credit_balance: None,
            has_purchased: None,
            has_payment_provider_id: db.payment_provider_id.as_ref().is_some_and(|s| !s.is_empty()),
            batch_notifications_enabled: db.batch_notifications_enabled,
            low_balance_threshold: db.low_balance_threshold,
            auto_topup_amount: db.auto_topup_amount,
            auto_topup_threshold: db.auto_topup_threshold,
            has_auto_topup_payment_method: db.payment_provider_id.as_ref().is_some_and(|s| !s.is_empty()),
            auto_topup_monthly_limit: db.auto_topup_monthly_limit,
            user_type: db.user_type,
            verified: db.verified,
            zero_data_retention: db.zero_data_retention,
            invoicing_enabled: db.invoicing_enabled,
            self_hosted_only: db.self_hosted_only,
            toleration_preset: None,
            organizations: None,
            active_organization_id: None,
            onboarding_redirect_url: None,
        }
    }
}

impl UserResponse {
    pub fn with_toleration_preset(mut self, preset: Option<String>) -> Self {
        self.toleration_preset = preset;
        self
    }

    pub fn with_groups(mut self, groups: Vec<GroupResponse>) -> Self {
        self.groups = Some(groups);
        self
    }

    pub fn with_credit_balance(mut self, balance: f64) -> Self {
        self.credit_balance = Some(balance);
        self
    }

    pub fn with_has_purchased(mut self, has_purchased: bool) -> Self {
        self.has_purchased = Some(has_purchased);
        self
    }

    pub fn with_organizations(mut self, organizations: Vec<super::organizations::OrganizationSummary>) -> Self {
        self.organizations = Some(organizations);
        self
    }

    pub fn with_active_organization(mut self, id: Option<UserId>) -> Self {
        self.active_organization_id = id;
        self
    }

    pub fn with_onboarding_redirect_url(mut self, url: String) -> Self {
        self.onboarding_redirect_url = Some(url);
        self
    }
}

impl From<UserDBResponse> for CurrentUser {
    fn from(db: UserDBResponse) -> Self {
        Self {
            auth_method: AuthMethod::NativeSession,
            id: db.id,
            username: db.username,
            email: db.email,
            is_admin: db.is_admin,
            roles: db.roles,
            display_name: db.display_name,
            avatar_url: db.avatar_url,
            payment_provider_id: db.payment_provider_id,
            organizations: vec![],
            active_organization: None,
            api_key_id: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetUserQuery {
    #[schema(example = "groups,billing")]
    pub include: Option<String>,
}
