
use crate::api::models::users::Role;
use crate::db::{
    errors::{DbError, Result},
    handlers::users::{UserFilter, Users},
    models::{
        organizations::{
            OrganizationCreateDBRequest, OrganizationMemberDBResponse, OrganizationUpdateDBRequest, PendingOrgEmailChangeDBResponse,
        },
        users::UserDBResponse,
    },
};
use crate::types::{UserId, abbrev_uuid};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Acquire, FromRow, PgConnection};
use tracing::instrument;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct OrganizationFilter {
    pub skip: i64,
    pub limit: i64,
    pub search: Option<String>,
}

impl OrganizationFilter {
    pub fn new(skip: i64, limit: i64) -> Self {
        Self { skip, limit, search: None }
    }

    pub fn with_search(mut self, search: String) -> Self {
        self.search = Some(search);
        self
    }

    fn to_user_filter(&self) -> UserFilter {
        let filter = UserFilter::organizations(self.skip, self.limit);
        if let Some(ref search) = self.search {
            filter.with_search(search.clone())
        } else {
            filter
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
struct MemberRow {
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

impl From<MemberRow> for OrganizationMemberDBResponse {
    fn from(r: MemberRow) -> Self {
        Self {
            id: r.id,
            user_id: r.user_id,
            organization_id: r.organization_id,
            role: r.role,
            status: r.status,
            created_at: r.created_at,
            invite_email: r.invite_email,
            invited_by: r.invited_by,
            expires_at: r.expires_at,
        }
    }
}

pub struct Organizations<'c> {
    db: &'c mut PgConnection,
}

impl<'c> Organizations<'c> {
    pub fn new(db: &'c mut PgConnection) -> Self {
        Self { db }
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&id)), err)]
    pub async fn exists(&mut self, id: UserId) -> Result<bool> {
        let exists = sqlx::query_scalar!(
            "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1 AND user_type = 'organization' AND is_deleted = false) as \"exists!\"",
            id
        )
        .fetch_one(&mut *self.db)
        .await?;
        Ok(exists)
    }

    #[instrument(level = "debug", skip(self), fields(domain = %domain), err)]
    pub async fn find_by_domain(&mut self, domain: &str) -> Result<Option<UserDBResponse>> {
        if !crate::auth::utils::is_claimable_domain_shape(domain) {
            return Ok(None);
        }

        let row = sqlx::query!(
            r#"
            SELECT id, username, email, display_name, avatar_url, auth_source, created_at, updated_at,
                   is_admin, password_hash, external_user_id, payment_provider_id,
                   is_deleted, is_internal, batch_notifications_enabled, first_batch_email_sent,
                   low_balance_notification_sent, low_balance_threshold,
                   auto_topup_amount, auto_topup_threshold, auto_topup_monthly_limit, user_type, verified, invoicing_enabled, zero_data_retention,
                   self_hosted_only, toleration_preset
            FROM users
            WHERE (username = $1 OR username LIKE $1 || '~%')
              AND user_type = 'organization'
              -- A soft-deleted workspace must never receive join requests:
              -- nobody is left to approve them, so the requester would sit on a
              -- queue no one can see.
              AND is_deleted = false
              -- Same reasoning one level down: a workspace whose owners and
              -- admins have all been deleted is still `is_deleted = false`, but
              -- there is equally nobody home to approve anything.
              --
              -- `Users::delete` no longer creates these - it hands a workspace
              -- to a successor or closes it - but it used to walk away from the
              -- departing user's `user_organizations` rows, which stranded any
              -- workspace whose only owner was deleted: still claiming the
              -- domain, still offered to every colleague who signed up,
              -- unadministrable. This covers those legacy rows, and anything
              -- else that reaches the same state by a route we have not thought
              -- of. `users` inside the subquery is the outer row - the inner one
              -- is aliased.
              AND EXISTS (
                  SELECT 1
                  FROM user_organizations uo
                  JOIN users admin ON admin.id = uo.user_id
                  WHERE uo.organization_id = users.id
                    AND uo.role IN ('owner', 'admin')
                    AND uo.status = 'active'
                    AND admin.is_deleted = false
              )
            ORDER BY created_at ASC
            LIMIT 1
            "#,
            domain
        )
        .fetch_optional(&mut *self.db)
        .await?;

        match row {
            Some(r) => {
                let roles = sqlx::query_scalar!(r#"SELECT role as "role!: Role" FROM user_roles WHERE user_id = $1"#, r.id)
                    .fetch_all(&mut *self.db)
                    .await?;

                Ok(Some(UserDBResponse {
                    id: r.id,
                    username: r.username,
                    email: r.email,
                    display_name: r.display_name,
                    avatar_url: r.avatar_url,
                    created_at: r.created_at,
                    updated_at: r.updated_at,
                    last_login: None,
                    auth_source: r.auth_source,
                    is_admin: r.is_admin,
                    roles,
                    password_hash: r.password_hash,
                    external_user_id: r.external_user_id,
                    payment_provider_id: r.payment_provider_id,
                    batch_notifications_enabled: r.batch_notifications_enabled,
                    first_batch_email_sent: r.first_batch_email_sent,
                    low_balance_notification_sent: r.low_balance_notification_sent,
                    low_balance_threshold: r.low_balance_threshold,
                    auto_topup_amount: r.auto_topup_amount,
                    auto_topup_threshold: r.auto_topup_threshold,
                    auto_topup_monthly_limit: r.auto_topup_monthly_limit,
                    user_type: r.user_type,
                    verified: r.verified,
                    invoicing_enabled: r.invoicing_enabled,
                    zero_data_retention: r.zero_data_retention,
                    self_hosted_only: r.self_hosted_only,
                    toleration_preset: r.toleration_preset,
                }))
            }
            None => Ok(None),
        }
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id)), err)]
    pub async fn auto_join_enabled(&mut self, org_id: UserId) -> Result<bool> {
        let enabled = sqlx::query_scalar!(
            r#"
            SELECT auto_join_enabled
            FROM users
            WHERE id = $1 AND user_type = 'organization' AND is_deleted = false
            "#,
            org_id,
        )
        .fetch_optional(&mut *self.db)
        .await?;

        Ok(enabled.unwrap_or(false))
    }

    #[instrument(level = "debug", skip(self), fields(account_id = %abbrev_uuid(&account_id)), err)]
    pub async fn disabled_modalities(&mut self, account_id: UserId) -> Result<crate::modalities::ModalitySet> {
        let values = sqlx::query_scalar!(
            r#"
            SELECT disabled_modalities
            FROM users
            WHERE id = $1 AND is_deleted = false
            "#,
            account_id,
        )
        .fetch_optional(&mut *self.db)
        .await?;

        Ok(values.as_deref().map(crate::modalities::ModalitySet::from_db).unwrap_or_default())
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id), ?disabled), err)]
    pub async fn set_disabled_modalities(&mut self, org_id: UserId, disabled: &[crate::modalities::Modality]) -> Result<bool> {
        let values: Vec<String> = disabled.iter().map(|m| m.as_str().to_string()).collect();
        let result = sqlx::query!(
            r#"
            UPDATE users SET disabled_modalities = $2, updated_at = NOW()
            WHERE id = $1 AND user_type = 'organization' AND is_deleted = false
            "#,
            org_id,
            &values,
        )
        .execute(&mut *self.db)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id), enabled), err)]
    pub async fn set_auto_join_enabled(&mut self, org_id: UserId, enabled: bool) -> Result<bool> {
        let result = sqlx::query!(
            r#"
            UPDATE users SET auto_join_enabled = $2, updated_at = NOW()
            WHERE id = $1 AND user_type = 'organization' AND is_deleted = false
            "#,
            org_id,
            enabled,
        )
        .execute(&mut *self.db)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id)), err)]
    pub async fn members_can_create_keys(&mut self, org_id: UserId) -> Result<bool> {
        let enabled = sqlx::query_scalar!(
            r#"
            SELECT members_can_create_keys
            FROM users
            WHERE id = $1 AND user_type = 'organization' AND is_deleted = false
            "#,
            org_id,
        )
        .fetch_optional(&mut *self.db)
        .await?;

        Ok(enabled.unwrap_or(false))
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id), enabled), err)]
    pub async fn set_members_can_create_keys(&mut self, org_id: UserId, enabled: bool) -> Result<bool> {
        let result = sqlx::query!(
            r#"
            UPDATE users SET members_can_create_keys = $2, updated_at = NOW()
            WHERE id = $1 AND user_type = 'organization' AND is_deleted = false
            "#,
            org_id,
            enabled,
        )
        .execute(&mut *self.db)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[instrument(level = "debug", skip(self, request, default_roles), fields(name = %request.name), err)]
    pub async fn create(&mut self, request: &OrganizationCreateDBRequest, default_roles: &[Role]) -> Result<UserDBResponse> {
        let org_id = Uuid::new_v4();
        let mut tx = self.db.begin().await?;

        let row = sqlx::query!(
            r#"
            INSERT INTO users (id, username, email, display_name, avatar_url, auth_source, user_type, is_admin)
            VALUES ($1, $2, $3, $4, $5, 'organization', 'organization', false)
            RETURNING id, username, email, display_name, avatar_url, auth_source, created_at, updated_at,
                      is_admin, password_hash, external_user_id, payment_provider_id,
                      is_deleted, is_internal, batch_notifications_enabled, first_batch_email_sent,
                      low_balance_notification_sent, low_balance_threshold,
                      auto_topup_amount, auto_topup_threshold, auto_topup_monthly_limit, user_type, verified, invoicing_enabled, zero_data_retention,
                   self_hosted_only, toleration_preset
            "#,
            org_id,
            request.name,
            request.email,
            request.display_name,
            request.avatar_url,
        )
        .fetch_one(&mut *tx)
        .await?;

        let mut org_roles: Vec<Role> = default_roles.to_vec();
        if !org_roles.iter().any(|r| matches!(r, Role::StandardUser)) {
            org_roles.push(Role::StandardUser);
        }
        for role in &org_roles {
            sqlx::query!("INSERT INTO user_roles (user_id, role) VALUES ($1, $2)", org_id, role as &Role)
                .execute(&mut *tx)
                .await?;
        }

        sqlx::query!(
            "INSERT INTO user_organizations (user_id, organization_id, role, status) VALUES ($1, $2, 'owner', 'active')",
            request.created_by,
            org_id
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(UserDBResponse {
            id: row.id,
            username: row.username,
            email: row.email,
            display_name: row.display_name,
            avatar_url: row.avatar_url,
            created_at: row.created_at,
            updated_at: row.updated_at,
            last_login: None,
            auth_source: row.auth_source,
            is_admin: row.is_admin,
            roles: org_roles,
            password_hash: row.password_hash,
            external_user_id: row.external_user_id,
            payment_provider_id: row.payment_provider_id,
            batch_notifications_enabled: row.batch_notifications_enabled,
            first_batch_email_sent: row.first_batch_email_sent,
            low_balance_notification_sent: row.low_balance_notification_sent,
            low_balance_threshold: row.low_balance_threshold,
            auto_topup_amount: row.auto_topup_amount,
            auto_topup_threshold: row.auto_topup_threshold,
            auto_topup_monthly_limit: row.auto_topup_monthly_limit,
            user_type: row.user_type,
            verified: row.verified,
            invoicing_enabled: row.invoicing_enabled,
            zero_data_retention: row.zero_data_retention,
            self_hosted_only: row.self_hosted_only,
            toleration_preset: row.toleration_preset,
        })
    }

    #[instrument(level = "debug", skip(self, filter), fields(limit = filter.limit, skip = filter.skip), err)]
    pub async fn list(&mut self, filter: &OrganizationFilter) -> Result<Vec<UserDBResponse>> {
        use crate::db::handlers::repository::Repository;
        Users::new(self.db).list(&filter.to_user_filter()).await
    }

    #[instrument(level = "debug", skip(self, filter), fields(search = filter.search), err)]
    pub async fn count(&mut self, filter: &OrganizationFilter) -> Result<i64> {
        Users::new(self.db).count(&filter.to_user_filter()).await
    }

    #[instrument(level = "debug", skip(self, request), fields(org_id = %abbrev_uuid(&id)), err)]
    pub async fn update(&mut self, id: UserId, request: &OrganizationUpdateDBRequest) -> Result<UserDBResponse> {
        let row = sqlx::query!(
            r#"
            UPDATE users SET
                display_name = COALESCE($2, display_name),
                avatar_url = COALESCE($3, avatar_url),
                email = COALESCE($4, email),
                batch_notifications_enabled = COALESCE($5, batch_notifications_enabled),
                low_balance_threshold = CASE
                    WHEN $6::boolean THEN $7
                    ELSE low_balance_threshold
                END,
                low_balance_notification_sent = CASE
                    WHEN $6::boolean THEN false
                    ELSE low_balance_notification_sent
                END,
                zero_data_retention = COALESCE($8, zero_data_retention),
                self_hosted_only = COALESCE($9, self_hosted_only),
                toleration_preset = CASE
                    WHEN $10::boolean THEN $11
                    ELSE toleration_preset
                END,
                updated_at = NOW()
            WHERE id = $1 AND user_type = 'organization' AND is_deleted = false
            RETURNING id, username, email, display_name, avatar_url, auth_source, created_at, updated_at,
                      is_admin, password_hash, external_user_id, payment_provider_id,
                      batch_notifications_enabled, first_batch_email_sent,
                      low_balance_notification_sent, low_balance_threshold,
                      auto_topup_amount, auto_topup_threshold, auto_topup_monthly_limit, user_type, verified, invoicing_enabled, zero_data_retention,
                   self_hosted_only, toleration_preset
            "#,
            id,
            request.display_name,
            request.avatar_url,
            request.email,
            request.batch_notifications_enabled,
            request.low_balance_threshold.is_some() as bool,
            request.low_balance_threshold.flatten(),
            request.zero_data_retention,
            request.self_hosted_only,
            request.toleration_preset.is_some() as bool,
            request.toleration_preset.clone().flatten(),
        )
        .fetch_optional(&mut *self.db)
        .await?
        .ok_or(DbError::NotFound)?;

        let roles: Vec<Role> = sqlx::query_scalar!(r#"SELECT role as "role: Role" FROM user_roles WHERE user_id = $1"#, id)
            .fetch_all(&mut *self.db)
            .await?;

        Ok(UserDBResponse {
            id: row.id,
            username: row.username,
            email: row.email,
            display_name: row.display_name,
            avatar_url: row.avatar_url,
            created_at: row.created_at,
            updated_at: row.updated_at,
            last_login: None,
            auth_source: row.auth_source,
            is_admin: row.is_admin,
            roles,
            password_hash: row.password_hash,
            external_user_id: row.external_user_id,
            payment_provider_id: row.payment_provider_id,
            batch_notifications_enabled: row.batch_notifications_enabled,
            first_batch_email_sent: row.first_batch_email_sent,
            low_balance_notification_sent: row.low_balance_notification_sent,
            low_balance_threshold: row.low_balance_threshold,
            auto_topup_amount: row.auto_topup_amount,
            auto_topup_threshold: row.auto_topup_threshold,
            auto_topup_monthly_limit: row.auto_topup_monthly_limit,
            user_type: row.user_type,
            verified: row.verified,
            invoicing_enabled: row.invoicing_enabled,
            zero_data_retention: row.zero_data_retention,
            self_hosted_only: row.self_hosted_only,
            toleration_preset: row.toleration_preset,
        })
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&id)), err)]
    pub async fn delete(&mut self, id: UserId) -> Result<bool> {
        let scrubbed_email = format!("deleted-{}@deleted.local", id);
        let scrubbed_username = format!("deleted-{}", id);

        let result = sqlx::query!(
            r#"
            UPDATE users
            SET
                email = $1,
                username = $2,
                display_name = NULL,
                avatar_url = NULL,
                is_deleted = true,
                updated_at = NOW()
            WHERE id = $3 AND user_type = 'organization' AND is_deleted = false
            "#,
            scrubbed_email,
            scrubbed_username,
            id
        )
        .execute(&mut *self.db)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn list_member_organization_ids(&mut self, user_id: UserId) -> Result<Vec<UserId>> {
        Ok(sqlx::query_scalar!(
            r#"SELECT organization_id FROM user_organizations WHERE user_id = $1 AND status = 'active'"#,
            user_id
        )
        .fetch_all(&mut *self.db)
        .await?)
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id), user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn add_member(&mut self, org_id: UserId, user_id: UserId, role: &str) -> Result<OrganizationMemberDBResponse> {
        let row = sqlx::query_as!(
            MemberRow,
            r#"
            INSERT INTO user_organizations (user_id, organization_id, role, status)
            VALUES ($1, $2, $3, 'active')
            RETURNING id, user_id, organization_id, role, status, created_at,
                      invite_email, invited_by, expires_at
            "#,
            user_id,
            org_id,
            role,
        )
        .fetch_one(&mut *self.db)
        .await?;

        Ok(row.into())
    }

    #[instrument(level = "debug", skip(self), fields(membership_id = %abbrev_uuid(&membership_id)), err)]
    pub async fn set_membership_manage_keys(&mut self, membership_id: Uuid, granted: bool) -> Result<()> {
        if granted {
            sqlx::query!(
                "INSERT INTO organization_member_roles (user_organization_id, role) VALUES ($1, 'manage_keys') ON CONFLICT DO NOTHING",
                membership_id
            )
            .execute(&mut *self.db)
            .await?;
        } else {
            sqlx::query!(
                "DELETE FROM organization_member_roles WHERE user_organization_id = $1 AND role = 'manage_keys'",
                membership_id
            )
            .execute(&mut *self.db)
            .await?;
        }
        Ok(())
    }

    #[instrument(level = "debug", skip(self), fields(membership_id = %abbrev_uuid(&membership_id)), err)]
    pub async fn grant_default_member_roles(&mut self, membership_id: Uuid) -> Result<bool> {
        let result = sqlx::query!(
            r#"
            INSERT INTO organization_member_roles (user_organization_id, role)
            SELECT uo.id, 'manage_keys'
            FROM user_organizations uo
            JOIN users o ON o.id = uo.organization_id
            WHERE uo.id = $1
              AND uo.role = 'member'
              AND o.members_can_create_keys
            ON CONFLICT DO NOTHING
            "#,
            membership_id
        )
        .execute(&mut *self.db)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    #[instrument(level = "debug", skip(self), fields(membership_id = %abbrev_uuid(&membership_id)), err)]
    pub async fn membership_has_manage_keys(&mut self, membership_id: Uuid) -> Result<bool> {
        let exists = sqlx::query_scalar!(
            r#"SELECT EXISTS(SELECT 1 FROM organization_member_roles WHERE user_organization_id = $1 AND role = 'manage_keys') AS "exists!""#,
            membership_id
        )
        .fetch_one(&mut *self.db)
        .await?;
        Ok(exists)
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id)), err)]
    pub async fn list_manage_keys_membership_ids(&mut self, org_id: UserId) -> Result<std::collections::HashSet<Uuid>> {
        let ids = sqlx::query_scalar!(
            r#"
            SELECT omr.user_organization_id
            FROM organization_member_roles omr
            JOIN user_organizations uo ON uo.id = omr.user_organization_id
            WHERE uo.organization_id = $1 AND omr.role = 'manage_keys'
            "#,
            org_id
        )
        .fetch_all(&mut *self.db)
        .await?;
        Ok(ids.into_iter().collect())
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn user_manage_keys_org_ids(&mut self, user_id: UserId) -> Result<std::collections::HashSet<UserId>> {
        let ids = sqlx::query_scalar!(
            r#"
            SELECT uo.organization_id
            FROM user_organizations uo
            JOIN organization_member_roles omr ON omr.user_organization_id = uo.id
            WHERE uo.user_id = $1 AND omr.role = 'manage_keys'
            "#,
            user_id
        )
        .fetch_all(&mut *self.db)
        .await?;
        Ok(ids.into_iter().collect())
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id), user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn remove_member(&mut self, org_id: UserId, user_id: UserId) -> Result<bool> {
        let result = sqlx::query!(
            "DELETE FROM user_organizations WHERE user_id = $1 AND organization_id = $2",
            user_id,
            org_id
        )
        .execute(&mut *self.db)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id), user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn update_member_role(&mut self, org_id: UserId, user_id: UserId, role: &str) -> Result<OrganizationMemberDBResponse> {
        let row = sqlx::query_as!(
            MemberRow,
            r#"
            UPDATE user_organizations SET role = $3
            WHERE user_id = $1 AND organization_id = $2 AND status = 'active'
            RETURNING id, user_id, organization_id, role, status, created_at,
                      invite_email, invited_by, expires_at
            "#,
            user_id,
            org_id,
            role,
        )
        .fetch_optional(&mut *self.db)
        .await?
        .ok_or(DbError::NotFound)?;

        Ok(row.into())
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id)), err)]
    pub async fn list_members(&mut self, org_id: UserId) -> Result<Vec<OrganizationMemberDBResponse>> {
        let rows = sqlx::query_as!(
            MemberRow,
            r#"
            SELECT uo.id, uo.user_id, uo.organization_id, uo.role, uo.status,
                   uo.created_at, uo.invite_email, uo.invited_by, uo.expires_at
            FROM user_organizations uo
            LEFT JOIN users u ON u.id = uo.user_id
            WHERE uo.organization_id = $1
              AND uo.status <> 'requested'
              AND (uo.user_id IS NULL OR u.is_deleted = false)
            ORDER BY uo.status ASC, uo.created_at ASC
            "#,
            org_id
        )
        .fetch_all(&mut *self.db)
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id), user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn create_join_request(&mut self, org_id: UserId, user_id: UserId) -> Result<OrganizationMemberDBResponse> {
        let inserted = sqlx::query_as!(
            MemberRow,
            r#"
            INSERT INTO user_organizations (user_id, organization_id, role, status)
            VALUES ($1, $2, 'member', 'requested')
            ON CONFLICT (user_id, organization_id) DO NOTHING
            RETURNING id, user_id, organization_id, role, status, created_at,
                      invite_email, invited_by, expires_at
            "#,
            user_id,
            org_id,
        )
        .fetch_optional(&mut *self.db)
        .await?;

        if let Some(row) = inserted {
            return Ok(row.into());
        }

        let existing = sqlx::query_as!(
            MemberRow,
            r#"
            SELECT id, user_id, organization_id, role, status, created_at,
                   invite_email, invited_by, expires_at
            FROM user_organizations
            WHERE user_id = $1 AND organization_id = $2
            "#,
            user_id,
            org_id,
        )
        .fetch_optional(&mut *self.db)
        .await?
        .ok_or(DbError::NotFound)?;

        Ok(existing.into())
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn list_user_join_requests(&mut self, user_id: UserId) -> Result<Vec<OrganizationMemberDBResponse>> {
        let rows = sqlx::query_as!(
            MemberRow,
            r#"
            SELECT uo.id, uo.user_id, uo.organization_id, uo.role, uo.status,
                   uo.created_at, uo.invite_email, uo.invited_by, uo.expires_at
            FROM user_organizations uo
            INNER JOIN users o ON o.id = uo.organization_id
            WHERE uo.user_id = $1
              AND uo.status = 'requested'
              AND o.is_deleted = false
            ORDER BY uo.created_at ASC
            "#,
            user_id
        )
        .fetch_all(&mut *self.db)
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id)), err)]
    pub async fn list_join_requests(&mut self, org_id: UserId) -> Result<Vec<OrganizationMemberDBResponse>> {
        let rows = sqlx::query_as!(
            MemberRow,
            r#"
            SELECT uo.id, uo.user_id, uo.organization_id, uo.role, uo.status,
                   uo.created_at, uo.invite_email, uo.invited_by, uo.expires_at
            FROM user_organizations uo
            INNER JOIN users u ON u.id = uo.user_id
            WHERE uo.organization_id = $1
              AND uo.status = 'requested'
              AND u.is_deleted = false
            ORDER BY uo.created_at ASC
            "#,
            org_id
        )
        .fetch_all(&mut *self.db)
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id), join_request_id = %abbrev_uuid(&request_id)), err)]
    pub async fn approve_join_request(&mut self, org_id: UserId, request_id: Uuid, role: &str) -> Result<Option<UserId>> {
        let approved = sqlx::query_scalar!(
            r#"
            UPDATE user_organizations
            SET status = 'active', role = $3
            WHERE id = $1 AND organization_id = $2 AND status = 'requested'
            RETURNING user_id
            "#,
            request_id,
            org_id,
            role,
        )
        .fetch_optional(&mut *self.db)
        .await?;

        Ok(approved.flatten())
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id)), err)]
    pub async fn list_admin_emails(&mut self, org_id: UserId) -> Result<Vec<String>> {
        let emails = sqlx::query_scalar!(
            r#"
            SELECT u.email
            FROM user_organizations uo
            JOIN users u ON u.id = uo.user_id
            WHERE uo.organization_id = $1
              AND uo.status = 'active'
              AND uo.role IN ('owner', 'admin')
              AND u.is_deleted = false
            ORDER BY u.email
            "#,
            org_id,
        )
        .fetch_all(&mut *self.db)
        .await?;

        Ok(emails)
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id), join_request_id = %abbrev_uuid(&request_id)), err)]
    pub async fn decline_join_request(&mut self, org_id: UserId, request_id: Uuid) -> Result<bool> {
        let result = sqlx::query!(
            "DELETE FROM user_organizations WHERE id = $1 AND organization_id = $2 AND status = 'requested'",
            request_id,
            org_id,
        )
        .execute(&mut *self.db)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn list_user_organizations(&mut self, user_id: UserId) -> Result<Vec<OrganizationMemberDBResponse>> {
        let rows = sqlx::query_as!(
            MemberRow,
            r#"
            SELECT uo.id, uo.user_id, uo.organization_id, uo.role, uo.status,
                   uo.created_at, uo.invite_email, uo.invited_by, uo.expires_at
            FROM user_organizations uo
            INNER JOIN users u ON u.id = uo.organization_id
            WHERE uo.user_id = $1 AND uo.status = 'active' AND u.is_deleted = false
            ORDER BY uo.created_at ASC
            "#,
            user_id
        )
        .fetch_all(&mut *self.db)
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn count_user_organizations(&mut self, user_id: UserId) -> Result<i64> {
        let count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM user_organizations uo
            INNER JOIN users u ON u.id = uo.organization_id
            WHERE uo.user_id = $1 AND uo.status = 'active' AND u.is_deleted = false
            "#,
            user_id
        )
        .fetch_one(&mut *self.db)
        .await?;

        Ok(count)
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id), org_id = %abbrev_uuid(&org_id)), err)]
    pub async fn get_user_org_role(&mut self, user_id: UserId, org_id: UserId) -> Result<Option<String>> {
        let row = sqlx::query_scalar!(
            "SELECT role FROM user_organizations WHERE user_id = $1 AND organization_id = $2 AND status = 'active'",
            user_id,
            org_id
        )
        .fetch_optional(&mut *self.db)
        .await?;

        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip(self, token_hash), fields(org_id = %abbrev_uuid(&org_id), invite_email = %invite_email), err)]
    pub async fn create_invite(
        &mut self,
        org_id: UserId,
        user_id: Option<UserId>,
        invite_email: &str,
        role: &str,
        invited_by: UserId,
        token_hash: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<OrganizationMemberDBResponse> {
        let row = sqlx::query_as!(
            MemberRow,
            r#"
            INSERT INTO user_organizations (user_id, organization_id, role, status, invite_email, invited_by, invite_token_hash, expires_at)
            VALUES ($1, $2, $3, 'pending', $4, $5, $6, $7)
            RETURNING id, user_id, organization_id, role, status, created_at,
                      invite_email, invited_by, expires_at
            "#,
            user_id,
            org_id,
            role,
            invite_email,
            invited_by,
            token_hash,
            expires_at,
        )
        .fetch_one(&mut *self.db)
        .await?;

        Ok(row.into())
    }

    #[instrument(level = "debug", skip(self), err)]
    pub async fn find_pending_invite_for_email(&mut self, email: &str) -> Result<Option<OrganizationMemberDBResponse>> {
        let row = sqlx::query_as!(
            MemberRow,
            r#"
            SELECT uo.id, uo.user_id, uo.organization_id, uo.role, uo.status,
                   uo.created_at, uo.invite_email, uo.invited_by, uo.expires_at
            FROM user_organizations uo
            INNER JOIN users o ON o.id = uo.organization_id
            WHERE LOWER(uo.invite_email) = LOWER($1)
              AND uo.status = 'pending'
              AND o.is_deleted = false
              AND (uo.expires_at IS NULL OR uo.expires_at > NOW())
            ORDER BY uo.created_at ASC
            LIMIT 1
            "#,
            email,
        )
        .fetch_optional(&mut *self.db)
        .await?;

        Ok(row.map(Into::into))
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id), invite_id = %abbrev_uuid(&invite_id)), err)]
    pub async fn find_invite_by_id(&mut self, org_id: UserId, invite_id: Uuid) -> Result<Option<OrganizationMemberDBResponse>> {
        let row = sqlx::query_as!(
            MemberRow,
            r#"
            SELECT id, user_id, organization_id, role, status, created_at,
                   invite_email, invited_by, expires_at
            FROM user_organizations
            WHERE id = $1 AND organization_id = $2 AND status = 'pending'
            "#,
            invite_id,
            org_id,
        )
        .fetch_optional(&mut *self.db)
        .await?;

        Ok(row.map(Into::into))
    }

    #[instrument(level = "debug", skip(self, token_hash), err)]
    pub async fn find_invite_by_token_hash(&mut self, token_hash: &str) -> Result<Option<OrganizationMemberDBResponse>> {
        let row = sqlx::query_as!(
            MemberRow,
            r#"
            SELECT id, user_id, organization_id, role, status, created_at,
                   invite_email, invited_by, expires_at
            FROM user_organizations
            WHERE invite_token_hash = $1 AND status = 'pending'
            "#,
            token_hash,
        )
        .fetch_optional(&mut *self.db)
        .await?;

        Ok(row.map(Into::into))
    }

    #[instrument(level = "debug", skip(self), fields(invite_id = %abbrev_uuid(&invite_id), user_id = %abbrev_uuid(&user_id)), err(level = "warn"))]
    pub async fn accept_invite(&mut self, invite_id: UserId, user_id: UserId) -> Result<OrganizationMemberDBResponse> {
        let row = sqlx::query_as!(
            MemberRow,
            r#"
            UPDATE user_organizations
            SET status = 'active', user_id = $2, invite_token_hash = NULL
            WHERE id = $1 AND status = 'pending'
            RETURNING id, user_id, organization_id, role, status, created_at,
                      invite_email, invited_by, expires_at
            "#,
            invite_id,
            user_id,
        )
        .fetch_optional(&mut *self.db)
        .await?
        .ok_or(DbError::NotFound)?;

        Ok(row.into())
    }

    #[instrument(level = "debug", skip(self, token_hash), fields(org_id = %abbrev_uuid(&org_id), invite_id = %abbrev_uuid(&invite_id)), err)]
    pub async fn refresh_invite_token(
        &mut self,
        org_id: UserId,
        invite_id: Uuid,
        token_hash: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<Option<(String, String)>> {
        let row = sqlx::query!(
            r#"
            UPDATE user_organizations
            SET invite_token_hash = $3, expires_at = $4
            WHERE id = $1 AND organization_id = $2 AND status = 'pending'
            RETURNING invite_email, role
            "#,
            invite_id,
            org_id,
            token_hash,
            expires_at,
        )
        .fetch_optional(&mut *self.db)
        .await?;

        Ok(row.and_then(|r| r.invite_email.map(|email| (email, r.role))))
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id), invite_id = %abbrev_uuid(&invite_id)), err)]
    pub async fn cancel_invite(&mut self, org_id: UserId, invite_id: UserId) -> Result<bool> {
        let result = sqlx::query!(
            "DELETE FROM user_organizations WHERE id = $1 AND organization_id = $2 AND status = 'pending'",
            invite_id,
            org_id
        )
        .execute(&mut *self.db)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[instrument(level = "debug", skip(self), fields(org_id = %abbrev_uuid(&org_id)), err)]
    pub async fn find_pending_email_change_for_org(&mut self, org_id: UserId) -> Result<Option<PendingOrgEmailChangeDBResponse>> {
        let row = sqlx::query!(
            r#"
            SELECT id, organization_id, new_email, requested_by,
                   new_email_confirmed_at, old_email_confirmed_at,
                   created_at, expires_at
            FROM pending_org_email_changes
            WHERE organization_id = $1
            "#,
            org_id,
        )
        .fetch_optional(&mut *self.db)
        .await?;

        Ok(row.map(|r| PendingOrgEmailChangeDBResponse {
            id: r.id,
            organization_id: r.organization_id,
            new_email: r.new_email,
            requested_by: r.requested_by,
            new_email_confirmed_at: r.new_email_confirmed_at,
            old_email_confirmed_at: r.old_email_confirmed_at,
            created_at: r.created_at,
            expires_at: r.expires_at,
        }))
    }

    #[instrument(
        level = "debug",
        skip(self, new_email_token_hash, old_email_token_hash),
        fields(org_id = %abbrev_uuid(&org_id)),
        err,
    )]
    pub async fn upsert_pending_email_change(
        &mut self,
        org_id: UserId,
        new_email: &str,
        requested_by: UserId,
        new_email_token_hash: &str,
        old_email_token_hash: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<PendingOrgEmailChangeDBResponse> {
        let row = sqlx::query!(
            r#"
            INSERT INTO pending_org_email_changes
                (organization_id, new_email, requested_by, new_email_token_hash, old_email_token_hash, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (organization_id) DO UPDATE SET
                new_email = EXCLUDED.new_email,
                requested_by = EXCLUDED.requested_by,
                new_email_token_hash = EXCLUDED.new_email_token_hash,
                old_email_token_hash = EXCLUDED.old_email_token_hash,
                new_email_confirmed_at = NULL,
                old_email_confirmed_at = NULL,
                expires_at = EXCLUDED.expires_at,
                created_at = NOW()
            RETURNING id, organization_id, new_email, requested_by,
                      new_email_confirmed_at, old_email_confirmed_at,
                      created_at, expires_at
            "#,
            org_id,
            new_email,
            requested_by,
            new_email_token_hash,
            old_email_token_hash,
            expires_at,
        )
        .fetch_one(&mut *self.db)
        .await?;

        Ok(PendingOrgEmailChangeDBResponse {
            id: row.id,
            organization_id: row.organization_id,
            new_email: row.new_email,
            requested_by: row.requested_by,
            new_email_confirmed_at: row.new_email_confirmed_at,
            old_email_confirmed_at: row.old_email_confirmed_at,
            created_at: row.created_at,
            expires_at: row.expires_at,
        })
    }

    #[instrument(level = "debug", skip(self, token_hash), err)]
    pub async fn confirm_new_email_side(&mut self, token_hash: &str) -> Result<Option<PendingOrgEmailChangeDBResponse>> {
        let row = sqlx::query!(
            r#"
            UPDATE pending_org_email_changes p
            SET new_email_confirmed_at = NOW()
            FROM users u
            WHERE p.new_email_token_hash = $1
              AND p.new_email_confirmed_at IS NULL
              AND p.expires_at > NOW()
              AND p.organization_id = u.id
              AND u.is_deleted = false
            RETURNING p.id, p.organization_id, p.new_email, p.requested_by,
                      p.new_email_confirmed_at, p.old_email_confirmed_at,
                      p.created_at, p.expires_at
            "#,
            token_hash,
        )
        .fetch_optional(&mut *self.db)
        .await?;

        Ok(row.map(|r| PendingOrgEmailChangeDBResponse {
            id: r.id,
            organization_id: r.organization_id,
            new_email: r.new_email,
            requested_by: r.requested_by,
            new_email_confirmed_at: r.new_email_confirmed_at,
            old_email_confirmed_at: r.old_email_confirmed_at,
            created_at: r.created_at,
            expires_at: r.expires_at,
        }))
    }

    #[instrument(level = "debug", skip(self, token_hash), err)]
    pub async fn confirm_old_email_side(&mut self, token_hash: &str) -> Result<Option<PendingOrgEmailChangeDBResponse>> {
        let row = sqlx::query!(
            r#"
            UPDATE pending_org_email_changes p
            SET old_email_confirmed_at = NOW()
            FROM users u
            WHERE p.old_email_token_hash = $1
              AND p.old_email_confirmed_at IS NULL
              AND p.expires_at > NOW()
              AND p.organization_id = u.id
              AND u.is_deleted = false
            RETURNING p.id, p.organization_id, p.new_email, p.requested_by,
                      p.new_email_confirmed_at, p.old_email_confirmed_at,
                      p.created_at, p.expires_at
            "#,
            token_hash,
        )
        .fetch_optional(&mut *self.db)
        .await?;

        Ok(row.map(|r| PendingOrgEmailChangeDBResponse {
            id: r.id,
            organization_id: r.organization_id,
            new_email: r.new_email,
            requested_by: r.requested_by,
            new_email_confirmed_at: r.new_email_confirmed_at,
            old_email_confirmed_at: r.old_email_confirmed_at,
            created_at: r.created_at,
            expires_at: r.expires_at,
        }))
    }

    #[instrument(level = "debug", skip(self), err)]
    pub async fn delete_pending_email_change(&mut self, id: Uuid) -> Result<bool> {
        let result = sqlx::query!("DELETE FROM pending_org_email_changes WHERE id = $1", id)
            .execute(&mut *self.db)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::models::users::{Role, UserCreate};
    use crate::db::handlers::repository::Repository;
    use crate::db::handlers::users::Users;
    use crate::db::models::organizations::{OrganizationCreateDBRequest, OrganizationUpdateDBRequest};
    use crate::db::models::users::UserCreateDBRequest;
    use sqlx::PgPool;

    const TEST_DEFAULT_ROLES: &[Role] = &[Role::StandardUser, Role::BatchAPIUser];

    async fn create_individual(pool: &PgPool, username: &str, email: &str) -> UserId {
        let mut conn = pool.acquire().await.unwrap();
        let mut repo = Users::new(&mut conn);
        let user = repo
            .create(&UserCreateDBRequest::from(UserCreate {
                username: username.to_string(),
                email: email.to_string(),
                display_name: Some(format!("User {username}")),
                avatar_url: None,
                roles: vec![Role::StandardUser],
            }))
            .await
            .unwrap();
        user.id
    }


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_organization(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "billing@acme.example.com".to_string(),
                    display_name: Some("Acme Corporation".to_string()),
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        assert_eq!(org.username, "acme-corp");
        assert_eq!(org.email, "billing@acme.example.com");
        assert_eq!(org.display_name.as_deref(), Some("Acme Corporation"));
        assert_eq!(org.user_type, "organization");
        assert_eq!(org.auth_source, "organization");
        assert!(!org.is_admin);

        let mut persisted_roles: Vec<String> =
            sqlx::query_scalar!(r#"SELECT role::text as "role!" FROM user_roles WHERE user_id = $1"#, org.id)
                .fetch_all(&pool)
                .await
                .unwrap();
        persisted_roles.sort();
        assert_eq!(persisted_roles, vec!["BATCHAPIUSER", "STANDARDUSER"]);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_organization_adds_creator_as_owner(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "billing@acme.example.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        let role = orgs.get_user_org_role(creator, org.id).await.unwrap();
        assert_eq!(role, Some("owner".to_string()));

        let members = orgs.list_members(org.id).await.unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].user_id, Some(creator));
        assert_eq!(members[0].role, "owner");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_organizations(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        orgs.create(
            &OrganizationCreateDBRequest {
                name: "acme-corp".to_string(),
                email: "billing@acme.example.com".to_string(),
                display_name: Some("Acme Corporation".to_string()),
                avatar_url: None,
                created_by: creator,
            },
            TEST_DEFAULT_ROLES,
        )
        .await
        .unwrap();

        orgs.create(
            &OrganizationCreateDBRequest {
                name: "globex-inc".to_string(),
                email: "info@globex.example.com".to_string(),
                display_name: Some("Globex Inc".to_string()),
                avatar_url: None,
                created_by: creator,
            },
            TEST_DEFAULT_ROLES,
        )
        .await
        .unwrap();

        let filter = OrganizationFilter::new(0, 100);
        let list = orgs.list(&filter).await.unwrap();
        assert_eq!(list.len(), 2);
        for o in &list {
            assert_eq!(o.user_type, "organization");
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_organizations_with_search(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        orgs.create(
            &OrganizationCreateDBRequest {
                name: "acme-corp".to_string(),
                email: "billing@acme.example.com".to_string(),
                display_name: Some("Acme Corporation".to_string()),
                avatar_url: None,
                created_by: creator,
            },
            TEST_DEFAULT_ROLES,
        )
        .await
        .unwrap();

        orgs.create(
            &OrganizationCreateDBRequest {
                name: "globex-inc".to_string(),
                email: "info@globex.example.com".to_string(),
                display_name: Some("Globex Inc".to_string()),
                avatar_url: None,
                created_by: creator,
            },
            TEST_DEFAULT_ROLES,
        )
        .await
        .unwrap();

        let filter = OrganizationFilter::new(0, 100).with_search("acme".to_string());
        let list = orgs.list(&filter).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].username, "acme-corp");

        let filter = OrganizationFilter::new(0, 100).with_search("globex".to_string());
        let list = orgs.list(&filter).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].username, "globex-inc");

        let filter = OrganizationFilter::new(0, 100).with_search("nonexistent".to_string());
        let list = orgs.list(&filter).await.unwrap();
        assert!(list.is_empty());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_count_organizations(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let filter = OrganizationFilter::new(0, 100);
        assert_eq!(orgs.count(&filter).await.unwrap(), 0);

        orgs.create(
            &OrganizationCreateDBRequest {
                name: "acme-corp".to_string(),
                email: "billing@acme.example.com".to_string(),
                display_name: None,
                avatar_url: None,
                created_by: creator,
            },
            TEST_DEFAULT_ROLES,
        )
        .await
        .unwrap();

        assert_eq!(orgs.count(&filter).await.unwrap(), 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_update_organization(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "old@acme.example.com".to_string(),
                    display_name: Some("Old Name".to_string()),
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        let updated = orgs
            .update(
                org.id,
                &OrganizationUpdateDBRequest {
                    display_name: Some("New Acme Name".to_string()),
                    avatar_url: None,
                    email: Some("new@acme.example.com".to_string()),
                    batch_notifications_enabled: None,
                    low_balance_threshold: None,
                    zero_data_retention: None,
                    self_hosted_only: None,
                    toleration_preset: Default::default(),
                },
            )
            .await
            .unwrap();

        assert_eq!(updated.display_name.as_deref(), Some("New Acme Name"));
        assert_eq!(updated.email, "new@acme.example.com");
        assert_eq!(updated.user_type, "organization");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_update_organization_partial(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "billing@acme.example.com".to_string(),
                    display_name: Some("Acme Corporation".to_string()),
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        let updated = orgs
            .update(
                org.id,
                &OrganizationUpdateDBRequest {
                    display_name: None,
                    avatar_url: None,
                    email: Some("new@acme.example.com".to_string()),
                    batch_notifications_enabled: None,
                    low_balance_threshold: None,
                    zero_data_retention: None,
                    self_hosted_only: None,
                    toleration_preset: Default::default(),
                },
            )
            .await
            .unwrap();

        assert_eq!(updated.display_name.as_deref(), Some("Acme Corporation"));
        assert_eq!(updated.email, "new@acme.example.com");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_update_organization_notification_settings(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "billing@acme.example.com".to_string(),
                    display_name: Some("Acme Corporation".to_string()),
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        assert!(!org.batch_notifications_enabled);
        assert!(org.low_balance_threshold.is_none());

        let updated = orgs
            .update(
                org.id,
                &OrganizationUpdateDBRequest {
                    display_name: None,
                    avatar_url: None,
                    email: None,
                    batch_notifications_enabled: Some(true),
                    low_balance_threshold: Some(Some(10.0)),
                    zero_data_retention: None,
                    self_hosted_only: None,
                    toleration_preset: Default::default(),
                },
            )
            .await
            .unwrap();

        assert!(updated.batch_notifications_enabled);
        assert_eq!(updated.low_balance_threshold, Some(10.0));
        assert!(!updated.low_balance_notification_sent);

        let updated = orgs
            .update(
                org.id,
                &OrganizationUpdateDBRequest {
                    display_name: None,
                    avatar_url: None,
                    email: None,
                    batch_notifications_enabled: None,
                    low_balance_threshold: Some(Some(25.0)),
                    zero_data_retention: None,
                    self_hosted_only: None,
                    toleration_preset: Default::default(),
                },
            )
            .await
            .unwrap();

        assert!(updated.batch_notifications_enabled);
        assert_eq!(updated.low_balance_threshold, Some(25.0));
        assert!(!updated.low_balance_notification_sent);

        let updated = orgs
            .update(
                org.id,
                &OrganizationUpdateDBRequest {
                    display_name: None,
                    avatar_url: None,
                    email: None,
                    batch_notifications_enabled: None,
                    low_balance_threshold: Some(None),
                    zero_data_retention: None,
                    self_hosted_only: None,
                    toleration_preset: Default::default(),
                },
            )
            .await
            .unwrap();

        assert!(updated.batch_notifications_enabled);
        assert!(updated.low_balance_threshold.is_none());

        let updated = orgs
            .update(
                org.id,
                &OrganizationUpdateDBRequest {
                    display_name: None,
                    avatar_url: None,
                    email: None,
                    batch_notifications_enabled: None,
                    low_balance_threshold: None,
                    zero_data_retention: None,
                    self_hosted_only: None,
                    toleration_preset: Default::default(),
                },
            )
            .await
            .unwrap();

        assert!(updated.low_balance_threshold.is_none());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_organization(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "billing@acme.example.com".to_string(),
                    display_name: Some("Acme Corporation".to_string()),
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        let deleted = orgs.delete(org.id).await.unwrap();
        assert!(deleted);

        let filter = OrganizationFilter::new(0, 100);
        let list = orgs.list(&filter).await.unwrap();
        assert!(list.is_empty());

        let deleted_again = orgs.delete(org.id).await.unwrap();
        assert!(!deleted_again);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_organization_scrubs_data(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "billing@acme.example.com".to_string(),
                    display_name: Some("Acme Corporation".to_string()),
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        orgs.delete(org.id).await.unwrap();

        let row = sqlx::query!("SELECT username, email, display_name, is_deleted FROM users WHERE id = $1", org.id)
            .fetch_one(&pool)
            .await
            .unwrap();

        assert!(row.is_deleted);
        assert!(row.display_name.is_none());
        assert!(row.email.contains("deleted"));
        assert!(row.username.contains("deleted"));
    }


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_add_member(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;
        let bob = create_individual(&pool, "bob", "bob@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "billing@acme.example.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        let member = orgs.add_member(org.id, bob, "member").await.unwrap();
        assert_eq!(member.user_id, Some(bob));
        assert_eq!(member.organization_id, org.id);
        assert_eq!(member.role, "member");
        assert_eq!(member.status, "active");

        let members = orgs.list_members(org.id).await.unwrap();
        assert_eq!(members.len(), 2);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_add_duplicate_member_fails(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;
        let bob = create_individual(&pool, "bob", "bob@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "billing@acme.example.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        orgs.add_member(org.id, bob, "member").await.unwrap();

        let result = orgs.add_member(org.id, bob, "admin").await;
        assert!(result.is_err());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_remove_member(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;
        let bob = create_individual(&pool, "bob", "bob@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "billing@acme.example.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        orgs.add_member(org.id, bob, "member").await.unwrap();

        let removed = orgs.remove_member(org.id, bob).await.unwrap();
        assert!(removed);

        let members = orgs.list_members(org.id).await.unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].user_id, Some(creator));

        let removed_again = orgs.remove_member(org.id, bob).await.unwrap();
        assert!(!removed_again);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_update_member_role(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;
        let bob = create_individual(&pool, "bob", "bob@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "billing@acme.example.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        orgs.add_member(org.id, bob, "member").await.unwrap();

        let updated = orgs.update_member_role(org.id, bob, "admin").await.unwrap();
        assert_eq!(updated.role, "admin");

        let role = orgs.get_user_org_role(bob, org.id).await.unwrap();
        assert_eq!(role, Some("admin".to_string()));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_update_member_role_nonexistent_returns_not_found(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;
        let bob = create_individual(&pool, "bob", "bob@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "billing@acme.example.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        let result = orgs.update_member_role(org.id, bob, "admin").await;
        assert!(result.is_err());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_user_organizations(pool: PgPool) {
        let alice = create_individual(&pool, "alice", "alice@example.com").await;
        let bob = create_individual(&pool, "bob", "bob@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org1 = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "billing@acme.example.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: alice,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        orgs.create(
            &OrganizationCreateDBRequest {
                name: "globex-inc".to_string(),
                email: "info@globex.example.com".to_string(),
                display_name: None,
                avatar_url: None,
                created_by: alice,
            },
            TEST_DEFAULT_ROLES,
        )
        .await
        .unwrap();

        orgs.add_member(org1.id, bob, "member").await.unwrap();

        let alice_orgs = orgs.list_user_organizations(alice).await.unwrap();
        assert_eq!(alice_orgs.len(), 2);

        let bob_orgs = orgs.list_user_organizations(bob).await.unwrap();
        assert_eq!(bob_orgs.len(), 1);
        assert_eq!(bob_orgs[0].organization_id, org1.id);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_user_org_role_not_member(pool: PgPool) {
        let alice = create_individual(&pool, "alice", "alice@example.com").await;
        let bob = create_individual(&pool, "bob", "bob@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "billing@acme.example.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: alice,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        let role = orgs.get_user_org_role(bob, org.id).await.unwrap();
        assert_eq!(role, None);
    }


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_cannot_add_member_to_individual_user(pool: PgPool) {
        let alice = create_individual(&pool, "alice", "alice@example.com").await;
        let bob = create_individual(&pool, "bob", "bob@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let result = orgs.add_member(alice, bob, "member").await;
        assert!(result.is_err(), "Should not allow adding members to an individual user");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_deleted_org_excluded_from_list_user_organizations(pool: PgPool) {
        let alice = create_individual(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme-corp".to_string(),
                    email: "billing@acme.example.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: alice,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        orgs.delete(org.id).await.unwrap();

        let alice_orgs = orgs.list_user_organizations(alice).await.unwrap();
        assert!(alice_orgs.is_empty());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_organizations_pagination(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        for i in 0..5 {
            orgs.create(
                &OrganizationCreateDBRequest {
                    name: format!("org-{i}"),
                    email: format!("org-{i}@example.com"),
                    display_name: None,
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();
        }

        let page1 = orgs.list(&OrganizationFilter::new(0, 2)).await.unwrap();
        assert_eq!(page1.len(), 2);

        let page2 = orgs.list(&OrganizationFilter::new(2, 2)).await.unwrap();
        assert_eq!(page2.len(), 2);

        let page3 = orgs.list(&OrganizationFilter::new(4, 2)).await.unwrap();
        assert_eq!(page3.len(), 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_orgs_can_share_contact_email(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let shared_email = "shared@contact.example.com";

        let org1 = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "org-alpha".to_string(),
                    email: shared_email.to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        let org2 = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "org-beta".to_string(),
                    email: shared_email.to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: creator,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        assert_eq!(org1.email, shared_email);
        assert_eq!(org2.email, shared_email);
        assert_ne!(org1.id, org2.id);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_find_by_domain(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);

        let result = orgs.find_by_domain("acme.com").await.unwrap();
        assert!(result.is_none());

        orgs.create(
            &OrganizationCreateDBRequest {
                name: "acme.com".to_string(),
                email: "contact@acme.com".to_string(),
                display_name: Some("Acme Corp".to_string()),
                avatar_url: None,
                created_by: creator,
            },
            TEST_DEFAULT_ROLES,
        )
        .await
        .unwrap();

        let result = orgs.find_by_domain("acme.com").await.unwrap();
        assert!(result.is_some());
        let org = result.unwrap();
        assert_eq!(org.username, "acme.com");
        assert_eq!(org.user_type, "organization");
    }

    #[dwctl_test_macros::test]
    async fn test_deleting_owner_promotes_earliest_admin(pool: PgPool) {
        let owner = create_individual(&pool, "owner", "owner@acme.com").await;
        let member = create_individual(&pool, "member", "member@acme.com").await;
        let admin_early = create_individual(&pool, "admin1", "admin1@acme.com").await;
        let admin_late = create_individual(&pool, "admin2", "admin2@acme.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme.com".to_string(),
                    email: "contact@acme.com".to_string(),
                    display_name: Some("Acme Corp".to_string()),
                    avatar_url: None,
                    created_by: owner,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();
        orgs.add_member(org.id, member, "member").await.unwrap();
        orgs.add_member(org.id, admin_early, "admin").await.unwrap();
        orgs.add_member(org.id, admin_late, "admin").await.unwrap();
        drop(conn);

        let mut conn = pool.acquire().await.unwrap();
        assert!(Users::new(&mut conn).delete(owner).await.unwrap());

        let mut orgs = Organizations::new(&mut conn);
        assert_eq!(
            orgs.get_user_org_role(admin_early, org.id).await.unwrap(),
            Some("owner".to_string()),
            "the earliest admin takes over"
        );
        assert_eq!(orgs.get_user_org_role(admin_late, org.id).await.unwrap(), Some("admin".to_string()));
        assert_eq!(orgs.get_user_org_role(member, org.id).await.unwrap(), Some("member".to_string()));
        assert_eq!(
            orgs.get_user_org_role(owner, org.id).await.unwrap(),
            None,
            "the departed owner keeps no membership"
        );
        assert!(
            orgs.find_by_domain("acme.com").await.unwrap().is_some(),
            "and the workspace stays routable"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_deleting_owner_promotes_earliest_member_when_no_admins(pool: PgPool) {
        let owner = create_individual(&pool, "owner", "owner@acme.com").await;
        let first = create_individual(&pool, "first", "first@acme.com").await;
        let second = create_individual(&pool, "second", "second@acme.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme.com".to_string(),
                    email: "contact@acme.com".to_string(),
                    display_name: Some("Acme Corp".to_string()),
                    avatar_url: None,
                    created_by: owner,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();
        orgs.add_member(org.id, first, "member").await.unwrap();
        orgs.add_member(org.id, second, "member").await.unwrap();
        drop(conn);

        let mut conn = pool.acquire().await.unwrap();
        assert!(Users::new(&mut conn).delete(owner).await.unwrap());

        let mut orgs = Organizations::new(&mut conn);
        assert_eq!(orgs.get_user_org_role(first, org.id).await.unwrap(), Some("owner".to_string()));
        assert_eq!(orgs.get_user_org_role(second, org.id).await.unwrap(), Some("member".to_string()));
    }

    #[dwctl_test_macros::test]
    async fn test_deleting_a_member_revokes_the_org_keys_they_hold(pool: PgPool) {
        let owner = create_individual(&pool, "owner", "owner@acme.com").await;
        let member = create_individual(&pool, "member", "member@acme.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme.com".to_string(),
                    email: "contact@acme.com".to_string(),
                    display_name: Some("Acme Corp".to_string()),
                    avatar_url: None,
                    created_by: owner,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();
        orgs.add_member(org.id, member, "member").await.unwrap();

        sqlx::query!(
            "INSERT INTO api_keys (user_id, created_by, name, secret_hash, secret_prefix, purpose) VALUES ($1, $2, 'issued', sha256(convert_to('sk-issued-key', 'UTF8')), left('sk-issued-key', 8), 'inference')",
            org.id,
            member
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query!(
            "INSERT INTO api_keys (user_id, created_by, name, secret_hash, secret_prefix, purpose) VALUES ($1, $2, 'owners', sha256(convert_to('sk-owner-key', 'UTF8')), left('sk-owner-key', 8), 'inference')",
            org.id,
            owner
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        assert!(Users::new(&mut conn).delete(member).await.unwrap());

        let live: Vec<String> = sqlx::query_scalar!(
            r#"SELECT name as "name!" FROM api_keys WHERE user_id = $1 AND is_deleted = false ORDER BY name"#,
            org.id
        )
        .fetch_all(&mut *conn)
        .await
        .unwrap();
        assert_eq!(
            live,
            vec!["owners".to_string()],
            "the departed member's issued key must stop authenticating"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_closing_a_workspace_with_a_connection_still_deletes_the_account(pool: PgPool) {
        let owner = create_individual(&pool, "owner", "owner@acme.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme.com".to_string(),
                    email: "contact@acme.com".to_string(),
                    display_name: Some("Acme Corp".to_string()),
                    avatar_url: None,
                    created_by: owner,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();

        let key_id: uuid::Uuid = sqlx::query_scalar!(
            "INSERT INTO api_keys (user_id, created_by, name, secret_hash, secret_prefix, purpose) VALUES ($1, $1, 'org key', sha256(convert_to('sk-conn-key', 'UTF8')), left('sk-conn-key', 8), 'inference') RETURNING id",
            org.id
        )
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        sqlx::query!(
            "INSERT INTO connections (user_id, api_key_id, kind, provider, name, config_encrypted) VALUES ($1, $2, 'source', 'openai', 'conn', '\\x00'::bytea)",
            org.id,
            key_id
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        assert!(
            Users::new(&mut conn).delete(owner).await.unwrap(),
            "the account must delete even though a connection pins the workspace's key"
        );

        let (org_deleted, key_live) = sqlx::query!(
            r#"SELECT (SELECT is_deleted FROM users WHERE id = $1) as "org_deleted!",
                      (SELECT count(*) FROM api_keys WHERE user_id = $1 AND is_deleted = false) as "key_live!""#,
            org.id
        )
        .fetch_one(&mut *conn)
        .await
        .map(|r| (r.org_deleted, r.key_live))
        .unwrap();
        assert!(org_deleted, "the workspace is still closed");
        assert_eq!(key_live, 0, "and its keys stop authenticating");
    }

    #[dwctl_test_macros::test]
    async fn test_deleting_one_of_two_owners_leaves_the_other(pool: PgPool) {
        let leaving = create_individual(&pool, "leaving", "leaving@acme.com").await;
        let staying = create_individual(&pool, "staying", "staying@acme.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme.com".to_string(),
                    email: "contact@acme.com".to_string(),
                    display_name: Some("Acme Corp".to_string()),
                    avatar_url: None,
                    created_by: leaving,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();
        orgs.add_member(org.id, staying, "owner").await.unwrap();
        drop(conn);

        let mut conn = pool.acquire().await.unwrap();
        assert!(Users::new(&mut conn).delete(leaving).await.unwrap());

        let mut orgs = Organizations::new(&mut conn);
        assert_eq!(orgs.get_user_org_role(staying, org.id).await.unwrap(), Some("owner".to_string()));
        assert!(orgs.find_by_domain("acme.com").await.unwrap().is_some());
    }

    #[dwctl_test_macros::test]
    async fn test_deleting_sole_owner_closes_the_workspace(pool: PgPool) {
        let owner = create_individual(&pool, "owner", "owner@acme.com").await;
        let departed = create_individual(&pool, "departed", "departed@acme.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme.com".to_string(),
                    email: "contact@acme.com".to_string(),
                    display_name: Some("Acme Corp".to_string()),
                    avatar_url: None,
                    created_by: owner,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();
        orgs.add_member(org.id, departed, "admin").await.unwrap();
        drop(conn);

        let mut conn = pool.acquire().await.unwrap();
        assert!(Users::new(&mut conn).delete(departed).await.unwrap());
        sqlx::query!(
            "INSERT INTO api_keys (user_id, created_by, name, secret_hash, secret_prefix, purpose) VALUES ($1, $1, 'org key', sha256(convert_to('sk-test-org-key', 'UTF8')), left('sk-test-org-key', 8), 'inference')",
            org.id
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        assert!(Users::new(&mut conn).delete(owner).await.unwrap());

        let closed = sqlx::query!("SELECT username, email, display_name, is_deleted FROM users WHERE id = $1", org.id)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert!(closed.is_deleted, "the workspace is closed with its last owner");
        assert_eq!(
            closed.username,
            format!("deleted-{}", org.id),
            "and scrubbed the same way a user is"
        );
        assert_eq!(closed.email, format!("deleted-{}@deleted.local", org.id));
        assert!(closed.display_name.is_none());

        let keys = sqlx::query_scalar!(
            r#"SELECT count(*) as "count!" FROM api_keys WHERE user_id = $1 AND is_deleted = false"#,
            org.id
        )
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(keys, 0, "nothing may keep authenticating as a closed workspace");

        let mut orgs = Organizations::new(&mut conn);
        assert!(orgs.find_by_domain("acme.com").await.unwrap().is_none());
    }

    #[dwctl_test_macros::test]
    async fn test_find_by_domain_skips_orgs_with_no_live_admin(pool: PgPool) {
        let owner = create_individual(&pool, "owner", "owner@acme.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme.com".to_string(),
                    email: "contact@acme.com".to_string(),
                    display_name: Some("Acme Corp".to_string()),
                    avatar_url: None,
                    created_by: owner,
                },
                TEST_DEFAULT_ROLES,
            )
            .await
            .unwrap();
        assert!(
            orgs.find_by_domain("acme.com").await.unwrap().is_some(),
            "routable while the owner is live"
        );
        drop(conn);

        let mut conn = pool.acquire().await.unwrap();
        sqlx::query!("UPDATE users SET is_deleted = true WHERE id = $1", owner)
            .execute(&mut *conn)
            .await
            .unwrap();

        let still_live = sqlx::query_scalar!(r#"SELECT is_deleted as "is_deleted!" FROM users WHERE id = $1"#, org.id)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert!(!still_live, "precondition: the workspace itself is not deleted");
        let mut orgs = Organizations::new(&mut conn);
        assert!(
            orgs.find_by_domain("acme.com").await.unwrap().is_none(),
            "an ownerless workspace must not collect join requests nobody can approve"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_find_by_domain_ignores_sql_wildcards(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@example.com").await;
        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        orgs.create(
            &OrganizationCreateDBRequest {
                name: "acme.com~a1b2c3d4".to_string(),
                email: "contact@acme.com".to_string(),
                display_name: Some("Acme Corp".to_string()),
                avatar_url: None,
                created_by: creator,
            },
            TEST_DEFAULT_ROLES,
        )
        .await
        .unwrap();

        assert!(
            orgs.find_by_domain("acme.com").await.unwrap().is_some(),
            "control: the real domain resolves"
        );
        for wildcard in ["%", "_", "%.com", "acme.co_", "acme%"] {
            assert!(
                orgs.find_by_domain(wildcard).await.unwrap().is_none(),
                "{wildcard} must not be treated as a pattern"
            );
        }
    }

    #[dwctl_test_macros::test]
    async fn test_find_by_domain_ignores_single_label_names(pool: PgPool) {
        let creator = create_individual(&pool, "alice", "alice@gmail.com").await;
        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        orgs.create(
            &OrganizationCreateDBRequest {
                name: "user~a1b2c3d4".to_string(),
                email: "alice@gmail.com".to_string(),
                display_name: Some("Alice's workspace".to_string()),
                avatar_url: None,
                created_by: creator,
            },
            TEST_DEFAULT_ROLES,
        )
        .await
        .unwrap();

        assert!(
            orgs.find_by_domain("user").await.unwrap().is_none(),
            "a personal-email workspace must not be reachable through its namespace"
        );
        assert!(orgs.find_by_domain("localhost").await.unwrap().is_none());
    }
}
