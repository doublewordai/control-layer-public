
use crate::types::{UserId, abbrev_uuid};
use crate::{
    api::models::users::Role,
    db::{
        errors::{DbError, Result},
        handlers::{Groups, api_keys::ApiKeys, repository::Repository},
        models::{
            api_keys::ApiKeyPurpose,
            users::{UserCreateDBRequest, UserDBResponse, UserUpdateDBRequest},
        },
    },
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, FromRow, PgConnection};
use std::time::Duration;
use tracing::instrument;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct UserFilter {
    pub skip: i64,
    pub limit: i64,
    pub search: Option<String>,
    pub user_type: String,
}

impl UserFilter {
    pub fn new(skip: i64, limit: i64) -> Self {
        Self {
            skip,
            limit,
            search: None,
            user_type: "individual".to_string(),
        }
    }

    pub fn organizations(skip: i64, limit: i64) -> Self {
        Self {
            skip,
            limit,
            search: None,
            user_type: "organization".to_string(),
        }
    }

    pub fn with_search(mut self, search: String) -> Self {
        self.search = Some(search);
        self
    }
}

#[derive(Debug, Clone)]
pub struct AutoTopupUser {
    pub id: UserId,
    pub email: String,
    pub username: String,
    pub display_name: Option<String>,
    pub payment_provider_id: String,
    pub auto_topup_threshold: rust_decimal::Decimal,
    pub auto_topup_amount: rust_decimal::Decimal,
    pub checkpoint_balance: Option<rust_decimal::Decimal>,
    pub auto_topup_monthly_limit: Option<rust_decimal::Decimal>,
    pub auto_topup_limit_notification_sent: bool,
    pub auto_topup_soft_failure_count: i32,
    pub invoicing_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct LowBalanceUser {
    pub id: UserId,
    pub email: String,
    pub username: String,
    pub display_name: Option<String>,
    pub low_balance_threshold: rust_decimal::Decimal,
    pub low_balance_notification_sent: bool,
    pub checkpoint_balance: Option<rust_decimal::Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
struct User {
    pub id: UserId,
    pub username: String,
    pub email: String,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub auth_source: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_login: Option<DateTime<Utc>>,
    pub is_admin: bool,
    pub password_hash: Option<String>,
    pub external_user_id: Option<String>,
    pub payment_provider_id: Option<String>,
    pub is_deleted: bool,
    pub is_internal: bool,
    pub batch_notifications_enabled: bool,
    pub first_batch_email_sent: bool,
    pub low_balance_notification_sent: bool,
    pub low_balance_threshold: Option<f32>,
    pub auto_topup_amount: Option<f32>,
    pub auto_topup_threshold: Option<f32>,
    pub auto_topup_monthly_limit: Option<f32>,
    pub auto_topup_limit_notification_sent: bool,
    pub auto_topup_soft_failure_count: i32,
    pub auto_topup_retry_after: Option<DateTime<Utc>>,
    pub user_type: String,
    pub verified: bool,
    pub zero_data_retention: bool,
    pub invoicing_enabled: bool,
    pub self_hosted_only: bool,
    pub toleration_preset: Option<String>,
    #[allow(dead_code)]
    pub auto_join_enabled: bool,
}

pub struct Users<'c> {
    db: &'c mut PgConnection,
}

impl From<(Vec<Role>, User)> for UserDBResponse {
    fn from((roles, user): (Vec<Role>, User)) -> Self {
        Self {
            id: user.id,
            username: user.username,
            email: user.email,
            display_name: user.display_name,
            avatar_url: user.avatar_url,
            created_at: user.created_at,
            updated_at: user.updated_at,
            last_login: user.last_login,
            auth_source: user.auth_source,
            is_admin: user.is_admin,
            roles,
            password_hash: user.password_hash,
            external_user_id: user.external_user_id,
            payment_provider_id: user.payment_provider_id,
            batch_notifications_enabled: user.batch_notifications_enabled,
            first_batch_email_sent: user.first_batch_email_sent,
            low_balance_notification_sent: user.low_balance_notification_sent,
            low_balance_threshold: user.low_balance_threshold,
            auto_topup_amount: user.auto_topup_amount,
            auto_topup_threshold: user.auto_topup_threshold,
            auto_topup_monthly_limit: user.auto_topup_monthly_limit,
            user_type: user.user_type,
            verified: user.verified,
            zero_data_retention: user.zero_data_retention,
            invoicing_enabled: user.invoicing_enabled,
            self_hosted_only: user.self_hosted_only,
            toleration_preset: user.toleration_preset,
        }
    }
}

#[async_trait::async_trait]
impl<'c> Repository for Users<'c> {
    type CreateRequest = UserCreateDBRequest;
    type UpdateRequest = UserUpdateDBRequest;
    type Response = UserDBResponse;
    type Created = UserDBResponse;
    type Id = UserId;
    type Filter = UserFilter;

    #[instrument(level = "debug", skip(self, request), fields(username = %request.username), err)]
    async fn create(&mut self, request: &Self::CreateRequest) -> Result<Self::Created> {
        let user_id = Uuid::new_v4();

        let mut tx = self.db.begin().await?;
        let user = sqlx::query_as!(
            User,
            r#"
            INSERT INTO users (id, username, email, display_name, avatar_url, auth_source, is_admin, password_hash, external_user_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id, username, email, display_name, avatar_url, auth_source, created_at, updated_at, last_login, is_admin, password_hash, external_user_id, payment_provider_id, is_deleted, is_internal, batch_notifications_enabled, first_batch_email_sent, low_balance_notification_sent, low_balance_threshold, user_type, auto_topup_amount, auto_topup_threshold, auto_topup_monthly_limit, auto_topup_limit_notification_sent, verified, zero_data_retention, auto_topup_soft_failure_count, auto_topup_retry_after, invoicing_enabled, auto_join_enabled, self_hosted_only, toleration_preset
            "#,
            user_id,
            request.username,
            request.email,
            request.display_name,
            request.avatar_url,
            request.auth_source,
            request.is_admin,
            request.password_hash,
            request.external_user_id
        )
        .fetch_one(&mut *tx)
        .await?;

        let mut roles_to_insert = request.roles.clone();
        if !roles_to_insert.contains(&Role::StandardUser) {
            roles_to_insert.push(Role::StandardUser);
        }

        for role in &roles_to_insert {
            sqlx::query!("INSERT INTO user_roles (user_id, role) VALUES ($1, $2)", user_id, role as &Role)
                .execute(&mut *tx)
                .await?;
        }

        let mut api_keys_repo = ApiKeys::new(&mut tx);
        api_keys_repo
            .get_or_create_hidden_key(user_id, ApiKeyPurpose::Batch, user_id)
            .await?;
        api_keys_repo
            .get_or_create_hidden_key(user_id, ApiKeyPurpose::Playground, user_id)
            .await?;

        tx.commit().await?;

        Ok(UserDBResponse::from((roles_to_insert, user)))
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&id)), err)]
    async fn get_by_id(&mut self, id: Self::Id) -> Result<Option<Self::Response>> {
        let result = sqlx::query!(
            r#"
            SELECT
                u.id,
                u.username,
                u.email,
                u.display_name,
                u.avatar_url,
                u.auth_source,
                u.created_at,
                u.updated_at,
                u.last_login,
                u.is_admin,
                u.password_hash,
                u.external_user_id,
                u.payment_provider_id,
                u.is_deleted,
                u.is_internal,
                u.batch_notifications_enabled,
                u.first_batch_email_sent,
                u.low_balance_notification_sent,
                u.low_balance_threshold,
                u.auto_topup_amount,
                u.auto_topup_threshold,
                u.auto_topup_monthly_limit,
                u.auto_topup_limit_notification_sent,
                u.user_type,
                u.verified,
                u.invoicing_enabled,
                u.zero_data_retention,
                u.self_hosted_only,
                u.toleration_preset,
                ARRAY_AGG(ur.role) FILTER (WHERE ur.role IS NOT NULL) as "roles: Vec<Role>"
            FROM users u
            LEFT JOIN user_roles ur ON ur.user_id = u.id
            WHERE u.id = $1 AND u.id != '00000000-0000-0000-0000-000000000000' AND u.is_deleted = false
            GROUP BY u.id, u.username, u.email, u.display_name, u.avatar_url, u.auth_source, u.created_at, u.updated_at, u.last_login, u.is_admin, u.password_hash, u.external_user_id, u.payment_provider_id, u.is_deleted, u.is_internal, u.batch_notifications_enabled, u.first_batch_email_sent, u.low_balance_notification_sent, u.low_balance_threshold, u.auto_topup_amount, u.auto_topup_threshold, u.auto_topup_monthly_limit, u.auto_topup_limit_notification_sent, u.user_type, u.verified, u.invoicing_enabled, u.zero_data_retention, u.self_hosted_only, u.toleration_preset
            "#,
            id
        )
        .fetch_optional(&mut *self.db)
        .await?;

        if let Some(row) = result {
            let user = User {
                id: row.id,
                username: row.username,
                email: row.email,
                display_name: row.display_name,
                avatar_url: row.avatar_url,
                auth_source: row.auth_source,
                created_at: row.created_at,
                updated_at: row.updated_at,
                last_login: row.last_login,
                is_admin: row.is_admin,
                password_hash: row.password_hash,
                external_user_id: row.external_user_id,
                payment_provider_id: row.payment_provider_id,
                is_deleted: row.is_deleted,
                is_internal: row.is_internal,
                batch_notifications_enabled: row.batch_notifications_enabled,
                first_batch_email_sent: row.first_batch_email_sent,
                low_balance_notification_sent: row.low_balance_notification_sent,
                low_balance_threshold: row.low_balance_threshold,
                auto_topup_amount: row.auto_topup_amount,
                auto_topup_threshold: row.auto_topup_threshold,
                auto_topup_monthly_limit: row.auto_topup_monthly_limit,
                auto_topup_limit_notification_sent: row.auto_topup_limit_notification_sent,
                auto_topup_soft_failure_count: 0,
                auto_topup_retry_after: None,
                user_type: row.user_type,
                verified: row.verified,
                invoicing_enabled: row.invoicing_enabled,
                zero_data_retention: row.zero_data_retention,
                self_hosted_only: row.self_hosted_only,
                toleration_preset: row.toleration_preset,
                auto_join_enabled: false,
            };

            let roles = row.roles.unwrap_or_default();

            Ok(Some(UserDBResponse::from((roles, user))))
        } else {
            Ok(None)
        }
    }

    #[instrument(level = "debug", skip(self, ids), fields(count = ids.len()), err)]
    async fn get_bulk(&mut self, ids: Vec<UserId>) -> Result<std::collections::HashMap<Self::Id, UserDBResponse>> {
        if ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let rows = sqlx::query!(
            r#"
            SELECT
                u.id,
                u.username,
                u.email,
                u.display_name,
                u.avatar_url,
                u.auth_source,
                u.created_at,
                u.updated_at,
                u.last_login,
                u.is_admin,
                u.password_hash,
                u.external_user_id,
                u.payment_provider_id,
                u.is_deleted,
                u.is_internal,
                u.batch_notifications_enabled,
                u.first_batch_email_sent,
                u.low_balance_notification_sent,
                u.low_balance_threshold,
                u.auto_topup_amount,
                u.auto_topup_threshold,
                u.auto_topup_monthly_limit,
                u.auto_topup_limit_notification_sent,
                u.user_type,
                u.verified,
                u.invoicing_enabled,
                u.zero_data_retention,
                u.self_hosted_only,
                u.toleration_preset,
                ARRAY_AGG(ur.role) FILTER (WHERE ur.role IS NOT NULL) as "roles: Vec<Role>"
            FROM users u
            LEFT JOIN user_roles ur ON ur.user_id = u.id
            WHERE u.id = ANY($1) AND u.id != '00000000-0000-0000-0000-000000000000' AND u.is_deleted = false
            GROUP BY u.id, u.username, u.email, u.display_name, u.avatar_url, u.auth_source, u.created_at, u.updated_at, u.last_login, u.is_admin, u.password_hash, u.external_user_id, u.payment_provider_id, u.is_deleted, u.is_internal, u.batch_notifications_enabled, u.first_batch_email_sent, u.low_balance_notification_sent, u.low_balance_threshold, u.auto_topup_amount, u.auto_topup_threshold, u.auto_topup_monthly_limit, u.auto_topup_limit_notification_sent, u.user_type, u.verified, u.invoicing_enabled, u.zero_data_retention, u.self_hosted_only, u.toleration_preset
            "#,
            ids.as_slice()
        )
        .fetch_all(&mut *self.db)
        .await?;

        let mut result = std::collections::HashMap::new();

        for row in rows {
            let user = User {
                id: row.id,
                username: row.username,
                email: row.email,
                display_name: row.display_name,
                avatar_url: row.avatar_url,
                auth_source: row.auth_source,
                created_at: row.created_at,
                updated_at: row.updated_at,
                last_login: row.last_login,
                is_admin: row.is_admin,
                password_hash: row.password_hash,
                external_user_id: row.external_user_id,
                payment_provider_id: row.payment_provider_id,
                is_deleted: row.is_deleted,
                is_internal: row.is_internal,
                batch_notifications_enabled: row.batch_notifications_enabled,
                first_batch_email_sent: row.first_batch_email_sent,
                low_balance_notification_sent: row.low_balance_notification_sent,
                low_balance_threshold: row.low_balance_threshold,
                auto_topup_amount: row.auto_topup_amount,
                auto_topup_threshold: row.auto_topup_threshold,
                auto_topup_monthly_limit: row.auto_topup_monthly_limit,
                auto_topup_limit_notification_sent: row.auto_topup_limit_notification_sent,
                auto_topup_soft_failure_count: 0,
                auto_topup_retry_after: None,
                user_type: row.user_type,
                verified: row.verified,
                invoicing_enabled: row.invoicing_enabled,
                zero_data_retention: row.zero_data_retention,
                self_hosted_only: row.self_hosted_only,
                toleration_preset: row.toleration_preset,
                auto_join_enabled: false,
            };

            let roles = row.roles.unwrap_or_default();

            result.insert(user.id, UserDBResponse::from((roles, user)));
        }

        Ok(result)
    }
    #[instrument(level = "debug", skip(self, filter), fields(limit = filter.limit, skip = filter.skip, search = filter.search), err)]
    async fn list(&mut self, filter: &Self::Filter) -> Result<Vec<Self::Response>> {
        use sqlx::QueryBuilder;

        let mut query = QueryBuilder::new(
            "SELECT id, username, email, display_name, avatar_url, auth_source, created_at, updated_at, last_login, is_admin, password_hash, external_user_id, payment_provider_id, is_deleted, is_internal, batch_notifications_enabled, first_batch_email_sent, low_balance_notification_sent, low_balance_threshold, user_type, auto_topup_amount, auto_topup_threshold, auto_topup_monthly_limit, auto_topup_limit_notification_sent, verified, zero_data_retention, auto_topup_soft_failure_count, auto_topup_retry_after, invoicing_enabled, auto_join_enabled, self_hosted_only, toleration_preset FROM users WHERE id != '00000000-0000-0000-0000-000000000000' AND is_deleted = false AND user_type = ",
        );
        query.push_bind(filter.user_type.clone());

        if let Some(ref search) = filter.search {
            let search_pattern = format!("%{}%", search.to_lowercase());
            query.push(" AND (LOWER(COALESCE(display_name, '')) LIKE ");
            query.push_bind(search_pattern.clone());
            query.push(" OR LOWER(username) LIKE ");
            query.push_bind(search_pattern.clone());
            query.push(" OR LOWER(email) LIKE ");
            query.push_bind(search_pattern);
            query.push(")");
        }

        query.push(" ORDER BY created_at DESC LIMIT ");
        query.push_bind(filter.limit);
        query.push(" OFFSET ");
        query.push_bind(filter.skip);

        let users = query.build_query_as::<User>().fetch_all(&mut *self.db).await?;

        let mut tx = self.db.begin().await?;

        let mut result = Vec::new();
        for user in users {
            let roles = sqlx::query!("SELECT role as \"role: Role\" FROM user_roles WHERE user_id = $1", user.id)
                .fetch_all(&mut *tx)
                .await?;

            let roles: Vec<Role> = roles.into_iter().map(|r| r.role).collect();

            result.push(UserDBResponse::from((roles, user)));
        }
        tx.commit().await?;
        Ok(result)
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&id)), err)]
    async fn delete(&mut self, id: Self::Id) -> Result<bool> {
        let scrubbed_email = format!("deleted-{}@deleted.local", id);
        let scrubbed_username = format!("deleted-{}", id);

        let mut tx = self.db.begin().await?;

        let result = sqlx::query!(
            r#"
            UPDATE users
            SET
                email = $1,
                username = $2,
                display_name = NULL,
                avatar_url = NULL,
                password_hash = NULL,
                external_user_id = NULL,
                payment_provider_id = NULL,
                is_deleted = true,
                updated_at = NOW()
            WHERE id = $3 AND is_deleted = false
            "#,
            scrubbed_email,
            scrubbed_username,
            id
        )
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() > 0 {
            sqlx::query!(
                "UPDATE api_keys SET is_deleted = true WHERE created_by = $1 AND user_id != $1 AND is_deleted = false",
                id
            )
            .execute(&mut *tx)
            .await?;

            let connection_ids = sqlx::query_scalar!("SELECT id FROM connections WHERE user_id = $1 AND deleted_at IS NULL", id)
                .fetch_all(&mut *tx)
                .await?;
            for connection_id in connection_ids {
                crate::db::handlers::connections::Connections::new(&mut tx)
                    .soft_delete(connection_id)
                    .await?;
            }

            sqlx::query!(
                r#"
                UPDATE connections
                SET api_key_id = NULL
                WHERE api_key_id IN (SELECT id FROM api_keys WHERE user_id = $1)
                "#,
                id
            )
            .execute(&mut *tx)
            .await?;

            sqlx::query!(r#"DELETE FROM api_keys WHERE user_id = $1"#, id)
                .execute(&mut *tx)
                .await?;

            for org_id in sqlx::query_scalar!(
                r#"SELECT organization_id FROM user_organizations WHERE user_id = $1 AND role = 'owner' AND status = 'active'"#,
                id
            )
            .fetch_all(&mut *tx)
            .await?
            {
                let successor = sqlx::query_scalar!(
                    r#"
                    SELECT uo.user_id
                    FROM user_organizations uo
                    JOIN users u ON u.id = uo.user_id
                    WHERE uo.organization_id = $1
                      AND uo.user_id <> $2
                      AND uo.status = 'active'
                      AND u.is_deleted = false
                    ORDER BY CASE uo.role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 ELSE 2 END, uo.created_at ASC, uo.user_id ASC
                    LIMIT 1
                    "#,
                    org_id,
                    id
                )
                .fetch_optional(&mut *tx)
                .await?;

                match successor {
                    Some(successor_id) => {
                        sqlx::query!(
                            r#"UPDATE user_organizations SET role = 'owner' WHERE organization_id = $1 AND user_id = $2 AND status = 'active'"#,
                            org_id,
                            successor_id
                        )
                        .execute(&mut *tx)
                        .await?;
                    }
                    None => {
                        sqlx::query!(
                            r#"
                            UPDATE users
                            SET email = $1, username = $2, display_name = NULL, avatar_url = NULL,
                                is_deleted = true, updated_at = NOW()
                            WHERE id = $3 AND user_type = 'organization' AND is_deleted = false
                            "#,
                            format!("deleted-{org_id}@deleted.local"),
                            format!("deleted-{org_id}"),
                            org_id
                        )
                        .execute(&mut *tx)
                        .await?;
                        sqlx::query!(
                            r#"UPDATE api_keys SET is_deleted = true WHERE user_id = $1 AND is_deleted = false"#,
                            org_id
                        )
                        .execute(&mut *tx)
                        .await?;
                    }
                }
            }

            let revoked = sqlx::query!(
                r#"
                UPDATE api_keys SET is_deleted = true
                WHERE created_by = $1
                  AND is_deleted = false
                  AND user_id IN (SELECT organization_id FROM user_organizations WHERE user_id = $1)
                "#,
                id
            )
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if revoked > 0 {
                tracing::info!(
                    user_id = %abbrev_uuid(&id),
                    revoked,
                    "Revoked organization API keys held by a deleted user"
                );
            }

            sqlx::query!(r#"DELETE FROM user_organizations WHERE user_id = $1"#, id)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;

        Ok(result.rows_affected() > 0)
    }

    #[instrument(level = "debug", skip(self, request), fields(user_id = %abbrev_uuid(&id)), err)]
    async fn update(&mut self, id: Self::Id, request: &Self::UpdateRequest) -> Result<Self::Response> {

        let user;
        {
            let mut tx = self.db.begin().await?;

            user = sqlx::query_as!(
                User,
                r#"
            UPDATE users SET
                display_name = COALESCE($2, display_name),
                avatar_url = COALESCE($3, avatar_url),
                password_hash = COALESCE($4, password_hash),
                batch_notifications_enabled = COALESCE($5, batch_notifications_enabled),
                low_balance_threshold = CASE
                    WHEN $6::boolean THEN $7
                    ELSE low_balance_threshold
                END,
                low_balance_notification_sent = CASE
                    WHEN $6::boolean THEN false
                    ELSE low_balance_notification_sent
                END,
                auto_topup_amount = CASE
                    WHEN $8::boolean THEN $9
                    ELSE auto_topup_amount
                END,
                auto_topup_threshold = CASE
                    WHEN $10::boolean THEN $11
                    ELSE auto_topup_threshold
                END,
                auto_topup_monthly_limit = CASE
                    WHEN $12::boolean THEN $13
                    ELSE auto_topup_monthly_limit
                END,
                auto_topup_limit_notification_sent = CASE
                    WHEN $12::boolean THEN false
                    ELSE auto_topup_limit_notification_sent
                END,
                auto_topup_soft_failure_count = CASE
                    WHEN $8::boolean OR $10::boolean THEN 0
                    ELSE auto_topup_soft_failure_count
                END,
                auto_topup_retry_after = CASE
                    WHEN $8::boolean OR $10::boolean THEN NULL
                    ELSE auto_topup_retry_after
                END,
                zero_data_retention = COALESCE($14, zero_data_retention),
                self_hosted_only = COALESCE($15, self_hosted_only),
                toleration_preset = CASE
                    WHEN $16::boolean THEN $17
                    ELSE toleration_preset
                END,
                updated_at = NOW()
            WHERE id = $1
            RETURNING id, username, email, display_name, avatar_url, auth_source, created_at, updated_at, last_login, is_admin, password_hash, external_user_id, payment_provider_id, is_deleted, is_internal, batch_notifications_enabled, first_batch_email_sent, low_balance_notification_sent, low_balance_threshold, user_type, auto_topup_amount, auto_topup_threshold, auto_topup_monthly_limit, auto_topup_limit_notification_sent, verified, zero_data_retention, auto_topup_soft_failure_count, auto_topup_retry_after, invoicing_enabled, auto_join_enabled, self_hosted_only, toleration_preset
            "#,
                id,
                request.display_name,
                request.avatar_url,
                request.password_hash,
                request.batch_notifications_enabled,
                request.low_balance_threshold.is_some() as bool,
                request.low_balance_threshold.flatten(),
                request.auto_topup_amount.is_some() as bool,
                request.auto_topup_amount.flatten(),
                request.auto_topup_threshold.is_some() as bool,
                request.auto_topup_threshold.flatten(),
                request.auto_topup_monthly_limit.is_some() as bool,
                request.auto_topup_monthly_limit.flatten(),
                request.zero_data_retention,
                request.self_hosted_only,
                request.toleration_preset.is_some() as bool,
                request.toleration_preset.clone().flatten(),
            )
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| DbError::NotFound)?;

            if let Some(roles) = &request.roles {
                let mut updated_roles = roles.clone();
                if !updated_roles.contains(&Role::StandardUser) {
                    updated_roles.push(Role::StandardUser);
                }

                sqlx::query!("DELETE FROM user_roles WHERE user_id = $1", id)
                    .execute(&mut *tx)
                    .await?;

                for role in &updated_roles {
                    sqlx::query!("INSERT INTO user_roles (user_id, role) VALUES ($1, $2)", id, role as &Role)
                        .execute(&mut *tx)
                        .await?;
                }
            }
            tx.commit().await?;
        }

        let roles = sqlx::query!("SELECT role as \"role: Role\" FROM user_roles WHERE user_id = $1", id)
            .fetch_all(&mut *self.db)
            .await?;

        let roles: Vec<Role> = roles.into_iter().map(|r| r.role).collect();

        Ok(UserDBResponse::from((roles, user)))
    }
}

impl<'c> Users<'c> {
    pub fn new(db: &'c mut PgConnection) -> Self {
        Self { db }
    }

    #[instrument(level = "debug", skip(self, filter), fields(search = filter.search), err)]
    pub async fn count(&mut self, filter: &UserFilter) -> Result<i64> {
        use sqlx::QueryBuilder;

        let mut query = QueryBuilder::new(
            "SELECT COUNT(*) FROM users WHERE id != '00000000-0000-0000-0000-000000000000' AND is_deleted = false AND user_type = ",
        );
        query.push_bind(filter.user_type.clone());

        if let Some(ref search) = filter.search {
            let search_pattern = format!("%{}%", search.to_lowercase());
            query.push(" AND (LOWER(COALESCE(display_name, '')) LIKE ");
            query.push_bind(search_pattern.clone());
            query.push(" OR LOWER(username) LIKE ");
            query.push_bind(search_pattern.clone());
            query.push(" OR LOWER(email) LIKE ");
            query.push_bind(search_pattern);
            query.push(")");
        }

        let count: (i64,) = query.build_query_as().fetch_one(&mut *self.db).await?;
        Ok(count.0)
    }

    #[instrument(level = "debug", skip(self, email), err)]
    pub async fn get_user_by_email(&mut self, email: &str) -> Result<Option<UserDBResponse>> {
        let user = sqlx::query_as!(
            User,
            "SELECT id, username, email, display_name, avatar_url, auth_source, created_at, updated_at, last_login, is_admin, password_hash, external_user_id, payment_provider_id, is_deleted, is_internal, batch_notifications_enabled, first_batch_email_sent, low_balance_notification_sent, low_balance_threshold, user_type, auto_topup_amount, auto_topup_threshold, auto_topup_monthly_limit, auto_topup_limit_notification_sent, verified, zero_data_retention, auto_topup_soft_failure_count, auto_topup_retry_after, invoicing_enabled, auto_join_enabled, self_hosted_only, toleration_preset FROM users WHERE email = $1 AND id != '00000000-0000-0000-0000-000000000000' AND is_deleted = false AND user_type = 'individual' AND NOT is_service_account",
            email
        )
        .fetch_optional(&mut *self.db)
        .await?;

        if let Some(user) = user {
            let roles = sqlx::query!("SELECT role as \"role: Role\" FROM user_roles WHERE user_id = $1", user.id)
                .fetch_all(&mut *self.db)
                .await?;

            let roles: Vec<Role> = roles.into_iter().map(|r| r.role).collect();

            Ok(Some(UserDBResponse::from((roles, user))))
        } else {
            Ok(None)
        }
    }

    #[instrument(level = "debug", skip(self, external_user_id), err)]
    pub async fn get_user_by_external_user_id(&mut self, external_user_id: &str) -> Result<Option<UserDBResponse>> {
        let user = sqlx::query_as!(
            User,
            "SELECT id, username, email, display_name, avatar_url, auth_source, created_at, updated_at, last_login, is_admin, password_hash, external_user_id, payment_provider_id, is_deleted, is_internal, batch_notifications_enabled, first_batch_email_sent, low_balance_notification_sent, low_balance_threshold, user_type, auto_topup_amount, auto_topup_threshold, auto_topup_monthly_limit, auto_topup_limit_notification_sent, verified, zero_data_retention, auto_topup_soft_failure_count, auto_topup_retry_after, invoicing_enabled, auto_join_enabled, self_hosted_only, toleration_preset FROM users WHERE external_user_id = $1 AND id != '00000000-0000-0000-0000-000000000000' AND is_deleted = false AND NOT is_service_account",
            external_user_id
        )
        .fetch_optional(&mut *self.db)
        .await?;

        if let Some(user) = user {
            let roles = sqlx::query!("SELECT role as \"role: Role\" FROM user_roles WHERE user_id = $1", user.id)
                .fetch_all(&mut *self.db)
                .await?;

            let roles: Vec<Role> = roles.into_iter().map(|r| r.role).collect();

            Ok(Some(UserDBResponse::from((roles, user))))
        } else {
            Ok(None)
        }
    }

    #[instrument(level = "debug", skip(self, email), fields(user_id = %abbrev_uuid(&user_id)), err)]
    async fn update_user_email(&mut self, user_id: UserId, email: &str) -> Result<()> {
        sqlx::query!("UPDATE users SET email = $1 WHERE id = $2", email, user_id)
            .execute(&mut *self.db)
            .await?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self, external_user_id), fields(user_id = %abbrev_uuid(&user_id)), err)]
    async fn update_user_external_id(&mut self, user_id: UserId, external_user_id: &str) -> Result<()> {
        sqlx::query!("UPDATE users SET external_user_id = $1 WHERE id = $2", external_user_id, user_id)
            .execute(&mut *self.db)
            .await?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self, external_user_id, email, groups_and_provider, default_roles), err)]
    pub async fn get_or_create_proxy_header_user(
        &mut self,
        external_user_id: &str,
        email: &str,
        groups_and_provider: Option<(Vec<String>, &str)>,
        default_roles: &[Role],
    ) -> Result<(UserDBResponse, bool)> {
        tracing::trace!(
            "Starting get_or_create_proxy_header_user for external_user_id: {}",
            external_user_id
        );

        sqlx::query!("SELECT pg_advisory_xact_lock(hashtext($1))", external_user_id)
            .execute(&mut *self.db)
            .await?;
        tracing::trace!("Acquired advisory lock for external_user_id");

        let (user, was_created) = 'user_lookup: {
            if let Some(mut user) = self.get_user_by_external_user_id(external_user_id).await? {
                tracing::debug!("Found existing user by external_user_id");
                tracing::trace!("Found user by external_user_id: {}", external_user_id);
                if user.email != email {
                    tracing::debug!("Updating email for user {}", abbrev_uuid(&user.id));
                    tracing::trace!("Updating email from {} to {}", user.email, email);
                    self.update_user_email(user.id, email).await?;
                    user.email = email.to_string();
                }

                break 'user_lookup (user, false);
            }

            if let Some(mut user) = self.get_user_by_email(email).await? {
                tracing::debug!("Found existing user by email");
                tracing::trace!("Found user by email: {}", email);
                if let Some(existing_external_id) = &user.external_user_id {
                    tracing::debug!("User {} has existing external_user_id set", abbrev_uuid(&user.id));
                    tracing::trace!("Existing external_user_id: {}", existing_external_id);
                    if existing_external_id == external_user_id {
                        tracing::debug!("External user ID matches for user {}, using existing user", abbrev_uuid(&user.id));
                        break 'user_lookup (user, false);
                    }
                    tracing::debug!("External user ID mismatch for user {}, creating new user", abbrev_uuid(&user.id));
                } else {

                    if external_user_id == email {
                        tracing::debug!(
                            "External user ID equals email for user {}, skipping backfill",
                            abbrev_uuid(&user.id)
                        );
                        break 'user_lookup (user, false);
                    }
                    tracing::debug!("Backfilling external_user_id for user {}", abbrev_uuid(&user.id));
                    tracing::trace!("Backfilling external_user_id to {}", external_user_id);

                    self.update_user_external_id(user.id, external_user_id).await?;
                    user.external_user_id = Some(external_user_id.to_string());

                    break 'user_lookup (user, false);
                }
            }

            tracing::debug!("Creating new user via proxy header auth");
            tracing::trace!(
                "No existing user found for external_user_id: {} and email: {}, creating new user",
                external_user_id,
                email
            );
            let display_name = crate::auth::utils::default_display_name(email);
            tracing::trace!("Defaulted display name to email prefix");

            let create_request = UserCreateDBRequest {
                username: external_user_id.to_string(),
                email: email.to_string(),
                display_name: Some(display_name),
                avatar_url: None,
                is_admin: false,
                roles: default_roles.to_vec(),
                auth_source: "proxy-header".to_string(),
                password_hash: None,
                external_user_id: Some(external_user_id.to_string()),
            };

            let created_user = self.create(&create_request).await?;
            (created_user, true)
        };

        if let Some((groups, provider)) = groups_and_provider {
            let mut group_repo = Groups::new(&mut *self.db);
            group_repo
                .sync_groups_with_sso(
                    user.id,
                    groups,
                    provider,
                    &format!("A group provisioned by the {provider} SSO source."),
                )
                .await?;
        }


        Ok((user, was_created))
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn mark_first_batch_email_sent(&mut self, user_id: UserId) -> Result<()> {
        sqlx::query!("UPDATE users SET first_batch_email_sent = true WHERE id = $1", user_id)
            .execute(&mut *self.db)
            .await?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    pub async fn users_with_low_balance_threshold(&mut self) -> Result<Vec<LowBalanceUser>> {
        let rows = sqlx::query_as!(
            LowBalanceUser,
            r#"
            SELECT u.id, u.email, u.username, u.display_name,
                   u.low_balance_threshold::decimal(20, 9) as "low_balance_threshold!",
                   u.low_balance_notification_sent,
                   c.balance as "checkpoint_balance?"
            FROM users u
            LEFT JOIN user_balance_checkpoints c ON c.user_id = u.id
            WHERE u.id != '00000000-0000-0000-0000-000000000000'
              AND u.is_deleted = false
              AND u.low_balance_threshold IS NOT NULL
            "#,
        )
        .fetch_all(&mut *self.db)
        .await?;

        Ok(rows)
    }

    #[instrument(level = "debug", skip(self, user_ids), fields(count = user_ids.len()), err)]
    pub async fn mark_low_balance_notification_sent(&mut self, user_ids: &[UserId]) -> Result<()> {
        sqlx::query!("UPDATE users SET low_balance_notification_sent = true WHERE id = ANY($1)", user_ids)
            .execute(&mut *self.db)
            .await?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self, user_ids), fields(count = user_ids.len()), err)]
    pub async fn clear_low_balance_notification_sent(&mut self, user_ids: &[UserId]) -> Result<()> {
        sqlx::query!(
            "UPDATE users SET low_balance_notification_sent = false WHERE id = ANY($1)",
            user_ids
        )
        .execute(&mut *self.db)
        .await?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self, user_ids), fields(count = user_ids.len()), err)]
    pub async fn mark_auto_topup_limit_notification_sent(&mut self, user_ids: &[UserId]) -> Result<()> {
        sqlx::query!(
            "UPDATE users SET auto_topup_limit_notification_sent = true WHERE id = ANY($1)",
            user_ids
        )
        .execute(&mut *self.db)
        .await?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self, user_ids), fields(count = user_ids.len()), err)]
    pub async fn clear_auto_topup_limit_notification_sent(&mut self, user_ids: &[UserId]) -> Result<()> {
        sqlx::query!(
            "UPDATE users SET auto_topup_limit_notification_sent = false WHERE id = ANY($1)",
            user_ids
        )
        .execute(&mut *self.db)
        .await?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    pub async fn poll_low_balance_users(&mut self) -> Result<Vec<LowBalanceUser>> {
        let rows = sqlx::query_as!(
            LowBalanceUser,
            r#"
            WITH clear_recovered AS (
                UPDATE users u
                SET low_balance_notification_sent = false
                FROM user_balance_checkpoints c
                WHERE u.id = c.user_id
                  AND u.low_balance_notification_sent = true
                  AND u.low_balance_threshold IS NOT NULL
                  AND c.balance >= u.low_balance_threshold
            )
            SELECT u.id, u.email, u.username, u.display_name,
                   u.low_balance_threshold::decimal(20, 9) as "low_balance_threshold!",
                   u.low_balance_notification_sent,
                   c.balance as "checkpoint_balance?"
            FROM users u
            LEFT JOIN user_balance_checkpoints c ON u.id = c.user_id
            WHERE u.id != '00000000-0000-0000-0000-000000000000'
              AND u.is_deleted = false
              AND u.low_balance_notification_sent = false
              AND u.low_balance_threshold IS NOT NULL
              AND c.balance < u.low_balance_threshold
            "#,
        )
        .fetch_all(&mut *self.db)
        .await?;

        Ok(rows)
    }

    #[instrument(level = "debug", skip(self), err)]
    pub async fn users_with_auto_topup_enabled(&mut self) -> Result<Vec<AutoTopupUser>> {
        let rows = sqlx::query_as!(
            AutoTopupUser,
            r#"
            SELECT u.id, u.email, u.username, u.display_name,
                   u.payment_provider_id as "payment_provider_id!",
                   u.auto_topup_threshold::decimal(20, 9) as "auto_topup_threshold!",
                   u.auto_topup_amount::decimal(20, 9) as "auto_topup_amount!",
                   c.balance as "checkpoint_balance?",
                   u.auto_topup_monthly_limit::decimal(20, 9) as "auto_topup_monthly_limit?",
                   u.auto_topup_limit_notification_sent,
                   u.auto_topup_soft_failure_count,
                   u.invoicing_enabled
            FROM users u
            LEFT JOIN user_balance_checkpoints c ON c.user_id = u.id
            WHERE u.id != '00000000-0000-0000-0000-000000000000'
              AND u.is_deleted = false
              AND u.auto_topup_threshold IS NOT NULL
              AND u.auto_topup_amount IS NOT NULL
              AND u.payment_provider_id IS NOT NULL
              AND (u.auto_topup_retry_after IS NULL OR u.auto_topup_retry_after <= NOW())
            "#,
        )
        .fetch_all(&mut *self.db)
        .await?;

        Ok(rows)
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id), expected_failure_count), err)]
    pub async fn apply_auto_topup_soft_decline(
        &mut self,
        user_id: UserId,
        expected_failure_count: i32,
        retry_delay: Duration,
    ) -> Result<bool> {
        let result = if expected_failure_count == 0 {
            sqlx::query(
                r#"
                UPDATE users
                SET auto_topup_soft_failure_count = 1,
                    auto_topup_retry_after = NOW() + make_interval(secs => $3),
                    updated_at = NOW()
                WHERE id = $1
                  AND auto_topup_soft_failure_count = $2
                  AND auto_topup_amount IS NOT NULL
                  AND auto_topup_threshold IS NOT NULL
                "#,
            )
            .bind(user_id)
            .bind(expected_failure_count)
            .bind(retry_delay.as_secs_f64())
            .execute(&mut *self.db)
            .await?
        } else {
            sqlx::query!(
                r#"
                UPDATE users
                SET auto_topup_amount = NULL,
                    auto_topup_threshold = NULL,
                    auto_topup_monthly_limit = NULL,
                    auto_topup_soft_failure_count = 0,
                    auto_topup_retry_after = NULL,
                    updated_at = NOW()
                WHERE id = $1
                  AND auto_topup_soft_failure_count = $2
                  AND auto_topup_amount IS NOT NULL
                  AND auto_topup_threshold IS NOT NULL
                "#,
                user_id,
                expected_failure_count,
            )
            .execute(&mut *self.db)
            .await?
        };

        Ok(result.rows_affected() == 1)
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn pause_auto_topup_after_provider_failure(&mut self, user_id: UserId, retry_delay: Duration) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE users
            SET auto_topup_retry_after = NOW() + make_interval(secs => $2),
                updated_at = NOW()
            WHERE id = $1
              AND auto_topup_amount IS NOT NULL
              AND auto_topup_threshold IS NOT NULL
              AND (auto_topup_retry_after IS NULL OR auto_topup_retry_after <= NOW())
            "#,
        )
        .bind(user_id)
        .bind(retry_delay.as_secs_f64())
        .execute(&mut *self.db)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn disable_auto_topup_after_hard_decline(&mut self, user_id: UserId) -> Result<bool> {
        let result = sqlx::query!(
            r#"
            UPDATE users
            SET auto_topup_amount = NULL,
                auto_topup_threshold = NULL,
                auto_topup_monthly_limit = NULL,
                auto_topup_soft_failure_count = 0,
                auto_topup_retry_after = NULL,
                updated_at = NOW()
            WHERE id = $1
              AND auto_topup_amount IS NOT NULL
              AND auto_topup_threshold IS NOT NULL
            "#,
            user_id,
        )
        .execute(&mut *self.db)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn reset_auto_topup_failure_state(&mut self, user_id: UserId) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE users
            SET auto_topup_soft_failure_count = 0,
                auto_topup_retry_after = NULL,
                updated_at = NOW()
            WHERE id = $1
              AND (auto_topup_soft_failure_count != 0 OR auto_topup_retry_after IS NOT NULL)
            "#,
            user_id,
        )
        .execute(&mut *self.db)
        .await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    pub async fn set_payment_provider_id_if_empty(&mut self, user_id: UserId, payment_provider_id: &str) -> Result<bool> {
        let rows_affected = sqlx::query!(
            "UPDATE users SET payment_provider_id = $1 WHERE id = $2 AND (payment_provider_id IS NULL OR payment_provider_id = '')",
            payment_provider_id,
            user_id
        )
        .execute(&mut *self.db)
        .await?
        .rows_affected();

        Ok(rows_affected > 0)
    }

    #[instrument(level = "debug", skip(self), err)]
    pub async fn set_verified(&mut self, user_id: UserId) -> Result<bool> {
        let rows_affected = sqlx::query!("UPDATE users SET verified = true WHERE id = $1 AND verified = false", user_id)
            .execute(&mut *self.db)
            .await?
            .rows_affected();

        Ok(rows_affected > 0)
    }

    #[instrument(level = "debug", skip(self), err)]
    pub async fn is_verified(&mut self, user_id: UserId) -> Result<bool> {
        let verified = sqlx::query_scalar!("SELECT verified FROM users WHERE id = $1", user_id)
            .fetch_optional(&mut *self.db)
            .await?
            .unwrap_or(false);

        Ok(verified)
    }
}

#[cfg(test)]
mod tests {
    use super::super::repository::Repository;
    use super::*;
    use crate::api::models::users::{Role, UserCreate};
    use crate::db::handlers::credits::Credits;
    use crate::db::models::credits::CreditTransactionCreateDBRequest;
    use rust_decimal::Decimal;
    use sqlx::PgPool;
    use std::str::FromStr;

    async fn create_standard_user(conn: &mut PgConnection, username: &str) -> UserDBResponse {
        Users::new(conn)
            .create(&UserCreateDBRequest::from(UserCreate {
                username: username.to_string(),
                email: format!("{username}@example.com"),
                display_name: None,
                avatar_url: None,
                roles: vec![Role::StandardUser],
            }))
            .await
            .unwrap()
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_user(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let mut repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest::from(UserCreate {
            username: "testuser".to_string(),
            email: "test@example.com".to_string(),
            display_name: Some("Test User".to_string()),
            avatar_url: None,
            roles: vec![Role::StandardUser],
        });

        let result = repo.create(&user_create).await;
        assert!(result.is_ok());

        let user = result.unwrap();
        assert_eq!(user.username, "testuser");
        assert_eq!(user.email, "test@example.com");
        assert_eq!(user.display_name, Some("Test User".to_string()));
        assert_eq!(user.roles, vec![Role::StandardUser]);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_user_hard_deletes_api_keys(pool: PgPool) {
        use crate::db::models::api_keys::ApiKeyCreateDBRequest;
        let mut conn = pool.acquire().await.unwrap();

        let user = {
            let mut repo = Users::new(&mut conn);
            repo.create(&UserCreateDBRequest::from(UserCreate {
                username: "doomed".to_string(),
                email: "doomed@example.com".to_string(),
                display_name: Some("Doomed".to_string()),
                avatar_url: None,
                roles: vec![Role::StandardUser],
            }))
            .await
            .unwrap()
        };

        {
            let mut keys = ApiKeys::new(&mut conn);
            keys.create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "doomed-key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Realtime,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();
        }

        let before: i64 = sqlx::query_scalar!(r#"SELECT COUNT(*) as "c!" FROM api_keys WHERE user_id = $1"#, user.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(before >= 1, "user should own at least the realtime key");

        let deleted = {
            let mut repo = Users::new(&mut conn);
            repo.delete(user.id).await.unwrap()
        };
        assert!(deleted);

        let after: i64 = sqlx::query_scalar!(r#"SELECT COUNT(*) as "c!" FROM api_keys WHERE user_id = $1"#, user.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(after, 0);
        let row = sqlx::query!(r#"SELECT is_deleted, email FROM users WHERE id = $1"#, user.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(row.is_deleted);
        assert!(row.email.starts_with("deleted-"));

        let again = {
            let mut repo = Users::new(&mut conn);
            repo.delete(user.id).await.unwrap()
        };
        assert!(!again);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_soft_deleted_connection_releases_api_key_for_user_deletion(pool: PgPool) {
        use crate::db::{handlers::connections::Connections, models::api_keys::ApiKeyCreateDBRequest};

        let mut conn = pool.acquire().await.unwrap();
        let user = create_standard_user(&mut conn, "connection-owner").await;
        let api_key = ApiKeys::new(&mut conn)
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "connection-key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Batch,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();
        let connection = Connections::new(&mut conn)
            .create(user.id, Some(api_key.id), "source", "s3", "connection", b"encrypted-config")
            .await
            .unwrap();

        assert!(Connections::new(&mut conn).soft_delete(connection.id).await.unwrap());

        let (deleted_at, connection_api_key_id): (Option<DateTime<Utc>>, Option<Uuid>) =
            sqlx::query_as("SELECT deleted_at, api_key_id FROM connections WHERE id = $1")
                .bind(connection.id)
                .fetch_one(&mut *conn)
                .await
                .unwrap();
        assert!(deleted_at.is_some());
        assert!(connection_api_key_id.is_none());

        assert!(Users::new(&mut conn).delete(user.id).await.unwrap());
        let remaining_keys: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM api_keys WHERE user_id = $1")
            .bind(user.id)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(remaining_keys, 0);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_user_releases_api_key_from_active_connection(pool: PgPool) {
        use crate::db::{handlers::connections::Connections, models::api_keys::ApiKeyCreateDBRequest};

        let mut conn = pool.acquire().await.unwrap();
        let user = create_standard_user(&mut conn, "active-connection-owner").await;
        let api_key = ApiKeys::new(&mut conn)
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "active-connection-key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Batch,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();
        let connection = Connections::new(&mut conn)
            .create(user.id, Some(api_key.id), "source", "s3", "connection", b"encrypted-config")
            .await
            .unwrap();
        let sync_id: Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO sync_operations (connection_id, status, strategy, triggered_by, sync_config)
            VALUES ($1, 'pending', 'snapshot', $2, '{}')
            RETURNING id
            "#,
        )
        .bind(connection.id)
        .bind(user.id)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        let sync_entry_id: Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO sync_entries (sync_id, connection_id, external_key, status)
            VALUES ($1, $2, 'pending.jsonl', 'pending')
            RETURNING id
            "#,
        )
        .bind(sync_id)
        .bind(connection.id)
        .fetch_one(&mut *conn)
        .await
        .unwrap();

        assert!(Users::new(&mut conn).delete(user.id).await.unwrap());

        let (connection_api_key_id, deleted_at): (Option<Uuid>, Option<DateTime<Utc>>) =
            sqlx::query_as("SELECT api_key_id, deleted_at FROM connections WHERE id = $1")
                .bind(connection.id)
                .fetch_one(&mut *conn)
                .await
                .unwrap();
        assert!(connection_api_key_id.is_none());
        assert!(deleted_at.is_some());

        let sync_status: String = sqlx::query_scalar("SELECT status FROM sync_operations WHERE id = $1")
            .bind(sync_id)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(sync_status, "deleted");

        let entry_status: String = sqlx::query_scalar("SELECT status FROM sync_entries WHERE id = $1")
            .bind(sync_entry_id)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(entry_status, "deleted");

        let remaining_keys: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM api_keys WHERE user_id = $1")
            .bind(user.id)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(remaining_keys, 0);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_user_soft_deletes_attributed_keys_preserving_audit_trail(pool: PgPool) {
        use crate::db::{
            handlers::organizations::Organizations,
            models::{api_keys::ApiKeyCreateDBRequest, organizations::OrganizationCreateDBRequest},
        };

        let mut conn = pool.acquire().await.unwrap();
        let deleted_user = create_standard_user(&mut conn, "deleted-key-holder").await;
        let retained_user = create_standard_user(&mut conn, "retained-key-holder").await;
        let key_owner = Organizations::new(&mut conn)
            .create(
                &OrganizationCreateDBRequest {
                    name: "shared-key-owner".to_string(),
                    email: "org@example.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: retained_user.id,
                },
                &[Role::StandardUser],
            )
            .await
            .unwrap();

        let (deleted_user_key, retained_user_key) = {
            let mut keys = ApiKeys::new(&mut conn);
            let deleted_user_key = keys
                .create(&ApiKeyCreateDBRequest {
                    user_id: key_owner.id,
                    name: "deleted-user-key".to_string(),
                    description: None,
                    purpose: ApiKeyPurpose::Realtime,
                    created_by: deleted_user.id,
                    spend_limit: None,
                    spend_limit_interval: None,
                })
                .await
                .unwrap();
            let retained_user_key = keys
                .create(&ApiKeyCreateDBRequest {
                    user_id: key_owner.id,
                    name: "retained-user-key".to_string(),
                    description: None,
                    purpose: ApiKeyPurpose::Realtime,
                    created_by: retained_user.id,
                    spend_limit: None,
                    spend_limit_interval: None,
                })
                .await
                .unwrap();
            (deleted_user_key, retained_user_key)
        };

        assert!(Users::new(&mut conn).delete(deleted_user.id).await.unwrap());

        let audit_record: (UserId, UserId, bool) = sqlx::query_as("SELECT user_id, created_by, is_deleted FROM api_keys WHERE id = $1")
            .bind(deleted_user_key.id)
            .fetch_one(&pool)
            .await
            .expect("revoked keys must remain available for audit attribution");
        assert_eq!(audit_record, (key_owner.id, deleted_user.id, true));
        assert!(
            ApiKeys::new(&mut conn)
                .get_user_id_by_secret(&deleted_user_key.secret)
                .await
                .unwrap()
                .is_none(),
            "the deleted user's attributed key must no longer authenticate"
        );
        let retained_key_is_active: bool = sqlx::query_scalar!(
            "SELECT EXISTS(SELECT 1 FROM api_keys WHERE id = $1 AND is_deleted = false) as \"exists!\"",
            retained_user_key.id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(retained_key_is_active, "keys attributed to other users must remain active");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_user_by_email(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let mut repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest::from(UserCreate {
            username: "emailuser".to_string(),
            email: "email@example.com".to_string(),
            display_name: None,
            avatar_url: None,
            roles: vec![Role::StandardUser],
        });

        let created_user = repo.create(&user_create).await.unwrap();

        let found_user = repo.get_user_by_email("email@example.com").await.unwrap();
        assert!(found_user.is_some());

        let found_user = found_user.unwrap();
        assert_eq!(found_user.id, created_user.id);
        assert_eq!(found_user.username, "emailuser");
        assert_eq!(found_user.roles, vec![Role::StandardUser]);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_system_user(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let admin_user = crate::test::utils::get_system_user(&mut conn).await;
        assert_eq!(admin_user.username, "system");
        assert_eq!(admin_user.email, "system@internal");
        assert_eq!(admin_user.id.to_string(), "00000000-0000-0000-0000-000000000000");
        assert!(admin_user.is_admin);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_update_user_roles_always_includes_standard_user(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let mut repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest::from(UserCreate {
            username: "roleuser".to_string(),
            email: "roleuser@example.com".to_string(),
            display_name: None,
            avatar_url: None,
            roles: vec![Role::StandardUser, Role::PlatformManager],
        });

        let created_user = repo.create(&user_create).await.unwrap();
        assert_eq!(created_user.roles.len(), 2);
        assert!(created_user.roles.contains(&Role::StandardUser));
        assert!(created_user.roles.contains(&Role::PlatformManager));

        let update_request = UserUpdateDBRequest {
            display_name: None,
            avatar_url: None,
            roles: Some(vec![Role::RequestViewer]),
            password_hash: None,
            batch_notifications_enabled: None,
            low_balance_threshold: None,
            auto_topup_amount: None,
            auto_topup_threshold: None,
            auto_topup_monthly_limit: None,
            zero_data_retention: None,
            self_hosted_only: None,
            toleration_preset: Default::default(),
        };

        let updated_user = repo.update(created_user.id, &update_request).await.unwrap();

        assert_eq!(updated_user.roles.len(), 2);
        assert!(updated_user.roles.contains(&Role::StandardUser));
        assert!(updated_user.roles.contains(&Role::RequestViewer));
        assert!(!updated_user.roles.contains(&Role::PlatformManager));

        let update_request = UserUpdateDBRequest {
            display_name: None,
            avatar_url: None,
            roles: Some(vec![]),
            password_hash: None,
            batch_notifications_enabled: None,
            low_balance_threshold: None,
            auto_topup_amount: None,
            auto_topup_threshold: None,
            auto_topup_monthly_limit: None,
            zero_data_retention: None,
            self_hosted_only: None,
            toleration_preset: Default::default(),
        };

        let updated_user = repo.update(created_user.id, &update_request).await.unwrap();

        assert_eq!(updated_user.roles.len(), 1);
        assert!(updated_user.roles.contains(&Role::StandardUser));
    }

    async fn create_user_with_balance(pool: &PgPool, balance: &str, threshold: Option<f32>) -> UserId {
        let mut conn = pool.acquire().await.unwrap();
        let mut repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest::from(UserCreate {
            username: format!("lowbal_{}", Uuid::new_v4().simple()),
            email: format!("lowbal_{}@example.com", Uuid::new_v4().simple()),
            display_name: Some("Low Balance Test".to_string()),
            avatar_url: None,
            roles: vec![Role::StandardUser],
        });
        let user = repo.create(&user_create).await.unwrap();

        if threshold.is_some() {
            let update = UserUpdateDBRequest {
                display_name: None,
                avatar_url: None,
                roles: None,
                password_hash: None,
                batch_notifications_enabled: None,
                low_balance_threshold: Some(threshold),
                auto_topup_amount: None,
                auto_topup_threshold: None,
                auto_topup_monthly_limit: None,
                zero_data_retention: None,
                self_hosted_only: None,
                toleration_preset: Default::default(),
            };
            repo.update(user.id, &update).await.unwrap();
        }

        let amount = Decimal::from_str(balance).unwrap();
        if amount > Decimal::ZERO {
            drop(conn);
            let mut conn = pool.acquire().await.unwrap();
            let mut credits = Credits::new(&mut conn);
            let grant = CreditTransactionCreateDBRequest::admin_grant(user.id, user.id, amount, None);
            credits.create_transaction(&grant).await.unwrap();
        }

        user.id
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_users_with_threshold_skips_users_without_threshold(pool: PgPool) {
        create_user_with_balance(&pool, "1.00", None).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut users = Users::new(&mut conn);
        let result = users.users_with_low_balance_threshold().await.unwrap();
        assert!(result.is_empty());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_users_with_threshold_returns_user_with_checkpoint(pool: PgPool) {
        let user_id = create_user_with_balance(&pool, "1.50", Some(2.0)).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut users = Users::new(&mut conn);
        let result = users.users_with_low_balance_threshold().await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, user_id);
        assert_eq!(result[0].low_balance_threshold, Decimal::from_str("2.0").unwrap());
        assert!(!result[0].low_balance_notification_sent);
        assert_eq!(result[0].checkpoint_balance, Some(Decimal::from_str("1.50").unwrap()));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_users_with_threshold_returns_user_without_credits(pool: PgPool) {
        let user_id = create_user_with_balance(&pool, "0", Some(2.0)).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut users = Users::new(&mut conn);
        let result = users.users_with_low_balance_threshold().await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, user_id);
        assert_eq!(result[0].checkpoint_balance, Some(Decimal::ZERO));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_mark_and_clear_low_balance_notification(pool: PgPool) {
        let user_id = create_user_with_balance(&pool, "1.00", Some(2.0)).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut users = Users::new(&mut conn);

        let result = users.users_with_low_balance_threshold().await.unwrap();
        assert!(!result[0].low_balance_notification_sent);

        users.mark_low_balance_notification_sent(&[user_id]).await.unwrap();
        let result = users.users_with_low_balance_threshold().await.unwrap();
        assert!(result[0].low_balance_notification_sent);

        let low = users.poll_low_balance_users().await.unwrap();
        assert!(low.is_empty());

        users.clear_low_balance_notification_sent(&[user_id]).await.unwrap();
        let result = users.users_with_low_balance_threshold().await.unwrap();
        assert!(!result[0].low_balance_notification_sent);

        let low = users.poll_low_balance_users().await.unwrap();
        assert_eq!(low.len(), 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_poll_low_balance_clears_flag_after_topup(pool: PgPool) {
        let user_id = create_user_with_balance(&pool, "1.00", Some(2.0)).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut users = Users::new(&mut conn);
        let low = users.poll_low_balance_users().await.unwrap();
        assert_eq!(low.len(), 1);
        users.mark_low_balance_notification_sent(&[user_id]).await.unwrap();
        drop(conn);

        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);
        let grant = CreditTransactionCreateDBRequest::admin_grant(user_id, user_id, Decimal::from_str("10.00").unwrap(), None);
        credits.create_transaction(&grant).await.unwrap();
        drop(conn);

        let mut conn = pool.acquire().await.unwrap();
        let mut users = Users::new(&mut conn);
        let low = users.poll_low_balance_users().await.unwrap();
        assert!(low.is_empty());

        let user = users.get_by_id(user_id).await.unwrap().unwrap();
        assert!(!user.low_balance_notification_sent);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_poll_low_balance_full_cycle(pool: PgPool) {
        let user_id = create_user_with_balance(&pool, "100.00", Some(2.0)).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut users = Users::new(&mut conn);
        let low = users.poll_low_balance_users().await.unwrap();
        assert!(low.is_empty(), "User above threshold should not appear");
        drop(conn);

        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);
        let deduct = CreditTransactionCreateDBRequest {
            user_id,
            transaction_type: crate::db::models::credits::CreditTransactionType::AdminRemoval,
            amount: Decimal::from_str("99.00").unwrap(),
            source_id: Uuid::new_v4().to_string(),
            description: None,
            fusillade_batch_id: None,
            api_key_id: None,
        };
        credits.create_transaction(&deduct).await.unwrap();
        drop(conn);

        let mut conn = pool.acquire().await.unwrap();
        let mut users = Users::new(&mut conn);
        let low = users.poll_low_balance_users().await.unwrap();
        assert_eq!(low.len(), 1, "User below threshold should appear");
        assert_eq!(low[0].id, user_id);

        users.mark_low_balance_notification_sent(&[user_id]).await.unwrap();
        let low = users.poll_low_balance_users().await.unwrap();
        assert!(low.is_empty(), "Notified user should not appear again");
        drop(conn);

        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);
        let grant = CreditTransactionCreateDBRequest::admin_grant(user_id, user_id, Decimal::from_str("50.00").unwrap(), None);
        credits.create_transaction(&grant).await.unwrap();
        drop(conn);

        let mut conn = pool.acquire().await.unwrap();
        let mut users = Users::new(&mut conn);
        let low = users.poll_low_balance_users().await.unwrap();
        assert!(low.is_empty(), "Topped-up user should not appear");
        drop(conn);

        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);
        let deduct2 = CreditTransactionCreateDBRequest {
            user_id,
            transaction_type: crate::db::models::credits::CreditTransactionType::AdminRemoval,
            amount: Decimal::from_str("50.00").unwrap(),
            source_id: Uuid::new_v4().to_string(),
            description: None,
            fusillade_batch_id: None,
            api_key_id: None,
        };
        credits.create_transaction(&deduct2).await.unwrap();
        drop(conn);

        let mut conn = pool.acquire().await.unwrap();
        let mut users = Users::new(&mut conn);
        let low = users.poll_low_balance_users().await.unwrap();
        assert_eq!(low.len(), 1, "User should be notifiable again after recovery + re-drop");
        assert_eq!(low[0].id, user_id);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_poll_low_balance_negative_balance(pool: PgPool) {
        let user_id = create_user_with_balance(&pool, "5.00", Some(2.0)).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);
        let deduct = CreditTransactionCreateDBRequest {
            user_id,
            transaction_type: crate::db::models::credits::CreditTransactionType::AdminRemoval,
            amount: Decimal::from_str("10.00").unwrap(),
            source_id: Uuid::new_v4().to_string(),
            description: None,
            fusillade_batch_id: None,
            api_key_id: None,
        };
        credits.create_transaction(&deduct).await.unwrap();
        drop(conn);

        let mut conn = pool.acquire().await.unwrap();
        let mut users = Users::new(&mut conn);
        let low = users.poll_low_balance_users().await.unwrap();
        assert_eq!(low.len(), 1);
        assert_eq!(low[0].id, user_id);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_update_low_balance_threshold_resets_flag(pool: PgPool) {
        let user_id = create_user_with_balance(&pool, "1.00", Some(2.0)).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut users = Users::new(&mut conn);

        users.mark_low_balance_notification_sent(&[user_id]).await.unwrap();

        let update = UserUpdateDBRequest {
            display_name: None,
            avatar_url: None,
            roles: None,
            password_hash: None,
            batch_notifications_enabled: None,
            low_balance_threshold: Some(Some(5.0)),
            auto_topup_amount: None,
            auto_topup_threshold: None,
            auto_topup_monthly_limit: None,
            zero_data_retention: None,
            self_hosted_only: None,
            toleration_preset: Default::default(),
        };
        let updated = users.update(user_id, &update).await.unwrap();
        assert!(!updated.low_balance_notification_sent);
        assert_eq!(updated.low_balance_threshold, Some(5.0));

        let result = users.users_with_low_balance_threshold().await.unwrap();
        assert_eq!(result[0].low_balance_threshold, Decimal::from_str("5.0").unwrap());
        assert!(!result[0].low_balance_notification_sent);
    }

    #[dwctl_test_macros::test]
    async fn test_users_with_auto_topup_enabled(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let user_id = {
            let mut repo = Users::new(&mut conn);
            let user_create = UserCreateDBRequest::from(UserCreate {
                username: format!("autotopup_{}", Uuid::new_v4().simple()),
                email: format!("autotopup_{}@example.com", Uuid::new_v4().simple()),
                display_name: Some("Auto Topup Test".to_string()),
                avatar_url: None,
                roles: vec![Role::StandardUser],
            });
            repo.create(&user_create).await.unwrap().id
        };

        sqlx::query!(
            r#"UPDATE users SET
                auto_topup_amount = 25.0,
                auto_topup_threshold = 5.0,
                payment_provider_id = 'cus_test_456'
            WHERE id = $1"#,
            user_id
        )
        .execute(&pool)
        .await
        .unwrap();

        let mut users = Users::new(&mut conn);
        let result = users.users_with_auto_topup_enabled().await.unwrap();

        assert_eq!(result.len(), 1, "Should find exactly one auto-topup user");
        assert_eq!(result[0].id, user_id);
        assert_eq!(result[0].auto_topup_amount, Decimal::from_str("25.0").unwrap());
        assert_eq!(result[0].auto_topup_threshold, Decimal::from_str("5.0").unwrap());
        assert_eq!(result[0].payment_provider_id, "cus_test_456");
    }

    #[dwctl_test_macros::test]
    async fn test_set_verified_flips_flag_idempotently(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let user_id = {
            let mut repo = Users::new(&mut conn);
            let create = UserCreateDBRequest::from(UserCreate {
                username: format!("verify_{}", Uuid::new_v4().simple()),
                email: format!("verify_{}@example.com", Uuid::new_v4().simple()),
                display_name: Some("Verify Me".to_string()),
                avatar_url: None,
                roles: vec![Role::StandardUser],
            });
            repo.create(&create).await.unwrap().id
        };

        let initial = sqlx::query_scalar!("SELECT verified FROM users WHERE id = $1", user_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(!initial, "users should start unverified");

        let mut users = Users::new(&mut conn);
        let changed = users.set_verified(user_id).await.unwrap();
        assert!(changed, "first set_verified call should flip the flag");

        let flipped = sqlx::query_scalar!("SELECT verified FROM users WHERE id = $1", user_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(flipped);

        let changed_again = users.set_verified(user_id).await.unwrap();
        assert!(!changed_again, "second call should be a no-op since user is already verified");
    }

    #[dwctl_test_macros::test]
    async fn test_users_with_auto_topup_enabled_excludes_incomplete(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let user_id = {
            let mut repo = Users::new(&mut conn);
            let user_create = UserCreateDBRequest::from(UserCreate {
                username: format!("autotopup_{}", Uuid::new_v4().simple()),
                email: format!("autotopup_{}@example.com", Uuid::new_v4().simple()),
                display_name: Some("Incomplete Topup Test".to_string()),
                avatar_url: None,
                roles: vec![Role::StandardUser],
            });
            repo.create(&user_create).await.unwrap().id
        };

        sqlx::query!(
            r#"UPDATE users SET
                auto_topup_amount = 25.0,
                auto_topup_threshold = 5.0
            WHERE id = $1"#,
            user_id
        )
        .execute(&pool)
        .await
        .unwrap();

        let mut users = Users::new(&mut conn);
        let result = users.users_with_auto_topup_enabled().await.unwrap();

        assert!(result.is_empty(), "Should not include user with missing payment_provider_id");
    }

    #[dwctl_test_macros::test]
    async fn test_auto_topup_decline_first_soft_failure_uses_configured_delay(pool: PgPool) {
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        sqlx::query!(
            r#"
            UPDATE users
            SET auto_topup_amount = 25.0,
                auto_topup_threshold = 10.0,
                payment_provider_id = 'cus_soft_decline'
            WHERE id = $1
            "#,
            user.id
        )
        .execute(&pool)
        .await
        .unwrap();

        let started_at = Utc::now();
        let mut conn = pool.acquire().await.unwrap();
        let retry_delay = Duration::from_secs(90);
        let applied = Users::new(&mut conn)
            .apply_auto_topup_soft_decline(user.id, 0, retry_delay)
            .await
            .unwrap();

        assert!(applied);
        let row = sqlx::query!(
            r#"
            SELECT auto_topup_amount, auto_topup_threshold,
                   auto_topup_soft_failure_count, auto_topup_retry_after
            FROM users
            WHERE id = $1
            "#,
            user.id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.auto_topup_amount, Some(25.0));
        assert_eq!(row.auto_topup_threshold, Some(10.0));
        assert_eq!(row.auto_topup_soft_failure_count, 1);
        let retry_after = row.auto_topup_retry_after.unwrap();
        assert!(retry_after >= started_at + chrono::Duration::seconds(89));
        assert!(retry_after <= Utc::now() + chrono::Duration::seconds(91));

        let candidates = Users::new(&mut conn).users_with_auto_topup_enabled().await.unwrap();
        assert!(candidates.iter().all(|candidate| candidate.id != user.id));
    }

    #[dwctl_test_macros::test]
    async fn test_auto_topup_provider_failure_uses_configured_delay(pool: PgPool) {
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        sqlx::query!(
            r#"
            UPDATE users
            SET auto_topup_amount = 25.0,
                auto_topup_threshold = 10.0,
                payment_provider_id = 'cus_provider_failure'
            WHERE id = $1
            "#,
            user.id
        )
        .execute(&pool)
        .await
        .unwrap();

        let started_at = Utc::now();
        let mut conn = pool.acquire().await.unwrap();
        let applied = Users::new(&mut conn)
            .pause_auto_topup_after_provider_failure(user.id, Duration::from_secs(45))
            .await
            .unwrap();

        assert!(applied);
        let retry_after = sqlx::query_scalar!("SELECT auto_topup_retry_after FROM users WHERE id = $1", user.id)
            .fetch_one(&pool)
            .await
            .unwrap()
            .unwrap();
        assert!(retry_after >= started_at + chrono::Duration::seconds(44));
        assert!(retry_after <= Utc::now() + chrono::Duration::seconds(46));
        assert!(
            !Users::new(&mut conn)
                .pause_auto_topup_after_provider_failure(user.id, Duration::from_secs(45))
                .await
                .unwrap(),
            "a duplicate provider failure must not extend the active pause"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_auto_topup_decline_second_soft_failure_disables_with_compare_and_set(pool: PgPool) {
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        sqlx::query!(
            r#"
            UPDATE users
            SET auto_topup_amount = 25.0,
                auto_topup_threshold = 10.0,
                auto_topup_monthly_limit = 100.0,
                auto_topup_soft_failure_count = 1,
                auto_topup_retry_after = NOW() - INTERVAL '1 minute',
                payment_provider_id = 'cus_soft_decline'
            WHERE id = $1
            "#,
            user.id
        )
        .execute(&pool)
        .await
        .unwrap();

        let mut conn = pool.acquire().await.unwrap();
        assert!(
            Users::new(&mut conn)
                .apply_auto_topup_soft_decline(user.id, 1, Duration::from_secs(60))
                .await
                .unwrap()
        );
        assert!(
            !Users::new(&mut conn)
                .apply_auto_topup_soft_decline(user.id, 1, Duration::from_secs(60))
                .await
                .unwrap(),
            "a stale duplicate transition must not apply twice"
        );

        let row = sqlx::query!(
            r#"
            SELECT auto_topup_amount, auto_topup_threshold,
                   auto_topup_monthly_limit, auto_topup_soft_failure_count,
                   auto_topup_retry_after
            FROM users
            WHERE id = $1
            "#,
            user.id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.auto_topup_amount, None);
        assert_eq!(row.auto_topup_threshold, None);
        assert_eq!(row.auto_topup_monthly_limit, None);
        assert_eq!(row.auto_topup_soft_failure_count, 0);
        assert_eq!(row.auto_topup_retry_after, None);
    }

    #[dwctl_test_macros::test]
    async fn test_auto_topup_decline_hard_failure_disables_immediately(pool: PgPool) {
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        sqlx::query!(
            r#"
            UPDATE users
            SET auto_topup_amount = 25.0,
                auto_topup_threshold = 10.0,
                auto_topup_monthly_limit = 100.0,
                payment_provider_id = 'cus_hard_decline'
            WHERE id = $1
            "#,
            user.id
        )
        .execute(&pool)
        .await
        .unwrap();

        let mut conn = pool.acquire().await.unwrap();
        assert!(Users::new(&mut conn).disable_auto_topup_after_hard_decline(user.id).await.unwrap());
        assert!(!Users::new(&mut conn).disable_auto_topup_after_hard_decline(user.id).await.unwrap());

        let row = sqlx::query!(
            "SELECT auto_topup_amount, auto_topup_threshold, auto_topup_monthly_limit FROM users WHERE id = $1",
            user.id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.auto_topup_amount, None);
        assert_eq!(row.auto_topup_threshold, None);
        assert_eq!(row.auto_topup_monthly_limit, None);
    }

    #[dwctl_test_macros::test]
    async fn test_auto_topup_decline_reset_clears_failure_state(pool: PgPool) {
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        sqlx::query!(
            r#"
            UPDATE users
            SET auto_topup_soft_failure_count = 1,
                auto_topup_retry_after = NOW() + INTERVAL '24 hours'
            WHERE id = $1
            "#,
            user.id
        )
        .execute(&pool)
        .await
        .unwrap();

        let mut conn = pool.acquire().await.unwrap();
        Users::new(&mut conn).reset_auto_topup_failure_state(user.id).await.unwrap();

        let row = sqlx::query!(
            "SELECT auto_topup_soft_failure_count, auto_topup_retry_after FROM users WHERE id = $1",
            user.id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.auto_topup_soft_failure_count, 0);
        assert_eq!(row.auto_topup_retry_after, None);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_users_excludes_organizations(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let mut repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest::from(UserCreate {
            username: "individual".to_string(),
            email: "individual@example.com".to_string(),
            display_name: Some("Individual User".to_string()),
            avatar_url: None,
            roles: vec![Role::StandardUser],
        });
        repo.create(&user_create).await.unwrap();

        sqlx::query!(
            "INSERT INTO users (id, username, email, auth_source, user_type) VALUES ($1, $2, $3, 'organization', 'organization')",
            uuid::Uuid::new_v4(),
            "acme-org",
            "billing@acme.example.com",
        )
        .execute(&pool)
        .await
        .unwrap();

        let filter = UserFilter::new(0, 100);
        let users = repo.list(&filter).await.unwrap();

        for u in &users {
            assert_eq!(u.user_type, "individual", "Organization users should not appear in list");
        }
        assert!(users.iter().any(|u| u.username == "individual"));
        assert!(!users.iter().any(|u| u.username == "acme-org"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_count_users_excludes_organizations(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let mut repo = Users::new(&mut conn);

        let initial_count = repo.count(&UserFilter::new(0, 100)).await.unwrap();

        repo.create(&UserCreateDBRequest::from(UserCreate {
            username: "countuser".to_string(),
            email: "countuser@example.com".to_string(),
            display_name: None,
            avatar_url: None,
            roles: vec![Role::StandardUser],
        }))
        .await
        .unwrap();

        sqlx::query!(
            "INSERT INTO users (id, username, email, auth_source, user_type) VALUES ($1, $2, $3, 'organization', 'organization')",
            uuid::Uuid::new_v4(),
            "count-org",
            "count-org@example.com",
        )
        .execute(&pool)
        .await
        .unwrap();

        let new_count = repo.count(&UserFilter::new(0, 100)).await.unwrap();
        assert_eq!(new_count, initial_count + 1, "Count should increase by 1 (the individual), not 2");
    }
}
