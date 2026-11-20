
use crate::crypto::hash_api_key;
use crate::db::errors::DbError;
use crate::{
    api::models::users::{AuthMethod, CurrentUser, Role},
    auth::session,
    db::handlers::{Repository, Users},
    errors::{Error, Result},
};
use axum::{extract::FromRequestParts, http::request::Parts};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tracing::{debug, instrument, trace};

type AuthSuccess = (CurrentUser, Option<DateTime<Utc>>);

#[instrument(skip(parts, config, db))]
async fn try_jwt_session_auth(
    parts: &axum::http::request::Parts,
    config: &crate::config::Config,
    db: &PgPool,
) -> Option<Result<AuthSuccess>> {
    let cookie_header = parts.headers.get(axum::http::header::COOKIE)?;

    let cookie_str = match cookie_header.to_str() {
        Ok(s) => s,
        Err(e) => {
            return Some(Err(Error::BadRequest {
                message: format!("Invalid cookie header: {e}"),
            }));
        }
    };
    let cookie_name = &config.auth.native.session.cookie_name;

    for cookie in cookie_str.split(';') {
        let cookie = cookie.trim();
        if let Some((name, value)) = cookie.split_once('=')
            && name == cookie_name
        {
            let user_id = match session::verify_session_token(value, config) {
                Ok(id) => id,
                Err(_) => {
                    continue;
                }
            };

            let mut conn = match db.acquire().await {
                Ok(conn) => conn,
                Err(e) => return Some(Err(DbError::from(e).into())),
            };
            let mut user_repo = Users::new(&mut conn);

            let user = match user_repo.get_by_id(user_id).await {
                Ok(Some(user)) => user,
                Ok(None) => {
                    return Some(Err(Error::Unauthenticated {
                        message: Some("User no longer exists".to_string()),
                    }));
                }
                Err(e) => return Some(Err(Error::Database(e))),
            };

            let last_login = user.last_login;
            return Some(Ok((
                CurrentUser {
                    auth_method: AuthMethod::NativeSession,
                    id: user.id,
                    username: user.username,
                    email: user.email,
                    is_admin: user.is_admin,
                    roles: user.roles,
                    display_name: user.display_name,
                    avatar_url: user.avatar_url,
                    payment_provider_id: user.payment_provider_id,
                    organizations: vec![],
                    active_organization: None,
                    api_key_id: None,
                },
                last_login,
            )));
        }
    }
    None
}

#[instrument(skip(parts, state), level = "TRACE")]
async fn try_proxy_header_auth<P: sqlx_pool_router::PoolProvider + Clone + Send + Sync + 'static>(
    parts: &axum::http::request::Parts,
    state: &crate::AppState<P>,
) -> Option<Result<AuthSuccess>> {
    let config = state.current_config();
    let db: &PgPool = &state.db.write();
    tracing::trace!("Trying proxy header auth, config: {:?}", config.auth.proxy_header);
    let external_user_id = parts
        .headers
        .get(&config.auth.proxy_header.header_name)
        .and_then(|h| h.to_str().ok())?;

    let user_email = parts
        .headers
        .get(&config.auth.proxy_header.email_header_name)
        .and_then(|h| h.to_str().ok())
        .unwrap_or(external_user_id);

    let groups_and_provider = if config.auth.proxy_header.import_idp_groups {
        parts
            .headers
            .get(&config.auth.proxy_header.groups_field_name)
            .and_then(|h| h.to_str().ok())
            .and_then(|group_string| {
                let groups: Vec<String> = group_string
                    .split(',')
                    .map(|g| g.trim().to_string())
                    .filter(|g| !config.auth.proxy_header.blacklisted_sso_groups.contains(g))
                    .collect();

                if groups.is_empty() {
                    None
                } else {
                    let provider = parts
                        .headers
                        .get(&config.auth.proxy_header.provider_field_name)
                        .and_then(|h| h.to_str().ok())
                        .unwrap_or("unknown");
                    Some((groups, provider))
                }
            })
    } else {
        None
    };
    tracing::trace!(
        "Proxy header auth: external_user_id='{}', email='{}', groups_and_provider={:?}",
        external_user_id,
        user_email,
        groups_and_provider
    );

    let mut tx = match db.begin().await {
        Ok(tx) => tx,
        Err(e) => return Some(Err(DbError::from(e).into())),
    };
    let mut user_repo = Users::new(&mut tx);
    let mut is_new_user = false;

    let user_result = if config.auth.proxy_header.auto_create_users {
        match user_repo
            .get_or_create_proxy_header_user(external_user_id, user_email, groups_and_provider, &config.auth.default_user_roles)
            .await
        {
            Ok((user, was_created)) => {
                if was_created {
                    is_new_user = true;
                    let initial_credits = config.credits.initial_credits_for_standard_users;
                    if initial_credits > rust_decimal::Decimal::ZERO && user.roles.contains(&Role::StandardUser) {
                        use crate::db::handlers::credits::Credits;
                        use crate::db::models::credits::CreditTransactionCreateDBRequest;

                        let mut credits_repo = Credits::new(&mut tx);
                        let request = CreditTransactionCreateDBRequest::admin_grant(
                            user.id,
                            uuid::Uuid::nil(),
                            initial_credits,
                            Some("Initial credits on account creation".to_string()),
                        );
                        if let Err(e) = credits_repo.create_transaction(&request).await {
                            return Some(Err(Error::Database(e)));
                        }
                    }

                    if let Some(domain) = crate::auth::utils::email_domain(&user.email)
                        && !config.auth.is_personal_email_domain(&domain)
                    {
                        use crate::db::handlers::Organizations;

                        let mut org_repo = Organizations::new(&mut tx);
                        match org_repo.find_by_domain(&domain).await {
                            Ok(Some(org)) => {
                                match org_repo.auto_join_enabled(org.id).await {
                                    Ok(true) => {
                                        match org_repo.add_member(org.id, user.id, "member").await {
                                            Ok(membership) => {
                                                if let Err(e) = org_repo.grant_default_member_roles(membership.id).await {
                                                    tracing::warn!(
                                                        "Failed to apply default member roles after auto-join for domain {domain}: {e}"
                                                    );
                                                }
                                            }
                                            Err(e) => {
                                                tracing::warn!("Failed to auto-join user into org for domain {domain}: {e}");
                                            }
                                        }
                                    }
                                    Ok(false) => {
                                        tracing::debug!(
                                            "Organization claims domain {domain} but auto-join is off; leaving the choice to the user"
                                        );
                                    }
                                    Err(e) => {
                                        tracing::warn!("Failed to read auto-join setting for org on domain {domain}: {e}");
                                    }
                                }
                            }
                            Ok(None) => {
                                tracing::debug!("No organization claims domain {domain}; leaving user in personal context");
                            }
                            Err(e) => {
                                tracing::warn!("Failed to look up org by domain {domain}: {e}");
                            }
                        }
                    }
                }

                let last_login = user.last_login;
                Some((
                    CurrentUser {
                        auth_method: AuthMethod::TrustedSso,
                        id: user.id,
                        username: user.username,
                        email: user.email,
                        is_admin: user.is_admin,
                        roles: user.roles,
                        display_name: user.display_name,
                        avatar_url: user.avatar_url,
                        payment_provider_id: user.payment_provider_id,
                        organizations: vec![],
                        active_organization: None,
                        api_key_id: None,
                    },
                    last_login,
                ))
            }
            Err(e) => return Some(Err(Error::Database(e))),
        }
    } else {
        debug!("Auto-create disabled, looking up existing user");
        match user_repo.get_user_by_external_user_id(external_user_id).await {
            Ok(Some(user)) => {
                debug!("Found existing user");
                let last_login = user.last_login;
                Some((
                    CurrentUser {
                        auth_method: AuthMethod::TrustedSso,
                        id: user.id,
                        username: user.username,
                        email: user.email,
                        is_admin: user.is_admin,
                        roles: user.roles,
                        display_name: user.display_name,
                        avatar_url: user.avatar_url,
                        payment_provider_id: user.payment_provider_id,
                        organizations: vec![],
                        active_organization: None,
                        api_key_id: None,
                    },
                    last_login,
                ))
            }
            Ok(None) => {
                debug!("User not found and auto-create disabled");
                None
            }
            Err(e) => return Some(Err(Error::Database(e))),
        }
    };

    match tx.commit().await {
        Ok(_) => {}
        Err(e) => return Some(Err(DbError::from(e).into())),
    }


    if is_new_user
        && config.sample_files.enabled
        && config.batches.enabled
        && let Some((ref user, _)) = user_result
    {
        let state_clone = state.clone();
        let user_id = user.id;
        tokio::spawn(async move {
            if let Err(e) = crate::api::handlers::auth::create_sample_files_for_new_user(&state_clone, user_id).await {
                tracing::warn!(user_id = %user_id, error = %e, "Failed to create sample files for new user");
            }
        });
    }

    user_result.map(Ok)
}

#[instrument(skip(parts, db))]
async fn try_api_key_auth(parts: &axum::http::request::Parts, db: &PgPool) -> Option<Result<AuthSuccess>> {
    let auth_header = match parts.headers.get(axum::http::header::AUTHORIZATION) {
        Some(header) => header,
        None if parts.headers.contains_key("x-api-key") => {
            return Some(Err(Error::Unauthenticated { message: None }));
        }
        None => return None,
    };
    if parts.headers.get_all(axum::http::header::AUTHORIZATION).iter().count() != 1 {
        return Some(Err(Error::Unauthenticated { message: None }));
    }

    let auth_str = match auth_header.to_str() {
        Ok(s) => s,
        Err(e) => {
            return Some(Err(Error::BadRequest {
                message: format!("Invalid authorization header: {e}"),
            }));
        }
    };

    let api_key = match auth_str.split_once(' ') {
        Some((scheme, key)) if scheme.eq_ignore_ascii_case("bearer") && !key.is_empty() => key,
        _ => return Some(Err(Error::Unauthenticated { message: None })),
    };

    let mut conn = match db.acquire().await {
        Ok(conn) => conn,
        Err(e) => return Some(Err(DbError::from(e).into())),
    };
    let api_key_result = match sqlx::query!(
        r#"
        SELECT ak.id AS api_key_id, ak.user_id, ak.created_by, ak.purpose,
               u.username, u.email, u.is_admin, u.display_name, u.avatar_url,
               u.payment_provider_id, u.last_login
        FROM api_keys ak
        INNER JOIN users u ON ak.created_by = u.id
        INNER JOIN users account ON ak.user_id = account.id
        WHERE ak.secret_hash = $1 AND ak.is_deleted = false
          AND u.is_deleted = false AND account.is_deleted = false
        "#,
        &hash_api_key(api_key)[..]
    )
    .fetch_optional(&mut *conn)
    .await
    {
        Ok(result) => result,
        Err(e) => return Some(Err(DbError::from(e).into())),
    };

    let api_key_data = match api_key_result {
        Some(data) => data,
        None => {
            return Some(Err(Error::Unauthenticated {
                message: Some(crate::errors::INVALID_API_KEY_MESSAGE.to_string()),
            }));
        }
    };

    let path = parts
        .extensions
        .get::<axum::extract::OriginalUri>()
        .map(|uri| uri.path().to_owned());
    let path = path.as_deref().unwrap_or_else(|| parts.uri.path());
    let purpose_str = &api_key_data.purpose;
    let is_system_key =
        purpose_str == "platform" && api_key_data.api_key_id.is_nil() && api_key_data.created_by.is_nil() && api_key_data.user_id.is_nil();

    debug!(path, purpose = purpose_str.as_str(), "API key purpose check");
    let is_valid = if is_system_key || matches!(path, "/admin/openapi.json" | "/admin/docs" | "/ai/openapi.json" | "/ai/docs") {
        true
    } else if path.starts_with("/admin/") {
        purpose_str == "platform"
    } else if path.starts_with("/ai/") {
        crate::db::models::api_keys::is_inference_purpose(purpose_str)
    } else {
        true
    };

    if !is_valid {
        return Some(Err(Error::InsufficientPermissions {
            required: crate::types::Permission::Granted,
            action: crate::types::Operation::ReadAll,
            resource: format!("endpoint {} with API key purpose '{}'", path, purpose_str),
        }));
    }

    let roles = match sqlx::query_scalar!(
        r#"
        SELECT role as "role!: Role"
        FROM user_roles
        WHERE user_id = $1
        "#,
        api_key_data.created_by
    )
    .fetch_all(&mut *conn)
    .await
    {
        Ok(roles) => roles,
        Err(e) => return Some(Err(DbError::from(e).into())),
    };

    let active_organization = if api_key_data.created_by != api_key_data.user_id {
        let membership = crate::db::handlers::Organizations::new(&mut conn)
            .get_user_org_role(api_key_data.created_by, api_key_data.user_id)
            .await;
        match membership {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Some(Err(Error::Unauthenticated {
                    message: Some(crate::errors::INVALID_API_KEY_MESSAGE.to_string()),
                }));
            }
            Err(error) => return Some(Err(Error::Database(error))),
        }
        Some(api_key_data.user_id)
    } else {
        None
    };

    Some(Ok((
        CurrentUser {
            auth_method: if is_system_key {
                AuthMethod::SystemApiKey
            } else if purpose_str == "platform"
                && !api_key_data.api_key_id.is_nil()
                && !api_key_data.created_by.is_nil()
                && api_key_data.created_by == api_key_data.user_id
            {
                AuthMethod::PlatformApiKey
            } else {
                AuthMethod::ApiKey
            },
            id: api_key_data.created_by,
            username: api_key_data.username,
            email: api_key_data.email,
            is_admin: api_key_data.is_admin,
            roles,
            display_name: api_key_data.display_name,
            avatar_url: api_key_data.avatar_url,
            payment_provider_id: api_key_data.payment_provider_id,
            organizations: vec![],
            active_organization,
            api_key_id: Some(api_key_data.api_key_id),
        },
        api_key_data.last_login,
    )))
}

pub const ORGANIZATION_HEADER: &str = "x-organization-id";

async fn populate_org_context(user: &mut CurrentUser, parts: &Parts, db: &PgPool) {
    let mut conn = match db.acquire().await {
        Ok(conn) => conn,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to acquire connection for org context");
            return;
        }
    };

    let mut org_repo = crate::db::handlers::Organizations::new(&mut conn);

    let manage_keys_orgs = match org_repo.user_manage_keys_org_ids(user.id).await {
        Ok(ids) => ids,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to load manage_keys org roles");
            return;
        }
    };

    match org_repo.list_user_organizations(user.id).await {
        Ok(memberships) => {
            user.organizations = memberships
                .into_iter()
                .map(|m| crate::api::models::users::UserOrganizationContext {
                    id: m.organization_id,
                    can_manage_keys: matches!(m.role.as_str(), "owner" | "admin") || manage_keys_orgs.contains(&m.organization_id),
                    name: String::new(),
                    role: m.role,
                })
                .collect();
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to load user organizations");
            return;
        }
    }

    if !user.organizations.is_empty() {
        let org_ids: Vec<uuid::Uuid> = user.organizations.iter().map(|o| o.id).collect();
        match sqlx::query!(r#"SELECT id, username, display_name FROM users WHERE id = ANY($1)"#, &org_ids)
            .fetch_all(&mut *conn)
            .await
        {
            Ok(rows) => {
                for org in &mut user.organizations {
                    if let Some(row) = rows.iter().find(|r| r.id == org.id) {
                        org.name = row.display_name.clone().unwrap_or_else(|| row.username.clone());
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to load organization names");
            }
        }
    }

    let org_id_str = parts
        .headers
        .get_all("cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(';'))
        .find_map(|cookie| {
            let cookie = cookie.trim();
            cookie.strip_prefix("dw_active_org=").filter(|v| !v.is_empty())
        })
        .map(String::from)
        .or_else(|| {
            parts
                .headers
                .get(ORGANIZATION_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(String::from)
        });

    if let Some(ref value) = org_id_str
        && let Ok(org_id) = value.parse::<uuid::Uuid>()
    {
        if user.organizations.iter().any(|o| o.id == org_id) {
            user.active_organization = Some(org_id);
        } else {
            tracing::debug!(
                org_id = %org_id,
                "Active organization references org user is not a member of"
            );
        }
    }
}

fn maybe_update_last_login(user_id: crate::types::UserId, last_login: Option<DateTime<Utc>>, db: &PgPool) {
    let should_update = match last_login {
        None => true,
        Some(ts) => Utc::now() - ts > chrono::Duration::minutes(5),
    };
    if should_update {
        let pool = db.clone();
        tokio::spawn(async move {
            if let Err(e) = sqlx::query!("UPDATE users SET last_login = NOW() WHERE id = $1", user_id)
                .execute(&pool)
                .await
            {
                tracing::warn!(user_id = %user_id, error = %e, "Failed to update last_login");
            }
        });
    }
}

impl<P: sqlx_pool_router::PoolProvider + Clone + Send + Sync> FromRequestParts<crate::AppState<P>> for CurrentUser {
    type Rejection = Error;

    #[instrument(skip(parts, state))]
    async fn from_request_parts(parts: &mut Parts, state: &crate::AppState<P>) -> Result<Self> {

        let mut auth_errors = Vec::new();
        let mut any_auth_attempted = false;

        match try_api_key_auth(parts, &state.db.write()).await {
            Some(Ok((mut user, last_login))) => {
                let method = user.auth_method;
                user = user.with_auth_method(method);
                if let Some(api_key_id) = user.api_key_id {
                    state.api_key_usage.record(api_key_id);
                }
                debug!("Authentication successful via API key");
                trace!("Authenticated user: {}", user.id);
                populate_org_context(&mut user, parts, &state.db.write()).await;
                maybe_update_last_login(user.id, last_login, &state.db.write());
                return Ok(user);
            }
            Some(Err(e)) => return Err(e),
            None => {
                trace!("No API key authentication attempted");
            }
        }

        let config = state.current_config();
        if config.auth.native.enabled {
            match try_jwt_session_auth(parts, &config, &state.db.write()).await {
                Some(Ok((mut user, last_login))) => {
                    user = user.with_auth_method(AuthMethod::NativeSession);
                    debug!("Authentication successful via JWT session");
                    trace!("Authenticated user: {}", user.id);
                    populate_org_context(&mut user, parts, &state.db.write()).await;
                    maybe_update_last_login(user.id, last_login, &state.db.write());
                    return Ok(user);
                }
                Some(Err(e)) => {
                    trace!("JWT session authentication failed: {:?}", e);
                    any_auth_attempted = true;
                    auth_errors.push(("JWT session", e));
                }
                None => {
                    trace!("No JWT session authentication attempted");
                }
            }
        }

        if config.auth.proxy_header.enabled {
            match try_proxy_header_auth(parts, state).await {
                Some(Ok((mut user, last_login))) => {
                    user = user.with_auth_method(AuthMethod::TrustedSso);
                    debug!("Authentication successful via proxy header");
                    trace!("Authenticated user: {}", user.id);
                    populate_org_context(&mut user, parts, &state.db.write()).await;
                    maybe_update_last_login(user.id, last_login, &state.db.write());
                    return Ok(user);
                }
                Some(Err(e)) => {
                    trace!("Proxy header authentication failed: {:?}", e);
                    any_auth_attempted = true;
                    auth_errors.push(("Proxy header", e));
                }
                None => {
                    trace!("No proxy header authentication attempted");
                }
            }
        }

        if !any_auth_attempted {
            debug!("Authentication failed: no credentials provided");
            trace!("No authentication credentials found in request");
            Err(Error::Unauthenticated { message: None })
        } else {
            debug!("Authentication failed: invalid credentials");
            trace!("All authentication attempts failed ({}): {:?}", auth_errors.len(), auth_errors);
            Err(Error::Unauthenticated { message: None })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        api::models::{
            transactions::TransactionFilters,
            users::{AuthMethod, CurrentUser, Role},
        },
        db::handlers::{Organizations, Users, repository::Repository},
        errors::Error,
        test::utils::create_test_config,
        test::utils::require_admin,
    };
    use axum::{extract::FromRequestParts as _, http::request::Parts};
    use sqlx::PgPool;

    fn create_test_parts_with_auth(external_user_id: &str, email: &str) -> Parts {
        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", email)
            .body(())
            .unwrap();

        let (parts, _body) = request.into_parts();
        parts
    }

    #[dwctl_test_macros::test]
    async fn test_existing_user_extraction(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let test_user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;

        let external_user_id = test_user.external_user_id.as_ref().unwrap();
        let mut parts = create_test_parts_with_auth(external_user_id, &test_user.email);

        let result = CurrentUser::from_request_parts(&mut parts, &state).await;
        assert!(result.is_ok());

        let current_user = result.unwrap();
        assert_eq!(current_user.email, test_user.email);
        assert_eq!(current_user.username, test_user.username);
        assert!(current_user.roles.contains(&Role::StandardUser));
    }

    #[dwctl_test_macros::test]
    async fn test_auto_create_nonexistent_user(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let new_email = "newuser@example.com";
        let new_external_id = "auth0|newuser123";
        let mut parts = create_test_parts_with_auth(new_external_id, new_email);

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut users_repo = Users::new(&mut pool_conn);
        let existing = users_repo.get_user_by_email(new_email).await.unwrap();
        assert!(existing.is_none());

        let result = CurrentUser::from_request_parts(&mut parts, &state).await;
        assert!(result.is_ok());

        let current_user = result.unwrap();
        assert_eq!(current_user.email, new_email);
        assert_eq!(current_user.username, new_external_id);
        assert!(current_user.roles.contains(&Role::StandardUser));
        assert_eq!(current_user.display_name.as_deref(), Some("newuser"));

        let created_user = users_repo.get_user_by_email(new_email).await.unwrap();
        assert!(created_user.is_some());
        let db_user = created_user.unwrap();
        assert_eq!(db_user.auth_source, "proxy-header");
        assert_eq!(db_user.display_name.as_deref(), Some("newuser"));
    }

    #[dwctl_test_macros::test]
    async fn test_missing_header_returns_unauthorized(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let request = axum::http::Request::builder().uri("http://localhost/test").body(()).unwrap();

        let (mut parts, _body) = request.into_parts();

        let result = CurrentUser::from_request_parts(&mut parts, &state).await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.status_code(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[dwctl_test_macros::test]
    async fn test_backwards_compatibility_single_header(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let email = "legacy-user@example.com";
        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", email)
            .body(())
            .unwrap();

        let (mut parts, _body) = request.into_parts();

        let result = CurrentUser::from_request_parts(&mut parts, &state).await;
        assert!(result.is_ok());

        let current_user = result.unwrap();
        assert_eq!(current_user.email, email);
        assert_eq!(current_user.username, email);
        assert!(current_user.roles.contains(&Role::StandardUser));

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut users_repo = Users::new(&mut pool_conn);
        let db_user = users_repo.get_user_by_email(email).await.unwrap().unwrap();

        assert_eq!(db_user.external_user_id, Some(email.to_string()));
    }

    #[dwctl_test_macros::test]
    async fn test_multiple_federated_identities_same_email(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let shared_email = "user@example.com";

        let github_external_id = "github|user123";
        let request1 = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", github_external_id)
            .header("x-doubleword-email", shared_email)
            .body(())
            .unwrap();

        let (mut parts1, _) = request1.into_parts();
        let result1 = CurrentUser::from_request_parts(&mut parts1, &state).await;
        assert!(result1.is_ok(), "First identity should succeed");

        let user1 = result1.unwrap();
        assert_eq!(user1.email, shared_email);
        assert_eq!(user1.username, github_external_id);

        let google_external_id = "google|user456";
        let request2 = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", google_external_id)
            .header("x-doubleword-email", shared_email)
            .body(())
            .unwrap();

        let (mut parts2, _) = request2.into_parts();
        let result2 = CurrentUser::from_request_parts(&mut parts2, &state).await;

        assert!(
            result2.is_ok(),
            "Second identity should create separate user. Error: {:?}",
            result2.as_ref().err()
        );

        let user2 = result2.unwrap();
        assert_eq!(user2.email, shared_email);
        assert_eq!(user2.username, google_external_id);
        assert_ne!(user1.id, user2.id, "Should be different users");
    }

    #[dwctl_test_macros::test]
    async fn test_migration_backfill_external_user_id(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let email = "legacy-user@example.com";

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut users_repo = Users::new(&mut pool_conn);
        let legacy_user = users_repo
            .create(&crate::db::models::users::UserCreateDBRequest {
                username: "legacyuser".to_string(),
                email: email.to_string(),
                display_name: None,
                avatar_url: None,
                is_admin: false,
                roles: vec![Role::StandardUser],
                auth_source: "proxy-header".to_string(),
                password_hash: None,
                external_user_id: None,
            })
            .await
            .unwrap();

        let legacy_user_id = legacy_user.id;
        drop(pool_conn);

        let federated_external_id = "auth0|github|user123";
        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", federated_external_id)
            .header("x-doubleword-email", email)
            .body(())
            .unwrap();

        let (mut parts, _) = request.into_parts();
        let result = CurrentUser::from_request_parts(&mut parts, &state).await;

        assert!(result.is_ok(), "Should backfill external_user_id for existing user");
        let user = result.unwrap();
        assert_eq!(user.id, legacy_user_id, "Should use the same existing user");
        assert_eq!(user.email, email);

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut users_repo = Users::new(&mut pool_conn);
        let db_user = users_repo
            .get_user_by_external_user_id(federated_external_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(db_user.id, legacy_user_id);
        assert_eq!(db_user.external_user_id, Some(federated_external_id.to_string()));
    }

    #[dwctl_test_macros::test]
    async fn test_backwards_compat_no_backfill(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let email = "legacy-user@example.com";

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut users_repo = Users::new(&mut pool_conn);
        let legacy_user = users_repo
            .create(&crate::db::models::users::UserCreateDBRequest {
                username: "legacyuser".to_string(),
                email: email.to_string(),
                display_name: None,
                avatar_url: None,
                is_admin: false,
                roles: vec![Role::StandardUser],
                auth_source: "proxy-header".to_string(),
                password_hash: None,
                external_user_id: None,
            })
            .await
            .unwrap();

        let legacy_user_id = legacy_user.id;
        drop(pool_conn);

        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", email)
            .body(())
            .unwrap();

        let (mut parts, _) = request.into_parts();
        let result = CurrentUser::from_request_parts(&mut parts, &state).await;

        assert!(result.is_ok(), "Should work in backwards compat mode");
        let user = result.unwrap();
        assert_eq!(user.id, legacy_user_id, "Should use existing user");
        assert_eq!(user.email, email);

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut users_repo = Users::new(&mut pool_conn);
        let db_user = users_repo.get_user_by_email(email).await.unwrap().unwrap();
        assert_eq!(db_user.external_user_id, None, "Should NOT backfill in backwards compat mode");

        drop(pool_conn);
        let federated_external_id = "github|user123";
        let request2 = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", federated_external_id)
            .header("x-doubleword-email", email)
            .body(())
            .unwrap();

        let (mut parts2, _) = request2.into_parts();
        let result2 = CurrentUser::from_request_parts(&mut parts2, &state).await;

        assert!(result2.is_ok(), "Should work with both headers");
        let user2 = result2.unwrap();
        assert_eq!(user2.id, legacy_user_id, "Should still use same user");

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut users_repo = Users::new(&mut pool_conn);
        let db_user_after = users_repo.get_user_by_email(email).await.unwrap().unwrap();
        assert_eq!(
            db_user_after.external_user_id,
            Some(federated_external_id.to_string()),
            "Should backfill now that proxy sends both headers"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_only_email_header_sent_fails(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-email", "user@example.com")
            .body(())
            .unwrap();

        let (mut parts, _) = request.into_parts();
        let result = CurrentUser::from_request_parts(&mut parts, &state).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status_code(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[dwctl_test_macros::test]
    async fn test_auto_create_disabled_existing_user(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.proxy_header.auto_create_users = false;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let test_user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let external_user_id = test_user.external_user_id.as_ref().unwrap();

        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", &test_user.email)
            .body(())
            .unwrap();

        let (mut parts, _) = request.into_parts();
        let result = CurrentUser::from_request_parts(&mut parts, &state).await;

        assert!(result.is_ok(), "Should succeed for existing user even with auto_create disabled");
        let current_user = result.unwrap();
        assert_eq!(current_user.email, test_user.email);
    }

    #[dwctl_test_macros::test]
    async fn test_auto_create_disabled_new_user_fails(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.proxy_header.auto_create_users = false;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", "github|newuser789")
            .header("x-doubleword-email", "newuser@example.com")
            .body(())
            .unwrap();

        let (mut parts, _) = request.into_parts();
        let result = CurrentUser::from_request_parts(&mut parts, &state).await;

        assert!(result.is_err(), "Should fail for new user when auto_create disabled");
        assert_eq!(result.unwrap_err().status_code(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[dwctl_test_macros::test]
    async fn test_existing_user_email_update(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let external_user_id = "github|user123";
        let old_email = "old@example.com";
        let new_email = "new@example.com";

        let request1 = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", old_email)
            .body(())
            .unwrap();

        let (mut parts1, _) = request1.into_parts();
        let result1 = CurrentUser::from_request_parts(&mut parts1, &state).await;
        assert!(result1.is_ok());
        let user1 = result1.unwrap();
        let user_id = user1.id;
        assert_eq!(user1.email, old_email);

        let request2 = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", new_email)
            .body(())
            .unwrap();

        let (mut parts2, _) = request2.into_parts();
        let result2 = CurrentUser::from_request_parts(&mut parts2, &state).await;
        assert!(result2.is_ok(), "Should update email for existing user");

        let user2 = result2.unwrap();
        assert_eq!(user2.id, user_id, "Should be same user");
        assert_eq!(user2.email, new_email, "Email should be updated");

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut users_repo = Users::new(&mut pool_conn);
        let db_user = users_repo.get_user_by_external_user_id(external_user_id).await.unwrap().unwrap();
        assert_eq!(db_user.email, new_email);
    }

    #[dwctl_test_macros::test]
    async fn test_idempotent_logins(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let external_user_id = "auth0|user456";
        let email = "user@example.com";

        for i in 0..3 {
            let request = axum::http::Request::builder()
                .uri("http://localhost/test")
                .header("x-doubleword-user", external_user_id)
                .header("x-doubleword-email", email)
                .body(())
                .unwrap();

            let (mut parts, _) = request.into_parts();
            let result = CurrentUser::from_request_parts(&mut parts, &state).await;

            assert!(result.is_ok(), "Login attempt {} should succeed", i + 1);
            let user = result.unwrap();
            assert_eq!(user.email, email);
        }

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut users_repo = Users::new(&mut pool_conn);
        let db_user = users_repo.get_user_by_external_user_id(external_user_id).await.unwrap().unwrap();
        assert_eq!(db_user.email, email);
    }

    #[dwctl_test_macros::test]
    async fn test_special_characters_in_external_user_id(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let test_cases = vec![
            "auth0|google-oauth2|123456789",
            "okta|user@domain.com",
            "azure-ad|user_with_underscores",
            "github|user-with-dashes",
        ];

        for external_user_id in test_cases {
            let email = format!("{}@example.com", external_user_id.replace(['|', '@'], "_"));

            let request = axum::http::Request::builder()
                .uri("http://localhost/test")
                .header("x-doubleword-user", external_user_id)
                .header("x-doubleword-email", &email)
                .body(())
                .unwrap();

            let (mut parts, _) = request.into_parts();
            let result = CurrentUser::from_request_parts(&mut parts, &state).await;

            assert!(result.is_ok(), "Should handle external_user_id: {}", external_user_id);
            let user = result.unwrap();
            assert_eq!(user.username, external_user_id);
        }
    }

    #[test]
    fn test_username_extraction_from_email() {
        let test_cases = vec![
            ("simple@example.com", "simple"),
            ("user.name@domain.co.uk", "user.name"),
            ("test+tag@gmail.com", "test+tag"),
            ("no-at-sign", "no-at-sign"),
            ("@domain.com", "user"),
        ];

        for (email, expected_username) in test_cases {
            let username = email.split('@').next().unwrap_or("user");
            let username = if username.is_empty() { "user" } else { username }.to_string();
            assert_eq!(username, expected_username, "Failed for email: {email}");
        }
    }

    #[test]
    fn test_require_admin_function() {
        let admin_user = CurrentUser {
            auth_method: AuthMethod::TrustedSso,
            id: uuid::Uuid::new_v4(),
            username: "admin".to_string(),
            email: "admin@example.com".to_string(),
            is_admin: true,
            roles: vec![Role::PlatformManager],
            display_name: None,
            avatar_url: None,
            payment_provider_id: None,
            organizations: vec![],
            active_organization: None,
            api_key_id: None,
        };

        let result = require_admin(admin_user);
        assert!(result.is_ok());

        let regular_user = CurrentUser {
            auth_method: AuthMethod::TrustedSso,
            id: uuid::Uuid::new_v4(),
            username: "user".to_string(),
            email: "user@example.com".to_string(),
            is_admin: false,
            roles: vec![Role::StandardUser],
            display_name: None,
            avatar_url: None,
            payment_provider_id: None,
            organizations: vec![],
            active_organization: None,
            api_key_id: None,
        };

        let result = require_admin(regular_user);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    #[dwctl_test_macros::test]
    async fn test_jwt_reflects_current_user_state(pool: PgPool) {
        use crate::auth::session;

        let mut config = create_test_config();
        config.auth.native.enabled = true;

        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;

        let current_user = CurrentUser {
            auth_method: AuthMethod::TrustedSso,
            id: user.id,
            username: user.username.clone(),
            email: user.email.clone(),
            is_admin: user.is_admin,
            roles: user.roles.clone(),
            display_name: user.display_name.clone(),
            avatar_url: user.avatar_url.clone(),
            payment_provider_id: None,
            organizations: vec![],
            active_organization: None,
            api_key_id: None,
        };
        let jwt_token = session::create_session_token(&current_user, &config).unwrap();

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config.clone()).await;

        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("cookie", format!("{}={}", config.auth.native.session.cookie_name, jwt_token))
            .body(())
            .unwrap();
        let (mut parts, _body) = request.into_parts();

        let extracted_user = CurrentUser::from_request_parts(&mut parts, &state).await.unwrap();
        assert_eq!(extracted_user.id, user.id);
        assert_eq!(extracted_user.roles, vec![Role::StandardUser]);
        assert!(!extracted_user.is_admin);

        let mut conn = pool.acquire().await.unwrap();
        let mut users_repo = Users::new(&mut conn);
        let update = crate::db::models::users::UserUpdateDBRequest {
            display_name: None,
            avatar_url: None,
            roles: Some(vec![Role::StandardUser, Role::PlatformManager]),
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
        users_repo.update(user.id, &update).await.unwrap();

        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("cookie", format!("{}={}", config.auth.native.session.cookie_name, jwt_token))
            .body(())
            .unwrap();
        let (mut parts, _body) = request.into_parts();

        let extracted_user = CurrentUser::from_request_parts(&mut parts, &state).await.unwrap();
        assert_eq!(extracted_user.id, user.id);
        assert!(extracted_user.roles.contains(&Role::StandardUser));
        assert!(!extracted_user.roles.contains(&Role::PlatformManager));
    }

    #[dwctl_test_macros::test]
    async fn test_jwt_invalidated_when_user_deleted(pool: PgPool) {
        use crate::auth::session;

        let mut config = create_test_config();
        config.auth.native.enabled = true;

        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;

        let current_user = CurrentUser {
            auth_method: AuthMethod::TrustedSso,
            id: user.id,
            username: user.username.clone(),
            email: user.email.clone(),
            is_admin: user.is_admin,
            roles: user.roles.clone(),
            display_name: user.display_name.clone(),
            avatar_url: user.avatar_url.clone(),
            payment_provider_id: None,
            organizations: vec![],
            active_organization: None,
            api_key_id: None,
        };
        let jwt_token = session::create_session_token(&current_user, &config).unwrap();

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config.clone()).await;

        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("cookie", format!("{}={}", config.auth.native.session.cookie_name, jwt_token))
            .body(())
            .unwrap();
        let (mut parts, _body) = request.into_parts();
        let result = CurrentUser::from_request_parts(&mut parts, &state).await;
        assert!(result.is_ok());

        let mut conn = pool.acquire().await.unwrap();
        let mut users_repo = Users::new(&mut conn);
        users_repo.delete(user.id).await.unwrap();

        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("cookie", format!("{}={}", config.auth.native.session.cookie_name, jwt_token))
            .body(())
            .unwrap();
        let (mut parts, _body) = request.into_parts();
        let result = CurrentUser::from_request_parts(&mut parts, &state).await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(matches!(error, Error::Unauthenticated { .. }));
    }

    #[dwctl_test_macros::test]
    async fn test_proxy_header_user_receives_initial_credits(pool: PgPool) {
        use crate::db::handlers::credits::Credits;
        use crate::db::models::credits::CreditTransactionType;

        let mut config = create_test_config();
        config.auth.proxy_header.enabled = true;
        config.auth.proxy_header.auto_create_users = true;
        config.credits.initial_credits_for_standard_users = rust_decimal::Decimal::new(10000, 2);

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let new_email = "proxy-user@example.com";
        let new_external_id = "auth0|proxyuser123";
        let mut parts = create_test_parts_with_auth(new_external_id, new_email);

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut users_repo = Users::new(&mut pool_conn);
        let existing = users_repo.get_user_by_email(new_email).await.unwrap();
        assert!(existing.is_none());
        drop(pool_conn);

        let result = CurrentUser::from_request_parts(&mut parts, &state).await;
        assert!(result.is_ok(), "Should successfully create user via proxy header");

        let current_user = result.unwrap();
        assert_eq!(current_user.email, new_email);
        assert!(current_user.roles.contains(&Role::StandardUser));

        let mut conn = pool.acquire().await.unwrap();
        let mut credits_repo = Credits::new(&mut conn);

        let balance = credits_repo.get_user_balance(current_user.id).await.unwrap();
        assert_eq!(
            balance,
            rust_decimal::Decimal::new(10000, 2),
            "User should have initial credits balance of 100.00"
        );

        let transactions = credits_repo
            .list_user_transactions(current_user.id, 0, 10, &TransactionFilters::default())
            .await
            .unwrap();

        assert_eq!(transactions.len(), 1, "Should have exactly one transaction");
        assert_eq!(transactions[0].amount, rust_decimal::Decimal::new(10000, 2));
        assert_eq!(transactions[0].transaction_type, CreditTransactionType::AdminGrant);
        assert!(transactions[0].description.as_ref().unwrap().contains("Initial credits"));

        let balance = credits_repo.get_user_balance(current_user.id).await.unwrap();
        assert_eq!(balance, rust_decimal::Decimal::new(10000, 2));
    }

    #[dwctl_test_macros::test]
    async fn test_proxy_header_existing_user_no_duplicate_credits(pool: PgPool) {
        use crate::db::handlers::credits::Credits;

        let mut config = create_test_config();
        config.auth.proxy_header.enabled = true;
        config.auth.proxy_header.auto_create_users = true;
        config.credits.initial_credits_for_standard_users = rust_decimal::Decimal::new(10000, 2);

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let email = "existing-proxy@example.com";
        let external_id = "auth0|existing123";

        let mut parts1 = create_test_parts_with_auth(external_id, email);
        let result1 = CurrentUser::from_request_parts(&mut parts1, &state).await;
        assert!(result1.is_ok());
        let user = result1.unwrap();

        let mut conn = pool.acquire().await.unwrap();
        let mut credits_repo = Credits::new(&mut conn);
        let balance = credits_repo.get_user_balance(user.id).await.unwrap();
        assert_eq!(balance, rust_decimal::Decimal::new(10000, 2));
        drop(conn);

        let mut parts2 = create_test_parts_with_auth(external_id, email);
        let result2 = CurrentUser::from_request_parts(&mut parts2, &state).await;
        assert!(result2.is_ok());

        let mut conn = pool.acquire().await.unwrap();
        let mut credits_repo = Credits::new(&mut conn);
        let balance_after = credits_repo.get_user_balance(user.id).await.unwrap();
        assert_eq!(
            balance_after,
            rust_decimal::Decimal::new(10000, 2),
            "Balance should remain the same on subsequent logins"
        );

        let transactions = credits_repo
            .list_user_transactions(user.id, 0, 10, &TransactionFilters::default())
            .await
            .unwrap();
        assert_eq!(transactions.len(), 1, "Should still have exactly one transaction");
    }


    #[dwctl_test_macros::test]
    async fn test_org_context_from_header(pool: PgPool) {
        use crate::db::handlers::Organizations;
        use crate::db::models::organizations::OrganizationCreateDBRequest;

        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let test_user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let external_user_id = test_user.external_user_id.as_ref().unwrap();

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "test-org-header".to_string(),
                    email: "org@example.com".to_string(),
                    display_name: Some("Test Org".to_string()),
                    avatar_url: None,
                    created_by: test_user.id,
                },
                &[
                    crate::api::models::users::Role::StandardUser,
                    crate::api::models::users::Role::BatchAPIUser,
                ],
            )
            .await
            .unwrap();
        drop(conn);

        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", &test_user.email)
            .header(super::ORGANIZATION_HEADER, org.id.to_string())
            .body(())
            .unwrap();
        let (mut parts, _body) = request.into_parts();

        let result = CurrentUser::from_request_parts(&mut parts, &state).await;
        assert!(result.is_ok());

        let current_user = result.unwrap();
        assert_eq!(current_user.active_organization, Some(org.id));
        assert!(!current_user.organizations.is_empty());
        assert!(current_user.organizations.iter().any(|o| o.id == org.id));
    }

    #[dwctl_test_macros::test]
    async fn test_org_context_names_the_org_by_display_name(pool: PgPool) {
        use crate::db::handlers::Organizations;
        use crate::db::models::organizations::OrganizationCreateDBRequest;

        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let test_user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let external_user_id = test_user.external_user_id.as_ref().unwrap();

        let mut conn = pool.acquire().await.unwrap();
        let org = Organizations::new(&mut conn)
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme.test~a1b2c3d4".to_string(),
                    email: "billing@acme.test".to_string(),
                    display_name: Some("Acme Corporation".to_string()),
                    avatar_url: None,
                    created_by: test_user.id,
                },
                &[crate::api::models::users::Role::StandardUser],
            )
            .await
            .unwrap();
        drop(conn);

        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", &test_user.email)
            .body(())
            .unwrap();
        let (mut parts, _body) = request.into_parts();

        let current_user = CurrentUser::from_request_parts(&mut parts, &state).await.unwrap();
        let listed = current_user
            .organizations
            .iter()
            .find(|o| o.id == org.id)
            .expect("the owner's organization is listed");
        assert_eq!(
            listed.name, "Acme Corporation",
            "the switcher shows the display name, not the routing key"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_org_context_invalid_org_id_ignored(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let test_user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let external_user_id = test_user.external_user_id.as_ref().unwrap();

        let fake_org_id = uuid::Uuid::new_v4();
        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", &test_user.email)
            .header(super::ORGANIZATION_HEADER, fake_org_id.to_string())
            .body(())
            .unwrap();
        let (mut parts, _body) = request.into_parts();

        let result = CurrentUser::from_request_parts(&mut parts, &state).await;
        assert!(result.is_ok());

        let current_user = result.unwrap();
        assert_eq!(current_user.active_organization, None);
    }

    #[dwctl_test_macros::test]
    async fn test_org_context_no_header_means_personal(pool: PgPool) {
        use crate::db::handlers::Organizations;
        use crate::db::models::organizations::OrganizationCreateDBRequest;

        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let test_user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let external_user_id = test_user.external_user_id.as_ref().unwrap();

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        orgs.create(
            &OrganizationCreateDBRequest {
                name: "test-org-no-header".to_string(),
                email: "org@example.com".to_string(),
                display_name: None,
                avatar_url: None,
                created_by: test_user.id,
            },
            &[
                crate::api::models::users::Role::StandardUser,
                crate::api::models::users::Role::BatchAPIUser,
            ],
        )
        .await
        .unwrap();
        drop(conn);

        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", &test_user.email)
            .body(())
            .unwrap();
        let (mut parts, _body) = request.into_parts();

        let result = CurrentUser::from_request_parts(&mut parts, &state).await;
        assert!(result.is_ok());

        let current_user = result.unwrap();
        assert_eq!(current_user.active_organization, None);
        assert_eq!(current_user.organizations.len(), 1);
    }

    #[dwctl_test_macros::test]
    async fn test_org_context_malformed_header_ignored(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let test_user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let external_user_id = test_user.external_user_id.as_ref().unwrap();

        let request = axum::http::Request::builder()
            .uri("http://localhost/test")
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", &test_user.email)
            .header(super::ORGANIZATION_HEADER, "not-a-uuid")
            .body(())
            .unwrap();
        let (mut parts, _body) = request.into_parts();

        let result = CurrentUser::from_request_parts(&mut parts, &state).await;
        assert!(result.is_ok());

        let current_user = result.unwrap();
        assert_eq!(current_user.active_organization, None);
    }

    #[dwctl_test_macros::test]
    async fn test_last_login_updated_on_first_auth(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.proxy_header.enabled = true;
        config.auth.proxy_header.auto_create_users = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let email = "new-last-login@example.com";
        let external_id = "auth0|lastlogin123";

        let mut parts = create_test_parts_with_auth(external_id, email);
        let user = CurrentUser::from_request_parts(&mut parts, &state).await.unwrap();

        let row = sqlx::query!("SELECT last_login, created_at FROM users WHERE id = $1", user.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(
            row.last_login.is_none() || {
                let ll = row.last_login.unwrap();
                (ll - row.created_at).num_seconds().abs() < 10
            },
            "On first auth, last_login should be null or just set by background task"
        );

        let mut last_login = None;
        for _ in 0..50 {
            let row = sqlx::query!("SELECT last_login FROM users WHERE id = $1", user.id)
                .fetch_one(&pool)
                .await
                .unwrap();
            if row.last_login.is_some() {
                last_login = row.last_login;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(last_login.is_some(), "Background task should have set last_login");
    }

    #[dwctl_test_macros::test]
    async fn test_last_login_not_updated_when_recent(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.proxy_header.enabled = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let test_user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let external_user_id = test_user.external_user_id.as_ref().unwrap();

        let recent = chrono::Utc::now() - chrono::Duration::minutes(1);
        sqlx::query!("UPDATE users SET last_login = $1 WHERE id = $2", recent, test_user.id)
            .execute(&pool)
            .await
            .unwrap();

        let mut parts = create_test_parts_with_auth(external_user_id, &test_user.email);
        let _ = CurrentUser::from_request_parts(&mut parts, &state).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let row = sqlx::query!("SELECT last_login FROM users WHERE id = $1", test_user.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let actual = row.last_login.unwrap();
        let diff = (actual - recent).num_seconds().abs();
        assert!(diff < 2, "last_login should not have been updated (diff: {diff}s)");
    }


    #[dwctl_test_macros::test]
    async fn test_proxy_header_signup_does_not_create_an_org(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.proxy_header.enabled = true;
        config.auth.proxy_header.auto_create_users = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let mut parts = create_test_parts_with_auth("auth0|alice-acme", "alice@acme.com");
        let current_user = CurrentUser::from_request_parts(&mut parts, &state).await.unwrap();

        let mut conn = pool.acquire().await.unwrap();
        let mut org_repo = Organizations::new(&mut conn);
        assert!(
            org_repo.find_by_domain("acme.com").await.unwrap().is_none(),
            "signup must not claim the domain on the user's behalf"
        );
        assert!(
            org_repo.list_user_organizations(current_user.id).await.unwrap().is_empty(),
            "the user starts in personal context"
        );
    }

    async fn claim_domain(pool: &PgPool, domain: &str, owner: crate::types::UserId) -> crate::types::UserId {
        let mut conn = pool.acquire().await.unwrap();
        let org = Organizations::new(&mut conn)
            .create(
                &crate::db::models::organizations::OrganizationCreateDBRequest {
                    name: domain.to_string(),
                    email: format!("billing@{domain}"),
                    display_name: Some(domain.to_string()),
                    avatar_url: None,
                    created_by: owner,
                },
                &[crate::api::models::users::Role::StandardUser],
            )
            .await
            .unwrap();
        org.id
    }

    #[dwctl_test_macros::test]
    async fn test_proxy_header_signup_leaves_domain_match_to_the_user(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.proxy_header.enabled = true;
        config.auth.proxy_header.auto_create_users = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let mut parts1 = create_test_parts_with_auth("auth0|first", "first@widgets.io");
        let first = CurrentUser::from_request_parts(&mut parts1, &state).await.unwrap();
        claim_domain(&pool, "widgets.io", first.id).await;

        let mut parts2 = create_test_parts_with_auth("auth0|second", "second@widgets.io");
        let second = CurrentUser::from_request_parts(&mut parts2, &state).await.unwrap();

        let mut conn = pool.acquire().await.unwrap();
        let mut org_repo = Organizations::new(&mut conn);
        let org = org_repo.find_by_domain("widgets.io").await.unwrap().unwrap();

        assert_eq!(
            org_repo.get_user_org_role(first.id, org.id).await.unwrap(),
            Some("owner".to_string())
        );

        assert_eq!(
            org_repo.get_user_org_role(second.id, org.id).await.unwrap(),
            None,
            "sharing a domain must not grant membership"
        );

        assert!(
            org_repo.list_join_requests(org.id).await.unwrap().is_empty(),
            "signup must not ask to join for the user"
        );
        assert!(
            org_repo.list_user_join_requests(second.id).await.unwrap().is_empty(),
            "the user has asked for nothing"
        );

        let members = org_repo.list_members(org.id).await.unwrap();
        assert_eq!(members.len(), 1, "only the owner is a member");
        assert_eq!(members[0].user_id, Some(first.id));
    }

    #[dwctl_test_macros::test]
    async fn test_proxy_header_signup_auto_joins_when_organization_opted_in(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.proxy_header.enabled = true;
        config.auth.proxy_header.auto_create_users = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let mut parts1 = create_test_parts_with_auth("auth0|first", "first@widgets.io");
        let first = CurrentUser::from_request_parts(&mut parts1, &state).await.unwrap();
        let org_id = claim_domain(&pool, "widgets.io", first.id).await;

        {
            let mut conn = pool.acquire().await.unwrap();
            let mut org_repo = Organizations::new(&mut conn);
            assert!(
                !org_repo.auto_join_enabled(org_id).await.unwrap(),
                "a new workspace must not admit its domain until asked to"
            );
            assert!(org_repo.set_auto_join_enabled(org_id, true).await.unwrap());
        }

        let mut parts2 = create_test_parts_with_auth("auth0|second", "second@widgets.io");
        let second = CurrentUser::from_request_parts(&mut parts2, &state).await.unwrap();

        let mut conn = pool.acquire().await.unwrap();
        let mut org_repo = Organizations::new(&mut conn);
        assert_eq!(
            org_repo.get_user_org_role(second.id, org_id).await.unwrap(),
            Some("member".to_string()),
            "auto-join admits as a plain member, never an admin"
        );
        assert!(
            org_repo.list_join_requests(org_id).await.unwrap().is_empty(),
            "there is nobody left to ask once the org has said yes in advance"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_proxy_header_repeat_login_does_not_duplicate_auto_join(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.proxy_header.enabled = true;
        config.auth.proxy_header.auto_create_users = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let mut owner_parts = create_test_parts_with_auth("auth0|owner", "owner@widgets.io");
        let owner = CurrentUser::from_request_parts(&mut owner_parts, &state).await.unwrap();
        let org_id = claim_domain(&pool, "widgets.io", owner.id).await;
        {
            let mut conn = pool.acquire().await.unwrap();
            Organizations::new(&mut conn).set_auto_join_enabled(org_id, true).await.unwrap();
        }

        for i in 0..3 {
            let mut parts = create_test_parts_with_auth("auth0|joiner", "joiner@widgets.io");
            CurrentUser::from_request_parts(&mut parts, &state)
                .await
                .unwrap_or_else(|_| panic!("login {i} should succeed"));
        }

        let mut conn = pool.acquire().await.unwrap();
        let mut org_repo = Organizations::new(&mut conn);
        let members = org_repo.list_members(org_id).await.unwrap();
        assert_eq!(members.len(), 2, "the owner and the joiner, once each");
    }

    #[dwctl_test_macros::test]
    async fn test_proxy_header_signup_skips_personal_email(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.proxy_header.enabled = true;
        config.auth.proxy_header.auto_create_users = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let mut parts = create_test_parts_with_auth("auth0|gmail-user", "someone@gmail.com");
        let result = CurrentUser::from_request_parts(&mut parts, &state).await;
        assert!(result.is_ok());

        let mut conn = pool.acquire().await.unwrap();
        let mut org_repo = Organizations::new(&mut conn);
        let org = org_repo.find_by_domain("gmail.com").await.unwrap();
        assert!(org.is_none(), "Should not create org for personal email domain");
    }
}
