
use axum::{
    Json,
    extract::{Path, State},
};
use uuid::Uuid;

use sqlx_pool_router::PoolProvider;

use crate::{
    AppState,
    api::models::{
        auth::{
            AuthResponse, AuthSuccessResponse, ChangePasswordRequest, LoginInfo, LoginRequest, LoginResponse, LogoutResponse,
            PasswordResetConfirmRequest, PasswordResetRequest, PasswordResetResponse, RegisterRequest, RegisterResponse, RegistrationInfo,
        },
        users::{CurrentUser, Role, UserResponse},
    },
    auth::{password, session},
    db::{
        handlers::{Deployments, PasswordResetTokens, Repository, Users, api_keys::ApiKeys, credits::Credits},
        models::{
            api_keys::ApiKeyPurpose, credits::CreditTransactionCreateDBRequest, deployments::ModelStatus, users::UserCreateDBRequest,
        },
    },
    email::EmailService,
    errors::Error,
};

#[utoipa::path(
    get,
    path = "/authentication/register",
    tag = "authentication",
    summary = "Check registration availability",
    description = "Returns whether user registration is enabled on this instance. \
        Use this before displaying a registration form to determine if self-registration \
        is allowed.",
    responses(
        (status = 200, description = "Registration info", body = RegistrationInfo),
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_registration_info<P: PoolProvider>(State(state): State<AppState<P>>) -> Result<Json<RegistrationInfo>, Error> {
    let config = state.current_config();
    Ok(Json(RegistrationInfo {
        enabled: config.auth.native.enabled && config.auth.native.allow_registration,
        message: if config.auth.native.enabled && config.auth.native.allow_registration {
            "Registration is enabled".to_string()
        } else {
            "Registration is disabled".to_string()
        },
    }))
}

#[utoipa::path(
    post,
    path = "/authentication/register",
    request_body = RegisterRequest,
    tag = "authentication",
    summary = "Register new account",
    description = "Create a new user account with email and password. On success, returns the \
        created user and sets a session cookie for immediate login. Registration must be enabled \
        in the instance configuration. New users receive default roles and initial credits if configured.",
    responses(
        (status = 201, description = "User registered successfully", body = AuthResponse),
        (status = 400, description = "Invalid input"),
        (status = 409, description = "User already exists"),
    )
)]
#[tracing::instrument(skip_all)]
pub async fn register<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Json(request): Json<RegisterRequest>,
) -> Result<RegisterResponse, Error> {
    let config = state.current_config();
    if !config.auth.native.enabled {
        return Err(Error::BadRequest {
            message: "Native authentication is disabled".to_string(),
        });
    }

    if !config.auth.native.allow_registration {
        return Err(Error::BadRequest {
            message: "User registration is disabled".to_string(),
        });
    }

    let password_config = &config.auth.native.password;
    if request.password.len() < password_config.min_length {
        return Err(Error::BadRequest {
            message: format!("Password must be at least {} characters", password_config.min_length),
        });
    }
    if request.password.len() > password_config.max_length {
        return Err(Error::BadRequest {
            message: format!("Password must be no more than {} characters", password_config.max_length),
        });
    }

    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;

    let mut user_repo = Users::new(&mut tx);
    if user_repo.get_user_by_email(&request.email).await?.is_some() {
        return Err(Error::BadRequest {
            message: "An account with this email address already exists".to_string(),
        });
    }

    let password = request.password.clone();
    let argon2_params = password::Argon2Params {
        memory_kib: password_config.argon2_memory_kib,
        iterations: password_config.argon2_iterations,
        parallelism: password_config.argon2_parallelism,
    };
    let password_hash = tokio::task::spawn_blocking(move || password::hash_string_with_params(&password, Some(argon2_params)))
        .await
        .map_err(|e| Error::Internal {
            operation: format!("spawn password hashing task: {e}"),
        })??;
    let display_name = if request.display_name.is_none() {
        Some(crate::auth::utils::default_display_name(&request.email))
    } else {
        request.display_name
    };

    let create_request = UserCreateDBRequest {
        username: request.username,
        email: request.email,
        display_name,
        avatar_url: None,
        is_admin: false,
        roles: config.auth.default_user_roles.clone(),
        auth_source: "native".to_string(),
        password_hash: Some(password_hash),
        external_user_id: None,
    };

    let created_user = user_repo.create(&create_request).await?;

    let initial_credits = config.credits.initial_credits_for_standard_users;
    if initial_credits > rust_decimal::Decimal::ZERO && create_request.roles.contains(&Role::StandardUser) {
        let mut credits_repo = Credits::new(&mut tx);
        let request = CreditTransactionCreateDBRequest::admin_grant(
            created_user.id,
            uuid::Uuid::nil(),
            initial_credits,
            Some("Initial credits on account creation".to_string()),
        );
        credits_repo.create_transaction(&request).await?;
    }

    tx.commit().await.map_err(|e| Error::Database(e.into()))?;


    if config.sample_files.enabled && config.batches.enabled {
        let user_id = created_user.id;
        let state_clone = state.clone();
        tokio::spawn(async move {
            if let Err(e) = create_sample_files_for_new_user(&state_clone, user_id).await {
                tracing::warn!(user_id = %user_id, error = %e, "Failed to create sample files for new user");
            }
        });
    }

    let user_response = UserResponse::from(created_user.clone());
    let current_user = CurrentUser::from(created_user);

    let token = session::create_session_token(&current_user, &config)?;

    let cookie = create_session_cookie(&token, &config);

    let auth_response = AuthResponse {
        user: user_response,
        message: "Registration successful".to_string(),
    };

    Ok(RegisterResponse { auth_response, cookie })
}

pub async fn create_sample_files_for_new_user<P: PoolProvider>(state: &AppState<P>, user_id: Uuid) -> Result<(), Error> {
    use crate::db::handlers::deployments::DeploymentFilter;
    use crate::sample_files;
    let config = state.current_config();

    let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;

    let mut deployments_repo = Deployments::new(&mut conn);
    let filter = DeploymentFilter::new(0, i64::MAX)
        .with_accessible_to(user_id)
        .with_statuses(vec![ModelStatus::Active])
        .with_deleted(false);
    let accessible_deployments = deployments_repo.list(&filter).await.map_err(Error::Database)?;

    let endpoint = format!("http://{}:{}/ai", config.host, config.port);

    let created_files = sample_files::create_sample_files_for_user(
        state.request_manager.as_ref(),
        user_id,
        &endpoint,
        &accessible_deployments,
        &config.sample_files,
    )
    .await?;

    tracing::debug!(
        user_id = %user_id,
        file_count = created_files.len(),
        "Created sample files for new user"
    );

    Ok(())
}

#[utoipa::path(
    get,
    path = "/authentication/login",
    tag = "authentication",
    summary = "Check login availability",
    description = "Returns whether native (email/password) login is enabled on this instance. \
        Use this before displaying a login form. If disabled, users should authenticate via \
        configured SSO providers instead.",
    responses(
        (status = 200, description = "Login info", body = LoginInfo),
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_login_info<P: PoolProvider>(State(state): State<AppState<P>>) -> Result<Json<LoginInfo>, Error> {
    let config = state.current_config();
    Ok(Json(LoginInfo {
        enabled: config.auth.native.enabled,
        message: if config.auth.native.enabled {
            "Native login is enabled".to_string()
        } else {
            "Native login is disabled".to_string()
        },
    }))
}

#[utoipa::path(
    post,
    path = "/authentication/login",
    request_body = LoginRequest,
    tag = "authentication",
    summary = "Login with credentials",
    description = "Authenticate with email and password. On success, returns the user details \
        and sets a session cookie. Native authentication must be enabled in the instance \
        configuration. The session cookie can be used for subsequent authenticated requests.",
    responses(
        (status = 200, description = "Login successful", body = AuthResponse),
        (status = 401, description = "Invalid credentials"),
    )
)]
#[tracing::instrument(skip_all)]
pub async fn login<P: PoolProvider>(State(state): State<AppState<P>>, Json(request): Json<LoginRequest>) -> Result<LoginResponse, Error> {
    let config = state.current_config();
    if !config.auth.native.enabled {
        return Err(Error::BadRequest {
            message: "Native authentication is disabled".to_string(),
        });
    }
    let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;

    let mut user_repo = Users::new(&mut pool_conn);

    let user = user_repo
        .get_user_by_email(&request.email)
        .await?
        .ok_or_else(|| Error::Unauthenticated {
            message: Some("Invalid email or password".to_string()),
        })?;

    let password_hash = user.password_hash.as_ref().ok_or_else(|| Error::Unauthenticated {
        message: Some("Invalid email or password".to_string()),
    })?;

    let password = request.password.clone();
    let hash = password_hash.clone();
    let is_valid = tokio::task::spawn_blocking(move || password::verify_string(&password, &hash))
        .await
        .map_err(|e| Error::Internal {
            operation: format!("spawn password verification task: {e}"),
        })??;

    if !is_valid {
        return Err(Error::Unauthenticated {
            message: Some("Invalid email or password".to_string()),
        });
    }

    let user_response = UserResponse::from(user.clone());
    let current_user = CurrentUser::from(user);

    let token = session::create_session_token(&current_user, &config)?;

    let cookie = create_session_cookie(&token, &config);

    let auth_response = AuthResponse {
        user: user_response,
        message: "Login successful".to_string(),
    };

    Ok(LoginResponse { auth_response, cookie })
}

#[utoipa::path(
    post,
    path = "/authentication/logout",
    tag = "authentication",
    summary = "End session",
    description = "Log out the current user by clearing the session cookie. After calling this \
        endpoint, subsequent requests will require re-authentication.",
    responses(
        (status = 200, description = "Logout successful", body = AuthSuccessResponse),
    )
)]
#[tracing::instrument(skip_all)]
pub async fn logout<P: PoolProvider>(State(state): State<AppState<P>>) -> Result<LogoutResponse, Error> {
    let config = state.current_config();
    let session_config = &config.auth.native.session;
    let secure = if session_config.cookie_secure { "; Secure" } else { "" };

    let domain = session_config
        .cookie_domain
        .as_ref()
        .map(|d| format!("; Domain={d}"))
        .unwrap_or_default();

    let cookie = format!(
        "{}=; Path=/; HttpOnly{}{}; SameSite={}; Max-Age=0",
        session_config.cookie_name, secure, domain, session_config.cookie_same_site
    );

    let org_cookie = format!(
        "dw_active_org=; Path=/; HttpOnly{}{}; SameSite={}; Max-Age=0",
        secure, domain, session_config.cookie_same_site
    );

    let auth_response = AuthSuccessResponse {
        message: "Logout successful".to_string(),
    };

    Ok(LogoutResponse {
        auth_response,
        cookie,
        extra_cookies: vec![org_cookie],
    })
}

#[utoipa::path(
    post,
    path = "/authentication/password-resets",
    request_body = PasswordResetRequest,
    tag = "authentication",
    summary = "Request password reset",
    description = "Request a password reset email for the specified email address. For security, \
        this endpoint always returns success even if the email doesn't exist (to prevent email \
        enumeration). If the email is valid and associated with a native auth account, a reset \
        link will be sent.",
    responses(
        (status = 200, description = "Password reset email sent", body = PasswordResetResponse),
        (status = 400, description = "Invalid request"),
    )
)]
#[tracing::instrument(skip_all)]
pub async fn request_password_reset<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Json(request): Json<PasswordResetRequest>,
) -> Result<Json<PasswordResetResponse>, Error> {
    let config = state.current_config();
    if !config.auth.native.enabled {
        return Err(Error::BadRequest {
            message: "Native authentication is disabled".to_string(),
        });
    }
    let mut tx = state.db.write().begin().await.unwrap();

    let mut user_repo = Users::new(&mut tx);

    let user = user_repo.get_user_by_email(&request.email).await?;

    let mut token_repo = PasswordResetTokens::new(&mut tx);

    if let Some(user) = user
        && user.password_hash.is_some()
    {
        let (raw_token, token) = token_repo.create_for_user(user.id, &config).await?;

        let email_service = EmailService::new(&config)?;
        email_service
            .send_password_reset_email(&user.email, user.display_name.as_deref(), &token.id, &raw_token)
            .await?;
    }
    tx.commit().await.map_err(|e| Error::Database(e.into()))?;

    Ok(Json(PasswordResetResponse {
        message: "If an account with that email exists, a password reset link has been sent.".to_string(),
    }))
}

#[utoipa::path(
    post,
    path = "/authentication/password-resets/{token_id}/confirm",
    request_body = PasswordResetConfirmRequest,
    tag = "authentication",
    summary = "Complete password reset",
    description = "Set a new password using the token received via email. The token_id is from the \
        URL and the raw token is included in the request body. Tokens expire after a configured \
        period and can only be used once.",
    responses(
        (status = 200, description = "Password reset successful", body = PasswordResetResponse),
        (status = 400, description = "Invalid or expired token"),
    )
)]
#[tracing::instrument(skip_all)]
pub async fn confirm_password_reset<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(token_id): Path<Uuid>,
    Json(request): Json<PasswordResetConfirmRequest>,
) -> Result<Json<PasswordResetResponse>, Error> {
    let config = state.current_config();
    if !config.auth.native.enabled {
        return Err(Error::BadRequest {
            message: "Native authentication is disabled".to_string(),
        });
    }

    let password_config = &config.auth.native.password;
    if request.new_password.len() < password_config.min_length {
        return Err(Error::BadRequest {
            message: format!("Password must be at least {} characters", password_config.min_length),
        });
    }
    if request.new_password.len() > password_config.max_length {
        return Err(Error::BadRequest {
            message: format!("Password must be no more than {} characters", password_config.max_length),
        });
    }

    let new_password_hash = tokio::task::spawn_blocking({
        let password = request.new_password.clone();
        let argon2_params = password::Argon2Params {
            memory_kib: password_config.argon2_memory_kib,
            iterations: password_config.argon2_iterations,
            parallelism: password_config.argon2_parallelism,
        };
        move || password::hash_string_with_params(&password, Some(argon2_params))
    })
    .await
    .map_err(|e| Error::Internal {
        operation: format!("spawn password hashing task: {e}"),
    })??;

    let update_request = crate::db::models::users::UserUpdateDBRequest {
        display_name: None,
        avatar_url: None,
        roles: None,
        password_hash: Some(new_password_hash),
        batch_notifications_enabled: None,
        low_balance_threshold: None,
        auto_topup_amount: None,
        auto_topup_threshold: None,
        auto_topup_monthly_limit: None,
        zero_data_retention: None,
        self_hosted_only: None,
        toleration_preset: Default::default(),
    };

    let mut tx = state.db.write().begin().await.unwrap();
    let token;
    {
        let mut token_repo = PasswordResetTokens::new(&mut tx);

        token = token_repo
            .find_valid_token_by_id(token_id, &request.token)
            .await?
            .ok_or_else(|| Error::BadRequest {
                message: "Invalid or expired reset token".to_string(),
            })?;
    }

    {
        let mut user_repo = Users::new(&mut tx);

        let _user = user_repo.get_by_id(token.user_id).await?.ok_or_else(|| Error::BadRequest {
            message: "User not found".to_string(),
        })?;

        user_repo.update(token.user_id, &update_request).await?;
    }

    {
        let mut token_repo = PasswordResetTokens::new(&mut tx);
        token_repo.invalidate_for_user(token.user_id).await?;
    }
    tx.commit().await.map_err(|e| Error::Database(e.into()))?;

    Ok(Json(PasswordResetResponse {
        message: "Password has been reset successfully".to_string(),
    }))
}

#[utoipa::path(
    post,
    path = "/authentication/password-change",
    request_body = ChangePasswordRequest,
    tag = "authentication",
    responses(
        (status = 200, description = "Password changed successfully", body = AuthSuccessResponse),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Current password is incorrect"),
    ),
    security(
        ("session_token" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn change_password<P: PoolProvider>(
    State(state): State<AppState<P>>,
    current_user: CurrentUser,
    Json(request): Json<ChangePasswordRequest>,
) -> Result<Json<AuthSuccessResponse>, Error> {
    let config = state.current_config();
    if !config.auth.native.enabled {
        return Err(Error::BadRequest {
            message: "Native authentication is disabled".to_string(),
        });
    }

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut user_repo = Users::new(&mut pool_conn);

    let user = user_repo.get_by_id(current_user.id).await?.ok_or_else(|| Error::Unauthenticated {
        message: Some("User not found".to_string()),
    })?;

    let password_hash = user.password_hash.as_ref().ok_or_else(|| Error::BadRequest {
        message: "Cannot change password for non-native authentication users".to_string(),
    })?;

    let current_password = request.current_password.clone();
    let hash = password_hash.clone();
    let is_valid = tokio::task::spawn_blocking(move || password::verify_string(&current_password, &hash))
        .await
        .map_err(|e| Error::Internal {
            operation: format!("spawn password verification task: {e}"),
        })??;

    if !is_valid {
        return Err(Error::Unauthenticated {
            message: Some("Current password is incorrect".to_string()),
        });
    }

    let password_config = &config.auth.native.password;
    if request.new_password.len() < password_config.min_length {
        return Err(Error::BadRequest {
            message: format!("Password must be at least {} characters", password_config.min_length),
        });
    }
    if request.new_password.len() > password_config.max_length {
        return Err(Error::BadRequest {
            message: format!("Password must be no more than {} characters", password_config.max_length),
        });
    }

    let new_password_hash = tokio::task::spawn_blocking({
        let password = request.new_password.clone();
        let argon2_params = password::Argon2Params {
            memory_kib: password_config.argon2_memory_kib,
            iterations: password_config.argon2_iterations,
            parallelism: password_config.argon2_parallelism,
        };
        move || password::hash_string_with_params(&password, Some(argon2_params))
    })
    .await
    .map_err(|e| Error::Internal {
        operation: format!("spawn password hashing task: {e}"),
    })??;

    let update_request = crate::db::models::users::UserUpdateDBRequest {
        display_name: None,
        avatar_url: None,
        roles: None,
        password_hash: Some(new_password_hash),
        batch_notifications_enabled: None,
        low_balance_threshold: None,
        auto_topup_amount: None,
        auto_topup_threshold: None,
        auto_topup_monthly_limit: None,
        zero_data_retention: None,
        self_hosted_only: None,
        toleration_preset: Default::default(),
    };

    user_repo.update(current_user.id, &update_request).await?;

    Ok(Json(AuthSuccessResponse {
        message: "Password changed successfully".to_string(),
    }))
}

fn create_session_cookie(token: &str, config: &crate::config::Config) -> String {
    let session_config = &config.auth.native.session;
    let max_age = session_config.timeout.as_secs();

    let secure = if session_config.cookie_secure { "; Secure" } else { "" };
    let domain = session_config
        .cookie_domain
        .as_ref()
        .map(|d| format!("; Domain={d}"))
        .unwrap_or_default();
    format!(
        "{}={}; Path=/; HttpOnly{}{}; SameSite={}; Max-Age={}",
        session_config.cookie_name, token, secure, domain, session_config.cookie_same_site, max_age
    )
}


#[derive(Debug, serde::Deserialize)]
pub struct CliCallbackQuery {
    pub port: u16,
    pub state: String,
    pub org: Option<String>,
}

#[tracing::instrument(skip_all)]
pub async fn cli_callback<P: PoolProvider>(
    State(state): State<AppState<P>>,
    headers: axum::http::HeaderMap,
    axum::extract::Query(query): axum::extract::Query<CliCallbackQuery>,
    current_user: CurrentUser,
) -> Result<axum::response::Response, Error> {
    use crate::db::handlers::organizations::Organizations;
    use axum::response::IntoResponse;

    if let Some(auth_header) = headers.get(axum::http::header::AUTHORIZATION)
        && auth_header.to_str().is_ok_and(|s| s.starts_with("Bearer "))
    {
        return Err(Error::Unauthenticated {
            message: Some("CLI callback must be accessed via browser SSO, not API keys.".to_string()),
        });
    }

    if query.port < 1024 {
        return Err(Error::BadRequest {
            message: format!("Invalid port: {}. Must be between 1024 and 65535.", query.port),
        });
    }

    let user_id = current_user.id;
    let user_username = current_user.username.clone();

    struct AccountContext {
        target_user_id: crate::types::UserId,
        account_name: String,
        org_id: Option<String>,
    }

    let ctx = if let Some(ref org_slug) = query.org {
        let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let mut org_repo = Organizations::new(&mut pool_conn);
        let memberships = org_repo.list_user_organizations(user_id).await.map_err(Error::Database)?;

        let org_ids: Vec<crate::types::UserId> = memberships.iter().map(|m| m.organization_id).collect();
        let mut users_repo = crate::db::handlers::Users::new(&mut pool_conn);
        let org_map = users_repo.get_bulk(org_ids).await.map_err(Error::Database)?;

        let matched_org = memberships.iter().find_map(|m| {
            org_map.get(&m.organization_id).and_then(|org| {
                let matches = org.username.eq_ignore_ascii_case(org_slug)
                    || org.display_name.as_deref().is_some_and(|dn| dn.eq_ignore_ascii_case(org_slug));
                if matches {
                    Some((m.organization_id, org.username.clone(), org.display_name.clone()))
                } else {
                    None
                }
            })
        });

        match matched_org {
            Some((org_id, org_username, _org_display_name)) => AccountContext {
                target_user_id: org_id,
                account_name: format!("{}@{}", user_username, org_username),
                org_id: Some(org_id.to_string()),
            },
            None => {
                return Err(Error::BadRequest {
                    message: format!("Organization '{}' not found or you are not a member.", org_slug),
                });
            }
        }
    } else {
        AccountContext {
            target_user_id: user_id,
            account_name: user_username.clone(),
            org_id: None,
        }
    };

    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;

    let timestamp = chrono::Utc::now().format("%Y%m%d-%H%M%S");

    let inference_key = {
        let mut repo = ApiKeys::new(&mut tx);
        repo.create(&crate::db::models::api_keys::ApiKeyCreateDBRequest {
            user_id: ctx.target_user_id,
            name: format!("DW CLI inference ({})", timestamp),
            description: Some("Created by dw login".to_string()),
            purpose: ApiKeyPurpose::Realtime,
            created_by: user_id,
            spend_limit: None,
            spend_limit_interval: None,
        })
        .await
        .map_err(Error::Database)?
    };

    let platform_key = {
        let mut repo = ApiKeys::new(&mut tx);
        repo.create(&crate::db::models::api_keys::ApiKeyCreateDBRequest {
            user_id: ctx.target_user_id,
            name: format!("DW CLI platform ({})", timestamp),
            description: Some("Created by dw login".to_string()),
            purpose: ApiKeyPurpose::Platform,
            created_by: user_id,
            spend_limit: None,
            spend_limit_interval: None,
        })
        .await
        .map_err(Error::Database)?
    };

    let (email, display_name, org_name) = if ctx.org_id.is_some() {
        let key_row = sqlx::query!("SELECT created_by FROM api_keys WHERE id = $1", inference_key.id,)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| Error::Database(e.into()))?;

        let individual = {
            let mut repo = crate::db::handlers::Users::new(&mut tx);
            repo.get_by_id(key_row.created_by)
                .await
                .map_err(Error::Database)?
                .ok_or_else(|| Error::Internal {
                    operation: "CLI callback: creator not found".to_string(),
                })?
        };

        let org = {
            let mut repo = crate::db::handlers::Users::new(&mut tx);
            repo.get_by_id(ctx.target_user_id)
                .await
                .map_err(Error::Database)?
                .ok_or_else(|| Error::Internal {
                    operation: "CLI callback: org not found".to_string(),
                })?
        };

        (
            individual.email,
            individual.display_name.unwrap_or(individual.username),
            Some(org.display_name.unwrap_or(org.username)),
        )
    } else {
        let user = {
            let mut repo = crate::db::handlers::Users::new(&mut tx);
            repo.get_by_id(ctx.target_user_id)
                .await
                .map_err(Error::Database)?
                .ok_or_else(|| Error::Internal {
                    operation: "CLI callback: user not found".to_string(),
                })?
        };
        (user.email, user.display_name.unwrap_or(user.username), None)
    };

    tx.commit().await.map_err(|e| Error::Database(e.into()))?;

    let mut redirect_url = url::Url::parse(&format!("http://127.0.0.1:{}/callback", query.port)).map_err(|e| Error::Internal {
        operation: format!("build redirect URL: {e}"),
    })?;

    {
        let mut params = redirect_url.query_pairs_mut();
        params.append_pair("state", &query.state);
        params.append_pair("inference_key", &inference_key.secret);
        params.append_pair("inference_key_id", &inference_key.id.to_string());
        params.append_pair("platform_key", &platform_key.secret);
        params.append_pair("platform_key_id", &platform_key.id.to_string());
        params.append_pair("user_id", &ctx.target_user_id.to_string());
        params.append_pair("email", &email);
        params.append_pair("display_name", &display_name);
        params.append_pair("account_name", &ctx.account_name);
        params.append_pair("account_type", if ctx.org_id.is_some() { "organization" } else { "personal" });
        if let Some(ref org_name) = org_name {
            params.append_pair("org_name", org_name);
        }
        if let Some(ref org_id) = ctx.org_id {
            params.append_pair("org_id", org_id);
        }
    }

    Ok((
        axum::http::StatusCode::FOUND,
        [
            ("location", redirect_url.as_str()),
            ("cache-control", "no-store"),
            ("pragma", "no-cache"),
            ("referrer-policy", "no-referrer"),
        ],
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::models::transactions::TransactionFilters, db::models::credits::CreditTransactionType, test::utils::create_test_config,
    };
    use axum_test::TestServer;
    use sqlx::PgPool;

    #[dwctl_test_macros::test]
    async fn test_register_success(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.allow_registration = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/register", axum::routing::post(register))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = RegisterRequest {
            username: "testuser".to_string(),
            email: "test@example.com".to_string(),
            password: "password123".to_string(),
            display_name: Some("Test User".to_string()),
        };

        let response = server.post("/auth/register").json(&request).await;

        response.assert_status(axum::http::StatusCode::CREATED);
        assert!(response.headers().get("set-cookie").is_some());

        let body: AuthResponse = response.json();
        assert_eq!(body.user.email, "test@example.com");
        assert_eq!(body.message, "Registration successful");
    }

    #[dwctl_test_macros::test]
    async fn test_register_disabled(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = false;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/register", axum::routing::post(register))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = RegisterRequest {
            username: "testuser".to_string(),
            email: "test@example.com".to_string(),
            password: "password123".to_string(),
            display_name: None,
        };

        let response = server.post("/auth/register").json(&request).await;
        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_password_validation(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.password.min_length = 10;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/register", axum::routing::post(register))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = RegisterRequest {
            username: "testuser".to_string(),
            email: "test@example.com".to_string(),
            password: "short".to_string(),
            display_name: None,
        };

        let response = server.post("/auth/register").json(&request).await;
        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_register_with_initial_credits(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.allow_registration = true;
        config.credits.initial_credits_for_standard_users = rust_decimal::Decimal::new(10000, 2);

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let app = axum::Router::new()
            .route("/auth/register", axum::routing::post(register))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = RegisterRequest {
            username: "testuser_credits".to_string(),
            email: "credits@example.com".to_string(),
            password: "password123".to_string(),
            display_name: Some("Credits Test User".to_string()),
        };

        let response = server.post("/auth/register").json(&request).await;
        response.assert_status(axum::http::StatusCode::CREATED);

        let body: AuthResponse = response.json();
        assert_eq!(body.user.email, "credits@example.com");

        let mut conn = pool.acquire().await.unwrap();
        let mut credits_repo = Credits::new(&mut conn);

        let balance = credits_repo.get_user_balance(body.user.id).await.unwrap();
        assert_eq!(
            balance,
            rust_decimal::Decimal::new(10000, 2),
            "User should have initial credits balance of 100.00"
        );

        let transactions = credits_repo
            .list_user_transactions(body.user.id, 0, 10, &TransactionFilters::default())
            .await
            .unwrap();

        assert_eq!(transactions.len(), 1, "Should have exactly one transaction");
        assert_eq!(transactions[0].amount, rust_decimal::Decimal::new(10000, 2));
        assert_eq!(transactions[0].transaction_type, CreditTransactionType::AdminGrant);
        assert!(transactions[0].description.as_ref().unwrap().contains("Initial credits"));

        let balance = credits_repo.get_user_balance(body.user.id).await.unwrap();
        assert_eq!(balance, rust_decimal::Decimal::new(10000, 2));
    }

    #[dwctl_test_macros::test]
    async fn test_register_without_initial_credits_when_zero(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.allow_registration = true;
        config.credits.initial_credits_for_standard_users = rust_decimal::Decimal::ZERO;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let app = axum::Router::new()
            .route("/auth/register", axum::routing::post(register))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = RegisterRequest {
            username: "testuser_nocredits".to_string(),
            email: "nocredits@example.com".to_string(),
            password: "password123".to_string(),
            display_name: Some("No Credits Test User".to_string()),
        };

        let response = server.post("/auth/register").json(&request).await;
        response.assert_status(axum::http::StatusCode::CREATED);

        let body: AuthResponse = response.json();
        assert_eq!(body.user.email, "nocredits@example.com");

        let transactions = sqlx::query!(r#"SELECT id FROM credits_transactions WHERE user_id = $1"#, body.user.id)
            .fetch_all(&pool)
            .await
            .unwrap();

        assert_eq!(
            transactions.len(),
            0,
            "Should have no credit transactions when initial credits is zero"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_register_auto_generates_display_name(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.allow_registration = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let app = axum::Router::new()
            .route("/auth/register", axum::routing::post(register))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = RegisterRequest {
            username: "autogen_user".to_string(),
            email: "autogen@example.com".to_string(),
            password: "password123".to_string(),
            display_name: None,
        };

        let response = server.post("/auth/register").json(&request).await;
        response.assert_status(axum::http::StatusCode::CREATED);

        let body: AuthResponse = response.json();
        assert_eq!(body.user.email, "autogen@example.com");

        assert_eq!(body.user.display_name.as_deref(), Some("autogen"));
    }

    #[dwctl_test_macros::test]
    async fn test_get_registration_info_enabled(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.allow_registration = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/register-info", axum::routing::get(get_registration_info))
            .with_state(state);

        let server = TestServer::new(app).unwrap();
        let response = server.get("/auth/register-info").await;

        response.assert_status(axum::http::StatusCode::OK);
        let body: RegistrationInfo = response.json();
        assert!(body.enabled);
        assert_eq!(body.message, "Registration is enabled");
    }

    #[dwctl_test_macros::test]
    async fn test_get_registration_info_disabled_native_auth(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = false;
        config.auth.native.allow_registration = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/register-info", axum::routing::get(get_registration_info))
            .with_state(state);

        let server = TestServer::new(app).unwrap();
        let response = server.get("/auth/register-info").await;

        response.assert_status(axum::http::StatusCode::OK);
        let body: RegistrationInfo = response.json();
        assert!(!body.enabled);
        assert_eq!(body.message, "Registration is disabled");
    }

    #[dwctl_test_macros::test]
    async fn test_get_registration_info_disabled_allow_registration(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.allow_registration = false;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/register-info", axum::routing::get(get_registration_info))
            .with_state(state);

        let server = TestServer::new(app).unwrap();
        let response = server.get("/auth/register-info").await;

        response.assert_status(axum::http::StatusCode::OK);
        let body: RegistrationInfo = response.json();
        assert!(!body.enabled);
        assert_eq!(body.message, "Registration is disabled");
    }

    #[dwctl_test_macros::test]
    async fn test_get_login_info_enabled(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/login-info", axum::routing::get(get_login_info))
            .with_state(state);

        let server = TestServer::new(app).unwrap();
        let response = server.get("/auth/login-info").await;

        response.assert_status(axum::http::StatusCode::OK);
        let body: LoginInfo = response.json();
        assert!(body.enabled);
        assert_eq!(body.message, "Native login is enabled");
    }

    #[dwctl_test_macros::test]
    async fn test_get_login_info_disabled(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = false;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/login-info", axum::routing::get(get_login_info))
            .with_state(state);

        let server = TestServer::new(app).unwrap();
        let response = server.get("/auth/login-info").await;

        response.assert_status(axum::http::StatusCode::OK);
        let body: LoginInfo = response.json();
        assert!(!body.enabled);
        assert_eq!(body.message, "Native login is disabled");
    }

    #[dwctl_test_macros::test]
    async fn test_login_success(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let test_params = password::Argon2Params {
            memory_kib: 128,
            iterations: 1,
            parallelism: 1,
        };
        let password_hash = password::hash_string_with_params("testpassword", Some(test_params)).unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let mut user_repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest {
            username: "loginuser".to_string(),
            email: "login@example.com".to_string(),
            display_name: Some("Login User".to_string()),
            avatar_url: None,
            is_admin: false,
            roles: vec![Role::StandardUser],
            auth_source: "native".to_string(),
            password_hash: Some(password_hash),
            external_user_id: None,
        };

        let created_user = user_repo.create(&user_create).await.unwrap();
        drop(conn);

        let app = axum::Router::new()
            .route("/auth/login", axum::routing::post(login))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = LoginRequest {
            email: "login@example.com".to_string(),
            password: "testpassword".to_string(),
        };

        let response = server.post("/auth/login").json(&request).await;

        response.assert_status(axum::http::StatusCode::OK);
        assert!(response.headers().get("set-cookie").is_some());

        let body: AuthResponse = response.json();
        assert_eq!(body.user.email, "login@example.com");
        assert_eq!(body.user.id, created_user.id);
        assert_eq!(body.message, "Login successful");
    }

    #[dwctl_test_macros::test]
    async fn test_login_disabled(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = false;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/login", axum::routing::post(login))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = LoginRequest {
            email: "test@example.com".to_string(),
            password: "password".to_string(),
        };

        let response = server.post("/auth/login").json(&request).await;
        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_login_invalid_email(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/login", axum::routing::post(login))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = LoginRequest {
            email: "nonexistent@example.com".to_string(),
            password: "password".to_string(),
        };

        let response = server.post("/auth/login").json(&request).await;
        response.assert_status(axum::http::StatusCode::UNAUTHORIZED);
    }

    #[dwctl_test_macros::test]
    async fn test_login_invalid_password(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let password_hash = password::hash_string_with_params(
            "correctpassword",
            Some(password::Argon2Params {
                memory_kib: 128,
                iterations: 1,
                parallelism: 1,
            }),
        )
        .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let mut user_repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest {
            username: "wrongpwuser".to_string(),
            email: "wrongpw@example.com".to_string(),
            display_name: Some("Wrong Password User".to_string()),
            avatar_url: None,
            is_admin: false,
            roles: vec![Role::StandardUser],
            auth_source: "native".to_string(),
            password_hash: Some(password_hash),
            external_user_id: None,
        };

        user_repo.create(&user_create).await.unwrap();
        drop(conn);

        let app = axum::Router::new()
            .route("/auth/login", axum::routing::post(login))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = LoginRequest {
            email: "wrongpw@example.com".to_string(),
            password: "wrongpassword".to_string(),
        };

        let response = server.post("/auth/login").json(&request).await;
        response.assert_status(axum::http::StatusCode::UNAUTHORIZED);
    }

    #[dwctl_test_macros::test]
    async fn test_login_user_without_password(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut user_repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest {
            username: "ssouser".to_string(),
            email: "sso@example.com".to_string(),
            display_name: Some("SSO User".to_string()),
            avatar_url: None,
            is_admin: false,
            roles: vec![Role::StandardUser],
            auth_source: "proxy".to_string(),
            password_hash: None,
            external_user_id: None,
        };

        user_repo.create(&user_create).await.unwrap();
        drop(conn);

        let app = axum::Router::new()
            .route("/auth/login", axum::routing::post(login))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = LoginRequest {
            email: "sso@example.com".to_string(),
            password: "anypassword".to_string(),
        };

        let response = server.post("/auth/login").json(&request).await;
        response.assert_status(axum::http::StatusCode::UNAUTHORIZED);
    }

    #[dwctl_test_macros::test]
    async fn test_logout(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/logout", axum::routing::post(logout))
            .with_state(state);

        let server = TestServer::new(app).unwrap();
        let response = server.post("/auth/logout").await;

        response.assert_status(axum::http::StatusCode::OK);

        let cookie_header = response.headers().get("set-cookie");
        assert!(cookie_header.is_some());
        let cookie_str = cookie_header.unwrap().to_str().unwrap();
        assert!(cookie_str.contains("Max-Age=0"));

        let body: AuthSuccessResponse = response.json();
        assert_eq!(body.message, "Logout successful");
    }

    #[dwctl_test_macros::test]
    async fn test_register_duplicate_email(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.allow_registration = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut user_repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest {
            username: "existinguser".to_string(),
            email: "duplicate@example.com".to_string(),
            display_name: Some("Existing User".to_string()),
            avatar_url: None,
            is_admin: false,
            roles: vec![Role::StandardUser],
            auth_source: "native".to_string(),
            password_hash: Some(
                password::hash_string_with_params(
                    "password",
                    Some(password::Argon2Params {
                        memory_kib: 128,
                        iterations: 1,
                        parallelism: 1,
                    }),
                )
                .unwrap(),
            ),
            external_user_id: None,
        };

        user_repo.create(&user_create).await.unwrap();
        drop(conn);

        let app = axum::Router::new()
            .route("/auth/register", axum::routing::post(register))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = RegisterRequest {
            username: "newuser".to_string(),
            email: "duplicate@example.com".to_string(),
            password: "password123".to_string(),
            display_name: None,
        };

        let response = server.post("/auth/register").json(&request).await;
        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_register_password_too_long(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.allow_registration = true;
        config.auth.native.password.max_length = 20;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/register", axum::routing::post(register))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = RegisterRequest {
            username: "testuser".to_string(),
            email: "test@example.com".to_string(),
            password: "thispasswordiswaytoolongandexceedsthelimit".to_string(),
            display_name: None,
        };

        let response = server.post("/auth/register").json(&request).await;
        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_register_registration_disabled(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.allow_registration = false;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/register", axum::routing::post(register))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = RegisterRequest {
            username: "testuser".to_string(),
            email: "test@example.com".to_string(),
            password: "password123".to_string(),
            display_name: None,
        };

        let response = server.post("/auth/register").json(&request).await;
        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_request_password_reset_disabled(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = false;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/password-reset", axum::routing::post(request_password_reset))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = PasswordResetRequest {
            email: "test@example.com".to_string(),
        };

        let response = server.post("/auth/password-reset").json(&request).await;
        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_request_password_reset_nonexistent_user(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/password-reset", axum::routing::post(request_password_reset))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = PasswordResetRequest {
            email: "nonexistent@example.com".to_string(),
        };

        let response = server.post("/auth/password-reset").json(&request).await;

        response.assert_status(axum::http::StatusCode::OK);
        let body: PasswordResetResponse = response.json();
        assert!(body.message.contains("If an account with that email exists"));
    }

    #[dwctl_test_macros::test]
    async fn test_request_password_reset_sso_user(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut user_repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest {
            username: "ssouser".to_string(),
            email: "sso@example.com".to_string(),
            display_name: None,
            avatar_url: None,
            is_admin: false,
            roles: vec![Role::StandardUser],
            auth_source: "proxy".to_string(),
            password_hash: None,
            external_user_id: None,
        };

        user_repo.create(&user_create).await.unwrap();
        drop(conn);

        let app = axum::Router::new()
            .route("/auth/password-reset", axum::routing::post(request_password_reset))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = PasswordResetRequest {
            email: "sso@example.com".to_string(),
        };

        let response = server.post("/auth/password-reset").json(&request).await;

        response.assert_status(axum::http::StatusCode::OK);
        let body: PasswordResetResponse = response.json();
        assert!(body.message.contains("If an account with that email exists"));
    }

    #[dwctl_test_macros::test]
    async fn test_confirm_password_reset_disabled(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = false;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route(
                "/auth/password-reset/{token_id}/confirm",
                axum::routing::post(confirm_password_reset),
            )
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let token_id = Uuid::new_v4();
        let request = PasswordResetConfirmRequest {
            token: "sometoken".to_string(),
            new_password: "newpassword123".to_string(),
        };

        let response = server
            .post(&format!("/auth/password-reset/{}/confirm", token_id))
            .json(&request)
            .await;

        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_confirm_password_reset_invalid_token(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route(
                "/auth/password-reset/{token_id}/confirm",
                axum::routing::post(confirm_password_reset),
            )
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let token_id = Uuid::new_v4();
        let request = PasswordResetConfirmRequest {
            token: "invalidtoken".to_string(),
            new_password: "newpassword123".to_string(),
        };

        let response = server
            .post(&format!("/auth/password-reset/{}/confirm", token_id))
            .json(&request)
            .await;

        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_confirm_password_reset_password_too_short(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.password.min_length = 10;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route(
                "/auth/password-reset/{token_id}/confirm",
                axum::routing::post(confirm_password_reset),
            )
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let token_id = Uuid::new_v4();
        let request = PasswordResetConfirmRequest {
            token: "sometoken".to_string(),
            new_password: "short".to_string(),
        };

        let response = server
            .post(&format!("/auth/password-reset/{}/confirm", token_id))
            .json(&request)
            .await;

        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_confirm_password_reset_password_too_long(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.password.max_length = 20;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route(
                "/auth/password-reset/{token_id}/confirm",
                axum::routing::post(confirm_password_reset),
            )
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let token_id = Uuid::new_v4();
        let request = PasswordResetConfirmRequest {
            token: "sometoken".to_string(),
            new_password: "thispasswordiswaytoolongandexceedsthelimit".to_string(),
        };

        let response = server
            .post(&format!("/auth/password-reset/{}/confirm", token_id))
            .json(&request)
            .await;

        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_password_reset_full_flow(pool: PgPool) {
        use crate::test::utils::create_test_config;

        let mut config = create_test_config();
        config.auth.native.enabled = true;

        let email_path = if let crate::config::EmailTransportConfig::File { path } = &config.email.transport {
            path.clone()
        } else {
            panic!("Expected File transport in test config");
        };

        let app = crate::Application::new_with_pool(config, Some(pool.clone()), None)
            .await
            .expect("Failed to create application");

        let (app, _bg_services) = app.into_test_server();

        let old_password_hash = password::hash_string_with_params(
            "oldpassword123",
            Some(password::Argon2Params {
                memory_kib: 128,
                iterations: 1,
                parallelism: 1,
            }),
        )
        .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let mut user_repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest {
            username: "resetuser".to_string(),
            email: "reset@example.com".to_string(),
            display_name: Some("Reset User".to_string()),
            avatar_url: None,
            is_admin: false,
            roles: vec![Role::StandardUser],
            auth_source: "native".to_string(),
            password_hash: Some(old_password_hash.clone()),
            external_user_id: None,
        };

        let _created_user = user_repo.create(&user_create).await.unwrap();
        drop(conn);

        let reset_request = PasswordResetRequest {
            email: "reset@example.com".to_string(),
        };

        let response = app.post("/authentication/password-resets").json(&reset_request).await;

        response.assert_status(axum::http::StatusCode::OK);
        let body: PasswordResetResponse = response.json();
        assert!(body.message.contains("If an account with that email exists"));

        let emails_dir = std::path::Path::new(&email_path);

        let mut email_files: Vec<_> = std::fs::read_dir(emails_dir).unwrap().filter_map(|e| e.ok()).collect();
        email_files.sort_by_key(|e| e.metadata().unwrap().modified().unwrap());

        let email_file = email_files.last().expect("No email file found");
        let email_content = std::fs::read_to_string(email_file.path()).unwrap();

        let decoded_content = email_content.replace("=\r\n", "").replace("=\n", "").replace("=3D", "=");

        let reset_link_start = decoded_content.find("/reset-password?id=").expect("Reset link not found");
        let link_portion = &decoded_content[reset_link_start..];

        let link_end = link_portion
            .find(&[' ', '\n', '\r', '"', '<', '>'][..])
            .unwrap_or(link_portion.len());
        let reset_link = &link_portion[..link_end];

        let url_parts: Vec<&str> = reset_link.split(&['?', '&'][..]).collect();
        let token_id_str = url_parts
            .iter()
            .find(|s| s.starts_with("id="))
            .and_then(|s| s.strip_prefix("id="))
            .expect("token_id not found in reset link");
        let token_str = url_parts
            .iter()
            .find(|s| s.starts_with("token="))
            .and_then(|s| s.strip_prefix("token="))
            .expect("token not found in reset link");

        let token_id = Uuid::parse_str(token_id_str).unwrap();
        let raw_token = token_str.to_string();

        let confirm_request = PasswordResetConfirmRequest {
            token: raw_token.clone(),
            new_password: "newpassword456".to_string(),
        };

        let response = app
            .post(&format!("/authentication/password-resets/{}/confirm", token_id))
            .json(&confirm_request)
            .await;

        response.assert_status(axum::http::StatusCode::OK);
        let body: PasswordResetResponse = response.json();
        assert_eq!(body.message, "Password has been reset successfully");

        let login_old_password = LoginRequest {
            email: "reset@example.com".to_string(),
            password: "oldpassword123".to_string(),
        };

        let response = app.post("/authentication/login").json(&login_old_password).await;

        response.assert_status(axum::http::StatusCode::UNAUTHORIZED);

        let login_new_password = LoginRequest {
            email: "reset@example.com".to_string(),
            password: "newpassword456".to_string(),
        };

        let response = app.post("/authentication/login").json(&login_new_password).await;

        response.assert_status(axum::http::StatusCode::OK);
        let body: AuthResponse = response.json();
        assert_eq!(body.user.email, "reset@example.com");
        assert_eq!(body.message, "Login successful");

        let reuse_request = PasswordResetConfirmRequest {
            token: raw_token,
            new_password: "anotherpassword789".to_string(),
        };

        let response = app
            .post(&format!("/authentication/password-resets/{}/confirm", token_id))
            .json(&reuse_request)
            .await;

        response.assert_status(axum::http::StatusCode::BAD_REQUEST);

        std::fs::remove_file(email_file.path()).ok();
    }

    #[dwctl_test_macros::test]
    async fn test_change_password_success_full(pool: PgPool) {
        use crate::test::utils::create_test_config;

        let mut config = create_test_config();
        config.auth.native.enabled = true;

        let app = crate::Application::new_with_pool(config, Some(pool.clone()), None)
            .await
            .expect("Failed to create application");

        let (app, _bg_services) = app.into_test_server();

        let old_password_hash = password::hash_string_with_params(
            "oldpassword123",
            Some(password::Argon2Params {
                memory_kib: 128,
                iterations: 1,
                parallelism: 1,
            }),
        )
        .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let mut user_repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest {
            username: "changepassworduser".to_string(),
            email: "changepassword@example.com".to_string(),
            display_name: Some("Change Password User".to_string()),
            avatar_url: None,
            is_admin: false,
            roles: vec![Role::StandardUser],
            auth_source: "native".to_string(),
            password_hash: Some(old_password_hash.clone()),
            external_user_id: Some("changepassworduser".to_string()),
        };

        let created_user = user_repo.create(&user_create).await.unwrap();
        drop(conn);

        let user_response = UserResponse::from(created_user);

        let change_request = ChangePasswordRequest {
            current_password: "oldpassword123".to_string(),
            new_password: "newpassword456".to_string(),
        };

        let auth_headers = crate::test::utils::add_auth_headers(&user_response);
        let response = app
            .post("/authentication/password-change")
            .json(&change_request)
            .add_header(&auth_headers[0].0, &auth_headers[0].1)
            .add_header(&auth_headers[1].0, &auth_headers[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::OK);
        let body: AuthSuccessResponse = response.json();
        assert_eq!(body.message, "Password changed successfully");

        let login_old = LoginRequest {
            email: "changepassword@example.com".to_string(),
            password: "oldpassword123".to_string(),
        };

        let response = app.post("/authentication/login").json(&login_old).await;
        response.assert_status(axum::http::StatusCode::UNAUTHORIZED);

        let login_new = LoginRequest {
            email: "changepassword@example.com".to_string(),
            password: "newpassword456".to_string(),
        };

        let response = app.post("/authentication/login").json(&login_new).await;
        response.assert_status(axum::http::StatusCode::OK);
    }

    #[dwctl_test_macros::test]
    async fn test_change_password_wrong_current(pool: PgPool) {
        use crate::test::utils::create_test_config;

        let mut config = create_test_config();
        config.auth.native.enabled = true;

        let app = crate::Application::new_with_pool(config, Some(pool.clone()), None)
            .await
            .expect("Failed to create application");

        let (app, _bg_services) = app.into_test_server();

        let password_hash = password::hash_string_with_params(
            "correctpassword",
            Some(password::Argon2Params {
                memory_kib: 128,
                iterations: 1,
                parallelism: 1,
            }),
        )
        .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let mut user_repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest {
            username: "wrongcurrentuser".to_string(),
            email: "wrongcurrent@example.com".to_string(),
            display_name: None,
            avatar_url: None,
            is_admin: false,
            roles: vec![Role::StandardUser],
            auth_source: "native".to_string(),
            password_hash: Some(password_hash),
            external_user_id: Some("wrongcurrentuser".to_string()),
        };

        let created_user = user_repo.create(&user_create).await.unwrap();
        drop(conn);

        let user_response = UserResponse::from(created_user);

        let change_request = ChangePasswordRequest {
            current_password: "wrongpassword".to_string(),
            new_password: "newpassword456".to_string(),
        };

        let auth_headers = crate::test::utils::add_auth_headers(&user_response);
        let response = app
            .post("/authentication/password-change")
            .json(&change_request)
            .add_header(&auth_headers[0].0, &auth_headers[0].1)
            .add_header(&auth_headers[1].0, &auth_headers[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::UNAUTHORIZED);
    }

    #[dwctl_test_macros::test]
    async fn test_change_password_sso_user_cannot_change(pool: PgPool) {
        use crate::test::utils::create_test_config;

        let mut config = create_test_config();
        config.auth.native.enabled = true;

        let app = crate::Application::new_with_pool(config, Some(pool.clone()), None)
            .await
            .expect("Failed to create application");

        let (app, _bg_services) = app.into_test_server();

        let mut conn = pool.acquire().await.unwrap();
        let mut user_repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest {
            username: "ssochangeuser".to_string(),
            email: "ssochange@example.com".to_string(),
            display_name: None,
            avatar_url: None,
            is_admin: false,
            roles: vec![Role::StandardUser],
            auth_source: "proxy".to_string(),
            password_hash: None,
            external_user_id: Some("ssochangeuser".to_string()),
        };

        let created_user = user_repo.create(&user_create).await.unwrap();
        drop(conn);

        let user_response = UserResponse::from(created_user);

        let change_request = ChangePasswordRequest {
            current_password: "anypassword".to_string(),
            new_password: "newpassword456".to_string(),
        };

        let auth_headers = crate::test::utils::add_auth_headers(&user_response);
        let response = app
            .post("/authentication/password-change")
            .json(&change_request)
            .add_header(&auth_headers[0].0, &auth_headers[0].1)
            .add_header(&auth_headers[1].0, &auth_headers[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_change_password_too_short(pool: PgPool) {
        use crate::test::utils::create_test_config;

        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.password.min_length = 10;

        let app = crate::Application::new_with_pool(config, Some(pool.clone()), None)
            .await
            .expect("Failed to create application");

        let (app, _bg_services) = app.into_test_server();

        let password_hash = password::hash_string_with_params(
            "oldpassword123",
            Some(password::Argon2Params {
                memory_kib: 128,
                iterations: 1,
                parallelism: 1,
            }),
        )
        .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let mut user_repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest {
            username: "shortpwchangeuser".to_string(),
            email: "shortpwchange@example.com".to_string(),
            display_name: None,
            avatar_url: None,
            is_admin: false,
            roles: vec![Role::StandardUser],
            auth_source: "native".to_string(),
            password_hash: Some(password_hash),
            external_user_id: Some("shortpwchangeuser".to_string()),
        };

        let created_user = user_repo.create(&user_create).await.unwrap();
        drop(conn);

        let user_response = UserResponse::from(created_user);

        let change_request = ChangePasswordRequest {
            current_password: "oldpassword123".to_string(),
            new_password: "short".to_string(),
        };

        let auth_headers = crate::test::utils::add_auth_headers(&user_response);
        let response = app
            .post("/authentication/password-change")
            .json(&change_request)
            .add_header(&auth_headers[0].0, &auth_headers[0].1)
            .add_header(&auth_headers[1].0, &auth_headers[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_change_password_too_long(pool: PgPool) {
        use crate::test::utils::create_test_config;

        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.password.max_length = 20;

        let app = crate::Application::new_with_pool(config, Some(pool.clone()), None)
            .await
            .expect("Failed to create application");

        let (app, _bg_services) = app.into_test_server();

        let password_hash = password::hash_string_with_params(
            "oldpassword",
            Some(password::Argon2Params {
                memory_kib: 128,
                iterations: 1,
                parallelism: 1,
            }),
        )
        .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let mut user_repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest {
            username: "longpwchangeuser".to_string(),
            email: "longpwchange@example.com".to_string(),
            display_name: None,
            avatar_url: None,
            is_admin: false,
            roles: vec![Role::StandardUser],
            auth_source: "native".to_string(),
            password_hash: Some(password_hash),
            external_user_id: Some("longpwchangeuser".to_string()),
        };

        let created_user = user_repo.create(&user_create).await.unwrap();
        drop(conn);

        let user_response = UserResponse::from(created_user);

        let change_request = ChangePasswordRequest {
            current_password: "oldpassword".to_string(),
            new_password: "thispasswordiswaytoolongandexceedsthelimit".to_string(),
        };

        let auth_headers = crate::test::utils::add_auth_headers(&user_response);
        let response = app
            .post("/authentication/password-change")
            .json(&change_request)
            .add_header(&auth_headers[0].0, &auth_headers[0].1)
            .add_header(&auth_headers[1].0, &auth_headers[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_change_password_when_disabled(pool: PgPool) {
        use crate::test::utils::create_test_config;

        let mut config = create_test_config();
        config.auth.native.enabled = false;

        let app = crate::Application::new_with_pool(config, Some(pool.clone()), None)
            .await
            .expect("Failed to create application");

        let (app, _bg_services) = app.into_test_server();

        let password_hash = password::hash_string_with_params(
            "oldpassword",
            Some(password::Argon2Params {
                memory_kib: 128,
                iterations: 1,
                parallelism: 1,
            }),
        )
        .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let mut user_repo = Users::new(&mut conn);

        let user_create = UserCreateDBRequest {
            username: "disabledchangeuser".to_string(),
            email: "disabledchange@example.com".to_string(),
            display_name: None,
            avatar_url: None,
            is_admin: false,
            roles: vec![Role::StandardUser],
            auth_source: "native".to_string(),
            password_hash: Some(password_hash),
            external_user_id: Some("disabledchangeuser".to_string()),
        };

        let created_user = user_repo.create(&user_create).await.unwrap();
        drop(conn);

        let user_response = UserResponse::from(created_user);

        let change_request = ChangePasswordRequest {
            current_password: "oldpassword".to_string(),
            new_password: "newpassword".to_string(),
        };

        let auth_headers = crate::test::utils::add_auth_headers(&user_response);
        let response = app
            .post("/authentication/password-change")
            .json(&change_request)
            .add_header(&auth_headers[0].0, &auth_headers[0].1)
            .add_header(&auth_headers[1].0, &auth_headers[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_register_with_configured_default_roles(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.allow_registration = true;
        config.auth.default_user_roles = vec![Role::StandardUser, Role::RequestViewer];

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let app = axum::Router::new()
            .route("/auth/register", axum::routing::post(register))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = RegisterRequest {
            username: "testuser".to_string(),
            email: "test@example.com".to_string(),
            password: "password123".to_string(),
            display_name: Some("Test User".to_string()),
        };

        let response = server.post("/auth/register").json(&request).await;

        response.assert_status(axum::http::StatusCode::CREATED);

        let body: AuthResponse = response.json();
        assert_eq!(body.user.email, "test@example.com");

        assert_eq!(body.user.roles.len(), 2);
        assert!(body.user.roles.contains(&Role::StandardUser));
        assert!(body.user.roles.contains(&Role::RequestViewer));
    }

    #[dwctl_test_macros::test]
    async fn test_register_standard_user_role_always_present(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.allow_registration = true;
        config.auth.default_user_roles = vec![Role::RequestViewer];

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let app = axum::Router::new()
            .route("/auth/register", axum::routing::post(register))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let request = RegisterRequest {
            username: "testuser2".to_string(),
            email: "test2@example.com".to_string(),
            password: "password123".to_string(),
            display_name: Some("Test User 2".to_string()),
        };

        let response = server.post("/auth/register").json(&request).await;

        response.assert_status(axum::http::StatusCode::CREATED);

        let body: AuthResponse = response.json();

        assert!(body.user.roles.contains(&Role::StandardUser));
        assert!(body.user.roles.contains(&Role::RequestViewer));
    }

    #[dwctl_test_macros::test]
    async fn test_session_cookie_includes_domain_when_configured(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.allow_registration = true;
        config.auth.native.session.cookie_domain = Some(".example.com".to_string());

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/register", axum::routing::post(register))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let response = server
            .post("/auth/register")
            .json(&RegisterRequest {
                username: "domaintest".to_string(),
                email: "domain@example.com".to_string(),
                password: "password123".to_string(),
                display_name: None,
            })
            .await;

        response.assert_status(axum::http::StatusCode::CREATED);
        let cookie = response.headers().get("set-cookie").unwrap().to_str().unwrap();
        assert!(cookie.contains("Domain=.example.com"), "cookie should include Domain: {cookie}");
    }

    #[dwctl_test_macros::test]
    async fn test_session_cookie_omits_domain_when_not_configured(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        config.auth.native.allow_registration = true;
        config.auth.native.session.cookie_domain = None;

        let state = crate::test::utils::create_test_app_state_with_config(pool, config).await;

        let app = axum::Router::new()
            .route("/auth/register", axum::routing::post(register))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let response = server
            .post("/auth/register")
            .json(&RegisterRequest {
                username: "nodomaintest".to_string(),
                email: "nodomain@example.com".to_string(),
                password: "password123".to_string(),
                display_name: None,
            })
            .await;

        response.assert_status(axum::http::StatusCode::CREATED);
        let cookie = response.headers().get("set-cookie").unwrap().to_str().unwrap();
        assert!(!cookie.contains("Domain="), "cookie should not include Domain: {cookie}");
    }


    async fn cli_callback_request(
        server: &TestServer,
        external_id: &str,
        email: &str,
        port: u16,
        state: &str,
        org: Option<&str>,
    ) -> axum_test::TestResponse {
        let mut url_builder = url::Url::parse("http://localhost/admin/api/v1/auth/cli-callback").unwrap();
        url_builder
            .query_pairs_mut()
            .append_pair("port", &port.to_string())
            .append_pair("state", state);
        if let Some(org_slug) = org {
            url_builder.query_pairs_mut().append_pair("org", org_slug);
        }
        let url = format!("{}?{}", url_builder.path(), url_builder.query().unwrap_or(""));

        server
            .get(&url)
            .add_header("x-doubleword-user", external_id)
            .add_header("x-doubleword-email", email)
            .await
    }

    fn parse_redirect_params(response: &axum_test::TestResponse) -> std::collections::HashMap<String, String> {
        let location = response.headers().get("location").unwrap().to_str().unwrap();
        url::Url::parse(location)
            .unwrap()
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect()
    }

    #[dwctl_test_macros::test]
    async fn test_cli_login_personal_success(pool: PgPool) {
        let (server, _bg) = crate::test::utils::create_test_app(pool.clone(), false).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let external_id = user.external_user_id.unwrap();

        let response = cli_callback_request(&server, &external_id, &user.email, 12345, "test-state", None).await;

        response.assert_status(axum::http::StatusCode::FOUND);

        let location = response.headers().get("location").unwrap().to_str().unwrap();
        assert!(
            location.starts_with("http://127.0.0.1:12345/callback?"),
            "unexpected redirect: {location}"
        );

        let params = parse_redirect_params(&response);

        assert_eq!(params.get("state").unwrap(), "test-state");

        assert!(params.contains_key("inference_key"), "missing inference_key");
        assert!(params.contains_key("platform_key"), "missing platform_key");
        assert!(params.contains_key("inference_key_id"), "missing inference_key_id");
        assert!(params.contains_key("platform_key_id"), "missing platform_key_id");

        assert_eq!(params.get("user_id").unwrap(), &user.id.to_string());
        assert_eq!(
            params.get("account_name").unwrap(),
            &user.username,
            "personal account_name should be the username"
        );
        assert_eq!(params.get("account_type").unwrap(), "personal");
        assert!(!params.contains_key("org_id"));
        assert!(!params.contains_key("org_name"));

        assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
        assert_eq!(response.headers().get("pragma").unwrap(), "no-cache");
        assert_eq!(response.headers().get("referrer-policy").unwrap(), "no-referrer");
    }

    #[dwctl_test_macros::test]
    async fn test_cli_login_org_success(pool: PgPool) {
        let (server, _bg) = crate::test::utils::create_test_app(pool.clone(), false).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let external_id = user.external_user_id.clone().unwrap();
        let org = crate::test::utils::create_test_org(&pool, user.id).await;

        let response = cli_callback_request(&server, &external_id, &user.email, 54321, "org-state", Some(&org.username)).await;

        response.assert_status(axum::http::StatusCode::FOUND);

        let params = parse_redirect_params(&response);
        let org_id_str = org.id.to_string();

        assert_eq!(params.get("user_id").unwrap(), &org_id_str, "user_id should be org id");
        assert_eq!(params.get("org_id").unwrap(), &org_id_str);
        assert_eq!(params.get("account_type").unwrap(), "organization");
        assert!(params.contains_key("org_name"), "org_name should be present");
        assert!(
            params.get("account_name").unwrap().contains('@'),
            "org account_name should be username@org-slug, got: {}",
            params.get("account_name").unwrap()
        );
    }

    #[dwctl_test_macros::test]
    async fn test_cli_callback_unknown_org(pool: PgPool) {
        let (server, _bg) = crate::test::utils::create_test_app(pool.clone(), false).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let external_id = user.external_user_id.unwrap();

        let response = cli_callback_request(&server, &external_id, &user.email, 12345, "s", Some("nonexistent-org")).await;

        response.assert_status(axum::http::StatusCode::BAD_REQUEST);

        let mut conn = pool.acquire().await.unwrap();
        let key_count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM api_keys WHERE created_by = $1 AND name LIKE 'DW CLI%' AND is_deleted = false",
            user.id
        )
        .fetch_one(&mut *conn)
        .await
        .unwrap()
        .unwrap_or(0);

        assert_eq!(key_count, 0, "no CLI keys should be created when org lookup fails");
    }

    #[dwctl_test_macros::test]
    async fn test_cli_callback_rejects_api_key_auth(pool: PgPool) {
        let (server, _bg) = crate::test::utils::create_test_app(pool.clone(), false).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut repo = crate::db::handlers::api_keys::ApiKeys::new(&mut conn);
        let key = repo
            .create(&crate::db::models::api_keys::ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "test-key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Platform,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();
        drop(conn);

        let response = server
            .get("/admin/api/v1/auth/cli-callback?port=12345&state=s")
            .add_header("authorization", &format!("Bearer {}", key.secret))
            .await;

        response.assert_status(axum::http::StatusCode::UNAUTHORIZED);
    }

    #[dwctl_test_macros::test]
    async fn test_cli_callback_rejects_invalid_port(pool: PgPool) {
        let (server, _bg) = crate::test::utils::create_test_app(pool.clone(), false).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let external_id = user.external_user_id.unwrap();

        for port in [0, 80, 443, 1023] {
            let response = cli_callback_request(&server, &external_id, &user.email, port, "s", None).await;
            response.assert_status(axum::http::StatusCode::BAD_REQUEST);
        }
    }
}
