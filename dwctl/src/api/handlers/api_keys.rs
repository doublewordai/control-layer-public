
use crate::api::models::api_keys::ListApiKeysQuery;
use crate::{
    AppState,
    api::models::{
        api_keys::{ApiKeyCreate, ApiKeyInfoResponse, ApiKeyResponse, ApiKeySecretResponse, ApiKeyUpdate},
        pagination::PaginatedResponse,
        users::CurrentUser,
    },
    auth::permissions::{
        can_create_all_resources, can_create_own_resource, can_delete_all_resources, can_delete_own_resource, can_read_all_resources,
        can_read_own_resource, can_update_all_resources, can_update_own_resource, is_org_member, resolve_key_capabilities,
    },
    crypto::hash_api_key,
    db::handlers::{Repository, api_keys::ApiKeyFilter, api_keys::ApiKeys},
    db::models::api_keys::{ApiKeyCreateDBRequest, ApiKeyPurpose, ApiKeyUpdateDBRequest, CreatedApiKey},
    errors::{Error, Result},
    keystore::Keystore,
    pending_secrets::resolve_reveal,
    types::{ApiKeyId, Operation, Permission, Resource, UserIdOrCurrent},
};
use rust_decimal::Decimal;
use sqlx_pool_router::PoolProvider;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use sqlx::Acquire;

const VALID_CAP_INTERVALS: [&str; 3] = ["daily", "weekly", "monthly"];

const NO_KEYSTORE_FOR_ISSUED_KEY: &str = "Issuing an API key to another user needs a keystore to hold its secret until the holder reveals it, and this server has none configured. Create the key from the holder's own account instead, or ask an administrator to configure the keystore.";

fn wrap_pending_secret(keystore: &Keystore, secret: &str) -> Result<Vec<u8>> {
    keystore.wrap(secret.as_bytes()).map_err(|e| {
        tracing::error!(error = %e, "failed to wrap pending API key secret");
        Error::Internal {
            operation: "wrap pending API key secret".to_string(),
        }
    })
}

fn validate_cap_fields(spend_limit: Option<&Decimal>, spend_limit_interval: Option<&str>) -> Result<()> {
    if let Some(limit) = spend_limit
        && *limit <= Decimal::ZERO
    {
        return Err(Error::BadRequest {
            message: "spend_limit must be greater than zero".to_string(),
        });
    }
    if let Some(interval) = spend_limit_interval {
        if !VALID_CAP_INTERVALS.contains(&interval) {
            return Err(Error::BadRequest {
                message: format!("spend_limit_interval must be one of {VALID_CAP_INTERVALS:?} (calendar-aligned UTC windows)"),
            });
        }
        if spend_limit.is_none() {
            return Err(Error::BadRequest {
                message: "spend_limit_interval requires spend_limit".to_string(),
            });
        }
    }
    Ok(())
}

#[utoipa::path(
    post,
    path = "/users/{user_id}/api-keys",
    tag = "api_keys",
    summary = "Create API key",
    description = "Create an API key for the current user or a specified user",
    params(
        ("user_id" = String, Path, description = "User ID (UUID) or 'current' for current user"),
    ),
    responses(
        (status = 201, description = "API key created successfully", body = ApiKeyResponse),
        (status = 400, description = "Bad request - invalid API key data"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden - can only manage own API keys unless admin"),
        (status = 500, description = "Internal server error"),
        (status = 501, description = "Issuing a key to another user needs a keystore, and none is configured"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn create_user_api_key<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(user_id): Path<UserIdOrCurrent>,
    current_user: CurrentUser,
    Json(data): Json<ApiKeyCreate>,
) -> Result<(StatusCode, Json<ApiKeyResponse>)> {
    if data.name.trim().is_empty() {
        return Err(Error::BadRequest {
            message: "API key name cannot be empty".to_string(),
        });
    }
    validate_cap_fields(data.spend_limit.as_ref(), data.spend_limit_interval.as_deref())?;

    let target_user_id = match user_id {
        UserIdOrCurrent::Current(_) => current_user.id,
        UserIdOrCurrent::Id(uuid) => uuid,
    };

    let can_create_all = can_create_all_resources(&current_user, Resource::ApiKeys);
    let can_create_own = can_create_own_resource(&current_user, Resource::ApiKeys, target_user_id);

    if !can_create_all && !can_create_own {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let member = is_org_member(&current_user, target_user_id, &mut conn)
            .await
            .map_err(Error::Database)?;
        if !member {
            return Err(Error::InsufficientPermissions {
                required: Permission::Any(vec![
                    Permission::Allow(Resource::ApiKeys, Operation::CreateAll),
                    Permission::Allow(Resource::ApiKeys, Operation::CreateOwn),
                ]),
                action: Operation::CreateOwn,
                resource: format!("API keys for user {target_user_id}"),
            });
        }
    }

    match &data.purpose {
        crate::db::models::api_keys::ApiKeyPurpose::Batch
        | crate::db::models::api_keys::ApiKeyPurpose::Playground
        | crate::db::models::api_keys::ApiKeyPurpose::Continuation => {
            return Err(Error::BadRequest {
                message:
                    "Cannot manually create API keys with 'batch', 'playground' or 'continuation' purpose. These are reserved for internal system use."
                        .to_string(),
            });
        }
        _ => {}
    }

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;

    let caps = resolve_key_capabilities(&current_user, target_user_id, &mut pool_conn)
        .await
        .map_err(Error::Database)?;
    let target_is_org = caps.target_is_org;

    if data.member_id.is_some() && !can_create_all && !caps.is_org_manager {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::ApiKeys, Operation::CreateAll),
            action: Operation::CreateAll,
            resource: "API keys with member_id (requires PlatformManager or org owner/admin)".to_string(),
        });
    }

    if !can_create_all && !caps.can_self_manage_keys() {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::ApiKeys, Operation::CreateOwn),
            action: Operation::CreateOwn,
            resource: "API keys in a managed-keys organization (ask an org owner/admin to issue you a key)".to_string(),
        });
    }

    if let Some(member_id) = data.member_id {
        if !target_is_org {
            return Err(Error::BadRequest {
                message: "member_id can only be used when creating keys for an organization".to_string(),
            });
        }
        let mut org_repo = crate::db::handlers::Organizations::new(&mut pool_conn);
        let role = org_repo
            .get_user_org_role(member_id, target_user_id)
            .await
            .map_err(Error::Database)?;
        if role.is_none() {
            return Err(Error::BadRequest {
                message: format!("User {member_id} is not a member of organization {target_user_id}"),
            });
        }
    }

    let creating_for_other = can_create_all && target_user_id != current_user.id;
    let created_by = if target_is_org {
        data.member_id.unwrap_or(current_user.id)
    } else if creating_for_other {
        target_user_id
    } else {
        current_user.id
    };

    let issued_to_other = created_by != current_user.id;
    let keystore = match (&state.keystore, issued_to_other) {
        (Some(keystore), true) => Some(keystore),
        (None, true) => {
            return Err(Error::NotImplemented {
                message: NO_KEYSTORE_FOR_ISSUED_KEY.to_string(),
            });
        }
        (_, false) => None,
    };

    drop(pool_conn);

    let has_cap = data.spend_limit.is_some();
    let db_request = ApiKeyCreateDBRequest::new(target_user_id, created_by, data);

    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = ApiKeys::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);

    let CreatedApiKey { key: mut api_key, secret } = repo.create(&db_request).await?;

    if let Some(keystore) = keystore {
        let pending_secret = wrap_pending_secret(keystore, &secret)?;
        repo.mark_secret_reveal_pending(api_key.id, &pending_secret).await?;
        api_key.secret_revealed_at = None;
    }

    if has_cap {
        repo.get_or_create_child_hidden_key(api_key.id).await?;
        repo.reset_spend_window(api_key.id).await?;
    }

    let key_id = api_key.id;
    let spend_states = repo.get_spend_states(&[key_id]).await?;
    tx.commit().await.map_err(|e| Error::Database(e.into()))?;


    Ok((
        StatusCode::CREATED,
        Json(ApiKeyResponse::new(api_key, secret).with_spend_state(spend_states.get(&key_id))),
    ))
}

#[utoipa::path(
    get,
    path = "/users/{user_id}/api-keys",
    tag = "api_keys",
    summary = "List API keys",
    description = "List API keys for the current user or a specified user",
    params(
        ("user_id" = String, Path, description = "User ID (UUID) or 'current' for current user"),
        ListApiKeysQuery
    ),
    responses(
        (status = 200, description = "Paginated list of API keys", body = PaginatedResponse<ApiKeyInfoResponse>),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden - can only view own API keys unless admin"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn list_user_api_keys<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(user_id): Path<UserIdOrCurrent>,
    Query(query): Query<ListApiKeysQuery>,
    current_user: CurrentUser,
) -> Result<Json<PaginatedResponse<ApiKeyInfoResponse>>> {
    let target_user_id = match user_id {
        UserIdOrCurrent::Current(_) => current_user.id,
        UserIdOrCurrent::Id(uuid) => uuid,
    };

    let can_read_all = can_read_all_resources(&current_user, Resource::ApiKeys);
    let can_read_own = can_read_own_resource(&current_user, Resource::ApiKeys, target_user_id);

    if !can_read_all && !can_read_own {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let member = is_org_member(&current_user, target_user_id, &mut conn)
            .await
            .map_err(Error::Database)?;
        if !member {
            return Err(Error::InsufficientPermissions {
                required: Permission::Any(vec![
                    Permission::Allow(Resource::ApiKeys, Operation::ReadAll),
                    Permission::Allow(Resource::ApiKeys, Operation::ReadOwn),
                ]),
                action: Operation::ReadOwn,
                resource: format!("API keys for user {target_user_id}"),
            });
        }
    }

    let caps = {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        resolve_key_capabilities(&current_user, target_user_id, &mut conn)
            .await
            .map_err(Error::Database)?
    };
    let skip_created_by_filter = can_read_all || caps.is_org_manager;

    let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = ApiKeys::new(&mut pool_conn);

    let skip = query.pagination.skip();
    let limit = query.pagination.limit();

    let created_by = if skip_created_by_filter {
        query.created_by
    } else {
        if let Some(requested) = query.created_by
            && requested != current_user.id
        {
            return Err(Error::InsufficientPermissions {
                required: Permission::Allow(Resource::ApiKeys, Operation::ReadAll),
                action: Operation::ReadAll,
                resource: format!("API keys held by user {requested}"),
            });
        }
        Some(current_user.id)
    };
    let filter = ApiKeyFilter {
        skip,
        limit,
        user_id: Some(target_user_id),
        created_by,
    };

    let total_count = repo.count(&filter).await?;
    let api_keys = repo.list(&filter).await?;

    let ids: Vec<ApiKeyId> = api_keys.iter().map(|k| k.id).collect();
    let spend_states = repo.get_spend_states(&ids).await?;

    let data: Vec<ApiKeyInfoResponse> = api_keys
        .into_iter()
        .map(|k| {
            let state = spend_states.get(&k.id);
            ApiKeyInfoResponse::from(k).with_spend_state(state)
        })
        .collect();

    Ok(Json(PaginatedResponse::new(data, total_count, skip, limit)))
}

#[utoipa::path(
    get,
    path = "/users/{user_id}/api-keys/{id}",
    tag = "api_keys",
    summary = "Get API key",
    description = "Get a specific API key for the current user or a specified user",
    params(
        ("user_id" = String, Path, description = "User ID (UUID) or 'current' for current user"),
        ("id" = uuid::Uuid, Path, description = "API key ID to retrieve"),
    ),
    responses(
        (status = 200, description = "API key information", body = ApiKeyInfoResponse),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden - can only view own API keys unless admin"),
        (status = 404, description = "API key not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_user_api_key<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((user_id, api_key_id)): Path<(UserIdOrCurrent, ApiKeyId)>,
    current_user: CurrentUser,
) -> Result<Json<ApiKeyInfoResponse>> {
    let target_user_id = match user_id {
        UserIdOrCurrent::Current(_) => current_user.id,
        UserIdOrCurrent::Id(uuid) => uuid,
    };

    let can_read_all = can_read_all_resources(&current_user, Resource::ApiKeys);
    let can_read_own = can_read_own_resource(&current_user, Resource::ApiKeys, target_user_id);

    if !can_read_all && !can_read_own {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let member = is_org_member(&current_user, target_user_id, &mut conn)
            .await
            .map_err(Error::Database)?;
        if !member {
            return Err(Error::InsufficientPermissions {
                required: Permission::Any(vec![
                    Permission::Allow(Resource::ApiKeys, Operation::ReadAll),
                    Permission::Allow(Resource::ApiKeys, Operation::ReadOwn),
                ]),
                action: Operation::ReadOwn,
                resource: format!("API keys for user {target_user_id}"),
            });
        }
    }

    let caps = {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        resolve_key_capabilities(&current_user, target_user_id, &mut conn)
            .await
            .map_err(Error::Database)?
    };
    let skip_created_by_filter = can_read_all || caps.is_org_manager;

    let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = ApiKeys::new(&mut pool_conn);

    let api_key = repo
        .get_by_id(api_key_id)
        .await?
        .filter(|key| key.user_id == target_user_id)
        .filter(|key| skip_created_by_filter || key.created_by == current_user.id)
        .filter(|key| !key.hidden)
        .filter(|key| key.parent_api_key_id.is_none())
        .filter(|key| matches!(key.purpose, ApiKeyPurpose::Realtime | ApiKeyPurpose::Platform))
        .ok_or_else(|| Error::NotFound {
            resource: "API key".to_string(),
            id: api_key_id.to_string(),
        })?;

    let key_id = api_key.id;
    let spend_states = repo.get_spend_states(&[key_id]).await?;

    Ok(Json(ApiKeyInfoResponse::from(api_key).with_spend_state(spend_states.get(&key_id))))
}

#[utoipa::path(
    patch,
    path = "/users/{user_id}/api-keys/{id}",
    tag = "api_keys",
    summary = "Update API key",
    description = "Update an API key's name, description, or spending cap. \
                   Setting a cap where none existed provisions cap-scope batch/flex execution and starts a fresh spend window; \
                   changing the cap interval or passing reset_window also restarts the window; \
                   passing spend_limit: null removes the cap.",
    request_body = ApiKeyUpdate,
    params(
        ("user_id" = String, Path, description = "User ID (UUID) or 'current' for current user"),
        ("id" = uuid::Uuid, Path, description = "API key ID to update"),
    ),
    responses(
        (status = 200, description = "Updated API key", body = ApiKeyInfoResponse),
        (status = 400, description = "Bad request - invalid update data"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden - can only manage own API keys unless admin"),
        (status = 404, description = "API key not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn update_user_api_key<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((user_id, api_key_id)): Path<(UserIdOrCurrent, ApiKeyId)>,
    current_user: CurrentUser,
    Json(data): Json<ApiKeyUpdate>,
) -> Result<Json<ApiKeyInfoResponse>> {
    let target_user_id = match user_id {
        UserIdOrCurrent::Current(_) => current_user.id,
        UserIdOrCurrent::Id(uuid) => uuid,
    };

    let can_update_all = can_update_all_resources(&current_user, Resource::ApiKeys);
    let can_update_own = can_update_own_resource(&current_user, Resource::ApiKeys, target_user_id);

    if !can_update_all && !can_update_own {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let member = is_org_member(&current_user, target_user_id, &mut conn)
            .await
            .map_err(Error::Database)?;
        if !member {
            return Err(Error::InsufficientPermissions {
                required: Permission::Any(vec![
                    Permission::Allow(Resource::ApiKeys, Operation::UpdateAll),
                    Permission::Allow(Resource::ApiKeys, Operation::UpdateOwn),
                ]),
                action: Operation::UpdateOwn,
                resource: format!("API keys for user {target_user_id}"),
            });
        }
    }

    let caps = {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        resolve_key_capabilities(&current_user, target_user_id, &mut conn)
            .await
            .map_err(Error::Database)?
    };
    if !can_update_all && !caps.is_org_manager && !caps.can_self_manage_keys() {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::ApiKeys, Operation::UpdateOwn),
            action: Operation::UpdateOwn,
            resource: "API keys in a managed-keys organization (issued keys are read-only; ask an org owner/admin)".to_string(),
        });
    }
    let skip_created_by_filter = can_update_all || caps.is_org_manager;

    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = ApiKeys::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);

    let key = repo
        .get_by_id(api_key_id)
        .await?
        .filter(|key| key.user_id == target_user_id)
        .filter(|key| skip_created_by_filter || key.created_by == current_user.id)
        .filter(|key| !key.hidden)
        .filter(|key| key.parent_api_key_id.is_none())
        .filter(|key| matches!(key.purpose, ApiKeyPurpose::Realtime | ApiKeyPurpose::Platform))
        .ok_or_else(|| Error::NotFound {
            resource: "API key".to_string(),
            id: api_key_id.to_string(),
        })?;

    let old_limit = key.spend_limit;
    let old_interval = key.spend_limit_interval.clone();
    let new_limit = match data.spend_limit {
        None => old_limit,
        Some(v) => v,
    };
    let new_interval = match data.spend_limit_interval.clone() {
        None => {
            if new_limit.is_none() { None } else { old_interval.clone() }
        }
        Some(v) => v,
    };
    validate_cap_fields(new_limit.as_ref(), new_interval.as_deref())?;

    let reset_window = data.reset_window.unwrap_or(false);
    if reset_window && new_limit.is_none() {
        return Err(Error::BadRequest {
            message: "reset_window requires a spending cap".to_string(),
        });
    }

    if data.name.is_some() || data.description.is_some() {
        if let Some(name) = &data.name
            && name.trim().is_empty()
        {
            return Err(Error::BadRequest {
                message: "API key name cannot be empty".to_string(),
            });
        }
        repo.update(
            api_key_id,
            &ApiKeyUpdateDBRequest {
                name: data.name.clone(),
                description: data.description.clone(),
            },
        )
        .await?;
    }

    let cap_changed = new_limit != old_limit || new_interval != old_interval;
    let newly_capped = old_limit.is_none() && new_limit.is_some();
    let interval_changed = new_limit.is_some() && new_interval != old_interval;

    if cap_changed {
        repo.update_spend_cap(api_key_id, new_limit, new_interval.clone()).await?;
    }
    if newly_capped {
        repo.get_or_create_child_hidden_key(api_key_id).await?;
    }
    if newly_capped || interval_changed || reset_window {
        repo.reset_spend_window(api_key_id).await?;
    }

    if reset_window && !cap_changed {
        repo.notify_spend_cap_rearm().await?;
    }

    let updated = repo.get_by_id(api_key_id).await?.ok_or_else(|| Error::NotFound {
        resource: "API key".to_string(),
        id: api_key_id.to_string(),
    })?;
    let spend_states = repo.get_spend_states(&[api_key_id]).await?;

    tx.commit().await.map_err(|e| Error::Database(e.into()))?;

    Ok(Json(
        ApiKeyInfoResponse::from(updated).with_spend_state(spend_states.get(&api_key_id)),
    ))
}

#[utoipa::path(
    post,
    path = "/users/{user_id}/api-keys/{id}/rotate",
    tag = "api_keys",
    summary = "Rotate API key secret",
    description = "Replace the key's secret with a new one. The old secret is invalidated immediately for NEW requests; batches already submitted keep running (cancel them separately if the old secret is compromised). The key's usage limit and counted spend are unaffected.",
    params(
        ("user_id" = String, Path, description = "User ID (UUID) or 'current' for current user"),
        ("id" = uuid::Uuid, Path, description = "API key ID"),
    ),
    responses(
        (status = 200, description = "The new API key secret", body = ApiKeySecretResponse),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "API key not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn rotate_user_api_key<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((user_id, api_key_id)): Path<(UserIdOrCurrent, ApiKeyId)>,
    current_user: CurrentUser,
) -> Result<Json<ApiKeySecretResponse>> {
    let target_user_id = match user_id {
        UserIdOrCurrent::Current(_) => current_user.id,
        UserIdOrCurrent::Id(uuid) => uuid,
    };

    let can_update_all = can_update_all_resources(&current_user, Resource::ApiKeys);
    let can_update_own = can_update_own_resource(&current_user, Resource::ApiKeys, target_user_id);
    if !can_update_all && !can_update_own {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let member = is_org_member(&current_user, target_user_id, &mut conn)
            .await
            .map_err(Error::Database)?;
        if !member {
            return Err(Error::InsufficientPermissions {
                required: Permission::Any(vec![
                    Permission::Allow(Resource::ApiKeys, Operation::UpdateAll),
                    Permission::Allow(Resource::ApiKeys, Operation::UpdateOwn),
                ]),
                action: Operation::UpdateOwn,
                resource: format!("API keys for user {target_user_id}"),
            });
        }
    }

    let caps = {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        resolve_key_capabilities(&current_user, target_user_id, &mut conn)
            .await
            .map_err(Error::Database)?
    };
    let skip_created_by_filter = can_update_all || caps.is_org_manager;

    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = ApiKeys::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
    let api_key = repo
        .get_by_id(api_key_id)
        .await?
        .filter(|key| key.user_id == target_user_id)
        .filter(|key| skip_created_by_filter || key.created_by == current_user.id)
        .filter(|key| !key.hidden)
        .filter(|key| key.parent_api_key_id.is_none())
        .filter(|key| matches!(key.purpose, ApiKeyPurpose::Realtime | ApiKeyPurpose::Platform))
        .ok_or_else(|| Error::NotFound {
            resource: "API key".to_string(),
            id: api_key_id.to_string(),
        })?;

    let new_secret = repo.rotate_secret(api_key.id).await?;
    if api_key.secret_revealed_at.is_none() {
        match &state.keystore {
            Some(keystore) => {
                let pending_secret = wrap_pending_secret(keystore, &new_secret)?;
                repo.set_pending_secret(api_key.id, &pending_secret, &hash_api_key(&new_secret))
                    .await?;
            }
            None => repo.mark_secret_revealed(api_key.id).await?,
        }
    }
    tx.commit().await.map_err(|e| Error::Database(e.into()))?;

    tracing::info!(
        rotated_by = %current_user.id,
        api_key_id = %api_key.id,
        key_owner = %api_key.user_id,
        key_created_by = %api_key.created_by,
        "api key secret rotated"
    );
    metrics::counter!("dwctl_api_key_rotations_total").increment(1);

    Ok(Json(ApiKeySecretResponse { key: new_secret }))
}

#[utoipa::path(
    post,
    path = "/users/{user_id}/api-keys/{id}/reveal",
    tag = "api_keys",
    summary = "Reveal an issued API key's secret (one-off)",
    description = "Fetch the secret of a key issued to you, exactly once. Only the key's holder may call this; afterwards (and for self-created keys) the secret is only obtainable by rotation.",
    params(
        ("user_id" = String, Path, description = "User ID (UUID) or 'current' for current user"),
        ("id" = uuid::Uuid, Path, description = "API key ID to reveal"),
    ),
    responses(
        (status = 200, description = "The API key secret", body = ApiKeySecretResponse),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden - insufficient permissions"),
        (status = 404, description = "API key not found (or not held by the current user)"),
        (status = 409, description = "Conflict - the one-off reveal was already used, or its secret is no longer held (rotate the key)"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn reveal_user_api_key<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((user_id, api_key_id)): Path<(UserIdOrCurrent, ApiKeyId)>,
    current_user: CurrentUser,
) -> Result<Json<ApiKeySecretResponse>> {
    let target_user_id = match user_id {
        UserIdOrCurrent::Current(_) => current_user.id,
        UserIdOrCurrent::Id(uuid) => uuid,
    };

    let can_update_all = can_update_all_resources(&current_user, Resource::ApiKeys);
    let can_update_own = can_update_own_resource(&current_user, Resource::ApiKeys, target_user_id);
    if !can_update_all && !can_update_own {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let member = is_org_member(&current_user, target_user_id, &mut conn)
            .await
            .map_err(Error::Database)?;
        if !member {
            return Err(Error::InsufficientPermissions {
                required: Permission::Any(vec![
                    Permission::Allow(Resource::ApiKeys, Operation::UpdateAll),
                    Permission::Allow(Resource::ApiKeys, Operation::UpdateOwn),
                ]),
                action: Operation::UpdateOwn,
                resource: format!("API keys for user {target_user_id}"),
            });
        }
    }

    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = ApiKeys::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
    let api_key = repo
        .get_by_id(api_key_id)
        .await?
        .filter(|key| key.user_id == target_user_id)
        .filter(|key| key.created_by == current_user.id)
        .filter(|key| !key.hidden)
        .filter(|key| key.parent_api_key_id.is_none())
        .ok_or_else(|| Error::NotFound {
            resource: "API key".to_string(),
            id: api_key_id.to_string(),
        })?;

    let pending = repo.reveal_secret_once(api_key.id).await?.ok_or_else(|| Error::Conflict {
        message: "This key's one-off reveal has already been used. Rotate the key to get a fresh secret.".to_string(),
        conflicts: None,
    })?;
    let secret = resolve_reveal(state.keystore.as_ref(), &pending).ok_or_else(|| {
        tracing::warn!(api_key_id = %api_key.id, "pending API key secret could not be recovered for reveal");
        Error::Conflict {
            message: "This key's secret is no longer held for reveal. Rotate the key to get a fresh secret.".to_string(),
            conflicts: None,
        }
    })?;
    tx.commit().await.map_err(|e| Error::Database(e.into()))?;

    tracing::info!(
        revealed_by = %current_user.id,
        api_key_id = %api_key.id,
        key_owner = %api_key.user_id,
        "api key secret revealed (one-off)"
    );
    metrics::counter!("dwctl_api_key_reveals_total").increment(1);

    Ok(Json(ApiKeySecretResponse { key: secret }))
}

#[utoipa::path(
    delete,
    path = "/users/{user_id}/api-keys/{id}",
    tag = "api_keys",
    summary = "Delete API key",
    description = "Delete a specific API key for the current user or a specified user",
    params(
        ("user_id" = String, Path, description = "User ID (UUID) or 'current' for current user"),
        ("id" = uuid::Uuid, Path, description = "API key ID to delete"),
    ),
    responses(
        (status = 204, description = "API key deleted successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden - can only delete own API keys unless admin"),
        (status = 404, description = "API key not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn delete_user_api_key<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((user_id, api_key_id)): Path<(UserIdOrCurrent, ApiKeyId)>,
    current_user: CurrentUser,
) -> Result<StatusCode> {
    let target_user_id = match user_id {
        UserIdOrCurrent::Current(_) => current_user.id,
        UserIdOrCurrent::Id(uuid) => uuid,
    };

    let can_delete_all = can_delete_all_resources(&current_user, Resource::ApiKeys);
    let can_delete_own = can_delete_own_resource(&current_user, Resource::ApiKeys, target_user_id);

    if !can_delete_all && !can_delete_own {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let member = is_org_member(&current_user, target_user_id, &mut conn)
            .await
            .map_err(Error::Database)?;
        if !member {
            return Err(Error::InsufficientPermissions {
                required: Permission::Any(vec![
                    Permission::Allow(Resource::ApiKeys, Operation::DeleteAll),
                    Permission::Allow(Resource::ApiKeys, Operation::DeleteOwn),
                ]),
                action: Operation::DeleteOwn,
                resource: format!("API keys for user {target_user_id}"),
            });
        }
    }

    let caps = {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        resolve_key_capabilities(&current_user, target_user_id, &mut conn)
            .await
            .map_err(Error::Database)?
    };
    if !can_delete_all && !caps.is_org_manager && !caps.can_self_manage_keys() {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::ApiKeys, Operation::DeleteOwn),
            action: Operation::DeleteOwn,
            resource: "API keys in a managed-keys organization (ask an org owner/admin)".to_string(),
        });
    }
    let skip_created_by_filter = can_delete_all || caps.is_org_manager;

    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = ApiKeys::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);

    repo.get_by_id(api_key_id)
        .await?
        .filter(|key| key.user_id == target_user_id)
        .filter(|key| skip_created_by_filter || key.created_by == current_user.id)
        .filter(|key| !key.hidden)
        .filter(|key| key.parent_api_key_id.is_none())
        .ok_or_else(|| Error::NotFound {
            resource: "API key".to_string(),
            id: api_key_id.to_string(),
        })?;

    repo.delete(api_key_id).await?;
    tx.commit().await.map_err(|e| Error::Database(e.into()))?;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use crate::api::models::api_keys::{ApiKeyInfoResponse, ApiKeyResponse, ApiKeySecretResponse};
    use crate::api::models::pagination::PaginatedResponse;
    use crate::api::models::users::Role;
    use crate::crypto::hash_api_key;
    use crate::keystore::test_keystore;
    use crate::test::utils::*;
    use serde_json::json;
    use sqlx::PgPool;

    async fn stored_pending_secret(pool: &PgPool, id: uuid::Uuid) -> Option<Vec<u8>> {
        sqlx::query_scalar!("SELECT pending_secret FROM api_keys WHERE id = $1", id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn patch_key(
        app: &axum_test::TestServer,
        user: &crate::api::models::users::UserResponse,
        key_id: crate::types::ApiKeyId,
        body: serde_json::Value,
    ) -> axum_test::TestResponse {
        let auth = add_auth_headers(user);
        app.patch(&format!("/admin/api/v1/users/current/api-keys/{key_id}"))
            .json(&body)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await
    }

    fn window_spend_of(resp: &ApiKeyInfoResponse) -> rust_decimal::Decimal {
        resp.spend.expect("capped key should carry a spend value")
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_api_key_with_spend_cap(pool: PgPool) {
        use crate::db::handlers::api_keys::ApiKeys;

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let auth = add_auth_headers(&user);

        let response = app
            .post("/admin/api/v1/users/current/api-keys")
            .json(&json!({
                "name": "Capped Key",
                "spend_limit": "50",
                "spend_limit_interval": "daily"
            }))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let created: ApiKeyResponse = response.json();
        assert_eq!(created.spend_limit, Some(rust_decimal::Decimal::from(50)));
        assert_eq!(created.spend_limit_interval.as_deref(), Some("daily"));
        assert!(created.resets_at.is_some(), "windowed cap advertises its next reset");
        assert_eq!(created.spend, Some(rust_decimal::Decimal::ZERO), "fresh cap starts a zeroed window");

        let mut conn = pool.acquire().await.unwrap();
        let child: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT id FROM api_keys WHERE parent_api_key_id = $1 AND purpose = 'batch' AND hidden = true")
                .bind(created.id)
                .fetch_optional(&pool)
                .await
                .unwrap();
        assert!(child.is_some(), "cap-set must mint the batch/flex child");
        let window_spend: rust_decimal::Decimal =
            sqlx::query_scalar("SELECT window_spend FROM api_key_spend_checkpoints WHERE api_key_id = $1")
                .bind(created.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(window_spend, rust_decimal::Decimal::ZERO);

        let resolved = ApiKeys::new(&mut conn)
            .resolve_batch_execution_key(user.id, user.id, Some(created.id))
            .await
            .unwrap();
        assert_eq!(Some(resolved), child);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_api_key_cap_validation(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let auth = add_auth_headers(&user);
        let post = |body: serde_json::Value| {
            let auth = add_auth_headers(&user);
            let app = &app;
            async move {
                app.post("/admin/api/v1/users/current/api-keys")
                    .json(&body)
                    .add_header(&auth[0].0, &auth[0].1)
                    .add_header(&auth[1].0, &auth[1].1)
                    .await
            }
        };

        post(json!({"name": "k1", "spend_limit": "0"}))
            .await
            .assert_status(axum::http::StatusCode::BAD_REQUEST);
        post(json!({"name": "k2", "spend_limit": "5", "spend_limit_interval": "hourly"}))
            .await
            .assert_status(axum::http::StatusCode::BAD_REQUEST);
        post(json!({"name": "k3", "spend_limit_interval": "daily"}))
            .await
            .assert_status(axum::http::StatusCode::BAD_REQUEST);

        let ok = app
            .post("/admin/api/v1/users/current/api-keys")
            .json(&json!({"name": "k4", "spend_limit": "5"}))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        ok.assert_status(axum::http::StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_patch_api_key_spend_cap_matrix(pool: PgPool) {
        use rust_decimal::Decimal;

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;

        let resp = patch_key(&app, &user, key.id, json!({"spend_limit": "25"})).await;
        resp.assert_status_ok();
        let body: ApiKeyInfoResponse = resp.json();
        assert_eq!(body.spend_limit, Some(Decimal::from(25)));
        assert_eq!(window_spend_of(&body), Decimal::ZERO);
        let child_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM api_keys WHERE parent_api_key_id = $1")
            .bind(key.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(child_count, 1, "setting a cap mints exactly one child");

        sqlx::query("UPDATE api_key_spend_checkpoints SET window_spend = 5, total_spend = 5 WHERE api_key_id = $1")
            .bind(key.id)
            .execute(&pool)
            .await
            .unwrap();

        let resp = patch_key(&app, &user, key.id, json!({"spend_limit": "40"})).await;
        resp.assert_status_ok();
        let body: ApiKeyInfoResponse = resp.json();
        assert_eq!(body.spend_limit, Some(Decimal::from(40)));
        assert_eq!(window_spend_of(&body), Decimal::from(5), "raising the limit keeps counted spend");

        let resp = patch_key(&app, &user, key.id, json!({"spend_limit_interval": "weekly"})).await;
        resp.assert_status_ok();
        let body: ApiKeyInfoResponse = resp.json();
        assert_eq!(body.spend_limit_interval.as_deref(), Some("weekly"));
        assert_eq!(window_spend_of(&body), Decimal::ZERO, "interval change resets the window");
        assert_eq!(body.total_spend, Some(Decimal::from(5)), "lifetime total survives resets");

        sqlx::query("UPDATE api_key_spend_checkpoints SET window_spend = 7 WHERE api_key_id = $1")
            .bind(key.id)
            .execute(&pool)
            .await
            .unwrap();
        let resp = patch_key(&app, &user, key.id, json!({"reset_window": true})).await;
        resp.assert_status_ok();
        let body: ApiKeyInfoResponse = resp.json();
        assert_eq!(window_spend_of(&body), Decimal::ZERO, "reset_window re-arms the cap");

        let resp = patch_key(&app, &user, key.id, json!({"spend_limit": null})).await;
        resp.assert_status_ok();
        let body: ApiKeyInfoResponse = resp.json();
        assert_eq!(body.spend_limit, None);
        assert_eq!(body.spend_limit_interval, None, "clearing the cap clears the interval");
        assert_eq!(body.spend, None, "uncapped keys never display (frozen) spend");
        assert_eq!(body.total_spend, None, "uncapped keys never display (frozen) totals");
        assert_eq!(body.resets_at, None);
        let child_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM api_keys WHERE parent_api_key_id = $1 AND is_deleted = false")
            .bind(key.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(child_count, 1, "cap removal must not revoke the child");

        sqlx::query("UPDATE api_key_spend_checkpoints SET window_spend = 99 WHERE api_key_id = $1")
            .bind(key.id)
            .execute(&pool)
            .await
            .unwrap();
        let resp = patch_key(&app, &user, key.id, json!({"spend_limit": "10"})).await;
        resp.assert_status_ok();
        let body: ApiKeyInfoResponse = resp.json();
        assert_eq!(window_spend_of(&body), Decimal::ZERO, "re-capping must not inherit prior spend");

        patch_key(&app, &user, key.id, json!({"spend_limit": null}))
            .await
            .assert_status_ok();
        patch_key(&app, &user, key.id, json!({"reset_window": true}))
            .await
            .assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_patch_api_key_permissions_and_system_keys(pool: PgPool) {
        use crate::db::handlers::api_keys::ApiKeys;
        use crate::db::models::api_keys::ApiKeyPurpose;

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let other = create_test_user(&pool, Role::StandardUser).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let key = create_test_api_key_for_user(&pool, owner.id).await;

        let auth = add_auth_headers(&other);
        app.patch(&format!("/admin/api/v1/users/{}/api-keys/{}", owner.id, key.id))
            .json(&json!({"spend_limit": "5"}))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await
            .assert_status(axum::http::StatusCode::FORBIDDEN);

        let auth = add_auth_headers(&admin);
        app.patch(&format!("/admin/api/v1/users/{}/api-keys/{}", owner.id, key.id))
            .json(&json!({"spend_limit": "5"}))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await
            .assert_status_forbidden();

        let mut conn = pool.acquire().await.unwrap();
        let mut repo = ApiKeys::new(&mut conn);
        let child_id = repo.get_or_create_child_hidden_key(key.id).await.unwrap();
        let shared_id = repo
            .get_or_create_hidden_key(owner.id, ApiKeyPurpose::Batch, owner.id)
            .await
            .unwrap();
        drop(conn);
        patch_key(&app, &owner, child_id, json!({"name": "nope"}))
            .await
            .assert_status(axum::http::StatusCode::NOT_FOUND);
        patch_key(&app, &owner, shared_id, json!({"name": "nope"}))
            .await
            .assert_status(axum::http::StatusCode::NOT_FOUND);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_capped_key_end_to_end_402(pool: PgPool) {
        use crate::db::handlers::{Credits, Tariffs};
        use crate::db::models::credits::{CreditTransactionCreateDBRequest, CreditTransactionType};
        use crate::db::models::tariffs::TariffCreateDBRequest;
        use onwards::auth::KeyHash;

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let endpoint_id = create_test_endpoint(&pool, "cap-e2e-endpoint", user.id).await;
        let deployment_id = create_test_model(&pool, "cap-e2e-model-name", "cap-e2e-model", endpoint_id, user.id).await;
        add_deployment_to_group(&pool, deployment_id, group.id, user.id).await;
        let mut conn = pool.acquire().await.unwrap();
        Tariffs::new(&mut conn)
            .create(&TariffCreateDBRequest {
                deployed_model_id: deployment_id,
                name: "cap-e2e-tariff".to_string(),
                api_key_purpose: Some(crate::db::models::api_keys::ApiKeyPurpose::Realtime),
                input_price_per_token: rust_decimal::Decimal::new(1, 5),
                output_price_per_token: rust_decimal::Decimal::new(3, 5),
                valid_from: None,
                completion_window: None,
                user_id: None,
            })
            .await
            .unwrap();
        sqlx::query("INSERT INTO model_tariffs (deployed_model_id,name,api_key_purpose,completion_window,input_price_per_token,output_price_per_token) VALUES ($1,'batch','batch','24h',0.00001,0.00003)")
            .bind(deployment_id).execute(&mut *conn).await.unwrap();
        Credits::new(&mut conn)
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::AdminGrant,
                amount: rust_decimal::Decimal::from(100),
                source_id: uuid::Uuid::new_v4().to_string(),
                description: Some("credits".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();
        drop(conn);

        let auth = add_auth_headers(&user);
        let created: ApiKeyResponse = app
            .post("/admin/api/v1/users/current/api-keys")
            .json(&json!({"name": "e2e-capped", "spend_limit": "0.01"}))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await
            .json();
        let child_id: uuid::Uuid = sqlx::query_scalar("SELECT id FROM api_keys WHERE parent_api_key_id = $1")
            .bind(created.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let child_secret = crate::test::utils::bearer_for_key(&pool, child_id).await;

        let targets = crate::sync::onwards_config::load_targets_from_db(&pool, &[], false).await.unwrap();
        let has_key = |targets: &onwards::target::Targets, secret: &str| {
            let expected = KeyHash::from(secret.to_string());
            targets.targets.get("cap-e2e-model").is_some_and(|p| {
                p.value()
                    .default_pool()
                    .keys()
                    .is_some_and(|keys| keys.iter().any(|c| c == &expected))
            })
        };
        assert!(has_key(&targets, &created.key));
        assert!(has_key(&targets, &child_secret));

        sqlx::query("UPDATE api_key_spend_checkpoints SET window_spend = 0.02, total_spend = 0.02 WHERE api_key_id = $1")
            .bind(created.id)
            .execute(&pool)
            .await
            .unwrap();
        let targets = crate::sync::onwards_config::load_targets_from_db(&pool, &[], false).await.unwrap();
        assert!(!has_key(&targets, &created.key), "exhausted root must leave the paid pool");
        assert!(!has_key(&targets, &child_secret), "the child is yanked with its root");

        let router = axum::Router::new()
            .route(
                "/ai/v1/chat/completions",
                axum::routing::post(|| async {
                    axum::response::Response::builder()
                        .status(axum::http::StatusCode::FORBIDDEN)
                        .body(axum::body::Body::from("Forbidden"))
                        .unwrap()
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                sqlx_pool_router::DynPools::new(pool.clone()),
                crate::error_enrichment::error_enrichment_middleware,
            ));
        let proxy = axum_test::TestServer::new(router).unwrap();
        let response = proxy
            .post("/ai/v1/chat/completions")
            .add_header("authorization", &format!("Bearer {}", created.key))
            .json(&json!({"model": "cap-e2e-model", "messages": [{"role": "user", "content": "hi"}]}))
            .await;
        response.assert_status(axum::http::StatusCode::PAYMENT_REQUIRED);
        let body = response.text();
        assert!(body.contains("spend_cap_exceeded"), "expected explicit cap code, got: {body}");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_api_key_for_self(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let api_key_data = json!({
            "name": "Test API Key",
            "description": "A test API key",
            "purpose": "realtime"
        });

        let response = app
            .post("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .json(&api_key_data)
            .await;

        response.assert_status(axum::http::StatusCode::CREATED);
        let api_key: ApiKeyResponse = response.json();
        assert_eq!(api_key.name, "Test API Key");
        assert_eq!(api_key.description, Some("A test API key".to_string()));
        assert!(api_key.key.starts_with("sk-"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_api_key_for_other_user_as_admin(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let regular_user = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, regular_user.id, group.id).await;

        let api_key_data = json!({
            "name": "Admin Created Key",
            "description": "Created by admin for user",
            "purpose": "realtime"
        });

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", regular_user.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&api_key_data)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_api_key_for_other_user_as_non_admin_forbidden(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        let api_key_data = json!({
            "name": "Forbidden Key",
            "description": "This should not work",
            "purpose": "realtime"
        });

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", user2.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .json(&api_key_data)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_user_api_keys(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let api_key = create_test_api_key_for_user(&pool, user.id).await;

        let response = app
            .get("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(paginated.data.len(), 1);
        assert_eq!(paginated.total_count, 1);
        assert_eq!(paginated.data[0].name, api_key.name);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_user_api_keys_with_pagination_query_params(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        for i in 1..=5 {
            let api_key_data = json!({
                "name": format!("Test API Key {}", i),
                "description": format!("Description for key {}", i),
                "purpose": "realtime"
            });

            app.post("/admin/api/v1/users/current/api-keys")
                .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
                .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
                .json(&api_key_data)
                .await
                .assert_status(axum::http::StatusCode::CREATED);
        }

        let response = app
            .get("/admin/api/v1/users/current/api-keys?skip=1&limit=2")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(paginated.data.len(), 2, "Should return exactly 2 items with limit=2");
        assert_eq!(paginated.total_count, 5, "Total count should be 5");
        assert_eq!(paginated.skip, 1);
        assert_eq!(paginated.limit, 2);

        let response = app
            .get("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(paginated.data.len(), 5, "Should return all items with default pagination");
        assert_eq!(paginated.total_count, 5);

        let response = app
            .get("/admin/api/v1/users/current/api-keys?limit=9999")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(paginated.data.len(), 5, "Should return all items even with large limit");
        assert_eq!(paginated.total_count, 5);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_user_api_key_for_self(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let api_key = create_test_api_key_for_user(&pool, user.id).await;

        let response = app
            .delete(&format!("/admin/api/v1/users/current/api-keys/{}", api_key.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::NO_CONTENT);

        let list_response = app
            .get("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        list_response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = list_response.json();
        assert_eq!(paginated.data.len(), 0);
        assert_eq!(paginated.total_count, 0);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_rejects_cap_scope_child_key(pool: PgPool) {
        use crate::db::handlers::api_keys::ApiKeys;
        use crate::db::handlers::repository::Repository;

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let api_key = create_test_api_key_for_user(&pool, user.id).await;

        let child_id = {
            let mut conn = pool.acquire().await.unwrap();
            ApiKeys::new(&mut conn).get_or_create_child_hidden_key(api_key.id).await.unwrap()
        };

        let response = app
            .delete(&format!("/admin/api/v1/users/current/api-keys/{child_id}"))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::NOT_FOUND);

        let mut conn = pool.acquire().await.unwrap();
        assert!(ApiKeys::new(&mut conn).get_by_id(child_id).await.unwrap().is_some());

        let response = app
            .delete(&format!("/admin/api/v1/users/current/api-keys/{}", api_key.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::NO_CONTENT);
        assert!(ApiKeys::new(&mut conn).get_by_id(child_id).await.unwrap().is_none());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_user_api_key_for_other_user_as_admin(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let regular_user = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, regular_user.id, group.id).await;
        let api_key = create_test_api_key_for_user(&pool, regular_user.id).await;

        let response = app
            .delete(&format!("/admin/api/v1/users/{}/api-keys/{}", regular_user.id, api_key.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_forbidden();

        let list_response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys", regular_user.id))
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .await;

        list_response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = list_response.json();
        assert_eq!(paginated.data.len(), 1);
        assert_eq!(paginated.total_count, 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_user_api_key_for_other_user_as_non_admin_forbidden(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user2.id, group.id).await;
        let api_key = create_test_api_key_for_user(&pool, user2.id).await;

        let response = app
            .delete(&format!("/admin/api/v1/users/{}/api-keys/{}", user2.id, api_key.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_forbidden();

        let list_response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys", user2.id))
            .add_header(&add_auth_headers(&user2)[0].0, &add_auth_headers(&user2)[0].1)
            .add_header(&add_auth_headers(&user2)[1].0, &add_auth_headers(&user2)[1].1)
            .await;

        list_response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = list_response.json();
        assert_eq!(paginated.data.len(), 1);
        assert_eq!(paginated.total_count, 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_nonexistent_api_key_returns_not_found(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let fake_api_key_id = uuid::Uuid::new_v4();

        let response = app
            .delete(&format!("/admin/api/v1/users/current/api-keys/{fake_api_key_id}"))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_not_found();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_api_key_belonging_to_different_user_returns_not_found(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user1.id, group.id).await;
        add_user_to_group(&pool, user2.id, group.id).await;

        let api_key = create_test_api_key_for_user(&pool, user2.id).await;

        let response = app
            .delete(&format!("/admin/api/v1/users/current/api-keys/{}", api_key.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_not_found();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_api_keys_for_other_user_as_admin(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let regular_user = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, regular_user.id, group.id).await;

        let _api_key1 = create_test_api_key_for_user(&pool, regular_user.id).await;
        let _api_key2 = create_test_api_key_for_user(&pool, regular_user.id).await;

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys", regular_user.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_api_keys_for_other_user_as_non_admin_forbidden(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user2.id, group.id).await;
        create_test_api_key_for_user(&pool, user2.id).await;

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys", user2.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_api_key_for_other_user_as_admin(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let regular_user = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, regular_user.id, group.id).await;
        let api_key = create_test_api_key_for_user(&pool, regular_user.id).await;

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys/{}", regular_user.id, api_key.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_api_key_for_other_user_as_non_admin_forbidden(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user2.id, group.id).await;
        let api_key = create_test_api_key_for_user(&pool, user2.id).await;

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys/{}", user2.id, api_key.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_request_viewer_api_key_permissions(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let request_viewer = create_test_user(&pool, Role::RequestViewer).await;
        let other_user = create_test_user(&pool, Role::StandardUser).await;

        let api_key_data = json!({
            "name": "RequestViewer Key",
            "description": "Should work - StandardUser can manage own keys",
            "purpose": "realtime"
        });

        let response = app
            .post("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .json(&api_key_data)
            .await;

        response.assert_status(axum::http::StatusCode::CREATED);

        let response = app
            .get("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;

        response.assert_status_ok();

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys", other_user.id))
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_multi_role_user_api_key_permissions(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;

        let multi_role_user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::RequestViewer]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, multi_role_user.id, group.id).await;

        let api_key_data = json!({
            "name": "Multi Role Key",
            "description": "Should work due to StandardUser role",
            "purpose": "realtime"
        });

        let response = app
            .post("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&multi_role_user)[0].0, &add_auth_headers(&multi_role_user)[0].1)
            .add_header(&add_auth_headers(&multi_role_user)[1].0, &add_auth_headers(&multi_role_user)[1].1)
            .json(&api_key_data)
            .await;

        response.assert_status(axum::http::StatusCode::CREATED);

        let response = app
            .get("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&multi_role_user)[0].0, &add_auth_headers(&multi_role_user)[0].1)
            .add_header(&add_auth_headers(&multi_role_user)[1].0, &add_auth_headers(&multi_role_user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(paginated.data.len(), 1);
        assert_eq!(paginated.total_count, 1);
        assert_eq!(paginated.data[0].name, "Multi Role Key");

        let api_key_id = paginated.data[0].id;
        let response = app
            .get(&format!("/admin/api/v1/users/current/api-keys/{api_key_id}"))
            .add_header(&add_auth_headers(&multi_role_user)[0].0, &add_auth_headers(&multi_role_user)[0].1)
            .add_header(&add_auth_headers(&multi_role_user)[1].0, &add_auth_headers(&multi_role_user)[1].1)
            .await;

        response.assert_status_ok();

        let response = app
            .delete(&format!("/admin/api/v1/users/current/api-keys/{api_key_id}"))
            .add_header(&add_auth_headers(&multi_role_user)[0].0, &add_auth_headers(&multi_role_user)[0].1)
            .add_header(&add_auth_headers(&multi_role_user)[1].0, &add_auth_headers(&multi_role_user)[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::NO_CONTENT);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_cannot_access_customer_api_keys(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_user(&pool, Role::PlatformManager).await;
        let standard_user = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, standard_user.id, group.id).await;

        let api_key_data = json!({
            "name": "Manager Created Key",
            "description": "Created by platform manager",
            "purpose": "realtime"
        });

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", standard_user.id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .json(&api_key_data)
            .await;

        response.assert_status_forbidden();
        let api_key = create_test_api_key_for_user(&pool, standard_user.id).await;

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys", standard_user.id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;

        response.assert_status_forbidden();

        let api_key_id = api_key.id;
        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys/{}", standard_user.id, api_key_id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;

        response.assert_status_forbidden();

        let response = app
            .get("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(paginated.data.len(), 1);
        assert_eq!(paginated.data[0].id, api_key.id);

        let response = app
            .delete(&format!("/admin/api/v1/users/{}/api-keys/{}", standard_user.id, api_key_id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_api_key_isolation_between_users(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user1.id, group.id).await;
        add_user_to_group(&pool, user2.id, group.id).await;

        let api_key1 = create_test_api_key_for_user(&pool, user1.id).await;
        let api_key2 = create_test_api_key_for_user(&pool, user2.id).await;

        let response = app
            .get("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_ok();
        let user1_paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(user1_paginated.data.len(), 1);
        assert_eq!(user1_paginated.total_count, 1);
        assert_eq!(user1_paginated.data[0].id, api_key1.id);

        let response = app
            .get("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&user2)[0].0, &add_auth_headers(&user2)[0].1)
            .add_header(&add_auth_headers(&user2)[1].0, &add_auth_headers(&user2)[1].1)
            .await;

        response.assert_status_ok();
        let user2_paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(user2_paginated.data.len(), 1);
        assert_eq!(user2_paginated.total_count, 1);
        assert_eq!(user2_paginated.data[0].id, api_key2.id);

        let response = app
            .get(&format!("/admin/api/v1/users/current/api-keys/{}", api_key2.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_not_found();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_error_messages_are_user_friendly(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys", user2.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_forbidden();
        let body = response.text();

        assert!(body.contains("Insufficient permissions to Read"));
        assert!(!body.contains("ReadAll"));
        assert!(!body.contains("ReadOwn"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_specific_api_key_for_self(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let api_key = create_test_api_key_for_user(&pool, user.id).await;

        let response = app
            .get(&format!("/admin/api/v1/users/current/api-keys/{}", api_key.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let returned_key: ApiKeyInfoResponse = response.json();
        assert_eq!(returned_key.id, api_key.id);
        assert_eq!(returned_key.name, api_key.name);
        assert_eq!(returned_key.description, api_key.description);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_api_key_creation_returns_key_value_only_once(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let api_key_data = json!({
            "name": "Test Key for Security",
            "description": "Testing key exposure",
            "purpose": "realtime"
        });

        let create_response = app
            .post("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .json(&api_key_data)
            .await;

        create_response.assert_status(axum::http::StatusCode::CREATED);
        let created_key: ApiKeyResponse = create_response.json();

        assert!(created_key.key.starts_with("sk-"));
        assert!(created_key.key.len() > 10);

        let list_response = app
            .get("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        list_response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = list_response.json();
        assert_eq!(paginated.data.len(), 1);
        assert_eq!(paginated.total_count, 1);


        let get_response = app
            .get(&format!("/admin/api/v1/users/current/api-keys/{}", created_key.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        get_response.assert_status_ok();
        let retrieved_key: ApiKeyInfoResponse = get_response.json();

        assert_eq!(retrieved_key.id, created_key.id);
        assert_eq!(retrieved_key.name, created_key.name);
    }


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_org_member_can_create_api_key(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;

        let api_key_data = json!({
            "name": "Bob Org Key",
            "description": "Created by member",
            "purpose": "realtime"
        });

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .json(&api_key_data)
            .await;

        response.assert_status(axum::http::StatusCode::CREATED);
        let api_key: ApiKeyResponse = response.json();
        assert_eq!(api_key.name, "Bob Org Key");
        assert!(api_key.key.starts_with("sk-"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_non_member_cannot_create_org_api_key(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let outsider = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;

        let api_key_data = json!({
            "name": "Outsider Key",
            "purpose": "realtime"
        });

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&outsider)[0].0, &add_auth_headers(&outsider)[0].1)
            .add_header(&add_auth_headers(&outsider)[1].0, &add_auth_headers(&outsider)[1].1)
            .json(&api_key_data)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_org_member_list_only_own_keys(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .json(&json!({"name": "Alice Key", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .json(&json!({"name": "Bob Key", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .await;
        response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(paginated.data.len(), 2);

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .await;
        response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(paginated.data.len(), 1);
        assert_eq!(paginated.data[0].name, "Bob Key");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_org_created_by_filter_server_side(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let carol = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;
        add_org_member(&pool, org.id, carol.id, "member").await;

        for (name, member) in [("Bob 1", bob.id), ("Bob 2", bob.id), ("Carol 1", carol.id)] {
            let response = app
                .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
                .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
                .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
                .json(&json!({"name": name, "purpose": "realtime", "member_id": member}))
                .await;
            response.assert_status(axum::http::StatusCode::CREATED);
        }
        let response = app
            .post("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .json(&json!({"name": "Bob personal", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys?created_by={}", org.id, bob.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .await;
        response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(paginated.total_count, 2);
        assert_eq!(paginated.data.len(), 2);
        assert!(paginated.data.iter().all(|k| k.created_by == bob.id));
        assert!(paginated.data.iter().all(|k| k.name.starts_with("Bob ")));

        let response = app
            .get(&format!(
                "/admin/api/v1/users/{}/api-keys?created_by={}&limit=1&skip=1",
                org.id, bob.id
            ))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .await;
        response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(paginated.total_count, 2);
        assert_eq!(paginated.data.len(), 1);
        assert_eq!(paginated.data[0].created_by, bob.id);

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys?created_by={}", org.id, carol.id))
            .add_header(&add_auth_headers(&carol)[0].0, &add_auth_headers(&carol)[0].1)
            .add_header(&add_auth_headers(&carol)[1].0, &add_auth_headers(&carol)[1].1)
            .await;
        response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(paginated.data.len(), 1);
        assert_eq!(paginated.data[0].name, "Carol 1");

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys?created_by={}", org.id, bob.id))
            .add_header(&add_auth_headers(&carol)[0].0, &add_auth_headers(&carol)[0].1)
            .add_header(&add_auth_headers(&carol)[1].0, &add_auth_headers(&carol)[1].1)
            .await;
        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_issued_key_first_reveal_flow(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let keystore = test_keystore("test-1", TEST_WRAP_KEYS);
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .json(&json!({"name": "Issued", "purpose": "realtime", "member_id": bob.id}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let issued: ApiKeyResponse = response.json();
        assert!(issued.key.starts_with("sk-"), "issuer still gets the secret at creation");
        assert!(issued.secret_revealed_at.is_none(), "issued key must be born unrevealed");
        let wrapped = stored_pending_secret(&pool, issued.id)
            .await
            .expect("issued key holds a wrapped secret");
        assert!(!wrapped.windows(issued.key.len()).any(|w| w == issued.key.as_bytes()));
        assert_eq!(keystore.unwrap(&wrapped).unwrap(), issued.key.as_bytes());

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .await;
        response.assert_status_ok();
        let listed: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert!(listed.data[0].secret_revealed_at.is_none());

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys/{}/reveal", org.id, issued.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .await;
        response.assert_status_not_found();

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys/{}/rotate", org.id, issued.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .await;
        response.assert_status_ok();
        let rotated: ApiKeySecretResponse = response.json();
        let rewrapped = stored_pending_secret(&pool, issued.id)
            .await
            .expect("rotation keeps a wrapped secret");
        assert_eq!(keystore.unwrap(&rewrapped).unwrap(), rotated.key.as_bytes());

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys/{}/reveal", org.id, issued.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .await;
        response.assert_status_ok();
        let revealed: ApiKeySecretResponse = response.json();
        assert_eq!(revealed.key, rotated.key);
        assert_eq!(stored_pending_secret(&pool, issued.id).await, None, "the reveal clears the wrap");

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .await;
        let listed: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert!(listed.data[0].secret_revealed_at.is_some());
        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys/{}/reveal", org.id, issued.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::CONFLICT);

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .json(&json!({"name": "Self", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let self_key: ApiKeyResponse = response.json();
        assert!(self_key.secret_revealed_at.is_some());
        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys/{}/reveal", org.id, self_key.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::CONFLICT);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_issuing_key_without_keystore_is_refused(pool: PgPool) {
        let mut config = create_test_config();
        config.keystore = None;
        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .json(&json!({"name": "Issued", "purpose": "realtime", "member_id": bob.id}))
            .await;
        response.assert_status(axum::http::StatusCode::NOT_IMPLEMENTED);
        assert!(response.maybe_header("retry-after").is_none(), "retrying cannot help");
        assert!(response.text().contains("keystore"), "error says why: {}", response.text());
        let keys: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) AS "count!" FROM api_keys WHERE user_id IN ($1, $2) AND hidden = false"#,
            org.id,
            bob.id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(keys, 0);

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .json(&json!({"name": "Self", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let self_key: ApiKeyResponse = response.json();
        assert_eq!(stored_pending_secret(&pool, self_key.id).await, None);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_reveal_of_key_pending_without_a_wrap_fails_and_stays_available(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let response = app
            .post("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .json(&json!({"name": "Legacy", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let key: ApiKeyResponse = response.json();
        sqlx::query!("UPDATE api_keys SET secret_revealed_at = NULL WHERE id = $1", key.id)
            .execute(&pool)
            .await
            .unwrap();
        let stored_hash: Vec<u8> = sqlx::query_scalar!("SELECT secret_hash FROM api_keys WHERE id = $1", key.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(stored_hash, hash_api_key(&key.key));

        let response = app
            .post(&format!("/admin/api/v1/users/current/api-keys/{}/reveal", key.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::CONFLICT);
        assert!(response.text().contains("Rotate the key"), "{}", response.text());
        assert!(!response.text().contains(&key.key));
        let revealed_at: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar!("SELECT secret_revealed_at FROM api_keys WHERE id = $1", key.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(revealed_at, None, "a failed reveal stays available");

        let response = app
            .post(&format!("/admin/api/v1/users/current/api-keys/{}/rotate", key.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .await;
        response.assert_status_ok();
        let rotated: ApiKeySecretResponse = response.json();
        assert_ne!(rotated.key, key.key);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_rotating_a_pending_key_without_keystore_spends_the_reveal(pool: PgPool) {
        let mut config = create_test_config();
        config.keystore = None;
        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let auth = add_auth_headers(&bob);
        let response = app
            .post("/admin/api/v1/users/current/api-keys")
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .json(&json!({"name": "Pending", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let key: ApiKeyResponse = response.json();
        sqlx::query!("UPDATE api_keys SET secret_revealed_at = NULL WHERE id = $1", key.id)
            .execute(&pool)
            .await
            .unwrap();

        let response = app
            .post(&format!("/admin/api/v1/users/current/api-keys/{}/rotate", key.id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        response.assert_status_ok();
        let row = sqlx::query!("SELECT secret_revealed_at, pending_secret FROM api_keys WHERE id = $1", key.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(row.secret_revealed_at.is_some(), "the rotation spends the reveal");
        assert_eq!(row.pending_secret, None);

        let response = app
            .post(&format!("/admin/api/v1/users/current/api-keys/{}/reveal", key.id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::CONFLICT);
        assert!(response.text().contains("already been used"), "{}", response.text());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_org_key_get_scoping_manager_sees_member_key(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let carol = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;
        add_org_member(&pool, org.id, carol.id, "member").await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .json(&json!({"name": "Bob Key", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let bob_key: ApiKeyResponse = response.json();

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys/{}", org.id, bob_key.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .await;
        response.assert_status_ok();

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys/{}", org.id, bob_key.id))
            .add_header(&add_auth_headers(&carol)[0].0, &add_auth_headers(&carol)[0].1)
            .add_header(&add_auth_headers(&carol)[1].0, &add_auth_headers(&carol)[1].1)
            .await;
        response.assert_status_not_found();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_org_key_delete_scoping(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let carol = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;
        add_org_member(&pool, org.id, carol.id, "member").await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .json(&json!({"name": "Bob Key", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let bob_key: ApiKeyResponse = response.json();

        let response = app
            .delete(&format!("/admin/api/v1/users/{}/api-keys/{}", org.id, bob_key.id))
            .add_header(&add_auth_headers(&carol)[0].0, &add_auth_headers(&carol)[0].1)
            .add_header(&add_auth_headers(&carol)[1].0, &add_auth_headers(&carol)[1].1)
            .await;
        response.assert_status_not_found();

        let response = app
            .delete(&format!("/admin/api/v1/users/{}/api-keys/{}", org.id, bob_key.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::NO_CONTENT);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_org_member_can_delete_own_key(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .json(&json!({"name": "Bob Key", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let bob_key: ApiKeyResponse = response.json();

        let response = app
            .delete(&format!("/admin/api/v1/users/{}/api-keys/{}", org.id, bob_key.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::NO_CONTENT);

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .await;
        response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(paginated.data.len(), 0);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_cannot_see_unrelated_org_keys(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;

        app.post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .json(&json!({"name": "Alice Key", "purpose": "realtime"}))
            .await
            .assert_status(axum::http::StatusCode::CREATED);

        app.post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .json(&json!({"name": "Bob Key", "purpose": "realtime"}))
            .await
            .assert_status(axum::http::StatusCode::CREATED);

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&pm)[0].0, &add_auth_headers(&pm)[0].1)
            .add_header(&add_auth_headers(&pm)[1].0, &add_auth_headers(&pm)[1].1)
            .await;
        response.assert_status_forbidden();
    }


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_org_member_key_created_by_is_member_not_org(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .json(&json!({"name": "Alice Org Key", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let key: ApiKeyResponse = response.json();

        assert_eq!(key.created_by, alice.id, "created_by should be the member, not the org");
        assert_eq!(key.user_id, org.id, "user_id should be the org");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_pm_org_member_key_created_by_is_pm_not_org(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let org = create_test_org(&pool, pm.id).await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&pm)[0].0, &add_auth_headers(&pm)[0].1)
            .add_header(&add_auth_headers(&pm)[1].0, &add_auth_headers(&pm)[1].1)
            .json(&json!({"name": "PM Org Key", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let key: ApiKeyResponse = response.json();

        assert_eq!(key.created_by, pm.id, "created_by should be the PM, not the org");
        assert_eq!(key.user_id, org.id, "user_id should be the org");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_system_key_issued_for_another_user_belongs_to_and_authenticates_as_them(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let keystore = test_keystore("test-1", TEST_WRAP_KEYS);
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let system_secret = crate::crypto::generate_api_key();
        sqlx::query("UPDATE api_keys SET secret_hash = $1, secret_prefix = $2, purpose = 'platform' WHERE id = $3")
            .bind(&crate::crypto::hash_api_key(&system_secret)[..])
            .bind(crate::crypto::api_key_prefix(&system_secret))
            .bind(uuid::Uuid::nil())
            .execute(&pool)
            .await
            .unwrap();

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", alice.id))
            .add_header("Authorization", format!("Bearer {system_secret}"))
            .json(&json!({"name": "On behalf", "purpose": "platform"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let key: ApiKeyResponse = response.json();

        assert_eq!(key.created_by, alice.id);
        assert_eq!(key.user_id, alice.id);
        assert!(key.secret_revealed_at.is_none());
        let wrapped = stored_pending_secret(&pool, key.id).await.expect("held wrapped");
        assert_eq!(keystore.unwrap(&wrapped).unwrap(), key.key.as_bytes());

        let response = app
            .get("/admin/api/v1/users/current")
            .add_header("Authorization", format!("Bearer {}", key.key))
            .await;
        response.assert_status_ok();
        assert_eq!(response.json::<serde_json::Value>()["id"], alice.id.to_string());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_pm_creates_org_key_with_member_id(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&pm)[0].0, &add_auth_headers(&pm)[0].1)
            .add_header(&add_auth_headers(&pm)[1].0, &add_auth_headers(&pm)[1].1)
            .json(&json!({"name": "Attributed Key", "purpose": "realtime", "member_id": alice.id}))
            .await;
        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_pm_member_id_rejected_for_non_org_target(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, alice.id, group.id).await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", alice.id))
            .add_header(&add_auth_headers(&pm)[0].0, &add_auth_headers(&pm)[0].1)
            .add_header(&add_auth_headers(&pm)[1].0, &add_auth_headers(&pm)[1].1)
            .json(&json!({"name": "Bad Key", "purpose": "realtime", "member_id": pm.id}))
            .await;
        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_pm_member_id_rejected_for_non_member(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let outsider = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&pm)[0].0, &add_auth_headers(&pm)[0].1)
            .add_header(&add_auth_headers(&pm)[1].0, &add_auth_headers(&pm)[1].1)
            .json(&json!({"name": "Bad Key", "purpose": "realtime", "member_id": outsider.id}))
            .await;
        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_plain_member_cannot_use_member_id(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .json(&json!({"name": "Bad Key", "purpose": "realtime", "member_id": alice.id}))
            .await;
        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_pm_creates_key_for_individual_user_created_by_is_target(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, alice.id, group.id).await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", alice.id))
            .add_header(&add_auth_headers(&pm)[0].0, &add_auth_headers(&pm)[0].1)
            .add_header(&add_auth_headers(&pm)[1].0, &add_auth_headers(&pm)[1].1)
            .json(&json!({"name": "PM For Alice", "purpose": "realtime"}))
            .await;
        response.assert_status_forbidden();
    }


    async fn revoke_manage_keys(pool: &PgPool, org_id: crate::types::UserId, user_id: crate::types::UserId) {
        sqlx::query(
            "DELETE FROM organization_member_roles omr USING user_organizations uo \
             WHERE uo.id = omr.user_organization_id AND uo.organization_id = $1 AND uo.user_id = $2 AND omr.role = 'manage_keys'",
        )
        .bind(org_id)
        .bind(user_id)
        .execute(pool)
        .await
        .unwrap();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_org_manager_creates_key_with_member_id(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .json(&json!({"name": "Issued to Bob", "purpose": "realtime", "member_id": bob.id}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let key: ApiKeyResponse = response.json();
        assert_eq!(key.created_by, bob.id, "issued key is attributed to the member");
        assert_eq!(key.user_id, org.id);

        let response = app
            .get(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .await;
        response.assert_status_ok();
        let paginated: PaginatedResponse<ApiKeyInfoResponse> = response.json();
        assert_eq!(paginated.data.len(), 1);
        assert_eq!(paginated.data[0].name, "Issued to Bob");

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys/{}/rotate", org.id, key.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .await;
        response.assert_status_ok();
        let secret: ApiKeySecretResponse = response.json();
        assert_ne!(secret.key, key.key);
        assert!(secret.key.starts_with("sk-"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_org_manager_can_update_member_key(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .json(&json!({"name": "Bob Key", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let bob_key: ApiKeyResponse = response.json();

        let response = app
            .patch(&format!("/admin/api/v1/users/{}/api-keys/{}", org.id, bob_key.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .json(&json!({"spend_limit": "25", "spend_limit_interval": "monthly"}))
            .await;
        response.assert_status_ok();
        let updated: ApiKeyInfoResponse = response.json();
        assert_eq!(updated.spend_limit, Some(rust_decimal::Decimal::from(25)));
        assert_eq!(updated.created_by, bob.id, "managing a key never re-attributes it");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_member_without_manage_keys_cannot_create_keys(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;
        revoke_manage_keys(&pool, org.id, bob.id).await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .json(&json!({"name": "Bob Key", "purpose": "realtime"}))
            .await;
        response.assert_status_forbidden();

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .json(&json!({"name": "Alice Key", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);

        let response = app
            .post("/admin/api/v1/users/current/api-keys")
            .add_header(&add_auth_headers(&bob)[0].0, &add_auth_headers(&bob)[0].1)
            .add_header(&add_auth_headers(&bob)[1].0, &add_auth_headers(&bob)[1].1)
            .json(&json!({"name": "Bob Personal", "purpose": "realtime"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_issued_keys_are_view_and_rotate_only_without_grant(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;
        revoke_manage_keys(&pool, org.id, bob.id).await;

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .json(&json!({"name": "Issued", "purpose": "realtime", "member_id": bob.id, "spend_limit": "10", "spend_limit_interval": "weekly"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let key: ApiKeyResponse = response.json();

        let bob_hdrs = add_auth_headers(&bob);

        app.get(&format!("/admin/api/v1/users/{}/api-keys/{}", org.id, key.id))
            .add_header(&bob_hdrs[0].0, &bob_hdrs[0].1)
            .add_header(&bob_hdrs[1].0, &bob_hdrs[1].1)
            .await
            .assert_status_ok();
        app.post(&format!("/admin/api/v1/users/{}/api-keys/{}/rotate", org.id, key.id))
            .add_header(&bob_hdrs[0].0, &bob_hdrs[0].1)
            .add_header(&bob_hdrs[1].0, &bob_hdrs[1].1)
            .await
            .assert_status_ok();

        app.patch(&format!("/admin/api/v1/users/{}/api-keys/{}", org.id, key.id))
            .add_header(&bob_hdrs[0].0, &bob_hdrs[0].1)
            .add_header(&bob_hdrs[1].0, &bob_hdrs[1].1)
            .json(&json!({"name": "Renamed"}))
            .await
            .assert_status_forbidden();
        app.patch(&format!("/admin/api/v1/users/{}/api-keys/{}", org.id, key.id))
            .add_header(&bob_hdrs[0].0, &bob_hdrs[0].1)
            .add_header(&bob_hdrs[1].0, &bob_hdrs[1].1)
            .json(&json!({"reset_window": true}))
            .await
            .assert_status_forbidden();
        app.delete(&format!("/admin/api/v1/users/{}/api-keys/{}", org.id, key.id))
            .add_header(&bob_hdrs[0].0, &bob_hdrs[0].1)
            .add_header(&bob_hdrs[1].0, &bob_hdrs[1].1)
            .await
            .assert_status_forbidden();

        app.patch(&format!("/admin/api/v1/users/{}/api-keys/{}", org.id, key.id))
            .add_header(&add_auth_headers(&alice)[0].0, &add_auth_headers(&alice)[0].1)
            .add_header(&add_auth_headers(&alice)[1].0, &add_auth_headers(&alice)[1].1)
            .json(&json!({"name": "Renamed by manager"}))
            .await
            .assert_status_ok();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_rotate_api_key(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let other = create_test_user(&pool, Role::StandardUser).await;
        let auth = add_auth_headers(&user);

        let response = app
            .post("/admin/api/v1/users/current/api-keys")
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .json(&json!({"name": "Rotatable", "spend_limit": "50", "spend_limit_interval": "daily"}))
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
        let key: ApiKeyResponse = response.json();

        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys/{}/rotate", user.id, key.id))
            .add_header(&add_auth_headers(&other)[0].0, &add_auth_headers(&other)[0].1)
            .add_header(&add_auth_headers(&other)[1].0, &add_auth_headers(&other)[1].1)
            .await;
        response.assert_status_forbidden();

        let response = app
            .post(&format!("/admin/api/v1/users/current/api-keys/{}/rotate", key.id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        response.assert_status_ok();
        let rotated: ApiKeySecretResponse = response.json();
        assert_ne!(rotated.key, key.key);
        assert!(rotated.key.starts_with("sk-"));

        let stored: Vec<u8> = sqlx::query_scalar("SELECT secret_hash FROM api_keys WHERE id = $1")
            .bind(key.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(stored, crate::crypto::hash_api_key(&rotated.key));

        let response = app
            .get(&format!("/admin/api/v1/users/current/api-keys/{}", key.id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        response.assert_status_ok();
        let info: ApiKeyInfoResponse = response.json();
        assert_eq!(info.id, key.id);
        assert_eq!(info.spend_limit, Some(rust_decimal::Decimal::from(50)));
        assert_eq!(info.created_by, user.id);

        let child_id: uuid::Uuid = sqlx::query_scalar("SELECT id FROM api_keys WHERE parent_api_key_id = $1")
            .bind(key.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let response = app
            .post(&format!("/admin/api/v1/users/current/api-keys/{}/rotate", child_id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        response.assert_status_not_found();

        let hidden_root_id = {
            use crate::db::handlers::api_keys::ApiKeys;
            use crate::db::models::api_keys::ApiKeyPurpose;
            let mut conn = pool.acquire().await.unwrap();
            ApiKeys::new(&mut conn)
                .get_or_create_hidden_key(user.id, ApiKeyPurpose::Batch, user.id)
                .await
                .unwrap()
        };
        let response = app
            .post(&format!("/admin/api/v1/users/current/api-keys/{}/rotate", hidden_root_id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        response.assert_status_not_found();

        for system_key in [child_id, hidden_root_id] {
            let response = app
                .get(&format!("/admin/api/v1/users/current/api-keys/{}", system_key))
                .add_header(&auth[0].0, &auth[0].1)
                .add_header(&auth[1].0, &auth[1].1)
                .await;
            response.assert_status_not_found();
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_reset_window_fires_onwards_rearm_notify(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;

        patch_key(&app, &user, key.id, json!({"spend_limit": "10"}))
            .await
            .assert_status_ok();

        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await.unwrap();
        listener.listen(crate::config::ONWARDS_CONFIG_CHANGED_CHANNEL).await.unwrap();
        while tokio::time::timeout(std::time::Duration::from_millis(10), listener.try_recv())
            .await
            .is_ok()
        {}

        patch_key(&app, &user, key.id, json!({"reset_window": true}))
            .await
            .assert_status_ok();

        let mut saw_rearm = false;
        while let Ok(Ok(Some(notification))) = tokio::time::timeout(std::time::Duration::from_secs(5), listener.try_recv()).await {
            if notification.payload().starts_with("api_key_spend_cap_rearm:") {
                saw_rearm = true;
                break;
            }
        }
        assert!(saw_rearm, "reset_window must NOTIFY the onwards config channel");
    }
}
