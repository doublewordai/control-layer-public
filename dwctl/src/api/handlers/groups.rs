
use sqlx_pool_router::PoolProvider;

use crate::api::models::deployments::DeployedModelResponse;
use crate::api::models::groups::{GroupCreate, GroupResponse, GroupUpdate, ListGroupsQuery};
use crate::api::models::pagination::PaginatedResponse;
use crate::api::models::users::{CurrentUser, UserResponse};
use crate::auth::permissions::{RequiresPermission, can_read_all_resources, can_read_own_resource, operation, resource};
use crate::db::handlers::{Deployments, Groups, Repository, Users, groups::GroupFilter};
use crate::db::models::groups::{GroupCreateDBRequest, GroupUpdateDBRequest};
use crate::errors::{Error, Result};
use crate::types::{Operation, Permission, Resource};
use crate::{
    AppState,
    types::{DeploymentId, GroupId, UserId},
};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};

#[utoipa::path(
    get,
    path = "/groups",
    tag = "groups",
    summary = "List groups",
    description = "Retrieve all groups with optional pagination and search filtering. Groups are \
        used to organize users and control access to model deployments. Use the `include` query \
        parameter to fetch related users and models in a single request.",
    responses(
        (status = 200, description = "List of groups", body = Vec<GroupResponse>),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("skip" = Option<i64>, Query, description = "Number of groups to skip"),
        ("limit" = Option<i64>, Query, description = "Maximum number of groups to return"),
        ("search" = Option<String>, Query, description = "Search query to filter groups by name or description (case-insensitive substring match)"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn list_groups<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Query(query): Query<ListGroupsQuery>,
    _: RequiresPermission<resource::Groups, operation::ReadAll>,
) -> Result<Json<PaginatedResponse<GroupResponse>>> {
    let mut conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;

    let groups;
    let total_count;
    let skip;
    let limit;
    {
        let mut repo = Groups::new(&mut conn);
        skip = query.pagination.skip();
        limit = query.pagination.limit();

        let mut filter = GroupFilter::new(skip, limit);

        if let Some(search) = query.search.as_ref()
            && !search.trim().is_empty()
        {
            filter = filter.with_search(search.trim().to_string());
        }

        groups = repo.list(&filter).await?;
        total_count = repo.count(&filter).await?;
    }

    let includes: Vec<&str> = query
        .include
        .as_deref()
        .unwrap_or("")
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    let mut response_groups = Vec::new();

    if !includes.is_empty() {
        let group_ids: Vec<_> = groups.iter().map(|g| g.id).collect();

        let resolved_users_map = if includes.contains(&"users") {
            let groups_users_map;
            {
                let mut repo = Groups::new(&mut conn);
                groups_users_map = repo.get_groups_users_bulk(&group_ids).await?;
            }
            let all_user_ids: Vec<UserId> = groups_users_map
                .values()
                .flat_map(|user_ids| user_ids.iter())
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();

            let users_bulk;
            {
                let mut users_repo = Users::new(&mut conn);
                users_bulk = users_repo.get_bulk(all_user_ids).await?;
            }

            let mut resolved_map = std::collections::HashMap::new();
            for (group_id, user_ids) in groups_users_map {
                let users: Vec<UserResponse> = user_ids
                    .iter()
                    .filter_map(|user_id| users_bulk.get(user_id))
                    .map(|user_db| UserResponse::from(user_db.clone()))
                    .collect();
                resolved_map.insert(group_id, users);
            }
            Some(resolved_map)
        } else {
            None
        };

        let resolved_models_map = if includes.contains(&"models") {
            let groups_deployments_map;
            {
                let mut repo = Groups::new(&mut conn);
                groups_deployments_map = repo.get_groups_deployments_bulk(&group_ids).await?;
            }
            let all_deployment_ids: Vec<DeploymentId> = groups_deployments_map
                .values()
                .flat_map(|deployment_ids| deployment_ids.iter())
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();

            let mut deployments_repo = Deployments::new(&mut conn);
            let deployments_bulk = deployments_repo.get_bulk(all_deployment_ids).await?;
            let mut resolved_map = std::collections::HashMap::new();
            for (group_id, deployment_ids) in groups_deployments_map {
                let models: Vec<DeployedModelResponse> = deployment_ids
                    .iter()
                    .filter_map(|deployment_id| deployments_bulk.get(deployment_id))
                    .map(|deployment_db| DeployedModelResponse::from(deployment_db.clone()))
                    .collect();
                resolved_map.insert(group_id, models);
            }
            Some(resolved_map)
        } else {
            None
        };

        for group in groups {
            let users = resolved_users_map.as_ref().and_then(|map| map.get(&group.id).cloned());
            let models = resolved_models_map.as_ref().and_then(|map| map.get(&group.id).cloned());

            let response_group = GroupResponse::from(group).with_relationships(users, models);
            response_groups.push(response_group);
        }
    } else {
        response_groups = groups.into_iter().map(GroupResponse::from).collect();
    }

    let paginated_response = PaginatedResponse::new(response_groups, total_count, skip, limit);
    Ok(Json(paginated_response))
}

#[utoipa::path(
    post,
    path = "/groups",
    tag = "groups",
    summary = "Create group",
    description = "Create a new group for organizing users and controlling model access. After \
        creation, use the membership endpoints to add users and grant access to model deployments.",
    request_body = GroupCreate,
    responses(
        (status = 201, description = "Group created successfully", body = GroupResponse),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn create_group<P: PoolProvider>(
    State(state): State<AppState<P>>,
    current_user: RequiresPermission<resource::Groups, operation::CreateAll>,
    Json(create): Json<GroupCreate>,
) -> Result<(StatusCode, Json<GroupResponse>)> {
    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Groups::new(&mut pool_conn);
    let request = GroupCreateDBRequest::new(current_user.id, create);

    let group = repo.create(&request).await?;
    Ok((StatusCode::CREATED, Json(GroupResponse::from(group))))
}

#[utoipa::path(
    get,
    path = "/groups/{group_id}",
    tag = "groups",
    summary = "Get group",
    responses(
        (status = 200, description = "Group details", body = GroupResponse),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Group not found"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("group_id" = uuid::Uuid, Path, description = "Group ID")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_group<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(group_id): Path<GroupId>,
    _: RequiresPermission<resource::Groups, operation::ReadAll>,
) -> Result<Json<GroupResponse>> {
    let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Groups::new(&mut pool_conn);

    match repo.get_by_id(group_id).await? {
        Some(group) => Ok(Json(GroupResponse::from(group))),
        None => Err(Error::NotFound {
            resource: "Group".to_string(),
            id: group_id.to_string(),
        }),
    }
}

#[utoipa::path(
    patch,
    path = "/groups/{group_id}",
    tag = "groups",
    summary = "Update group",
    request_body = GroupUpdate,
    responses(
        (status = 200, description = "Group updated successfully", body = GroupResponse),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Group not found"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("group_id" = uuid::Uuid, Path, description = "Group ID")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn update_group<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(group_id): Path<GroupId>,
    _: RequiresPermission<resource::Groups, operation::UpdateAll>,
    Json(update): Json<GroupUpdate>,
) -> Result<Json<GroupResponse>> {
    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Groups::new(&mut pool_conn);
    let request = GroupUpdateDBRequest::from(update);

    let group = repo.update(group_id, &request).await?;
    Ok(Json(GroupResponse::from(group)))
}

#[utoipa::path(
    delete,
    path = "/groups/{group_id}",
    tag = "groups",
    summary = "Delete group",
    responses(
        (status = 204, description = "Group deleted successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Group not found"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("group_id" = uuid::Uuid, Path, description = "Group ID")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn delete_group<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(group_id): Path<GroupId>,
    _: RequiresPermission<resource::Groups, operation::DeleteAll>,
) -> Result<StatusCode> {
    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Groups::new(&mut pool_conn);

    if repo.delete(group_id).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(Error::NotFound {
            resource: "Group".to_string(),
            id: group_id.to_string(),
        })
    }
}

#[utoipa::path(
    post,
    path = "/groups/{group_id}/users/{user_id}",
    tag = "groups",
    summary = "Add user to group",
    responses(
        (status = 204, description = "User added to group successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("group_id" = uuid::Uuid, Path, description = "Group ID"),
        ("user_id" = uuid::Uuid, Path, description = "User ID")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn add_user_to_group<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((group_id, user_id)): Path<(GroupId, UserId)>,
    _: RequiresPermission<resource::Groups, operation::UpdateAll>,
) -> Result<StatusCode> {
    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Groups::new(&mut pool_conn);
    repo.add_user_to_group(user_id, group_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    delete,
    path = "/groups/{group_id}/users/{user_id}",
    tag = "groups",
    summary = "Remove user from group",
    responses(
        (status = 204, description = "User removed from group successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Relationship not found"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("group_id" = uuid::Uuid, Path, description = "Group ID"),
        ("user_id" = uuid::Uuid, Path, description = "User ID")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn remove_user_from_group<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((group_id, user_id)): Path<(GroupId, UserId)>,
    _: RequiresPermission<resource::Groups, operation::UpdateAll>,
) -> Result<StatusCode> {
    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Groups::new(&mut pool_conn);
    repo.remove_user_from_group(user_id, group_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/users/{user_id}/groups/{group_id}",
    tag = "groups",
    summary = "Add group to user",
    responses(
        (status = 204, description = "User added to group successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("user_id" = uuid::Uuid, Path, description = "User ID"),
        ("group_id" = uuid::Uuid, Path, description = "Group ID")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn add_group_to_user<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((user_id, group_id)): Path<(UserId, GroupId)>,
    _: RequiresPermission<resource::Groups, operation::UpdateAll>,
) -> Result<StatusCode> {
    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Groups::new(&mut pool_conn);
    repo.add_user_to_group(user_id, group_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    delete,
    path = "/users/{user_id}/groups/{group_id}",
    tag = "groups",
    summary = "Remove group from user",
    responses(
        (status = 204, description = "User removed from group successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Relationship not found"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("user_id" = uuid::Uuid, Path, description = "User ID"),
        ("group_id" = uuid::Uuid, Path, description = "Group ID")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn remove_group_from_user<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((user_id, group_id)): Path<(UserId, GroupId)>,
    _: RequiresPermission<resource::Groups, operation::UpdateAll>,
) -> Result<StatusCode> {
    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Groups::new(&mut pool_conn);
    repo.remove_user_from_group(user_id, group_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/groups/{group_id}/users",
    tag = "groups",
    summary = "Get group users",
    responses(
        (status = 200, description = "List of users in group", body = Vec<String>),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Group not found"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("group_id" = uuid::Uuid, Path, description = "Group ID")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_group_users<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(group_id): Path<GroupId>,
    _: RequiresPermission<resource::Users, operation::ReadAll>,
) -> Result<Json<Vec<UserId>>> {
    let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Groups::new(&mut pool_conn);

    Ok(Json(repo.get_group_users(group_id).await?))
}

#[utoipa::path(
    get,
    path = "/users/{user_id}/groups",
    tag = "groups",
    summary = "Get user groups",
    description = "Retrieve all groups that a specific user belongs to. Platform managers can view \
        any user's groups; standard users can only view their own. This is useful for understanding \
        a user's model access permissions.",
    responses(
        (status = 200, description = "List of groups for user", body = Vec<GroupResponse>),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "User not found"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("user_id" = uuid::Uuid, Path, description = "User ID")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_user_groups<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(user_id): Path<UserId>,
    current_user: CurrentUser,
) -> Result<Json<Vec<GroupResponse>>> {
    let can_read_all_users = can_read_all_resources(&current_user, Resource::Users);
    let can_read_own_user = can_read_own_resource(&current_user, Resource::Users, user_id);

    if !can_read_all_users && !can_read_own_user {
        return Err(Error::InsufficientPermissions {
            required: Permission::Any(vec![
                Permission::Allow(Resource::Users, Operation::ReadAll),
                Permission::Allow(Resource::Users, Operation::ReadOwn),
            ]),
            action: Operation::ReadOwn,
            resource: "user groups".to_string(),
        });
    }

    let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Groups::new(&mut pool_conn);

    let groups = repo.get_user_groups(user_id).await?;

    let response_groups: Vec<GroupResponse> = groups
        .into_iter()
        .map(|group| {
            let mut group_response = GroupResponse::from(group);
            if !can_read_all_users {
                group_response = group_response.mask_created_by();
            }
            group_response
        })
        .collect();

    Ok(Json(response_groups))
}


#[utoipa::path(
    post,
    path = "/groups/{group_id}/models/{deployment_id}",
    tag = "models",
    summary = "Grant group access to model",
    description = "Grant all members of the specified group access to use a model deployment. \
        Users in the group will be able to make API requests using this model. Access is \
        additive - granting the same access twice has no effect.",
    responses(
        (status = 204, description = "Group granted access to model successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("group_id" = uuid::Uuid, Path, description = "Group ID"),
        ("deployment_id" = uuid::Uuid, Path, description = "Deployment ID")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn add_deployment_to_group<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((group_id, deployment_id)): Path<(GroupId, DeploymentId)>,
    current_user: RequiresPermission<resource::Groups, operation::UpdateAll>,
) -> Result<StatusCode> {
    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Groups::new(&mut pool_conn);
    repo.add_deployment_to_group(deployment_id, group_id, current_user.id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    delete,
    path = "/groups/{group_id}/models/{deployment_id}",
    tag = "models",
    summary = "Revoke group access to model",
    responses(
        (status = 204, description = "Group access to model revoked successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("group_id" = uuid::Uuid, Path, description = "Group ID"),
        ("deployment_id" = uuid::Uuid, Path, description = "Deployment ID")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn remove_deployment_from_group<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((group_id, deployment_id)): Path<(GroupId, DeploymentId)>,
    _: RequiresPermission<resource::Groups, operation::UpdateAll>,
) -> Result<StatusCode> {
    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Groups::new(&mut pool_conn);
    repo.remove_deployment_from_group(deployment_id, group_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/groups/{group_id}/models",
    tag = "groups",
    summary = "Get models accessible by group",
    responses(
        (status = 200, description = "List of models accessible by group", body = Vec<String>),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Group not found"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("group_id" = uuid::Uuid, Path, description = "Group ID")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_group_deployments<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(group_id): Path<GroupId>,
    _: RequiresPermission<resource::Groups, operation::ReadAll>,
) -> Result<Json<Vec<DeploymentId>>> {
    let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Groups::new(&mut pool_conn);
    let deployments = repo.get_group_deployments(group_id).await?;
    Ok(Json(deployments))
}

#[utoipa::path(
    get,
    path = "/models/{deployment_id}/groups",
    tag = "models",
    summary = "Get groups with model access",
    responses(
        (status = 200, description = "List of groups with access to model", body = Vec<String>),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Deployment not found"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("deployment_id" = uuid::Uuid, Path, description = "Deployment ID")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_deployment_groups<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(deployment_id): Path<DeploymentId>,
    _: RequiresPermission<resource::Groups, operation::ReadAll>,
) -> Result<Json<Vec<GroupId>>> {
    let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Groups::new(&mut pool_conn);
    let groups = repo.get_deployment_groups(deployment_id).await?;
    Ok(Json(groups))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::{
        api::models::{groups::GroupResponse, pagination::PaginatedResponse, users::Role},
        db::{
            handlers::{Deployments, Groups, Repository},
            models::{deployments::DeploymentCreateDBRequest, groups::GroupCreateDBRequest},
        },
        test::utils::*,
        types::{DeploymentId, GroupId, UserId},
    };
    use axum::http::StatusCode;
    use serde_json::json;
    use sqlx::PgPool;

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_groups_with_pagination(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        for i in 0..5 {
            let group_create = GroupCreateDBRequest {
                name: format!("Test Group {i}"),
                description: Some(format!("Description for group {i}")),
                created_by: user.id,
            };
            group_repo.create(&group_create).await.expect("Failed to create test group");
        }

        let response = app
            .get("/admin/api/v1/groups?limit=3")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated_response: PaginatedResponse<GroupResponse> = response.json();
        assert_eq!(paginated_response.data.len(), 3);
        assert_eq!(paginated_response.limit, 3);

        let response = app
            .get("/admin/api/v1/groups?skip=2&limit=2")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated_response: PaginatedResponse<GroupResponse> = response.json();
        assert_eq!(paginated_response.data.len(), 2);
        assert_eq!(paginated_response.skip, 2);
        assert_eq!(paginated_response.limit, 2);

        let response = app
            .get("/admin/api/v1/groups?skip=1000&limit=10")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated_response: PaginatedResponse<GroupResponse> = response.json();
        assert!(paginated_response.data.is_empty());

        let response = app
            .get("/admin/api/v1/groups")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated_response: PaginatedResponse<GroupResponse> = response.json();
        assert_eq!(paginated_response.data.len(), 6);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_add_user_to_group(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_admin_user(&pool, Role::PlatformManager).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "Test Group".to_string(),
            description: Some("Test group for membership".to_string()),
            created_by: user1.id,
        };
        let group = group_repo.create(&group_create).await.expect("Failed to create test group");

        let response = app
            .post(&format!("/admin/api/v1/groups/{}/users/{}", group.id, user2.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status(StatusCode::NO_CONTENT);

        let response = app
            .get(&format!("/admin/api/v1/groups/{}/users", group.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_ok();
        let user_ids: Vec<UserId> = response.json();
        assert!(user_ids.contains(&user2.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_remove_user_from_group(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_admin_user(&pool, Role::PlatformManager).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "Test Group".to_string(),
            description: Some("Test group for membership".to_string()),
            created_by: user1.id,
        };
        let group = group_repo.create(&group_create).await.expect("Failed to create test group");

        let response = app
            .post(&format!("/admin/api/v1/groups/{}/users/{}", group.id, user2.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;
        response.assert_status(StatusCode::NO_CONTENT);

        let response = app
            .delete(&format!("/admin/api/v1/groups/{}/users/{}", group.id, user2.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status(StatusCode::NO_CONTENT);

        let response = app
            .get(&format!("/admin/api/v1/groups/{}/users", group.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_ok();
        let user_ids: Vec<UserId> = response.json();
        assert!(!user_ids.contains(&user2.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_group_users(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_admin_user(&pool, Role::PlatformManager).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;
        let user3 = create_test_user(&pool, Role::StandardUser).await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "Test Group".to_string(),
            description: Some("Test group for listing users".to_string()),
            created_by: user1.id,
        };
        let group = group_repo.create(&group_create).await.expect("Failed to create test group");

        app.post(&format!("/admin/api/v1/groups/{}/users/{}", group.id, user2.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await
            .assert_status(StatusCode::NO_CONTENT);

        app.post(&format!("/admin/api/v1/groups/{}/users/{}", group.id, user3.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await
            .assert_status(StatusCode::NO_CONTENT);

        let response = app
            .get(&format!("/admin/api/v1/groups/{}/users", group.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_ok();
        let user_ids: Vec<UserId> = response.json();
        assert_eq!(user_ids.len(), 2);
        assert!(user_ids.contains(&user2.id));
        assert!(user_ids.contains(&user3.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_user_groups(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let mut group_ids = vec![];

        for i in 0..3 {
            let group_create = GroupCreateDBRequest {
                name: format!("Test Group {i}"),
                description: Some(format!("Test group {i} for user membership")),
                created_by: user.id,
            };
            let group = group_repo.create(&group_create).await.expect("Failed to create test group");
            group_ids.push(group.id);

            app.post(&format!("/admin/api/v1/groups/{}/users/{}", group.id, user.id))
                .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
                .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
                .await
                .assert_status(StatusCode::NO_CONTENT);
        }

        let response = app
            .get(&format!("/admin/api/v1/users/{}/groups", user.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let groups: Vec<GroupResponse> = response.json();
        assert_eq!(groups.len(), 4);

        for group_id in group_ids {
            assert!(groups.iter().any(|g| g.id == group_id));
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_duplicate_membership_prevention(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_admin_user(&pool, Role::PlatformManager).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "Test Group".to_string(),
            description: Some("Test group for duplicate prevention".to_string()),
            created_by: user1.id,
        };
        let group = group_repo.create(&group_create).await.expect("Failed to create test group");

        let response = app
            .post(&format!("/admin/api/v1/groups/{}/users/{}", group.id, user2.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;
        response.assert_status(StatusCode::NO_CONTENT);

        let response = app
            .post(&format!("/admin/api/v1/groups/{}/users/{}", group.id, user2.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;
        response.assert_status(StatusCode::NO_CONTENT);

        let response = app
            .get(&format!("/admin/api/v1/groups/{}/users", group.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_ok();
        let user_ids: Vec<UserId> = response.json();
        let user2_count = user_ids.iter().filter(|&id| *id == user2.id).count();
        assert_eq!(user2_count, 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_symmetric_group_user_endpoints(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "Test Group".to_string(),
            description: Some("Test symmetric endpoints".to_string()),
            created_by: user.id,
        };
        let group = group_repo.create(&group_create).await.expect("Failed to create test group");

        let response = app
            .post(&format!("/admin/api/v1/users/{}/groups/{}", user.id, group.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        response.assert_status(StatusCode::NO_CONTENT);

        let response = app
            .get(&format!("/admin/api/v1/groups/{}/users", group.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        response.assert_status_ok();
        let user_ids: Vec<UserId> = response.json();
        assert!(user_ids.contains(&user.id));

        let response = app
            .get(&format!("/admin/api/v1/users/{}/groups", user.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        response.assert_status_ok();
        let groups: Vec<GroupResponse> = response.json();
        assert!(groups.iter().any(|g| g.id == group.id));

        let response = app
            .delete(&format!("/admin/api/v1/users/{}/groups/{}", user.id, group.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        response.assert_status(StatusCode::NO_CONTENT);

        let response = app
            .get(&format!("/admin/api/v1/groups/{}/users", group.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        response.assert_status_ok();
        let user_ids: Vec<UserId> = response.json();
        assert!(!user_ids.contains(&user.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_add_deployment_to_group_api(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let group_create = json!({
            "name": "Test Group",
            "description": "Test group for deployment access"
        });

        let response = app
            .post("/admin/api/v1/groups")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&group_create)
            .await;

        response.assert_status(StatusCode::CREATED);
        let group: GroupResponse = response.json();

        let endpoint_id = get_test_endpoint_id(&pool).await;

        let mut pool_conn2 = pool.acquire().await.unwrap();
        let mut deployment_repo = Deployments::new(&mut pool_conn2);
        let mut deployment_create = DeploymentCreateDBRequest::builder()
            .created_by(admin_user.id)
            .model_name("test-model".to_string())
            .alias("test-alias".to_string())
            .build();
        deployment_create.hosted_on = Some(endpoint_id);
        let deployment = deployment_repo
            .create(&deployment_create)
            .await
            .expect("Failed to create test deployment");

        let response = app
            .post(&format!("/admin/api/v1/groups/{}/models/{}", group.id, deployment.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status(StatusCode::NO_CONTENT);

        let response = app
            .get(&format!("/admin/api/v1/groups/{}/models", group.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let deployments: Vec<DeploymentId> = response.json();
        assert!(deployments.contains(&deployment.id));

        let response = app
            .get(&format!("/admin/api/v1/models/{}/groups", deployment.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let groups: Vec<GroupId> = response.json();
        assert!(groups.contains(&group.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_remove_deployment_from_group_api(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let group_create = json!({
            "name": "Test Group",
            "description": "Test group for deployment access"
        });

        let response = app
            .post("/admin/api/v1/groups")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&group_create)
            .await;

        response.assert_status(StatusCode::CREATED);
        let group: GroupResponse = response.json();

        let endpoint_id = get_test_endpoint_id(&pool).await;

        let mut pool_conn2 = pool.acquire().await.unwrap();
        let mut deployment_repo = Deployments::new(&mut pool_conn2);
        let mut deployment_create = DeploymentCreateDBRequest::builder()
            .created_by(admin_user.id)
            .model_name("test-model".to_string())
            .alias("test-alias".to_string())
            .build();
        deployment_create.hosted_on = Some(endpoint_id);
        let deployment = deployment_repo
            .create(&deployment_create)
            .await
            .expect("Failed to create test deployment");

        let response = app
            .post(&format!("/admin/api/v1/groups/{}/models/{}", group.id, deployment.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status(StatusCode::NO_CONTENT);

        let response = app
            .delete(&format!("/admin/api/v1/groups/{}/models/{}", group.id, deployment.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status(StatusCode::NO_CONTENT);

        let response = app
            .get(&format!("/admin/api/v1/groups/{}/models", group.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let deployments: Vec<DeploymentId> = response.json();
        assert!(!deployments.contains(&deployment.id));

        let response = app
            .get(&format!("/admin/api/v1/models/{}/groups", deployment.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let groups: Vec<GroupId> = response.json();
        assert!(!groups.contains(&group.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_deployment_group_access_control(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let regular_user = create_test_user(&pool, Role::StandardUser).await;

        let group_create = json!({
            "name": "Test Group",
            "description": "Test group for deployment access"
        });

        let response = app
            .post("/admin/api/v1/groups")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&group_create)
            .await;

        response.assert_status(StatusCode::CREATED);
        let group: GroupResponse = response.json();

        let endpoint_id = get_test_endpoint_id(&pool).await;

        let mut pool_conn2 = pool.acquire().await.unwrap();
        let mut deployment_repo = Deployments::new(&mut pool_conn2);
        let mut deployment_create = DeploymentCreateDBRequest::builder()
            .created_by(admin_user.id)
            .model_name("test-model".to_string())
            .alias("test-alias".to_string())
            .build();
        deployment_create.hosted_on = Some(endpoint_id);
        let deployment = deployment_repo
            .create(&deployment_create)
            .await
            .expect("Failed to create test deployment");

        let response = app
            .post(&format!("/admin/api/v1/groups/{}/models/{}", group.id, deployment.id))
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .await;

        response.assert_status(StatusCode::FORBIDDEN);

        let response = app
            .get(&format!("/admin/api/v1/groups/{}/models", group.id))
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .await;

        response.assert_status(StatusCode::FORBIDDEN);

        let response = app
            .get(&format!("/admin/api/v1/models/{}/groups", deployment.id))
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .await;

        response.assert_status(StatusCode::FORBIDDEN);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_groups_with_include_parameters(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let regular_user = create_test_user(&pool, Role::StandardUser).await;

        let group_create = json!({
            "name": "Test Group",
            "description": "Test group for include parameters"
        });

        let response = app
            .post("/admin/api/v1/groups")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&group_create)
            .await;

        response.assert_status(StatusCode::CREATED);
        let group: GroupResponse = response.json();

        app.post(&format!("/admin/api/v1/groups/{}/users/{}", group.id, regular_user.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await
            .assert_status(StatusCode::NO_CONTENT);

        let endpoint_id = get_test_endpoint_id(&pool).await;

        let mut pool_conn2 = pool.acquire().await.unwrap();
        let mut deployment_repo = Deployments::new(&mut pool_conn2);
        let mut deployment_create = DeploymentCreateDBRequest::builder()
            .created_by(admin_user.id)
            .model_name("test-model".to_string())
            .alias("test-alias".to_string())
            .build();
        deployment_create.hosted_on = Some(endpoint_id);
        let deployment = deployment_repo
            .create(&deployment_create)
            .await
            .expect("Failed to create test deployment");

        app.post(&format!("/admin/api/v1/groups/{}/models/{}", group.id, deployment.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await
            .assert_status(StatusCode::NO_CONTENT);

        let response = app
            .get("/admin/api/v1/groups")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated_response: PaginatedResponse<GroupResponse> = response.json();
        let found_group = paginated_response.data.iter().find(|g| g.id == group.id).expect("Group not found");
        assert!(found_group.users.is_none());
        assert!(found_group.models.is_none());

        let response = app
            .get("/admin/api/v1/groups?include=users")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated_response: PaginatedResponse<GroupResponse> = response.json();
        let found_group = paginated_response.data.iter().find(|g| g.id == group.id).expect("Group not found");
        assert!(found_group.users.is_some());
        assert!(found_group.models.is_none());
        let users = found_group.users.as_ref().unwrap().iter().map(|x| x.id).collect::<HashSet<_>>();
        assert!(users.contains(&regular_user.id));

        let response = app
            .get("/admin/api/v1/groups?include=models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated_response: PaginatedResponse<GroupResponse> = response.json();
        let found_group = paginated_response.data.iter().find(|g| g.id == group.id).expect("Group not found");
        assert!(found_group.users.is_none());
        assert!(found_group.models.is_some());
        let models = found_group.models.as_ref().unwrap().iter().map(|x| x.id).collect::<HashSet<_>>();
        assert!(models.contains(&deployment.id));

        let response = app
            .get("/admin/api/v1/groups?include=users,models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated_response: PaginatedResponse<GroupResponse> = response.json();
        let found_group = paginated_response.data.iter().find(|g| g.id == group.id).expect("Group not found");
        assert!(found_group.users.is_some());
        assert!(found_group.models.is_some());
        let users = found_group.users.as_ref().unwrap().iter().map(|x| x.id).collect::<HashSet<_>>();
        let models = found_group.models.as_ref().unwrap().iter().map(|x| x.id).collect::<HashSet<_>>();
        assert!(users.contains(&regular_user.id));
        assert!(models.contains(&deployment.id));

        let response = app
            .get("/admin/api/v1/groups?include=users,models&limit=10")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let paginated_response: PaginatedResponse<GroupResponse> = response.json();
        let found_group = paginated_response.data.iter().find(|g| g.id == group.id).expect("Group not found");
        assert!(found_group.users.is_some());
        assert!(found_group.models.is_some());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_can_see_other_user_groups(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_user(&pool, Role::PlatformManager).await;
        let standard_user = create_test_user(&pool, Role::StandardUser).await;

        let mut pool_conn = pool.clone().acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);

        let group1_create = GroupCreateDBRequest {
            name: "User Group 1".to_string(),
            description: Some("First group for standard user".to_string()),
            created_by: platform_manager.id,
        };
        let group1 = group_repo.create(&group1_create).await.expect("Failed to create test group");

        let group2_create = GroupCreateDBRequest {
            name: "User Group 2".to_string(),
            description: Some("Second group for standard user".to_string()),
            created_by: platform_manager.id,
        };
        let group2 = group_repo.create(&group2_create).await.expect("Failed to create test group");

        app.post(&format!("/admin/api/v1/groups/{}/users/{}", group1.id, standard_user.id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await
            .assert_status(StatusCode::NO_CONTENT);

        app.post(&format!("/admin/api/v1/groups/{}/users/{}", group2.id, standard_user.id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await
            .assert_status(StatusCode::NO_CONTENT);

        let response = app
            .get(&format!("/admin/api/v1/users/{}/groups", standard_user.id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;

        response.assert_status_ok();
        let groups: Vec<GroupResponse> = response.json();

        assert_eq!(groups.len(), 3, "Platform manager should see all user's groups");
        assert!(groups.iter().any(|g| g.id == group1.id), "Should see group1");
        assert!(groups.iter().any(|g| g.id == group2.id), "Should see group2");

        let another_standard_user = create_test_user(&pool, Role::StandardUser).await;

        app.post(&format!("/admin/api/v1/groups/{}/users/{}", group1.id, another_standard_user.id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await
            .assert_status(StatusCode::NO_CONTENT);

        let response = app
            .get(&format!("/admin/api/v1/users/{}/groups", another_standard_user.id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;

        response.assert_status_ok();
        let groups: Vec<GroupResponse> = response.json();

        assert_eq!(groups.len(), 2, "Platform manager should see user's actual groups");
        assert!(groups.iter().any(|g| g.id == group1.id), "Should see group1");
        assert!(!groups.iter().any(|g| g.id == group2.id), "Should NOT see group2");

        let request_viewer_only = create_test_user(&pool, Role::RequestViewer).await;
        let response = app
            .get(&format!("/admin/api/v1/users/{}/groups", standard_user.id))
            .add_header(
                &add_auth_headers(&request_viewer_only)[0].0,
                &add_auth_headers(&request_viewer_only)[0].1,
            )
            .add_header(
                &add_auth_headers(&request_viewer_only)[1].0,
                &add_auth_headers(&request_viewer_only)[1].1,
            )
            .await;

        response.assert_status(StatusCode::FORBIDDEN);

        let response = app
            .get(&format!("/admin/api/v1/users/{}/groups", standard_user.id))
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;

        response.assert_status_ok();

        let response = app
            .get(&format!("/admin/api/v1/users/{}/groups", another_standard_user.id))
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;

        response.assert_status(StatusCode::FORBIDDEN);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_multiple_roles_with_platform_manager_can_see_user_groups(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let standard_user = create_test_user(&pool, Role::StandardUser).await;

        let multi_role_user = create_test_user_with_roles(&pool, vec![Role::PlatformManager, Role::RequestViewer]).await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);

        let group_create = GroupCreateDBRequest {
            name: "Multi Role Test Group".to_string(),
            description: Some("Group for multi-role user test".to_string()),
            created_by: multi_role_user.id,
        };
        let group = group_repo.create(&group_create).await.expect("Failed to create test group");

        app.post(&format!("/admin/api/v1/groups/{}/users/{}", group.id, standard_user.id))
            .add_header(&add_auth_headers(&multi_role_user)[0].0, &add_auth_headers(&multi_role_user)[0].1)
            .add_header(&add_auth_headers(&multi_role_user)[1].0, &add_auth_headers(&multi_role_user)[1].1)
            .await
            .assert_status(StatusCode::NO_CONTENT);

        let response = app
            .get(&format!("/admin/api/v1/users/{}/groups", standard_user.id))
            .add_header(&add_auth_headers(&multi_role_user)[0].0, &add_auth_headers(&multi_role_user)[0].1)
            .add_header(&add_auth_headers(&multi_role_user)[1].0, &add_auth_headers(&multi_role_user)[1].1)
            .await;

        response.assert_status_ok();
        let groups: Vec<GroupResponse> = response.json();

        assert!(groups.len() >= 2, "Should see user's groups including the new group");
        assert!(groups.iter().any(|g| g.id == group.id), "Should see the created group");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_user_group_access_permissions(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        let group_create = json!({
            "name": "Access Test Group",
            "description": "Testing user group access permissions"
        });

        let response = app
            .post("/admin/api/v1/groups")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .json(&group_create)
            .await;

        response.assert_status(StatusCode::CREATED);
        let group: GroupResponse = response.json();

        app.post(&format!("/admin/api/v1/groups/{}/users/{}", group.id, user1.id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await
            .assert_status(StatusCode::NO_CONTENT);

        let response = app
            .get(&format!("/admin/api/v1/users/{}/groups", user1.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_ok();
        let user1_groups: Vec<GroupResponse> = response.json();
        assert!(user1_groups.iter().any(|g| g.id == group.id));

        let response = app
            .get(&format!("/admin/api/v1/users/{}/groups", user2.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_forbidden();

        let response = app
            .get(&format!("/admin/api/v1/users/{}/groups", user2.id))
            .add_header(&add_auth_headers(&user2)[0].0, &add_auth_headers(&user2)[0].1)
            .add_header(&add_auth_headers(&user2)[1].0, &add_auth_headers(&user2)[1].1)
            .await;

        response.assert_status_ok();
        let user2_groups: Vec<GroupResponse> = response.json();
        assert!(!user2_groups.iter().any(|g| g.id == group.id));

        let response = app
            .get(&format!("/admin/api/v1/groups/{}/users", group.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_forbidden();

        let response = app
            .get(&format!("/admin/api/v1/groups/{}/users", group.id))
            .add_header(&add_auth_headers(&user2)[0].0, &add_auth_headers(&user2)[0].1)
            .add_header(&add_auth_headers(&user2)[1].0, &add_auth_headers(&user2)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_deployment_group_management_permissions(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;
        let standard_user = create_test_user(&pool, Role::StandardUser).await;
        let request_viewer = create_test_user(&pool, Role::RequestViewer).await;

        let group_create = json!({
            "name": "Deployment Access Group",
            "description": "Testing deployment-group permissions"
        });

        let response = app
            .post("/admin/api/v1/groups")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .json(&group_create)
            .await;

        response.assert_status(StatusCode::CREATED);
        let group: GroupResponse = response.json();

        let endpoint_id = get_test_endpoint_id(&pool).await;
        let mut pool_conn = pool.acquire().await.unwrap();
        let mut deployment_repo = Deployments::new(&mut pool_conn);
        let deployment_create = DeploymentCreateDBRequest::builder()
            .created_by(platform_manager.id)
            .model_name("perm-test-model".to_string())
            .alias("perm-test-alias".to_string())
            .hosted_on(endpoint_id)
            .build();
        let deployment = deployment_repo.create(&deployment_create).await.unwrap();

        let response = app
            .post(&format!("/admin/api/v1/groups/{}/models/{}", group.id, deployment.id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;

        response.assert_status(StatusCode::NO_CONTENT);

        let response = app
            .post(&format!("/admin/api/v1/groups/{}/models/{}", group.id, deployment.id))
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;

        response.assert_status_forbidden();

        let response = app
            .post(&format!("/admin/api/v1/groups/{}/models/{}", group.id, deployment.id))
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;

        response.assert_status_forbidden();

        let response = app
            .delete(&format!("/admin/api/v1/groups/{}/models/{}", group.id, deployment.id))
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;

        response.assert_status_forbidden();

        let response = app
            .delete(&format!("/admin/api/v1/groups/{}/models/{}", group.id, deployment.id))
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;

        response.assert_status_forbidden();

        let response = app
            .get(&format!("/admin/api/v1/groups/{}/models", group.id))
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;

        response.assert_status_forbidden();

        let response = app
            .get(&format!("/admin/api/v1/groups/{}/models", group.id))
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;

        response.assert_status_forbidden();

        let response = app
            .get(&format!("/admin/api/v1/models/{}/groups", deployment.id))
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;

        response.assert_status_forbidden();

        let response = app
            .get(&format!("/admin/api/v1/models/{}/groups", deployment.id))
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;

        response.assert_status_forbidden();

        let response = app
            .get(&format!("/admin/api/v1/groups/{}/models", group.id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;

        response.assert_status_ok();

        let response = app
            .get(&format!("/admin/api/v1/models/{}/groups", deployment.id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;

        response.assert_status_ok();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_groups_list_permission_filtering(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;
        let request_viewer = create_test_user(&pool, Role::RequestViewer).await;
        let _standard_user = create_test_user(&pool, Role::StandardUser).await;

        let group_create = json!({
            "name": "Permission Filter Test",
            "description": "Testing permission filtering"
        });

        let response = app
            .post("/admin/api/v1/groups")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .json(&group_create)
            .await;

        response.assert_status(StatusCode::CREATED);

        let response = app
            .get("/admin/api/v1/groups")
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;

        response.assert_status_forbidden();

        let response = app
            .get("/admin/api/v1/groups/00000000-0000-0000-0000-000000000000")
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_layered_roles_platform_manager_plus_request_viewer(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let standard_user = create_test_user(&pool, Role::StandardUser).await;

        let full_admin = create_test_user_with_roles(&pool, vec![Role::PlatformManager, Role::RequestViewer]).await;

        let group_create = json!({
            "name": "Full Admin Group",
            "description": "Created by PlatformManager + RequestViewer"
        });

        let response = app
            .post("/admin/api/v1/groups")
            .add_header(&add_auth_headers(&full_admin)[0].0, &add_auth_headers(&full_admin)[0].1)
            .add_header(&add_auth_headers(&full_admin)[1].0, &add_auth_headers(&full_admin)[1].1)
            .json(&group_create)
            .await;

        response.assert_status(StatusCode::CREATED);

        let response = app
            .get("/admin/api/v1/groups")
            .add_header(&add_auth_headers(&full_admin)[0].0, &add_auth_headers(&full_admin)[0].1)
            .add_header(&add_auth_headers(&full_admin)[1].0, &add_auth_headers(&full_admin)[1].1)
            .await;

        response.assert_status_ok();

        let response = app
            .get(&format!("/admin/api/v1/users/{}/groups", standard_user.id))
            .add_header(&add_auth_headers(&full_admin)[0].0, &add_auth_headers(&full_admin)[0].1)
            .add_header(&add_auth_headers(&full_admin)[1].0, &add_auth_headers(&full_admin)[1].1)
            .await;

        response.assert_status_ok();

    }
}
