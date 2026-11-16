
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use fusillade::Storage;
use sqlx_pool_router::PoolProvider;
use uuid::Uuid;

use crate::{
    AppState,
    api::models::{
        batch_requests::{ListBatchRequestsQuery, ResponseDetail, ResponseSummary},
        pagination::PaginatedResponse,
        users::CurrentUser,
    },
    auth::permissions::{RequiresPermission, can_delete_all_resources, can_read_all_resources, operation, resource},
    errors::{Error, Result},
    types::Resource,
};

fn is_batch_owner(current_user: &CurrentUser, created_by: &str) -> bool {
    let user_id = current_user.id.to_string();
    if created_by == user_id {
        return true;
    }
    if let Some(org_id) = current_user.active_organization
        && created_by == org_id.to_string()
    {
        return true;
    }
    false
}

#[utoipa::path(
    get,
    path = "/admin/api/v1/batches/requests",
    params(ListBatchRequestsQuery),
    responses(
        (status = 200, description = "Paginated list of responses", body = PaginatedResponse<ResponseSummary>),
        (status = 403, description = "Insufficient permissions"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "batch_requests",
)]
#[tracing::instrument(skip_all)]
pub async fn list_batch_requests<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Query(query): Query<ListBatchRequestsQuery>,
    current_user: CurrentUser,
    _: RequiresPermission<resource::Batches, operation::ReadOwn>,
) -> Result<Json<PaginatedResponse<ResponseSummary>>> {
    let skip = query.pagination.skip();
    let limit = query.pagination.limit();
    let can_read_all = can_read_all_resources(&current_user, Resource::Batches);

    let created_by_filter: Option<String> = if let Some(member_id) = query.member_id {
        if !can_read_all {
            return Err(Error::BadRequest {
                message: "member_id filter requires platform manager permissions".to_string(),
            });
        }
        Some(member_id.to_string())
    } else if let Some(org_id) = current_user.active_organization {
        Some(org_id.to_string())
    } else if can_read_all {
        None
    } else {
        Some(current_user.id.to_string())
    };

    let models: Option<Vec<String>> = query
        .model
        .as_ref()
        .map(|m| m.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect());

    let result = state
        .request_manager
        .list_requests(fusillade::ListRequestsFilter {
            created_by: created_by_filter,
            status: query.status.clone(),
            models,
            created_after: query.created_after,
            created_before: query.created_before,
            service_tiers: query
                .service_tiers
                .as_ref()
                .map(|s| s.split(',').map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect()),
            active_first: query.active_first.unwrap_or(true),
            skip,
            limit,
        })
        .await
        .map_err(|e| Error::Internal {
            operation: format!("list responses: {}", e),
        })?;

    let request_ids: Vec<Uuid> = result.data.iter().map(|r| r.id).collect();

    let analytics = if !request_ids.is_empty() {
        sqlx::query_as::<_, AnalyticsRow>(
            r#"
            SELECT DISTINCT ON (fusillade_request_id)
                fusillade_request_id as request_id,
                prompt_tokens,
                completion_tokens,
                reasoning_tokens,
                total_tokens,
                api_key_id
            FROM http_analytics
            WHERE fusillade_request_id = ANY($1)
            ORDER BY fusillade_request_id, timestamp DESC
            "#,
        )
        .bind(&request_ids)
        .fetch_all(state.db.read())
        .await
        .map_err(|e| Error::Database(e.into()))?
    } else {
        vec![]
    };

    let analytics_map: std::collections::HashMap<Uuid, AnalyticsRow> = analytics.into_iter().map(|a| (a.request_id, a)).collect();

    let costs = if !request_ids.is_empty() {
        sqlx::query_as::<_, CostRow>(
            r#"
            SELECT DISTINCT ON (fusillade_request_id)
                fusillade_request_id as request_id,
                amount::float8 as total_cost
            FROM credits_transactions
            WHERE fusillade_request_id = ANY($1) AND transaction_type = 'usage'
            ORDER BY fusillade_request_id, seq DESC
            "#,
        )
        .bind(&request_ids)
        .fetch_all(state.db.write())
        .await
        .map_err(|e| Error::Database(e.into()))?
    } else {
        vec![]
    };
    let cost_map: std::collections::HashMap<Uuid, f64> =
        costs.into_iter().filter_map(|c| c.total_cost.map(|v| (c.request_id, v))).collect();

    let unique_creator_ids: Vec<String> = result
        .data
        .iter()
        .map(|r| r.created_by.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let emails = if !unique_creator_ids.is_empty() {
        sqlx::query_as::<_, EmailRow>("SELECT id::text as user_id, email FROM users WHERE id::text = ANY($1)")
            .bind(&unique_creator_ids)
            .fetch_all(state.db.write())
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "failed to fetch creator emails for responses");
                vec![]
            })
    } else {
        vec![]
    };

    let email_map: std::collections::HashMap<String, String> = emails.into_iter().map(|e| (e.user_id, e.email)).collect();

    let api_key_ids: Vec<Uuid> = analytics_map
        .values()
        .filter_map(|a| a.api_key_id)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let holder_map = fetch_api_key_holders(state.db.read(), &api_key_ids).await;

    let data: Vec<ResponseSummary> = result
        .data
        .into_iter()
        .map(|r| {
            let a = analytics_map.get(&r.id);
            let email: Option<String> = email_map.get(&r.created_by).cloned();
            let holder = a.and_then(|a| a.api_key_id).and_then(|id| holder_map.get(&id));
            ResponseSummary {
                id: r.id,
                batch_id: r.batch_id,
                model: r.model,
                status: r.status,
                created_at: r.created_at,
                completed_at: r.completed_at,
                failed_at: r.failed_at,
                duration_ms: r.duration_ms,
                response_status: r.response_status,
                service_tier: r.service_tier,
                prompt_tokens: a.and_then(|a| a.prompt_tokens),
                completion_tokens: a.and_then(|a| a.completion_tokens),
                reasoning_tokens: a.and_then(|a| a.reasoning_tokens),
                total_tokens: a.and_then(|a| a.total_tokens),
                total_cost: cost_map.get(&r.id).copied(),
                created_by_email: email,
                api_key_id: holder.map(|h| h.root_api_key_id),
                api_key_name: holder.map(|h| h.api_key_name.clone()),
                api_key_holder_email: holder.and_then(|h| h.api_key_holder_email.clone()),
            }
        })
        .collect();

    Ok(Json(PaginatedResponse::new(data, result.total_count, skip, limit)))
}

#[utoipa::path(
    get,
    path = "/admin/api/v1/batches/requests/{request_id}",
    params(
        ("request_id" = Uuid, Path, description = "The request ID"),
    ),
    responses(
        (status = 200, description = "Response detail", body = ResponseDetail),
        (status = 404, description = "Response not found"),
        (status = 403, description = "Insufficient permissions"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "batch_requests",
)]
#[tracing::instrument(skip_all)]
pub async fn get_batch_request<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(request_id): Path<Uuid>,
    current_user: CurrentUser,
    _: RequiresPermission<resource::Batches, operation::ReadOwn>,
) -> Result<Json<ResponseDetail>> {
    let detail = state
        .request_manager
        .get_request_detail(fusillade::RequestId(request_id))
        .await
        .map_err(|e| match e {
            fusillade::FusilladeError::RequestNotFound(_) => Error::NotFound {
                resource: "Response".to_string(),
                id: request_id.to_string(),
            },
            other => {
                tracing::error!(request_id = %request_id, error = %other, "failed to fetch response detail");
                Error::Internal {
                    operation: format!("get response detail: {}", other),
                }
            }
        })?;

    let can_read_all = can_read_all_resources(&current_user, Resource::Batches);
    if !can_read_all && !is_batch_owner(&current_user, &detail.created_by) {
        return Err(Error::NotFound {
            resource: "Response".to_string(),
            id: request_id.to_string(),
        });
    }

    let analytics = sqlx::query_as::<_, AnalyticsRow>(
        r#"
        SELECT DISTINCT ON (fusillade_request_id)
            fusillade_request_id as request_id,
            prompt_tokens,
            completion_tokens,
            reasoning_tokens,
            total_tokens,
            api_key_id
        FROM http_analytics
        WHERE fusillade_request_id = $1
        ORDER BY fusillade_request_id, timestamp DESC
        "#,
    )
    .bind(request_id)
    .fetch_optional(state.db.read())
    .await
    .map_err(|e| Error::Database(e.into()))?;

    let cost = sqlx::query_as::<_, CostRow>(
        r#"
        SELECT DISTINCT ON (fusillade_request_id)
            fusillade_request_id as request_id,
            amount::float8 as total_cost
        FROM credits_transactions
        WHERE fusillade_request_id = $1 AND transaction_type = 'usage'
        ORDER BY fusillade_request_id, seq DESC
        "#,
    )
    .bind(request_id)
    .fetch_optional(state.db.write())
    .await
    .map_err(|e| Error::Database(e.into()))?
    .and_then(|c| c.total_cost);

    let created_by_email = if let Ok(created_by_uuid) = Uuid::parse_str(&detail.created_by) {
        sqlx::query_as::<_, EmailRow>("SELECT id::text as user_id, email FROM users WHERE id = $1")
            .bind(created_by_uuid)
            .fetch_optional(state.db.write())
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "failed to fetch creator email for response detail");
                None
            })
            .map(|row| row.email)
    } else {
        None
    };

    let holder = match analytics.as_ref().and_then(|a| a.api_key_id) {
        Some(api_key_id) => fetch_api_key_holders(state.db.read(), &[api_key_id]).await.remove(&api_key_id),
        None => None,
    };

    let is_zdr = detail.body.as_deref().is_some_and(crate::inference::zdr::is_zdr_body);

    Ok(Json(ResponseDetail {
        id: detail.id,
        batch_id: detail.batch_id,
        model: detail.model,
        status: detail.status,
        created_at: detail.created_at,
        completed_at: detail.completed_at,
        failed_at: detail.failed_at,
        duration_ms: detail.duration_ms,
        response_status: detail.response_status,
        service_tier: detail.service_tier,
        prompt_tokens: analytics.as_ref().and_then(|a| a.prompt_tokens),
        completion_tokens: analytics.as_ref().and_then(|a| a.completion_tokens),
        reasoning_tokens: analytics.as_ref().and_then(|a| a.reasoning_tokens),
        total_tokens: analytics.as_ref().and_then(|a| a.total_tokens),
        total_cost: cost,
        body: if is_zdr { String::new() } else { detail.body.unwrap_or_default() },
        response_body: if is_zdr { None } else { detail.response_body },
        error: if is_zdr { None } else { detail.error },
        created_by: detail.created_by,
        created_by_email,
        api_key_id: holder.as_ref().map(|h| h.root_api_key_id),
        api_key_name: holder.as_ref().map(|h| h.api_key_name.clone()),
        api_key_holder_email: holder.and_then(|h| h.api_key_holder_email),
    }))
}

#[utoipa::path(
    delete,
    path = "/admin/api/v1/batches/requests/{request_id}",
    params(
        ("request_id" = Uuid, Path, description = "The request ID"),
    ),
    responses(
        (status = 204, description = "Response deleted"),
        (status = 404, description = "Response not found"),
        (status = 403, description = "Insufficient permissions"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "batch_requests",
)]
#[tracing::instrument(skip_all)]
pub async fn delete_batch_request<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(request_id): Path<Uuid>,
    current_user: CurrentUser,
    _: RequiresPermission<resource::Batches, operation::DeleteOwn>,
) -> Result<StatusCode> {
    let detail = state
        .request_manager
        .get_request_detail(fusillade::RequestId(request_id))
        .await
        .map_err(|e| match e {
            fusillade::FusilladeError::RequestNotFound(_) => Error::NotFound {
                resource: "Response".to_string(),
                id: request_id.to_string(),
            },
            _ => Error::Internal {
                operation: "get response for deletion".to_string(),
            },
        })?;

    let can_delete_all = can_delete_all_resources(&current_user, Resource::Batches);
    if !can_delete_all && !is_batch_owner(&current_user, &detail.created_by) {
        return Err(Error::NotFound {
            resource: "Response".to_string(),
            id: request_id.to_string(),
        });
    }

    let erasure = if can_delete_all {
        state.request_manager.delete_response_group(request_id).await
    } else {
        state
            .request_manager
            .delete_owned_response_group(request_id, &detail.created_by)
            .await
    };
    erasure.map_err(|e| match e {
        fusillade::FusilladeError::RequestNotFound(_) => Error::NotFound {
            resource: "Response".to_string(),
            id: request_id.to_string(),
        },
        _ => Error::Internal {
            operation: "delete response graph".to_string(),
        },
    })?;

    Ok(StatusCode::NO_CONTENT)
}

#[derive(sqlx::FromRow)]
struct AnalyticsRow {
    #[allow(dead_code)]
    request_id: Uuid,
    prompt_tokens: Option<i64>,
    completion_tokens: Option<i64>,
    reasoning_tokens: Option<i64>,
    total_tokens: Option<i64>,
    api_key_id: Option<Uuid>,
}

#[derive(sqlx::FromRow)]
struct ApiKeyHolderRow {
    api_key_id: Uuid,
    root_api_key_id: Uuid,
    api_key_name: String,
    api_key_holder_email: Option<String>,
}

async fn fetch_api_key_holders<'a, E>(executor: E, api_key_ids: &[Uuid]) -> std::collections::HashMap<Uuid, ApiKeyHolderRow>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    if api_key_ids.is_empty() {
        return std::collections::HashMap::new();
    }
    sqlx::query_as::<_, ApiKeyHolderRow>(
        r#"
        SELECT ak.id      AS api_key_id,
               root.id    AS root_api_key_id,
               root.name  AS api_key_name,
               holder.email AS api_key_holder_email
        FROM api_keys ak
        JOIN api_keys root ON root.id = COALESCE(ak.parent_api_key_id, ak.id)
        LEFT JOIN users holder ON holder.id = ak.created_by
        WHERE ak.id = ANY($1)
        "#,
    )
    .bind(api_key_ids)
    .fetch_all(executor)
    .await
    .unwrap_or_else(|e| {
        tracing::warn!(error = %e, "failed to resolve API key holders for responses");
        vec![]
    })
    .into_iter()
    .map(|row| (row.api_key_id, row))
    .collect()
}

#[derive(sqlx::FromRow)]
struct CostRow {
    request_id: Uuid,
    total_cost: Option<f64>,
}

#[derive(sqlx::FromRow)]
struct EmailRow {
    user_id: String,
    email: String,
}

#[cfg(test)]
mod tests {
    use crate::{api::models::users::Role, test::utils::*};
    use sqlx::PgPool;

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_batch_requests_requires_auth(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;

        let response = app.get("/admin/api/v1/batches/requests").await;
        response.assert_status_unauthorized();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_batch_requests_empty(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;

        let response = app
            .get("/admin/api/v1/batches/requests")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let body: serde_json::Value = response.json();
        assert_eq!(body["total_count"], 0);
        assert!(body["data"].as_array().unwrap().is_empty());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_member_id_filter_rejected_for_non_pm(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;

        let response = app
            .get(&format!("/admin/api/v1/batches/requests?member_id={}", uuid::Uuid::new_v4()))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_batch_request_returns_404_for_missing(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let auth = add_auth_headers(&user);

        let response = app
            .get(&format!("/admin/api/v1/batches/requests/{}", uuid::Uuid::new_v4()))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;

        response.assert_status_not_found();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_batch_requests_pm_in_org_context_scopes_to_org(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;

        let pm_user = create_test_user_with_roles(&pool, vec![Role::PlatformManager, Role::StandardUser, Role::BatchAPIUser]).await;
        let other_user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let org = create_test_org(&pool, pm_user.id).await;
        let auth = add_auth_headers(&pm_user);

        let body = serde_json::json!({"model": "test-model", "messages": [{"role": "user", "content": "hi"}]});
        let owners = [
            ("pm_user", pm_user.id.to_string()),
            ("other_user", other_user.id.to_string()),
            ("org", org.id.to_string()),
        ];
        for (_label, owner) in &owners {
            let template_id = uuid::Uuid::new_v4();
            let request_id = uuid::Uuid::new_v4();
            crate::test::utils::insert_fusillade_template(
                &pool,
                template_id,
                None,
                "test-model",
                "http://test",
                "/v1/chat/completions",
                &serde_json::to_string(&body).unwrap(),
                None,
            )
            .await;
            sqlx::query(
                "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, created_at, created_by) VALUES ($1, NULL, $2, 'test-model', 'pending', NOW(), $3)",
            )
            .bind(request_id)
            .bind(template_id)
            .bind(owner)
            .execute(&pool)
            .await
            .expect("insert request");
        }

        let personal_resp = app
            .get("/admin/api/v1/batches/requests")
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        personal_resp.assert_status_ok();
        let personal_body: serde_json::Value = personal_resp.json();
        assert_eq!(
            personal_body["total_count"], 1,
            "PM in personal context should see only their own row",
        );

        let org_cookie = format!("dw_active_org={}", org.id);
        let org_resp = app
            .get("/admin/api/v1/batches/requests")
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        org_resp.assert_status_ok();
        let org_body: serde_json::Value = org_resp.json();
        assert_eq!(org_body["total_count"], 1, "PM in org context should see only the org's row",);
        let org_rows = org_body["data"].as_array().expect("data array");
        assert_eq!(org_rows.len(), 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_batch_request_populates_created_by_email(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let auth = add_auth_headers(&user);

        let template_id = uuid::Uuid::new_v4();
        let request_id = uuid::Uuid::new_v4();
        let body = serde_json::json!({"model": "test-model", "messages": [{"role": "user", "content": "hi"}]});

        crate::test::utils::insert_fusillade_template(
            &pool,
            template_id,
            None,
            "test-model",
            "http://test",
            "/v1/chat/completions",
            &serde_json::to_string(&body).unwrap(),
            None,
        )
        .await;

        sqlx::query(
            "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, created_at, created_by) VALUES ($1, NULL, $2, 'test-model', 'pending', NOW(), $3)",
        )
        .bind(request_id)
        .bind(template_id)
        .bind(user.id.to_string())
        .execute(&pool)
        .await
        .expect("insert request");

        let response = app
            .get(&format!("/admin/api/v1/batches/requests/{}", request_id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;

        response.assert_status_ok();
        let body: serde_json::Value = response.json();
        assert_eq!(body["id"], request_id.to_string());
        assert_eq!(body["created_by_email"], user.email);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_responses_surface_api_key_holder_not_just_billing_owner(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let member = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let org = create_test_org(&pool, owner.id).await;
        add_org_member(&pool, org.id, member.id, "member").await;
        let auth = add_auth_headers(&owner);
        let org_cookie = format!("dw_active_org={}", org.id);

        let visible_key_id = uuid::Uuid::new_v4();
        let child_key_id = uuid::Uuid::new_v4();
        sqlx::query(
            "INSERT INTO api_keys (id, name, secret_hash, secret_prefix, purpose, user_id, created_by, hidden) VALUES ($1, 'Member key', sha256(convert_to($2, 'UTF8')), left($2, 8), 'realtime', $3, $4, false)",
        )
        .bind(visible_key_id)
        .bind(format!("sk-visible-{}", visible_key_id.simple()))
        .bind(org.id)
        .bind(member.id)
        .execute(&pool)
        .await
        .expect("insert visible key");
        sqlx::query(
            "INSERT INTO api_keys (id, name, secret_hash, secret_prefix, purpose, user_id, created_by, hidden, parent_api_key_id) VALUES ($1, 'Internal batch key (cap scope)', sha256(convert_to($2, 'UTF8')), left($2, 8), 'batch', $3, $4, true, $5)",
        )
        .bind(child_key_id)
        .bind(format!("sk-child-{}", child_key_id.simple()))
        .bind(org.id)
        .bind(member.id)
        .bind(visible_key_id)
        .execute(&pool)
        .await
        .expect("insert child key");

        let template_id = uuid::Uuid::new_v4();
        let request_id = uuid::Uuid::new_v4();
        let body = serde_json::json!({"model": "test-model", "messages": [{"role": "user", "content": "hi"}]});
        crate::test::utils::insert_fusillade_template(
            &pool,
            template_id,
            None,
            "test-model",
            "http://test",
            "/v1/chat/completions",
            &serde_json::to_string(&body).unwrap(),
            None,
        )
        .await;
        sqlx::query(
            "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, created_at, created_by) VALUES ($1, NULL, $2, 'test-model', 'pending', NOW(), $3)",
        )
        .bind(request_id)
        .bind(template_id)
        .bind(org.id.to_string())
        .execute(&pool)
        .await
        .expect("insert request");

        sqlx::query(
            "INSERT INTO http_analytics (instance_id, correlation_id, timestamp, method, uri, fusillade_request_id, api_key_id) VALUES ($1, 1, NOW(), 'POST', '/ai/v1/responses', $2, $3)",
        )
        .bind(uuid::Uuid::new_v4())
        .bind(request_id)
        .bind(child_key_id)
        .execute(&pool)
        .await
        .expect("insert analytics");

        let expect_attribution = |row: &serde_json::Value| {
            assert_eq!(
                row["created_by_email"], org.email,
                "billing owner stays the org — this is the field that reads as the owner's own address",
            );
            assert_eq!(
                row["api_key_id"],
                visible_key_id.to_string(),
                "cap-scope child must resolve to the visible key its holder manages",
            );
            assert_eq!(row["api_key_name"], "Member key");
            assert_eq!(
                row["api_key_holder_email"], member.email,
                "the holder is the member the key was issued to, not the owner who minted it",
            );
        };

        let list = app
            .get("/admin/api/v1/batches/requests")
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        list.assert_status_ok();
        let list_body: serde_json::Value = list.json();
        let rows = list_body["data"].as_array().expect("data array");
        assert_eq!(rows.len(), 1);
        expect_attribution(&rows[0]);

        let detail = app
            .get(&format!("/admin/api/v1/batches/requests/{}", request_id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        detail.assert_status_ok();
        expect_attribution(&detail.json::<serde_json::Value>());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_response_without_analytics_row_has_null_api_key_fields(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let auth = add_auth_headers(&user);

        let template_id = uuid::Uuid::new_v4();
        let request_id = uuid::Uuid::new_v4();
        let body = serde_json::json!({"model": "test-model", "messages": [{"role": "user", "content": "hi"}]});
        crate::test::utils::insert_fusillade_template(
            &pool,
            template_id,
            None,
            "test-model",
            "http://test",
            "/v1/chat/completions",
            &serde_json::to_string(&body).unwrap(),
            None,
        )
        .await;
        sqlx::query(
            "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, created_at, created_by) VALUES ($1, NULL, $2, 'test-model', 'pending', NOW(), $3)",
        )
        .bind(request_id)
        .bind(template_id)
        .bind(user.id.to_string())
        .execute(&pool)
        .await
        .expect("insert request");

        let response = app
            .get(&format!("/admin/api/v1/batches/requests/{}", request_id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        response.assert_status_ok();
        let body: serde_json::Value = response.json();
        assert!(body["api_key_id"].is_null());
        assert!(body["api_key_name"].is_null());
        assert!(body["api_key_holder_email"].is_null());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_batch_request_removes_row(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let auth = add_auth_headers(&user);

        let template_id = uuid::Uuid::new_v4();
        let request_id = uuid::Uuid::new_v4();
        let body = serde_json::json!({"model": "test-model", "messages": [{"role": "user", "content": "hi"}]});

        crate::test::utils::insert_fusillade_template(
            &pool,
            template_id,
            None,
            "test-model",
            "http://test",
            "/v1/chat/completions",
            &serde_json::to_string(&body).unwrap(),
            None,
        )
        .await;

        sqlx::query(
            "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, created_at, created_by) VALUES ($1, NULL, $2, 'test-model', 'pending', NOW(), $3)",
        )
        .bind(request_id)
        .bind(template_id)
        .bind(user.id.to_string())
        .execute(&pool)
        .await
        .expect("insert request");

        let response = app
            .delete(&format!("/admin/api/v1/batches/requests/{}", request_id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::NO_CONTENT);

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM fusillade.requests WHERE id = $1")
            .bind(request_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count.0, 0, "fusillade row should be hard-deleted");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_batch_request_404_for_other_users_row(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let intruder = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let intruder_auth = add_auth_headers(&intruder);

        let template_id = uuid::Uuid::new_v4();
        let request_id = uuid::Uuid::new_v4();
        let body = serde_json::json!({"model": "test-model", "messages": []});

        crate::test::utils::insert_fusillade_template(
            &pool,
            template_id,
            None,
            "test-model",
            "http://test",
            "/v1/chat/completions",
            &serde_json::to_string(&body).unwrap(),
            None,
        )
        .await;

        sqlx::query(
            "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, created_at, created_by) VALUES ($1, NULL, $2, 'test-model', 'pending', NOW(), $3)",
        )
        .bind(request_id)
        .bind(template_id)
        .bind(owner.id.to_string())
        .execute(&pool)
        .await
        .expect("insert request");

        let response = app
            .delete(&format!("/admin/api/v1/batches/requests/{}", request_id))
            .add_header(&intruder_auth[0].0, &intruder_auth[0].1)
            .add_header(&intruder_auth[1].0, &intruder_auth[1].1)
            .await;
        response.assert_status_not_found();

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM fusillade.requests WHERE id = $1")
            .bind(request_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count.0, 1, "row should remain — ownership check should reject");
    }
}
