
use axum::{
    extract::{Query, State},
    response::Json,
};
use moka::future::Cache;
use once_cell::sync::Lazy;
use sqlx_pool_router::PoolProvider;
use uuid::Uuid;

use crate::{
    AppState,
    api::models::{
        requests::{
            AggregateRequestsQuery, HttpAnalyticsFilter, ListAnalyticsResponse, ListRequestsQuery, ModelUserUsageResponse,
            RequestsAggregateResponse, UsageDateQuery, UserBatchUsageResponse,
        },
        users::CurrentUser,
    },
    auth::permissions::{RequiresPermission, operation, resource},
    db::handlers::analytics::{
        get_model_user_usage, get_realtime_tariffs, get_requests_aggregate, get_user_batch_count_for_range, get_user_batch_counts,
        get_user_model_breakdown, get_user_model_breakdown_for_range, list_http_analytics, refresh_user_model_usage_daily,
    },
    errors::Error,
};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use utoipa::IntoParams;

type UsageCacheKey = (Uuid, Option<i64>, Option<i64>);

static USAGE_CACHE: Lazy<Cache<UsageCacheKey, UserBatchUsageResponse>> = Lazy::new(|| {
    Cache::builder()
        .max_capacity(5_000)
        .time_to_live(std::time::Duration::from_secs(3600))
        .build()
});

#[utoipa::path(
    get,
    path = "/admin/api/v1/requests",
    params(ListRequestsQuery),
    responses(
        (status = 200, description = "List of analytics entries", body = ListAnalyticsResponse),
        (status = 400, description = "Invalid query parameters"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "requests",
)]
#[tracing::instrument(skip_all)]
pub async fn list_requests<P: PoolProvider>(
    Query(query): Query<ListRequestsQuery>,
    State(state): State<AppState<P>>,
    _: RequiresPermission<resource::Requests, operation::ReadAll>,
) -> Result<Json<ListAnalyticsResponse>, Error> {
    let (skip, limit) = query.pagination.params();

    let filter = HttpAnalyticsFilter {
        method: query.method,
        uri_pattern: query.uri_pattern,
        status_code: query.status_code,
        status_code_min: query.status_code_min,
        status_code_max: query.status_code_max,
        min_duration_ms: query.min_duration_ms,
        max_duration_ms: query.max_duration_ms,
        timestamp_after: query.timestamp_after,
        timestamp_before: query.timestamp_before,
        model: query.model,
        fusillade_batch_id: query.fusillade_batch_id,
        custom_id: query.custom_id,
    };

    let entries = list_http_analytics(&state.db.read(), skip, limit, query.order_desc.unwrap_or(true), filter).await?;

    Ok(Json(ListAnalyticsResponse { entries }))
}

#[utoipa::path(
    get,
    path = "/admin/api/v1/requests/aggregate",
    params(AggregateRequestsQuery),
    responses(
        (status = 200, description = "Aggregated request metrics", body = RequestsAggregateResponse),
        (status = 500, description = "Internal server error"),
    ),
    tag = "requests",
)]
#[tracing::instrument(skip_all)]
pub async fn aggregate_requests<P: PoolProvider>(
    Query(query): Query<AggregateRequestsQuery>,
    State(state): State<AppState<P>>,
    _: RequiresPermission<resource::Analytics, operation::ReadAll>,
) -> Result<Json<RequestsAggregateResponse>, Error> {
    let now = chrono::Utc::now();
    let time_range_start = query.timestamp_after.unwrap_or_else(|| now - chrono::Duration::hours(24));
    let time_range_end = query.timestamp_before.unwrap_or(now);
    let model_filter = query.model.as_deref();

    let response = get_requests_aggregate(&state.db.read(), time_range_start, time_range_end, model_filter).await?;

    Ok(Json(response))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct AggregateByUserQuery {
    pub model: Option<String>,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
}

#[utoipa::path(
    get,
    path = "/admin/api/v1/requests/aggregate-by-user",
    params(AggregateByUserQuery),
    responses(
        (status = 200, description = "User aggregated request metrics", body = ModelUserUsageResponse),
        (status = 400, description = "Model parameter is required"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "requests",
)]
#[tracing::instrument(skip_all)]
pub async fn aggregate_by_user<P: PoolProvider>(
    Query(query): Query<AggregateByUserQuery>,
    State(state): State<AppState<P>>,
    _: RequiresPermission<resource::Analytics, operation::ReadAll>,
) -> Result<Json<ModelUserUsageResponse>, Error> {
    let model_alias = query.model.ok_or_else(|| Error::BadRequest {
        message: "Model parameter is required".to_string(),
    })?;

    let end_date = query.end_date.unwrap_or_else(Utc::now);
    let start_date = query.start_date.unwrap_or_else(|| end_date - Duration::hours(24));

    let usage_data = get_model_user_usage(&state.db.read(), &model_alias, start_date, end_date).await?;

    Ok(Json(usage_data))
}

#[utoipa::path(
    get,
    path = "/admin/api/v1/usage",
    params(UsageDateQuery),
    responses(
        (status = 200, description = "User batch usage metrics", body = UserBatchUsageResponse),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Caller is not allowed to view this user's usage"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "usage",
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_usage<P: PoolProvider>(
    Query(query): Query<UsageDateQuery>,
    State(state): State<AppState<P>>,
    current_user: CurrentUser,
) -> Result<Json<UserBatchUsageResponse>, Error> {
    let has_dates = query.start_date.is_some() || query.end_date.is_some();
    let refresh = query.refresh.unwrap_or(false);

    let target_user_id = resolve_usage_target(&state, &current_user, query.user_id).await?;

    let (cache_key, use_cache) = if has_dates {
        let end_date = query.end_date.unwrap_or_else(Utc::now);
        let start_date = query.start_date.unwrap_or_else(|| end_date - Duration::days(180));
        let span = end_date - start_date;
        let truncate = |dt: DateTime<Utc>| dt.date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
        (
            (target_user_id, Some(truncate(start_date)), Some(truncate(end_date))),
            span.num_days() >= 30,
        )
    } else {
        ((target_user_id, None, None), true)
    };

    if refresh {
        USAGE_CACHE.invalidate(&cache_key).await;
    } else if use_cache && let Some(cached) = USAGE_CACHE.get(&cache_key).await {
        return Ok(Json(cached));
    }

    let (batch_count, by_model) = if has_dates {
        let end_date = query.end_date.unwrap_or_else(Utc::now);
        let start = query.start_date.unwrap_or_else(|| end_date - Duration::days(180));
        let max_start = end_date - Duration::days(180);
        let start_date = if start < max_start { max_start } else { start };

        let db = state.db.read();
        tokio::try_join!(
            get_user_batch_count_for_range(&db, target_user_id, start_date, end_date),
            get_user_model_breakdown_for_range(&db, target_user_id, start_date, end_date),
        )?
    } else {
        if refresh {
            refresh_user_model_usage_daily(&state.db.write()).await?;
        }
        let db = state.db.read();
        let (batch_stats, by_model) = tokio::try_join!(
            get_user_batch_counts(&db, target_user_id),
            get_user_model_breakdown(&db, target_user_id),
        )?;
        (batch_stats.0, by_model)
    };

    let aliases: Vec<String> = by_model.iter().map(|entry| entry.model.clone()).collect();
    let tariffs = get_realtime_tariffs(&state.db.read(), target_user_id, &aliases).await?;

    let total_cost = by_model
        .iter()
        .fold(Decimal::ZERO, |acc, e| acc + e.cost.parse::<Decimal>().unwrap_or(Decimal::ZERO))
        .to_string();
    let total_requests: i64 = by_model.iter().map(|e| e.request_count).sum();
    let avg_requests_per_batch = if batch_count > 0 {
        total_requests as f64 / batch_count as f64
    } else {
        0.0
    };

    let mut total_input_tokens: i64 = 0;
    let mut total_output_tokens: i64 = 0;
    let mut total_request_count: i64 = 0;
    let mut estimated_realtime_cost = Decimal::ZERO;
    for entry in &by_model {
        total_input_tokens += entry.input_tokens;
        total_output_tokens += entry.output_tokens;
        total_request_count += entry.request_count;
        if let Some(&(input_price, output_price)) = tariffs.get(&entry.model) {
            estimated_realtime_cost += Decimal::from(entry.input_tokens) * input_price + Decimal::from(entry.output_tokens) * output_price;
        }
    }

    let usage = UserBatchUsageResponse {
        total_input_tokens,
        total_output_tokens,
        total_request_count,
        total_batch_count: batch_count,
        avg_requests_per_batch,
        total_cost,
        estimated_realtime_cost: estimated_realtime_cost.to_string(),
        by_model,
    };

    if use_cache {
        USAGE_CACHE.insert(cache_key, usage.clone()).await;
    }

    Ok(Json(usage))
}

async fn resolve_usage_target<P: PoolProvider>(
    state: &AppState<P>,
    current_user: &CurrentUser,
    requested: Option<crate::types::UserId>,
) -> Result<crate::types::UserId, Error> {
    let default_target = current_user.active_organization.unwrap_or(current_user.id);
    let Some(requested) = requested else {
        return Ok(default_target);
    };

    if requested == current_user.id {
        return Ok(requested);
    }

    let Some(active_org) = current_user.active_organization else {
        return Err(unauthorized_usage_target_error(requested));
    };

    let mut conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = crate::db::handlers::Organizations::new(&mut conn);
    let actor_role = repo.get_user_org_role(current_user.id, active_org).await?;
    if actor_role.is_none() {
        return Err(unauthorized_usage_target_error(requested));
    }

    if requested == active_org {
        return Ok(requested);
    }

    let target_role = repo.get_user_org_role(requested, active_org).await?;
    if matches!(actor_role.as_deref(), Some("owner" | "admin")) && target_role.is_some() {
        return Ok(requested);
    }

    Err(unauthorized_usage_target_error(requested))
}

fn unauthorized_usage_target_error(requested: crate::types::UserId) -> Error {
    Error::InsufficientPermissions {
        required: crate::types::Permission::Allow(crate::types::Resource::Requests, crate::types::Operation::ReadAll),
        action: crate::types::Operation::ReadAll,
        resource: format!("Usage for user {requested}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{api::models::requests::ListAnalyticsResponse, api::models::users::Role, test::utils::*};
    use chrono::{Duration, Utc};
    use sqlx::PgPool;
    use std::sync::atomic::{AtomicI64, Ordering};

    static CORRELATION_ID_COUNTER: AtomicI64 = AtomicI64::new(1);

    struct TestAnalyticsData<'a> {
        timestamp: chrono::DateTime<chrono::Utc>,
        model: &'a str,
        status_code: i32,
        duration_ms: f64,
        prompt_tokens: i64,
        completion_tokens: i64,
        fusillade_batch_id: Option<uuid::Uuid>,
    }

    async fn insert_test_analytics(pool: &PgPool, data: TestAnalyticsData<'_>) {
        use uuid::Uuid;

        let correlation_id = CORRELATION_ID_COUNTER.fetch_add(1, Ordering::SeqCst);

        sqlx::query!(
            r#"
            INSERT INTO http_analytics (
                instance_id, correlation_id, timestamp, uri, method, status_code, duration_ms,
                model, prompt_tokens, completion_tokens, total_tokens, fusillade_batch_id
            ) VALUES ($1, $2, $3, '/ai/chat/completions', 'POST', $4, $5, $6, $7, $8, $9, $10)
            "#,
            Uuid::new_v4(),
            correlation_id,
            data.timestamp,
            data.status_code,
            data.duration_ms as i64,
            data.model,
            data.prompt_tokens,
            data.completion_tokens,
            data.prompt_tokens + data.completion_tokens,
            data.fusillade_batch_id
        )
        .execute(pool)
        .await
        .expect("Failed to insert test analytics data");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_requests_unauthorized(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let response = app
            .get("/admin/api/v1/requests")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::FORBIDDEN);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_requests_success_empty(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::RequestViewer).await;

        let response = app
            .get("/admin/api/v1/requests")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let list_response: ListAnalyticsResponse = response.json();
        assert!(list_response.entries.is_empty());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_requests_with_data(pool: PgPool) {
        let base_time = Utc::now() - Duration::hours(1);
        insert_test_analytics(
            &pool,
            TestAnalyticsData {
                timestamp: base_time,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 100.0,
                prompt_tokens: 50,
                completion_tokens: 25,
                fusillade_batch_id: None,
            },
        )
        .await;
        insert_test_analytics(
            &pool,
            TestAnalyticsData {
                timestamp: base_time,
                model: "claude-3",
                status_code: 200,
                duration_ms: 150.0,
                prompt_tokens: 75,
                completion_tokens: 35,
                fusillade_batch_id: None,
            },
        )
        .await;

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::RequestViewer).await;

        let response = app
            .get("/admin/api/v1/requests")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let list_response: ListAnalyticsResponse = response.json();
        assert_eq!(list_response.entries.len(), 2);

        for entry in &list_response.entries {
            assert!(entry.model.is_some());
            assert_eq!(entry.status_code, Some(200));
            assert!(entry.duration_ms.is_some());
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_requests_with_model_filter(pool: PgPool) {
        let base_time = Utc::now() - Duration::hours(1);
        insert_test_analytics(
            &pool,
            TestAnalyticsData {
                timestamp: base_time,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 100.0,
                prompt_tokens: 50,
                completion_tokens: 25,
                fusillade_batch_id: None,
            },
        )
        .await;
        insert_test_analytics(
            &pool,
            TestAnalyticsData {
                timestamp: base_time,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 120.0,
                prompt_tokens: 60,
                completion_tokens: 30,
                fusillade_batch_id: None,
            },
        )
        .await;
        insert_test_analytics(
            &pool,
            TestAnalyticsData {
                timestamp: base_time,
                model: "claude-3",
                status_code: 200,
                duration_ms: 150.0,
                prompt_tokens: 75,
                completion_tokens: 35,
                fusillade_batch_id: None,
            },
        )
        .await;

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::RequestViewer).await;

        let response = app
            .get("/admin/api/v1/requests?model=gpt-4")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let list_response: ListAnalyticsResponse = response.json();
        assert_eq!(list_response.entries.len(), 2);
        assert!(list_response.entries.iter().all(|e| e.model.as_deref() == Some("gpt-4")));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_requests_with_batch_filter(pool: PgPool) {
        use uuid::Uuid;

        let batch_id = Uuid::new_v4();
        let other_batch_id = Uuid::new_v4();
        let base_time = Utc::now() - Duration::hours(1);

        insert_test_analytics(
            &pool,
            TestAnalyticsData {
                timestamp: base_time,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 100.0,
                prompt_tokens: 50,
                completion_tokens: 25,
                fusillade_batch_id: Some(batch_id),
            },
        )
        .await;
        insert_test_analytics(
            &pool,
            TestAnalyticsData {
                timestamp: base_time,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 120.0,
                prompt_tokens: 60,
                completion_tokens: 30,
                fusillade_batch_id: Some(batch_id),
            },
        )
        .await;
        insert_test_analytics(
            &pool,
            TestAnalyticsData {
                timestamp: base_time,
                model: "claude-3",
                status_code: 200,
                duration_ms: 150.0,
                prompt_tokens: 75,
                completion_tokens: 35,
                fusillade_batch_id: Some(other_batch_id),
            },
        )
        .await;
        insert_test_analytics(
            &pool,
            TestAnalyticsData {
                timestamp: base_time,
                model: "claude-3",
                status_code: 200,
                duration_ms: 160.0,
                prompt_tokens: 80,
                completion_tokens: 40,
                fusillade_batch_id: None,
            },
        )
        .await;

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::RequestViewer).await;

        let response = app
            .get(&format!("/admin/api/v1/requests?fusillade_batch_id={}", batch_id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let list_response: ListAnalyticsResponse = response.json();
        assert_eq!(list_response.entries.len(), 2);
        assert!(list_response.entries.iter().all(|e| e.fusillade_batch_id == Some(batch_id)));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_aggregate_requests_success(pool: PgPool) {
        let base_time = Utc::now() - Duration::hours(1);
        insert_test_analytics(
            &pool,
            TestAnalyticsData {
                timestamp: base_time,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 100.0,
                prompt_tokens: 50,
                completion_tokens: 25,
                fusillade_batch_id: None,
            },
        )
        .await;

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let response = app
            .get("/admin/api/v1/requests/aggregate")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let aggregate_response: RequestsAggregateResponse = response.json();
        assert_eq!(aggregate_response.total_requests, 1);
        assert!(aggregate_response.model.is_none());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_aggregate_requests_with_model_filter(pool: PgPool) {
        let base_time = Utc::now() - Duration::hours(1);
        insert_test_analytics(
            &pool,
            TestAnalyticsData {
                timestamp: base_time,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 100.0,
                prompt_tokens: 50,
                completion_tokens: 25,
                fusillade_batch_id: None,
            },
        )
        .await;
        insert_test_analytics(
            &pool,
            TestAnalyticsData {
                timestamp: base_time,
                model: "claude-3",
                status_code: 200,
                duration_ms: 150.0,
                prompt_tokens: 75,
                completion_tokens: 35,
                fusillade_batch_id: None,
            },
        )
        .await;

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let response = app
            .get("/admin/api/v1/requests/aggregate?model=gpt-4")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let aggregate_response: RequestsAggregateResponse = response.json();
        assert_eq!(aggregate_response.total_requests, 1);
        assert_eq!(aggregate_response.model, Some("gpt-4".to_string()));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_aggregate_requests_unauthorized(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let response = app
            .get("/admin/api/v1/requests/aggregate")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn test_list_requests_query_default() {
        let query = ListRequestsQuery::default();
        assert_eq!(query.pagination.skip(), 0);
        assert_eq!(query.pagination.limit(), 10);
        assert_eq!(query.order_desc, Some(true));
        assert!(query.method.is_none());
        assert!(query.uri_pattern.is_none());
        assert!(query.status_code.is_none());
        assert!(query.model.is_none());
        assert!(query.fusillade_batch_id.is_none());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_standard_user_cannot_access_requests(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let standard_user = create_test_user(&pool, Role::StandardUser).await;

        let response = app
            .get("/admin/api/v1/requests")
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;

        response.assert_status_forbidden();

        let response = app
            .get("/admin/api/v1/requests/aggregate")
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_request_viewer_can_access_monitoring_data(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let request_viewer = create_test_user(&pool, Role::RequestViewer).await;

        let response = app
            .get("/admin/api/v1/requests")
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;

        response.assert_status_ok();
        let _list_response: ListAnalyticsResponse = response.json();

        let response = app
            .get("/admin/api/v1/requests/aggregate")
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;

        response.assert_status_ok();
        let _aggregate_response: RequestsAggregateResponse = response.json();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_cannot_access_raw_requests(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_user(&pool, Role::PlatformManager).await;

        let response = app
            .get("/admin/api/v1/requests")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;

        response.assert_status_forbidden();

        let response = app
            .get("/admin/api/v1/requests/aggregate")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;

        response.assert_status_ok();
    }

    async fn insert_analytics_for_user(pool: &PgPool, user_id: uuid::Uuid, prompt_tokens: i64, completion_tokens: i64) {
        let correlation_id = CORRELATION_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        sqlx::query!(
            r#"
            INSERT INTO http_analytics (
                instance_id, correlation_id, timestamp, uri, method, status_code, duration_ms,
                model, prompt_tokens, completion_tokens, total_tokens, user_id
            ) VALUES ($1, $2, $3, '/ai/chat/completions', 'POST', 200, 100, 'gpt-4', $4, $5, $6, $7)
            "#,
            uuid::Uuid::new_v4(),
            correlation_id,
            chrono::Utc::now() - chrono::Duration::minutes(5),
            prompt_tokens,
            completion_tokens,
            prompt_tokens + completion_tokens,
            user_id,
        )
        .execute(pool)
        .await
        .expect("Failed to insert test analytics row");

        refresh_user_model_usage_daily(pool)
            .await
            .expect("Failed to refresh user_model_usage_daily rollup");
    }

    fn build_usage_url(user_id: Option<uuid::Uuid>) -> String {
        let start = (Utc::now() - Duration::hours(1)).format("%Y-%m-%dT%H:%M:%SZ");
        let end = Utc::now().format("%Y-%m-%dT%H:%M:%SZ");
        match user_id {
            Some(id) => format!("/admin/api/v1/usage?start_date={start}&end_date={end}&user_id={id}"),
            None => format!("/admin/api/v1/usage?start_date={start}&end_date={end}"),
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_usage_defaults_to_active_organization(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let org = crate::test::utils::create_test_org(&pool, user.id).await;

        insert_analytics_for_user(&pool, user.id, 100, 50).await;
        insert_analytics_for_user(&pool, org.id, 1000, 500).await;

        let org_cookie = format!("dw_active_org={}", org.id);
        let auth = add_auth_headers(&user);

        let response = app
            .get(&build_usage_url(None))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;

        response.assert_status_ok();
        let body: UserBatchUsageResponse = response.json();
        assert_eq!(
            body.total_input_tokens, 1000,
            "should return the org's tokens, not the human user's"
        );
        assert_eq!(body.total_output_tokens, 500);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_usage_user_id_filter_to_self(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let org = crate::test::utils::create_test_org(&pool, user.id).await;

        insert_analytics_for_user(&pool, user.id, 100, 50).await;
        insert_analytics_for_user(&pool, org.id, 1000, 500).await;

        let org_cookie = format!("dw_active_org={}", org.id);
        let auth = add_auth_headers(&user);

        let response = app
            .get(&build_usage_url(Some(user.id)))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;

        response.assert_status_ok();
        let body: UserBatchUsageResponse = response.json();
        assert_eq!(body.total_input_tokens, 100);
        assert_eq!(body.total_output_tokens, 50);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_usage_admin_can_filter_to_member(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let member = create_test_user(&pool, Role::StandardUser).await;
        let org = crate::test::utils::create_test_org(&pool, owner.id).await;
        crate::test::utils::add_org_member(&pool, org.id, member.id, "member").await;

        insert_analytics_for_user(&pool, member.id, 25, 25).await;

        let org_cookie = format!("dw_active_org={}", org.id);
        let auth = add_auth_headers(&owner);

        let response = app
            .get(&build_usage_url(Some(member.id)))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;

        response.assert_status_ok();
        let body: UserBatchUsageResponse = response.json();
        assert_eq!(body.total_input_tokens, 25);
        assert_eq!(body.total_output_tokens, 25);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_usage_non_admin_cannot_filter_to_other_member(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let member_a = create_test_user(&pool, Role::StandardUser).await;
        let member_b = create_test_user(&pool, Role::StandardUser).await;
        let org = crate::test::utils::create_test_org(&pool, owner.id).await;
        crate::test::utils::add_org_member(&pool, org.id, member_a.id, "member").await;
        crate::test::utils::add_org_member(&pool, org.id, member_b.id, "member").await;

        let org_cookie = format!("dw_active_org={}", org.id);
        let auth = add_auth_headers(&member_a);

        let response = app
            .get(&build_usage_url(Some(member_b.id)))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;

        response.assert_status(axum::http::StatusCode::FORBIDDEN);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_usage_personal_context_other_user_id_forbidden(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;

        let auth = add_auth_headers(&alice);
        let response = app
            .get(&build_usage_url(Some(bob.id)))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::FORBIDDEN);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_requests_query_parameters(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let request_viewer = create_test_user(&pool, Role::RequestViewer).await;

        let test_queries = vec![
            "limit=10&skip=0",
            "method=POST",
            "status_code=200",
            "status_code_min=200&status_code_max=299",
            "order_desc=true",
            "order_desc=false",
            "model=gpt-4",
        ];

        for query in test_queries {
            let response = app
                .get(&format!("/admin/api/v1/requests?{}", query))
                .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
                .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
                .await;

            response.assert_status_ok();
            let _: ListAnalyticsResponse = response.json();
        }
    }

    async fn insert_test_analytics_for_user(
        pool: &PgPool,
        user_id: uuid::Uuid,
        timestamp: chrono::DateTime<chrono::Utc>,
        model: &str,
        prompt_tokens: i64,
        completion_tokens: i64,
    ) {
        let correlation_id = CORRELATION_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        sqlx::query!(
            r#"
            INSERT INTO http_analytics (
                instance_id, correlation_id, timestamp, uri, method, status_code, duration_ms,
                model, prompt_tokens, completion_tokens, total_tokens, user_id
            ) VALUES ($1, $2, $3, '/ai/chat/completions', 'POST', 200, 100, $4, $5, $6, $7, $8)
            "#,
            uuid::Uuid::new_v4(),
            correlation_id,
            timestamp,
            model,
            prompt_tokens,
            completion_tokens,
            prompt_tokens + completion_tokens,
            user_id,
        )
        .execute(pool)
        .await
        .expect("Failed to insert test analytics data with user_id");

        refresh_user_model_usage_daily(pool)
            .await
            .expect("Failed to refresh user_model_usage_daily rollup");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_usage_pm_in_org_context_scopes_to_org(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;

        let pm_user = create_test_user_with_roles(&pool, vec![Role::PlatformManager, Role::StandardUser, Role::BatchAPIUser]).await;
        let org = create_test_org(&pool, pm_user.id).await;
        let auth = add_auth_headers(&pm_user);

        let one_hour_ago = Utc::now() - Duration::hours(1);
        insert_test_analytics_for_user(&pool, pm_user.id, one_hour_ago, "personal-model", 50, 25).await;
        insert_test_analytics_for_user(&pool, org.id, one_hour_ago, "org-model", 100, 50).await;

        use chrono::SecondsFormat;
        let start = (one_hour_ago - Duration::minutes(1)).to_rfc3339_opts(SecondsFormat::Secs, true);
        let end = (Utc::now() + Duration::minutes(1)).to_rfc3339_opts(SecondsFormat::Secs, true);
        let url = format!("/admin/api/v1/usage?start_date={}&end_date={}", start, end);

        let personal_resp = app
            .get(&url)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        personal_resp.assert_status_ok();
        let personal_body: serde_json::Value = personal_resp.json();
        let personal_models = personal_body["by_model"].as_array().expect("by_model array");
        assert_eq!(
            personal_models.len(),
            1,
            "personal context should see only the human user's analytics",
        );
        assert_eq!(personal_models[0]["model"], "personal-model");

        let org_cookie = format!("dw_active_org={}", org.id);
        let org_resp = app
            .get(&url)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        org_resp.assert_status_ok();
        let org_body: serde_json::Value = org_resp.json();
        let org_models = org_body["by_model"].as_array().expect("by_model array");
        assert_eq!(
            org_models.len(),
            1,
            "org context should see only the org's analytics, not the human user's",
        );
        assert_eq!(org_models[0]["model"], "org-model");
    }
}
