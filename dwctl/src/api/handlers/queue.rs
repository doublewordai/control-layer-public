
use axum::{
    extract::{Query, State},
    response::Json,
};
use fusillade::request::ServiceTierFilter;
use fusillade::{Storage, TrailingDemandCount};
use serde::{Deserialize, Serialize};
use sqlx_pool_router::PoolProvider;
use std::collections::HashMap;
use utoipa::{IntoParams, ToSchema};

use crate::{
    AppState,
    auth::permissions::{RequiresPermission, operation, resource},
    errors::Error,
};

type PendingCountsByModelAndWindow = HashMap<String, HashMap<String, i64>>;

type GroupedDemandByModelAndWindow = HashMap<String, HashMap<String, HashMap<String, HashMap<String, i64>>>>;

#[derive(Debug, Serialize, ToSchema)]
#[serde(untagged)]
pub enum DemandResponse {
    Flat(PendingCountsByModelAndWindow),
    Grouped(GroupedDemandByModelAndWindow),
}

fn parse_service_tiers(raw: Option<&str>) -> Vec<Option<String>> {
    let mut tiers = Vec::new();

    if let Some(raw) = raw {
        for tier in raw.split(',').map(str::trim).filter(|tier| !tier.is_empty()) {
            if tier.eq_ignore_ascii_case("batch") || tier.eq_ignore_ascii_case("null") || tier.eq_ignore_ascii_case("none") {
                tiers.push(None);
            } else {
                tiers.push(Some(tier.to_ascii_lowercase()));
            }
        }
    }

    if tiers.is_empty() {
        tiers.push(None);
    }

    tiers
}

fn parse_demand_duration(raw: &str) -> Option<i64> {
    let s = raw.trim();
    let (s, sign): (&str, i64) = match s.strip_prefix('-') {
        Some(rest) => (rest, -1),
        None => (s, 1),
    };
    let (digits, mult): (&str, i64) = if let Some(d) = s.strip_suffix('h') {
        (d, 3600)
    } else if let Some(d) = s.strip_suffix('m') {
        (d, 60)
    } else {
        (s.strip_suffix('s')?, 1)
    };
    let n: i64 = digits.parse().ok()?;
    if n < 0 {
        return None;
    }
    n.checked_mul(mult)?.checked_mul(sign)
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct DemandQuery {
    pub window: String,
    pub service_tiers: Option<String>,
    pub group_by: Option<String>,
}

fn parse_demand_window(raw: &str) -> Result<Option<(String, Option<i64>, i64)>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let (start_secs, end_secs) = match trimmed.split_once(':') {
        Some((start, end)) => {
            let s = parse_demand_duration(start).ok_or_else(|| format!("malformed window start in {:?}", trimmed))?;
            let e = parse_demand_duration(end).ok_or_else(|| format!("malformed window end in {:?}", trimmed))?;
            if s >= e {
                return Err(format!("window start must be before end in {:?}", trimmed));
            }
            (Some(s), e)
        }
        None => {
            let e = parse_demand_duration(trimmed).ok_or_else(|| format!("malformed window {:?}", trimmed))?;
            if e < 0 { (Some(e), 0) } else { (None, e) }
        }
    };
    Ok(Some((trimmed.to_string(), start_secs, end_secs)))
}

#[utoipa::path(
    get,
    path = "/admin/api/v1/monitoring/demand",
    params(DemandQuery),
    responses(
        (status = 200, description = "Demand counts by model and window: `model -> window -> count`, or `model -> window -> tier -> outcome -> count` with `group_by=service_tier`", body = DemandResponse),
        (status = 400, description = "Missing or malformed window parameter"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "monitoring",
)]
#[tracing::instrument(skip_all)]
pub async fn get_demand<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Query(params): Query<DemandQuery>,
    _: RequiresPermission<resource::System, operation::ReadAll>,
) -> Result<Json<DemandResponse>, Error> {
    let grouped = match params.group_by.as_deref() {
        None => false,
        Some("service_tier") => true,
        Some(other) => {
            return Err(Error::BadRequest {
                message: format!("unsupported group_by {:?}: only `service_tier` is supported", other),
            });
        }
    };

    let windows: Vec<(String, Option<i64>, i64)> = params
        .window
        .split(',')
        .map(parse_demand_window)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|message| Error::BadRequest { message })?
        .into_iter()
        .flatten()
        .collect();

    if windows.is_empty() {
        return Err(Error::BadRequest {
            message: "window query parameter must list at least one window (e.g. `window=1h,24h` or `window=0s:1h,1h:24h`)".to_string(),
        });
    }

    let trailing_windows: Vec<(String, i64, i64)> = windows
        .iter()
        .filter_map(|(label, start, end)| match start {
            Some(s) if *s < 0 => Some((label.clone(), *s, (*end).min(0))),
            _ => None,
        })
        .collect();

    let states = vec!["pending".to_string(), "claimed".to_string(), "processing".to_string()];
    let model_filter: Vec<String> = Vec::new();
    let service_tiers = parse_service_tiers(params.service_tiers.as_deref());

    if !grouped {
        let service_tier_filter = ServiceTierFilter::Include(service_tiers);

        let mut counts = state
            .request_manager
            .get_pending_request_counts_by_model_and_window(&windows, &states, &model_filter, &service_tier_filter, false)
            .await
            .map_err(|e| Error::Internal {
                operation: format!("get demand by window: {}", e),
            })?;

        if !trailing_windows.is_empty() {
            let trailing = state
                .request_manager
                .get_completed_request_counts_by_model_and_window(&trailing_windows, &model_filter, &service_tier_filter)
                .await
                .map_err(|e| Error::Internal {
                    operation: format!("get trailing demand by window: {}", e),
                })?;
            for row in trailing {
                *counts.entry(row.model).or_default().entry(row.window_label).or_insert(0) += row.count;
            }
        }

        return Ok(Json(DemandResponse::Flat(counts)));
    }

    let mut cube: GroupedDemandByModelAndWindow = HashMap::new();
    let pending: Vec<TrailingDemandCount> = state
        .request_manager
        .get_pending_request_counts_by_model_window_and_tier(
            &windows,
            &states,
            &model_filter,
            &ServiceTierFilter::Include(service_tiers.clone()),
            false,
        )
        .await
        .map_err(|e| Error::Internal {
            operation: format!("get demand by window and tier: {}", e),
        })?;
    for row in pending {
        let tier_key = row.service_tier.as_deref().unwrap_or("batch").to_string();
        cube.entry(row.model)
            .or_default()
            .entry(row.window_label)
            .or_default()
            .entry(tier_key)
            .or_default()
            .insert(row.outcome, row.count);
    }

    if !trailing_windows.is_empty() {
        let trailing: Vec<TrailingDemandCount> = state
            .request_manager
            .get_completed_request_counts_by_model_and_window(&trailing_windows, &model_filter, &ServiceTierFilter::Include(service_tiers))
            .await
            .map_err(|e| Error::Internal {
                operation: format!("get trailing demand by window: {}", e),
            })?;
        for row in trailing {
            let tier_key = row.service_tier.as_deref().unwrap_or("batch").to_string();
            cube.entry(row.model)
                .or_default()
                .entry(row.window_label)
                .or_default()
                .entry(tier_key)
                .or_default()
                .insert(row.outcome, row.count);
        }
    }

    Ok(Json(DemandResponse::Grouped(cube)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::models::users::Role;
    use crate::test::utils::*;
    use axum_test::TestServer;
    use sqlx::PgPool;

    #[dwctl_test_macros::test]
    async fn test_demand_endpoint_requires_system_permission(pool: sqlx::PgPool) {
        let (server, _bg): (TestServer, _) = create_test_app(pool.clone(), false).await;

        let standard_user = create_test_user(&pool, Role::StandardUser).await;
        let response = server
            .get("/admin/api/v1/monitoring/demand?window=1h,24h")
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::FORBIDDEN);

        let platform_manager = create_test_user(&pool, Role::PlatformManager).await;
        let response = server
            .get("/admin/api/v1/monitoring/demand?window=1h,24h")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;
        response.assert_status_ok();
    }

    #[dwctl_test_macros::test]
    async fn test_demand_returns_empty_when_no_requests(pool: PgPool) {
        let (server, _bg): (TestServer, _) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;

        let response = server
            .get("/admin/api/v1/monitoring/demand?window=1h,24h")
            .add_header(&add_auth_headers(&admin)[0].0, &add_auth_headers(&admin)[0].1)
            .add_header(&add_auth_headers(&admin)[1].0, &add_auth_headers(&admin)[1].1)
            .await;

        response.assert_status_ok();
        let counts: HashMap<String, HashMap<String, i64>> = response.json();

        assert_eq!(counts.len(), 0, "Should have no pending requests");
    }

    #[dwctl_test_macros::test]
    async fn test_demand_defaults_to_batch_tier_only(pool: PgPool) {
        use fusillade::{BatchInput, RequestTemplateInput, Storage};
        use sqlx::postgres::PgConnectOptions;
        use sqlx_pool_router::TestDbPools;

        let (server, _bg): (TestServer, _) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;

        let base_opts: PgConnectOptions = pool.connect_options().as_ref().clone();
        let fusillade_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .min_connections(0)
            .connect_with(base_opts.options([("search_path", "fusillade")]))
            .await
            .expect("Failed to create fusillade pool");
        let fusillade_pools = TestDbPools::new(fusillade_pool.clone()).await.expect("TestDbPools");
        let request_manager = fusillade_arsenal::PostgresRequestManager::new(fusillade_pools, Default::default());

        let model = "test-model";
        let mut batch_ids = Vec::new();
        for completion_window in ["24h", "1h"] {
            let template = RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/chat/completions".to_string(),
                body: r#"{"input":"x"}"#.to_string(),
                model: model.to_string(),
            };
            let file_id = request_manager
                .create_file(format!("queue-test-{completion_window}"), None, vec![template])
                .await
                .expect("create_file");
            let batch = request_manager
                .create_batch(BatchInput {
                    file_id,
                    endpoint: "/v1/chat/completions".to_string(),
                    completion_window: completion_window.to_string(),
                    metadata: None,
                    created_by: None,
                    api_key_id: None,
                    total_requests: None,
                })
                .await
                .expect("create_batch");
            batch_ids.push(batch.id.0);
        }

        request_manager
            .create_realtime(fusillade::CreateRealtimeInput {
                request_id: uuid::Uuid::new_v4(),
                body: r#"{"input":"x"}"#.to_string(),
                model: model.to_string(),
                endpoint: "http://localhost".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "queue-user".to_string(),
            })
            .await
            .expect("create_realtime");

        for batch_id in &batch_ids {
            sqlx::query("UPDATE batches SET expires_at = NOW() + interval '30 minutes' WHERE id = $1")
                .bind(batch_id)
                .execute(&fusillade_pool)
                .await
                .expect("pin expires_at");
        }

        let response = server
            .get("/admin/api/v1/monitoring/demand?window=1h,24h")
            .add_header(&add_auth_headers(&admin)[0].0, &add_auth_headers(&admin)[0].1)
            .add_header(&add_auth_headers(&admin)[1].0, &add_auth_headers(&admin)[1].1)
            .await;

        response.assert_status_ok();
        let counts: HashMap<String, HashMap<String, i64>> = response.json();

        let model_counts = counts
            .get(model)
            .unwrap_or_else(|| panic!("expected '{model}' in response, got {counts:?}"));
        let count_24h = *model_counts.get("24h").unwrap_or(&0);
        assert_eq!(
            count_24h, 1,
            "expected only the batch-tier request within 24h; got {count_24h} ({model_counts:?})"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_demand_service_tiers_query_includes_requested_tiers(pool: PgPool) {
        use fusillade::{BatchInput, RequestTemplateInput, Storage};
        use sqlx::postgres::PgConnectOptions;
        use sqlx_pool_router::TestDbPools;

        let (server, _bg): (TestServer, _) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;

        let base_opts: PgConnectOptions = pool.connect_options().as_ref().clone();
        let fusillade_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .min_connections(0)
            .connect_with(base_opts.options([("search_path", "fusillade")]))
            .await
            .expect("Failed to create fusillade pool");
        let fusillade_pools = TestDbPools::new(fusillade_pool.clone()).await.expect("TestDbPools");
        let request_manager = fusillade_arsenal::PostgresRequestManager::new(fusillade_pools, Default::default());

        let model = "query-tier-model";
        let mut batch_ids = Vec::new();
        for completion_window in ["24h", "1h"] {
            let template = RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/chat/completions".to_string(),
                body: r#"{"input":"x"}"#.to_string(),
                model: model.to_string(),
            };
            let file_id = request_manager
                .create_file(format!("queue-query-test-{completion_window}"), None, vec![template])
                .await
                .expect("create_file");
            let batch = request_manager
                .create_batch(BatchInput {
                    file_id,
                    endpoint: "/v1/chat/completions".to_string(),
                    completion_window: completion_window.to_string(),
                    metadata: None,
                    created_by: None,
                    api_key_id: None,
                    total_requests: None,
                })
                .await
                .expect("create_batch");
            batch_ids.push(batch.id.0);
        }

        request_manager
            .create_realtime(fusillade::CreateRealtimeInput {
                request_id: uuid::Uuid::new_v4(),
                body: r#"{"input":"x"}"#.to_string(),
                model: model.to_string(),
                endpoint: "http://localhost".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "queue-user".to_string(),
            })
            .await
            .expect("create_realtime");

        for batch_id in &batch_ids {
            sqlx::query("UPDATE batches SET expires_at = NOW() + interval '30 minutes' WHERE id = $1")
                .bind(batch_id)
                .execute(&fusillade_pool)
                .await
                .expect("pin expires_at");
        }

        let response = server
            .get("/admin/api/v1/monitoring/demand?window=1h,24h&service_tiers=batch,flex")
            .add_header(&add_auth_headers(&admin)[0].0, &add_auth_headers(&admin)[0].1)
            .add_header(&add_auth_headers(&admin)[1].0, &add_auth_headers(&admin)[1].1)
            .await;

        response.assert_status_ok();
        let counts: HashMap<String, HashMap<String, i64>> = response.json();
        let model_counts = counts
            .get(model)
            .unwrap_or_else(|| panic!("expected '{model}' in response, got {counts:?}"));

        assert_eq!(
            *model_counts.get("24h").unwrap_or(&0),
            2,
            "batch + flex should count when explicitly requested, while priority remains excluded"
        );
    }

    #[test]
    fn test_parse_service_tiers_defaults_to_batch_tier() {
        assert_eq!(parse_service_tiers(None), vec![None]);
        assert_eq!(parse_service_tiers(Some("")), vec![None]);
        assert_eq!(parse_service_tiers(Some("   ")), vec![None]);
    }

    #[test]
    fn test_parse_service_tiers_maps_batch_aliases_to_null_tier() {
        assert_eq!(parse_service_tiers(Some("batch")), vec![None]);
        assert_eq!(parse_service_tiers(Some("null,none")), vec![None, None]);
    }

    #[test]
    fn test_parse_service_tiers_parses_named_tiers() {
        assert_eq!(
            parse_service_tiers(Some("batch, flex, PRIORITY")),
            vec![None, Some("flex".to_string()), Some("priority".to_string())]
        );
    }

    async fn mark_fusillade_request_processing(pool: &PgPool, id: uuid::Uuid) -> sqlx::Result<()> {
        sqlx::query(
            r#"
            UPDATE requests
            SET state = 'processing',
                daemon_id = gen_random_uuid(),
                claimed_at = NOW(),
                started_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(pool)
        .await?;

        Ok(())
    }

    #[dwctl_test_macros::test]
    async fn test_demand_requires_system_permission(pool: sqlx::PgPool) {
        let (server, _bg): (TestServer, _) = create_test_app(pool.clone(), false).await;

        let standard_user = create_test_user(&pool, Role::StandardUser).await;
        let response = server
            .get("/admin/api/v1/monitoring/demand?window=1h,24h")
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::FORBIDDEN);

        let platform_manager = create_test_user(&pool, Role::PlatformManager).await;
        let response = server
            .get("/admin/api/v1/monitoring/demand?window=1h,24h")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;
        response.assert_status_ok();
    }

    #[dwctl_test_macros::test]
    async fn test_demand_rejects_missing_window(pool: PgPool) {
        let (server, _bg): (TestServer, _) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;

        let response = server
            .get("/admin/api/v1/monitoring/demand")
            .add_header(&add_auth_headers(&admin)[0].0, &add_auth_headers(&admin)[0].1)
            .add_header(&add_auth_headers(&admin)[1].0, &add_auth_headers(&admin)[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_demand_rejects_empty_window(pool: PgPool) {
        let (server, _bg): (TestServer, _) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;

        let response = server
            .get("/admin/api/v1/monitoring/demand?window=")
            .add_header(&add_auth_headers(&admin)[0].0, &add_auth_headers(&admin)[0].1)
            .add_header(&add_auth_headers(&admin)[1].0, &add_auth_headers(&admin)[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_demand_accepts_arbitrary_windows(pool: PgPool) {
        let (server, _bg): (TestServer, _) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;

        let response = server
            .get("/admin/api/v1/monitoring/demand?window=15m,2h,1h:72h")
            .add_header(&add_auth_headers(&admin)[0].0, &add_auth_headers(&admin)[0].1)
            .add_header(&add_auth_headers(&admin)[1].0, &add_auth_headers(&admin)[1].1)
            .await;
        response.assert_status_ok();
        let counts: HashMap<String, HashMap<String, i64>> = response.json();
        assert_eq!(counts.len(), 0, "no pending requests exist in a clean database");
    }

    #[dwctl_test_macros::test]
    async fn test_demand_accepts_zero_start(pool: PgPool) {
        let (server, _bg): (TestServer, _) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;

        let response = server
            .get("/admin/api/v1/monitoring/demand?window=0s:1h")
            .add_header(&add_auth_headers(&admin)[0].0, &add_auth_headers(&admin)[0].1)
            .add_header(&add_auth_headers(&admin)[1].0, &add_auth_headers(&admin)[1].1)
            .await;
        response.assert_status_ok();
    }

    #[dwctl_test_macros::test]
    async fn test_demand_accepts_service_tiers(pool: PgPool) {
        let (server, _bg): (TestServer, _) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;

        let response = server
            .get("/admin/api/v1/monitoring/demand?window=1h,24h&service_tiers=none,flex")
            .add_header(&add_auth_headers(&admin)[0].0, &add_auth_headers(&admin)[0].1)
            .add_header(&add_auth_headers(&admin)[1].0, &add_auth_headers(&admin)[1].1)
            .await;
        response.assert_status_ok();
    }

    #[dwctl_test_macros::test]
    async fn test_demand_rejects_malformed_window(pool: PgPool) {
        let (server, _bg): (TestServer, _) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;

        for bad in [
            "window=foo",
            "window=1x",
            "window=1h,bad",
            "window=2h:1h",
            "window=1h:1h",
            "window=-1h:-2h",
            "window=-1h:-1h",
            "window=--1h",
            "window=-",
            "window=1h&group_by=bogus",
        ] {
            let response = server
                .get(&format!("/admin/api/v1/monitoring/demand?{}", bad))
                .add_header(&add_auth_headers(&admin)[0].0, &add_auth_headers(&admin)[0].1)
                .add_header(&add_auth_headers(&admin)[1].0, &add_auth_headers(&admin)[1].1)
                .await;
            response.assert_status(axum::http::StatusCode::BAD_REQUEST);
        }
    }

    #[test]
    fn test_parse_demand_window_signed() {
        assert_eq!(parse_demand_window("1h"), Ok(Some(("1h".to_string(), None, 3600))));
        assert_eq!(parse_demand_window("30m"), Ok(Some(("30m".to_string(), None, 1800))));
        assert_eq!(parse_demand_window("-1h"), Ok(Some(("-1h".to_string(), Some(-3600), 0))));
        assert_eq!(parse_demand_window("-30m"), Ok(Some(("-30m".to_string(), Some(-1800), 0))));
        assert_eq!(parse_demand_window("-1h:1h"), Ok(Some(("-1h:1h".to_string(), Some(-3600), 3600))));
        assert_eq!(
            parse_demand_window("-2h:-1h"),
            Ok(Some(("-2h:-1h".to_string(), Some(-7200), -3600)))
        );
        assert_eq!(parse_demand_window("-1h:0s"), Ok(Some(("-1h:0s".to_string(), Some(-3600), 0))));
        assert!(parse_demand_window("-1h:-2h").is_err());
        assert!(parse_demand_window("-1h:-1h").is_err());
        assert!(parse_demand_window("--1h").is_err());
        assert!(parse_demand_window("-").is_err());
    }

    #[dwctl_test_macros::test]
    async fn test_demand_trailing_windows_and_cube(pool: PgPool) {
        use fusillade::{BatchInput, PersistCompletedRealtimeInput, RequestTemplateInput, Storage};
        use sqlx::postgres::PgConnectOptions;
        use sqlx_pool_router::TestDbPools;

        let (server, _bg): (TestServer, _) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;

        let base_opts: PgConnectOptions = pool.connect_options().as_ref().clone();
        let fusillade_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .min_connections(0)
            .connect_with(base_opts.options([("search_path", "fusillade")]))
            .await
            .expect("Failed to create fusillade pool");
        let fusillade_pools = TestDbPools::new(fusillade_pool.clone()).await.expect("TestDbPools");
        let request_manager = fusillade_arsenal::PostgresRequestManager::new(fusillade_pools, Default::default());

        let model = "trailing-model";

        let template = RequestTemplateInput {
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/v1/chat/completions".to_string(),
            body: r#"{"input":"x"}"#.to_string(),
            model: model.to_string(),
        };
        let file_id = request_manager
            .create_file("trailing-test".to_string(), None, vec![template])
            .await
            .expect("create_file");
        let batch = request_manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .expect("create_batch");
        sqlx::query("UPDATE batches SET expires_at = NOW() + interval '30 minutes' WHERE id = $1")
            .bind(batch.id.0)
            .execute(&fusillade_pool)
            .await
            .expect("pin expires_at");

        let now = chrono::Utc::now();
        let realtime_input = |status_code: u16| PersistCompletedRealtimeInput {
            request_id: uuid::Uuid::new_v4(),
            response_body: r#"{"ok":true}"#.to_string(),
            status_code,
            request_body: r#"{"input":"x"}"#.to_string(),
            model: model.to_string(),
            endpoint: "http://localhost".to_string(),
            method: "POST".to_string(),
            path: "/v1/responses".to_string(),
            created_by: "queue-user".to_string(),
            started_at: now - chrono::Duration::seconds(90),
            completed_at: now - chrono::Duration::seconds(60),
        };
        request_manager
            .persist_completed_realtime_batch(&[realtime_input(200), realtime_input(402)])
            .await
            .expect("persist realtime rows");

        let get = |query: String| {
            let server = &server;
            let admin = &admin;
            async move {
                server
                    .get(&format!("/admin/api/v1/monitoring/demand?{query}"))
                    .add_header(&add_auth_headers(admin)[0].0, &add_auth_headers(admin)[0].1)
                    .add_header(&add_auth_headers(admin)[1].0, &add_auth_headers(admin)[1].1)
                    .await
            }
        };

        let response = get("window=-1h".to_string()).await;
        response.assert_status_ok();
        let counts: HashMap<String, HashMap<String, i64>> = response.json();
        assert!(
            !counts.contains_key(model),
            "priority rows must not leak into the default batch tier: {counts:?}"
        );

        let response = get("window=-1h&service_tiers=priority".to_string()).await;
        response.assert_status_ok();
        let counts: HashMap<String, HashMap<String, i64>> = response.json();
        assert_eq!(counts.get(model).and_then(|m| m.get("-1h")), Some(&2), "{counts:?}");

        let response = get("window=-1h:1h&service_tiers=batch,priority".to_string()).await;
        response.assert_status_ok();
        let counts: HashMap<String, HashMap<String, i64>> = response.json();
        assert_eq!(counts.get(model).and_then(|m| m.get("-1h:1h")), Some(&3), "{counts:?}");

        let response = get("window=-1h:1h&service_tiers=batch,priority&group_by=service_tier".to_string()).await;
        response.assert_status_ok();
        let cube: GroupedDemandByModelAndWindow = response.json();
        let window = cube
            .get(model)
            .and_then(|m| m.get("-1h:1h"))
            .unwrap_or_else(|| panic!("expected {model}/-1h:1h in cube, got {cube:?}"));
        assert_eq!(window.get("batch").and_then(|t| t.get("pending")), Some(&1), "{cube:?}");
        assert_eq!(window.get("priority").and_then(|t| t.get("completed")), Some(&1), "{cube:?}");
        assert_eq!(window.get("priority").and_then(|t| t.get("failed")), Some(&1), "{cube:?}");
    }
}
