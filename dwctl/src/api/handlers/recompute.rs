
use axum::Json;
use axum::extract::{Query, State};
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use utoipa::IntoParams;
use uuid::Uuid;

use crate::AppState;
use crate::auth::permissions::{RequiresPermission, operation, resource};
use crate::errors::{Error, Result};
use crate::recompute::cache_fields::CreationTier;
use crate::recompute::report::RecomputeReport;
use crate::recompute::source::CorpusFilter;
use sqlx_pool_router::PoolProvider;

const MAX_LIMIT: i64 = 25_000;
const DEFAULT_LIMIT: i64 = 5_000;

const MAX_WINDOW_DAYS: i64 = 31;

#[derive(Debug, Deserialize, IntoParams)]
pub struct RecomputeQuery {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub user_id: Option<Uuid>,
    pub uri_pattern: Option<String>,
    pub model: Option<String>,
    pub limit: Option<i64>,
    pub flat_creation_tier: Option<String>,
}

#[utoipa::path(
    get,
    path = "/admin/api/v1/usage-recompute",
    tag = "analytics",
    summary = "Dry-run recompute of recorded usage",
    description = "Replays stored request/response payloads through the live serializer and re-prices them, \
reporting stored vs recomputed usage per request. Read-only: it writes nothing. Applying a correction is a \
separate documented procedure.",
    responses(
        (status = 200, description = "The recompute report.", body = RecomputeReport),
        (status = 400, description = "Invalid or unbounded corpus predicate."),
        (status = 403, description = "Requires analytics read access.")
    ),
    params(RecomputeQuery)
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id))]
pub async fn recompute_usage<P: PoolProvider>(
    Query(query): Query<RecomputeQuery>,
    State(state): State<AppState<P>>,
    current_user: RequiresPermission<resource::Analytics, operation::ReadAll>,
) -> Result<Json<RecomputeReport>> {
    if query.end <= query.start {
        return Err(Error::BadRequest {
            message: "end must be after start".to_string(),
        });
    }
    if query.end - query.start > Duration::days(MAX_WINDOW_DAYS) {
        return Err(Error::BadRequest {
            message: format!("window must be at most {MAX_WINDOW_DAYS} days; narrow it and run several corpora"),
        });
    }

    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    if limit <= 0 || limit > MAX_LIMIT {
        return Err(Error::BadRequest {
            message: format!("limit must be between 1 and {MAX_LIMIT}"),
        });
    }

    let flat_tier = match query.flat_creation_tier.as_deref() {
        None | Some("5m") => CreationTier::FiveMinute,
        Some("1h") => CreationTier::OneHour,
        Some("24h") => CreationTier::TwentyFourHour,
        Some(other) => {
            return Err(Error::BadRequest {
                message: format!("unknown flat_creation_tier {other:?}; expected 5m, 1h or 24h"),
            });
        }
    };

    let filter = CorpusFilter {
        start: query.start,
        end: query.end,
        user_id: query.user_id,
        uri_pattern: query.uri_pattern,
        model: query.model,
        limit,
    };

    let cfg = state.current_config();
    let classifier = cfg.cache.enabled.then(|| {
        let pool = state.db.read().into_inner();
        crate::prompt_cache::Classifier::new(
            crate::prompt_cache::PrincipalResolver::new(pool.clone()),
            crate::prompt_cache::ModelConfigResolver::new(pool.clone()),
            crate::prompt_cache::TokenizerClient::new(cfg.cache.tokenizer_url.clone()),
            std::sync::Arc::new(crate::recompute::cache_replay::HistoricalIndex::new(pool, Utc::now())),
            crate::prompt_cache::TierPolicy::from_config(&cfg.cache.enabled_ttls, &cfg.cache.default_ttl),
            crate::prompt_cache::TelemetryPolicy::from_config(
                cfg.cache.telemetry_blocks.strip_from_prompt,
                &cfg.cache.telemetry_blocks.prefixes,
            ),
            cfg.cache.render_counting,
        )
    });

    let tokenizer =
        (!cfg.cache.tokenizer_url.is_empty()).then(|| crate::prompt_cache::TokenizerClient::new(cfg.cache.tokenizer_url.clone()));

    let grace = chrono::Duration::from_std(cfg.background_services.prompt_cache_retention.grace()).unwrap_or(chrono::TimeDelta::MAX);
    let report = crate::recompute::recompute_corpus(&state.db.read(), &filter, flat_tier, grace, classifier.as_ref(), tokenizer.as_ref())
        .await
        .map_err(|e| Error::Internal {
            operation: format!("recompute usage: {e}"),
        })?;

    tracing::info!(
        rows_total = report.summary.rows_total,
        rows_changed = report.summary.rows_changed,
        net_correction = %report.summary.net_correction,
        "usage recompute dry run"
    );

    Ok(Json(report))
}

#[cfg(test)]
mod tests {
    use crate::api::models::users::Role;
    use crate::test::utils::{add_auth_headers, create_test_app, create_test_user, setup_fusillade_pool};
    use chrono::Utc;
    use rust_decimal::Decimal;
    use sqlx::PgPool;
    use uuid::Uuid;

    async fn seed_billed_request(pool: &PgPool, response_body: &str, prompt: i64, completion: i64, cost: Decimal) -> Uuid {
        let user_id = Uuid::new_v4();
        sqlx::query!(
            "INSERT INTO users (id, username, email, is_admin, auth_source) VALUES ($1,$2,$3,false,'test')",
            user_id,
            format!("u_{}", user_id.simple()),
            format!("{}@example.com", user_id.simple()),
        )
        .execute(pool)
        .await
        .unwrap();

        let template_id = Uuid::new_v4();
        crate::test::utils::insert_fusillade_template(
            pool,
            template_id,
            None,
            "m",
            "http://x",
            "/messages",
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":16}"#,
            None,
        )
        .await;

        let request_id = Uuid::new_v4();
        sqlx::query!(
            "INSERT INTO fusillade.requests (id, template_id, state, model, response_status, response_body,
                                             claimed_at, started_at, completed_at, created_by)
             VALUES ($1,$2,'completed','m',200,$3,NOW(),NOW(),NOW(),$4)",
            request_id,
            template_id,
            response_body,
            user_id.to_string(),
        )
        .execute(pool)
        .await
        .unwrap();

        sqlx::query!(
            "INSERT INTO http_analytics
               (instance_id, correlation_id, timestamp, method, uri, model, status_code, user_id,
                prompt_tokens, completion_tokens, total_tokens,
                total_cost, input_price_per_token, output_price_per_token, fusillade_request_id)
             VALUES ($1,1,NOW(),'POST','/messages',$2,200,$3,$4,$5,$6,$7,0.000001,0.000002,$8)",
            Uuid::new_v4(),
            "m",
            user_id,
            prompt,
            completion,
            prompt + completion,
            cost,
            request_id,
        )
        .execute(pool)
        .await
        .unwrap();

        user_id
    }

    fn window() -> (String, String) {
        let start = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        let end = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        (start, end)
    }

    #[dwctl_test_macros::test]
    async fn requires_analytics_read_permission(pool: PgPool) {
        setup_fusillade_pool(&pool).await;
        let (app, _bg) = create_test_app(pool.clone(), false).await;
        let (start, end) = window();

        let standard = create_test_user(&pool, Role::StandardUser).await;
        let headers = add_auth_headers(&standard);
        let response = app
            .get("/admin/api/v1/usage-recompute")
            .add_query_param("start", &start)
            .add_query_param("end", &end)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::FORBIDDEN);

        let viewer = create_test_user(&pool, Role::RequestViewer).await;
        let headers = add_auth_headers(&viewer);
        let response = app
            .get("/admin/api/v1/usage-recompute")
            .add_query_param("start", &start)
            .add_query_param("end", &end)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;
        response.assert_status_ok();
    }

    #[dwctl_test_macros::test]
    async fn malformed_predicates_are_rejected(pool: PgPool) {
        setup_fusillade_pool(&pool).await;
        let (app, _bg) = create_test_app(pool.clone(), false).await;
        let viewer = create_test_user(&pool, Role::RequestViewer).await;
        let headers = add_auth_headers(&viewer);
        let (start, end) = window();

        for (params, why) in [
            (vec![("start", end.clone()), ("end", start.clone())], "inverted window"),
            (
                vec![
                    ("start", (Utc::now() - chrono::Duration::days(60)).to_rfc3339()),
                    ("end", Utc::now().to_rfc3339()),
                ],
                "window wider than the 31-day cap",
            ),
            (
                vec![("start", start.clone()), ("end", end.clone()), ("limit", "0".into())],
                "zero limit",
            ),
            (
                vec![("start", start.clone()), ("end", end.clone()), ("limit", "999999".into())],
                "limit over the hard cap",
            ),
            (
                vec![("start", start.clone()), ("end", end.clone()), ("flat_creation_tier", "2h".into())],
                "unknown flat-creation tier",
            ),
        ] {
            let mut req = app.get("/admin/api/v1/usage-recompute");
            for (k, v) in &params {
                req = req.add_query_param(k, v);
            }
            let response = req
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .await;
            response.assert_status(axum::http::StatusCode::BAD_REQUEST);
            assert!(!response.text().is_empty(), "{why}: the refusal must say why");
        }
    }

    #[dwctl_test_macros::test]
    async fn detects_the_anthropic_incident_over_http(pool: PgPool) {
        setup_fusillade_pool(&pool).await;
        let (app, _bg) = create_test_app(pool.clone(), false).await;
        let viewer = create_test_user(&pool, Role::RequestViewer).await;
        let headers = add_auth_headers(&viewer);
        let (start, end) = window();

        let user_id = seed_billed_request(
            &pool,
            r#"{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":565,"output_tokens":658,"cache_read_input_tokens":17631,"cache_creation_input_tokens":538}}"#,
            565,
            658,
            Decimal::from_str_exact("0.000447942276").unwrap(),
        )
        .await;

        let response = app
            .get("/admin/api/v1/usage-recompute")
            .add_query_param("start", &start)
            .add_query_param("end", &end)
            .add_query_param("user_id", &user_id.to_string())
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;
        response.assert_status_ok();

        let report: serde_json::Value = response.json();
        assert_eq!(report["summary"]["rows_total"], 1);
        assert_eq!(report["summary"]["rows_changed"], 1);
        let row = &report["rows"][0];
        assert_eq!(row["recomputed"]["prompt_tokens"], 18_734, "565 + 17631 + 538");
        assert_eq!(row["recomputed"]["cache_creation_5m"], 538, "recovered from the flat field");
        assert_eq!(row["cache_tier_inferred"], true, "the body never stated a tier");
        assert!(row["fusillade_request_id"].is_string(), "repair scripts key on this");
        let net: Decimal = report["summary"]["net_correction"].as_str().unwrap().parse().unwrap();
        assert!(net > Decimal::ZERO, "an undercharge, as measured: {net}");
    }

    #[dwctl_test_macros::test]
    async fn july_null_usage_surfaces_as_not_replayable_over_http(pool: PgPool) {
        setup_fusillade_pool(&pool).await;
        let (app, _bg) = create_test_app(pool.clone(), false).await;
        let viewer = create_test_user(&pool, Role::RequestViewer).await;
        let headers = add_auth_headers(&viewer);
        let (start, end) = window();

        let user_id = seed_billed_request(
            &pool,
            r#"{"choices":[{"finish_reason":"tool_calls","index":0,"message":{"role":"assistant","content":null}}],"created":1,"id":"c","model":"m","object":"chat.completion","usage":null}"#,
            0,
            0,
            Decimal::ZERO,
        )
        .await;

        let response = app
            .get("/admin/api/v1/usage-recompute")
            .add_query_param("start", &start)
            .add_query_param("end", &end)
            .add_query_param("user_id", &user_id.to_string())
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;
        response.assert_status_ok();

        let report: serde_json::Value = response.json();
        assert_eq!(report["summary"]["rows_not_replayable"], 1);
        assert_eq!(report["summary"]["rows_unchanged"], 0, "must not be certified healthy");
    }
}
