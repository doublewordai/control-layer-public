
use chrono::{DateTime, Duration, Timelike, Utc};
use moka::future::Cache;
use once_cell::sync::Lazy;
use sqlx::{FromRow, PgPool, Postgres, QueryBuilder, Transaction};
use std::collections::HashMap;
use tracing::instrument;

use crate::{
    api::models::{
        batches::BatchAnalytics,
        deployments::{ModelMetrics, ModelTimeSeriesPoint},
        requests::{
            AnalyticsEntry, HttpAnalyticsFilter, ModelBreakdownEntry, ModelUsage, ModelUserUsageResponse, RequestsAggregateResponse,
            StatusCodeBreakdown, TimeSeriesPoint, UserUsage,
        },
    },
    db::errors::Result,
    inference::serving_name::split_served,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use uuid::Uuid;

static METRICS_CACHE: Lazy<Cache<String, HashMap<String, ModelMetrics>>> = Lazy::new(|| {
    Cache::builder()
        .max_capacity(100)
        .time_to_live(std::time::Duration::from_secs(60))
        .build()
});

const DASHBOARD_READ_TIMEOUT: &str = "SET LOCAL statement_timeout = '30s'";

async fn begin_dashboard_read(db: &PgPool) -> Result<Transaction<'static, Postgres>> {
    let mut tx = db.begin().await?;
    sqlx::query(DASHBOARD_READ_TIMEOUT).execute(&mut *tx).await?;
    Ok(tx)
}

#[derive(Debug, Clone, Copy)]
pub enum TimeGranularity {
    TenMinutes,
    Hour,
}

#[derive(FromRow)]
struct TimeSeriesRow {
    pub timestamp: Option<DateTime<Utc>>,
    pub requests_count: Option<i64>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub avg_latency_ms: Option<f64>,
    pub p95_latency_ms: Option<f64>,
    pub p99_latency_ms: Option<f64>,
}

#[derive(FromRow)]
struct StatusCodeRow {
    pub status_code: Option<i32>,
    pub status_count: Option<i64>,
}

#[derive(FromRow)]
struct ModelUsageRow {
    pub model_name: Option<String>,
    pub model_count: Option<i64>,
    pub model_avg_latency_ms: Option<f64>,
}

#[derive(FromRow)]
struct TotalRequestsRow {
    pub total_requests: Option<i64>,
}

#[derive(FromRow)]
struct ModelMetricsRow {
    pub model: Option<String>,
    pub total_requests: Option<i64>,
    pub avg_latency_ms: Option<f64>,
    pub total_input_tokens: Option<i64>,
    pub total_output_tokens: Option<i64>,
    pub last_active_at: Option<DateTime<Utc>>,
}

#[derive(FromRow)]
struct BulkTimeSeriesRow {
    pub model: Option<String>,
    pub timestamp: Option<DateTime<Utc>>,
    pub requests_count: Option<i64>,
}

#[instrument(level = "debug", skip(db), err)]
async fn get_total_requests(
    db: &PgPool,
    time_range_start: DateTime<Utc>,
    time_range_end: DateTime<Utc>,
    model_filter: Option<&str>,
) -> Result<i64> {
    let mut tx = begin_dashboard_read(db).await?;
    let total_requests = if let Some(model) = model_filter {
        sqlx::query_as!(
            TotalRequestsRow,
            "SELECT COUNT(*) as total_requests FROM http_analytics WHERE timestamp >= $1 AND timestamp <= $2 AND model = $3",
            time_range_start,
            time_range_end,
            model
        )
        .fetch_one(&mut *tx)
        .await?
        .total_requests
        .unwrap_or(0)
    } else {
        sqlx::query_as!(
            TotalRequestsRow,
            "SELECT COUNT(*) as total_requests FROM http_analytics WHERE timestamp >= $1 AND timestamp <= $2",
            time_range_start,
            time_range_end
        )
        .fetch_one(&mut *tx)
        .await?
        .total_requests
        .unwrap_or(0)
    };
    tx.commit().await?;

    Ok(total_requests)
}

#[instrument(level = "debug", skip(db), err)]
async fn get_time_series(
    db: &PgPool,
    time_range_start: DateTime<Utc>,
    time_range_end: DateTime<Utc>,
    model_filter: Option<&str>,
    granularity: TimeGranularity,
) -> Result<Vec<TimeSeriesPoint>> {
    match granularity {
        TimeGranularity::Hour => get_time_series_hourly(db, time_range_start, time_range_end, model_filter).await,
        TimeGranularity::TenMinutes => get_time_series_ten_minutes(db, time_range_start, time_range_end, model_filter).await,
    }
}

#[instrument(level = "debug", skip(db), err)]
async fn get_time_series_hourly(
    db: &PgPool,
    time_range_start: DateTime<Utc>,
    time_range_end: DateTime<Utc>,
    model_filter: Option<&str>,
) -> Result<Vec<TimeSeriesPoint>> {
    let mut tx = begin_dashboard_read(db).await?;
    let rows = if let Some(model) = model_filter {
        sqlx::query_as!(
            TimeSeriesRow,
            r#"
            SELECT
                date_trunc('hour', timestamp) as timestamp,
                COUNT(*) as requests_count,
                COALESCE(SUM(prompt_tokens), 0)::bigint as input_tokens,
                COALESCE(SUM(completion_tokens), 0)::bigint as output_tokens,
                AVG(duration_ms)::float8 as avg_latency_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)::float8 as p95_latency_ms,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms)::float8 as p99_latency_ms
            FROM http_analytics
            WHERE timestamp >= $1 AND timestamp <= $2 AND model = $3
            GROUP BY date_trunc('hour', timestamp)
            ORDER BY timestamp
            "#,
            time_range_start,
            time_range_end,
            model
        )
        .fetch_all(&mut *tx)
        .await?
    } else {
        sqlx::query_as!(
            TimeSeriesRow,
            r#"
            SELECT
                date_trunc('hour', timestamp) as timestamp,
                COUNT(*) as requests_count,
                COALESCE(SUM(prompt_tokens), 0)::bigint as input_tokens,
                COALESCE(SUM(completion_tokens), 0)::bigint as output_tokens,
                AVG(duration_ms)::float8 as avg_latency_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)::float8 as p95_latency_ms,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms)::float8 as p99_latency_ms
            FROM http_analytics
            WHERE timestamp >= $1 AND timestamp <= $2
            GROUP BY date_trunc('hour', timestamp)
            ORDER BY timestamp
            "#,
            time_range_start,
            time_range_end
        )
        .fetch_all(&mut *tx)
        .await?
    };

    tx.commit().await?;

    let time_series = rows
        .into_iter()
        .filter_map(|row| {
            row.timestamp.map(|timestamp| TimeSeriesPoint {
                timestamp,
                duration_minutes: 60,
                requests: row.requests_count.unwrap_or(0),
                input_tokens: row.input_tokens.unwrap_or(0),
                output_tokens: row.output_tokens.unwrap_or(0),
                avg_latency_ms: row.avg_latency_ms,
                p95_latency_ms: row.p95_latency_ms,
                p99_latency_ms: row.p99_latency_ms,
            })
        })
        .collect();

    let filled_time_series = fill_missing_intervals(time_series, time_range_start, time_range_end);

    Ok(filled_time_series)
}

#[instrument(level = "debug", skip(db), err)]
async fn get_time_series_ten_minutes(
    db: &PgPool,
    time_range_start: DateTime<Utc>,
    time_range_end: DateTime<Utc>,
    model_filter: Option<&str>,
) -> Result<Vec<TimeSeriesPoint>> {
    let mut tx = begin_dashboard_read(db).await?;
    let rows = if let Some(model) = model_filter {
        sqlx::query_as!(
            TimeSeriesRow,
            r#"
            SELECT
                date_trunc('hour', timestamp) + INTERVAL '10 minute' * FLOOR(EXTRACT(minute FROM timestamp) / 10) as timestamp,
                COUNT(*) as requests_count,
                COALESCE(SUM(prompt_tokens), 0)::bigint as input_tokens,
                COALESCE(SUM(completion_tokens), 0)::bigint as output_tokens,
                AVG(duration_ms)::float8 as avg_latency_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)::float8 as p95_latency_ms,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms)::float8 as p99_latency_ms
            FROM http_analytics
            WHERE timestamp >= $1 AND timestamp <= $2 AND model = $3
            GROUP BY date_trunc('hour', timestamp) + INTERVAL '10 minute' * FLOOR(EXTRACT(minute FROM timestamp) / 10)
            ORDER BY timestamp
            "#,
            time_range_start,
            time_range_end,
            model
        )
        .fetch_all(&mut *tx)
        .await?
    } else {
        sqlx::query_as!(
            TimeSeriesRow,
            r#"
            SELECT
                date_trunc('hour', timestamp) + INTERVAL '10 minute' * FLOOR(EXTRACT(minute FROM timestamp) / 10) as timestamp,
                COUNT(*) as requests_count,
                COALESCE(SUM(prompt_tokens), 0)::bigint as input_tokens,
                COALESCE(SUM(completion_tokens), 0)::bigint as output_tokens,
                AVG(duration_ms)::float8 as avg_latency_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)::float8 as p95_latency_ms,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms)::float8 as p99_latency_ms
            FROM http_analytics
            WHERE timestamp >= $1 AND timestamp <= $2
            GROUP BY date_trunc('hour', timestamp) + INTERVAL '10 minute' * FLOOR(EXTRACT(minute FROM timestamp) / 10)
            ORDER BY timestamp
            "#,
            time_range_start,
            time_range_end
        )
        .fetch_all(&mut *tx)
        .await?
    };

    tx.commit().await?;

    let time_series = rows
        .into_iter()
        .filter_map(|row| {
            row.timestamp.map(|timestamp| TimeSeriesPoint {
                timestamp,
                duration_minutes: 10,
                requests: row.requests_count.unwrap_or(0),
                input_tokens: row.input_tokens.unwrap_or(0),
                output_tokens: row.output_tokens.unwrap_or(0),
                avg_latency_ms: row.avg_latency_ms,
                p95_latency_ms: row.p95_latency_ms,
                p99_latency_ms: row.p99_latency_ms,
            })
        })
        .collect();

    let filled_time_series = fill_missing_intervals_ten_minutes(time_series, time_range_start, time_range_end);

    Ok(filled_time_series)
}

fn fill_missing_intervals(
    mut time_series: Vec<TimeSeriesPoint>,
    time_range_start: DateTime<Utc>,
    time_range_end: DateTime<Utc>,
) -> Vec<TimeSeriesPoint> {
    time_series.sort_by_key(|point| point.timestamp);

    let existing_points: HashMap<DateTime<Utc>, &TimeSeriesPoint> = time_series.iter().map(|point| (point.timestamp, point)).collect();

    let start_hour = time_range_start
        .date_naive()
        .and_hms_opt(time_range_start.hour(), 0, 0)
        .map(|naive| naive.and_utc())
        .unwrap_or(time_range_start);

    let mut filled_series = Vec::new();
    let mut current = start_hour;

    while current <= time_range_end {
        if let Some(existing_point) = existing_points.get(&current) {
            filled_series.push((*existing_point).clone());
        } else {
            filled_series.push(TimeSeriesPoint {
                timestamp: current,
                duration_minutes: 60,
                requests: 0,
                input_tokens: 0,
                output_tokens: 0,
                avg_latency_ms: None,
                p95_latency_ms: None,
                p99_latency_ms: None,
            });
        }

        current += Duration::hours(1);
    }

    filled_series
}

fn fill_missing_intervals_ten_minutes(
    mut time_series: Vec<TimeSeriesPoint>,
    time_range_start: DateTime<Utc>,
    time_range_end: DateTime<Utc>,
) -> Vec<TimeSeriesPoint> {
    time_series.sort_by_key(|point| point.timestamp);

    let existing_points: HashMap<DateTime<Utc>, &TimeSeriesPoint> = time_series.iter().map(|point| (point.timestamp, point)).collect();

    let start_ten_minutes = time_range_start
        .date_naive()
        .and_hms_opt(time_range_start.hour(), (time_range_start.minute() / 10) * 10, 0)
        .map(|naive| naive.and_utc())
        .unwrap_or(time_range_start);

    let mut filled_series = Vec::new();
    let mut current = start_ten_minutes;

    while current <= time_range_end {
        if let Some(existing_point) = existing_points.get(&current) {
            filled_series.push((*existing_point).clone());
        } else {
            filled_series.push(TimeSeriesPoint {
                timestamp: current,
                duration_minutes: 10,
                requests: 0,
                input_tokens: 0,
                output_tokens: 0,
                avg_latency_ms: None,
                p95_latency_ms: None,
                p99_latency_ms: None,
            });
        }
        current += Duration::minutes(10);
    }

    filled_series
}

fn fill_missing_intervals_for_sparklines(
    mut time_series: Vec<ModelTimeSeriesPoint>,
    time_range_start: DateTime<Utc>,
    time_range_end: DateTime<Utc>,
) -> Vec<ModelTimeSeriesPoint> {
    time_series.sort_by_key(|point| point.timestamp);

    let existing_points: HashMap<DateTime<Utc>, &ModelTimeSeriesPoint> = time_series.iter().map(|point| (point.timestamp, point)).collect();

    let start_ten_minutes = time_range_start
        .date_naive()
        .and_hms_opt(time_range_start.hour(), (time_range_start.minute() / 10) * 10, 0)
        .map(|naive| naive.and_utc())
        .unwrap_or(time_range_start);

    let mut filled_series = Vec::new();
    let mut current = start_ten_minutes;

    while current <= time_range_end {
        if let Some(existing_point) = existing_points.get(&current) {
            filled_series.push((*existing_point).clone());
        } else {
            filled_series.push(ModelTimeSeriesPoint {
                timestamp: current,
                requests: 0,
            });
        }
        current += Duration::minutes(10);
    }

    filled_series
}

#[instrument(level = "debug", skip(db), err)]
async fn get_status_codes(
    db: &PgPool,
    time_range_start: DateTime<Utc>,
    time_range_end: DateTime<Utc>,
    model_filter: Option<&str>,
) -> Result<Vec<StatusCodeRow>> {
    let mut tx = begin_dashboard_read(db).await?;
    let rows = if let Some(model) = model_filter {
        sqlx::query_as!(
            StatusCodeRow,
            "SELECT status_code, COUNT(*) as status_count FROM http_analytics WHERE timestamp >= $1 AND timestamp <= $2 AND model = $3 AND status_code IS NOT NULL GROUP BY status_code ORDER BY status_count DESC",
            time_range_start,
            time_range_end,
            model
        )
        .fetch_all(&mut *tx)
        .await?
    } else {
        sqlx::query_as!(
            StatusCodeRow,
            "SELECT status_code, COUNT(*) as status_count FROM http_analytics WHERE timestamp >= $1 AND timestamp <= $2 AND status_code IS NOT NULL GROUP BY status_code ORDER BY status_count DESC",
            time_range_start,
            time_range_end
        )
        .fetch_all(&mut *tx)
        .await?
    };

    tx.commit().await?;

    Ok(rows)
}

#[instrument(level = "debug", skip(db), err)]
async fn get_model_usage(db: &PgPool, time_range_start: DateTime<Utc>, time_range_end: DateTime<Utc>) -> Result<Vec<ModelUsageRow>> {
    let mut tx = begin_dashboard_read(db).await?;
    let rows = sqlx::query_as!(
        ModelUsageRow,
        "SELECT model as model_name, COUNT(*) as model_count, COALESCE(AVG(duration_ms), 0)::float8 as model_avg_latency_ms FROM http_analytics WHERE timestamp >= $1 AND timestamp <= $2 AND model IS NOT NULL GROUP BY model ORDER BY model_count DESC",
        time_range_start,
        time_range_end
    )
    .fetch_all(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(rows)
}

#[instrument(level = "debug", skip(db), err)]
pub async fn get_requests_aggregate(
    db: &PgPool,
    time_range_start: DateTime<Utc>,
    time_range_end: DateTime<Utc>,
    model_filter: Option<&str>,
) -> Result<RequestsAggregateResponse> {
    let (total_requests, time_series, status_code_rows, model_rows) = if model_filter.is_some() {
        let (total_requests, time_series, status_code_rows) = tokio::try_join!(
            get_total_requests(db, time_range_start, time_range_end, model_filter),
            get_time_series(db, time_range_start, time_range_end, model_filter, TimeGranularity::Hour),
            get_status_codes(db, time_range_start, time_range_end, model_filter),
        )?;
        (total_requests, time_series, status_code_rows, Vec::new())
    } else {
        let (total_requests, time_series, status_code_rows, model_rows) = tokio::try_join!(
            get_total_requests(db, time_range_start, time_range_end, model_filter),
            get_time_series(db, time_range_start, time_range_end, model_filter, TimeGranularity::Hour),
            get_status_codes(db, time_range_start, time_range_end, model_filter),
            get_model_usage(db, time_range_start, time_range_end),
        )?;
        (total_requests, time_series, status_code_rows, model_rows)
    };

    let status_codes: Vec<StatusCodeBreakdown> = status_code_rows
        .into_iter()
        .filter_map(|row| match (row.status_code, row.status_count) {
            (Some(status_code), Some(status_count)) => Some(StatusCodeBreakdown {
                status: status_code.to_string(),
                count: status_count,
                percentage: if total_requests > 0 {
                    (status_count as f64 * 100.0) / total_requests as f64
                } else {
                    0.0
                },
            }),
            _ => None,
        })
        .collect();

    let models = if !model_rows.is_empty() {
        let models: Vec<ModelUsage> = model_rows
            .into_iter()
            .filter_map(|row| match (row.model_name, row.model_count) {
                (Some(model_name), Some(model_count)) => Some(ModelUsage {
                    model: model_name,
                    count: model_count,
                    percentage: if total_requests > 0 {
                        (model_count as f64 * 100.0) / total_requests as f64
                    } else {
                        0.0
                    },
                    avg_latency_ms: row.model_avg_latency_ms.unwrap_or(0.0),
                }),
                _ => None,
            })
            .collect();
        Some(models)
    } else {
        None
    };

    Ok(RequestsAggregateResponse {
        total_requests,
        model: model_filter.map(|m| m.to_string()),
        status_codes,
        models,
        time_series,
    })
}

#[instrument(level = "debug", skip(db), err)]
pub async fn get_model_metrics(db: &PgPool, mut model_aliases: Vec<String>) -> Result<HashMap<String, ModelMetrics>> {
    if model_aliases.is_empty() {
        return Ok(HashMap::new());
    }

    model_aliases.sort();
    let cache_key = model_aliases.join(",");

    if let Some(cached) = METRICS_CACHE.get(&cache_key).await {
        tracing::debug!("Cache hit for model metrics");
        return Ok(cached);
    }

    tracing::debug!("Cache miss for model metrics, executing query");
    let result = get_model_metrics_impl(db, model_aliases.clone()).await?;

    METRICS_CACHE.insert(cache_key, result.clone()).await;

    Ok(result)
}

async fn get_model_metrics_impl(db: &PgPool, model_aliases: Vec<String>) -> Result<HashMap<String, ModelMetrics>> {
    let mut metrics_map: HashMap<String, ModelMetrics> = HashMap::new();
    for alias in &model_aliases {
        metrics_map.insert(
            alias.clone(),
            ModelMetrics {
                avg_latency_ms: None,
                total_requests: 0,
                total_input_tokens: 0,
                total_output_tokens: 0,
                last_active_at: None,
                time_series: None,
            },
        );
    }

    let mut tx = begin_dashboard_read(db).await?;

    let metrics_rows = sqlx::query_as!(
        ModelMetricsRow,
        r#"
        SELECT
            model,
            COUNT(*) as total_requests,
            AVG(duration_ms)::float8 as avg_latency_ms,
            COALESCE(SUM(prompt_tokens), 0)::bigint as total_input_tokens,
            COALESCE(SUM(completion_tokens), 0)::bigint as total_output_tokens,
            MAX(timestamp) as last_active_at
        FROM http_analytics
        WHERE model = ANY($1)
        GROUP BY model
        "#,
        &model_aliases
    )
    .fetch_all(&mut *tx)
    .await?;

    for row in metrics_rows {
        if let Some(model) = row.model
            && let Some(metrics) = metrics_map.get_mut(&model)
        {
            metrics.avg_latency_ms = row.avg_latency_ms;
            metrics.total_requests = row.total_requests.unwrap_or(0);
            metrics.total_input_tokens = row.total_input_tokens.unwrap_or(0);
            metrics.total_output_tokens = row.total_output_tokens.unwrap_or(0);
            metrics.last_active_at = row.last_active_at;
        }
    }

    let now = Utc::now();
    let two_hours_ago = now - Duration::hours(2);

    let time_series_rows = sqlx::query_as!(
        BulkTimeSeriesRow,
        r#"
        SELECT
            model,
            date_trunc('hour', timestamp) + INTERVAL '10 minute' * FLOOR(EXTRACT(minute FROM timestamp) / 10) as timestamp,
            COUNT(*) as requests_count
        FROM http_analytics
        WHERE model = ANY($1)
            AND timestamp >= $2
            AND timestamp <= $3
        GROUP BY model, date_trunc('hour', timestamp) + INTERVAL '10 minute' * FLOOR(EXTRACT(minute FROM timestamp) / 10)
        ORDER BY model, timestamp
        "#,
        &model_aliases,
        two_hours_ago,
        now
    )
    .fetch_all(&mut *tx)
    .await;

    tx.commit().await?;

    if let Ok(rows) = time_series_rows {
        let mut model_time_series: HashMap<String, Vec<ModelTimeSeriesPoint>> = HashMap::new();
        for row in rows {
            if let (Some(model), Some(timestamp)) = (row.model, row.timestamp) {
                model_time_series.entry(model).or_default().push(ModelTimeSeriesPoint {
                    timestamp,
                    requests: row.requests_count.unwrap_or(0),
                });
            }
        }

        for model in &model_aliases {
            if let Some(metrics) = metrics_map.get_mut(model) {
                let time_series = model_time_series.get(model).cloned().unwrap_or_default();
                let filled_time_series = fill_missing_intervals_for_sparklines(time_series, two_hours_ago, now);
                metrics.time_series = Some(filled_time_series);
            }
        }
    } else {
        tracing::warn!("Failed to fetch bulk time series data for models");
    }

    Ok(metrics_map)
}

#[derive(FromRow)]
struct UserUsageRow {
    pub user_id: Option<uuid::Uuid>,
    pub user_email: Option<String>,
    pub request_count: Option<i64>,
    pub total_input_tokens: Option<i64>,
    pub total_output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
    pub last_active_at: Option<DateTime<Utc>>,
}

#[instrument(level = "debug", skip(db), err)]
pub async fn get_model_user_usage(
    db: &PgPool,
    model_alias: &str,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<ModelUserUsageResponse> {
    let user_rows = sqlx::query_as!(
        UserUsageRow,
        r#"
        SELECT
            umu.user_id,
            u.email as "user_email?",
            COALESCE(SUM(umu.request_count), 0)::bigint as request_count,
            COALESCE(SUM(umu.input_tokens), 0)::bigint as total_input_tokens,
            COALESCE(SUM(umu.output_tokens), 0)::bigint as total_output_tokens,
            COALESCE(SUM(umu.input_tokens + umu.output_tokens), 0)::bigint as total_tokens,
            SUM(umu.cost)::float8 as total_cost,
            (MAX(umu.usage_date)::timestamp AT TIME ZONE 'UTC') as last_active_at
        FROM user_model_usage_daily umu
        LEFT JOIN users u ON u.id = umu.user_id
        WHERE (umu.model = $1 OR starts_with(umu.model, $1 || ':'))
            AND umu.usage_date >= ($2::timestamptz AT TIME ZONE 'UTC')::date
            AND umu.usage_date <= ($3::timestamptz AT TIME ZONE 'UTC')::date
        GROUP BY umu.user_id, u.email
        ORDER BY request_count DESC
        "#,
        model_alias,
        start_date,
        end_date
    )
    .fetch_all(db)
    .await?;

    let totals_row = sqlx::query!(
        r#"
        SELECT
            SUM(request_count)::bigint as total_requests,
            COALESCE(SUM(input_tokens + output_tokens), 0)::bigint as total_tokens,
            SUM(cost)::float8 as total_cost
        FROM user_model_usage_daily
        WHERE (model = $1 OR starts_with(model, $1 || ':'))
            AND usage_date >= ($2::timestamptz AT TIME ZONE 'UTC')::date
            AND usage_date <= ($3::timestamptz AT TIME ZONE 'UTC')::date
        "#,
        model_alias,
        start_date,
        end_date
    )
    .fetch_one(db)
    .await?;

    let users: Vec<UserUsage> = user_rows
        .into_iter()
        .map(|row| UserUsage {
            user_id: row.user_id.map(|id| id.to_string()),
            user_email: row.user_email,
            request_count: row.request_count.unwrap_or(0),
            total_tokens: row.total_tokens.unwrap_or(0),
            input_tokens: row.total_input_tokens.unwrap_or(0),
            output_tokens: row.total_output_tokens.unwrap_or(0),
            total_cost: row.total_cost,
            last_active_at: row.last_active_at,
        })
        .collect();

    Ok(ModelUserUsageResponse {
        model: model_alias.to_string(),
        start_date,
        end_date,
        total_requests: totals_row.total_requests.unwrap_or(0),
        total_tokens: totals_row.total_tokens.unwrap_or(0),
        total_cost: totals_row.total_cost,
        users,
    })
}

#[instrument(level = "debug", skip(pool))]
pub async fn get_batch_analytics(pool: &PgPool, batch_id: &Uuid) -> Result<Option<BatchAnalytics>> {
    let row = sqlx::query!(
        r#"
        SELECT
            total_requests,
            total_prompt_tokens,
            total_completion_tokens,
            total_reasoning_tokens,
            total_tokens,
            sum_duration_ms,
            count_duration_ms,
            sum_ttfb_ms,
            count_ttfb_ms,
            total_amount,
            total_list_cost
        FROM batch_aggregates
        WHERE fusillade_batch_id = $1
        "#,
        batch_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| {
        batch_analytics_from_row(
            r.total_requests,
            r.total_prompt_tokens,
            r.total_completion_tokens,
            r.total_reasoning_tokens,
            r.total_tokens,
            r.sum_duration_ms,
            r.count_duration_ms,
            r.sum_ttfb_ms,
            r.count_ttfb_ms,
            r.total_amount,
            r.total_list_cost,
        )
    }))
}

#[allow(clippy::too_many_arguments)]
fn batch_analytics_from_row(
    total_requests: i64,
    total_prompt_tokens: i64,
    total_completion_tokens: i64,
    total_reasoning_tokens: i64,
    total_tokens: i64,
    sum_duration_ms: i64,
    count_duration_ms: i64,
    sum_ttfb_ms: i64,
    count_ttfb_ms: i64,
    total_amount: Decimal,
    total_list_cost: Decimal,
) -> BatchAnalytics {
    let avg = |sum: i64, count: i64| (count > 0).then(|| sum as f64 / count as f64);
    BatchAnalytics {
        total_requests,
        total_prompt_tokens,
        total_completion_tokens,
        total_reasoning_tokens: (total_reasoning_tokens > 0).then_some(total_reasoning_tokens),
        total_tokens,
        avg_duration_ms: avg(sum_duration_ms, count_duration_ms),
        avg_ttfb_ms: avg(sum_ttfb_ms, count_ttfb_ms),
        total_cost: Some(total_amount.to_string()),
        total_list_cost: Some(total_list_cost.to_string()),
    }
}

#[instrument(level = "debug", skip(pool))]
pub async fn get_batches_analytics_bulk(pool: &PgPool, batch_ids: &[Uuid]) -> Result<HashMap<Uuid, BatchAnalytics>> {
    if batch_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query!(
        r#"
        SELECT
            fusillade_batch_id,
            total_requests,
            total_prompt_tokens,
            total_completion_tokens,
            total_reasoning_tokens,
            total_tokens,
            sum_duration_ms,
            count_duration_ms,
            sum_ttfb_ms,
            count_ttfb_ms,
            total_amount,
            total_list_cost
        FROM batch_aggregates
        WHERE fusillade_batch_id = ANY($1)
        "#,
        batch_ids
    )
    .fetch_all(pool)
    .await?;

    let mut result = HashMap::new();
    for row in rows {
        result.insert(
            row.fusillade_batch_id,
            batch_analytics_from_row(
                row.total_requests,
                row.total_prompt_tokens,
                row.total_completion_tokens,
                row.total_reasoning_tokens,
                row.total_tokens,
                row.sum_duration_ms,
                row.count_duration_ms,
                row.sum_ttfb_ms,
                row.count_ttfb_ms,
                row.total_amount,
                row.total_list_cost,
            ),
        );
    }

    for batch_id in batch_ids {
        result.entry(*batch_id).or_default();
    }

    Ok(result)
}

#[derive(FromRow)]
struct HttpAnalyticsRow {
    pub id: i64,
    pub timestamp: DateTime<Utc>,
    pub method: String,
    pub uri: String,
    pub model: Option<String>,
    pub status_code: Option<i32>,
    pub duration_ms: Option<i64>,
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub reasoning_tokens: i64,
    pub total_tokens: Option<i64>,
    pub response_type: Option<String>,
    pub fusillade_batch_id: Option<Uuid>,
    pub input_price_per_token: Option<Decimal>,
    pub output_price_per_token: Option<Decimal>,
    pub custom_id: Option<String>,
}

#[instrument(level = "debug", skip(pool), err)]
pub async fn list_http_analytics(
    pool: &PgPool,
    skip: i64,
    limit: i64,
    order_desc: bool,
    filters: HttpAnalyticsFilter,
) -> Result<Vec<AnalyticsEntry>> {
    let mut query = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            id,
            timestamp,
            method,
            uri,
            model,
            status_code,
            duration_ms,
            prompt_tokens,
            completion_tokens,
            reasoning_tokens,
            total_tokens,
            response_type,
            fusillade_batch_id,
            input_price_per_token,
            output_price_per_token,
            custom_id
        FROM http_analytics
        WHERE TRUE
        "#,
    );

    if let Some(value) = filters.timestamp_after {
        query.push(" AND timestamp >= ").push_bind(value);
    }
    if let Some(value) = filters.timestamp_before {
        query.push(" AND timestamp <= ").push_bind(value);
    }
    if let Some(value) = filters.model {
        query.push(" AND model = ").push_bind(value);
    }
    if let Some(value) = filters.fusillade_batch_id {
        query.push(" AND fusillade_batch_id = ").push_bind(value);
    }
    if let Some(value) = filters.method {
        query.push(" AND method = ").push_bind(value);
    }
    if let Some(value) = filters.uri_pattern {
        query.push(" AND uri LIKE ").push_bind(value);
    }
    if let Some(value) = filters.status_code {
        query.push(" AND status_code = ").push_bind(value);
    }
    if let Some(value) = filters.status_code_min {
        query.push(" AND status_code >= ").push_bind(value);
    }
    if let Some(value) = filters.status_code_max {
        query.push(" AND status_code <= ").push_bind(value);
    }
    if let Some(value) = filters.min_duration_ms {
        query.push(" AND duration_ms >= ").push_bind(value);
    }
    if let Some(value) = filters.max_duration_ms {
        query.push(" AND duration_ms <= ").push_bind(value);
    }
    if let Some(value) = filters.custom_id {
        query.push(" AND custom_id = ").push_bind(value);
    }

    query
        .push(" ORDER BY timestamp DESC LIMIT ")
        .push_bind(limit)
        .push(" OFFSET ")
        .push_bind(skip);

    let rows = query.build_query_as::<HttpAnalyticsRow>().fetch_all(pool).await?;

    let mut entries: Vec<AnalyticsEntry> = rows
        .into_iter()
        .map(|row| AnalyticsEntry {
            id: row.id,
            timestamp: row.timestamp,
            method: row.method,
            uri: row.uri,
            model: row.model,
            status_code: row.status_code,
            duration_ms: row.duration_ms,
            prompt_tokens: row.prompt_tokens,
            completion_tokens: row.completion_tokens,
            reasoning_tokens: if row.reasoning_tokens > 0 {
                Some(row.reasoning_tokens)
            } else {
                None
            },
            total_tokens: row.total_tokens,
            response_type: row.response_type,
            fusillade_batch_id: row.fusillade_batch_id,
            input_price_per_token: row.input_price_per_token.map(|p| p.to_string()),
            output_price_per_token: row.output_price_per_token.map(|p| p.to_string()),
            custom_id: row.custom_id,
        })
        .collect();

    if !order_desc {
        entries.reverse();
    }

    Ok(entries)
}

#[derive(FromRow)]
struct BatchCountRow {
    pub total_batch_count: Option<i64>,
    pub avg_requests_per_batch: Option<Decimal>,
    pub total_cost: Option<Decimal>,
}

#[derive(FromRow)]
struct ModelBreakdownRow {
    pub model: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cost: Option<Decimal>,
    pub request_count: Option<i64>,
}

#[instrument(level = "debug", skip(pool), err)]
pub async fn get_user_batch_counts(pool: &PgPool, user_id: Uuid) -> Result<(i64, f64, String)> {
    let row = sqlx::query_as!(
        BatchCountRow,
        r#"
        SELECT
            COUNT(*) as total_batch_count,
            COALESCE(AVG(transaction_count), 0) as avg_requests_per_batch,
            COALESCE(SUM(total_amount), 0) as total_cost
        FROM batch_aggregates
        WHERE user_id = $1
        "#,
        user_id
    )
    .fetch_one(pool)
    .await?;

    Ok((
        row.total_batch_count.unwrap_or(0),
        row.avg_requests_per_batch.and_then(|d| d.to_f64()).unwrap_or(0.0),
        row.total_cost.map(|d| d.to_string()).unwrap_or_else(|| "0".to_string()),
    ))
}

const USAGE_DAILY_REFRESH_LOCK: i64 = 0x4457_4354_5553_4744;

#[instrument(level = "debug", skip(pool), err)]
pub async fn refresh_user_model_usage_daily(pool: &PgPool) -> Result<()> {
    let mut tx = pool.begin().await?;

    let got_lock: bool = sqlx::query_scalar!("SELECT pg_try_advisory_xact_lock($1)", USAGE_DAILY_REFRESH_LOCK)
        .fetch_one(&mut *tx)
        .await?
        .unwrap_or(false);
    if !got_lock {
        return Ok(());
    }

    let cursor: i64 = sqlx::query_scalar!("SELECT last_processed_id FROM user_model_usage_daily_cursor WHERE id = TRUE FOR UPDATE")
        .fetch_one(&mut *tx)
        .await?;

    let new_max: Option<i64> = sqlx::query_scalar!(
        r#"
        SELECT MAX(id) FROM http_analytics
        WHERE id > $1 AND user_id IS NOT NULL AND model IS NOT NULL
        "#,
        cursor
    )
    .fetch_one(&mut *tx)
    .await?;

    let Some(new_max) = new_max else {
        return Ok(());
    };

    sqlx::query!(
        r#"
        INSERT INTO user_model_usage_daily (user_id, model, usage_date, input_tokens, output_tokens, cost, request_count)
        SELECT user_id,
               usage_model,
               (timestamp AT TIME ZONE 'UTC')::date,
               COALESCE(SUM(prompt_tokens), 0),
               COALESCE(SUM(completion_tokens), 0),
               COALESCE(SUM(total_cost), 0),
               COUNT(*)
        FROM (
            SELECT user_id, timestamp, prompt_tokens, completion_tokens, total_cost,
                   CASE WHEN requested_serving_class = resolved_serving_class
                             AND resolved_serving_class <> 'standard'
                        THEN model || ':' || resolved_serving_class
                        ELSE model
                   END AS usage_model
            FROM http_analytics
            WHERE id > $1 AND id <= $2
                  AND user_id IS NOT NULL AND model IS NOT NULL
                  AND status_code BETWEEN 200 AND 299
        ) ha
        GROUP BY user_id, usage_model, (timestamp AT TIME ZONE 'UTC')::date
        ON CONFLICT (user_id, model, usage_date)
        DO UPDATE SET
            input_tokens = user_model_usage_daily.input_tokens + EXCLUDED.input_tokens,
            output_tokens = user_model_usage_daily.output_tokens + EXCLUDED.output_tokens,
            cost = user_model_usage_daily.cost + EXCLUDED.cost,
            request_count = user_model_usage_daily.request_count + EXCLUDED.request_count,
            updated_at = NOW()
        "#,
        cursor,
        new_max
    )
    .execute(&mut *tx)
    .await?;

    sqlx::query!(
        "UPDATE user_model_usage_daily_cursor SET last_processed_id = $1, updated_at = NOW() WHERE id = TRUE",
        new_max
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

#[instrument(level = "debug", skip(pool), err)]
pub async fn get_realtime_tariffs(pool: &PgPool, account: Uuid, models: &[String]) -> Result<HashMap<String, (Decimal, Decimal)>> {
    let (mut class_aliases, mut classes) = (Vec::new(), Vec::new());
    for model in models {
        if let (alias, Some(class)) = split_served(model) {
            class_aliases.push(alias.to_string());
            classes.push(class.to_string());
        }
    }

    let class_rows = sqlx::query!(
        r#"
        SELECT dm.alias || ':' || c.class AS "model!",
               t.input_price_per_token as "input_price_per_token!", t.output_price_per_token as "output_price_per_token!"
        FROM unnest($2::text[], $3::text[]) AS c(alias, class)
        JOIN deployed_models dm ON dm.alias = c.alias
        CROSS JOIN LATERAL effective_model_tariff(dm.id, $1, 'realtime', NULL,
            CASE WHEN class_routes_active(dm.id) THEN c.class ELSE 'standard' END, NOW()) t
        "#,
        account,
        &class_aliases,
        &classes
    )
    .fetch_all(pool)
    .await?;

    let rows = sqlx::query!(
        r#"
        SELECT dm.alias, t.input_price_per_token as "input_price_per_token!", t.output_price_per_token as "output_price_per_token!"
        FROM deployed_models dm
        CROSS JOIN LATERAL (
            SELECT choices.input_price_per_token, choices.output_price_per_token FROM (
                SELECT user_id, serving_class, input_price_per_token, output_price_per_token
                FROM effective_model_display_tariff(dm.id, $1, 'realtime', NULL, NOW())
                UNION SELECT user_id, serving_class, input_price_per_token, output_price_per_token
                FROM effective_model_tariff(dm.id, $1, 'realtime', NULL, 'standard', NOW())
            ) choices
            ORDER BY CASE WHEN choices.user_id = $1 AND choices.serving_class IS NULL THEN 0
                          WHEN choices.user_id = $1 THEN 1 ELSE 2 END
            LIMIT 1
        ) t
        WHERE dm.alias = ANY($2)
        "#,
        account,
        models
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| (r.alias, (r.input_price_per_token, r.output_price_per_token)))
        .chain(
            class_rows
                .into_iter()
                .map(|r| (r.model, (r.input_price_per_token, r.output_price_per_token))),
        )
        .collect())
}

#[instrument(level = "debug", skip(pool), err)]
pub async fn get_user_model_breakdown(pool: &PgPool, user_id: Uuid) -> Result<Vec<ModelBreakdownEntry>> {
    let rows = sqlx::query_as!(
        ModelBreakdownRow,
        r#"
        SELECT model,
               SUM(input_tokens)::bigint  AS input_tokens,
               SUM(output_tokens)::bigint AS output_tokens,
               SUM(cost)                  AS cost,
               SUM(request_count)::bigint AS request_count
        FROM user_model_usage_daily
        WHERE user_id = $1
        GROUP BY model
        ORDER BY SUM(request_count) DESC
        "#,
        user_id
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            row.model.map(|model| ModelBreakdownEntry {
                model,
                input_tokens: row.input_tokens.unwrap_or(0),
                output_tokens: row.output_tokens.unwrap_or(0),
                cost: row.cost.map(|d| d.to_string()).unwrap_or_else(|| "0".to_string()),
                request_count: row.request_count.unwrap_or(0),
            })
        })
        .collect())
}


#[instrument(level = "debug", skip(pool), err)]
pub async fn get_user_model_breakdown_for_range(
    pool: &PgPool,
    user_id: Uuid,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<ModelBreakdownEntry>> {
    let rows = sqlx::query_as!(
        ModelBreakdownRow,
        r#"
        SELECT model,
               SUM(input_tokens)::bigint  AS input_tokens,
               SUM(output_tokens)::bigint AS output_tokens,
               SUM(cost)                  AS cost,
               SUM(request_count)::bigint AS request_count
        FROM user_model_usage_daily
        WHERE user_id = $1
          AND usage_date >= ($2::timestamptz AT TIME ZONE 'UTC')::date
          AND usage_date <= ($3::timestamptz AT TIME ZONE 'UTC')::date
        GROUP BY model
        ORDER BY SUM(request_count) DESC
        "#,
        user_id,
        start,
        end
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            row.model.map(|model| ModelBreakdownEntry {
                model,
                input_tokens: row.input_tokens.unwrap_or(0),
                output_tokens: row.output_tokens.unwrap_or(0),
                cost: row.cost.map(|d| d.to_string()).unwrap_or_else(|| "0".to_string()),
                request_count: row.request_count.unwrap_or(0),
            })
        })
        .collect())
}

#[instrument(level = "debug", skip(pool), err)]
pub async fn get_user_batch_count_for_range(pool: &PgPool, user_id: Uuid, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<i64> {
    let row = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count!"
        FROM batch_aggregates
        WHERE user_id = $1
          AND created_at >= $2 AND created_at <= $3
        "#,
        user_id,
        start,
        end
    )
    .fetch_one(pool)
    .await?;

    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use sqlx::PgPool;

    #[dwctl_test_macros::test]
    async fn list_http_analytics_matches_custom_id_exactly(pool: PgPool) {
        let target = "platform-tests-analytics-run-123";
        for (custom_id, timestamp) in [
            (target, Utc::now()),
            ("prefix-platform-tests-analytics-run-123-suffix", Utc::now() - Duration::seconds(1)),
            ("PLATFORM-TESTS-ANALYTICS-RUN-123", Utc::now() - Duration::seconds(2)),
        ] {
            sqlx::query(
                r#"
                INSERT INTO http_analytics (
                    instance_id, correlation_id, timestamp, uri, method, custom_id
                ) VALUES ($1, 1, $2, '/ai/v1/chat/completions', 'POST', $3)
                "#,
            )
            .bind(uuid::Uuid::new_v4())
            .bind(timestamp)
            .bind(custom_id)
            .execute(&pool)
            .await
            .unwrap();
        }

        let entries = list_http_analytics(
            &pool,
            0,
            100,
            true,
            HttpAnalyticsFilter {
                custom_id: Some(target.to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].custom_id.as_deref(), Some(target));
    }

    #[test]
    fn test_fill_missing_intervals_empty_input() {
        let time_series = vec![];
        let start_time = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end_time = start_time + Duration::hours(24);
        let result = fill_missing_intervals(time_series, start_time, end_time);

        assert!(!result.is_empty());
        assert!(result.iter().all(|p| p.requests == 0));
        assert!(result.iter().all(|p| p.input_tokens == 0));
        assert!(result.iter().all(|p| p.output_tokens == 0));
        assert!(result.iter().all(|p| p.avg_latency_ms.is_none()));
    }

    #[test]
    fn test_fill_missing_intervals_single_point() {
        let start_time = Utc.with_ymd_and_hms(2024, 1, 1, 10, 0, 0).unwrap();
        let end_time = start_time + Duration::hours(24);
        let time_series = vec![TimeSeriesPoint {
            timestamp: start_time,
            duration_minutes: 60,
            requests: 5,
            input_tokens: 100,
            output_tokens: 50,
            avg_latency_ms: Some(200.0),
            p95_latency_ms: Some(300.0),
            p99_latency_ms: Some(400.0),
        }];

        let result = fill_missing_intervals(time_series, start_time, end_time);

        assert!(!result.is_empty());
        assert_eq!(result[0].timestamp, start_time);
        assert_eq!(result[0].requests, 5);
    }

    #[test]
    fn test_fill_missing_intervals_gaps() {
        let start_time = Utc.with_ymd_and_hms(2024, 1, 1, 10, 0, 0).unwrap();
        let point1_time = start_time;
        let point2_time = start_time + Duration::hours(3);

        let time_series = vec![
            TimeSeriesPoint {
                timestamp: point1_time,
                duration_minutes: 60,
                requests: 5,
                input_tokens: 100,
                output_tokens: 50,
                avg_latency_ms: Some(200.0),
                p95_latency_ms: Some(300.0),
                p99_latency_ms: Some(400.0),
            },
            TimeSeriesPoint {
                timestamp: point2_time,
                duration_minutes: 60,
                requests: 3,
                input_tokens: 60,
                output_tokens: 30,
                avg_latency_ms: Some(150.0),
                p95_latency_ms: Some(250.0),
                p99_latency_ms: Some(350.0),
            },
        ];

        let end_time = start_time + Duration::hours(24);
        let result = fill_missing_intervals(time_series, start_time, end_time);

        let first_gap = result.iter().find(|p| p.timestamp == start_time + Duration::hours(1));
        assert!(first_gap.is_some());
        let gap_point = first_gap.unwrap();
        assert_eq!(gap_point.requests, 0);
        assert_eq!(gap_point.input_tokens, 0);
        assert_eq!(gap_point.output_tokens, 0);
        assert_eq!(gap_point.avg_latency_ms, None);
        assert_eq!(gap_point.p95_latency_ms, None);
        assert_eq!(gap_point.p99_latency_ms, None);
    }

    #[test]
    fn test_fill_missing_intervals_unsorted_input() {
        let start_time = Utc.with_ymd_and_hms(2024, 1, 1, 10, 0, 0).unwrap();
        let point1_time = start_time + Duration::hours(2);
        let point2_time = start_time;

        let time_series = vec![
            TimeSeriesPoint {
                timestamp: point1_time,
                duration_minutes: 60,
                requests: 3,
                input_tokens: 60,
                output_tokens: 30,
                avg_latency_ms: Some(150.0),
                p95_latency_ms: Some(250.0),
                p99_latency_ms: Some(350.0),
            },
            TimeSeriesPoint {
                timestamp: point2_time,
                duration_minutes: 60,
                requests: 5,
                input_tokens: 100,
                output_tokens: 50,
                avg_latency_ms: Some(200.0),
                p95_latency_ms: Some(300.0),
                p99_latency_ms: Some(400.0),
            },
        ];

        let end_time = start_time + Duration::hours(24);
        let result = fill_missing_intervals(time_series, start_time, end_time);

        let first_point = result.iter().find(|p| p.timestamp == start_time).unwrap();
        assert_eq!(first_point.requests, 5);

        let second_point = result.iter().find(|p| p.timestamp == start_time + Duration::hours(2)).unwrap();
        assert_eq!(second_point.requests, 3);
    }

    #[test]
    fn test_fill_missing_intervals_hour_truncation() {
        let start_time = Utc.with_ymd_and_hms(2024, 1, 1, 10, 30, 45).unwrap();
        let expected_start = Utc.with_ymd_and_hms(2024, 1, 1, 10, 0, 0).unwrap();

        let time_series = vec![TimeSeriesPoint {
            timestamp: expected_start,
            duration_minutes: 60,
            requests: 5,
            input_tokens: 100,
            output_tokens: 50,
            avg_latency_ms: Some(200.0),
            p95_latency_ms: Some(300.0),
            p99_latency_ms: Some(400.0),
        }];

        let end_time = start_time + Duration::hours(24);
        let result = fill_missing_intervals(time_series, start_time, end_time);

        assert_eq!(result[0].timestamp, expected_start);
    }

    async fn insert_test_analytics_data(
        pool: &PgPool,
        timestamp: DateTime<Utc>,
        model: &str,
        status_code: i32,
        duration_ms: f64,
        prompt_tokens: i64,
        completion_tokens: i64,
    ) {
        use uuid::Uuid;

        sqlx::query!(
            r#"
            INSERT INTO http_analytics (
                instance_id, correlation_id, timestamp, uri, method, status_code, duration_ms,
                model, prompt_tokens, completion_tokens, total_tokens
            ) VALUES ($1, $2, $3, '/ai/chat/completions', 'POST', $4, $5, $6, $7, $8, $9)
            "#,
            Uuid::new_v4(),
            1i64,
            timestamp,
            status_code,
            duration_ms as i64,
            model,
            prompt_tokens,
            completion_tokens,
            prompt_tokens + completion_tokens
        )
        .execute(pool)
        .await
        .expect("Failed to insert test analytics data");
    }

    #[dwctl_test_macros::test]
    async fn test_dashboard_read_timeout_is_scoped_to_its_transaction(pool: PgPool) {
        let single = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with((*pool.connect_options()).clone())
            .await
            .unwrap();
        let before: String = sqlx::query_scalar("SHOW statement_timeout").fetch_one(&single).await.unwrap();
        assert_eq!(before, "0");

        let mut tx = begin_dashboard_read(&single).await.unwrap();
        let inside: String = sqlx::query_scalar("SHOW statement_timeout").fetch_one(&mut *tx).await.unwrap();
        assert_eq!(inside, "30s");
        tx.commit().await.unwrap();

        let after: String = sqlx::query_scalar("SHOW statement_timeout").fetch_one(&single).await.unwrap();
        assert_eq!(after, "0");
    }

    #[dwctl_test_macros::test]
    async fn test_get_total_requests_no_filter(pool: PgPool) {
        let now = Utc::now();
        let one_hour_ago = now - Duration::hours(1);
        let two_hours_ago = now - Duration::hours(2);

        insert_test_analytics_data(&pool, one_hour_ago, "gpt-4", 200, 100.0, 50, 25).await;
        insert_test_analytics_data(&pool, one_hour_ago, "claude-3", 200, 150.0, 75, 35).await;
        insert_test_analytics_data(&pool, two_hours_ago, "gpt-4", 400, 200.0, 100, 50).await;

        let result = get_total_requests(&pool, two_hours_ago, now, None).await.unwrap();
        assert_eq!(result, 3);
    }

    #[dwctl_test_macros::test]
    async fn test_get_total_requests_with_model_filter(pool: PgPool) {
        let now = Utc::now();
        let one_hour_ago = now - Duration::hours(1);

        insert_test_analytics_data(&pool, one_hour_ago, "gpt-4", 200, 100.0, 50, 25).await;
        insert_test_analytics_data(&pool, one_hour_ago, "claude-3", 200, 150.0, 75, 35).await;
        insert_test_analytics_data(&pool, one_hour_ago, "gpt-4", 400, 200.0, 100, 50).await;

        let result = get_total_requests(&pool, one_hour_ago, now, Some("gpt-4")).await.unwrap();
        assert_eq!(result, 2);

        let result = get_total_requests(&pool, one_hour_ago, now, Some("claude-3")).await.unwrap();
        assert_eq!(result, 1);

        let result = get_total_requests(&pool, one_hour_ago, now, Some("nonexistent")).await.unwrap();
        assert_eq!(result, 0);
    }

    #[dwctl_test_macros::test]
    async fn test_get_time_series_basic(pool: PgPool) {
        let base_time = Utc.with_ymd_and_hms(2024, 1, 1, 10, 0, 0).unwrap();
        let hour1 = base_time;
        let hour2 = base_time + Duration::hours(1);

        insert_test_analytics_data(&pool, hour1, "gpt-4", 200, 100.0, 50, 25).await;
        insert_test_analytics_data(&pool, hour1, "gpt-4", 200, 200.0, 75, 35).await;
        insert_test_analytics_data(&pool, hour2, "gpt-4", 200, 150.0, 60, 30).await;

        let result = get_time_series(
            &pool,
            base_time,
            base_time + Duration::hours(24),
            Some("gpt-4"),
            TimeGranularity::Hour,
        )
        .await
        .unwrap();

        assert!(!result.is_empty());

        let hour1_point = result.iter().find(|p| p.timestamp == hour1);
        let hour2_point = result.iter().find(|p| p.timestamp == hour2);

        assert!(hour1_point.is_some());
        assert!(hour2_point.is_some());

        let h1 = hour1_point.unwrap();
        assert_eq!(h1.requests, 2);
        assert_eq!(h1.input_tokens, 125);
        assert_eq!(h1.output_tokens, 60);

        let h2 = hour2_point.unwrap();
        assert_eq!(h2.requests, 1);
        assert_eq!(h2.input_tokens, 60);
        assert_eq!(h2.output_tokens, 30);
    }

    #[dwctl_test_macros::test]
    async fn test_get_status_codes(pool: PgPool) {
        let now = Utc::now();
        let one_hour_ago = now - Duration::hours(1);

        insert_test_analytics_data(&pool, one_hour_ago, "gpt-4", 200, 100.0, 50, 25).await;
        insert_test_analytics_data(&pool, one_hour_ago, "gpt-4", 200, 150.0, 75, 35).await;
        insert_test_analytics_data(&pool, one_hour_ago, "gpt-4", 400, 200.0, 100, 50).await;
        insert_test_analytics_data(&pool, one_hour_ago, "claude-3", 500, 250.0, 80, 40).await;

        let result = get_status_codes(&pool, one_hour_ago, now, None).await.unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].status_code, Some(200));
        assert_eq!(result[0].status_count, Some(2));

        let result = get_status_codes(&pool, one_hour_ago, now, Some("gpt-4")).await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].status_code, Some(200));
        assert_eq!(result[0].status_count, Some(2));
        assert_eq!(result[1].status_code, Some(400));
        assert_eq!(result[1].status_count, Some(1));
    }

    #[dwctl_test_macros::test]
    async fn test_get_model_usage(pool: PgPool) {
        let now = Utc::now();
        let one_hour_ago = now - Duration::hours(1);

        insert_test_analytics_data(&pool, one_hour_ago, "gpt-4", 200, 100.0, 50, 25).await;
        insert_test_analytics_data(&pool, one_hour_ago, "gpt-4", 200, 200.0, 75, 35).await;
        insert_test_analytics_data(&pool, one_hour_ago, "claude-3", 200, 300.0, 60, 30).await;

        let result = get_model_usage(&pool, one_hour_ago, now).await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].model_name, Some("gpt-4".to_string()));
        assert_eq!(result[0].model_count, Some(2));
        assert_eq!(result[0].model_avg_latency_ms, Some(150.0));

        assert_eq!(result[1].model_name, Some("claude-3".to_string()));
        assert_eq!(result[1].model_count, Some(1));
        assert_eq!(result[1].model_avg_latency_ms, Some(300.0));
    }

    #[dwctl_test_macros::test]
    async fn test_get_requests_aggregate_full_integration(pool: PgPool) {
        let base_time = Utc.with_ymd_and_hms(2024, 1, 1, 10, 0, 0).unwrap();

        insert_test_analytics_data(&pool, base_time, "gpt-4", 200, 100.0, 50, 25).await;
        insert_test_analytics_data(&pool, base_time, "gpt-4", 200, 200.0, 75, 35).await;
        insert_test_analytics_data(&pool, base_time, "claude-3", 400, 300.0, 60, 30).await;
        insert_test_analytics_data(&pool, base_time + Duration::hours(1), "gpt-4", 500, 150.0, 40, 20).await;

        let result = get_requests_aggregate(&pool, base_time, base_time + Duration::hours(24), None)
            .await
            .unwrap();

        assert_eq!(result.total_requests, 4);
        assert!(result.model.is_none());

        assert_eq!(result.status_codes.len(), 3);
        let status_200 = result.status_codes.iter().find(|s| s.status == "200").unwrap();
        assert_eq!(status_200.count, 2);
        assert_eq!(status_200.percentage, 50.0);

        assert!(result.models.is_some());
        let models = result.models.as_ref().unwrap();
        assert_eq!(models.len(), 2);

        let gpt4 = models.iter().find(|m| m.model == "gpt-4").unwrap();
        assert_eq!(gpt4.count, 3);
        assert_eq!(gpt4.percentage, 75.0);
        assert_eq!(gpt4.avg_latency_ms, 150.0);

        assert!(!result.time_series.is_empty());
    }

    #[dwctl_test_macros::test]
    async fn test_get_requests_aggregate_with_model_filter(pool: PgPool) {
        let base_time = Utc::now() - Duration::hours(2);

        insert_test_analytics_data(&pool, base_time, "gpt-4", 200, 100.0, 50, 25).await;
        insert_test_analytics_data(&pool, base_time, "claude-3", 400, 300.0, 60, 30).await;

        let result = get_requests_aggregate(&pool, base_time, base_time + Duration::hours(24), Some("gpt-4"))
            .await
            .unwrap();

        assert_eq!(result.total_requests, 1);
        assert_eq!(result.model, Some("gpt-4".to_string()));

        assert!(result.models.is_none() || result.models.as_ref().unwrap().is_empty());

        assert_eq!(result.status_codes.len(), 1);
        assert_eq!(result.status_codes[0].status, "200");
    }

    #[dwctl_test_macros::test]
    async fn test_get_requests_aggregate_empty_database(pool: PgPool) {
        let base_time = Utc::now() - Duration::hours(24);
        let end_time = Utc::now();

        let result = get_requests_aggregate(&pool, base_time, end_time, None).await.unwrap();

        assert_eq!(result.total_requests, 0);
        assert_eq!(result.status_codes.len(), 0);
        assert!(result.models.is_none() || result.models.as_ref().unwrap().is_empty());

        assert!(!result.time_series.is_empty());
        assert!(result.time_series.iter().all(|p| p.requests == 0));
    }

    #[dwctl_test_macros::test]
    async fn test_percentage_calculations_precision(pool: PgPool) {
        let base_time = Utc::now() - Duration::hours(1);

        for _i in 0..7 {
            insert_test_analytics_data(&pool, base_time, "gpt-4", 200, 100.0, 50, 25).await;
        }
        for _i in 0..3 {
            insert_test_analytics_data(&pool, base_time, "claude-3", 400, 300.0, 60, 30).await;
        }

        let result = get_requests_aggregate(&pool, base_time, Utc::now(), None).await.unwrap();

        assert_eq!(result.total_requests, 10);

        let status_200 = result.status_codes.iter().find(|s| s.status == "200").unwrap();
        assert_eq!(status_200.percentage, 70.0);

        let status_400 = result.status_codes.iter().find(|s| s.status == "400").unwrap();
        assert_eq!(status_400.percentage, 30.0);

        let models = result.models.as_ref().unwrap();
        let gpt4 = models.iter().find(|m| m.model == "gpt-4").unwrap();
        assert_eq!(gpt4.percentage, 70.0);

        let claude3 = models.iter().find(|m| m.model == "claude-3").unwrap();
        assert_eq!(claude3.percentage, 30.0);
    }

    struct TestBatchAnalyticsData<'a> {
        fusillade_batch_id: Uuid,
        fusillade_request_id: Option<Uuid>,
        timestamp: DateTime<Utc>,
        model: &'a str,
        status_code: i32,
        duration_ms: f64,
        duration_to_first_byte_ms: Option<f64>,
        prompt_tokens: i64,
        completion_tokens: i64,
        reasoning_tokens: i64,
        input_price_per_token: Option<f64>,
        output_price_per_token: Option<f64>,
    }

    async fn insert_test_analytics_with_batch_id(pool: &PgPool, data: TestBatchAnalyticsData<'_>) {
        use crate::api::models::users::Role;
        use crate::pricing::list_price;
        use crate::test::utils::create_test_user;
        use rust_decimal::Decimal;
        use uuid::Uuid;

        let input_price = data.input_price_per_token.and_then(Decimal::from_f64_retain);
        let output_price = data.output_price_per_token.and_then(Decimal::from_f64_retain);
        let list_cost = list_price(data.prompt_tokens, data.completion_tokens, input_price, output_price);
        let list_cost = list_cost.unwrap_or(Decimal::ZERO);

        sqlx::query!(
            r#"
            INSERT INTO http_analytics (
                instance_id, correlation_id, timestamp, uri, method, status_code,
                duration_ms, duration_to_first_byte_ms, model, prompt_tokens,
                completion_tokens, reasoning_tokens, total_tokens, fusillade_batch_id, fusillade_request_id,
                input_price_per_token, output_price_per_token, total_cost
            ) VALUES ($1, $2, $3, '/ai/chat/completions', 'POST', $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            "#,
            Uuid::new_v4(),
            1i64,
            data.timestamp,
            data.status_code,
            data.duration_ms as i64,
            data.duration_to_first_byte_ms.map(|d| d as i64),
            data.model,
            data.prompt_tokens,
            data.completion_tokens,
            data.reasoning_tokens,
            data.prompt_tokens + data.completion_tokens,
            data.fusillade_batch_id,
            data.fusillade_request_id,
            input_price,
            output_price,
            Some(list_cost),
        )
        .execute(pool)
        .await
        .expect("Failed to insert test analytics data with batch ID");

        let user_id = create_test_user(pool, Role::StandardUser).await.id;
        let ttfb = data.duration_to_first_byte_ms.map(|d| d as i64);
        sqlx::query!(
            r#"
            INSERT INTO batch_aggregates (
                fusillade_batch_id, user_id, total_amount, transaction_count, max_seq, service_tier, total_requests,
                total_prompt_tokens, total_completion_tokens, total_reasoning_tokens, total_tokens,
                sum_duration_ms, count_duration_ms, sum_ttfb_ms, count_ttfb_ms, total_list_cost
            )
            VALUES ($1, $2, $3, 1, 1, 'batch', 1, $4, $5, $6, $7, $8, 1, $9, $10, $3)
            ON CONFLICT (fusillade_batch_id) DO UPDATE SET
                total_amount = batch_aggregates.total_amount + EXCLUDED.total_amount,
                transaction_count = batch_aggregates.transaction_count + EXCLUDED.transaction_count,
                total_requests = batch_aggregates.total_requests + EXCLUDED.total_requests,
                total_prompt_tokens = batch_aggregates.total_prompt_tokens + EXCLUDED.total_prompt_tokens,
                total_completion_tokens = batch_aggregates.total_completion_tokens + EXCLUDED.total_completion_tokens,
                total_reasoning_tokens = batch_aggregates.total_reasoning_tokens + EXCLUDED.total_reasoning_tokens,
                total_tokens = batch_aggregates.total_tokens + EXCLUDED.total_tokens,
                sum_duration_ms = batch_aggregates.sum_duration_ms + EXCLUDED.sum_duration_ms,
                count_duration_ms = batch_aggregates.count_duration_ms + EXCLUDED.count_duration_ms,
                sum_ttfb_ms = batch_aggregates.sum_ttfb_ms + EXCLUDED.sum_ttfb_ms,
                count_ttfb_ms = batch_aggregates.count_ttfb_ms + EXCLUDED.count_ttfb_ms,
                total_list_cost = batch_aggregates.total_list_cost + EXCLUDED.total_list_cost
            "#,
            data.fusillade_batch_id,
            user_id,
            list_cost,
            data.prompt_tokens,
            data.completion_tokens,
            data.reasoning_tokens,
            data.prompt_tokens + data.completion_tokens,
            data.duration_ms as i64,
            ttfb.unwrap_or(0),
            i64::from(ttfb.is_some()),
        )
        .execute(pool)
        .await
        .expect("Failed to fold test batch_aggregates row");
    }

    #[dwctl_test_macros::test]
    async fn test_get_batch_analytics_single_request(pool: PgPool) {
        let batch_id = Uuid::new_v4();
        let now = Utc::now();

        insert_test_analytics_with_batch_id(
            &pool,
            TestBatchAnalyticsData {
                fusillade_batch_id: batch_id,
                fusillade_request_id: Some(Uuid::new_v4()),
                timestamp: now,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 150.0,
                duration_to_first_byte_ms: Some(50.0),
                prompt_tokens: 100,
                completion_tokens: 50,
                reasoning_tokens: 20,
                input_price_per_token: Some(0.00001),
                output_price_per_token: Some(0.00003),
            },
        )
        .await;

        let result = get_batch_analytics(&pool, &batch_id)
            .await
            .unwrap()
            .expect("batch has batch_aggregates analytics");

        assert_eq!(result.total_requests, 1);
        assert_eq!(result.total_prompt_tokens, 100);
        assert_eq!(result.total_completion_tokens, 50);
        assert_eq!(result.total_reasoning_tokens, Some(20));
        assert_eq!(result.total_tokens, 150);
        assert_eq!(result.avg_duration_ms, Some(150.0));
        assert_eq!(result.avg_ttfb_ms, Some(50.0));

        let cost = result.total_cost.unwrap();
        let cost_f64: f64 = cost.parse().unwrap();
        assert!((cost_f64 - 0.0025).abs() < 0.00001);
    }

    #[dwctl_test_macros::test]
    async fn test_get_batch_analytics_serves_billed_cost_not_list_price(pool: PgPool) {
        use crate::api::models::users::Role;
        use crate::test::utils::create_test_user;

        let batch_id = Uuid::new_v4();
        let user_id = create_test_user(&pool, Role::StandardUser).await.id;
        sqlx::query!(
            r#"
            INSERT INTO batch_aggregates (
                fusillade_batch_id, user_id, total_amount, transaction_count, max_seq, service_tier,
                total_requests, total_prompt_tokens, total_completion_tokens, total_reasoning_tokens,
                total_tokens, sum_duration_ms, count_duration_ms, sum_ttfb_ms, count_ttfb_ms,
                total_list_cost
            )
            VALUES ($1, $2, 0.0199, 1000, 1, 'batch', 1000, 1433730, 50000, 0, 1483730, 500000, 1000, 0, 0, 0.0422)
            "#,
            batch_id,
            user_id,
        )
        .execute(&pool)
        .await
        .expect("insert batch_aggregates row");

        let result = get_batch_analytics(&pool, &batch_id)
            .await
            .unwrap()
            .expect("batch has batch_aggregates analytics");

        let billed: f64 = result.total_cost.expect("billed cost present").parse().unwrap();
        let list: f64 = result.total_list_cost.expect("list cost present").parse().unwrap();
        assert!((billed - 0.0199).abs() < 1e-9, "total_cost must be the BILLED amount, got {billed}");
        assert!((list - 0.0422).abs() < 1e-9, "total_list_cost must be the list price, got {list}");
        assert!(billed < list, "cache-discounted billed cost sits below list price");
    }

    #[dwctl_test_macros::test]
    async fn test_get_batch_analytics_multiple_requests(pool: PgPool) {
        let batch_id = Uuid::new_v4();
        let now = Utc::now();

        insert_test_analytics_with_batch_id(
            &pool,
            TestBatchAnalyticsData {
                fusillade_batch_id: batch_id,
                fusillade_request_id: Some(Uuid::new_v4()),
                timestamp: now,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 100.0,
                duration_to_first_byte_ms: Some(30.0),
                prompt_tokens: 50,
                completion_tokens: 25,
                reasoning_tokens: 10,
                input_price_per_token: Some(0.00001),
                output_price_per_token: Some(0.00003),
            },
        )
        .await;

        insert_test_analytics_with_batch_id(
            &pool,
            TestBatchAnalyticsData {
                fusillade_batch_id: batch_id,
                fusillade_request_id: Some(Uuid::new_v4()),
                timestamp: now,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 200.0,
                duration_to_first_byte_ms: Some(70.0),
                prompt_tokens: 100,
                completion_tokens: 50,
                reasoning_tokens: 20,
                input_price_per_token: Some(0.00001),
                output_price_per_token: Some(0.00003),
            },
        )
        .await;

        insert_test_analytics_with_batch_id(
            &pool,
            TestBatchAnalyticsData {
                fusillade_batch_id: batch_id,
                fusillade_request_id: Some(Uuid::new_v4()),
                timestamp: now,
                model: "claude-3",
                status_code: 200,
                duration_ms: 150.0,
                duration_to_first_byte_ms: Some(40.0),
                prompt_tokens: 75,
                completion_tokens: 35,
                reasoning_tokens: 5,
                input_price_per_token: Some(0.00002),
                output_price_per_token: Some(0.00004),
            },
        )
        .await;

        let result = get_batch_analytics(&pool, &batch_id)
            .await
            .unwrap()
            .expect("batch has batch_aggregates analytics");

        assert_eq!(result.total_requests, 3);
        assert_eq!(result.total_prompt_tokens, 225);
        assert_eq!(result.total_completion_tokens, 110);
        assert_eq!(result.total_reasoning_tokens, Some(35));
        assert_eq!(result.total_tokens, 335);

        assert_eq!(result.avg_duration_ms, Some(150.0));

        let avg_ttfb = result.avg_ttfb_ms.unwrap();
        assert!((avg_ttfb - 46.666666666666664).abs() < 0.0001);

        let cost = result.total_cost.unwrap();
        let cost_f64: f64 = cost.parse().unwrap();
        assert!((cost_f64 - 0.00665).abs() < 0.00001);
    }

    #[dwctl_test_macros::test]
    async fn test_get_batch_analytics_nonexistent_batch_id(pool: PgPool) {
        let nonexistent_batch_id = Uuid::new_v4();

        let result = get_batch_analytics(&pool, &nonexistent_batch_id).await.unwrap();

        assert!(result.is_none());
    }

    #[dwctl_test_macros::test]
    async fn test_get_batch_analytics_filters_by_batch_id(pool: PgPool) {
        let batch_id_1 = Uuid::new_v4();
        let batch_id_2 = Uuid::new_v4();
        let now = Utc::now();

        insert_test_analytics_with_batch_id(
            &pool,
            TestBatchAnalyticsData {
                fusillade_batch_id: batch_id_1,
                fusillade_request_id: Some(Uuid::new_v4()),
                timestamp: now,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 100.0,
                duration_to_first_byte_ms: Some(30.0),
                prompt_tokens: 50,
                completion_tokens: 25,
                reasoning_tokens: 0,
                input_price_per_token: Some(0.00001),
                output_price_per_token: Some(0.00003),
            },
        )
        .await;

        insert_test_analytics_with_batch_id(
            &pool,
            TestBatchAnalyticsData {
                fusillade_batch_id: batch_id_2,
                fusillade_request_id: Some(Uuid::new_v4()),
                timestamp: now,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 200.0,
                duration_to_first_byte_ms: Some(40.0),
                prompt_tokens: 100,
                completion_tokens: 50,
                reasoning_tokens: 0,
                input_price_per_token: Some(0.00001),
                output_price_per_token: Some(0.00003),
            },
        )
        .await;

        let result = get_batch_analytics(&pool, &batch_id_1)
            .await
            .unwrap()
            .expect("batch has batch_aggregates analytics");

        assert_eq!(result.total_requests, 1);
        assert_eq!(result.total_prompt_tokens, 50);
        assert_eq!(result.total_completion_tokens, 25);
        assert_eq!(result.total_tokens, 75);
        assert_eq!(result.avg_duration_ms, Some(100.0));
    }

    #[dwctl_test_macros::test]
    async fn test_get_batch_analytics_missing_optional_fields(pool: PgPool) {
        let batch_id = Uuid::new_v4();
        let now = Utc::now();

        insert_test_analytics_with_batch_id(
            &pool,
            TestBatchAnalyticsData {
                fusillade_batch_id: batch_id,
                fusillade_request_id: Some(Uuid::new_v4()),
                timestamp: now,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 100.0,
                duration_to_first_byte_ms: None,
                prompt_tokens: 50,
                completion_tokens: 25,
                reasoning_tokens: 0,
                input_price_per_token: None,
                output_price_per_token: None,
            },
        )
        .await;

        let result = get_batch_analytics(&pool, &batch_id)
            .await
            .unwrap()
            .expect("batch has batch_aggregates analytics");

        assert_eq!(result.total_requests, 1);
        assert_eq!(result.total_prompt_tokens, 50);
        assert_eq!(result.total_completion_tokens, 25);
        assert_eq!(result.total_tokens, 75);
        assert_eq!(result.avg_duration_ms, Some(100.0));
        assert_eq!(result.avg_ttfb_ms, None);

        let cost = result.total_cost.unwrap();
        let cost_f64: f64 = cost.parse().unwrap();
        assert_eq!(cost_f64, 0.0);
    }

    #[dwctl_test_macros::test]
    async fn test_get_batch_analytics_multiple_requests_same_batch(pool: PgPool) {
        let batch_id = Uuid::new_v4();
        let other_batch_id = Uuid::new_v4();
        let now = Utc::now();

        insert_test_analytics_with_batch_id(
            &pool,
            TestBatchAnalyticsData {
                fusillade_batch_id: batch_id,
                fusillade_request_id: Some(Uuid::new_v4()),
                timestamp: now,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 100.0,
                duration_to_first_byte_ms: Some(30.0),
                prompt_tokens: 50,
                completion_tokens: 25,
                reasoning_tokens: 0,
                input_price_per_token: Some(0.00001),
                output_price_per_token: Some(0.00003),
            },
        )
        .await;

        insert_test_analytics_with_batch_id(
            &pool,
            TestBatchAnalyticsData {
                fusillade_batch_id: batch_id,
                fusillade_request_id: Some(Uuid::new_v4()),
                timestamp: now,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 150.0,
                duration_to_first_byte_ms: Some(45.0),
                prompt_tokens: 75,
                completion_tokens: 30,
                reasoning_tokens: 0,
                input_price_per_token: Some(0.00001),
                output_price_per_token: Some(0.00003),
            },
        )
        .await;

        insert_test_analytics_with_batch_id(
            &pool,
            TestBatchAnalyticsData {
                fusillade_batch_id: other_batch_id,
                fusillade_request_id: Some(Uuid::new_v4()),
                timestamp: now,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 200.0,
                duration_to_first_byte_ms: Some(40.0),
                prompt_tokens: 100,
                completion_tokens: 50,
                reasoning_tokens: 0,
                input_price_per_token: Some(0.00001),
                output_price_per_token: Some(0.00003),
            },
        )
        .await;

        let result = get_batch_analytics(&pool, &batch_id)
            .await
            .unwrap()
            .expect("batch has batch_aggregates analytics");

        assert_eq!(result.total_requests, 2);
        assert_eq!(result.total_prompt_tokens, 125);
        assert_eq!(result.total_completion_tokens, 55);
        assert_eq!(result.avg_duration_ms, Some(125.0));
    }

    #[dwctl_test_macros::test]
    async fn test_get_batches_analytics_bulk_empty_input(pool: PgPool) {
        let result = get_batches_analytics_bulk(&pool, &[]).await.unwrap();
        assert!(result.is_empty());
    }

    #[dwctl_test_macros::test]
    async fn test_get_batches_analytics_bulk_multiple_batches(pool: PgPool) {
        let batch_id_1 = Uuid::new_v4();
        let batch_id_2 = Uuid::new_v4();
        let batch_id_3 = Uuid::new_v4();
        let now = Utc::now();

        insert_test_analytics_with_batch_id(
            &pool,
            TestBatchAnalyticsData {
                fusillade_batch_id: batch_id_1,
                fusillade_request_id: Some(Uuid::new_v4()),
                timestamp: now,
                model: "gpt-4",
                status_code: 200,
                duration_ms: 100.0,
                duration_to_first_byte_ms: Some(20.0),
                prompt_tokens: 50,
                completion_tokens: 25,
                reasoning_tokens: 0,
                input_price_per_token: Some(0.00001),
                output_price_per_token: Some(0.00003),
            },
        )
        .await;

        insert_test_analytics_with_batch_id(
            &pool,
            TestBatchAnalyticsData {
                fusillade_batch_id: batch_id_2,
                fusillade_request_id: Some(Uuid::new_v4()),
                timestamp: now,
                model: "gpt-3.5-turbo",
                status_code: 200,
                duration_ms: 50.0,
                duration_to_first_byte_ms: Some(10.0),
                prompt_tokens: 100,
                completion_tokens: 50,
                reasoning_tokens: 0,
                input_price_per_token: Some(0.000001),
                output_price_per_token: Some(0.000002),
            },
        )
        .await;
        insert_test_analytics_with_batch_id(
            &pool,
            TestBatchAnalyticsData {
                fusillade_batch_id: batch_id_2,
                fusillade_request_id: Some(Uuid::new_v4()),
                timestamp: now,
                model: "gpt-3.5-turbo",
                status_code: 200,
                duration_ms: 60.0,
                duration_to_first_byte_ms: Some(15.0),
                prompt_tokens: 200,
                completion_tokens: 100,
                reasoning_tokens: 0,
                input_price_per_token: Some(0.000001),
                output_price_per_token: Some(0.000002),
            },
        )
        .await;

        let result = get_batches_analytics_bulk(&pool, &[batch_id_1, batch_id_2, batch_id_3])
            .await
            .unwrap();

        assert_eq!(result.len(), 3);

        let analytics_1 = result.get(&batch_id_1).unwrap();
        assert_eq!(analytics_1.total_requests, 1);
        assert_eq!(analytics_1.total_prompt_tokens, 50);
        assert_eq!(analytics_1.total_completion_tokens, 25);

        let analytics_2 = result.get(&batch_id_2).unwrap();
        assert_eq!(analytics_2.total_requests, 2);
        assert_eq!(analytics_2.total_prompt_tokens, 300);
        assert_eq!(analytics_2.total_completion_tokens, 150);
        assert_eq!(analytics_2.avg_duration_ms, Some(55.0));

        let analytics_3 = result.get(&batch_id_3).unwrap();
        assert_eq!(analytics_3.total_requests, 0);
        assert_eq!(analytics_3.total_prompt_tokens, 0);
        assert_eq!(analytics_3.total_completion_tokens, 0);
        assert!(analytics_3.avg_duration_ms.is_none());
    }

    struct UsageAnalyticsParams<'a> {
        user_id: Uuid,
        model: &'a str,
        prompt_tokens: i64,
        completion_tokens: i64,
        total_cost: f64,
        timestamp: DateTime<Utc>,
        fusillade_batch_id: Option<Uuid>,
        status_code: i32,
    }

    async fn insert_usage_analytics(pool: &PgPool, params: UsageAnalyticsParams<'_>) {
        insert_usage_analytics_with_status(
            pool,
            UsageAnalyticsParams {
                status_code: 200,
                ..params
            },
        )
        .await;
    }

    async fn insert_usage_analytics_with_status(pool: &PgPool, params: UsageAnalyticsParams<'_>) {
        use rust_decimal::Decimal;

        let UsageAnalyticsParams {
            user_id,
            model,
            prompt_tokens,
            completion_tokens,
            total_cost,
            timestamp,
            fusillade_batch_id,
            status_code,
        } = params;

        sqlx::query!(
            r#"
            INSERT INTO http_analytics (
                instance_id, correlation_id, timestamp, uri, method, status_code,
                duration_ms, model, prompt_tokens, completion_tokens, total_tokens,
                user_id, fusillade_batch_id,
                input_price_per_token, output_price_per_token, total_cost
            ) VALUES (
                $1, $2, $3, '/ai/chat/completions', 'POST', $12,
                100, $4, $5, $6, $7,
                $8, $9,
                $10, $11, $13
            )
            "#,
            Uuid::new_v4(),
            1i64,
            timestamp,
            model,
            prompt_tokens,
            completion_tokens,
            prompt_tokens + completion_tokens,
            user_id,
            fusillade_batch_id,
            {
                let total_tokens = (prompt_tokens + completion_tokens) as f64;
                if total_tokens > 0.0 {
                    Decimal::from_f64_retain(total_cost / total_tokens)
                } else {
                    Some(Decimal::ZERO)
                }
            },
            {
                let total_tokens = (prompt_tokens + completion_tokens) as f64;
                if total_tokens > 0.0 {
                    Decimal::from_f64_retain(total_cost / total_tokens)
                } else {
                    Some(Decimal::ZERO)
                }
            },
            status_code,
            Decimal::from_f64_retain(total_cost),
        )
        .execute(pool)
        .await
        .expect("Failed to insert usage analytics data");
    }

    async fn create_usage_test_user(pool: &PgPool) -> Uuid {
        let user_id = Uuid::new_v4();
        let username = format!("test-{}", &user_id.to_string()[..8]);
        sqlx::query!(
            r#"
            INSERT INTO users (id, username, email, auth_source, display_name)
            VALUES ($1, $2, $3, 'native', 'Test User')
            "#,
            user_id,
            username,
            format!("{}@test.com", user_id),
        )
        .execute(pool)
        .await
        .expect("Failed to create test user");
        user_id
    }

    #[dwctl_test_macros::test]
    async fn test_all_time_breakdown_includes_realtime_requests(pool: PgPool) {
        let user_id = create_usage_test_user(&pool).await;
        let now = Utc::now();

        insert_usage_analytics(
            &pool,
            UsageAnalyticsParams {
                user_id,
                model: "gpt-4",
                prompt_tokens: 100,
                completion_tokens: 50,
                total_cost: 0.0,
                timestamp: now,
                fusillade_batch_id: None,
                status_code: 200,
            },
        )
        .await;
        insert_usage_analytics(
            &pool,
            UsageAnalyticsParams {
                user_id,
                model: "gpt-4",
                prompt_tokens: 200,
                completion_tokens: 100,
                total_cost: 0.0,
                timestamp: now,
                fusillade_batch_id: Some(Uuid::new_v4()),
                status_code: 200,
            },
        )
        .await;

        refresh_user_model_usage_daily(&pool).await.unwrap();

        let breakdown = get_user_model_breakdown(&pool, user_id).await.unwrap();
        assert_eq!(breakdown.len(), 1);
        assert_eq!(breakdown[0].model, "gpt-4");
        assert_eq!(breakdown[0].request_count, 2);
        assert_eq!(breakdown[0].input_tokens, 300);
        assert_eq!(breakdown[0].output_tokens, 150);
    }

    #[dwctl_test_macros::test]
    async fn test_refresh_user_model_usage_daily_aggregates_per_day(pool: PgPool) {
        let user_id = create_usage_test_user(&pool).await;
        let day1_am = Utc.with_ymd_and_hms(2026, 1, 1, 10, 0, 0).unwrap();
        let day1_pm = Utc.with_ymd_and_hms(2026, 1, 1, 20, 0, 0).unwrap();
        let day2 = Utc.with_ymd_and_hms(2026, 1, 2, 9, 0, 0).unwrap();

        for (ts, prompt, completion) in [(day1_am, 100, 50), (day1_pm, 200, 100), (day2, 10, 5)] {
            insert_usage_analytics(
                &pool,
                UsageAnalyticsParams {
                    user_id,
                    model: "gpt-4",
                    prompt_tokens: prompt,
                    completion_tokens: completion,
                    total_cost: 0.0,
                    timestamp: ts,
                    fusillade_batch_id: None,
                    status_code: 200,
                },
            )
            .await;
        }
        insert_usage_analytics_with_status(
            &pool,
            UsageAnalyticsParams {
                user_id,
                model: "gpt-4",
                prompt_tokens: 999,
                completion_tokens: 999,
                total_cost: 0.0,
                timestamp: day2,
                fusillade_batch_id: None,
                status_code: 500,
            },
        )
        .await;

        refresh_user_model_usage_daily(&pool).await.unwrap();

        let rows = sqlx::query!(
            r#"
            SELECT usage_date, input_tokens, output_tokens, request_count
            FROM user_model_usage_daily
            WHERE user_id = $1 AND model = 'gpt-4'
            ORDER BY usage_date
            "#,
            user_id
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(rows.len(), 2, "one row per distinct day");
        assert_eq!(rows[0].usage_date, day1_am.date_naive());
        assert_eq!(rows[0].input_tokens, 300);
        assert_eq!(rows[0].output_tokens, 150);
        assert_eq!(rows[0].request_count, 2);
        assert_eq!(rows[1].usage_date, day2.date_naive());
        assert_eq!(rows[1].input_tokens, 10);
        assert_eq!(rows[1].output_tokens, 5);
        assert_eq!(rows[1].request_count, 1);
    }

    #[dwctl_test_macros::test]
    async fn test_refresh_user_model_usage_daily_is_additive_across_runs(pool: PgPool) {
        let user_id = create_usage_test_user(&pool).await;
        let day = Utc.with_ymd_and_hms(2026, 2, 3, 12, 0, 0).unwrap();

        let insert = |prompt: i64, completion: i64| {
            let pool = pool.clone();
            async move {
                insert_usage_analytics(
                    &pool,
                    UsageAnalyticsParams {
                        user_id,
                        model: "gpt-4",
                        prompt_tokens: prompt,
                        completion_tokens: completion,
                        total_cost: 0.0,
                        timestamp: day,
                        fusillade_batch_id: None,
                        status_code: 200,
                    },
                )
                .await;
            }
        };

        insert(100, 50).await;
        refresh_user_model_usage_daily(&pool).await.unwrap();
        insert(20, 10).await;
        refresh_user_model_usage_daily(&pool).await.unwrap();

        let row = sqlx::query!(
            r#"
            SELECT input_tokens, output_tokens, request_count
            FROM user_model_usage_daily
            WHERE user_id = $1 AND model = 'gpt-4' AND usage_date = $2
            "#,
            user_id,
            day.date_naive(),
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.input_tokens, 120);
        assert_eq!(row.output_tokens, 60);
        assert_eq!(row.request_count, 2);
    }

    #[dwctl_test_macros::test]
    async fn test_get_user_model_breakdown_for_range_includes_realtime_requests(pool: PgPool) {
        let user_id = create_usage_test_user(&pool).await;
        let now = Utc::now();
        let one_hour_ago = now - Duration::hours(1);

        insert_usage_analytics(
            &pool,
            UsageAnalyticsParams {
                user_id,
                model: "claude-3",
                prompt_tokens: 80,
                completion_tokens: 40,
                total_cost: 0.0,
                timestamp: now,
                fusillade_batch_id: None,
                status_code: 200,
            },
        )
        .await;
        insert_usage_analytics(
            &pool,
            UsageAnalyticsParams {
                user_id,
                model: "claude-3",
                prompt_tokens: 120,
                completion_tokens: 60,
                total_cost: 0.0,
                timestamp: now,
                fusillade_batch_id: Some(Uuid::new_v4()),
                status_code: 200,
            },
        )
        .await;

        refresh_user_model_usage_daily(&pool).await.unwrap();

        let breakdown = get_user_model_breakdown_for_range(&pool, user_id, one_hour_ago, now).await.unwrap();
        assert_eq!(breakdown.len(), 1);
        assert_eq!(breakdown[0].model, "claude-3");
        assert_eq!(breakdown[0].request_count, 2);
        assert_eq!(breakdown[0].input_tokens, 200);
        assert_eq!(breakdown[0].output_tokens, 100);
    }

    #[dwctl_test_macros::test]
    async fn test_get_user_model_breakdown_for_range_spans_utc_midnight(pool: PgPool) {
        let user_id = create_usage_test_user(&pool).await;
        let before_midnight = DateTime::parse_from_rfc3339("2025-03-10T23:30:00Z").unwrap().with_timezone(&Utc);
        let after_midnight = DateTime::parse_from_rfc3339("2025-03-11T00:30:00Z").unwrap().with_timezone(&Utc);

        for (ts, prompt, completion) in [(before_midnight, 10, 5), (after_midnight, 20, 7)] {
            insert_usage_analytics(
                &pool,
                UsageAnalyticsParams {
                    user_id,
                    model: "claude-3",
                    prompt_tokens: prompt,
                    completion_tokens: completion,
                    total_cost: 0.0,
                    timestamp: ts,
                    fusillade_batch_id: None,
                    status_code: 200,
                },
            )
            .await;
        }

        refresh_user_model_usage_daily(&pool).await.unwrap();

        let start = DateTime::parse_from_rfc3339("2025-03-10T23:00:00Z").unwrap().with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2025-03-11T01:00:00Z").unwrap().with_timezone(&Utc);
        let breakdown = get_user_model_breakdown_for_range(&pool, user_id, start, end).await.unwrap();

        assert_eq!(breakdown.len(), 1);
        assert_eq!(breakdown[0].request_count, 2, "both sides of UTC midnight must be counted");
        assert_eq!(breakdown[0].input_tokens, 30);
        assert_eq!(breakdown[0].output_tokens, 12);
    }

    async fn insert_batch_aggregate(pool: &PgPool, user_id: Uuid, created_at: DateTime<Utc>, max_seq: i64) {
        sqlx::query!(
            r#"
            INSERT INTO batch_aggregates (fusillade_batch_id, user_id, total_amount, transaction_count, max_seq, created_at)
            VALUES ($1, $2, 0, 1, $3, $4)
            "#,
            Uuid::new_v4(),
            user_id,
            max_seq,
            created_at,
        )
        .execute(pool)
        .await
        .expect("Failed to insert batch_aggregate");
    }

    #[dwctl_test_macros::test]
    async fn test_batch_count_for_range_counts_batches_in_window(pool: PgPool) {
        let user_id = create_usage_test_user(&pool).await;
        let now = Utc::now();
        let one_hour_ago = now - Duration::hours(1);

        insert_batch_aggregate(&pool, user_id, now, 1).await;
        insert_batch_aggregate(&pool, user_id, now, 2).await;

        let count = get_user_batch_count_for_range(&pool, user_id, one_hour_ago, now).await.unwrap();
        assert_eq!(count, 2);
    }

    #[dwctl_test_macros::test]
    async fn test_all_time_breakdown_excludes_errors(pool: PgPool) {
        let user_id = create_usage_test_user(&pool).await;
        let now = Utc::now();

        insert_usage_analytics(
            &pool,
            UsageAnalyticsParams {
                user_id,
                model: "gpt-4",
                prompt_tokens: 100,
                completion_tokens: 50,
                total_cost: 0.0,
                timestamp: now,
                fusillade_batch_id: None,
                status_code: 200,
            },
        )
        .await;
        insert_usage_analytics(
            &pool,
            UsageAnalyticsParams {
                user_id,
                model: "gpt-4",
                prompt_tokens: 200,
                completion_tokens: 100,
                total_cost: 0.0,
                timestamp: now,
                fusillade_batch_id: None,
                status_code: 200,
            },
        )
        .await;
        insert_usage_analytics_with_status(
            &pool,
            UsageAnalyticsParams {
                user_id,
                model: "gpt-4",
                prompt_tokens: 80,
                completion_tokens: 0,
                total_cost: 0.0,
                timestamp: now,
                fusillade_batch_id: None,
                status_code: 400,
            },
        )
        .await;
        insert_usage_analytics_with_status(
            &pool,
            UsageAnalyticsParams {
                user_id,
                model: "gpt-4",
                prompt_tokens: 90,
                completion_tokens: 0,
                total_cost: 0.0,
                timestamp: now,
                fusillade_batch_id: None,
                status_code: 500,
            },
        )
        .await;
        insert_usage_analytics_with_status(
            &pool,
            UsageAnalyticsParams {
                user_id,
                model: "gpt-4",
                prompt_tokens: 70,
                completion_tokens: 0,
                total_cost: 0.0,
                timestamp: now,
                fusillade_batch_id: None,
                status_code: 429,
            },
        )
        .await;

        refresh_user_model_usage_daily(&pool).await.unwrap();

        let breakdown = get_user_model_breakdown(&pool, user_id).await.unwrap();
        assert_eq!(breakdown.len(), 1);
        assert_eq!(breakdown[0].model, "gpt-4");
        assert_eq!(breakdown[0].request_count, 2);
        assert_eq!(breakdown[0].input_tokens, 300);
        assert_eq!(breakdown[0].output_tokens, 150);
    }

    #[dwctl_test_macros::test]
    async fn test_get_user_model_breakdown_for_range_excludes_errors(pool: PgPool) {
        let user_id = create_usage_test_user(&pool).await;
        let now = Utc::now();
        let one_hour_ago = now - Duration::hours(1);

        insert_usage_analytics(
            &pool,
            UsageAnalyticsParams {
                user_id,
                model: "claude-3",
                prompt_tokens: 80,
                completion_tokens: 40,
                total_cost: 0.0,
                timestamp: now,
                fusillade_batch_id: None,
                status_code: 200,
            },
        )
        .await;
        insert_usage_analytics_with_status(
            &pool,
            UsageAnalyticsParams {
                user_id,
                model: "claude-3",
                prompt_tokens: 60,
                completion_tokens: 0,
                total_cost: 0.0,
                timestamp: now,
                fusillade_batch_id: None,
                status_code: 400,
            },
        )
        .await;
        insert_usage_analytics_with_status(
            &pool,
            UsageAnalyticsParams {
                user_id,
                model: "claude-3",
                prompt_tokens: 70,
                completion_tokens: 0,
                total_cost: 0.0,
                timestamp: now,
                fusillade_batch_id: None,
                status_code: 502,
            },
        )
        .await;

        refresh_user_model_usage_daily(&pool).await.unwrap();

        let breakdown = get_user_model_breakdown_for_range(&pool, user_id, one_hour_ago, now).await.unwrap();
        assert_eq!(breakdown.len(), 1);
        assert_eq!(breakdown[0].model, "claude-3");
        assert_eq!(breakdown[0].request_count, 1);
        assert_eq!(breakdown[0].input_tokens, 80);
        assert_eq!(breakdown[0].output_tokens, 40);
    }

    #[dwctl_test_macros::test]
    async fn test_batch_count_for_range_respects_window(pool: PgPool) {
        let user_id = create_usage_test_user(&pool).await;
        let now = Utc::now();
        let one_hour_ago = now - Duration::hours(1);

        insert_batch_aggregate(&pool, user_id, now, 1).await;
        insert_batch_aggregate(&pool, user_id, now - Duration::days(2), 2).await;

        let count = get_user_batch_count_for_range(&pool, user_id, one_hour_ago, now).await.unwrap();
        assert_eq!(count, 1);
    }
}
