
use std::collections::HashMap;
use tracing::info;

const MAX_WINDOW_SECONDS: i64 = 3_153_600_000;

#[derive(Debug)]
pub struct SlaCapacityCheckResult {
    pub has_capacity: bool,
    pub overloaded_models: HashMap<String, i64>,
}

pub fn check_sla_capacity(
    file_model_counts: &HashMap<String, i64>,
    pending_counts: &HashMap<String, HashMap<String, i64>>,
    model_throughputs: &HashMap<String, f32>,
    default_throughput: f32,
    completion_window: &str,
    relaxation_factor: f32,
) -> SlaCapacityCheckResult {
    let window_seconds = parse_window_to_seconds(completion_window);
    let mut overloaded_models = HashMap::new();

    for (model_alias, &new_requests) in file_model_counts {
        let pending = pending_counts
            .get(model_alias)
            .and_then(|windows| windows.get(completion_window))
            .copied()
            .unwrap_or(0);

        let throughput = model_throughputs.get(model_alias).copied().unwrap_or(default_throughput).max(0.0);

        let capacity_f64 = (throughput as f64) * (window_seconds as f64) * (relaxation_factor as f64);
        let effective_capacity = if capacity_f64 >= i64::MAX as f64 {
            i64::MAX
        } else if capacity_f64 <= 0.0 {
            0
        } else {
            capacity_f64 as i64
        };

        let total_requests = pending + new_requests;
        if total_requests > effective_capacity {
            let deficit = total_requests - effective_capacity;
            info!(
                model = %model_alias,
                pending = pending,
                new_requests = new_requests,
                capacity = capacity_f64,
                effective_capacity = effective_capacity,
                throughput = throughput,
                window = completion_window,
                deficit = deficit,
                "Model exceeds completion window capacity"
            );
            overloaded_models.insert(model_alias.clone(), deficit);
        }
    }

    SlaCapacityCheckResult {
        has_capacity: overloaded_models.is_empty(),
        overloaded_models,
    }
}

pub(super) fn parse_window_to_seconds(window: &str) -> i64 {
    let parsed = if window.ends_with('h') {
        window.trim_end_matches('h').parse::<i64>().ok().map(|h| h * 3600)
    } else if window.ends_with('m') {
        window.trim_end_matches('m').parse::<i64>().ok().map(|m| m * 60)
    } else if window.ends_with('s') {
        window.trim_end_matches('s').parse::<i64>().ok()
    } else {
        None
    };

    match parsed {
        Some(secs) if secs <= 0 => {
            tracing::warn!(
                window = %window,
                "Invalid non-positive window value, defaulting to 24h"
            );
            86400
        }
        Some(secs) if secs > MAX_WINDOW_SECONDS => {
            tracing::warn!(
                window = %window,
                max = MAX_WINDOW_SECONDS,
                "Window value too large, clamping to maximum"
            );
            MAX_WINDOW_SECONDS
        }
        Some(secs) => secs,
        None => {
            tracing::warn!(
                window = %window,
                "Failed to parse window, defaulting to 24h"
            );
            86400
        }
    }
}


use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;

pub(crate) struct CapacityReservationInput<'a> {
    pub completion_window: &'a str,
    pub file_model_counts: &'a HashMap<String, i64>,
    pub model_throughputs: &'a HashMap<String, f32>,
    pub model_ids_by_alias: &'a HashMap<String, Uuid>,
    pub default_throughput: f32,
    pub relaxation_factor: f32,
    pub reservation_ttl_secs: i64,
    pub include_pending_counts: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CapacityError {
    #[error("insufficient capacity for {completion_window} window: {models}")]
    InsufficientCapacity { completion_window: String, models: String },
    #[error("{0}")]
    Internal(String),
}

pub(crate) async fn reserve_capacity<P: sqlx_pool_router::PoolProvider>(
    dwctl_pool: &PgPool,
    request_manager: &fusillade_arsenal::PostgresRequestManager<P>,
    input: &CapacityReservationInput<'_>,
) -> Result<Vec<Uuid>, CapacityError> {
    use crate::db::handlers::BatchCapacityReservations;
    use fusillade::Storage;
    use fusillade::request::ServiceTierFilter;

    let mut pending_counts_since: Option<DateTime<Utc>> = None;
    let pending_counts: HashMap<String, HashMap<String, i64>> = if input.include_pending_counts {
        let since: DateTime<Utc> = sqlx::query_scalar!(r#"SELECT now() AS "now!""#)
            .fetch_one(dwctl_pool)
            .await
            .map_err(|e| CapacityError::Internal(format!("read reservation clock: {e}")))?;
        pending_counts_since = Some(since);

        let windows = vec![(
            input.completion_window.to_string(),
            None,
            parse_window_to_seconds(input.completion_window),
        )];
        let states = vec!["pending".to_string(), "claimed".to_string(), "processing".to_string()];
        let model_filter: Vec<String> = input.file_model_counts.keys().cloned().collect();

        match request_manager
            .get_pending_request_counts_by_model_and_window(&windows, &states, &model_filter, &ServiceTierFilter::Include(vec![None]), true)
            .await
        {
            Ok(counts) => counts,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "Pending-count query failed during batch admission; admitting on active reservations only"
                );
                pending_counts_since = None;
                HashMap::new()
            }
        }
    } else {
        HashMap::new()
    };

    let mut tx = dwctl_pool
        .begin()
        .await
        .map_err(|e| CapacityError::Internal(format!("begin reservation transaction: {e}")))?;

    let mut model_pairs: Vec<(String, Uuid)> = input.model_ids_by_alias.iter().map(|(a, id)| (a.clone(), *id)).collect();
    model_pairs.sort_by_key(|(_, id)| *id);

    for (alias, model_id) in &model_pairs {
        sqlx::query!(
            "SELECT pg_advisory_xact_lock(hashtext($1::text), hashtext($2::text))",
            model_id.to_string(),
            input.completion_window
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| CapacityError::Internal(format!("lock reservation for {alias}: {e}")))?;
    }

    let model_ids: Vec<Uuid> = model_pairs.iter().map(|(_, id)| *id).collect();
    let id_to_alias: HashMap<Uuid, String> = model_pairs.iter().map(|(a, id)| (*id, a.clone())).collect();
    let mut reservations = BatchCapacityReservations::new(&mut tx);

    let reserved_rows = reservations
        .sum_active_by_model_window(&model_ids, input.completion_window, pending_counts_since)
        .await
        .map_err(|e| CapacityError::Internal(format!("sum active reservations: {e}")))?;

    let mut pending_with_reservations = pending_counts;
    for (model_id, reserved) in reserved_rows {
        if let Some(alias) = id_to_alias.get(&model_id) {
            let windows = pending_with_reservations.entry(alias.clone()).or_default();
            let entry = windows.entry(input.completion_window.to_string()).or_insert(0);
            *entry += reserved;
        }
    }

    let capacity_result = check_sla_capacity(
        input.file_model_counts,
        &pending_with_reservations,
        input.model_throughputs,
        input.default_throughput,
        input.completion_window,
        input.relaxation_factor,
    );

    if !capacity_result.has_capacity {
        tx.rollback().await.ok();

        let overloaded_details: Vec<String> = capacity_result
            .overloaded_models
            .iter()
            .map(|(model, deficit)| format!("{model} (needs {deficit} more capacity)"))
            .collect();
        tracing::warn!(
            completion_window = %input.completion_window,
            overloaded_models = %overloaded_details.join(", "),
            "Batch rejected due to insufficient capacity"
        );

        let model_names: Vec<&str> = capacity_result.overloaded_models.keys().map(|s| s.as_str()).collect();
        return Err(CapacityError::InsufficientCapacity {
            completion_window: input.completion_window.to_string(),
            models: model_names.join(", "),
        });
    }

    let expires_at = Utc::now() + Duration::seconds(input.reservation_ttl_secs);
    let mut rows = Vec::new();
    for (alias, model_id) in &model_pairs {
        if let Some(&count) = input.file_model_counts.get(alias)
            && count > 0
        {
            rows.push((*model_id, input.completion_window, count, expires_at));
        }
    }

    let reservation_ids = reservations
        .insert_reservations(&rows)
        .await
        .map_err(|e| CapacityError::Internal(format!("insert reservations: {e}")))?;

    tx.commit()
        .await
        .map_err(|e| CapacityError::Internal(format!("commit reservation transaction: {e}")))?;

    Ok(reservation_ids)
}

pub(crate) async fn release_reservations(dwctl_pool: &PgPool, reservation_ids: &[Uuid]) -> Result<(), String> {
    use crate::db::handlers::BatchCapacityReservations;

    if reservation_ids.is_empty() {
        return Ok(());
    }

    let mut conn = dwctl_pool.acquire().await.map_err(|e| format!("acquire connection: {e}"))?;

    BatchCapacityReservations::new(&mut conn)
        .release_reservations(reservation_ids)
        .await
        .map_err(|e| format!("release reservations: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn test_parse_window_hours() {
        assert_eq!(parse_window_to_seconds("1h"), 3600);
        assert_eq!(parse_window_to_seconds("24h"), 86400);
        assert_eq!(parse_window_to_seconds("48h"), 172800);
        assert_eq!(parse_window_to_seconds("168h"), 604800);
    }

    #[test]
    fn test_parse_window_minutes() {
        assert_eq!(parse_window_to_seconds("1m"), 60);
        assert_eq!(parse_window_to_seconds("30m"), 1800);
        assert_eq!(parse_window_to_seconds("60m"), 3600);
        assert_eq!(parse_window_to_seconds("90m"), 5400);
    }

    #[test]
    fn test_parse_window_seconds() {
        assert_eq!(parse_window_to_seconds("1s"), 1);
        assert_eq!(parse_window_to_seconds("60s"), 60);
        assert_eq!(parse_window_to_seconds("3600s"), 3600);
    }

    #[test]
    fn test_parse_window_invalid_defaults_to_24h() {
        assert_eq!(parse_window_to_seconds("invalid"), 86400);
        assert_eq!(parse_window_to_seconds(""), 86400);
        assert_eq!(parse_window_to_seconds("abc"), 86400);
        assert_eq!(parse_window_to_seconds("24"), 86400);
        assert_eq!(parse_window_to_seconds("h24"), 86400);
    }

    #[test]
    fn test_parse_window_zero_defaults_to_24h() {
        assert_eq!(parse_window_to_seconds("0h"), 86400);
        assert_eq!(parse_window_to_seconds("0m"), 86400);
        assert_eq!(parse_window_to_seconds("0s"), 86400);
    }

    #[test]
    fn test_parse_window_negative_defaults_to_24h() {
        assert_eq!(parse_window_to_seconds("-1h"), 86400);
        assert_eq!(parse_window_to_seconds("-24h"), 86400);
        assert_eq!(parse_window_to_seconds("-30m"), 86400);
        assert_eq!(parse_window_to_seconds("-60s"), 86400);
    }

    #[test]
    fn test_parse_window_very_large_clamped() {
        assert_eq!(parse_window_to_seconds("999999999999h"), MAX_WINDOW_SECONDS);
        assert_eq!(parse_window_to_seconds("9999999999999999s"), MAX_WINDOW_SECONDS);
    }


    #[test]
    fn test_capacity_check_empty_batch() {
        let file_model_counts = HashMap::new();
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::new();

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
        assert!(result.overloaded_models.is_empty());
    }

    #[test]
    fn test_capacity_check_single_model_within_limits() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 1000)]);
        let pending_counts = HashMap::from([("gpt-4".to_string(), HashMap::from([("24h".to_string(), 5000)]))]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
        assert!(result.overloaded_models.is_empty());
    }

    #[test]
    fn test_capacity_check_single_model_exceeds_limits() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 50000)]);
        let pending_counts = HashMap::from([("gpt-4".to_string(), HashMap::from([("24h".to_string(), 50000)]))]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(!result.has_capacity);
        assert!(result.overloaded_models.contains_key("gpt-4"));
        assert_eq!(result.overloaded_models.get("gpt-4"), Some(&13600));
    }

    #[test]
    fn test_capacity_check_exactly_at_limit() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 40000)]);
        let pending_counts = HashMap::from([("gpt-4".to_string(), HashMap::from([("24h".to_string(), 46400)]))]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
        assert!(result.overloaded_models.is_empty());
    }

    #[test]
    fn test_capacity_check_one_over_limit() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 40001)]);
        let pending_counts = HashMap::from([("gpt-4".to_string(), HashMap::from([("24h".to_string(), 46400)]))]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(!result.has_capacity);
        assert_eq!(result.overloaded_models.get("gpt-4"), Some(&1));
    }


    #[test]
    fn test_capacity_check_multiple_models_all_within_limits() {
        let file_model_counts = HashMap::from([
            ("gpt-4".to_string(), 10000),
            ("gpt-3.5".to_string(), 50000),
            ("claude".to_string(), 15000),
        ]);
        let pending_counts = HashMap::from([
            ("gpt-4".to_string(), HashMap::from([("24h".to_string(), 5000)])),
            ("gpt-3.5".to_string(), HashMap::from([("24h".to_string(), 10000)])),
            ("claude".to_string(), HashMap::from([("24h".to_string(), 5000)])),
        ]);
        let model_throughputs = HashMap::from([
            ("gpt-4".to_string(), 1.0),
            ("gpt-3.5".to_string(), 2.0),
            ("claude".to_string(), 1.0),
        ]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
        assert!(result.overloaded_models.is_empty());
    }

    #[test]
    fn test_capacity_check_multiple_models_one_exceeds() {
        let file_model_counts = HashMap::from([
            ("gpt-4".to_string(), 10000),
            ("gpt-3.5".to_string(), 100000),
            ("claude".to_string(), 15000),
        ]);
        let pending_counts = HashMap::from([
            ("gpt-4".to_string(), HashMap::from([("24h".to_string(), 5000)])),
            ("gpt-3.5".to_string(), HashMap::from([("24h".to_string(), 100000)])),
            ("claude".to_string(), HashMap::from([("24h".to_string(), 5000)])),
        ]);
        let model_throughputs = HashMap::from([
            ("gpt-4".to_string(), 1.0),
            ("gpt-3.5".to_string(), 2.0),
            ("claude".to_string(), 1.0),
        ]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(!result.has_capacity);
        assert_eq!(result.overloaded_models.len(), 1);
        assert!(result.overloaded_models.contains_key("gpt-3.5"));
        assert_eq!(result.overloaded_models.get("gpt-3.5"), Some(&27200));
    }

    #[test]
    fn test_capacity_check_multiple_models_all_exceed() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 50000), ("gpt-3.5".to_string(), 100000)]);
        let pending_counts = HashMap::from([
            ("gpt-4".to_string(), HashMap::from([("24h".to_string(), 50000)])),
            ("gpt-3.5".to_string(), HashMap::from([("24h".to_string(), 100000)])),
        ]);
        let model_throughputs = HashMap::from([
            ("gpt-4".to_string(), 1.0),
            ("gpt-3.5".to_string(), 1.0),
        ]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(!result.has_capacity);
        assert_eq!(result.overloaded_models.len(), 2);
        assert!(result.overloaded_models.contains_key("gpt-4"));
        assert!(result.overloaded_models.contains_key("gpt-3.5"));
    }


    #[test]
    fn test_capacity_check_uses_default_throughput_for_unknown_model() {
        let file_model_counts = HashMap::from([("unknown-model".to_string(), 1000)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::new();

        let result = check_sla_capacity(
            &file_model_counts,
            &pending_counts,
            &model_throughputs,
            1.0,
            "24h",
            1.0,
        );

        assert!(result.has_capacity);
    }

    #[test]
    fn test_capacity_check_uses_default_throughput_exceeds() {
        let file_model_counts = HashMap::from([("unknown-model".to_string(), 100000)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::new();

        let result = check_sla_capacity(
            &file_model_counts,
            &pending_counts,
            &model_throughputs,
            1.0,
            "24h",
            1.0,
        );

        assert!(!result.has_capacity);
        assert_eq!(result.overloaded_models.get("unknown-model"), Some(&13600));
    }

    #[test]
    fn test_capacity_check_mixed_known_and_unknown_models() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 10000), ("unknown-model".to_string(), 50000)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::from([
            ("gpt-4".to_string(), 2.0),
        ]);

        let result = check_sla_capacity(
            &file_model_counts,
            &pending_counts,
            &model_throughputs,
            0.5,
            "24h",
            1.0,
        );

        assert!(!result.has_capacity);
        assert_eq!(result.overloaded_models.len(), 1);
        assert_eq!(result.overloaded_models.get("unknown-model"), Some(&6800));
    }


    #[test]
    fn test_capacity_check_1h_window() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 2000)]);
        let pending_counts = HashMap::from([("gpt-4".to_string(), HashMap::from([("1h".to_string(), 1000)]))]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "1h", 1.0);

        assert!(result.has_capacity);
    }

    #[test]
    fn test_capacity_check_1h_window_exceeds() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 3000)]);
        let pending_counts = HashMap::from([("gpt-4".to_string(), HashMap::from([("1h".to_string(), 1000)]))]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "1h", 1.0);

        assert!(!result.has_capacity);
        assert_eq!(result.overloaded_models.get("gpt-4"), Some(&400));
    }

    #[test]
    fn test_capacity_check_different_windows_same_model() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 1000)]);
        let pending_counts = HashMap::from([(
            "gpt-4".to_string(),
            HashMap::from([
                ("1h".to_string(), 3000),
                ("24h".to_string(), 10000),
            ]),
        )]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result_1h = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "1h", 1.0);
        assert!(!result_1h.has_capacity);

        let result_24h = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);
        assert!(result_24h.has_capacity);
    }


    #[test]
    fn test_capacity_check_no_pending_for_model() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 1000)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
    }

    #[test]
    fn test_capacity_check_pending_for_different_window() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 1000)]);
        let pending_counts = HashMap::from([
            ("gpt-4".to_string(), HashMap::from([("1h".to_string(), 50000)])),
        ]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
    }

    #[test]
    fn test_capacity_check_pending_for_different_model() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 1000)]);
        let pending_counts = HashMap::from([
            ("gpt-3.5".to_string(), HashMap::from([("24h".to_string(), 50000)])),
        ]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
    }


    #[test]
    fn test_capacity_check_high_throughput_model() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 5_000_000)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 100.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
    }

    #[test]
    fn test_capacity_check_fractional_throughput() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 40000)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 0.5)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
    }

    #[test]
    fn test_capacity_check_fractional_throughput_exceeds() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 50000)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 0.5)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(!result.has_capacity);
        assert_eq!(result.overloaded_models.get("gpt-4"), Some(&6800));
    }


    #[test]
    fn test_capacity_check_zero_new_requests() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 0)]);
        let pending_counts = HashMap::from([("gpt-4".to_string(), HashMap::from([("24h".to_string(), 50000)]))]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
    }

    #[test]
    fn test_capacity_check_zero_pending() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 50000)]);
        let pending_counts = HashMap::from([("gpt-4".to_string(), HashMap::from([("24h".to_string(), 0)]))]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
    }

    #[test]
    fn test_capacity_check_zero_throughput() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 1)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 0.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(!result.has_capacity);
        assert_eq!(result.overloaded_models.get("gpt-4"), Some(&1));
    }

    #[test]
    fn test_capacity_check_negative_throughput_treated_as_zero() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 1)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::from([("gpt-4".to_string(), -5.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(!result.has_capacity);
        assert_eq!(result.overloaded_models.get("gpt-4"), Some(&1));
    }

    #[test]
    fn test_capacity_check_negative_default_throughput_treated_as_zero() {
        let file_model_counts = HashMap::from([("unknown-model".to_string(), 1)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::new();

        let result = check_sla_capacity(
            &file_model_counts,
            &pending_counts,
            &model_throughputs,
            -10.0,
            "24h",
            1.0,
        );

        assert!(!result.has_capacity);
    }

    #[test]
    fn test_capacity_check_very_small_throughput() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 50)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 0.001)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
    }

    #[test]
    fn test_capacity_check_very_large_throughput_no_overflow() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 1_000_000_000)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1_000_000.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
    }


    #[test]
    fn test_capacity_check_composite_model_as_sum_of_components() {
        let file_model_counts = HashMap::from([("gpt-4-composite".to_string(), 10000)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::from([("gpt-4-composite".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
    }


    #[test]
    fn test_capacity_check_realistic_production_scenario() {
        let file_model_counts = HashMap::from([
            ("gpt-4-turbo".to_string(), 50000),
            ("gpt-3.5-turbo".to_string(), 200000),
            ("claude-3-sonnet".to_string(), 30000),
        ]);
        let pending_counts = HashMap::from([
            ("gpt-4-turbo".to_string(), HashMap::from([("24h".to_string(), 100000)])),
            ("gpt-3.5-turbo".to_string(), HashMap::from([("24h".to_string(), 500000)])),
            ("claude-3-sonnet".to_string(), HashMap::from([("24h".to_string(), 20000)])),
        ]);
        let model_throughputs = HashMap::from([
            ("gpt-4-turbo".to_string(), 2.0),
            ("gpt-3.5-turbo".to_string(), 10.0),
            ("claude-3-sonnet".to_string(), 1.0),
        ]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
    }

    #[test]
    fn test_capacity_check_burst_scenario() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 80000)]);
        let pending_counts = HashMap::from([("gpt-4".to_string(), HashMap::from([("24h".to_string(), 10000)]))]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(!result.has_capacity);
        assert_eq!(result.overloaded_models.get("gpt-4"), Some(&3600));
    }

    #[test]
    fn test_capacity_check_gradual_queue_buildup() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 1000)]);
        let pending_counts = HashMap::from([("gpt-4".to_string(), HashMap::from([("24h".to_string(), 85000)]))]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result.has_capacity);
    }


    #[test]
    fn test_capacity_check_1h_window_independent_of_24h_pending() {

        let file_model_counts = HashMap::from([("gpt-4".to_string(), 300)]);

        let pending_counts = HashMap::from([(
            "gpt-4".to_string(),
            HashMap::from([
                ("24h".to_string(), 80000),
                ("1h".to_string(), 0),
            ]),
        )]);

        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result_1h = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "1h", 1.0);

        assert!(
            result_1h.has_capacity,
            "1h batch should be accepted when 1h queue is empty, regardless of 24h queue"
        );
        assert!(result_1h.overloaded_models.is_empty());
    }

    #[test]
    fn test_capacity_check_24h_window_independent_of_1h_pending() {

        let file_model_counts = HashMap::from([("gpt-4".to_string(), 50000)]);

        let pending_counts = HashMap::from([(
            "gpt-4".to_string(),
            HashMap::from([
                ("1h".to_string(), 3500),
                ("24h".to_string(), 10000),
            ]),
        )]);

        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result_24h = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(result_24h.has_capacity, "24h batch should be accepted based on 24h queue only");
    }

    #[test]
    fn test_capacity_check_1h_saturated_24h_empty() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 1000)]);

        let pending_counts = HashMap::from([(
            "gpt-4".to_string(),
            HashMap::from([
                ("1h".to_string(), 3000),
                ("24h".to_string(), 0),
            ]),
        )]);

        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result_1h = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "1h", 1.0);
        assert!(!result_1h.has_capacity);
        assert_eq!(result_1h.overloaded_models.get("gpt-4"), Some(&400));

        let result_24h = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);
        assert!(result_24h.has_capacity);
    }

    #[test]
    fn test_capacity_check_low_throughput_different_windows() {

        let file_model_counts = HashMap::from([("model".to_string(), 1000)]);

        let pending_counts = HashMap::from([(
            "model".to_string(),
            HashMap::from([("24h".to_string(), 8000), ("1h".to_string(), 0)]),
        )]);

        let model_throughputs = HashMap::from([("model".to_string(), 0.1)]);

        let result_1h = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 0.1, "1h", 1.0);
        assert!(!result_1h.has_capacity);
        assert_eq!(result_1h.overloaded_models.get("model"), Some(&640));

        let result_24h = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 0.1, "24h", 1.0);
        assert!(!result_24h.has_capacity);
        assert_eq!(result_24h.overloaded_models.get("model"), Some(&360));
    }

    #[test]
    fn test_capacity_check_low_throughput_small_batch_accepted() {

        let file_model_counts = HashMap::from([("model".to_string(), 300)]);

        let pending_counts = HashMap::from([(
            "model".to_string(),
            HashMap::from([
                ("24h".to_string(), 8000),
                ("1h".to_string(), 0),
            ]),
        )]);

        let model_throughputs = HashMap::from([("model".to_string(), 0.1)]);

        let result_1h = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 0.1, "1h", 1.0);
        assert!(result_1h.has_capacity, "Small batch (300) should fit in 1h window capacity (360)");
    }

    #[test]
    fn test_capacity_check_missing_window_in_pending_counts() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 1000)]);

        let pending_counts = HashMap::from([(
            "gpt-4".to_string(),
            HashMap::from([
                ("24h".to_string(), 80000),
            ]),
        )]);

        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result_1h = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "1h", 1.0);
        assert!(result_1h.has_capacity);
    }


    #[test]
    fn test_relaxation_factor_one_is_strict() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 40001)]);
        let pending_counts = HashMap::from([("gpt-4".to_string(), HashMap::from([("24h".to_string(), 46400)]))]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 1.0);

        assert!(!result.has_capacity);
        assert_eq!(result.overloaded_models.get("gpt-4"), Some(&1));
    }

    #[test]
    fn test_relaxation_factor_above_one_expands_capacity() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 4000)]);
        let pending_counts = HashMap::from([("gpt-4".to_string(), HashMap::from([("1h".to_string(), 1000)]))]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let strict = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "1h", 1.0);
        assert!(!strict.has_capacity);

        let relaxed = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "1h", 1.5);
        assert!(relaxed.has_capacity);
    }

    #[test]
    fn test_relaxation_factor_zero_blocks_all_requests() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 1)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "24h", 0.0);

        assert!(!result.has_capacity);
        assert_eq!(result.overloaded_models.get("gpt-4"), Some(&1));
    }

    #[test]
    fn test_relaxation_factor_deficit_reflects_effective_capacity() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 8000)]);
        let pending_counts = HashMap::new();
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let result = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "1h", 2.0);

        assert!(!result.has_capacity);
        assert_eq!(result.overloaded_models.get("gpt-4"), Some(&800));
    }

    #[test]
    fn test_relaxation_factor_does_not_affect_other_windows() {
        let file_model_counts = HashMap::from([("gpt-4".to_string(), 5000)]);
        let pending_counts = HashMap::from([("gpt-4".to_string(), HashMap::from([("1h".to_string(), 0)]))]);
        let model_throughputs = HashMap::from([("gpt-4".to_string(), 1.0)]);

        let strict_1h = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "1h", 1.0);
        assert!(!strict_1h.has_capacity);

        let relaxed_1h = check_sla_capacity(&file_model_counts, &pending_counts, &model_throughputs, 1.0, "1h", 2.0);
        assert!(relaxed_1h.has_capacity);
    }
}
