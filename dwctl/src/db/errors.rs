use crate::types::Operation;
use metrics::{counter, describe_counter};
use thiserror::Error;

pub(crate) fn initialize_database_error_metrics() {
    describe_counter!(
        "dwctl_db_cached_plan_errors_total",
        "Database operations rejected because a cached prepared statement changed result type"
    );
    counter!("dwctl_db_cached_plan_errors_total").increment(0);
}

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Entity not found")]
    NotFound,

    #[error("Unique constraint violation")]
    UniqueViolation {
        constraint: Option<String>,
        table: Option<String>,
        message: String,
        conflicting_value: Option<String>,
    },

    #[error("Foreign key constraint violation")]
    ForeignKeyViolation {
        constraint: Option<String>,
        table: Option<String>,
        message: String,
    },

    #[error("Check constraint violation")]
    CheckViolation {
        constraint: Option<String>,
        table: Option<String>,
        message: String,
    },

    #[error("{operation:?} cannot be applied to entity of type {entity_type}: {reason}")]
    ProtectedEntity {
        operation: Operation,
        reason: String,
        entity_type: String,
        entity_id: Option<String>,
    },

    #[error("Invalid model field: {field} must not be empty or whitespace")]
    InvalidModelField { field: &'static str },

    #[error("Database connection pool exhausted")]
    PoolExhausted,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<sqlx::Error> for DbError {
    fn from(err: sqlx::Error) -> Self {
        match &err {
            sqlx::Error::RowNotFound => DbError::NotFound,
            sqlx::Error::PoolTimedOut => {
                counter!("dwctl_db_pool_acquire_timeouts_total").increment(1);
                DbError::PoolExhausted
            }
            sqlx::Error::Database(db_err) => {
                if db_err.code().as_deref() == Some("0A000") && db_err.message().contains("cached plan must not change result type") {
                    counter!("dwctl_db_cached_plan_errors_total").increment(1);
                }
                if db_err.is_unique_violation() {
                    let constraint = db_err.constraint().map(|s| s.to_string());

                    let conflicting_value = if let Some(pg_err) = db_err.try_downcast_ref::<sqlx::postgres::PgDatabaseError>() {
                        if let Some(detail_msg) = pg_err.detail() {
                            extract_conflicting_alias(detail_msg, constraint.as_deref())
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    DbError::UniqueViolation {
                        constraint,
                        table: db_err.table().map(|s| s.to_string()),
                        message: db_err.message().to_string(),
                        conflicting_value,
                    }
                } else if db_err.is_foreign_key_violation() {
                    DbError::ForeignKeyViolation {
                        constraint: db_err.constraint().map(|s| s.to_string()),
                        table: db_err.table().map(|s| s.to_string()),
                        message: db_err.message().to_string(),
                    }
                } else if db_err.is_check_violation() {
                    DbError::CheckViolation {
                        constraint: db_err.constraint().map(|s| s.to_string()),
                        table: db_err.table().map(|s| s.to_string()),
                        message: db_err.message().to_string(),
                    }
                } else {
                    DbError::Other(anyhow::Error::from(err))
                }
            }
            _ => DbError::Other(anyhow::Error::from(err)),
        }
    }
}

fn extract_conflicting_alias(detail: &str, constraint: Option<&str>) -> Option<String> {
    if constraint == Some("deployed_models_alias_unique") {
        if let Some(start) = detail.find("=(")
            && let Some(end) = detail[start + 2..].find(')')
        {
            return Some(detail[start + 2..start + 2 + end].to_string());
        }
    }
    None
}

pub type Result<T> = std::result::Result<T, DbError>;

#[cfg(test)]
mod tests {
    use super::{DbError, initialize_database_error_metrics};
    use metrics::with_local_recorder;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use sqlx::PgPool;

    #[dwctl_test_macros::test]
    async fn cached_result_shape_failure_is_counted_without_retry(pool: PgPool) {
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query("CREATE TABLE cached_shape (id integer)")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query("SELECT * FROM cached_shape").fetch_all(&mut *connection).await.unwrap();
        sqlx::query("ALTER TABLE cached_shape ADD COLUMN added text")
            .execute(&pool)
            .await
            .unwrap();
        let error = sqlx::query("SELECT * FROM cached_shape")
            .fetch_all(&mut *connection)
            .await
            .unwrap_err();
        assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("0A000"));
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        with_local_recorder(&recorder, || {
            initialize_database_error_metrics();
            assert!(handle.render().contains("dwctl_db_cached_plan_errors_total 0"));
            assert!(matches!(DbError::from(error), DbError::Other(_)));
        });
        assert!(handle.render().contains("dwctl_db_cached_plan_errors_total 1"));
        let repeated = sqlx::query("SELECT * FROM cached_shape")
            .persistent(false)
            .fetch_all(&mut *connection)
            .await
            .unwrap_err();
        assert!(repeated.to_string().contains("cached plan must not change result type"));
    }

    #[dwctl_test_macros::test]
    async fn unrelated_unsupported_feature_does_not_count_as_cached_plan(pool: PgPool) {
        let error = sqlx::query("SELECT count(*) FROM pg_class FOR UPDATE")
            .fetch_all(&pool)
            .await
            .unwrap_err();
        assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("0A000"));
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        with_local_recorder(&recorder, || {
            initialize_database_error_metrics();
            assert!(matches!(DbError::from(error), DbError::Other(_)));
        });
        assert!(handle.render().contains("dwctl_db_cached_plan_errors_total 0"));
    }
}
