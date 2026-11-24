
use crate::request::AnyRequest;
use futures::StreamExt;
pub use sqlx_pool_router::{DynPools, PoolHandle, PoolProvider, TestDbPools};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::Stream;
use sqlx::QueryBuilder;
use sqlx::Row;
use sqlx::postgres::PgPool;
use std::collections::{HashMap, HashSet};
use tokio::sync::{Mutex, Semaphore, SemaphorePermit, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use super::{
    ArchiveOutcome, BackgroundClaimKind, DaemonStorage, ModelFilter, ModelFilterState, Storage,
};
use crate::PostgresStorageConfig;
use crate::batch::{
    BackgroundBatchInput, Batch, BatchErrorDetails, BatchErrorItem, BatchId, BatchInput,
    BatchNotification, BatchOutputItem, BatchResponseDetails, BatchStatus, File, FileContentItem,
    FileId, FileMetadata, FileStreamItem, FileStreamResult, ListBatchesFilter, OutputFileType,
    RequestTemplateInput, TemplateId,
};
use crate::daemon::{
    AnyDaemonRecord, DaemonData, DaemonRecord, DaemonState, DaemonStatus, Dead, Initializing,
    Running,
};
use crate::error::{FusilladeError, Result};
use crate::manager::{
    RetainedResponseArchiveCutoffs, RetainedResponseArchiveOutcome,
    RetainedResponsePartitionRunway, RetainedResponseRetirementOutcome, RetainedResponseWriteError,
    RetentionPolicy, TrailingDemandCount,
};
use crate::request::{
    API_KEY_ID_METADATA_KEY, Canceled, CascadeTargetState, Claimed, Completed,
    CreateBackgroundInput, CreateFlexInput, CreateRealtimeInput, DaemonId, Failed, FailureReason,
    LeakStamp, Pending, PersistCompletedRealtimeInput, Processing, Request, RequestData,
    RequestDetail, RequestId, RequestState, ServiceTierFilter,
};

#[allow(dead_code)]
pub(crate) mod batch_archive_retirement;
mod batch_list;
pub(crate) mod partition_retirement;
mod request_maintenance;
pub(crate) mod retained_response;
pub(crate) mod retained_response_retirement;
pub(crate) mod template_retirement;
pub(crate) mod template_store;

use super::utils::{
    calculate_error_message_size, calculate_response_body_size, estimate_error_file_size,
    estimate_output_file_size,
};

fn failure_class(response_status: Option<i16>, error: Option<&str>) -> String {
    if let Some(status) = response_status {
        return format!("http_{status}");
    }
    let Some(reason) = error.and_then(|e| serde_json::from_str::<FailureReason>(e).ok()) else {
        return "unknown".to_string();
    };
    match reason {
        FailureReason::RetriableHttpStatus { status, .. }
        | FailureReason::NonRetriableHttpStatus { status, .. } => {
            format!("http_{status}")
        }
        FailureReason::NetworkError { .. } => "network_error".to_string(),
        FailureReason::Timeout { .. } => "timeout".to_string(),
        FailureReason::TaskTerminated => "task_terminated".to_string(),
        FailureReason::RequestBuilderError { .. } => "request_builder_error".to_string(),
        FailureReason::BatchTerminated => "batch_terminated".to_string(),
    }
}

fn sanitize_outbound_body(body: &str) -> std::borrow::Cow<'_, str> {
    if !body.contains("service_tier") && !body.contains("background") {
        return std::borrow::Cow::Borrowed(body);
    }
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(body) else {
        return std::borrow::Cow::Borrowed(body);
    };
    let Some(obj) = value.as_object_mut() else {
        return std::borrow::Cow::Borrowed(body);
    };
    let stripped =
        obj.shift_remove("service_tier").is_some() | obj.shift_remove("background").is_some();
    if !stripped {
        return std::borrow::Cow::Borrowed(body);
    }
    match serde_json::to_string(&value) {
        Ok(cleaned) => std::borrow::Cow::Owned(cleaned),
        Err(_) => std::borrow::Cow::Borrowed(body),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchInsertStrategy {
    Batched { batch_size: usize },
}

impl Default for BatchInsertStrategy {
    fn default() -> Self {
        BatchInsertStrategy::Batched { batch_size: 5000 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileExpiry {
    pub default: chrono::Duration,
    pub max: chrono::Duration,
}

impl Default for FileExpiry {
    fn default() -> Self {
        Self {
            default: chrono::Duration::days(30),
            max: chrono::Duration::days(30),
        }
    }
}

fn maintenance_budget(client_timeout: std::time::Duration) -> std::time::Duration {
    (client_timeout * 5 / 6).max(std::time::Duration::from_millis(1))
}

const DEFAULT_MAINTENANCE_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);

async fn bound_to_deadline(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deadline: std::time::Instant,
) -> std::result::Result<(), sqlx::Error> {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    let millis = remaining.as_millis().clamp(1, i32::MAX as u128);
    sqlx::query(&format!("SET LOCAL statement_timeout = {millis}"))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub struct PostgresRequestManager<P: PoolProvider> {
    pools: P,
    config: PostgresStorageConfig,
    retained_response_fence_seconds: Option<u64>,
    realtime_retention_seconds: Option<u64>,
    retained_response_retention_bounds_seconds: Option<(u64, u64)>,
    partition_maintenance_pool: Option<sqlx::PgPool>,
    partition_maintenance_attested: bool,
    partition_maintenance_lease_owner: Uuid,
    state_write_limiter: StateWriteLimiter,
    db_retry_config: crate::DbRetryConfig,
    query_schema: Option<Arc<str>>,
    download_buffer_size: usize,
    batch_insert_strategy: BatchInsertStrategy,
    file_expiry: FileExpiry,
    retry_age_limit: Option<chrono::Duration>,
    response_transformer: std::sync::OnceLock<Arc<dyn crate::transform::ResponseTransformer>>,
    maintenance_budget: std::time::Duration,
}

struct StateWriteLimiter {
    semaphore: Option<Semaphore>,
    waiting: AtomicUsize,
    in_flight: AtomicUsize,
}

impl StateWriteLimiter {
    fn new(limit: usize) -> Self {
        Self {
            semaphore: (limit > 0).then(|| Semaphore::new(limit)),
            waiting: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
        }
    }

    async fn acquire(&self, operation: &'static str) -> Result<StateWritePermit<'_>> {
        let started = std::time::Instant::now();
        let waiting = self.semaphore.as_ref().map(|_| {
            self.waiting.fetch_add(1, Ordering::Relaxed);
            metrics::gauge!("fusillade_state_writes_waiting", "operation" => operation)
                .increment(1.0);
            StateWriteWaitGuard {
                operation,
                waiting: &self.waiting,
            }
        });

        let permit = match &self.semaphore {
            Some(semaphore) => Some(semaphore.acquire().await.map_err(|_| {
                FusilladeError::Other(anyhow!("state write concurrency limiter closed"))
            })?),
            None => None,
        };

        drop(waiting);
        metrics::histogram!(
            "fusillade_state_write_wait_duration_seconds",
            "operation" => operation
        )
        .record(started.elapsed().as_secs_f64());
        metrics::gauge!("fusillade_state_writes_in_flight", "operation" => operation)
            .increment(1.0);
        self.in_flight.fetch_add(1, Ordering::Relaxed);

        Ok(StateWritePermit {
            _permit: permit,
            operation,
            in_flight: &self.in_flight,
        })
    }

    #[cfg(test)]
    fn counts(&self) -> (usize, usize) {
        (
            self.waiting.load(Ordering::Relaxed),
            self.in_flight.load(Ordering::Relaxed),
        )
    }
}

struct StateWriteWaitGuard<'a> {
    operation: &'static str,
    waiting: &'a AtomicUsize,
}

impl Drop for StateWriteWaitGuard<'_> {
    fn drop(&mut self) {
        self.waiting.fetch_sub(1, Ordering::Relaxed);
        metrics::gauge!(
            "fusillade_state_writes_waiting",
            "operation" => self.operation
        )
        .decrement(1.0);
    }
}

struct StateWritePermit<'a> {
    _permit: Option<SemaphorePermit<'a>>,
    operation: &'static str,
    in_flight: &'a AtomicUsize,
}

impl Drop for StateWritePermit<'_> {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        metrics::gauge!(
            "fusillade_state_writes_in_flight",
            "operation" => self.operation
        )
        .decrement(1.0);
    }
}

struct NewBatchRecord {
    file_id: FileId,
    endpoint: String,
    service_tier: Option<String>,
    completion_window: Option<String>,
    metadata: Option<serde_json::Value>,
    created_by: Option<String>,
    expires_at: Option<DateTime<Utc>>,
    api_key_id: Option<Uuid>,
    total_requests: i64,
}

struct NewPendingRequest {
    request_id: Uuid,
    body: String,
    model: String,
    endpoint: String,
    method: String,
    path: String,
    api_key_id: Uuid,
    created_by: String,
    service_tier: &'static str,
    metadata: Option<serde_json::Value>,
}

struct ClaimedRequestRow {
    id: Uuid,
    batch_id: Option<Uuid>,
    template_id: Uuid,
    retry_attempt: i32,
    custom_id: Option<String>,
    endpoint: String,
    method: String,
    path: String,
    body: String,
    model: String,
    api_key_id: Option<Uuid>,
    batch_expires_at: Option<DateTime<Utc>>,
    batch_id_str: String,
    batch_file_id: String,
    batch_endpoint: String,
    batch_completion_window: Option<String>,
    batch_metadata: Option<String>,
    batch_output_file_id: Option<String>,
    batch_error_file_id: Option<String>,
    batch_created_by: String,
    batch_created_at: String,
    batch_expires_at_str: Option<String>,
    batch_cancelling_at: Option<String>,
    batch_errors: Option<String>,
    batch_total_requests: String,
    leaked: bool,
    window_class: String,
    window_secs: f64,
}

macro_rules! batch_from_dynamic_row {
    ($row:expr) => {
        Batch {
            id: BatchId($row.get("id")),
            file_id: $row.get::<Option<Uuid>, _>("file_id").map(FileId),
            endpoint: $row.get("endpoint"),
            service_tier: $row.get("service_tier"),
            completion_window: $row.get("completion_window"),
            metadata: $row.get("metadata"),
            output_file_id: $row.get::<Option<Uuid>, _>("output_file_id").map(FileId),
            error_file_id: $row.get::<Option<Uuid>, _>("error_file_id").map(FileId),
            created_by: $row.get("created_by"),
            created_at: $row.get("created_at"),
            expires_at: $row.get("expires_at"),
            cancelling_at: $row.get("cancelling_at"),
            errors: $row.get("errors"),
            total_requests: $row.get("total_requests"),
            requests_started_at: $row.get("requests_started_at"),
            finalizing_at: $row.get("finalizing_at"),
            completed_at: $row.get("completed_at"),
            failed_at: $row.get("failed_at"),
            cancelled_at: $row.get("cancelled_at"),
            deleted_at: $row.get("deleted_at"),
            pending_requests: $row.get("pending_requests"),
            in_progress_requests: $row.get("in_progress_requests"),
            completed_requests: $row.get("completed_requests"),
            failed_requests: $row.get("failed_requests"),
            canceled_requests: $row.get("canceled_requests"),
            notification_sent_at: $row.get("notification_sent_at"),
            api_key_id: $row.get::<Option<Uuid>, _>("api_key_id"),
        }
    };
}

macro_rules! batch_status_from_dynamic_row {
    ($row:expr) => {
        BatchStatus {
            batch_id: BatchId($row.get("batch_id")),
            file_id: $row.get::<Option<Uuid>, _>("file_id").map(FileId),
            file_name: $row.get("file_name"),
            total_requests: $row.get("total_requests"),
            pending_requests: $row.get("pending_requests"),
            in_progress_requests: $row.get("in_progress_requests"),
            completed_requests: $row.get("completed_requests"),
            failed_requests: $row.get("failed_requests"),
            canceled_requests: $row.get("canceled_requests"),
            started_at: $row.get("started_at"),
            failed_at: $row.get("failed_at"),
            created_at: $row.get("created_at"),
        }
    };
}

impl<P: PoolProvider> PostgresRequestManager<P> {
    pub fn new(pools: P, config: PostgresStorageConfig) -> Self {
        let state_write_limiter = StateWriteLimiter::new(config.max_concurrent_state_writes);
        Self {
            pools,
            config,
            retained_response_fence_seconds: None,
            realtime_retention_seconds: None,
            retained_response_retention_bounds_seconds: None,
            partition_maintenance_pool: None,
            partition_maintenance_attested: false,
            partition_maintenance_lease_owner: Uuid::new_v4(),
            state_write_limiter,
            db_retry_config: crate::DbRetryConfig::default(),
            query_schema: None,
            download_buffer_size: 100,
            batch_insert_strategy: BatchInsertStrategy::default(),
            file_expiry: FileExpiry::default(),
            retry_age_limit: None,
            response_transformer: std::sync::OnceLock::new(),
            maintenance_budget: maintenance_budget(DEFAULT_MAINTENANCE_QUERY_TIMEOUT),
        }
    }

    pub fn with_maintenance_query_timeout(mut self, client_timeout: std::time::Duration) -> Self {
        self.maintenance_budget = maintenance_budget(client_timeout);
        self
    }

    pub async fn get_terminal_request_detail(
        &self,
        request_id: RequestId,
    ) -> Result<Option<RequestDetail>> {
        let status: Option<String> = sqlx::query_scalar("SELECT state FROM requests WHERE id = $1")
            .bind(request_id.0)
            .fetch_optional(self.write_executor())
            .await
            .map_err(|error| {
                FusilladeError::Other(anyhow!("Failed to read request status: {error}"))
            })?;
        if status
            .as_deref()
            .is_some_and(|state| !matches!(state, "completed" | "failed" | "canceled"))
        {
            return Ok(None);
        }
        let detail = retained_response::get_request_detail(self, request_id).await?;
        Ok(matches!(detail.status.as_str(), "completed" | "failed" | "canceled").then_some(detail))
    }

    pub fn with_client<H>(pools: P, http_client: Arc<H>) -> Self {
        let _ = http_client;
        Self::new(pools, PostgresStorageConfig::default())
    }

    pub fn set_response_transformer(
        &self,
        transformer: Arc<dyn crate::transform::ResponseTransformer>,
    ) -> std::result::Result<(), &'static str> {
        self.response_transformer
            .set(transformer)
            .map_err(|_| "response transformer already set")
    }

    pub fn with_config(mut self, config: PostgresStorageConfig) -> Self {
        self.state_write_limiter = StateWriteLimiter::new(config.max_concurrent_state_writes);
        self.config = config;
        self
    }

    pub fn set_config(&mut self, config: PostgresStorageConfig) {
        self.state_write_limiter = StateWriteLimiter::new(config.max_concurrent_state_writes);
        self.config = config;
    }

    pub fn with_retained_response_fence_seconds(mut self, seconds: Option<u64>) -> Self {
        self.retained_response_fence_seconds = seconds;
        self
    }

    pub fn with_realtime_retention_seconds(mut self, seconds: Option<u64>) -> Self {
        self.realtime_retention_seconds = seconds;
        self
    }

    pub fn retained_response_fence_seconds(&self) -> Option<u64> {
        self.retained_response_fence_seconds
    }

    pub fn with_retained_response_retention_bounds_seconds(
        mut self,
        bounds: Option<(u64, u64)>,
    ) -> Self {
        self.retained_response_retention_bounds_seconds = bounds;
        self
    }

    pub fn with_partition_maintenance_pool(mut self, pool: sqlx::PgPool) -> Result<Self> {
        if pool.options().get_max_connections() != 1 || pool.options().get_min_connections() != 0 {
            return Err(FusilladeError::ValidationError(
                "partition maintenance pool must use max_connections=1 and min_connections=0"
                    .to_string(),
            ));
        }
        self.partition_maintenance_pool = Some(pool);
        self.partition_maintenance_attested = false;
        Ok(self)
    }

    pub async fn attest_partition_maintenance_pool(mut self) -> Result<Self> {
        use sqlx::postgres::types::Oid;

        type MaintenanceTarget = (String, String, String, Oid, Option<Oid>, bool, bool, bool);

        let maintenance = self.partition_maintenance_pool.as_ref().ok_or_else(|| {
            crate::manager::RetainedResponseMaintenanceError::PartitionMaintenancePoolMissing
                .into_fusillade_error()
        })?;
        let target_sql = r#"
            SELECT current_database(), current_user, current_schema(),
                   namespace.oid,
                   parent.oid,
                   NOT pg_is_in_recovery(),
                   current_setting('transaction_read_only') = 'off',
                   COALESCE(pg_has_role(current_user, parent.relowner, 'USAGE'), FALSE)
            FROM pg_namespace namespace
            LEFT JOIN pg_class parent
              ON parent.relnamespace = namespace.oid
             AND parent.relname = 'retained_response_objects'
            WHERE namespace.nspname = current_schema()
        "#;
        let ordinary: Option<MaintenanceTarget> = sqlx::query_as(target_sql)
            .fetch_optional(self.write_executor())
            .await
            .map_err(|_| {
                FusilladeError::ValidationError(
                    "partition maintenance target attestation failed".to_string(),
                )
            })?;
        let session: Option<MaintenanceTarget> = sqlx::query_as(target_sql)
            .fetch_optional(maintenance)
            .await
            .map_err(|_| {
                FusilladeError::ValidationError(
                    "partition maintenance target attestation failed".to_string(),
                )
            })?;
        let exact = ordinary
            .as_ref()
            .is_some_and(|target| target.4.is_some() && target.5 && target.6 && target.7)
            && ordinary == session;
        if !exact {
            return Err(FusilladeError::ValidationError(
                "partition maintenance pool must target the exact writable retained response schema"
                    .to_string(),
            ));
        }

        let lock_key = i64::from_ne_bytes(
            *Uuid::new_v4()
                .as_bytes()
                .first_chunk()
                .expect("a UUID always contains at least eight bytes"),
        );
        let attestation_failed = || {
            FusilladeError::ValidationError(
                "partition maintenance target attestation failed".to_string(),
            )
        };
        let mut ordinary_transaction = self
            .pools
            .write()
            .begin()
            .await
            .map_err(|_| attestation_failed())?;
        if sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_key)
            .execute(&mut *ordinary_transaction)
            .await
            .is_err()
        {
            ordinary_transaction
                .rollback()
                .await
                .map_err(|_| attestation_failed())?;
            return Err(attestation_failed());
        }
        let mut maintenance_transaction = match maintenance.begin().await {
            Ok(transaction) => transaction,
            Err(_) => {
                ordinary_transaction
                    .rollback()
                    .await
                    .map_err(|_| attestation_failed())?;
                return Err(attestation_failed());
            }
        };
        let try_lock = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock($1)")
            .bind(lock_key)
            .fetch_one(&mut *maintenance_transaction)
            .await;
        let maintenance_rollback = maintenance_transaction.rollback().await;
        let ordinary_rollback = ordinary_transaction.rollback().await;
        if maintenance_rollback.is_err() || ordinary_rollback.is_err() {
            return Err(attestation_failed());
        }
        let same_live_database = !try_lock.map_err(|_| attestation_failed())?;
        if !same_live_database {
            return Err(FusilladeError::ValidationError(
                "partition maintenance pool must target the exact writable retained response schema"
                    .to_string(),
            ));
        }
        self.partition_maintenance_attested = true;
        Ok(self)
    }

    pub fn with_query_schema(mut self, schema: impl Into<String>) -> Self {
        self.query_schema = Some(Arc::from(schema.into()));
        self.partition_maintenance_attested = false;
        self
    }

    pub fn with_db_retry_config(mut self, config: crate::DbRetryConfig) -> Self {
        self.db_retry_config = config;
        self
    }

    pub fn db_retry_config(&self) -> &crate::DbRetryConfig {
        &self.db_retry_config
    }

    fn read_executor(&self) -> crate::db::RetryingPgPool {
        crate::db::RetryingPgPool::new(&self.pools.read(), &self.db_retry_config)
            .with_schema(self.query_schema.clone())
    }

    fn write_executor(&self) -> crate::db::RetryingPgPool {
        crate::db::RetryingPgPool::new(&self.pools.write(), &self.db_retry_config)
            .with_schema(self.query_schema.clone())
    }

    async fn begin_read(
        &self,
    ) -> std::result::Result<sqlx::Transaction<'static, sqlx::Postgres>, sqlx::Error> {
        crate::db::begin_transaction_in_schema(
            &self.pools.read(),
            &self.db_retry_config,
            self.query_schema.as_deref(),
        )
        .await
    }

    pub async fn begin_write(
        &self,
    ) -> std::result::Result<sqlx::Transaction<'static, sqlx::Postgres>, sqlx::Error> {
        crate::db::begin_transaction_in_schema(
            &self.pools.write(),
            &self.db_retry_config,
            self.query_schema.as_deref(),
        )
        .await
    }

    fn maintenance_deadline(&self) -> std::time::Instant {
        std::time::Instant::now() + self.maintenance_budget
    }

    async fn begin_maintenance_write(
        &self,
    ) -> std::result::Result<sqlx::Transaction<'static, sqlx::Postgres>, sqlx::Error> {
        self.begin_maintenance_write_until(self.maintenance_deadline())
            .await
    }

    async fn begin_maintenance_write_until(
        &self,
        deadline: std::time::Instant,
    ) -> std::result::Result<sqlx::Transaction<'static, sqlx::Postgres>, sqlx::Error> {
        let mut tx = self.begin_write().await?;
        bound_to_deadline(&mut tx, deadline).await?;
        Ok(tx)
    }

    async fn begin_response_write(
        &self,
        object_ids: &[Uuid],
    ) -> Result<sqlx::Transaction<'static, sqlx::Postgres>> {
        retained_response::begin_response_write_transaction(
            &self.pools.write(),
            &self.db_retry_config,
            self.query_schema.as_deref(),
            object_ids,
        )
        .await
    }

    async fn insert_batch_record(&self, input: NewBatchRecord) -> Result<Batch> {
        let mut tx = self
            .begin_write()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to begin transaction: {}", e)))?;

        let source_available = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT f.id FROM files f
            JOIN request_template_buckets bucket
              ON bucket.week_start = date_trunc('week', f.created_at AT TIME ZONE 'UTC')::date
            WHERE f.id = $1
              AND f.deleted_at IS NULL
              AND f.status = 'processed'
              AND (f.expires_at IS NULL OR f.expires_at > NOW())
              AND bucket.state = 'active'
            FOR UPDATE OF f
            FOR SHARE OF bucket
            "#,
        )
        .bind(*input.file_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to lock source file: {e}")))?
        .is_some();
        if !source_available {
            return Err(FusilladeError::ValidationError(
                "source file is deleted, expired, or its content has been retired".to_string(),
            ));
        }

        let row = sqlx::query!(
            r#"
            INSERT INTO batches (
                file_id, endpoint, service_tier, completion_window, metadata,
                created_by, expires_at, api_key_id, total_requests
            )
            VALUES ($1, $2, $3, $4, $5, COALESCE($6, ''), $7, $8, $9)
            RETURNING id, created_at
            "#,
            *input.file_id as Uuid,
            input.endpoint,
            input.service_tier,
            input.completion_window,
            input.metadata,
            input.created_by,
            input.expires_at,
            input.api_key_id,
            input.total_requests,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to create batch record: {}", e)))?;

        let created_by = input.created_by.as_deref().unwrap_or("");
        let results_expire_at = row.created_at + self.file_expiry.default;
        let output_file_id = self
            .create_virtual_output_file(&mut tx, row.id, created_by, results_expire_at)
            .await?;
        let error_file_id = self
            .create_virtual_error_file(&mut tx, row.id, created_by, results_expire_at)
            .await?;

        sqlx::query!(
            r#"
            UPDATE batches SET output_file_id = $2, error_file_id = $3 WHERE id = $1
            "#,
            row.id,
            output_file_id,
            error_file_id,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to update batch with file IDs: {}", e))
        })?;

        tx.commit()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to commit transaction: {}", e)))?;

        Ok(Batch {
            id: BatchId(row.id),
            file_id: Some(input.file_id),
            created_at: row.created_at,
            metadata: input.metadata,
            service_tier: input.service_tier,
            completion_window: input.completion_window,
            endpoint: input.endpoint,
            output_file_id: Some(FileId(output_file_id)),
            error_file_id: Some(FileId(error_file_id)),
            created_by: input.created_by.unwrap_or_default(),
            expires_at: input.expires_at,
            cancelling_at: None,
            errors: None,
            total_requests: input.total_requests,
            pending_requests: 0,
            in_progress_requests: 0,
            completed_requests: 0,
            failed_requests: 0,
            canceled_requests: 0,
            requests_started_at: None,
            finalizing_at: None,
            completed_at: None,
            failed_at: None,
            cancelled_at: None,
            deleted_at: None,
            notification_sent_at: None,
            api_key_id: input.api_key_id,
        })
    }

    async fn insert_batchless_pending_request(
        &self,
        input: NewPendingRequest,
    ) -> Result<RequestId> {
        let template_id = Uuid::new_v4();
        let mut tx = self.begin_response_write(&[input.request_id]).await?;

        if let Some(disposition) =
            retained_response::classify_response_write(&mut tx, &[input.request_id]).await?
        {
            return Err(disposition.into_fusillade_error());
        }

        let stored_body = sanitize_outbound_body(&input.body);
        template_store::ensure_current_week_partition(&mut tx).await?;
        template_store::insert_dedicated_template(
            &mut tx,
            template_store::DedicatedTemplate {
                id: template_id,
                endpoint: &input.endpoint,
                method: &input.method,
                path: &input.path,
                body: stored_body.as_ref(),
                model: &input.model,
                api_key_id: Some(input.api_key_id),
                metadata: input.metadata.as_ref(),
            },
        )
        .await
        .map_err(|_| FusilladeError::Other(anyhow!("Failed to insert pending request template")))?;

        let created_by = Some(input.created_by.trim()).filter(|owner| !owner.is_empty());
        let inserted = sqlx::query(
            "INSERT INTO requests (id, batch_id, template_id, model, custom_id, state, retry_attempt, service_tier, created_by)
             VALUES ($1, NULL, $2, $3, NULL, 'pending', 0, $4, $5)",
        )
        .bind(input.request_id)
        .bind(template_id)
        .bind(&input.model)
        .bind(input.service_tier)
        .bind(created_by)
        .execute(&mut *tx)
        .await;
        if let Err(error) = inserted {
            let _ = error;
            return Err(FusilladeError::Other(anyhow!(
                "Failed to insert pending request"
            )));
        }

        if let Some(disposition) =
            retained_response::classify_response_write(&mut tx, &[input.request_id]).await?
        {
            return Err(disposition.into_fusillade_error());
        }

        tx.commit()
            .await
            .map_err(|_| FusilladeError::Other(anyhow!("Failed to finish pending request")))?;

        Ok(RequestId(input.request_id))
    }

    pub fn config(&self) -> &PostgresStorageConfig {
        &self.config
    }

    async fn retry_db_operation<T, Op, Fut>(&self, operation: Op) -> Result<T>
    where
        Op: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        crate::retry_transient_db_errors(&self.db_retry_config, operation).await
    }

    fn pending_counts_statement_timeout_ms(&self) -> i64 {
        self.config.pending_request_counts_timeout_ms as i64
    }

    fn claimed_rows_to_requests(
        &self,
        rows: Vec<ClaimedRequestRow>,
        daemon_id: DaemonId,
        claimed_at: DateTime<Utc>,
    ) -> Vec<Request<Claimed>> {
        let mut parsed_metadata_cache: std::collections::HashMap<
            Uuid,
            Option<Arc<serde_json::Value>>,
        > = std::collections::HashMap::new();

        rows.into_iter()
            .map(|row| {
                let mut batch_metadata = std::collections::HashMap::new();

                let parse_metadata = |raw: Option<&str>| {
                    raw.and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                        .map(Arc::new)
                };
                let parsed_metadata = match row.batch_id {
                    Some(batch_uuid) => parsed_metadata_cache
                        .entry(batch_uuid)
                        .or_insert_with(|| parse_metadata(row.batch_metadata.as_deref()))
                        .clone(),
                    None => parse_metadata(row.batch_metadata.as_deref()),
                };

                for field_name in &self.config.batch_metadata_fields {
                    if field_name == API_KEY_ID_METADATA_KEY {
                        continue;
                    }
                    let value: Option<&str> = if row.batch_id.is_some() {
                        match field_name.as_str() {
                            "id" => Some(&row.batch_id_str),
                            "file_id" => Some(&row.batch_file_id),
                            "endpoint" => Some(&row.batch_endpoint),
                            "completion_window" => row.batch_completion_window.as_deref(),
                            "metadata" => row.batch_metadata.as_deref(),
                            "output_file_id" => row.batch_output_file_id.as_deref(),
                            "error_file_id" => row.batch_error_file_id.as_deref(),
                            "created_by" => Some(&row.batch_created_by),
                            "created_at" => Some(&row.batch_created_at),
                            "expires_at" => row.batch_expires_at_str.as_deref(),
                            "cancelling_at" => row.batch_cancelling_at.as_deref(),
                            "errors" => row.batch_errors.as_deref(),
                            "total_requests" => Some(&row.batch_total_requests),
                            _ => None,
                        }
                    } else {
                        match field_name.as_str() {
                            "created_at" => Some(&row.batch_created_at),
                            "completion_window" => row.batch_completion_window.as_deref(),
                            _ => None,
                        }
                    };

                    if let Some(v) = value {
                        batch_metadata.insert(field_name.clone(), v.to_string());
                    } else if let Some(metadata_json) = parsed_metadata.as_ref()
                        && let Some(v) = metadata_json.get(field_name).and_then(|v| v.as_str())
                    {
                        batch_metadata.insert(field_name.clone(), v.to_string());
                    }
                }

                batch_metadata
                    .entry("created_at".to_string())
                    .or_insert_with(|| row.batch_created_at.clone());
                batch_metadata.remove(API_KEY_ID_METADATA_KEY);
                if let Some(api_key_id) = row.api_key_id {
                    batch_metadata
                        .insert(API_KEY_ID_METADATA_KEY.to_string(), api_key_id.to_string());
                }
                if let Some(completion_window) = row.batch_completion_window.clone() {
                    batch_metadata
                        .entry("completion_window".to_string())
                        .or_insert(completion_window);
                }

                Request {
                    state: Claimed {
                        daemon_id,
                        claimed_at,
                        retry_attempt: row.retry_attempt as u32,
                        batch_expires_at: row.batch_expires_at,
                        leak: if row.leaked {
                            Some(LeakStamp {
                                window_class: row.window_class.clone(),
                                window_secs: row.window_secs,
                            })
                        } else {
                            None
                        },
                    },
                    data: RequestData {
                        id: RequestId(row.id),
                        batch_id: row.batch_id.map(BatchId),
                        template_id: TemplateId(row.template_id),
                        custom_id: row.custom_id,
                        endpoint: row.endpoint,
                        method: row.method,
                        path: row.path,
                        body: row.body,
                        model: row.model,
                        api_key: String::new(),
                        created_by: row.batch_created_by,
                        batch_metadata,
                    },
                }
            })
            .collect()
    }

    pub fn with_download_buffer_size(mut self, buffer_size: usize) -> Self {
        self.download_buffer_size = buffer_size;
        self
    }

    pub fn with_file_expiry(mut self, file_expiry: FileExpiry) -> Self {
        assert!(
            file_expiry.default > chrono::Duration::zero()
                && file_expiry.max >= file_expiry.default,
            "file expiry must be positive and default must not exceed max, got {file_expiry:?}"
        );
        self.file_expiry = file_expiry;
        self
    }

    pub fn with_retry_age_limit(mut self, limit: Option<chrono::Duration>) -> Self {
        self.retry_age_limit = limit;
        self
    }

    async fn check_retry_age<'e, E>(&self, executor: E, batch_id: Uuid) -> Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        let Some(limit) = self.retry_age_limit else {
            return Ok(());
        };
        let past: Option<bool> = sqlx::query_scalar(
            r#"
            SELECT NOW() > LEAST(b.created_at, COALESCE(f.created_at, b.created_at))
                           + $2 * INTERVAL '1 second' - INTERVAL '1 day'
            FROM batches b
            LEFT JOIN files f ON f.id = b.file_id
            WHERE b.id = $1
            "#,
        )
        .bind(batch_id)
        .bind(limit.num_seconds() as f64)
        .fetch_optional(executor)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to check batch retry age: {e}")))?;
        if past == Some(true) {
            return Err(FusilladeError::ValidationError(format!(
                "batch is too old to retry: retries are accepted until one day before its \
                 content reaches the end of its {}-day retention period",
                limit.num_days()
            )));
        }
        Ok(())
    }

    fn derive_expiry(&self, file: &mut File) {
        if file.expires_at.is_none()
            && matches!(
                file.purpose,
                Some(crate::batch::Purpose::BatchOutput | crate::batch::Purpose::BatchError)
            )
        {
            file.expires_at = Some(file.created_at + self.file_expiry.default);
        }
    }

    pub fn with_batch_insert_strategy(mut self, strategy: BatchInsertStrategy) -> Self {
        match strategy {
            BatchInsertStrategy::Batched { batch_size } => {
                assert!(
                    batch_size > 0,
                    "batch_size must be greater than 0, got {}",
                    batch_size
                );
            }
        }
        self.batch_insert_strategy = strategy;
        self
    }

    pub async fn mark_batch_failed(&self, batch_id: BatchId, error_message: &str) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE batches
            SET failed_at = NOW(),
                errors = $2
            WHERE id = $1 AND failed_at IS NULL
            "#,
            *batch_id as Uuid,
            serde_json::json!({"message": error_message}),
        )
        .execute(self.write_executor())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to mark batch as failed: {}", e)))?;

        Ok(())
    }

    pub fn pool(&self) -> sqlx_pool_router::PoolHandle {
        self.pools.write()
    }

    pub fn pools(&self) -> &P {
        &self.pools
    }
}

impl<P: PoolProvider> PostgresRequestManager<P> {
    async fn dropped_or_missing_in_transaction(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        request_id: RequestId,
    ) -> Result<Option<RequestId>> {
        let current: Option<String> =
            sqlx::query_scalar("SELECT state FROM requests WHERE id = $1")
                .bind(*request_id as Uuid)
                .fetch_optional(&mut **tx)
                .await
                .map_err(|_| FusilladeError::Other(anyhow!("Failed to classify request state")))?;

        match current {
            None => match retained_response::classify_response_write(tx, &[request_id.0]).await? {
                Some(retained_response::ResponseWriteDisposition::AlreadyRetained) => Ok(None),
                Some(disposition) => Err(disposition.into_fusillade_error()),
                None => Err(FusilladeError::RequestNotFound(request_id)),
            },
            Some(_) => {
                tracing::warn!("Dropped a late request transition after a terminal state");
                Ok(None)
            }
        }
    }

    async fn lost_ownership_or_missing_in_transaction(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        request_id: RequestId,
    ) -> Result<Option<RequestId>> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM requests WHERE id = $1)")
                .bind(*request_id as Uuid)
                .fetch_one(&mut **tx)
                .await
                .map_err(|_| {
                    FusilladeError::Other(anyhow!("Failed to classify request ownership"))
                })?;

        if exists {
            return Err(FusilladeError::LostOwnership(request_id));
        }

        match retained_response::classify_response_write(tx, &[request_id.0]).await? {
            Some(_) => Err(FusilladeError::LostOwnership(request_id)),
            None => Err(FusilladeError::RequestNotFound(request_id)),
        }
    }

    async fn terminal_replay_or_lost_ownership_in_transaction(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        request_id: RequestId,
    ) -> Result<Option<RequestId>> {
        let state: Option<String> = sqlx::query_scalar("SELECT state FROM requests WHERE id = $1")
            .bind(*request_id as Uuid)
            .fetch_optional(&mut **tx)
            .await
            .map_err(|_| {
                FusilladeError::Other(anyhow!("Failed to classify terminal request write"))
            })?;

        match state.as_deref() {
            Some("completed" | "failed") => {
                tracing::warn!("Dropped a duplicate terminal request transition");
                Ok(None)
            }
            Some(_) => Err(FusilladeError::LostOwnership(request_id)),
            None => match retained_response::classify_response_write(tx, &[request_id.0]).await? {
                Some(retained_response::ResponseWriteDisposition::AlreadyRetained) => Ok(None),
                Some(disposition) => Err(disposition.into_fusillade_error()),
                None => Err(FusilladeError::RequestNotFound(request_id)),
            },
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn unclaim_stale_requests(&self) -> Result<usize> {
        let claim_timeout_ms = self.config.claim_timeout_ms as i64;
        let processing_timeout_ms = self.config.processing_timeout_ms as i64;
        let stale_daemon_threshold_ms = self.config.stale_daemon_threshold_ms as i64;
        let limit = self.config.unclaim_batch_size as i64;

        let unclaim_start = std::time::Instant::now();
        let reclaimed = sqlx::query!(
            r#"
            WITH candidates AS MATERIALIZED (
                SELECT id, previous_state, reason, daemon_id, claimed_at
                FROM (
                    SELECT DISTINCT ON (id)
                        id, previous_state, reason, priority, daemon_id, claimed_at
                    FROM (
                    -- Explicit shutdown is authoritative and can be reclaimed immediately.
                    SELECT r.id, r.state AS previous_state, 'explicitly_dead'::text AS reason,
                           0 AS priority, r.daemon_id, r.claimed_at
                    FROM requests r
                    JOIN daemons d ON d.id = r.daemon_id
                    WHERE r.state IN ('claimed', 'processing')
                      AND d.status = 'dead'
                    UNION
                    -- A stale daemon self-fences locally at this same threshold.
                    SELECT r.id, r.state AS previous_state, 'lease_expired'::text AS reason,
                           1 AS priority, r.daemon_id, r.claimed_at
                    FROM requests r
                    JOIN daemons d ON d.id = r.daemon_id
                    WHERE r.state IN ('claimed', 'processing')
                      AND d.status = 'running'
                      AND d.last_heartbeat < NOW() - ($3 || ' milliseconds')::INTERVAL
                    UNION
                    -- Time-based fallback: request stuck too long regardless of daemon state
                    SELECT r.id, r.state AS previous_state, 'age_fallback'::text AS reason,
                           2 AS priority, r.daemon_id, r.claimed_at
                    FROM requests r
                    WHERE
                        (r.state = 'claimed' AND r.claimed_at < NOW() - ($1 || ' milliseconds')::INTERVAL)
                        OR
                        (r.state = 'processing' AND r.started_at < NOW() - ($2 || ' milliseconds')::INTERVAL)
                    ) eligible
                    ORDER BY id, priority
                ) deduplicated
                ORDER BY priority, id
                LIMIT $4
            ), updated AS (
                UPDATE requests r
                SET
                    state = 'pending',
                    daemon_id = NULL,
                    claimed_at = NULL,
                    started_at = NULL
                FROM candidates c
                WHERE r.id = c.id
                  AND r.state = c.previous_state
                  AND r.daemon_id = c.daemon_id
                  AND r.claimed_at = c.claimed_at
                RETURNING c.previous_state, c.reason
            )
            SELECT previous_state AS "previous_state!", reason AS "reason!"
            FROM updated
            "#,
            claim_timeout_ms.to_string(),
            processing_timeout_ms.to_string(),
            stale_daemon_threshold_ms.to_string(),
            limit,
        )
        .fetch_all(self.write_executor())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to unclaim stale requests: {}", e)))?;
        metrics::histogram!("fusillade_unclaim_stale_duration_seconds")
            .record(unclaim_start.elapsed().as_secs_f64());

        let count = reclaimed.len();

        if count > 0 {
            metrics::counter!("fusillade_stale_requests_reclaimed_total").increment(count as u64);
            let mut by_reason: HashMap<(String, String), u64> = HashMap::new();
            for row in reclaimed {
                *by_reason
                    .entry((row.previous_state, row.reason))
                    .or_default() += 1;
            }
            for ((previous_state, reason), reclaimed_count) in by_reason {
                metrics::counter!(
                    "fusillade_requests_reclaimed_total",
                    "previous_state" => previous_state,
                    "reason" => reason,
                )
                .increment(reclaimed_count);
            }
            tracing::warn!(
                count = count,
                claim_timeout_ms,
                processing_timeout_ms,
                stale_daemon_threshold_ms,
                "Unclaimed stale requests (likely due to daemon crash or shutdown)"
            );
        }

        Ok(count)
    }

    async fn check_and_mark_expired(&self, file: &mut File) -> Result<bool> {
        if file.status != crate::batch::FileStatus::Processed {
            return Ok(false);
        }

        if let Some(expires_at) = file.expires_at
            && Utc::now() > expires_at
        {
            sqlx::query!(
                r#"
                UPDATE files
                SET status = 'expired'
                WHERE id = $1 AND status = 'processed'
                "#,
                *file.id as Uuid,
            )
            .execute(self.write_executor())
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to mark file as expired: {}", e)))?;

            file.status = crate::batch::FileStatus::Expired;
            return Ok(true);
        }

        Ok(false)
    }

    fn file_lock_key(file_id: FileId) -> i64 {
        file_id.0.as_u128() as u64 as i64
    }

    fn calculate_virtual_file_size_from_batch(
        &self,
        batch: &Batch,
        file_type: OutputFileType,
        raw_size_sum: i64,
    ) -> Option<i64> {
        let request_count = match file_type {
            OutputFileType::Output => batch.completed_requests,
            OutputFileType::Error => batch.failed_requests,
        };

        if request_count == 0 {
            return Some(0);
        }

        match file_type {
            OutputFileType::Output => estimate_output_file_size(raw_size_sum, request_count, None),
            OutputFileType::Error => estimate_error_file_size(raw_size_sum, request_count, None),
        }
    }

    fn calculate_virtual_file_size_from_row(
        &self,
        row: &sqlx::postgres::PgRow,
        purpose: &Option<crate::batch::Purpose>,
        size_finalized: bool,
    ) -> Result<Option<i64>> {
        if size_finalized
            || (purpose != &Some(crate::batch::Purpose::BatchOutput)
                && purpose != &Some(crate::batch::Purpose::BatchError))
        {
            return Ok(None);
        }

        let raw_size_sum: Option<i64> = row.try_get("calculated_size").ok().flatten();
        let raw_sum = raw_size_sum.unwrap_or(0);

        let completed: Option<i64> = row.try_get("completed_requests").ok().flatten();
        let failed: Option<i64> = row.try_get("failed_requests").ok().flatten();

        let request_count = if purpose == &Some(crate::batch::Purpose::BatchOutput) {
            completed.unwrap_or(0)
        } else {
            failed.unwrap_or(0)
        };

        if request_count == 0 {
            return Ok(Some(0));
        }

        let estimated_size = if purpose == &Some(crate::batch::Purpose::BatchOutput) {
            estimate_output_file_size(raw_sum, request_count, None)
        } else {
            estimate_error_file_size(raw_sum, request_count, None)
        };

        if estimated_size.is_none() {
            tracing::warn!(
                "File size estimation overflow for {:?} file with {} requests",
                purpose,
                request_count
            );
        }

        Ok(estimated_size)
    }

    fn is_batch_complete(batch: &Batch) -> bool {
        let terminal_count =
            batch.completed_requests + batch.failed_requests + batch.canceled_requests;
        terminal_count == batch.total_requests && batch.total_requests > 0
    }

    async fn finalize_file_size(
        pool: &PgPool,
        retry_config: &crate::DbRetryConfig,
        query_schema: Option<&str>,
        file_id: FileId,
        estimated_size: i64,
    ) -> Result<bool> {
        let lock_key = Self::file_lock_key(file_id);
        let mut transaction =
            crate::db::begin_transaction_in_schema(pool, retry_config, query_schema)
                .await
                .map_err(|e| {
                    FusilladeError::Other(anyhow!("Failed to begin transaction: {}", e))
                })?;

        let lock_acquired =
            match sqlx::query_scalar!("SELECT pg_try_advisory_xact_lock($1)", lock_key)
                .fetch_one(&mut *transaction)
                .await
            {
                Ok(Some(acquired)) => acquired,
                Ok(None) => {
                    tracing::warn!(
                        file_id = %file_id,
                        "Advisory lock query returned NULL unexpectedly"
                    );
                    false
                }
                Err(e) => {
                    tracing::error!(
                        file_id = %file_id,
                        error = %e,
                        "Database error while trying to acquire advisory lock"
                    );
                    return Err(FusilladeError::Other(anyhow!(
                        "Failed to acquire lock: {}",
                        e
                    )));
                }
            };

        if !lock_acquired {
            transaction.rollback().await.map_err(|e| {
                FusilladeError::Other(anyhow!(
                    "Failed to roll back file finalization transaction: {}",
                    e
                ))
            })?;
            return Ok(false);
        }

        sqlx::query!(
            r#"
            UPDATE files
            SET size_bytes = $2, size_finalized = TRUE
            WHERE id = $1 AND size_finalized = FALSE
            "#,
            *file_id as Uuid,
            estimated_size,
        )
        .execute(&mut *transaction)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to update file size: {}", e)))?;

        transaction.commit().await.map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to commit file finalization: {}", e))
        })?;
        Ok(true)
    }

    fn spawn_finalize_if_complete(
        &self,
        row: &sqlx::postgres::PgRow,
        file_id: FileId,
        estimated_size: i64,
    ) {
        let total: Option<i64> = row.try_get("total_requests").ok().flatten();
        let completed: Option<i64> = row.try_get("completed_requests").ok().flatten();
        let failed: Option<i64> = row.try_get("failed_requests").ok().flatten();
        let canceled: Option<i64> = row.try_get("canceled_requests").ok().flatten();
        let in_progress: Option<i64> = row.try_get("in_progress_requests").ok().flatten();

        if let (Some(total_count), Some(comp), Some(fail), Some(canc), Some(_prog)) =
            (total, completed, failed, canceled, in_progress)
        {
            let terminal_count = comp + fail + canc;
            let is_complete = terminal_count == total_count && total_count > 0;

            if is_complete {
                let pools = self.pools.clone();
                let retry_config = self.db_retry_config.clone();
                let query_schema = self.query_schema.clone();

                tokio::spawn(async move {
                    if let Err(e) = Self::finalize_file_size(
                        &pools.write(),
                        &retry_config,
                        query_schema.as_deref(),
                        file_id,
                        estimated_size,
                    )
                    .await
                    {
                        tracing::warn!("Failed to finalize file size for {}: {}", file_id, e);
                    }
                });
            }
        }
    }

    async fn maybe_finalize_file_size(&self, file: &mut File) -> Result<()> {
        if file.size_finalized {
            return Ok(());
        }

        let file_type = match file.purpose {
            Some(crate::batch::Purpose::BatchOutput) => OutputFileType::Output,
            Some(crate::batch::Purpose::BatchError) => OutputFileType::Error,
            _ => return Ok(()),
        };

        let batch = match self.get_batch_by_output_file_id(file.id, file_type).await? {
            Some(b) => b,
            None => return Ok(()),
        };

        let state_filter = match file_type {
            OutputFileType::Output => "completed",
            OutputFileType::Error => "failed",
        };

        let raw_size_sum = sqlx::query_scalar!(
            r#"
            SELECT COALESCE(SUM(response_size), 0)::BIGINT as "sum!"
            FROM requests
            WHERE batch_id = $1
              AND state = $2
            "#,
            *batch.id as Uuid,
            state_filter,
        )
        .fetch_one(self.write_executor())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to calculate file size: {}", e)))?;

        let estimated_size = self
            .calculate_virtual_file_size_from_batch(&batch, file_type, raw_size_sum)
            .unwrap_or(0);

        file.size_bytes = estimated_size;

        if !Self::is_batch_complete(&batch) {
            return Ok(());
        }

        let finalized = Self::finalize_file_size(
            &self.pools.write(),
            &self.db_retry_config,
            self.query_schema.as_deref(),
            file.id,
            estimated_size,
        )
        .await?;

        if finalized {
            file.size_finalized = true;
        }

        Ok(())
    }

    async fn get_file_from_pool(
        &self,
        file_id: FileId,
        executor: crate::db::RetryingPgPool,
    ) -> Result<File> {
        let row = sqlx::query!(
            r#"
            SELECT id, name, description, size_bytes, size_finalized, status, error_message, purpose, expires_at, deleted_at, uploaded_by, created_at, updated_at, api_key_id, source_connection_id, source_external_key
            FROM files
            WHERE id = $1 AND deleted_at IS NULL
            "#,
            *file_id as Uuid,
        )
        .fetch_optional(executor)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch file: {}", e)))?
        .ok_or_else(|| FusilladeError::Other(anyhow!("File not found")))?;

        let status = row
            .status
            .parse::<crate::batch::FileStatus>()
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Invalid file status '{}': {}", row.status, e))
            })?;

        let purpose = row
            .purpose
            .map(|s| s.parse::<crate::batch::Purpose>())
            .transpose()
            .map_err(|e| FusilladeError::Other(anyhow!("Invalid purpose: {}", e)))?;

        Ok(File {
            id: FileId(row.id),
            name: row.name,
            description: row.description,
            size_bytes: row.size_bytes,
            size_finalized: row.size_finalized,
            status,
            error_message: row.error_message,
            purpose,
            expires_at: row.expires_at,
            deleted_at: row.deleted_at,
            uploaded_by: row.uploaded_by,
            created_at: row.created_at,
            updated_at: row.updated_at,
            api_key_id: row.api_key_id,
            source_connection_id: row.source_connection_id,
            source_external_key: row.source_external_key,
        })
    }
}

#[async_trait]
impl<P: PoolProvider> Storage for PostgresRequestManager<P> {
    async fn sum_owner_batch_requests_in_window(
        &self,
        owner: &str,
        completion_window: &str,
        cutoff: DateTime<Utc>,
        strict: bool,
    ) -> Result<i64> {
        let executor = if strict {
            self.write_executor()
        } else {
            self.read_executor()
        };

        let total = sqlx::query_scalar!(
            r#"
            SELECT COALESCE(SUM(total_requests), 0)::BIGINT AS "total!"
            FROM batches
            WHERE created_by = $1
              AND completion_window = $2
              AND created_at >= $3
              AND deleted_at IS NULL
            "#,
            owner,
            completion_window,
            cutoff,
        )
        .fetch_one(executor)
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Failed to sum batch requests for creditor {} in window {}: {}",
                owner,
                completion_window,
                e
            ))
        })?;

        Ok(total)
    }

    async fn count_owner_flex_requests_since(
        &self,
        owner: &str,
        cutoff: DateTime<Utc>,
        _strict: bool,
    ) -> Result<i64> {
        retained_response::count_owner_flex_requests_since(self, owner, cutoff, None).await
    }

    async fn count_owner_flex_requests_since_capped(
        &self,
        owner: &str,
        cutoff: DateTime<Utc>,
        _strict: bool,
        limit: i64,
    ) -> Result<i64> {
        retained_response::count_owner_flex_requests_since(self, owner, cutoff, Some(limit.max(0)))
            .await
    }

    async fn get_pending_request_counts_by_model_and_window(
        &self,
        windows: &[(String, Option<i64>, i64)],
        states: &[String],
        model_filter: &[String],
        service_tier_filter: &ServiceTierFilter,
        strict: bool,
    ) -> Result<HashMap<String, HashMap<String, i64>>> {
        let rows = self
            .get_pending_request_counts_by_model_window_and_tier(
                windows,
                states,
                model_filter,
                service_tier_filter,
                strict,
            )
            .await?;

        let mut result: HashMap<String, HashMap<String, i64>> = HashMap::new();
        for row in rows {
            *result
                .entry(row.model)
                .or_default()
                .entry(row.window_label)
                .or_insert(0) += row.count;
        }
        Ok(result)
    }

    async fn get_pending_request_counts_by_model_window_and_tier(
        &self,
        windows: &[(String, Option<i64>, i64)],
        states: &[String],
        model_filter: &[String],
        service_tier_filter: &ServiceTierFilter,
        strict: bool,
    ) -> Result<Vec<TrailingDemandCount>> {
        let include_background = matches!(
            service_tier_filter,
            ServiceTierFilter::Include(tiers)
                if tiers.iter().any(|tier| tier.as_deref() == Some("background"))
        );

        if (windows.is_empty() && !include_background) || states.is_empty() {
            return Ok(Vec::new());
        }

        let mut labels: Vec<String> = Vec::with_capacity(windows.len());
        let mut starts: Vec<i64> = Vec::with_capacity(windows.len());
        let mut has_starts: Vec<i16> = Vec::with_capacity(windows.len());
        let mut ends: Vec<i64> = Vec::with_capacity(windows.len());
        for (label, start, end) in windows {
            if let Some(start) = start
                && start > end
            {
                return Err(FusilladeError::ValidationError(format!(
                    "window {:?} has start ({}s) > end ({}s)",
                    label, start, end
                )));
            }
            labels.push(label.clone());
            starts.push(start.unwrap_or(0));
            has_starts.push(if start.is_some() { 1 } else { 0 });
            ends.push(*end);
        }

        let (tier_names, tier_include_null, tier_mode): (Vec<String>, bool, &str) =
            match service_tier_filter {
                ServiceTierFilter::Any => (Vec::new(), true, "any"),
                ServiceTierFilter::Include(tiers) => {
                    let (names, has_null) = ServiceTierFilter::split(tiers);
                    (names, has_null, "include")
                }
                ServiceTierFilter::Exclude(tiers) => {
                    let (names, has_null) = ServiceTierFilter::split(tiers);
                    (names, has_null, "exclude")
                }
            };

        let mut tx = if strict {
            self.begin_write().await
        } else {
            self.begin_read().await
        }
        .map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Failed to begin transaction for pending request counts: {}",
                e
            ))
        })?;

        sqlx::query("SET LOCAL plan_cache_mode = force_custom_plan")
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!(
                    "Failed to force custom plan for pending request counts: {}",
                    e
                ))
            })?;

        sqlx::query(&format!(
            "SET LOCAL statement_timeout = {}",
            self.pending_counts_statement_timeout_ms()
        ))
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Failed to set statement_timeout for pending request counts: {}",
                e
            ))
        })?;

        let flex_window_ms =
            self.config
                .service_tier_completion_windows_ms
                .get("flex")
                .copied()
                .unwrap_or(self.config.default_completion_window_ms) as i64;
        let default_window_ms = self.config.default_completion_window_ms as i64;

        let mut rows = sqlx::query(
            r#"
            WITH windows(label, start_seconds, has_start, end_seconds) AS (
                SELECT * FROM UNNEST($1::text[], $2::bigint[], $3::int2[], $4::bigint[])
            ),
            active_request_counts AS (
                SELECT
                    r.batch_id,
                    r.model,
                    r.service_tier,
                    COUNT(*)::BIGINT AS request_count
                FROM requests r
                WHERE r.state = ANY($5)
                -- Batchless rows are counted by batchless_counts below; the
                -- inner join in batch_counts would drop them anyway, so skip
                -- scanning them here and keep the two CTEs disjoint.
                AND r.batch_id IS NOT NULL
                AND r.template_id IS NOT NULL
                AND r.service_tier IS DISTINCT FROM 'background'
                AND (cardinality($6::text[]) = 0 OR r.model = ANY($6))
                AND (
                    $9 = 'any'
                    OR ($9 = 'include' AND (
                        (r.service_tier IS NOT NULL AND r.service_tier = ANY($7))
                        OR ($8 AND r.service_tier IS NULL)
                    ))
                    OR ($9 = 'exclude' AND (
                        (r.service_tier IS NULL AND NOT $8)
                        OR (r.service_tier IS NOT NULL AND r.service_tier <> ALL($7))
                    ))
                )
                GROUP BY r.batch_id, r.model, r.service_tier
            ),
            batch_counts AS (
                SELECT
                    arc.model as model,
                    w.label as window_label,
                    arc.service_tier as service_tier,
                    COALESCE(SUM(arc.request_count) FILTER (
                        WHERE (w.has_start = 0 OR b.expires_at >= NOW() + make_interval(secs => w.start_seconds))
                          AND b.expires_at < NOW() + make_interval(secs => w.end_seconds)
                    ), 0)::BIGINT as count
                FROM active_request_counts arc
                JOIN batches b ON arc.batch_id = b.id
                CROSS JOIN windows w
                -- Only count rows the daemon can actually claim. This MUST stay in
                -- sync with the batch eligibility filter in `claim_requests`
                -- (`ranked_batches`): excluding only `cancelling_at` here while the
                -- claim path also excludes terminal batches lets a `pending` row
                -- stranded under an already-completed batch (e.g. a retry that
                -- raced batch finalization) be reported as claimable forever while
                -- never actually being claimed.
                WHERE b.cancelling_at IS NULL
                  AND b.deleted_at IS NULL
                  AND b.completed_at IS NULL
                  AND b.failed_at IS NULL
                  AND b.cancelled_at IS NULL
                -- Row-level upper bound: a row that expires after every window's
                -- end can't contribute to any window's COUNT FILTER, so prune it
                -- here. Without this, the join reads every active+templated
                -- request and only filters inside the aggregate.
                AND b.expires_at < NOW() + make_interval(secs => (SELECT MAX(end_seconds) FROM windows))
                GROUP BY arc.model, w.label, arc.service_tier
            ),
            batchless_counts AS (
                -- Daemon-claimable rows with no parent batch (flex/async
                -- responses). They carry no batch expiry, so window them by
                -- the deadline the claim path synthesizes for them:
                -- created_at + the service_tier completion window
                -- (flex => $10, NULL/other => $11; milliseconds).
                SELECT
                    r.model as model,
                    w.label as window_label,
                    r.service_tier as service_tier,
                    COUNT(*) FILTER (
                        WHERE (w.has_start = 0 OR r.created_at + ((CASE WHEN r.service_tier = 'flex' THEN $10::BIGINT ELSE $11::BIGINT END) * interval '1 millisecond') >= NOW() + make_interval(secs => w.start_seconds))
                          AND r.created_at + ((CASE WHEN r.service_tier = 'flex' THEN $10::BIGINT ELSE $11::BIGINT END) * interval '1 millisecond') < NOW() + make_interval(secs => w.end_seconds)
                    )::BIGINT as count
                FROM requests r
                CROSS JOIN windows w
                WHERE r.batch_id IS NULL
                AND r.state = ANY($5)
                AND r.template_id IS NOT NULL
                AND r.service_tier IS DISTINCT FROM 'background'
                AND (cardinality($6::text[]) = 0 OR r.model = ANY($6))
                AND (
                    $9 = 'any'
                    OR ($9 = 'include' AND (
                        (r.service_tier IS NOT NULL AND r.service_tier = ANY($7))
                        OR ($8 AND r.service_tier IS NULL)
                    ))
                    OR ($9 = 'exclude' AND (
                        (r.service_tier IS NULL AND NOT $8)
                        OR (r.service_tier IS NOT NULL AND r.service_tier <> ALL($7))
                    ))
                )
                -- Same row-level upper bound as batch_counts: rows whose
                -- deadline is past every window's end can't contribute.
                AND r.created_at + ((CASE WHEN r.service_tier = 'flex' THEN $10::BIGINT ELSE $11::BIGINT END) * interval '1 millisecond') < NOW() + make_interval(secs => (SELECT MAX(end_seconds) FROM windows))
                GROUP BY r.model, w.label, r.service_tier
            )
            SELECT
                model,
                window_label,
                service_tier,
                SUM(count)::BIGINT as count
            FROM (
                SELECT * FROM batch_counts
                UNION ALL
                SELECT * FROM batchless_counts
            ) counts
            GROUP BY model, window_label, service_tier
            "#,
        )
        .bind(&labels)
        .bind(&starts)
        .bind(&has_starts)
        .bind(&ends)
        .bind(states)
        .bind(model_filter)
        .bind(&tier_names)
        .bind(tier_include_null)
        .bind(tier_mode)
        .bind(flex_window_ms)
        .bind(default_window_ms)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Failed to get pending request counts by model and window: {}",
                e
            ))
        })?;

        if include_background {
            let background_rows = sqlx::query(
                r#"
                SELECT r.model,
                       'background'::TEXT AS window_label,
                       'background'::TEXT AS service_tier,
                       COUNT(*)::BIGINT AS count
                FROM requests r
                LEFT JOIN batches b ON b.id = r.batch_id
                WHERE r.service_tier = 'background'
                  AND r.state = ANY($1)
                  AND r.template_id IS NOT NULL
                  AND (cardinality($2::TEXT[]) = 0 OR r.model = ANY($2))
                  AND (
                      r.batch_id IS NULL
                      OR (
                          b.id IS NOT NULL
                          AND b.service_tier = 'background'
                          AND b.cancelling_at IS NULL
                          AND b.deleted_at IS NULL
                          AND b.completed_at IS NULL
                          AND b.failed_at IS NULL
                          AND b.cancelled_at IS NULL
                      )
                  )
                GROUP BY r.model
                "#,
            )
            .bind(states)
            .bind(model_filter)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!(
                    "Failed to get background pending request counts: {}",
                    e
                ))
            })?;
            rows.extend(background_rows);
        }

        tx.commit().await.map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Failed to commit pending request counts transaction: {}",
                e
            ))
        })?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let model: String = row
                .try_get("model")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read model: {}", e)))?;
            let window_label: String = row.try_get("window_label").map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to read window_label: {}", e))
            })?;
            let service_tier: Option<String> = row.try_get("service_tier").map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to read service_tier: {}", e))
            })?;
            let count: i64 = row
                .try_get("count")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read count: {}", e)))?;
            result.push(TrailingDemandCount {
                model,
                window_label,
                service_tier,
                outcome: "pending".to_string(),
                count,
            });
        }

        Ok(result)
    }

    async fn get_completed_request_counts_by_model_and_window(
        &self,
        windows: &[(String, i64, i64)],
        model_filter: &[String],
        service_tier_filter: &ServiceTierFilter,
    ) -> Result<Vec<TrailingDemandCount>> {
        if windows.is_empty() {
            return Ok(Vec::new());
        }

        for (label, start, end) in windows {
            if start >= end || *end > 0 {
                return Err(FusilladeError::ValidationError(format!(
                    "trailing window {:?} must satisfy start < end <= 0 (got {}s..{}s)",
                    label, start, end
                )));
            }
        }

        let (tier_names, tier_include_null, tier_mode): (Vec<String>, bool, &str) =
            match service_tier_filter {
                ServiceTierFilter::Any => (Vec::new(), true, "any"),
                ServiceTierFilter::Include(tiers) => {
                    let (names, has_null) = ServiceTierFilter::split(tiers);
                    (names, has_null, "include")
                }
                ServiceTierFilter::Exclude(tiers) => {
                    let (names, has_null) = ServiceTierFilter::split(tiers);
                    (names, has_null, "exclude")
                }
            };

        let mut tx = self.begin_write().await.map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Failed to begin transaction for trailing request counts: {}",
                e
            ))
        })?;

        sqlx::query("SET LOCAL plan_cache_mode = force_custom_plan")
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!(
                    "Failed to force custom plan for trailing request counts: {}",
                    e
                ))
            })?;

        sqlx::query(&format!(
            "SET LOCAL statement_timeout = {}",
            self.pending_counts_statement_timeout_ms()
        ))
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Failed to set statement_timeout for trailing request counts: {}",
                e
            ))
        })?;

        const RETENTION_PRUNE_SLACK_DAYS: i64 = 2;
        let retention_bounds = self.retained_response_retention_bounds_seconds;
        let now = Utc::now();
        let mut result: Vec<TrailingDemandCount> = Vec::new();
        for (label, start, end) in windows {
            let start_ts = now + chrono::Duration::seconds(*start);
            let end_ts = now + chrono::Duration::seconds(*end);
            let (prune_lower, prune_upper) = match retention_bounds {
                Some((min_secs, max_secs)) => {
                    let min_days = (min_secs / 86_400) as i64;
                    let max_days = max_secs.div_ceil(86_400) as i64;
                    let lower_offset = (min_days - RETENTION_PRUNE_SLACK_DAYS).max(1);
                    (
                        Some(start_ts.date_naive() + chrono::Duration::days(lower_offset)),
                        Some(
                            end_ts.date_naive()
                                + chrono::Duration::days(max_days + RETENTION_PRUNE_SLACK_DAYS),
                        ),
                    )
                }
                None => (None, None),
            };
            let rows = sqlx::query(retained_response::TRAILING_DEMAND_SQL)
                .bind(start_ts)
                .bind(end_ts)
                .bind(model_filter)
                .bind(&tier_names)
                .bind(tier_include_null)
                .bind(tier_mode)
                .bind(prune_lower)
                .bind(prune_upper)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| {
                    FusilladeError::Other(anyhow!(
                        "Failed to get trailing request counts for window {:?}: {}",
                        label,
                        e
                    ))
                })?;

            for row in rows {
                result.push(TrailingDemandCount {
                    model: row.try_get("model").map_err(|e| {
                        FusilladeError::Other(anyhow!("Failed to read model: {}", e))
                    })?,
                    window_label: label.clone(),
                    service_tier: row.try_get("service_tier").map_err(|e| {
                        FusilladeError::Other(anyhow!("Failed to read service_tier: {}", e))
                    })?,
                    outcome: row.try_get("outcome").map_err(|e| {
                        FusilladeError::Other(anyhow!("Failed to read outcome: {}", e))
                    })?,
                    count: row.try_get("count").map_err(|e| {
                        FusilladeError::Other(anyhow!("Failed to read count: {}", e))
                    })?,
                });
            }
        }

        tx.commit().await.map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Failed to commit trailing request counts transaction: {}",
                e
            ))
        })?;

        Ok(result)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, available_capacity, user_active_counts),
        fields(limit)
    )]
    async fn claim_batchless_requests(
        &self,
        limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
        leak_cooldown: &HashSet<(String, String, String)>,
    ) -> Result<Vec<Request<Claimed>>> {
        let unclaimed_count = self.unclaim_stale_requests().await?;
        if unclaimed_count > 0 {
            tracing::info!(
                unclaimed_count,
                "Unclaimed stale requests before claiming new ones"
            );
        }

        let now = Utc::now();

        let mut model_capacity_pairs: Vec<(String, i64)> = available_capacity
            .iter()
            .filter(|(_, cap)| **cap > 0)
            .map(|(model, cap)| (model.clone(), *cap as i64))
            .collect();
        {
            use rand::seq::SliceRandom;
            let mut rng = rand::rng();
            model_capacity_pairs.shuffle(&mut rng);
        }

        let models_arr: Vec<String> = model_capacity_pairs
            .iter()
            .map(|(m, _)| m.clone())
            .collect();
        let capacities_arr: Vec<i64> = model_capacity_pairs.iter().map(|(_, c)| *c).collect();

        tracing::debug!(
            model_count = models_arr.len(),
            "Claiming for models with available capacity"
        );

        if models_arr.is_empty() {
            tracing::debug!("No models with available capacity, skipping claim");
            return Ok(Vec::new());
        }

        let user_ids_arr: Vec<String> = user_active_counts.keys().cloned().collect();
        let user_counts_arr: Vec<i64> = user_ids_arr
            .iter()
            .map(|u| *user_active_counts.get(u).unwrap_or(&0) as i64)
            .collect();

        let mut cooldown_user_arr: Vec<String> = Vec::with_capacity(leak_cooldown.len());
        let mut cooldown_window_arr: Vec<String> = Vec::with_capacity(leak_cooldown.len());
        let mut cooldown_model_arr: Vec<String> = Vec::with_capacity(leak_cooldown.len());
        for (user, window, model) in leak_cooldown {
            cooldown_user_arr.push(user.clone());
            cooldown_window_arr.push(window.clone());
            cooldown_model_arr.push(model.clone());
        }

        let ramp_exponent = self.config.claim_ramp_exponent;

        let tier_names_arr: Vec<String> = self
            .config
            .service_tier_completion_windows_ms
            .keys()
            .cloned()
            .collect();
        let tier_windows_arr: Vec<i64> = tier_names_arr
            .iter()
            .map(|t| self.config.service_tier_completion_windows_ms[t] as i64)
            .collect();
        let default_window_ms = self.config.default_completion_window_ms as i64;

        let rows = sqlx::query_as!(
            ClaimedRequestRow,
            r#"
            WITH all_models AS (
                SELECT model, capacity FROM unnest($4::TEXT[], $5::BIGINT[]) AS m(model, capacity)
            ),
            user_priority AS (
                SELECT * FROM unnest($6::TEXT[], $7::BIGINT[]) AS u(user_id, active_count)
            ),
            to_claim AS (
                SELECT claimed.id, claimed.template_id, claimed.batch_id, claimed.effective_expires_at,
                       claimed.leaked, claimed.window_class, claimed.window_secs
                FROM all_models m
                CROSS JOIN LATERAL (
                    SELECT c.id, c.template_id, c.batch_id, c.effective_expires_at,
                           c.leaked, c.window_class, c.window_secs
                    FROM (
                        SELECT bl.id, bl.template_id, bl.batch_id, bl.effective_expires_at,
                               bl.leaked, bl.window_class, bl.window_secs,
                               bl.ord_blend, bl.ord_exp, bl.ord_id
                        FROM (
                            SELECT r.id, r.template_id, r.batch_id,
                                   e.eff AS effective_expires_at,
                                   FALSE AS leaked, e.window_class AS window_class,
                                   e.w_secs AS window_secs,
                                   calc.blend AS ord_blend, e.eff AS ord_exp, r.id AS ord_id
                            FROM requests r
                            LEFT JOIN user_priority up ON r.created_by = up.user_id
                            LEFT JOIN LATERAL (
                                SELECT mfe.state FROM model_filters mfe
                                WHERE mfe.model = m.model
                                ORDER BY mfe.created_at DESC, mfe.id DESC LIMIT 1
                            ) mf ON true
                            CROSS JOIN LATERAL (
                                SELECT COALESCE(
                                    (SELECT stw.window_ms FROM unnest($10::TEXT[], $11::BIGINT[]) AS stw(tier, window_ms)
                                     WHERE stw.tier = r.service_tier),
                                    $12::BIGINT) AS window_ms
                            ) wm
                            CROSS JOIN LATERAL (
                                SELECT COALESCE(r.service_tier, 'default') AS window_class,
                                       wm.window_ms::DOUBLE PRECISION / 1000.0 AS w_secs,
                                       r.created_at + (wm.window_ms * interval '1 millisecond') AS eff
                            ) e
                            CROSS JOIN LATERAL (
                                SELECT
                                    (1.0 - $8::DOUBLE PRECISION)
                                        * COALESCE(up.active_count, 0)::DOUBLE PRECISION
                                        / GREATEST(NULLIF((SELECT MAX(v) FROM unnest($7::BIGINT[]) v), 0), 1)::DOUBLE PRECISION
                                    + $8::DOUBLE PRECISION
                                        * LEAST(GREATEST(EXTRACT(EPOCH FROM e.eff - $3), 0.0) / 86400.0, 1.0) AS blend
                            ) calc
                            WHERE r.state = 'pending' AND r.model = m.model AND r.batch_id IS NULL
                              AND r.template_id IS NOT NULL
                              AND r.service_tier IS DISTINCT FROM 'background'
                              AND (r.not_before IS NULL OR r.not_before <= $3)
                              AND ((mf.state IS NULL OR mf.state = 'live')
                                   OR (EXTRACT(EPOCH FROM (e.eff - $3))
                                       <= power(GREATEST(e.w_secs, 0.0) / 60.0, $9::DOUBLE PRECISION) * 60.0))
                            ORDER BY calc.blend ASC, e.eff ASC, r.id ASC
                            LIMIT m.capacity
                            FOR UPDATE OF r SKIP LOCKED
                        ) bl

                      UNION ALL
                        SELECT blb.id, blb.template_id, blb.batch_id, blb.effective_expires_at,
                               blb.leaked, blb.window_class, blb.window_secs,
                               blb.ord_blend, blb.ord_exp, blb.ord_id
                        FROM (
                            SELECT picks.id, picks.template_id, picks.batch_id, picks.effective_expires_at,
                                   picks.leaked, picks.window_class, picks.window_secs,
                                   picks.ord_blend, picks.ord_exp, picks.ord_id
                            FROM (
                                SELECT DISTINCT ON (cand.created_by, cand.window_class)
                                       cand.id, cand.template_id, cand.batch_id,
                                       cand.eff AS effective_expires_at,
                                       TRUE AS leaked, cand.window_class AS window_class,
                                       cand.w_secs AS window_secs,
                                       cand.blend AS ord_blend, cand.eff AS ord_exp, cand.id AS ord_id
                                FROM (
                                    SELECT r.id, r.template_id, r.batch_id, r.created_by,
                                           e.window_class, e.w_secs, e.eff, calc.blend
                                    FROM requests r
                                    LEFT JOIN user_priority up ON r.created_by = up.user_id
                                    LEFT JOIN LATERAL (
                                        SELECT mfe.state FROM model_filters mfe
                                        WHERE mfe.model = m.model
                                        ORDER BY mfe.created_at DESC, mfe.id DESC LIMIT 1
                                    ) mf ON true
                                    CROSS JOIN LATERAL (
                                        SELECT COALESCE(
                                            (SELECT stw.window_ms FROM unnest($10::TEXT[], $11::BIGINT[]) AS stw(tier, window_ms)
                                             WHERE stw.tier = r.service_tier),
                                            $12::BIGINT) AS window_ms
                                    ) wm
                                    CROSS JOIN LATERAL (
                                        SELECT COALESCE(r.service_tier, 'default') AS window_class,
                                               wm.window_ms::DOUBLE PRECISION / 1000.0 AS w_secs,
                                               r.created_at + (wm.window_ms * interval '1 millisecond') AS eff
                                    ) e
                                    CROSS JOIN LATERAL (
                                        SELECT
                                            (1.0 - $8::DOUBLE PRECISION)
                                                * COALESCE(up.active_count, 0)::DOUBLE PRECISION
                                                / GREATEST(NULLIF((SELECT MAX(v) FROM unnest($7::BIGINT[]) v), 0), 1)::DOUBLE PRECISION
                                            + $8::DOUBLE PRECISION
                                                * LEAST(GREATEST(EXTRACT(EPOCH FROM e.eff - $3), 0.0) / 86400.0, 1.0) AS blend
                                    ) calc
                                    WHERE r.state = 'pending' AND r.model = m.model AND r.batch_id IS NULL
                                      AND r.template_id IS NOT NULL
                                      AND r.service_tier IS DISTINCT FROM 'background'
                                      AND (r.not_before IS NULL OR r.not_before <= $3)
                                      AND NOT ((mf.state IS NULL OR mf.state = 'live')
                                               OR (EXTRACT(EPOCH FROM (e.eff - $3))
                                                   <= power(GREATEST(e.w_secs, 0.0) / 60.0, $9::DOUBLE PRECISION) * 60.0))
                                      AND NOT EXISTS (
                                          SELECT 1 FROM unnest($13::TEXT[], $14::TEXT[], $15::TEXT[]) AS cd(u, w, mdl)
                                          WHERE cd.u = r.created_by AND cd.w = COALESCE(r.service_tier, 'default') AND cd.mdl = r.model
                                      )
                                ) cand
                                ORDER BY cand.created_by, cand.window_class,
                                         cand.blend ASC, cand.eff ASC, cand.id ASC
                            ) picks
                            CROSS JOIN LATERAL (
                                SELECT 1 FROM requests r
                                WHERE r.id = picks.id AND r.state = 'pending'
                                FOR UPDATE OF r SKIP LOCKED
                            ) lk
                        ) blb
                    ) c
                    -- Source A (leaked = false) fills capacity first; Source B
                    -- trickle takes any leftover, ordered by the same blend.
                    ORDER BY c.leaked ASC, c.ord_blend ASC, c.ord_exp ASC, c.ord_id ASC
                    LIMIT m.capacity
                ) claimed
                LIMIT $2::BIGINT
            ),
            -- A pending row whose template is gone (its file soft-deleted, its
            -- weekly partition retired, or the row purged) can never be claimed.
            -- Fail it here, bounded to the rows this tick actually picked, so it
            -- burns a claim slot exactly once instead of on every tick; the
            -- claim update below skips it via its CROSS JOIN on the template.
            stranded AS (
                UPDATE requests r
                   SET state = 'failed',
                       error = 'request template no longer exists',
                       failed_at = $3
                  FROM to_claim tc
                  -- Per-row lookup by primary key, like the claim join below:
                  -- a bare NOT EXISTS on the generation-transparent view gets
                  -- flattened into an anti-join that hashes the whole legacy
                  -- template table; LIMIT 1 keeps it a probe per picked row.
                  LEFT JOIN LATERAL (
                      SELECT 1 AS present FROM active_request_templates t
                      WHERE t.id = tc.template_id LIMIT 1
                  ) t ON true
                 WHERE r.id = tc.id
                   AND t.present IS NULL
                RETURNING r.id
            )
            UPDATE requests r
            SET
                state = 'claimed',
                daemon_id = $1,
                claimed_at = $3
            FROM to_claim tc
            CROSS JOIN LATERAL (
                -- Per-row lookup by primary key. A plain join on the
                -- generation-transparent view lets the planner hash the whole
                -- legacy template table against the handful of claimed rows;
                -- LIMIT 1 keeps this subquery from being flattened into that join.
                SELECT * FROM active_request_templates t WHERE t.id = tc.template_id LIMIT 1
            ) t
            WHERE r.id = tc.id
            RETURNING r.id,
                      r.batch_id,
                      r.template_id as "template_id!", r.retry_attempt,
                      t.custom_id, t.endpoint as "endpoint!", t.method as "method!", t.path as "path!",
                      t.body as "body!", t.model as "model!",
                      t.api_key_id,
                      tc.effective_expires_at as "batch_expires_at?",
                      ''::TEXT as "batch_id_str!",
                      ''::TEXT as "batch_file_id!",
                      t.endpoint as "batch_endpoint!",
                      '1h'::TEXT as "batch_completion_window?",
                      -- A batchless request's metadata lives on its template; it is read
                      -- through the same allow-list and forwarded under the same header
                      -- names as a batch's, so the claim path treats both alike.
                      t.metadata::TEXT as "batch_metadata",
                      NULL::TEXT as "batch_output_file_id",
                      NULL::TEXT as "batch_error_file_id",
                      COALESCE(r.created_by, '') as "batch_created_by!",
                      to_char(r.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as "batch_created_at!",
                      to_char(tc.effective_expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as "batch_expires_at_str",
                      NULL::TEXT as "batch_cancelling_at",
                      NULL::TEXT as "batch_errors",
                      '1'::TEXT as "batch_total_requests!",
                      tc.leaked as "leaked!",
                      tc.window_class as "window_class!",
                      tc.window_secs as "window_secs!"
            "#,
            *daemon_id as Uuid,
            limit as i64,
            now,
            &models_arr,
            &capacities_arr,
            &user_ids_arr,
            &user_counts_arr,
            self.config.urgency_weight,
            ramp_exponent,
            &tier_names_arr,
            &tier_windows_arr,
            default_window_ms,
            &cooldown_user_arr,
            &cooldown_window_arr,
            &cooldown_model_arr,
        )
        .fetch_all(self.write_executor())
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Failed to claim batchless requests: {}",
                e
            ))
        })?;

        let claimed_count = rows.len();
        if claimed_count > 0 {
            tracing::debug!(
                claimed = claimed_count,
                "Claimed batchless requests across all models"
            );
        }

        Ok(self.claimed_rows_to_requests(rows, daemon_id, now))
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, available_capacity, user_active_counts),
        fields(limit, batch_limit)
    )]
    async fn claim_batch_requests(
        &self,
        limit: usize,
        batch_limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
    ) -> Result<Vec<Request<Claimed>>> {
        self.claim_batch_requests_inner(
            limit,
            batch_limit,
            daemon_id,
            available_capacity,
            user_active_counts,
            None,
        )
        .await
    }

    async fn claim_batch_requests_with_cooldown(
        &self,
        limit: usize,
        batch_limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
        leak_cooldown: &std::collections::HashSet<(String, String, String)>,
    ) -> Result<Vec<Request<Claimed>>> {
        self.claim_batch_requests_inner(
            limit,
            batch_limit,
            daemon_id,
            available_capacity,
            user_active_counts,
            Some(leak_cooldown),
        )
        .await
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, available_capacity, user_active_counts),
        fields(limit, batch_limit)
    )]
    async fn claim_background_requests_by_kind(
        &self,
        kind: BackgroundClaimKind,
        limit: usize,
        batch_limit: usize,
        daemon_id: DaemonId,
        available_capacity: &HashMap<String, usize>,
        user_active_counts: &HashMap<String, usize>,
    ) -> Result<Vec<Request<Claimed>>> {
        let now = Utc::now();
        let claim_batches = kind == BackgroundClaimKind::Batch;
        let mut model_capacity_pairs: Vec<(String, i64)> = available_capacity
            .iter()
            .filter(|(_, capacity)| **capacity > 0)
            .map(|(model, capacity)| (model.clone(), *capacity as i64))
            .collect();
        {
            use rand::seq::SliceRandom;
            let mut rng = rand::rng();
            model_capacity_pairs.shuffle(&mut rng);
        }

        if model_capacity_pairs.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }

        let models_arr: Vec<String> = model_capacity_pairs
            .iter()
            .map(|(model, _)| model.clone())
            .collect();
        let capacities_arr: Vec<i64> = model_capacity_pairs
            .iter()
            .map(|(_, capacity)| *capacity)
            .collect();
        let user_ids_arr: Vec<String> = user_active_counts.keys().cloned().collect();
        let user_counts_arr: Vec<i64> = user_ids_arr
            .iter()
            .map(|owner| *user_active_counts.get(owner).unwrap_or(&0) as i64)
            .collect();

        let rows = sqlx::query_as!(
            ClaimedRequestRow,
            r#"
            WITH locked_models AS (
                SELECT model, capacity, model_order
                FROM unnest($4::TEXT[], $5::BIGINT[]) WITH ORDINALITY
                     AS m(model, capacity, model_order)
            ),
            user_priority AS (
                SELECT *
                FROM unnest($6::TEXT[], $7::BIGINT[]) AS u(user_id, active_count)
            ),
            active_counts AS (
                SELECT locked.model, COUNT(active.id)::BIGINT AS active_count
                FROM locked_models locked
                LEFT JOIN requests active
                  ON active.model = locked.model
                 AND active.state IN ('claimed', 'processing')
                 AND active.template_id IS NOT NULL
                 AND active.service_tier IS DISTINCT FROM 'background'
                GROUP BY locked.model
            ),
            eligible_models AS (
                SELECT m.model,
                       LEAST(
                           m.capacity,
                           GREATEST($9::BIGINT - active.active_count, 0)
                       ) AS capacity,
                       m.model_order
                FROM locked_models m
                JOIN active_counts active ON active.model = m.model
                JOIN LATERAL (
                    SELECT mf.state
                    FROM model_filters mf
                    WHERE mf.model = m.model
                    ORDER BY mf.created_at DESC, mf.id DESC
                    LIMIT 1
                ) latest ON latest.state = 'live'
                WHERE active.active_count < $9::BIGINT
                  AND NOT EXISTS (
                    SELECT 1
                    FROM requests sla
                    LEFT JOIN batches parent ON parent.id = sla.batch_id
                    WHERE sla.model = m.model
                      AND sla.state = 'pending'
                      AND sla.template_id IS NOT NULL
                      AND sla.service_tier IS DISTINCT FROM 'background'
                      AND (sla.not_before IS NULL OR sla.not_before <= $3)
                      AND (
                          sla.batch_id IS NULL
                          OR (
                              parent.id IS NOT NULL
                              AND parent.service_tier IS DISTINCT FROM 'background'
                              AND parent.cancelling_at IS NULL
                              AND parent.deleted_at IS NULL
                              AND parent.completed_at IS NULL
                              AND parent.failed_at IS NULL
                              AND parent.cancelled_at IS NULL
                          )
                      )
                )
            ),
            selected_batches AS (
                SELECT em.model, selected.id AS batch_id
                FROM eligible_models em
                CROSS JOIN LATERAL (
                    SELECT b.id
                    FROM batches b
                    WHERE $10::BOOLEAN
                      AND b.service_tier = 'background'
                      AND b.cancelling_at IS NULL
                      AND b.deleted_at IS NULL
                      AND b.completed_at IS NULL
                      AND b.failed_at IS NULL
                      AND b.cancelled_at IS NULL
                      AND EXISTS (
                          SELECT 1
                          FROM requests pending
                          WHERE pending.batch_id = b.id
                            AND pending.model = em.model
                            AND pending.state = 'pending'
                            AND pending.template_id IS NOT NULL
                            AND pending.service_tier = 'background'
                            AND (pending.not_before IS NULL OR pending.not_before <= $3)
                      )
                    ORDER BY b.created_at, b.id
                    LIMIT $8::BIGINT
                ) selected
            ),
            candidate_rows AS (
                SELECT r.id, r.template_id, r.batch_id, r.model, r.created_at,
                       r.created_by AS owner, em.capacity, em.model_order
                FROM eligible_models em
                JOIN requests r
                  ON r.model = em.model
                 AND r.batch_id IS NULL
                 AND r.state = 'pending'
                 AND r.template_id IS NOT NULL
                 AND r.service_tier = 'background'
                 AND (r.not_before IS NULL OR r.not_before <= $3)
                WHERE NOT $10::BOOLEAN

                UNION ALL

                SELECT r.id, r.template_id, r.batch_id, r.model, r.created_at,
                       b.created_by AS owner, em.capacity, em.model_order
                FROM eligible_models em
                JOIN selected_batches sb ON sb.model = em.model
                JOIN batches b ON b.id = sb.batch_id
                JOIN requests r
                  ON r.batch_id = b.id
                 AND r.model = em.model
                 AND r.state = 'pending'
                 AND r.template_id IS NOT NULL
                 AND r.service_tier = 'background'
                 AND (r.not_before IS NULL OR r.not_before <= $3)
            ),
            owner_ranked AS (
                SELECT c.*,
                       COALESCE(up.active_count, 0) AS active_count,
                       row_number() OVER (
                           PARTITION BY c.model, c.owner
                           ORDER BY c.created_at, c.id
                       ) AS owner_rank
                FROM candidate_rows c
                LEFT JOIN user_priority up ON up.user_id = c.owner
            ),
            model_ranked AS (
                SELECT o.*,
                       row_number() OVER (
                           PARTITION BY o.model
                           ORDER BY o.active_count + o.owner_rank - 1,
                                    o.created_at, o.id
                       ) AS model_rank
                FROM owner_ranked o
            ),
            locked AS (
                SELECT ranked.id, ranked.template_id, ranked.batch_id,
                       ranked.model_rank, ranked.model_order, ranked.created_at
                FROM model_ranked ranked
                JOIN requests lock_row
                  ON lock_row.id = ranked.id
                 AND lock_row.state = 'pending'
                 AND lock_row.service_tier = 'background'
                 AND (lock_row.not_before IS NULL OR lock_row.not_before <= $3)
                WHERE ranked.model_rank <= ranked.capacity
                ORDER BY ranked.model_rank, ranked.model_order, ranked.created_at, ranked.id
                LIMIT $2::BIGINT
                FOR UPDATE OF lock_row SKIP LOCKED
            ),
            -- A pending row whose template is gone (its file soft-deleted, its
            -- weekly partition retired, or the row purged) can never be claimed.
            -- Fail it here, bounded to the rows this tick actually picked, so it
            -- burns a claim slot exactly once instead of on every tick; the
            -- claim update below skips it via its CROSS JOIN on the template.
            stranded AS (
                UPDATE requests r
                   SET state = 'failed',
                       error = 'request template no longer exists',
                       failed_at = $3
                  FROM locked claimed
                  -- Per-row lookup by primary key, like the claim join below:
                  -- a bare NOT EXISTS on the generation-transparent view gets
                  -- flattened into an anti-join that hashes the whole legacy
                  -- template table; LIMIT 1 keeps it a probe per picked row.
                  LEFT JOIN LATERAL (
                      SELECT 1 AS present FROM active_request_templates t
                      WHERE t.id = claimed.template_id LIMIT 1
                  ) t ON true
                 WHERE r.id = claimed.id
                   AND t.present IS NULL
                RETURNING r.id
            )
            UPDATE requests r
            SET state = 'claimed',
                daemon_id = $1,
                claimed_at = $3
            FROM locked claimed
            CROSS JOIN LATERAL (
                -- Per-row lookup by primary key. A plain join on the
                -- generation-transparent view lets the planner hash the whole
                -- legacy template table against the handful of claimed rows;
                -- LIMIT 1 keeps this subquery from being flattened into that join.
                SELECT * FROM active_request_templates t WHERE t.id = claimed.template_id LIMIT 1
            ) t
            LEFT JOIN batches b ON b.id = claimed.batch_id
            WHERE r.id = claimed.id
            RETURNING r.id,
                      r.batch_id,
                      r.template_id as "template_id!", r.retry_attempt,
                      t.custom_id, t.endpoint as "endpoint!", t.method as "method!", t.path as "path!",
                      t.body as "body!", t.model as "model!",
                      COALESCE(b.api_key_id, t.api_key_id) as "api_key_id",
                      NULL::TIMESTAMPTZ as "batch_expires_at?",
                      COALESCE(b.id::TEXT, '') as "batch_id_str!",
                      COALESCE(b.file_id::TEXT, '') as "batch_file_id!",
                      COALESCE(b.endpoint, t.endpoint) as "batch_endpoint!",
                      'background'::TEXT as "batch_completion_window?",
                      -- This query claims both batched and batchless background rows, so
                      -- take the batch's metadata when there is a batch and the template's
                      -- when there isn't. b.metadata is NULL for batchless rows and
                      -- t.metadata is NULL for file-ingested ones, so the two never race.
                      COALESCE(b.metadata, t.metadata)::TEXT as "batch_metadata",
                      b.output_file_id::TEXT as "batch_output_file_id",
                      b.error_file_id::TEXT as "batch_error_file_id",
                      COALESCE(b.created_by, r.created_by, '') as "batch_created_by!",
                      to_char(COALESCE(b.created_at, r.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as "batch_created_at!",
                      NULL::TEXT as "batch_expires_at_str",
                      to_char(b.cancelling_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as "batch_cancelling_at",
                      b.errors::TEXT as "batch_errors",
                      COALESCE(b.total_requests::TEXT, '1') as "batch_total_requests!",
                      FALSE as "leaked!",
                      'background'::TEXT as "window_class!",
                      0::DOUBLE PRECISION as "window_secs!"
            "#,
            *daemon_id as Uuid,
            limit as i64,
            now,
            &models_arr,
            &capacities_arr,
            &user_ids_arr,
            &user_counts_arr,
            batch_limit as i64,
            self.config.background_concurrency_limit as i64,
            claim_batches,
        )
        .fetch_all(self.write_executor())
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to claim background requests: {}", e))
        })?;

        if !rows.is_empty() {
            tracing::debug!(claimed = rows.len(), "Claimed background requests");
        }

        Ok(self.claimed_rows_to_requests(rows, daemon_id, now))
    }

    fn supports_background_claims(&self) -> bool {
        true
    }

    async fn claim_requests(
        &self,
        limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
        leak_cooldown: &HashSet<(String, String, String)>,
    ) -> Result<Vec<Request<Claimed>>> {
        self.claim_batchless_requests(
            limit,
            daemon_id,
            available_capacity,
            user_active_counts,
            leak_cooldown,
        )
        .await
    }

    async fn append_model_filter_event(&self, entry: &ModelFilter) -> Result<()> {
        self.append_model_filter_events(std::slice::from_ref(entry))
            .await
    }

    async fn append_model_filter_events(&self, entries: &[ModelFilter]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let now = Utc::now();
        let models: Vec<String> = entries.iter().map(|e| e.model.clone()).collect();
        let states: Vec<String> = entries
            .iter()
            .map(|e| e.state.as_str().to_string())
            .collect();
        let etas: Vec<Option<DateTime<Utc>>> =
            entries.iter().map(|e| e.expected_ready_at).collect();

        sqlx::query!(
            r#"
            INSERT INTO model_filters (model, state, expected_ready_at, created_at)
            SELECT model, state, expected_ready_at, $4
            FROM unnest($1::TEXT[], $2::TEXT[], $3::TIMESTAMPTZ[])
                WITH ORDINALITY AS t(model, state, expected_ready_at, ord)
            ORDER BY ord
            "#,
            &models,
            &states,
            &etas as &[Option<DateTime<Utc>>],
            now,
        )
        .execute(self.write_executor())
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to append model_filters events: {}", e))
        })?;

        Ok(())
    }

    async fn list_model_filters(&self) -> Result<Vec<ModelFilter>> {
        let rows = sqlx::query!(
            r#"
            SELECT model, state, expected_ready_at
            FROM (
                SELECT DISTINCT ON (model)
                    model, state, expected_ready_at
                FROM model_filters
                ORDER BY model, created_at DESC, id DESC
            ) latest
            WHERE state <> 'absent'
            ORDER BY model
            "#
        )
        .fetch_all(self.read_executor())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to list model_filters: {}", e)))?;

        rows.into_iter()
            .map(|row| {
                let state = ModelFilterState::parse_state(&row.state).ok_or_else(|| {
                    FusilladeError::Other(anyhow!("Unknown model_filters.state: {}", row.state))
                })?;
                Ok(ModelFilter {
                    model: row.model,
                    state,
                    expected_ready_at: row.expected_ready_at,
                })
            })
            .collect()
    }

    async fn current_filter_states(
        &self,
    ) -> Result<std::collections::HashMap<String, (ModelFilterState, DateTime<Utc>)>> {
        let rows = sqlx::query!(
            r#"
            SELECT DISTINCT ON (model)
                model, state, created_at
            FROM model_filters
            ORDER BY model, created_at DESC, id DESC
            "#
        )
        .fetch_all(self.read_executor())
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Failed to read current model_filters states: {}",
                e
            ))
        })?;

        rows.into_iter()
            .map(|row| {
                let state = ModelFilterState::parse_state(&row.state).ok_or_else(|| {
                    FusilladeError::Other(anyhow!("Unknown model_filters.state: {}", row.state))
                })?;
                Ok((row.model, (state, row.created_at)))
            })
            .collect()
    }

    async fn persist<T: RequestState + Clone>(
        &self,
        request: &Request<T>,
    ) -> Result<Option<RequestId>>
    where
        AnyRequest: From<Request<T>>,
    {
        const MAX_ATTEMPTS: u32 = 3;

        let mut any_request = AnyRequest::from(request.clone());
        if let Some(transformer) = self.response_transformer.get() {
            match &mut any_request {
                AnyRequest::Completed(req) => {
                    req.state.response_body = transformer
                        .transform(&req.data, &req.state.response_body)
                        .await?;
                }
                AnyRequest::Failed(req) => {
                    match &mut req.state.reason {
                        FailureReason::RetriableHttpStatus { body, .. }
                        | FailureReason::NonRetriableHttpStatus { body, .. } => {
                            *body = transformer.transform(&req.data, body).await?;
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        for attempt in 0..MAX_ATTEMPTS {
            tracing::debug!("Persisting request state");
            let any_request = any_request.clone();

            let state_write_permit = self.state_write_limiter.acquire("persist").await?;
            let result: Result<Option<RequestId>> = async {
                let mut tx = self.begin_response_write(&[request.data.id.0]).await?;
                match any_request {
                    AnyRequest::Pending(req) => {
                        let rows_affected = sqlx::query!(
                            r#"
                            UPDATE requests SET
                                state = 'pending',
                                retry_attempt = $2,
                                not_before = $3,
                                daemon_id = NULL,
                                claimed_at = NULL,
                                started_at = NULL
                            WHERE id = $1
                            "#,
                            *req.data.id as Uuid,
                            req.state.retry_attempt as i32,
                            req.state.not_before,
                        )
                        .execute(&mut *tx)
                        .await
                        .map_err(|_| FusilladeError::Other(anyhow!("Failed to update request")))?
                        .rows_affected();

                        if rows_affected == 0 {
                            return Self::dropped_or_missing_in_transaction(&mut tx, req.data.id)
                                .await;
                        }
                    }
                    AnyRequest::Claimed(req) => {
                        let rows_affected = sqlx::query!(
                            r#"
                            UPDATE requests SET
                                state = 'claimed',
                                retry_attempt = $2,
                                daemon_id = $3,
                                claimed_at = $4,
                                started_at = NULL,
                                not_before = NULL
                            WHERE id = $1
                              AND state = 'pending'
                            "#,
                            *req.data.id as Uuid,
                            req.state.retry_attempt as i32,
                            *req.state.daemon_id as Uuid,
                            req.state.claimed_at,
                        )
                        .execute(&mut *tx)
                        .await
                        .map_err(|_| FusilladeError::Other(anyhow!("Failed to update request")))?
                        .rows_affected();

                        if rows_affected == 0 {
                            return Self::lost_ownership_or_missing_in_transaction(
                                &mut tx,
                                req.data.id,
                            )
                            .await;
                        }
                    }
                    AnyRequest::Processing(req) => {
                        let rows_affected = sqlx::query!(
                            r#"
                            UPDATE requests SET
                                state = 'processing',
                                retry_attempt = $2,
                                daemon_id = $3,
                                claimed_at = $4,
                                started_at = clock_timestamp()
                            WHERE id = $1
                              AND state = 'claimed'
                              AND daemon_id = $3
                              AND claimed_at = $4
                            "#,
                            *req.data.id as Uuid,
                            req.state.retry_attempt as i32,
                            *req.state.daemon_id as Uuid,
                            req.state.claimed_at,
                        )
                        .execute(&mut *tx)
                        .await
                        .map_err(|_| FusilladeError::Other(anyhow!("Failed to update request")))?
                        .rows_affected();

                        if rows_affected == 0 {
                            return Self::lost_ownership_or_missing_in_transaction(
                                &mut tx,
                                req.data.id,
                            )
                            .await;
                        }

                        sqlx::query!(
                            r#"
                            UPDATE requests
                            SET started_at = clock_timestamp()
                            WHERE id = $1
                            "#,
                            *req.data.id as Uuid,
                        )
                        .execute(&mut *tx)
                        .await
                        .map_err(|_| {
                            FusilladeError::Other(anyhow!("Failed to stamp processing start"))
                        })?;
                    }
                    AnyRequest::Completed(req) => {
                        let response_size = calculate_response_body_size(&req.state.response_body)
                            .ok_or_else(|| {
                                FusilladeError::Other(anyhow!("Response body too large"))
                            })?;

                        let old_state = sqlx::query_scalar!(
                            r#"
                            WITH prev AS (
                                SELECT id, state AS old_state, batch_id
                                FROM requests WHERE id = $1
                                FOR UPDATE
                            ),
                            upd AS (
                                UPDATE requests r SET
                                    state = 'completed',
                                    response_status = $2,
                                    response_body = $3,
                                    claimed_at = $4,
                                    completed_at = $5,
                                    response_size = $6,
                                    routed_model = $7,
                                    canceled_at = NULL
                                FROM prev
                                WHERE r.id = prev.id
                                  AND r.state IN ('claimed', 'processing', 'canceled')
                                  AND r.claimed_at = $4
                                RETURNING prev.old_state, prev.batch_id
                            ),
                            counter_fix AS (
                                UPDATE batches b SET
                                    canceled_requests = b.canceled_requests - 1,
                                    completed_requests = b.completed_requests + 1
                                FROM upd
                                WHERE upd.old_state = 'canceled'
                                  AND b.id = upd.batch_id
                                  AND b.counts_frozen_at IS NOT NULL
                            )
                            SELECT old_state AS "old_state!" FROM upd
                            "#,
                            *req.data.id as Uuid,
                            req.state.response_status as i16,
                            req.state.response_body,
                            req.state.claimed_at,
                            req.state.completed_at,
                            response_size,
                            req.state.routed_model,
                        )
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(|_| FusilladeError::Other(anyhow!("Failed to update request")))?;

                        match old_state {
                            Some(old) if old == "canceled" => {
                                tracing::info!(
                                    "Late completion superseded best-effort cancellation \
                                     (result was billed; frozen counters repaired atomically)"
                                );
                            }
                            Some(_) => {}
                            None => {
                                return Self::terminal_replay_or_lost_ownership_in_transaction(
                                    &mut tx,
                                    req.data.id,
                                )
                                .await;
                            }
                        }
                    }
                    AnyRequest::Failed(req) => {
                        let error_json = serde_json::to_string(&req.state.reason).map_err(|e| {
                            FusilladeError::Other(anyhow!(
                                "Failed to serialize failure reason: {}",
                                e
                            ))
                        })?;

                        let response_size =
                            calculate_error_message_size(&error_json).ok_or_else(|| {
                                FusilladeError::Other(anyhow!("Error message too large"))
                            })?;

                        let old_state = sqlx::query_scalar!(
                            r#"
                            WITH prev AS (
                                SELECT id, state AS old_state, batch_id
                                FROM requests WHERE id = $1
                                FOR UPDATE
                            ),
                            upd AS (
                                UPDATE requests r SET
                                    state = 'failed',
                                    retry_attempt = $2,
                                    error = $3,
                                    failed_at = $4,
                                    response_size = $5,
                                    routed_model = $6,
                                    canceled_at = NULL
                                FROM prev
                                WHERE r.id = prev.id
                                  AND (
                                      ($7::timestamptz IS NOT NULL
                                       AND r.state IN ('claimed', 'processing', 'canceled')
                                       AND r.claimed_at = $7)
                                      OR
                                      ($7::timestamptz IS NULL
                                       AND r.state NOT IN ('completed', 'failed'))
                                  )
                                RETURNING prev.old_state, prev.batch_id
                            ),
                            counter_fix AS (
                                UPDATE batches b SET
                                    canceled_requests = b.canceled_requests - 1,
                                    failed_requests = b.failed_requests + 1
                                FROM upd
                                WHERE upd.old_state = 'canceled'
                                  AND b.id = upd.batch_id
                                  AND b.counts_frozen_at IS NOT NULL
                            )
                            SELECT old_state AS "old_state!" FROM upd
                            "#,
                            *req.data.id as Uuid,
                            req.state.retry_attempt as i32,
                            error_json,
                            req.state.failed_at,
                            response_size,
                            req.state.routed_model,
                            req.state.claimed_at,
                        )
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(|_| FusilladeError::Other(anyhow!("Failed to update request")))?;

                        match old_state {
                            Some(old) if old == "canceled" => {
                                tracing::info!(
                                    "Late failure superseded best-effort cancellation \
                                     (frozen counters repaired atomically)"
                                );
                            }
                            Some(_) => {}
                            None => {
                                if req.state.claimed_at.is_some() {
                                    return Self::terminal_replay_or_lost_ownership_in_transaction(
                                        &mut tx,
                                        req.data.id,
                                    )
                                    .await;
                                }
                                return Self::dropped_or_missing_in_transaction(
                                    &mut tx,
                                    req.data.id,
                                )
                                .await;
                            }
                        }
                    }
                    AnyRequest::Canceled(req) => {
                        let rows_affected = sqlx::query!(
                            r#"
                            UPDATE requests SET
                                state = 'canceled',
                                canceled_at = $2
                            WHERE id = $1
                              AND (
                                  ($3::timestamptz IS NOT NULL
                                   AND state IN ('claimed', 'processing', 'canceled')
                                   AND claimed_at = $3)
                                  OR
                                  ($3::timestamptz IS NULL
                                   AND state NOT IN ('completed', 'failed'))
                              )
                            "#,
                            *req.data.id as Uuid,
                            req.state.canceled_at,
                            req.state.claimed_at,
                        )
                        .execute(&mut *tx)
                        .await
                        .map_err(|_| FusilladeError::Other(anyhow!("Failed to update request")))?
                        .rows_affected();

                        if rows_affected == 0 {
                            if req.state.claimed_at.is_some() {
                                return Self::lost_ownership_or_missing_in_transaction(
                                    &mut tx,
                                    req.data.id,
                                )
                                .await;
                            }
                            return Self::dropped_or_missing_in_transaction(&mut tx, req.data.id)
                                .await;
                        }
                    }
                }

                tx.commit().await.map_err(|_| {
                    FusilladeError::Other(anyhow!("Failed to finish request state transition"))
                })?;
                Ok(None)
            }
            .await;
            drop(state_write_permit);

            match result {
                Ok(val) => return Ok(val),
                Err(FusilladeError::RequestNotFound(id)) => {
                    return Err(FusilladeError::RequestNotFound(id));
                }
                Err(FusilladeError::LostOwnership(id)) => {
                    return Err(FusilladeError::LostOwnership(id));
                }
                Err(error)
                    if RetainedResponseWriteError::from_fusillade_error(&error).is_some() =>
                {
                    return Err(error);
                }
                Err(_) if attempt < MAX_ATTEMPTS - 1 => {
                    tracing::warn!(
                        persist_attempt = attempt + 1,
                        error_class = "database",
                        "Failed to persist request state, retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(100 * 2u64.pow(attempt)))
                        .await;
                }
                Err(e) => return Err(e),
            }
        }

        Err(FusilladeError::Other(anyhow!(
            "Failed to persist request state after {} attempts",
            MAX_ATTEMPTS
        )))
    }

    async fn reschedule_for_retry(
        &self,
        request_id: RequestId,
        owner: DaemonId,
        claimed_at: DateTime<Utc>,
        retry_attempt: u32,
        not_before: Option<DateTime<Utc>>,
    ) -> Result<bool> {
        let _state_write_permit = self.state_write_limiter.acquire("retry").await?;
        let mut tx = self.begin_response_write(&[request_id.0]).await?;

        let rows_affected = sqlx::query!(
            r#"
            UPDATE requests SET
                state = 'pending',
                retry_attempt = $3,
                not_before = $4,
                daemon_id = NULL,
                claimed_at = NULL,
                started_at = NULL
            WHERE id = $1
              AND state IN ('claimed', 'processing')
              AND daemon_id = $2
              AND claimed_at = $5
            "#,
            *request_id as Uuid,
            *owner as Uuid,
            retry_attempt as i32,
            not_before,
            claimed_at,
        )
        .execute(&mut *tx)
        .await
        .map_err(|_| FusilladeError::Other(anyhow!("Failed to reschedule request for retry")))?
        .rows_affected();
        let outcome = if rows_affected > 0 {
            Ok(true)
        } else {
            match retained_response::classify_response_write(&mut tx, &[request_id.0]).await? {
                Some(retained_response::ResponseWriteDisposition::AlreadyRetained) => Ok(false),
                Some(disposition) => Err(disposition.into_fusillade_error()),
                None => Ok(false),
            }
        };
        tx.commit()
            .await
            .map_err(|_| FusilladeError::Other(anyhow!("Failed to finish request retry")))?;
        outcome
    }

    #[tracing::instrument(level = "debug", skip(self, ids), fields(count = ids.len()))]
    async fn get_requests(&self, ids: Vec<RequestId>) -> Result<Vec<Result<AnyRequest>>> {
        let uuid_ids: Vec<Uuid> = ids.iter().map(|id| **id).collect();

        let rows = sqlx::query!(
            r#"
            SELECT
                r.id as "id!", r.batch_id as "batch_id!", r.template_id as "template_id?", r.state as "state!",
                t.custom_id as "custom_id?", t.endpoint as "endpoint?", t.method as "method?",
                t.path as "path?", t.body as "body?", t.model as "model?",
                r.retry_attempt as "retry_attempt!", r.not_before, r.daemon_id, r.claimed_at, r.started_at,
                r.response_status, r.response_body, r.completed_at, r.error, r.failed_at, r.canceled_at,
                b.expires_at as batch_expires_at, r.routed_model
            FROM (
                -- Live-first union: a row lives in exactly one table, so the
                -- union yields each id at most once. The archive arm has no
                -- bucket to prune by (ids arrive without batch context) and
                -- plans as an Append of cheap per-partition index probes —
                -- fine for this admin/detail-shaped path.
                SELECT id, batch_id, template_id, state, retry_attempt, not_before, daemon_id,
                       claimed_at, started_at, response_status, response_body, completed_at,
                       error, failed_at, canceled_at, routed_model
                FROM requests WHERE id = ANY($1)
                UNION ALL
                SELECT id, batch_id, template_id, state, retry_attempt, not_before, daemon_id,
                       claimed_at, started_at, response_status, response_body, completed_at,
                       error, failed_at, canceled_at, routed_model
                FROM batch_requests_archive WHERE id = ANY($1)
            ) r
            JOIN batches b ON r.batch_id = b.id
            -- Per-row primary-key lookup; see the claim paths for why a plain
            -- join on the two-generation view is avoided.
            LEFT JOIN LATERAL (
                SELECT * FROM request_templates_all t
                WHERE t.id = r.template_id AND t.file_id IS NOT DISTINCT FROM b.file_id
                LIMIT 1
            ) t ON TRUE
            "#,
            &uuid_ids,
        )
        .fetch_all(self.read_executor())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch requests: {}", e)))?;

        let mut request_map: std::collections::HashMap<RequestId, Result<AnyRequest>> =
            std::collections::HashMap::new();

        for row in rows {
            let request_id = RequestId(row.id);

            let data = match (
                row.template_id,
                row.endpoint,
                row.method,
                row.path,
                row.body,
                row.model,
            ) {
                (
                    Some(template_id),
                    Some(endpoint),
                    Some(method),
                    Some(path),
                    Some(body),
                    Some(model),
                ) => RequestData {
                    id: request_id,
                    batch_id: Some(BatchId(row.batch_id)),
                    template_id: TemplateId(template_id),
                    custom_id: row.custom_id,
                    endpoint,
                    method,
                    path,
                    body,
                    model,
                    api_key: String::new(),
                    created_by: String::new(),
                    batch_metadata: std::collections::HashMap::new(),
                },
                _ => {
                    request_map.insert(
                        request_id,
                        Err(FusilladeError::Other(anyhow!(
                            "Request template has been deleted"
                        ))),
                    );
                    continue;
                }
            };

            let state = &row.state;

            let any_request = match state.as_str() {
                "pending" => Ok(AnyRequest::Pending(Request {
                    state: Pending {
                        retry_attempt: row.retry_attempt as u32,
                        not_before: row.not_before,
                        batch_expires_at: row.batch_expires_at,
                    },
                    data,
                })),
                "claimed" => Ok(AnyRequest::Claimed(Request {
                    state: Claimed {
                        daemon_id: DaemonId(row.daemon_id.ok_or_else(|| {
                            FusilladeError::Other(anyhow!("Missing daemon_id for claimed request"))
                        })?),
                        claimed_at: row.claimed_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!("Missing claimed_at for claimed request"))
                        })?,
                        retry_attempt: row.retry_attempt as u32,
                        batch_expires_at: row.batch_expires_at,
                        leak: None,
                    },
                    data,
                })),
                "processing" => {
                    let (_tx, rx) = tokio::sync::mpsc::channel(1);
                    let abort_handle = tokio::spawn(async {}).abort_handle();
                    Ok(AnyRequest::Processing(Request {
                        state: Processing {
                            daemon_id: DaemonId(row.daemon_id.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing daemon_id for processing request"
                                ))
                            })?),
                            claimed_at: row.claimed_at.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing claimed_at for processing request"
                                ))
                            })?,
                            started_at: row.started_at.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing started_at for processing request"
                                ))
                            })?,
                            retry_attempt: row.retry_attempt as u32,
                            batch_expires_at: row.batch_expires_at,
                            result_rx: Arc::new(Mutex::new(rx)),
                            abort_handle,
                        },
                        data,
                    }))
                }
                "completed" => Ok(AnyRequest::Completed(Request {
                    state: Completed {
                        response_status: row.response_status.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing response_status for completed request"
                            ))
                        })? as u16,
                        response_body: row.response_body.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing response_body for completed request"
                            ))
                        })?,
                        claimed_at: row.claimed_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing claimed_at for completed request"
                            ))
                        })?,
                        started_at: row.started_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing started_at for completed request"
                            ))
                        })?,
                        completed_at: row.completed_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing completed_at for completed request"
                            ))
                        })?,
                        routed_model: row.routed_model.unwrap_or_else(|| data.model.clone()),
                    },
                    data,
                })),
                "failed" => {
                    let error_json = row.error.ok_or_else(|| {
                        FusilladeError::Other(anyhow!("Missing error for failed request"))
                    })?;

                    let reason: FailureReason =
                        serde_json::from_str(&error_json).unwrap_or_else(|_| {
                            FailureReason::NetworkError {
                                error: error_json.clone(),
                            }
                        });

                    Ok(AnyRequest::Failed(Request {
                        state: Failed {
                            reason,
                            failed_at: row.failed_at.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing failed_at for failed request"
                                ))
                            })?,
                            claimed_at: row.claimed_at,
                            retry_attempt: row.retry_attempt as u32,
                            batch_expires_at: row.batch_expires_at,
                            routed_model: row.routed_model.unwrap_or_else(|| data.model.clone()),
                        },
                        data,
                    }))
                }
                "canceled" => Ok(AnyRequest::Canceled(Request {
                    state: Canceled {
                        canceled_at: row.canceled_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing canceled_at for canceled request"
                            ))
                        })?,
                        claimed_at: row.claimed_at,
                    },
                    data,
                })),
                _ => Err(FusilladeError::Other(anyhow!("Unknown state: {}", state))),
            };

            request_map.insert(request_id, any_request);
        }

        Ok(ids
            .into_iter()
            .map(|id| {
                request_map
                    .remove(&id)
                    .unwrap_or_else(|| Err(FusilladeError::RequestNotFound(id)))
            })
            .collect())
    }


    #[tracing::instrument(level = "debug", skip(self, templates), fields(name = %name, template_count = templates.len()))]
    async fn create_file(
        &self,
        name: String,
        description: Option<String>,
        templates: Vec<RequestTemplateInput>,
    ) -> Result<FileId> {
        use futures::stream;

        let mut items = vec![FileStreamItem::Metadata(FileMetadata {
            filename: Some(name),
            description,
            ..Default::default()
        })];

        for template in templates {
            items.push(FileStreamItem::Template(template));
        }

        let stream = stream::iter(items);
        match self.create_file_stream(stream).await? {
            FileStreamResult::Success(file_id) => Ok(file_id),
            FileStreamResult::Aborted => Err(FusilladeError::Other(anyhow!(
                "create_file produced an aborted stream result for an internally constructed stream"
            ))),
        }
    }

    #[tracing::instrument(level = "debug", skip(self, stream))]
    async fn create_file_stream<S: Stream<Item = FileStreamItem> + Send + Unpin>(
        &self,
        mut stream: S,
    ) -> Result<FileStreamResult> {
        use futures::StreamExt;

        let mut tx = self
            .begin_write()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to begin transaction: {}", e)))?;

        template_store::ensure_current_week_partition(&mut tx).await?;

        let mut metadata = FileMetadata::default();
        let mut file_id: Option<Uuid> = None;
        let mut template_count = 0;

        let batch_size = match self.batch_insert_strategy {
            BatchInsertStrategy::Batched { batch_size } => batch_size,
        };

        let mut template_buffer = Vec::with_capacity(batch_size);

        while let Some(item) = stream.next().await {
            match item {
                FileStreamItem::Metadata(meta) => {
                    if meta.filename.is_some() {
                        metadata.filename = meta.filename;
                    }
                    if meta.description.is_some() {
                        metadata.description = meta.description;
                    }
                    if meta.purpose.is_some() {
                        metadata.purpose = meta.purpose;
                    }
                    if meta.expires_after_anchor.is_some() {
                        metadata.expires_after_anchor = meta.expires_after_anchor;
                    }
                    if meta.expires_after_seconds.is_some() {
                        metadata.expires_after_seconds = meta.expires_after_seconds;
                    }
                    if meta.size_bytes.is_some() {
                        metadata.size_bytes = meta.size_bytes;
                    }
                    if meta.uploaded_by.is_some() {
                        metadata.uploaded_by = meta.uploaded_by;
                    }
                    if meta.api_key_id.is_some() {
                        metadata.api_key_id = meta.api_key_id;
                    }
                    if meta.source_connection_id.is_some() {
                        metadata.source_connection_id = meta.source_connection_id;
                    }
                    if meta.source_external_key.is_some() {
                        metadata.source_external_key = meta.source_external_key;
                    }
                }
                FileStreamItem::Template(template) => {
                    if file_id.is_none() {
                        let new_id = Uuid::new_v4();
                        let stub_name = metadata
                            .filename
                            .clone()
                            .unwrap_or_else(|| format!("upload-{}", new_id));
                        let status = crate::batch::FileStatus::Processed.to_string();

                        sqlx::query!(
                            r#"
                            INSERT INTO files (id, name, status, created_at, updated_at)
                            VALUES ($1, $2, $3, NOW(), NOW())
                            "#,
                            new_id,
                            stub_name,
                            status,
                        )
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| {
                            FusilladeError::Other(anyhow!("Failed to create file stub: {}", e))
                        })?;

                        file_id = Some(new_id);
                    }

                    template_buffer.push((template, template_count));
                    template_count += 1;

                    if template_buffer.len() >= batch_size {
                        Self::insert_template_batch(&mut tx, file_id.unwrap(), &template_buffer)
                            .await?;
                        template_buffer.clear();
                    }
                }
                FileStreamItem::Abort => {
                    tx.rollback().await.map_err(|e| {
                        FusilladeError::Other(anyhow!(
                            "Failed to roll back aborted file stream transaction: {}",
                            e
                        ))
                    })?;
                    return Ok(FileStreamResult::Aborted);
                }
                #[allow(deprecated)]
                FileStreamItem::Error(err) => {
                    tracing::warn!("FileStreamItem::Error is deprecated; use Abort instead");
                    return Err(FusilladeError::ValidationError(err));
                }
            }
        }

        if !template_buffer.is_empty() {
            Self::insert_template_batch(&mut tx, file_id.unwrap(), &template_buffer).await?;
        }

        let fid = if let Some(id) = file_id {
            id
        } else {
            let new_id = Uuid::new_v4();
            let stub_name = metadata
                .filename
                .clone()
                .unwrap_or_else(|| format!("upload-{}", new_id));
            let status = crate::batch::FileStatus::Processed.to_string();

            sqlx::query!(
                r#"
                INSERT INTO files (id, name, status, created_at, updated_at)
                VALUES ($1, $2, $3, NOW(), NOW())
                "#,
                new_id,
                stub_name,
                status,
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to create file: {}", e)))?;

            new_id
        };

        let size_bytes = metadata.size_bytes.unwrap_or(0);
        let status = crate::batch::FileStatus::Processed.to_string();
        let purpose = metadata.purpose.clone();

        let expires_at = if let Some(seconds) = metadata.expires_after_seconds {
            if seconds < 1 || seconds > self.file_expiry.max.num_seconds() {
                return Err(FusilladeError::ValidationError(format!(
                    "expires_after[seconds] must be between 1 and {}",
                    self.file_expiry.max.num_seconds()
                )));
            }
            let anchor = if let Some(anchor_str) = metadata.expires_after_anchor.as_ref() {
                match anchor_str.as_str() {
                    "created_at" => Utc::now(),
                    _ => {
                        tracing::warn!(
                            anchor = anchor_str,
                            "Unknown expires_after_anchor value, defaulting to 'created_at'"
                        );
                        Utc::now()
                    }
                }
            } else {
                Utc::now()
            };

            anchor.checked_add_signed(chrono::Duration::seconds(seconds))
        } else {
            Utc::now().checked_add_signed(self.file_expiry.default)
        };

        let description = metadata.description.clone();
        let uploaded_by = metadata.uploaded_by.clone();
        let name = metadata.filename.clone();

        sqlx::query!(
            r#"
            UPDATE files
            SET name = COALESCE($2, name),
                description = $3,
                size_bytes = $4,
                status = $5,
                purpose = $6,
                expires_at = $7,
                uploaded_by = $8,
                api_key_id = $9,
                source_connection_id = COALESCE($10, source_connection_id),
                source_external_key = COALESCE($11, source_external_key),
                size_finalized = TRUE,
                updated_at = NOW()
            WHERE id = $1
            "#,
            fid,
            name,
            description,
            size_bytes,
            status,
            purpose,
            expires_at,
            uploaded_by,
            metadata.api_key_id,
            metadata.source_connection_id,
            metadata.source_external_key,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to update file metadata: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to commit transaction: {}", e)))?;

        tracing::debug!(
            file_id = %fid,
            template_count = template_count,
            strategy = ?self.batch_insert_strategy,
            "Successfully created file with templates"
        );

        Ok(FileStreamResult::Success(FileId(fid)))
    }

    #[tracing::instrument(level = "debug", skip(self), fields(file_id = %file_id))]
    async fn get_file(&self, file_id: FileId) -> Result<File> {
        self.retry_db_operation(|| async {
            let mut file = self
                .get_file_from_pool(file_id, self.read_executor())
                .await?;

            self.derive_expiry(&mut file);
            self.check_and_mark_expired(&mut file).await?;

            self.maybe_finalize_file_size(&mut file).await?;

            Ok(file)
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self), fields(file_id = %file_id))]
    async fn get_file_from_primary_pool(&self, file_id: FileId) -> Result<File> {
        self.retry_db_operation(|| async {
            let mut file = self
                .get_file_from_pool(file_id, self.write_executor())
                .await?;

            self.derive_expiry(&mut file);
            self.check_and_mark_expired(&mut file).await?;

            self.maybe_finalize_file_size(&mut file).await?;

            Ok(file)
        })
        .await
    }

    async fn get_file_content(&self, file_id: FileId) -> Result<Vec<FileContentItem>> {
        let mut stream = self.get_file_content_stream(file_id, 0, None);
        let mut items = Vec::new();

        while let Some(result) = stream.next().await {
            items.push(result?);
        }

        Ok(items)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(file_id = %file_id))]
    async fn get_file_template_stats(
        &self,
        file_id: FileId,
    ) -> Result<Vec<crate::batch::ModelTemplateStats>> {
        let stats = sqlx::query!(
            r#"
            SELECT
                model AS "model!",
                COUNT(*)::BIGINT as "request_count!",
                SUM(body_byte_size)::BIGINT as "total_body_bytes!"
            FROM request_templates_all
            WHERE file_id = $1
            GROUP BY model
            ORDER BY model
            "#,
            *file_id as Uuid,
        )
        .fetch_all(self.read_executor())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch template stats: {}", e)))?;

        Ok(stats
            .into_iter()
            .map(|row| crate::batch::ModelTemplateStats {
                model: row.model,
                request_count: row.request_count,
                total_body_bytes: row.total_body_bytes,
            })
            .collect())
    }

    #[tracing::instrument(level = "debug", skip(self), fields(file_id = %file_id, search = ?search))]
    fn get_file_content_stream(
        &self,
        file_id: FileId,
        offset: usize,
        search: Option<String>,
    ) -> Pin<Box<dyn Stream<Item = Result<FileContentItem>> + Send>> {
        let pools = self.pools.clone();
        let retry_config = self.db_retry_config.clone();
        let query_schema = self.query_schema.clone();
        let (tx, rx) = mpsc::channel(self.download_buffer_size);
        let offset = offset as i64;

        tokio::spawn(async move {
            let file_result = sqlx::query!(
                r#"
                SELECT purpose
                FROM files
                WHERE id = $1 AND deleted_at IS NULL
                "#,
                *file_id as Uuid,
            )
            .fetch_one(
                crate::db::RetryingPgPool::new(&pools.read(), &retry_config)
                    .with_schema(query_schema.clone()),
            )
            .await;

            let purpose = match file_result {
                Ok(row) => row.purpose,
                Err(e) => {
                    let _ = tx
                        .send(Err(FusilladeError::Other(anyhow!(
                            "Failed to fetch file: {}",
                            e
                        ))))
                        .await;
                    return;
                }
            };

            match purpose.as_deref() {
                Some("batch_output") => {
                    Self::stream_batch_output(
                        pools,
                        retry_config,
                        query_schema,
                        file_id,
                        offset,
                        search,
                        tx,
                    )
                    .await;
                }
                Some("batch_error") => {
                    Self::stream_batch_error(
                        pools,
                        retry_config,
                        query_schema,
                        file_id,
                        offset,
                        search,
                        tx,
                    )
                    .await;
                }
                _ => {
                    Self::stream_request_templates(
                        pools,
                        retry_config,
                        query_schema,
                        file_id,
                        offset,
                        search,
                        tx,
                    )
                    .await;
                }
            }
        });

        Box::pin(ReceiverStream::new(rx))
    }

    #[tracing::instrument(level = "debug", skip(self, filter), fields(uploaded_by = ?filter.uploaded_by, status = ?filter.status, purpose = ?filter.purpose, after = ?filter.after, limit = ?filter.limit))]
    async fn list_files(&self, filter: crate::batch::FileFilter) -> Result<Vec<File>> {
        use sqlx::QueryBuilder;

        let after_created_at = if let Some(after_id) = &filter.after {
            sqlx::query!(
                r#"SELECT created_at FROM files WHERE id = $1"#,
                **after_id as Uuid
            )
            .fetch_optional(self.read_executor())
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch after cursor: {}", e)))?
            .map(|row| row.created_at)
        } else {
            None
        };

        let mut query_builder = QueryBuilder::new(
            r#"
            SELECT
                f.id, f.name, f.description, f.size_bytes, f.size_finalized,
                f.status, f.error_message, f.purpose, f.expires_at, f.deleted_at,
                f.uploaded_by, f.created_at, f.updated_at, f.api_key_id,
                f.source_connection_id, f.source_external_key,
                b.id as batch_id,
                b.total_requests,
                COALESCE(counts.completed, 0)::BIGINT as completed_requests,
                COALESCE(counts.failed, 0)::BIGINT as failed_requests,
                COALESCE(counts.canceled, 0)::BIGINT as canceled_requests,
                COALESCE(counts.in_progress, 0)::BIGINT as in_progress_requests,
                size_calc.calculated_size
            FROM files f
            LEFT JOIN batches b ON (
                (f.purpose = 'batch_output' AND b.output_file_id = f.id) OR
                (f.purpose = 'batch_error' AND b.error_file_id = f.id)
            )
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE r.state = 'completed') as completed,
                    COUNT(*) FILTER (WHERE r.state = 'failed') as failed,
                    COUNT(*) FILTER (WHERE r.state = 'canceled' OR (r.state IN ('pending', 'claimed', 'processing') AND b.cancelling_at IS NOT NULL)) as canceled,
                    COUNT(*) FILTER (WHERE r.state IN ('claimed', 'processing') AND b.cancelling_at IS NULL) as in_progress
                FROM requests r
                WHERE r.batch_id = b.id
            ) counts ON (f.purpose IN ('batch_output', 'batch_error'))
            LEFT JOIN LATERAL (
                SELECT SUM(r.response_size)::BIGINT as calculated_size
                FROM requests r
                WHERE r.batch_id = b.id
                AND ((f.purpose = 'batch_output' AND r.state = 'completed') OR
                    (f.purpose = 'batch_error' AND r.state = 'failed'))
            ) size_calc ON (f.purpose IN ('batch_output', 'batch_error') AND f.size_finalized = FALSE AND b.id IS NOT NULL)
            "#,
        );

        query_builder.push(" WHERE f.deleted_at IS NULL");

        if let Some(uploaded_by) = &filter.uploaded_by {
            query_builder.push(" AND f.uploaded_by = ");
            query_builder.push_bind(uploaded_by);
        }

        if let Some(status) = &filter.status {
            query_builder.push(" AND f.status = ");
            query_builder.push_bind(status);
        }

        if let Some(purpose) = &filter.purpose {
            query_builder.push(" AND f.purpose = ");
            query_builder.push_bind(purpose);
        }

        if let Some(search) = &filter.search {
            let search_pattern = format!("%{}%", search.to_lowercase());
            query_builder.push(" AND LOWER(f.name) LIKE ");
            query_builder.push_bind(search_pattern);
        }

        if let Some(api_key_ids) = &filter.api_key_ids {
            query_builder.push(" AND f.api_key_id = ANY(");
            query_builder.push_bind(api_key_ids.as_slice());
            query_builder.push(")");
        }

        if let (Some(after_id), Some(after_ts)) = (&filter.after, after_created_at) {
            let comparison = if filter.ascending { ">" } else { "<" };

            query_builder.push(" AND (f.created_at ");
            query_builder.push(comparison);
            query_builder.push(" ");
            query_builder.push_bind(after_ts);
            query_builder.push(" OR (f.created_at = ");
            query_builder.push_bind(after_ts);
            query_builder.push(" AND f.id ");
            query_builder.push(comparison);
            query_builder.push(" ");
            query_builder.push_bind(**after_id as Uuid);
            query_builder.push("))");
        }

        let order_direction = if filter.ascending { "ASC" } else { "DESC" };
        query_builder.push(" ORDER BY f.created_at ");
        query_builder.push(order_direction);
        query_builder.push(", f.id ");
        query_builder.push(order_direction);

        if let Some(limit) = filter.limit {
            query_builder.push(" LIMIT ");
            query_builder.push_bind(limit as i64);
        }

        let rows = query_builder
            .build()
            .fetch_all(self.read_executor())
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to list files: {}", e)))?;

        let mut files = Vec::new();

        for row in rows {
            let id: Uuid = row
                .try_get("id")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read id: {}", e)))?;
            let name: String = row
                .try_get("name")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read name: {}", e)))?;
            let description: Option<String> = row
                .try_get("description")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read description: {}", e)))?;
            let mut size_bytes: i64 = row
                .try_get("size_bytes")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read size_bytes: {}", e)))?;
            let size_finalized: bool = row.try_get("size_finalized").map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to read size_finalized: {}", e))
            })?;
            let status_str: String = row
                .try_get("status")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read status: {}", e)))?;
            let status = status_str
                .parse::<crate::batch::FileStatus>()
                .map_err(|e| {
                    FusilladeError::Other(anyhow!("Invalid file status '{}': {}", status_str, e))
                })?;
            let error_message: Option<String> = row.try_get("error_message").map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to read error_message: {}", e))
            })?;
            let purpose_str: Option<String> = row
                .try_get("purpose")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read purpose: {}", e)))?;
            let purpose = purpose_str
                .map(|s| s.parse::<crate::batch::Purpose>())
                .transpose()
                .map_err(|e| FusilladeError::Other(anyhow!("Invalid purpose: {}", e)))?;
            let expires_at: Option<chrono::DateTime<Utc>> = row
                .try_get("expires_at")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read expires_at: {}", e)))?;
            let deleted_at: Option<chrono::DateTime<Utc>> = row
                .try_get("deleted_at")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read deleted_at: {}", e)))?;
            let uploaded_by: Option<String> = row
                .try_get("uploaded_by")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read uploaded_by: {}", e)))?;
            let created_at: chrono::DateTime<Utc> = row
                .try_get("created_at")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read created_at: {}", e)))?;
            let updated_at: chrono::DateTime<Utc> = row
                .try_get("updated_at")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read updated_at: {}", e)))?;
            let api_key_id: Option<Uuid> = row
                .try_get("api_key_id")
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to read api_key_id: {}", e)))?;
            let source_connection_id: Option<Uuid> =
                row.try_get("source_connection_id").map_err(|e| {
                    FusilladeError::Other(anyhow!("Failed to read source_connection_id: {}", e))
                })?;
            let source_external_key: Option<String> =
                row.try_get("source_external_key").map_err(|e| {
                    FusilladeError::Other(anyhow!("Failed to read source_external_key: {}", e))
                })?;

            if let Some(estimated_size) =
                self.calculate_virtual_file_size_from_row(&row, &purpose, size_finalized)?
            {
                size_bytes = estimated_size;

                let file_id = FileId(id);
                self.spawn_finalize_if_complete(&row, file_id, estimated_size);
            }

            let mut file = File {
                id: FileId(id),
                name,
                description,
                size_bytes,
                size_finalized,
                status,
                error_message,
                purpose,
                expires_at,
                deleted_at,
                uploaded_by,
                created_at,
                updated_at,
                api_key_id,
                source_connection_id,
                source_external_key,
            };

            self.derive_expiry(&mut file);
            self.check_and_mark_expired(&mut file).await?;

            files.push(file);
        }

        Ok(files)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(file_id = %file_id))]
    async fn delete_file(&self, file_id: FileId) -> Result<()> {
        let mut tx = self
            .begin_write()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to begin transaction: {}", e)))?;

        let exists = sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS(
                SELECT 1 FROM files
                WHERE id = $1 AND deleted_at IS NULL
                FOR UPDATE
            )"#,
        )
        .bind(*file_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to lock file: {}", e)))?;
        if !exists {
            return Err(FusilladeError::FileNotFound(file_id));
        }

        sqlx::query!(
            r#"
            UPDATE batches
            SET cancelling_at = CASE
                    WHEN completed_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL
                        THEN COALESCE(cancelling_at, NOW())
                    ELSE cancelling_at
                END,
                cancelled_at = CASE
                    WHEN completed_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL
                        THEN COALESCE(cancelled_at, NOW())
                    ELSE cancelled_at
                END,
                file_id = NULL
            WHERE file_id = $1
            "#,
            *file_id as Uuid,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to update batches: {}", e)))?;

        sqlx::query!(
            r#"
            UPDATE batches
            SET output_file_id = NULL
            WHERE output_file_id = $1
            "#,
            *file_id as Uuid,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to clear output_file_id reference: {}", e))
        })?;

        sqlx::query!(
            r#"
            UPDATE batches
            SET error_file_id = NULL
            WHERE error_file_id = $1
            "#,
            *file_id as Uuid,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to clear error_file_id reference: {}", e))
        })?;

        let rows_affected = sqlx::query!(
            r#"
            UPDATE files
            SET deleted_at = NOW(),
                status = 'deleted'
            WHERE id = $1
              AND deleted_at IS NULL
            "#,
            *file_id as Uuid,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to soft-delete file: {}", e)))?
        .rows_affected();

        if rows_affected == 0 {
            tx.rollback()
                .await
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to rollback: {}", e)))?;
            return Err(FusilladeError::FileNotFound(file_id));
        }

        tx.commit()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to commit transaction: {}", e)))?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, input), fields(file_id = %input.file_id))]
    async fn create_batch(&self, input: BatchInput) -> Result<Batch> {
        let file_id = input.file_id;
        let batch = self.create_batch_record(input).await?;
        if let Err(e) = self.populate_batch(batch.id, file_id).await {
            let _ = self.mark_batch_failed(batch.id, &e.to_string()).await;
            return Err(e);
        }
        self.get_batch_from_pool(batch.id, self.write_executor())
            .await
    }

    #[tracing::instrument(level = "debug", skip(self, input), fields(file_id = %input.file_id))]
    async fn create_batch_record(&self, input: BatchInput) -> Result<Batch> {
        let now = Utc::now();
        let std_duration = humantime::parse_duration(&input.completion_window).map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Invalid completion_window '{}': {}. Expected format like '24h', '7d', etc.",
                input.completion_window,
                e
            ))
        })?;
        let chrono_duration = chrono::Duration::from_std(std_duration).map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Failed to convert completion_window duration: {}",
                e
            ))
        })?;
        let expires_at = now.checked_add_signed(chrono_duration).ok_or_else(|| {
            FusilladeError::Other(anyhow!(
                "Expiration time overflow when calculating expires_at from completion_window '{}'",
                input.completion_window
            ))
        })?;

        self.insert_batch_record(NewBatchRecord {
            file_id: input.file_id,
            endpoint: input.endpoint,
            service_tier: None,
            completion_window: Some(input.completion_window),
            metadata: input.metadata,
            created_by: input.created_by,
            expires_at: Some(expires_at),
            api_key_id: input.api_key_id,
            total_requests: input.total_requests.unwrap_or(0),
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self, input), fields(file_id = %input.file_id))]
    async fn create_background_batch(&self, input: BackgroundBatchInput) -> Result<Batch> {
        let file_id = input.file_id;
        let batch = self.create_background_batch_record(input).await?;
        if let Err(e) = self.populate_batch(batch.id, file_id).await {
            let _ = self.mark_batch_failed(batch.id, &e.to_string()).await;
            return Err(e);
        }
        self.get_batch_from_pool(batch.id, self.write_executor())
            .await
    }

    #[tracing::instrument(level = "debug", skip(self, input), fields(file_id = %input.file_id))]
    async fn create_background_batch_record(&self, input: BackgroundBatchInput) -> Result<Batch> {
        self.insert_batch_record(NewBatchRecord {
            file_id: input.file_id,
            endpoint: input.endpoint,
            service_tier: Some("background".to_string()),
            completion_window: None,
            metadata: input.metadata,
            created_by: input.created_by,
            expires_at: None,
            api_key_id: input.api_key_id,
            total_requests: input.total_requests.unwrap_or(0),
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self), fields(batch_id = %batch_id))]
    async fn populate_batch(&self, batch_id: BatchId, file_id: FileId) -> Result<()> {
        let populate_started = std::time::Instant::now();
        let mut tx = self
            .begin_write()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to begin transaction: {}", e)))?;

        sqlx::query!(
            "SELECT pg_advisory_xact_lock(hashtextextended('fusillade.populate_batch:' || $1::text, 0))",
            *batch_id as Uuid,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to acquire populate lock: {}", e)))?;

        let row = sqlx::query!(
            r#"
            SELECT
                b.service_tier,
                b.completion_window,
                b.cancelling_at,
                b.requests_started_at,
                b.deleted_at AS batch_deleted_at,
                (SELECT deleted_at FROM files WHERE id = $2) AS file_deleted_at
            FROM batches b
            WHERE b.id = $1
            "#,
            *batch_id as Uuid,
            *file_id as Uuid,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch batch state: {}", e)))?;

        if row.requests_started_at.is_some() {
            tx.commit().await.map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to commit transaction: {}", e))
            })?;
            tracing::info!(
                batch_id = %batch_id,
                "Batch already populated; skipping population (idempotent no-op)"
            );
            return Ok(());
        }

        if row.batch_deleted_at.is_some() {
            tracing::debug!(batch_id = %*batch_id, "Batch deleted before population, skipping");
            return Ok(());
        }
        if row.cancelling_at.is_some() {
            tracing::debug!(batch_id = %*batch_id, "Batch cancelled before population, skipping");
            return Ok(());
        }
        if row.file_deleted_at.is_some() {
            return Err(FusilladeError::ValidationError(
                "source file was deleted before batch population".to_string(),
            ));
        }

        let service_tier = match row.service_tier.as_deref() {
            Some("background") => Some("background"),
            None => {
                let completion_window = row.completion_window.as_deref().ok_or_else(|| {
                    FusilladeError::ValidationError(
                        "SLA batches require a completion_window".to_string(),
                    )
                })?;
                crate::request::query::service_tier_from_completion_window(completion_window)
            }
            Some(other) => {
                return Err(FusilladeError::ValidationError(format!(
                    "Unsupported batch service_tier '{other}'"
                )));
            }
        };

        let rows_affected = sqlx::query!(
            r#"
            INSERT INTO requests (batch_id, template_id, state, custom_id, retry_attempt, model, service_tier)
            SELECT $1, id, 'pending', custom_id, 0, model, $3
            FROM request_templates_all
            WHERE file_id = $2
            "#,
            *batch_id as Uuid,
            *file_id as Uuid,
            service_tier,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to create requests: {}", e)))?
        .rows_affected();

        if rows_affected == 0 {
            return Err(FusilladeError::ValidationError(
                "Cannot populate batch from file with no templates".to_string(),
            ));
        }

        sqlx::query!(
            r#"
            UPDATE batches
            SET total_requests = $2,
                requests_started_at = NOW()
            WHERE id = $1
            "#,
            *batch_id as Uuid,
            rows_affected as i64
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to update batch metadata: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to commit transaction: {}", e)))?;

        metrics::histogram!("fusillade_populate_duration_seconds")
            .record(populate_started.elapsed().as_secs_f64());
        metrics::counter!("fusillade_populate_rows_total").increment(rows_affected);

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self), fields(batch_id = %batch_id))]
    async fn get_batch(&self, batch_id: BatchId) -> Result<Batch> {
        self.retry_db_operation(|| async {
            self.get_batch_from_pool(batch_id, self.read_executor())
                .await
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self), fields(batch_id = %batch_id))]
    async fn get_batch_status(&self, batch_id: BatchId) -> Result<BatchStatus> {
        self.retry_db_operation(|| async {
            let mut query_builder = QueryBuilder::new(
                r#"
            SELECT
                b.id as batch_id,
                b.file_id,
                f.name as file_name,
                b.total_requests,
                b.requests_started_at as started_at,
                b.failed_at,
                b.created_at,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN 0
                     ELSE COALESCE(counts.pending, 0) END::BIGINT as pending_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN 0
                     ELSE COALESCE(counts.in_progress, 0) END::BIGINT as in_progress_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN b.completed_requests
                     ELSE COALESCE(counts.completed, 0) END::BIGINT as completed_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN b.failed_requests
                     ELSE COALESCE(counts.failed, 0) END::BIGINT as failed_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN b.canceled_requests
                     ELSE COALESCE(counts.canceled, 0) END::BIGINT as canceled_requests
            FROM batches b
            LEFT JOIN files f ON f.id = b.file_id
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE state = 'pending' AND b.cancelling_at IS NULL) as pending,
                    COUNT(*) FILTER (WHERE state IN ('claimed', 'processing') AND b.cancelling_at IS NULL) as in_progress,
                    COUNT(*) FILTER (WHERE state = 'completed') as completed,
                    COUNT(*) FILTER (WHERE state = 'failed') as failed,
                    COUNT(*) FILTER (WHERE state = 'canceled' OR (state IN ('pending', 'claimed', 'processing') AND b.cancelling_at IS NOT NULL)) as canceled
                FROM requests
                WHERE batch_id = b.id
                  -- Frozen batches serve persisted counters; one-time filter
                  -- skips the requests scan entirely.
                  AND b.counts_frozen_at IS NULL
            ) counts ON TRUE
            WHERE b.id = "#,
            );
            query_builder.push_bind(*batch_id as Uuid);
            query_builder.push(" AND b.deleted_at IS NULL");

            let row = query_builder
                .build()
                .fetch_optional(self.read_executor())
                .await
                .map_err(|e| {
                    FusilladeError::Other(anyhow!("Failed to fetch batch status: {}", e))
                })?
                .ok_or_else(|| FusilladeError::Other(anyhow!("Batch not found")))?;

            Ok(batch_status_from_dynamic_row!(row))
        })
        .await
    }

    async fn get_batch_by_output_file_id(
        &self,
        file_id: FileId,
        file_type: OutputFileType,
    ) -> Result<Option<Batch>> {
        let mut query_builder = QueryBuilder::new(
            r#"
            SELECT
                b.id, b.file_id, b.endpoint, b.service_tier, b.completion_window, b.metadata,
                b.output_file_id, b.error_file_id, b.created_by, b.created_at,
                b.expires_at, b.cancelling_at, b.errors,
                b.total_requests,
                b.requests_started_at,
                b.finalizing_at,
                b.completed_at,
                b.failed_at,
                b.cancelled_at,
                b.deleted_at,
                b.notification_sent_at,
                b.api_key_id,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN 0
                     ELSE COALESCE(counts.pending, 0) END::BIGINT as pending_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN 0
                     ELSE COALESCE(counts.in_progress, 0) END::BIGINT as in_progress_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN b.completed_requests
                     ELSE COALESCE(counts.completed, 0) END::BIGINT as completed_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN b.failed_requests
                     ELSE COALESCE(counts.failed, 0) END::BIGINT as failed_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN b.canceled_requests
                     ELSE COALESCE(counts.canceled, 0) END::BIGINT as canceled_requests
            FROM batches b
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE state = 'pending' AND b.cancelling_at IS NULL) as pending,
                    COUNT(*) FILTER (WHERE state IN ('claimed', 'processing') AND b.cancelling_at IS NULL) as in_progress,
                    COUNT(*) FILTER (WHERE state = 'completed') as completed,
                    COUNT(*) FILTER (WHERE state = 'failed') as failed,
                    COUNT(*) FILTER (WHERE state = 'canceled' OR (state IN ('pending', 'claimed', 'processing') AND b.cancelling_at IS NOT NULL)) as canceled
                FROM requests
                WHERE batch_id = b.id
                  -- Frozen batches serve persisted counters; one-time filter
                  -- skips the requests scan entirely.
                  AND b.counts_frozen_at IS NULL
            ) counts ON TRUE
            WHERE "#,
        );

        match file_type {
            OutputFileType::Output => {
                query_builder.push("b.output_file_id = ");
                query_builder.push_bind(*file_id as Uuid);
                query_builder.push(" AND b.deleted_at IS NULL");

                let row = query_builder
                    .build()
                    .fetch_optional(self.read_executor())
                    .await
                    .map_err(|e| {
                        FusilladeError::Other(anyhow!("Failed to get batch by output file: {}", e))
                    })?;

                Ok(row.map(|row| batch_from_dynamic_row!(row)))
            }
            OutputFileType::Error => {
                query_builder.push("b.error_file_id = ");
                query_builder.push_bind(*file_id as Uuid);
                query_builder.push(" AND b.deleted_at IS NULL");

                let row = query_builder
                    .build()
                    .fetch_optional(self.read_executor())
                    .await
                    .map_err(|e| {
                        FusilladeError::Other(anyhow!("Failed to get batch by error file: {}", e))
                    })?;

                Ok(row.map(|row| batch_from_dynamic_row!(row)))
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self), fields(created_by = ?filter.created_by, limit = filter.limit))]
    async fn list_batches(&self, filter: ListBatchesFilter) -> Result<Vec<Batch>> {
        let cursor = match filter.after {
            Some(after_id) => sqlx::query!(
                r#"
                SELECT b.created_at,
                       CASE WHEN b.completed_at IS NULL AND b.failed_at IS NULL
                                 AND b.cancelled_at IS NULL AND b.cancelling_at IS NULL
                            THEN 0 ELSE 1 END as "priority!: i32"
                FROM batches b
                WHERE b.id = $1
                "#,
                *after_id as Uuid,
            )
            .fetch_optional(self.read_executor())
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch after batch: {}", e)))?
            .map(|row| batch_list::BatchCursor {
                created_at: row.created_at,
                id: *after_id as Uuid,
                priority: row.priority,
            }),
            None => None,
        };

        let mut query_builder = QueryBuilder::new("");
        batch_list::push_query(&mut query_builder, &filter, cursor)?;

        let mut tx = self.begin_read().await.map_err(anyhow::Error::from)?;
        sqlx::query(&format!(
            "SET LOCAL statement_timeout = '{}'",
            batch_list::PAGE_BUDGET
        ))
        .execute(&mut *tx)
        .await
        .map_err(anyhow::Error::from)?;
        let rows = query_builder
            .build()
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to list batches: {}", e)))?;
        tx.commit().await.map_err(anyhow::Error::from)?;

        Ok(rows
            .into_iter()
            .map(|row| batch_from_dynamic_row!(row))
            .collect())
    }

    async fn list_file_batches(&self, file_id: FileId) -> Result<Vec<BatchStatus>> {
        let mut query_builder = QueryBuilder::new(
            r#"
            SELECT
                b.id as batch_id,
                b.file_id,
                f.name as file_name,
                b.total_requests,
                b.requests_started_at as started_at,
                b.failed_at,
                b.created_at,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN 0
                     ELSE COALESCE(counts.pending, 0) END::BIGINT as pending_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN 0
                     ELSE COALESCE(counts.in_progress, 0) END::BIGINT as in_progress_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN b.completed_requests
                     ELSE COALESCE(counts.completed, 0) END::BIGINT as completed_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN b.failed_requests
                     ELSE COALESCE(counts.failed, 0) END::BIGINT as failed_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN b.canceled_requests
                     ELSE COALESCE(counts.canceled, 0) END::BIGINT as canceled_requests
            FROM batches b
            LEFT JOIN files f ON f.id = b.file_id
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE state = 'pending' AND b.cancelling_at IS NULL) as pending,
                    COUNT(*) FILTER (WHERE state IN ('claimed', 'processing') AND b.cancelling_at IS NULL) as in_progress,
                    COUNT(*) FILTER (WHERE state = 'completed') as completed,
                    COUNT(*) FILTER (WHERE state = 'failed') as failed,
                    COUNT(*) FILTER (WHERE state = 'canceled' OR (state IN ('pending', 'claimed', 'processing') AND b.cancelling_at IS NOT NULL)) as canceled
                FROM requests
                WHERE batch_id = b.id
                  -- Frozen batches serve persisted counters; one-time filter
                  -- skips the requests scan entirely.
                  AND b.counts_frozen_at IS NULL
            ) counts ON TRUE
            WHERE b.file_id = "#,
        );
        query_builder.push_bind(*file_id as Uuid);
        query_builder.push(" AND b.deleted_at IS NULL ORDER BY b.created_at DESC");

        let rows = query_builder
            .build()
            .fetch_all(self.read_executor())
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to list batches: {}", e)))?;

        Ok(rows
            .into_iter()
            .map(|row| batch_status_from_dynamic_row!(row))
            .collect())
    }

    async fn get_cancelled_batch_ids(&self, batch_ids: &[BatchId]) -> Result<Vec<BatchId>> {
        if batch_ids.is_empty() {
            return Ok(Vec::new());
        }

        let uuids: Vec<Uuid> = batch_ids.iter().map(|id| **id).collect();

        let rows = sqlx::query_file_scalar!("src/postgres/sql/get_cancelled_batch_ids.sql", &uuids)
            .fetch_all(self.read_executor())
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to fetch cancelled batch IDs: {}", e))
            })?;

        Ok(rows.into_iter().map(BatchId::from).collect())
    }

    #[tracing::instrument(level = "debug", skip(self), fields(batch_id = %batch_id))]
    async fn cancel_batch(&self, batch_id: BatchId) -> Result<()> {
        let now = Utc::now();

        sqlx::query!(
            r#"
            UPDATE batches
            SET cancelling_at = $2,
                cancelled_at = $2
            WHERE id = $1 AND cancelling_at IS NULL
            "#,
            *batch_id as Uuid,
            now,
        )
        .execute(self.write_executor())
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to set cancellation timestamps: {}", e))
        })?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self), fields(batch_id = %batch_id, target_state = target_state.as_str()))]
    async fn cascade_batch_state_to_requests(
        &self,
        batch_id: BatchId,
        target_state: CascadeTargetState,
    ) -> Result<u64> {
        let rows_affected = match target_state {
            CascadeTargetState::Canceled => {
                sqlx::query!(
                    r#"
                    UPDATE requests
                    SET state = 'canceled',
                        canceled_at = COALESCE(canceled_at, NOW())
                    WHERE batch_id = $1
                      AND state IN ('pending', 'claimed', 'processing')
                    "#,
                    *batch_id as Uuid,
                )
                .execute(self.write_executor())
                .await
            }
            CascadeTargetState::Failed => {
                let default_error = serde_json::to_string(&FailureReason::BatchTerminated)
                    .expect("FailureReason serialization cannot fail");
                sqlx::query!(
                    r#"
                    UPDATE requests
                    SET state = 'failed',
                        failed_at = COALESCE(failed_at, NOW()),
                        error = COALESCE(error, $2),
                        response_size = CASE
                            WHEN COALESCE(response_size, 0) = 0
                                THEN octet_length(COALESCE(error, $2))
                            ELSE response_size
                        END
                    WHERE batch_id = $1
                      AND state IN ('pending', 'claimed', 'processing')
                    "#,
                    *batch_id as Uuid,
                    default_error,
                )
                .execute(self.write_executor())
                .await
            }
        }
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to cascade batch state to requests: {}", e))
        })?;

        tracing::info!(
            rows_updated = rows_affected.rows_affected(),
            "Cascaded batch state to requests"
        );

        Ok(rows_affected.rows_affected())
    }

    async fn delete_batch(&self, batch_id: BatchId) -> Result<()> {
        let mut tx = self
            .begin_write()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to begin transaction: {}", e)))?;

        let generated_files = sqlx::query!(
            r#"
            SELECT output_file_id, error_file_id
            FROM batches
            WHERE id = $1
              AND deleted_at IS NULL
            "#,
            *batch_id as Uuid,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch batch for deletion: {}", e)))?
        .ok_or_else(|| FusilladeError::Other(anyhow!("Batch not found")))?;

        let mut generated_file_ids = [
            generated_files.output_file_id,
            generated_files.error_file_id,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        generated_file_ids.sort_unstable();
        generated_file_ids.dedup();

        if !generated_file_ids.is_empty() {
            sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT id
                FROM files
                WHERE id = ANY($1)
                ORDER BY id
                FOR UPDATE
                "#,
            )
            .bind(&generated_file_ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to lock batch result files: {}", e))
            })?;
        }

        let rows_affected = sqlx::query!(
            r#"
            UPDATE batches
            SET deleted_at = NOW(),
                cancelling_at = CASE
                    WHEN completed_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL
                        THEN COALESCE(cancelling_at, NOW())
                    ELSE cancelling_at
                END,
                cancelled_at = CASE
                    WHEN completed_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL
                        THEN COALESCE(cancelled_at, NOW())
                    ELSE cancelled_at
                END
            WHERE id = $1
              AND deleted_at IS NULL
            "#,
            *batch_id as Uuid,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to soft-delete batch: {}", e)))?
        .rows_affected();

        if rows_affected == 0 {
            return Err(FusilladeError::Other(anyhow!("Batch not found")));
        }

        if !generated_file_ids.is_empty() {
            sqlx::query!(
                r#"
                UPDATE files
                SET deleted_at = NOW(),
                    status = 'deleted'
                WHERE id = ANY($1)
                  AND deleted_at IS NULL
                "#,
                &generated_file_ids,
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to soft-delete batch result files: {}", e))
            })?;
        }

        tx.commit().await.map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to commit batch deletion: {}", e))
        })?;

        Ok(())
    }

    async fn delete_request(&self, request_id: RequestId) -> Result<()> {
        let mut tx = self
            .begin_write()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to begin transaction: {}", e)))?;

        let template_id: Option<Option<Uuid>> = sqlx::query_scalar!(
            r#"DELETE FROM requests WHERE id = $1 RETURNING template_id"#,
            *request_id as Uuid,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to delete request: {}", e)))?;

        let Some(template_id) = template_id else {
            return Err(FusilladeError::RequestNotFound(request_id));
        };

        if let Some(template_id) = template_id {
            template_store::delete_dedicated_templates(
                &mut tx,
                &[template_id],
                template_store::DedicatedDeleteScope::Any,
            )
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to delete request_template: {}", e))
            })?;
        }

        tx.commit()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to commit transaction: {}", e)))?;

        Ok(())
    }

    async fn delete_response_group(&self, response_id: Uuid) -> Result<u64> {
        retained_response::delete_response_group(self, response_id).await
    }

    async fn delete_owned_response_group(
        &self,
        response_id: Uuid,
        creator_id: &str,
    ) -> Result<u64> {
        retained_response::delete_owned_response_group(self, response_id, creator_id).await
    }

    async fn bulk_delete_data(&self, creator_id: &str, batch_size: i64) -> Result<u64> {
        if batch_size < 1 {
            return Ok(0);
        }

        let mut tx = self
            .begin_write()
            .await
            .map_err(|_| FusilladeError::Other(anyhow!("Failed to begin creator erasure")))?;

        let file_ids: Vec<Uuid> = sqlx::query_scalar(
            r#"
            SELECT id FROM files
            WHERE uploaded_by = $1
              AND deleted_at IS NULL
            ORDER BY id
            LIMIT $2
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(creator_id)
        .bind(batch_size)
        .fetch_all(&mut *tx)
        .await
        .map_err(|_| FusilladeError::Other(anyhow!("Failed to lock creator files")))?;

        let batchless_requests = retained_response::delete_creator_response_groups(
            &mut tx,
            creator_id,
            batch_size,
            self.retained_response_fence_seconds,
        )
        .await?;

        let batches_affected = sqlx::query_scalar!(
            r#"
            WITH to_delete AS (
                SELECT id FROM batches
                WHERE created_by = $1
                  AND deleted_at IS NULL
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            UPDATE batches b
            SET deleted_at = NOW(),
                metadata = NULL,
                cancelling_at = CASE
                    WHEN b.completed_at IS NULL AND b.failed_at IS NULL AND b.cancelled_at IS NULL
                        THEN COALESCE(b.cancelling_at, NOW())
                    ELSE b.cancelling_at
                END,
                cancelled_at = CASE
                    WHEN b.completed_at IS NULL AND b.failed_at IS NULL AND b.cancelled_at IS NULL
                        THEN COALESCE(b.cancelled_at, NOW())
                    ELSE b.cancelled_at
                END
            FROM to_delete td
            WHERE b.id = td.id
            RETURNING b.id
            "#,
            creator_id,
            batch_size,
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(|_| FusilladeError::Other(anyhow!("Failed to erase creator batches")))?
        .len() as u64;

        let files_affected = sqlx::query_scalar::<_, Uuid>(
            r#"
            UPDATE files f
            SET deleted_at = NOW(),
                status = 'deleted'
            WHERE f.id = ANY($1)
              AND f.deleted_at IS NULL
            RETURNING f.id
            "#,
        )
        .bind(&file_ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(|_| FusilladeError::Other(anyhow!("Failed to erase creator files")))?
        .len() as u64;

        tx.commit()
            .await
            .map_err(|_| FusilladeError::Other(anyhow!("Failed to finish creator erasure")))?;

        let total = batchless_requests + batches_affected + files_affected;
        if total > 0 {
            tracing::info!(
                batchless_requests,
                batches = batches_affected,
                files = files_affected,
                "Erased creator data"
            );
        }

        Ok(total)
    }

    async fn retry_failed_requests(
        &self,
        batch_id: BatchId,
        requested_ids: Vec<RequestId>,
    ) -> Result<Vec<Result<()>>> {
        tracing::debug!(%batch_id, count = requested_ids.len(), "Retrying failed requests");

        self.check_retry_age(self.write_executor(), *batch_id)
            .await?;

        let requested_uuids = requested_ids.iter().map(|id| **id).collect::<Vec<_>>();
        let found: std::collections::HashMap<Uuid, (String, bool)> = sqlx::query!(
            r#"
            SELECT r.id AS "id!", r.state AS "state!", (t.id IS NOT NULL) AS "has_template!"
            FROM batches b
            CROSS JOIN LATERAL (
                SELECT id, state, template_id FROM requests
                WHERE batch_id = b.id AND id = ANY($2)
                UNION ALL
                SELECT id, state, template_id FROM batch_requests_archive a
                -- Scalar subquery (not b.archive_bucket) so the bucket is a
                -- run-time param and the archive is pruned to one partition.
                WHERE a.archive_bucket = (SELECT archive_bucket FROM batches WHERE id = $1)
                  AND a.batch_id = b.id AND a.id = ANY($2)
            ) r
            LEFT JOIN LATERAL (
                SELECT t.id FROM request_templates_all t
                WHERE t.id = r.template_id AND t.file_id IS NOT DISTINCT FROM b.file_id
                LIMIT 1
            ) t ON TRUE
            WHERE b.id = $1
            "#,
            *batch_id as Uuid,
            &requested_uuids,
        )
        .fetch_all(self.write_executor())
        .await
        .map_err(|_| FusilladeError::Other(anyhow!("Failed to read requests to retry")))?
        .into_iter()
        .map(|row| (row.id, (row.state, row.has_template)))
        .collect();

        let lifecycle_ids = found.keys().copied().collect::<Vec<_>>();
        if !lifecycle_ids.is_empty() {
            let mut lifecycle_tx = self.begin_response_write(&lifecycle_ids).await?;
            if let Some(disposition) =
                retained_response::classify_response_write(&mut lifecycle_tx, &lifecycle_ids)
                    .await?
            {
                return Err(disposition.into_fusillade_error());
            }
            lifecycle_tx.commit().await.map_err(|_| {
                FusilladeError::Other(anyhow!("Failed to finish failed-request retry check"))
            })?;
        }

        let retryable: Vec<(Uuid, Option<Uuid>)> = found
            .iter()
            .filter(|(_, (state, has_template))| state == "failed" && *has_template)
            .map(|(id, _)| (*id, Some(*batch_id)))
            .collect();

        let mut source_unavailable = false;
        let repended: std::collections::HashSet<Uuid> = if retryable.is_empty() {
            Default::default()
        } else {
            let retry_object_ids = retryable.iter().map(|(id, _)| *id).collect::<Vec<_>>();
            let mut tx = self.begin_response_write(&retry_object_ids).await?;
            if let Some(disposition) =
                retained_response::classify_response_write(&mut tx, &retry_object_ids).await?
            {
                return Err(disposition.into_fusillade_error());
            }

            let requested_batch_ids: Vec<Uuid> = retryable
                .iter()
                .filter_map(|(_, batch_id)| *batch_id)
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            let batch_sources: Vec<(Uuid, Uuid)> = if requested_batch_ids.is_empty() {
                Vec::new()
            } else {
                sqlx::query_as(
                    "SELECT id, file_id FROM batches WHERE id = ANY($1) AND deleted_at IS NULL AND file_id IS NOT NULL",
                )
                .bind(&requested_batch_ids)
                .fetch_all(&mut *tx)
                .await
                .map_err(|_| {
                    FusilladeError::Other(anyhow!("Failed to read retry batch sources"))
                })?
            };
            let source_by_batch: std::collections::HashMap<Uuid, Uuid> =
                batch_sources.into_iter().collect();
            let source_file_ids: Vec<Uuid> = source_by_batch
                .values()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            let available_file_ids: Vec<Uuid> = if source_file_ids.is_empty() {
                Vec::new()
            } else {
                sqlx::query_scalar(
                    r#"
                    SELECT f.id FROM files f
                    JOIN request_template_buckets bucket
                      ON bucket.week_start = date_trunc('week', f.created_at AT TIME ZONE 'UTC')::date
                    WHERE f.id = ANY($1)
                      AND f.deleted_at IS NULL
                      AND f.status = 'processed'
                      AND (f.expires_at IS NULL OR f.expires_at > NOW())
                      AND bucket.state = 'active'
                    ORDER BY f.id
                    FOR UPDATE OF f
                    FOR SHARE OF bucket
                    "#,
                )
                .bind(&source_file_ids)
                .fetch_all(&mut *tx)
                .await
                .map_err(|_| FusilladeError::Other(anyhow!("Failed to lock retry source files")))?
            };
            source_unavailable = available_file_ids.len() < source_file_ids.len();
            let safe_batch_ids: std::collections::HashSet<Uuid> = if available_file_ids.is_empty() {
                Default::default()
            } else {
                sqlx::query_as::<_, (Uuid, Uuid)>(
                    r#"
                    SELECT b.id, b.file_id FROM batches b
                    WHERE b.id = ANY($1)
                      AND b.deleted_at IS NULL
                      AND b.file_id = ANY($2)
                    ORDER BY b.id
                    FOR UPDATE OF b
                    "#,
                )
                .bind(&requested_batch_ids)
                .bind(&available_file_ids)
                .fetch_all(&mut *tx)
                .await
                .map_err(|_| FusilladeError::Other(anyhow!("Failed to lock retry batches")))?
                .into_iter()
                .filter(|(batch_id, file_id)| source_by_batch.get(batch_id) == Some(file_id))
                .map(|(batch_id, _)| batch_id)
                .collect()
            };
            let retry_ids: Vec<Uuid> = retryable
                .iter()
                .filter(|(_, batch_id)| {
                    batch_id.is_none_or(|batch_id| safe_batch_ids.contains(&batch_id))
                })
                .map(|(id, _)| *id)
                .collect();

            let unarchived: Vec<Uuid> = sqlx::query_scalar!(
                r#"
                INSERT INTO requests (id, batch_id, template_id, state, retry_attempt, not_before,
                                      daemon_id, claimed_at, started_at, response_status, response_body,
                                      completed_at, error, failed_at, canceled_at, created_at, updated_at,
                                      custom_id, model, response_size, routed_model, service_tier, created_by)
                SELECT id, batch_id, template_id, 'pending', 0, NULL,
                       NULL, NULL, NULL, NULL, NULL,
                       NULL, NULL, NULL, NULL, created_at, NOW(),
                       custom_id, model, 0, NULL, service_tier, created_by
                FROM batch_requests_archive
                WHERE id = ANY($1) AND state = 'failed'
                ON CONFLICT (id) DO NOTHING
                RETURNING id
                "#,
                &retry_ids,
            )
            .fetch_all(&mut *tx)
            .await
            .map_err(|_| {
                FusilladeError::Other(anyhow!("Failed to move archived rows back for retry"))
            })?;
            if !unarchived.is_empty() {
                sqlx::query!(
                    r#"
                    DELETE FROM batch_requests_archive a
                    WHERE a.id = ANY($1) AND a.state = 'failed'
                      AND EXISTS (SELECT 1 FROM requests r WHERE r.id = a.id)
                    "#,
                    &unarchived,
                )
                .execute(&mut *tx)
                .await
                .map_err(|_| {
                    FusilladeError::Other(anyhow!("Failed to delete moved-back archive rows"))
                })?;
            }

            let repended: Vec<Uuid> = sqlx::query_scalar!(
                r#"
                UPDATE requests
                SET state = 'pending',
                    retry_attempt = 0,
                    not_before = NULL,
                    error = NULL,
                    failed_at = NULL,
                    canceled_at = NULL,
                    completed_at = NULL,
                    response_status = NULL,
                    response_body = NULL,
                    response_size = 0,
                    routed_model = NULL,
                    daemon_id = NULL,
                    claimed_at = NULL,
                    started_at = NULL
                WHERE id = ANY($1) AND state = 'failed'
                RETURNING id
                "#,
                &retry_ids,
            )
            .fetch_all(&mut *tx)
            .await
            .map_err(|_| FusilladeError::Other(anyhow!("Failed to retry failed requests")))?;
            let repended: std::collections::HashSet<Uuid> =
                repended.into_iter().chain(unarchived).collect();

            let batch_ids: Vec<Uuid> = retryable
                .iter()
                .filter(|(id, _)| repended.contains(id))
                .filter_map(|(_, b)| *b)
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            if !batch_ids.is_empty() {
                sqlx::query!(
                    r#"
                    UPDATE batches b
                    SET completed_at = NULL,
                        failed_at = NULL,
                        finalizing_at = NULL,
                        cancelling_at = NULL,
                        cancelled_at = NULL,
                        notification_sent_at = NULL,
                        counts_frozen_at = NULL,
                        completed_requests = 0,
                        failed_requests = 0,
                        canceled_requests = 0,
                        retry_version = retry_version + 1,
                        -- Same phase 3 routing transition as the batch retry:
                        -- archived parents of moved-back rows become 'split'
                        -- (or fully 'live' if the archive side emptied);
                        -- archive_bucket is never cleared.
                        location = CASE
                            WHEN b.location = 'live' THEN 'live'
                            WHEN EXISTS (
                                SELECT 1 FROM batch_requests_archive a
                                WHERE a.archive_bucket = b.archive_bucket AND a.batch_id = b.id
                            ) THEN 'split'
                            ELSE 'live'
                        END
                    WHERE b.id = ANY($1)
                    "#,
                    &batch_ids,
                )
                .execute(&mut *tx)
                .await
                .map_err(|_| {
                    FusilladeError::Other(anyhow!("Failed to reset batch terminal state"))
                })?;
            }

            tx.commit().await.map_err(|_| {
                FusilladeError::Other(anyhow!("Failed to finish failed-request retry mutation"))
            })?;
            repended
        };

        Ok(requested_ids
            .iter()
            .map(|id| match found.get(&**id) {
                None => Err(FusilladeError::RequestNotFound(*id)),
                Some((_, false)) => Err(FusilladeError::Other(anyhow!(
                    "Request template has been deleted"
                ))),
                Some((state, true)) if state == "failed" => {
                    if repended.contains(&**id) {
                        Ok(())
                    } else if source_unavailable {
                        Err(FusilladeError::ValidationError(
                            "batch source file is deleted, expired, or its content has been retired"
                                .to_string(),
                        ))
                    } else {
                        Err(FusilladeError::InvalidState(
                            *id,
                            "state changed concurrently".to_string(),
                            "failed state".to_string(),
                        ))
                    }
                }
                Some(_) => Err(FusilladeError::InvalidState(
                    *id,
                    "non-failed state".to_string(),
                    "failed state".to_string(),
                )),
            })
            .collect())
    }

    async fn retry_failed_requests_for_batch(&self, batch_id: BatchId) -> Result<u64> {
        tracing::debug!(%batch_id, "Retrying failed/canceled requests for batch");

        let mut tx = self
            .begin_write()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to begin transaction: {}", e)))?;

        let source_file_id: Option<Option<Uuid>> =
            sqlx::query_scalar("SELECT file_id FROM batches WHERE id = $1 AND deleted_at IS NULL")
                .bind(*batch_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| {
                    FusilladeError::Other(anyhow!("Failed to read batch source: {}", e))
                })?;

        let Some(source_file_id) = source_file_id else {
            return Ok(0);
        };
        let Some(source_file_id) = source_file_id else {
            return Err(FusilladeError::ValidationError(
                "batch source file is unavailable".to_string(),
            ));
        };

        self.check_retry_age(&mut *tx, *batch_id).await?;

        let source_available = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT f.id FROM files f
            JOIN request_template_buckets bucket
              ON bucket.week_start = date_trunc('week', f.created_at AT TIME ZONE 'UTC')::date
            WHERE f.id = $1
              AND f.deleted_at IS NULL
              AND f.status = 'processed'
              AND (f.expires_at IS NULL OR f.expires_at > NOW())
              AND bucket.state = 'active'
            FOR UPDATE OF f
            FOR SHARE OF bucket
            "#,
        )
        .bind(source_file_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to lock batch source for retry: {}", e))
        })?
        .is_some();
        if !source_available {
            return Err(FusilladeError::ValidationError(
                "batch source file is deleted, expired, or its content has been retired"
                    .to_string(),
            ));
        }

        let routing = sqlx::query_as::<_, (String, Option<chrono::NaiveDate>, Option<Uuid>)>(
            r#"
            SELECT location, archive_bucket, file_id
            FROM batches WHERE id = $1 AND deleted_at IS NULL
            FOR UPDATE
            "#,
        )
        .bind(*batch_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to lock batch for retry: {}", e)))?;

        let Some(routing) = routing else {
            return Ok(0);
        };
        if routing.2 != Some(source_file_id) {
            return Err(FusilladeError::ValidationError(
                "batch source file changed while retrying".to_string(),
            ));
        }

        let unarchived = if routing.0 != "live" {
            let Some(bucket) = routing.1 else {
                return Err(FusilladeError::Other(anyhow!(
                    "batch {batch_id} has location '{}' but no archive_bucket \
                     (batches_archived_have_bucket should make this impossible)",
                    routing.0
                )));
            };
            let bucket_state: Option<String> = sqlx::query_scalar(
                "SELECT state FROM batch_archive_buckets WHERE week_start = $1 FOR SHARE",
            )
            .bind(bucket)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to lock archive bucket for retry: {}", e))
            })?;
            if bucket_state.as_deref() != Some("active") {
                return Err(FusilladeError::ValidationError(
                    "batch results have been retired; the batch can no longer be retried"
                        .to_string(),
                ));
            }
            let moved = sqlx::query!(
                r#"
                INSERT INTO requests (id, batch_id, template_id, state, retry_attempt, not_before,
                                      daemon_id, claimed_at, started_at, response_status, response_body,
                                      completed_at, error, failed_at, canceled_at, created_at, updated_at,
                                      custom_id, model, response_size, routed_model, service_tier, created_by)
                SELECT id, batch_id, template_id, 'pending', 0, NULL,
                       NULL, NULL, NULL, NULL, NULL,
                       NULL, NULL, NULL, NULL, created_at, NOW(),
                       custom_id, model, 0, NULL, service_tier, created_by
                FROM batch_requests_archive
                WHERE archive_bucket = $2 AND batch_id = $1 AND state IN ('failed', 'canceled')
                ON CONFLICT (id) DO NOTHING
                "#,
                *batch_id as Uuid,
                bucket,
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to move archived rows back to live: {}", e))
            })?
            .rows_affected();

            sqlx::query!(
                r#"
                DELETE FROM batch_requests_archive a
                WHERE a.archive_bucket = $2 AND a.batch_id = $1
                  AND a.state IN ('failed', 'canceled')
                  AND EXISTS (SELECT 1 FROM requests r WHERE r.id = a.id)
                "#,
                *batch_id as Uuid,
                bucket,
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to delete moved-back archive rows: {}", e))
            })?;

            moved
        } else {
            0
        };

        let result = sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'pending',
                retry_attempt = 0,
                not_before = NULL,
                error = NULL,
                failed_at = NULL,
                canceled_at = NULL,
                completed_at = NULL,
                response_status = NULL,
                response_body = NULL,
                response_size = 0,
                routed_model = NULL,
                daemon_id = NULL,
                claimed_at = NULL,
                started_at = NULL
            WHERE batch_id = $1 AND state IN ('failed', 'canceled')
            "#,
            *batch_id as Uuid,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to retry failed/canceled requests: {}", e))
        })?;

        let count = result.rows_affected() + unarchived;

        sqlx::query!(
            r#"
            UPDATE batches b
            SET completed_at = NULL,
                failed_at = NULL,
                finalizing_at = NULL,
                cancelling_at = NULL,
                cancelled_at = NULL,
                notification_sent_at = NULL,
                counts_frozen_at = NULL,
                completed_requests = 0,
                failed_requests = 0,
                canceled_requests = 0,
                retry_version = retry_version + 1,
                -- Phase 3 routing: an archived batch whose failed/canceled
                -- rows just moved back becomes 'split' (completed rows still
                -- archived) — or fully 'live' again if the archive side is
                -- now empty (nothing had completed). archive_bucket is NEVER
                -- cleared: it is the immutable partition stamp that re-
                -- archiving reuses so a batch can never scatter.
                location = CASE
                    WHEN b.location = 'live' THEN 'live'
                    WHEN EXISTS (
                        SELECT 1 FROM batch_requests_archive a
                        WHERE a.archive_bucket = b.archive_bucket AND a.batch_id = b.id
                    ) THEN 'split'
                    ELSE 'live'
                END
            WHERE b.id = $1
              AND ($2::BIGINT > 0 OR b.cancelling_at IS NOT NULL)
            "#,
            *batch_id as Uuid,
            count as i64,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to reset batch terminal state: {}", e))
        })?;

        tx.commit()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to commit transaction: {}", e)))?;

        tracing::debug!(%batch_id, count, "Retried failed/canceled requests for batch");

        Ok(count)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(batch_id = %batch_id))]
    async fn get_batch_requests(&self, batch_id: BatchId) -> Result<Vec<AnyRequest>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                r.id as "id!", r.batch_id as "batch_id!", r.template_id as "template_id?", r.state as "state!",
                t.custom_id as "custom_id?", t.endpoint as "endpoint?", t.method as "method?",
                t.path as "path?", t.body as "body?", t.model as "model?",
                r.retry_attempt as "retry_attempt!", r.not_before, r.daemon_id, r.claimed_at, r.started_at,
                r.response_status, r.response_body, r.completed_at, r.error, r.failed_at, r.canceled_at,
                b.expires_at as batch_expires_at, r.routed_model
            FROM (
                -- Always-union over live + bucket-pruned archive: correct for
                -- live, archived, and split batches alike (a row lives in
                -- exactly one table). The archive arm resolves the bucket
                -- from the batch row, so it prunes to one partition; for a
                -- never-archived batch the bucket is NULL and the arm is
                -- empty.
                SELECT id, batch_id, template_id, state, retry_attempt, not_before, daemon_id,
                       claimed_at, started_at, response_status, response_body, completed_at,
                       error, failed_at, canceled_at, routed_model, created_at
                FROM requests WHERE batch_id = $1
                UNION ALL
                SELECT a.id, a.batch_id, a.template_id, a.state, a.retry_attempt, a.not_before, a.daemon_id,
                       a.claimed_at, a.started_at, a.response_status, a.response_body, a.completed_at,
                       a.error, a.failed_at, a.canceled_at, a.routed_model, a.created_at
                FROM batch_requests_archive a
                WHERE a.archive_bucket = (SELECT archive_bucket FROM batches WHERE id = $1)
                  AND a.batch_id = $1
            ) r
            JOIN batches b ON r.batch_id = b.id
            -- Per-row primary-key lookup; see the claim paths for why a plain
            -- join on the two-generation view is avoided.
            LEFT JOIN LATERAL (
                SELECT * FROM request_templates_all t
                WHERE t.id = r.template_id AND t.file_id IS NOT DISTINCT FROM b.file_id
                LIMIT 1
            ) t ON TRUE
            WHERE b.deleted_at IS NULL
            ORDER BY r.created_at ASC
            "#,
            *batch_id as Uuid,
        )
        .fetch_all(self.read_executor())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch batch executions: {}", e)))?;

        let mut results = Vec::new();

        for row in rows {
            let request_id = RequestId(row.id);

            let data = match (
                row.template_id,
                row.endpoint,
                row.method,
                row.path,
                row.body,
                row.model,
            ) {
                (
                    Some(template_id),
                    Some(endpoint),
                    Some(method),
                    Some(path),
                    Some(body),
                    Some(model),
                ) => RequestData {
                    id: request_id,
                    batch_id: Some(BatchId(row.batch_id)),
                    template_id: TemplateId(template_id),
                    custom_id: row.custom_id,
                    endpoint,
                    method,
                    path,
                    body,
                    model,
                    api_key: String::new(),
                    created_by: String::new(),
                    batch_metadata: std::collections::HashMap::new(),
                },
                _ => {
                    tracing::debug!(request_id = %request_id, "Skipping batch request with deleted template");
                    continue;
                }
            };

            let state = &row.state;

            let any_request = match state.as_str() {
                "pending" => AnyRequest::Pending(Request {
                    state: Pending {
                        retry_attempt: row.retry_attempt as u32,
                        not_before: row.not_before,
                        batch_expires_at: row.batch_expires_at,
                    },
                    data,
                }),
                "claimed" => AnyRequest::Claimed(Request {
                    state: Claimed {
                        daemon_id: DaemonId(row.daemon_id.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing daemon_id for claimed execution"
                            ))
                        })?),
                        claimed_at: row.claimed_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing claimed_at for claimed execution"
                            ))
                        })?,
                        retry_attempt: row.retry_attempt as u32,
                        batch_expires_at: row.batch_expires_at,
                        leak: None,
                    },
                    data,
                }),
                "processing" => {
                    let (_tx, rx) = tokio::sync::mpsc::channel(1);
                    let abort_handle = tokio::spawn(async {}).abort_handle();
                    AnyRequest::Processing(Request {
                        state: Processing {
                            daemon_id: DaemonId(row.daemon_id.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing daemon_id for processing execution"
                                ))
                            })?),
                            claimed_at: row.claimed_at.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing claimed_at for processing execution"
                                ))
                            })?,
                            started_at: row.started_at.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing started_at for processing execution"
                                ))
                            })?,
                            retry_attempt: row.retry_attempt as u32,
                            batch_expires_at: row.batch_expires_at,
                            result_rx: Arc::new(Mutex::new(rx)),
                            abort_handle,
                        },
                        data,
                    })
                }
                "completed" => AnyRequest::Completed(Request {
                    state: Completed {
                        response_status: row.response_status.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing response_status for completed execution"
                            ))
                        })? as u16,
                        response_body: row.response_body.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing response_body for completed execution"
                            ))
                        })?,
                        claimed_at: row.claimed_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing claimed_at for completed execution"
                            ))
                        })?,
                        started_at: row.started_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing started_at for completed execution"
                            ))
                        })?,
                        completed_at: row.completed_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing completed_at for completed execution"
                            ))
                        })?,
                        routed_model: row.routed_model.unwrap_or_else(|| data.model.clone()),
                    },
                    data,
                }),
                "failed" => {
                    let error_json = row.error.ok_or_else(|| {
                        FusilladeError::Other(anyhow!("Missing error for failed execution"))
                    })?;

                    let reason: FailureReason =
                        serde_json::from_str(&error_json).unwrap_or_else(|_| {
                            FailureReason::NetworkError {
                                error: error_json.clone(),
                            }
                        });

                    AnyRequest::Failed(Request {
                        state: Failed {
                            reason,
                            failed_at: row.failed_at.ok_or_else(|| {
                                FusilladeError::Other(anyhow!(
                                    "Missing failed_at for failed execution"
                                ))
                            })?,
                            claimed_at: row.claimed_at,
                            retry_attempt: row.retry_attempt as u32,
                            batch_expires_at: row.batch_expires_at,
                            routed_model: row.routed_model.unwrap_or_else(|| data.model.clone()),
                        },
                        data,
                    })
                }
                "canceled" => AnyRequest::Canceled(Request {
                    state: Canceled {
                        canceled_at: row.canceled_at.ok_or_else(|| {
                            FusilladeError::Other(anyhow!(
                                "Missing canceled_at for canceled execution"
                            ))
                        })?,
                        claimed_at: row.claimed_at,
                    },
                    data,
                }),
                _ => {
                    return Err(FusilladeError::Other(anyhow!("Unknown state: {}", state)));
                }
            };

            results.push(any_request);
        }

        Ok(results)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(batch_id = %batch_id, search = ?search, status = ?status))]
    fn get_batch_results_stream(
        &self,
        batch_id: BatchId,
        offset: usize,
        search: Option<String>,
        status: Option<String>,
    ) -> Pin<Box<dyn Stream<Item = Result<crate::batch::BatchResultItem>> + Send>> {
        let pools = self.pools.clone();
        let retry_config = self.db_retry_config.clone();
        let query_schema = self.query_schema.clone();
        let (tx, rx) = mpsc::channel(self.download_buffer_size);
        let offset = offset as i64;

        tokio::spawn(async move {
            Self::stream_batch_results(
                pools,
                retry_config,
                query_schema,
                batch_id,
                offset,
                (search, status),
                tx,
            )
            .await;
        });

        Box::pin(ReceiverStream::new(rx))
    }

    #[tracing::instrument(level = "debug", skip(self, filter))]
    async fn list_requests(
        &self,
        filter: crate::request::ListRequestsFilter,
    ) -> Result<crate::request::RequestListResult> {
        retained_response::list_requests(self, filter).await
    }

    #[tracing::instrument(level = "debug", skip(self, request_id))]
    async fn get_request_detail(
        &self,
        request_id: crate::request::RequestId,
    ) -> Result<crate::request::RequestDetail> {
        retained_response::get_request_detail(self, request_id).await
    }

    #[tracing::instrument(level = "debug", skip(self, input))]
    async fn create_realtime(&self, input: CreateRealtimeInput) -> Result<RequestId> {
        let template_id = Uuid::new_v4();
        let now = Utc::now();

        let mut tx = self.begin_response_write(&[input.request_id]).await?;

        if let Some(disposition) =
            retained_response::classify_response_write(&mut tx, &[input.request_id]).await?
        {
            return Err(disposition.into_fusillade_error());
        }

        let stored_body = sanitize_outbound_body(&input.body);
        template_store::ensure_current_week_partition(&mut tx).await?;
        template_store::insert_dedicated_template(
            &mut tx,
            template_store::DedicatedTemplate {
                id: template_id,
                endpoint: &input.endpoint,
                method: &input.method,
                path: &input.path,
                body: stored_body.as_ref(),
                model: &input.model,
                api_key_id: None,
                metadata: None,
            },
        )
        .await
        .map_err(|_| FusilladeError::Other(anyhow!("Failed to insert realtime template")))?;

        let created_by = Some(input.created_by.trim()).filter(|s| !s.is_empty());
        let inserted = sqlx::query(
            "INSERT INTO requests (id, batch_id, template_id, model, custom_id, state, retry_attempt, service_tier, created_by, daemon_id, claimed_at, started_at)
             VALUES ($1, NULL, $2, $3, NULL, 'processing', 0, 'priority', $4, $5, $6, $6)",
        )
        .bind(input.request_id)
        .bind(template_id)
        .bind(&input.model)
        .bind(created_by)
        .bind(Uuid::nil())
        .bind(now)
        .execute(&mut *tx)
        .await;
        if let Err(error) = inserted {
            let _ = error;
            return Err(FusilladeError::Other(anyhow!(
                "Failed to insert realtime request"
            )));
        }

        if let Some(disposition) =
            retained_response::classify_response_write(&mut tx, &[input.request_id]).await?
        {
            return Err(disposition.into_fusillade_error());
        }

        tx.commit()
            .await
            .map_err(|_| FusilladeError::Other(anyhow!("Failed to finish realtime request")))?;

        Ok(RequestId(input.request_id))
    }

    #[tracing::instrument(level = "debug", skip(self, input))]
    async fn create_flex(&self, input: CreateFlexInput) -> Result<RequestId> {
        self.insert_batchless_pending_request(NewPendingRequest {
            request_id: input.request_id,
            body: input.body,
            model: input.model,
            endpoint: input.endpoint,
            method: input.method,
            path: input.path,
            api_key_id: input.api_key_id,
            created_by: input.created_by,
            service_tier: "flex",
            metadata: input.metadata,
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self, input))]
    async fn create_background(&self, input: CreateBackgroundInput) -> Result<RequestId> {
        self.insert_batchless_pending_request(NewPendingRequest {
            request_id: input.request_id,
            body: input.body,
            model: input.model,
            endpoint: input.endpoint,
            method: input.method,
            path: input.path,
            api_key_id: input.api_key_id,
            created_by: input.created_by,
            service_tier: "background",
            metadata: input.metadata,
        })
        .await
    }

    async fn complete_request(
        &self,
        request_id: RequestId,
        response_body: &str,
        status_code: u16,
    ) -> Result<()> {
        let size = response_body.len() as i64;
        let mut tx = self.begin_response_write(&[request_id.0]).await?;

        let row: Option<(bool, Option<String>)> = sqlx::query_as(
            "WITH updated AS (
                 UPDATE requests
                    SET state = 'completed',
                        response_status = $2,
                        response_body = $3,
                        response_size = $4,
                        completed_at = NOW()
                  WHERE id = $1 AND state = 'processing'
                 RETURNING 1
             )
             SELECT EXISTS(SELECT 1 FROM updated) AS matched,
                    (SELECT state FROM requests WHERE id = $1) AS current_state
             WHERE EXISTS(SELECT 1 FROM requests WHERE id = $1)",
        )
        .bind(request_id.0)
        .bind(status_code as i16)
        .bind(response_body)
        .bind(size)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| FusilladeError::Other(anyhow!("Failed to complete request")))?;

        match row {
            Some((true, _)) => {
                tx.commit().await.map_err(|_| {
                    FusilladeError::Other(anyhow!("Failed to finish request completion"))
                })?;
                Ok(())
            }
            Some((false, Some(current_state))) => Err(FusilladeError::RequestStateConflict {
                id: request_id,
                current_state,
                expected: "processing",
            }),
            Some((false, None)) | None => {
                match retained_response::classify_response_write(&mut tx, &[request_id.0]).await? {
                    Some(disposition) => Err(disposition.into_fusillade_error()),
                    None => Err(FusilladeError::RequestNotFound(request_id)),
                }
            }
        }
    }

    async fn fail_request(
        &self,
        request_id: RequestId,
        error: &str,
        status_code: u16,
    ) -> Result<()> {
        let reason = FailureReason::NonRetriableHttpStatus {
            status: status_code,
            body: error.to_string(),
        };
        let error_json = serde_json::to_string(&reason).map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to serialize failure reason: {}", e))
        })?;

        let error_size = error_json.len() as i64;
        let mut tx = self.begin_response_write(&[request_id.0]).await?;
        let row: Option<(bool, Option<String>)> = sqlx::query_as(
            "WITH updated AS (
                 UPDATE requests
                    SET state = 'failed',
                        error = $2,
                        response_size = $3,
                        failed_at = NOW()
                  WHERE id = $1 AND state = 'processing'
                 RETURNING 1
             )
             SELECT EXISTS(SELECT 1 FROM updated) AS matched,
                    (SELECT state FROM requests WHERE id = $1) AS current_state
             WHERE EXISTS(SELECT 1 FROM requests WHERE id = $1)",
        )
        .bind(request_id.0)
        .bind(&error_json)
        .bind(error_size)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| FusilladeError::Other(anyhow!("Failed to fail request")))?;

        match row {
            Some((true, _)) => {
                tx.commit().await.map_err(|_| {
                    FusilladeError::Other(anyhow!("Failed to finish request failure"))
                })?;
                Ok(())
            }
            Some((false, Some(current_state))) => Err(FusilladeError::RequestStateConflict {
                id: request_id,
                current_state,
                expected: "processing",
            }),
            Some((false, None)) | None => {
                match retained_response::classify_response_write(&mut tx, &[request_id.0]).await? {
                    Some(disposition) => Err(disposition.into_fusillade_error()),
                    None => Err(FusilladeError::RequestNotFound(request_id)),
                }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self, records), fields(batch_size = records.len()))]
    async fn persist_completed_realtime_batch(
        &self,
        records: &[PersistCompletedRealtimeInput],
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let mut canonical = Vec::with_capacity(records.len());
        let mut seen = std::collections::HashSet::with_capacity(records.len());
        for record in records {
            if seen.insert(record.request_id) {
                canonical.push(record);
            }
        }

        let all_ids: Vec<Uuid> = canonical.iter().map(|record| record.request_id).collect();
        let mut tx = self.begin_response_write(&all_ids).await?;

        let dispositions =
            retained_response::classify_response_write_ids(&mut tx, &all_ids).await?;
        if !dispositions.unavailable.is_empty() {
            return Err(RetainedResponseWriteError::NotFound.into_fusillade_error());
        }
        let writable = canonical
            .into_iter()
            .filter(|record| !dispositions.already_retained.contains(&record.request_id))
            .collect::<Vec<_>>();
        if writable.is_empty() {
            tx.commit()
                .await
                .map_err(|_| FusilladeError::Other(anyhow!("Failed to finish response write")))?;
            return Ok(());
        }

        let ids: Vec<Uuid> = writable.iter().map(|r| r.request_id).collect();
        let response_bodies: Vec<&str> =
            writable.iter().map(|r| r.response_body.as_str()).collect();
        let response_sizes: Vec<i64> = writable
            .iter()
            .map(|r| r.response_body.len() as i64)
            .collect();
        let response_statuses: Vec<i16> = writable.iter().map(|r| r.status_code as i16).collect();

        let to_failure_error = |r: &PersistCompletedRealtimeInput| -> Option<String> {
            if (200..300).contains(&r.status_code) {
                return None;
            }
            let reason = FailureReason::NonRetriableHttpStatus {
                status: r.status_code,
                body: r.response_body.clone(),
            };
            Some(serde_json::to_string(&reason).unwrap_or_else(|_| {
                format!(
                    r#"{{"type":"NonRetriableHttpStatus","details":{{"status":{},"body":""}}}}"#,
                    r.status_code
                )
            }))
        };
        let errors: Vec<Option<String>> = writable
            .iter()
            .map(|record| to_failure_error(record))
            .collect();

        let updated_rows = sqlx::query!(
            r#"
            UPDATE requests r
               SET state = CASE WHEN v.status BETWEEN 200 AND 299 THEN 'completed' ELSE 'failed' END,
                   response_status = v.status,
                   response_body = v.body,
                   response_size = v.size,
                   error = v.error,
                   completed_at = CASE WHEN v.status BETWEEN 200 AND 299 THEN NOW() ELSE NULL END,
                   failed_at = CASE WHEN v.status BETWEEN 200 AND 299 THEN NULL ELSE NOW() END
              FROM UNNEST(
                  $1::uuid[], $2::text[], $3::bigint[], $4::smallint[], $5::text[]
              ) AS v(id, body, size, status, error)
             WHERE r.id = v.id
               AND r.state = 'processing'
               AND r.service_tier = 'priority'
               AND r.batch_id IS NULL
            RETURNING r.id
            "#,
            &ids as &[Uuid],
            &response_bodies as &[&str],
            &response_sizes as &[i64],
            &response_statuses as &[i16],
            &errors as &[Option<String>],
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(|_| FusilladeError::Other(anyhow!("Failed to update existing requests")))?;

        let updated_set: std::collections::HashSet<Uuid> =
            updated_rows.iter().map(|r| r.id).collect();

        let existing_ids: std::collections::HashSet<Uuid> = sqlx::query_scalar(
            r#"
            SELECT id FROM requests WHERE id = ANY($1)
            UNION
            SELECT id FROM batch_requests_archive WHERE id = ANY($1)
            "#,
        )
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(|_| FusilladeError::Other(anyhow!("Failed to classify existing requests")))?
        .into_iter()
        .collect();
        let mut to_insert: Vec<&PersistCompletedRealtimeInput> = writable
            .iter()
            .copied()
            .filter(|r| {
                !updated_set.contains(&r.request_id) && !existing_ids.contains(&r.request_id)
            })
            .collect();

        let mut retained_count = 0;
        let mut retained_fallback_count = 0;
        if let Some(retention_seconds) = self.realtime_retention_seconds {
            let retained_ids = retained_response::insert_completed_realtime(
                &mut tx,
                &to_insert,
                retention_seconds,
                self.retained_response_fence_seconds,
            )
            .await?;
            retained_count = retained_ids.len() as u64;
            to_insert.retain(|record| !retained_ids.contains(&record.request_id));
            retained_fallback_count = to_insert.len() as u64;
        }

        if !to_insert.is_empty() {
            let template_ids: Vec<Uuid> = (0..to_insert.len()).map(|_| Uuid::new_v4()).collect();
            let request_ids: Vec<Uuid> = to_insert.iter().map(|r| r.request_id).collect();
            let endpoints: Vec<&str> = to_insert.iter().map(|r| r.endpoint.as_str()).collect();
            let methods: Vec<&str> = to_insert.iter().map(|r| r.method.as_str()).collect();
            let paths: Vec<&str> = to_insert.iter().map(|r| r.path.as_str()).collect();
            let stored_bodies: Vec<std::borrow::Cow<'_, str>> = to_insert
                .iter()
                .map(|r| sanitize_outbound_body(&r.request_body))
                .collect();
            let bodies: Vec<&str> = stored_bodies.iter().map(AsRef::as_ref).collect();
            let body_sizes: Vec<i64> = stored_bodies.iter().map(|b| b.len() as i64).collect();
            let models: Vec<&str> = to_insert.iter().map(|r| r.model.as_str()).collect();
            let created_bys: Vec<&str> = to_insert.iter().map(|r| r.created_by.as_str()).collect();
            let insert_response_bodies: Vec<&str> =
                to_insert.iter().map(|r| r.response_body.as_str()).collect();
            let insert_response_sizes: Vec<i64> = to_insert
                .iter()
                .map(|r| r.response_body.len() as i64)
                .collect();
            let insert_response_statuses: Vec<i16> =
                to_insert.iter().map(|r| r.status_code as i16).collect();
            let insert_errors: Vec<Option<String>> =
                to_insert.iter().map(|r| to_failure_error(r)).collect();
            let started_ats: Vec<DateTime<Utc>> = to_insert.iter().map(|r| r.started_at).collect();
            let completed_ats: Vec<DateTime<Utc>> =
                to_insert.iter().map(|r| r.completed_at).collect();

            template_store::ensure_current_week_partition(&mut tx).await?;
            sqlx::query(
                r#"
                WITH inserted AS (
                    INSERT INTO request_templates_g2 (
                        created_on, id, file_id, custom_id, endpoint, method,
                        path, body, model, body_byte_size
                    )
                    SELECT (transaction_timestamp() AT TIME ZONE 'UTC')::date,
                           id, NULL, NULL, endpoint, method, path, body, model,
                           body_byte_size
                    FROM UNNEST(
                        $1::uuid[], $2::text[], $3::text[], $4::text[],
                        $5::text[], $6::text[], $7::bigint[]
                    ) AS t(id, endpoint, method, path, body, model, body_byte_size)
                    RETURNING id, created_on
                )
                INSERT INTO request_template_routes (template_id, week_start)
                SELECT id, date_trunc('week', created_on)::date FROM inserted
                "#,
            )
            .bind(&template_ids)
            .bind(&endpoints)
            .bind(&methods)
            .bind(&paths)
            .bind(&bodies)
            .bind(&models)
            .bind(&body_sizes)
            .execute(&mut *tx)
            .await
            .map_err(|_| FusilladeError::Other(anyhow!("Failed to insert realtime templates")))?;

            let inserted_rows = sqlx::query_scalar::<_, Uuid>(
                r#"
                INSERT INTO requests (
                    id, batch_id, template_id, model, custom_id,
                    state, retry_attempt, service_tier, created_by,
                    daemon_id, claimed_at, started_at, completed_at, failed_at,
                    response_status, response_body, response_size, error, created_at
                )
                SELECT id, NULL, template_id, model, NULL,
                       CASE WHEN status BETWEEN 200 AND 299 THEN 'completed' ELSE 'failed' END,
                       0, 'priority', NULLIF(TRIM(created_by), ''),
                       $1, started_at, started_at,
                       CASE WHEN status BETWEEN 200 AND 299 THEN completed_at ELSE NULL END,
                       CASE WHEN status BETWEEN 200 AND 299 THEN NULL ELSE completed_at END,
                       status, body, size, error, started_at
                FROM UNNEST(
                    $2::uuid[], $3::uuid[], $4::text[], $5::text[],
                    $6::smallint[], $7::text[], $8::bigint[], $9::text[],
                    $10::timestamptz[], $11::timestamptz[]
                ) AS v(id, template_id, model, created_by, status, body, size, error, started_at, completed_at)
                ON CONFLICT (id) DO NOTHING
                RETURNING id
                "#,
            )
            .bind(Uuid::nil())
            .bind(&request_ids)
            .bind(&template_ids)
            .bind(&models)
            .bind(&created_bys)
            .bind(&insert_response_statuses)
            .bind(&insert_response_bodies)
            .bind(&insert_response_sizes)
            .bind(&insert_errors)
            .bind(&started_ats)
            .bind(&completed_ats)
            .fetch_all(&mut *tx)
            .await
            .map_err(|_| {
                FusilladeError::Other(anyhow!("Failed to insert completed realtime requests"))
            })?;

            let inserted_ids: std::collections::HashSet<Uuid> = inserted_rows.into_iter().collect();
            let post_insert_dispositions = retained_response::classify_response_write_ids(
                &mut tx,
                &inserted_ids.iter().copied().collect::<Vec<_>>(),
            )
            .await?;
            if !post_insert_dispositions.unavailable.is_empty() {
                return Err(RetainedResponseWriteError::NotFound.into_fusillade_error());
            }
            if !post_insert_dispositions.already_retained.is_empty() {
                let protected_request_ids = post_insert_dispositions
                    .already_retained
                    .iter()
                    .copied()
                    .collect::<Vec<_>>();
                sqlx::query("DELETE FROM requests WHERE id = ANY($1)")
                    .bind(&protected_request_ids)
                    .execute(&mut *tx)
                    .await
                    .map_err(|_| {
                        FusilladeError::Other(anyhow!("Failed to discard protected requests"))
                    })?;
            }
            let orphan_template_ids: Vec<Uuid> = request_ids
                .iter()
                .zip(template_ids.iter())
                .filter_map(|(request_id, template_id)| {
                    (!inserted_ids.contains(request_id)
                        || post_insert_dispositions
                            .already_retained
                            .contains(request_id))
                    .then_some(*template_id)
                })
                .collect();
            template_store::delete_dedicated_templates(
                &mut tx,
                &orphan_template_ids,
                template_store::DedicatedDeleteScope::Any,
            )
            .await
            .map_err(|_| FusilladeError::Other(anyhow!("Failed to discard unused templates")))?;
        }

        tx.commit()
            .await
            .map_err(|_| FusilladeError::Other(anyhow!("Failed to finish response write")))?;

        if retained_count > 0 {
            metrics::counter!("fusillade_realtime_retained_total").increment(retained_count);
        }
        if retained_fallback_count > 0 {
            metrics::counter!("fusillade_realtime_retained_fallback_total")
                .increment(retained_fallback_count);
        }

        Ok(())
    }
}

impl<P: PoolProvider> PostgresRequestManager<P> {
    async fn claim_batch_requests_inner(
        &self,
        limit: usize,
        batch_limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
        leak_cooldown: Option<&std::collections::HashSet<(String, String, String)>>,
    ) -> Result<Vec<Request<Claimed>>> {

        let now = Utc::now();
        let mut model_capacity_pairs: Vec<(String, i64)> = available_capacity
            .iter()
            .filter(|(_, cap)| **cap > 0)
            .map(|(model, cap)| (model.clone(), *cap as i64))
            .collect();
        {
            use rand::seq::SliceRandom;
            let mut rng = rand::rng();
            model_capacity_pairs.shuffle(&mut rng);
        }

        let models_arr: Vec<String> = model_capacity_pairs
            .iter()
            .map(|(model, _)| model.clone())
            .collect();
        let capacities_arr: Vec<i64> = model_capacity_pairs
            .iter()
            .map(|(_, capacity)| *capacity)
            .collect();

        if models_arr.is_empty() {
            tracing::debug!("No models with available capacity, skipping batch claim");
            return Ok(Vec::new());
        }

        let user_ids_arr: Vec<String> = user_active_counts.keys().cloned().collect();
        let user_counts_arr: Vec<i64> = user_ids_arr
            .iter()
            .map(|u| *user_active_counts.get(u).unwrap_or(&0) as i64)
            .collect();
        let batch_limit = batch_limit.max(1) as i64;

        let leak_enabled = leak_cooldown.is_some();
        let empty_cooldown = std::collections::HashSet::new();
        let leak_cooldown = leak_cooldown.unwrap_or(&empty_cooldown);

        let mut cooldown_users = Vec::with_capacity(leak_cooldown.len());
        let mut cooldown_windows = Vec::with_capacity(leak_cooldown.len());
        let mut cooldown_models = Vec::with_capacity(leak_cooldown.len());
        for (user, window, model) in leak_cooldown {
            cooldown_users.push(user.clone());
            cooldown_windows.push(window.clone());
            cooldown_models.push(model.clone());
        }

        let rows = sqlx::query_as!(
            ClaimedRequestRow,
            r#"
            WITH RECURSIVE all_models AS (
                SELECT model, capacity
                FROM unnest($4::TEXT[], $5::BIGINT[]) AS m(model, capacity)
            ),
            user_priority AS (
                SELECT * FROM unnest($7::TEXT[], $8::BIGINT[]) AS u(user_id, active_count)
            ),
            latest_model_filters AS (
                -- Scoped to the capacity-eligible models: DISTINCT ON over the
                -- whole event log would grow with the table for no benefit.
                SELECT DISTINCT ON (model) model, state
                FROM model_filters
                WHERE model = ANY($4::TEXT[])
                ORDER BY model, created_at DESC, id DESC
            ),
            -- Distinct batch_ids that still have pending rows for each
            -- capacity-eligible model, via an index-only "loose index scan"
            -- (hop to the next batch_id > the current one) so enumeration costs
            -- O(pairs · log N) — bounded by batches-with-pending-work per
            -- model, never by total pending rows (a naive DISTINCT would scan
            -- every pending index entry) nor by total open batches (the
            -- previous models × batches join). Relies on idx_requests_pending
            -- (model, batch_id).
            batch_groups AS (
                SELECT m.model, m.capacity,
                       (SELECT r.batch_id FROM requests r
                        WHERE r.state = 'pending' AND r.model = m.model
                          AND r.template_id IS NOT NULL AND r.batch_id IS NOT NULL
                          AND r.service_tier IS DISTINCT FROM 'background'
                        ORDER BY r.batch_id LIMIT 1) AS batch_id
                FROM all_models m
              UNION ALL
                SELECT g.model, g.capacity,
                       (SELECT r.batch_id FROM requests r
                        WHERE r.state = 'pending' AND r.model = g.model
                          AND r.template_id IS NOT NULL AND r.batch_id IS NOT NULL
                          AND r.service_tier IS DISTINCT FROM 'background'
                          AND r.batch_id > g.batch_id
                        ORDER BY r.batch_id LIMIT 1) AS batch_id
                FROM batch_groups g WHERE g.batch_id IS NOT NULL
            ),
            ranked_batches AS (
                SELECT *
                FROM (
                    SELECT g.model, g.capacity, b.id AS batch_id,
                           b.expires_at, b.created_at, b.created_by,
                           COALESCE(b.completion_window, '24h') AS window_class,
                           calc.pr, gate.claim_full,
                           row_number() OVER (
                               PARTITION BY g.model, COALESCE(b.created_by, ''),
                                            COALESCE(b.completion_window, '24h'), gate.claim_full
                               ORDER BY calc.pr ASC, b.expires_at ASC, b.id ASC
                           ) AS leak_rank
                    FROM batch_groups g
                    JOIN batches b
                      ON b.id = g.batch_id
                     AND b.cancelling_at IS NULL
                     AND b.deleted_at IS NULL
                     AND b.completed_at IS NULL
                     AND b.failed_at IS NULL
                     AND b.cancelled_at IS NULL
                     AND b.service_tier IS DISTINCT FROM 'background'
                    -- Live/unmanaged and near-deadline batches claim normally.
                    -- Other batches share the async one-row-per-bucket trickle.
                    LEFT JOIN latest_model_filters mf
                      ON mf.model = g.model
                    LEFT JOIN user_priority up ON b.created_by = up.user_id
                    CROSS JOIN LATERAL (
                        SELECT
                            (1.0 - $9::DOUBLE PRECISION)
                                * COALESCE(up.active_count, 0)::DOUBLE PRECISION
                                / GREATEST(NULLIF((SELECT MAX(v) FROM unnest($8::BIGINT[]) v), 0), 1)::DOUBLE PRECISION
                            + $9::DOUBLE PRECISION
                                * LEAST(GREATEST(EXTRACT(EPOCH FROM b.expires_at - $3), 0.0) / 86400.0, 1.0) AS pr
                    ) calc
                    CROSS JOIN LATERAL (
                        SELECT (
                            COALESCE(mf.state = 'live', NOT $10::BOOLEAN)
                            OR (EXTRACT(EPOCH FROM (b.expires_at - $3))
                                <= power(GREATEST(EXTRACT(EPOCH FROM (b.expires_at - b.created_at)), 0.0) / 60.0,
                                         $11::DOUBLE PRECISION) * 60.0)
                        ) AS claim_full
                    ) gate
                    WHERE (gate.claim_full OR ($15::BOOLEAN AND NOT EXISTS (
                        SELECT 1 FROM unnest($12::TEXT[], $13::TEXT[], $14::TEXT[]) AS cd(u, w, mdl)
                        WHERE cd.u = COALESCE(b.created_by, '')
                          AND cd.w = COALESCE(b.completion_window, '24h')
                          AND cd.mdl = g.model
                    )))
                      -- Claimable-NOW probe (per enumerated pair, so bounded):
                      -- the loose scan proves pending rows exist, but rows all
                      -- backing off on not_before shouldn't burn a rank slot.
                      AND EXISTS (
                        SELECT 1
                        FROM requests r
                        WHERE r.state = 'pending'
                          AND r.model = g.model
                          AND r.batch_id = g.batch_id
                          AND r.template_id IS NOT NULL
                          AND r.service_tier IS DISTINCT FROM 'background'
                          AND (r.not_before IS NULL OR r.not_before <= $3)
                    )
                ) ranked
                WHERE claim_full OR leak_rank = 1
            ),
            selected_batches AS (
                SELECT * FROM (
                    SELECT rb.*, row_number() OVER (
                        PARTITION BY model ORDER BY claim_full DESC, pr ASC, expires_at ASC, batch_id ASC
                    ) AS batch_rank
                    FROM ranked_batches rb
                ) ranked
                WHERE batch_rank <= $6
            ),
            candidate_rows AS (
                SELECT sb.model, sb.capacity, sb.batch_id, sb.expires_at,
                       sb.window_class, sb.pr, NOT sb.claim_full AS leaked, r.id, r.template_id, r.created_at,
                       GREATEST(EXTRACT(EPOCH FROM (sb.expires_at - sb.created_at)), 0.0)::DOUBLE PRECISION AS window_secs
                FROM selected_batches sb
                CROSS JOIN LATERAL (
                    SELECT r.id, r.template_id, r.created_at
                    FROM requests r
                    WHERE r.state = 'pending'
                      AND r.model = sb.model
                      AND r.batch_id = sb.batch_id
                      AND r.template_id IS NOT NULL
                      AND r.service_tier IS DISTINCT FROM 'background'
                      AND (r.not_before IS NULL OR r.not_before <= $3)
                    ORDER BY r.created_at ASC
                    LIMIT CASE WHEN sb.claim_full THEN sb.capacity ELSE 1 END
                    FOR UPDATE OF r SKIP LOCKED
                ) r
            ),
            to_claim AS (
                SELECT id, template_id, batch_id, expires_at AS effective_expires_at,
                       leaked, window_class, window_secs
                FROM (
                    SELECT c.*,
                           row_number() OVER (
                               PARTITION BY c.model
                               ORDER BY c.leaked ASC, c.pr ASC, c.expires_at ASC, c.batch_id ASC, c.created_at ASC
                           ) AS model_rank
                    FROM candidate_rows c
                ) ranked
                WHERE model_rank <= capacity
                ORDER BY leaked ASC, pr ASC, expires_at ASC, batch_id ASC, created_at ASC
                LIMIT $2::BIGINT
            ),
            -- A pending row whose template is gone (its file soft-deleted, its
            -- weekly partition retired, or the row purged) can never be claimed.
            -- Fail it here, bounded to the rows this tick actually picked, so it
            -- burns a claim slot exactly once instead of on every tick; the
            -- claim update below skips it via its CROSS JOIN on the template.
            stranded AS (
                UPDATE requests r
                   SET state = 'failed',
                       error = 'request template no longer exists',
                       failed_at = $3
                  FROM to_claim tc
                  -- Per-row lookup by primary key, like the claim join below:
                  -- a bare NOT EXISTS on the generation-transparent view gets
                  -- flattened into an anti-join that hashes the whole legacy
                  -- template table; LIMIT 1 keeps it a probe per picked row.
                  LEFT JOIN LATERAL (
                      SELECT 1 AS present FROM active_request_templates t
                      WHERE t.id = tc.template_id LIMIT 1
                  ) t ON true
                 WHERE r.id = tc.id
                   AND t.present IS NULL
                RETURNING r.id
            )
            UPDATE requests r
            SET
                state = 'claimed',
                daemon_id = $1,
                claimed_at = $3
            FROM to_claim tc
            CROSS JOIN LATERAL (
                -- Per-row lookup by primary key. A plain join on the
                -- generation-transparent view lets the planner hash the whole
                -- legacy template table against the handful of claimed rows;
                -- LIMIT 1 keeps this subquery from being flattened into that join.
                SELECT * FROM active_request_templates t WHERE t.id = tc.template_id LIMIT 1
            ) t
            JOIN batches b ON tc.batch_id = b.id
            WHERE r.id = tc.id
            RETURNING r.id,
                      r.batch_id,
                      r.template_id as "template_id!", r.retry_attempt,
                      t.custom_id, t.endpoint as "endpoint!", t.method as "method!", t.path as "path!",
                      t.body as "body!", t.model as "model!",
                      COALESCE(b.api_key_id, t.api_key_id) as "api_key_id",
                      tc.effective_expires_at as "batch_expires_at?",
                      b.id::TEXT as "batch_id_str!",
                      COALESCE(b.file_id::TEXT, '') as "batch_file_id!",
                      b.endpoint as "batch_endpoint!",
                      b.completion_window as "batch_completion_window?",
                      b.metadata::TEXT as "batch_metadata",
                      b.output_file_id::TEXT as "batch_output_file_id",
                      b.error_file_id::TEXT as "batch_error_file_id",
                      COALESCE(b.created_by, '') as "batch_created_by!",
                      to_char(b.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as "batch_created_at!",
                      to_char(tc.effective_expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as "batch_expires_at_str",
                      to_char(b.cancelling_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as "batch_cancelling_at",
                      b.errors::TEXT as "batch_errors",
                      COALESCE(b.total_requests::TEXT, '1') as "batch_total_requests!",
                      tc.leaked as "leaked!",
                      tc.window_class as "window_class!",
                      tc.window_secs as "window_secs!"
            "#,
            *daemon_id as Uuid,
            limit as i64,
            now,
            &models_arr,
            &capacities_arr,
            batch_limit,
            &user_ids_arr,
            &user_counts_arr,
            self.config.urgency_weight,
            self.config.batch_claim_require_live,
            self.config.claim_ramp_exponent,
            &cooldown_users,
            &cooldown_windows,
            &cooldown_models,
            leak_enabled,
        )
        .fetch_all(self.write_executor())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to claim batch requests: {}", e)))?;

        let claimed_count = rows.len();
        if claimed_count > 0 {
            tracing::debug!(claimed = claimed_count, "Claimed batched requests");
        }

        Ok(self.claimed_rows_to_requests(rows, daemon_id, now))
    }

    async fn get_batch_from_pool(
        &self,
        batch_id: BatchId,
        executor: crate::db::RetryingPgPool,
    ) -> Result<Batch> {
        self.get_batch_from_pool_inner(batch_id, executor, true)
            .await
    }

    async fn get_batch_from_pool_inner(
        &self,
        batch_id: BatchId,
        executor: crate::db::RetryingPgPool,
        retry_on_lost_race: bool,
    ) -> Result<Batch> {
        let executor_for_retry = executor.clone();
        let mut query_builder = QueryBuilder::new(
            r#"
            SELECT
                b.id, b.file_id, b.endpoint, b.service_tier, b.completion_window, b.metadata,
                b.output_file_id, b.error_file_id, b.created_by, b.created_at,
                b.expires_at, b.cancelling_at, b.errors,
                b.total_requests,
                b.requests_started_at,
                b.finalizing_at,
                b.completed_at,
                b.failed_at,
                b.cancelled_at,
                b.deleted_at,
                b.notification_sent_at,
                b.api_key_id,
                b.counts_frozen_at,
                b.retry_version,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN 0
                     ELSE COALESCE(counts.pending, 0) END::BIGINT as pending_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN 0
                     ELSE COALESCE(counts.in_progress, 0) END::BIGINT as in_progress_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN b.completed_requests
                     ELSE COALESCE(counts.completed, 0) + COALESCE(arch.completed, 0) END::BIGINT as completed_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN b.failed_requests
                     ELSE COALESCE(counts.failed, 0) + COALESCE(arch.failed, 0) END::BIGINT as failed_requests,
                CASE WHEN b.counts_frozen_at IS NOT NULL THEN b.canceled_requests
                     ELSE COALESCE(counts.canceled, 0) + COALESCE(arch.canceled, 0) END::BIGINT as canceled_requests,
                (COALESCE(counts.canceled_actual, 0) + COALESCE(arch.canceled, 0))::BIGINT as canceled_actual
            FROM batches b
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE state = 'pending' AND b.cancelling_at IS NULL) as pending,
                    COUNT(*) FILTER (WHERE state IN ('claimed', 'processing') AND b.cancelling_at IS NULL) as in_progress,
                    COUNT(*) FILTER (WHERE state = 'completed') as completed,
                    COUNT(*) FILTER (WHERE state = 'failed') as failed,
                    COUNT(*) FILTER (WHERE state = 'canceled' OR (state IN ('pending', 'claimed', 'processing') AND b.cancelling_at IS NOT NULL)) as canceled,
                    COUNT(*) FILTER (WHERE state = 'canceled') as canceled_actual
                FROM requests
                WHERE batch_id = b.id
                  -- Frozen batches serve the persisted counters; the gate is a
                  -- one-time filter that skips this requests scan entirely.
                  AND b.counts_frozen_at IS NULL
            ) counts ON TRUE
            -- Split batches (mid-retry of an archived batch) keep their
            -- already-done rows in the archive: the live recount alone would
            -- undercount and the batch could never re-terminalize. The
            -- archive side is bucket-pruned to one partition and, by the
            -- retry move-back's construction, holds only completed rows for
            -- a split batch (failed/canceled were moved back to live) — the
            -- failed/canceled counts here are belt-and-braces.
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE state = 'completed') as completed,
                    COUNT(*) FILTER (WHERE state = 'failed') as failed,
                    COUNT(*) FILTER (WHERE state = 'canceled') as canceled
                FROM batch_requests_archive a
                WHERE b.location = 'split'
                  AND b.counts_frozen_at IS NULL
                  AND a.archive_bucket = b.archive_bucket
                  AND a.batch_id = b.id
            ) arch ON TRUE
            WHERE b.id = "#,
        );
        query_builder.push_bind(*batch_id as Uuid);
        query_builder.push(" AND b.deleted_at IS NULL");

        let row = query_builder
            .build()
            .fetch_optional(executor)
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to fetch batch: {}", e)))?
            .ok_or_else(|| FusilladeError::Other(anyhow!("Batch not found")))?;

        let pending_requests: i64 = row.get("pending_requests");
        let in_progress_requests: i64 = row.get("in_progress_requests");
        let completed_requests: i64 = row.get("completed_requests");
        let failed_requests: i64 = row.get("failed_requests");
        let canceled_requests: i64 = row.get("canceled_requests");
        let canceled_actual: i64 = row.get("canceled_actual");
        let counts_frozen_at: Option<DateTime<Utc>> = row.get("counts_frozen_at");
        let retry_version: i64 = row.get("retry_version");
        let total_requests: i64 = row.get("total_requests");
        let completed_at: Option<DateTime<Utc>> = row.get("completed_at");
        let failed_at: Option<DateTime<Utc>> = row.get("failed_at");
        let cancelled_at: Option<DateTime<Utc>> = row.get("cancelled_at");
        let finalizing_at_db: Option<DateTime<Utc>> = row.get("finalizing_at");

        let terminal_count = completed_requests + failed_requests + canceled_requests;
        let is_terminal = terminal_count == total_requests && total_requests > 0;

        let is_actually_terminal = counts_frozen_at.is_none()
            && total_requests > 0
            && completed_requests + failed_requests + canceled_actual == total_requests;

        let (finalizing_at, completed_at, failed_at) = if is_terminal
            && completed_at.is_none()
            && failed_at.is_none()
            && cancelled_at.is_none()
        {
            let now = Utc::now();

            let (finalizing, completed, failed) = if completed_requests > 0 {
                (Some(now), Some(now), None)
            } else {
                (Some(now), None, Some(now))
            };

            let stamp_result = sqlx::query!(
                r#"
                UPDATE batches
                SET finalizing_at = COALESCE(finalizing_at, $2),
                    completed_at = COALESCE(completed_at, $3),
                    failed_at = COALESCE(failed_at, $4),
                    completed_requests = $5,
                    failed_requests = $6,
                    canceled_requests = $7,
                    counts_frozen_at = COALESCE(counts_frozen_at, $2)
                WHERE id = $1
                  AND retry_version = $8
                  AND completed_at IS NULL
                  AND failed_at IS NULL
                  AND cancelled_at IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM requests r
                      WHERE r.batch_id = $1
                        AND r.state NOT IN ('completed', 'failed', 'canceled')
                  )
                "#,
                *batch_id as Uuid,
                finalizing,
                completed,
                failed,
                completed_requests,
                failed_requests,
                canceled_requests,
                retry_version,
            )
            .execute(self.write_executor())
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to update terminal timestamps: {}", e))
            })?;

            if stamp_result.rows_affected() > 0 {
                (finalizing, completed, failed)
            } else if retry_on_lost_race {
                tracing::debug!(
                    %batch_id,
                    "Terminal stamp lost a race; re-reading batch for a consistent view"
                );
                return Box::pin(self.get_batch_from_pool_inner(
                    batch_id,
                    executor_for_retry,
                    false,
                ))
                .await;
            } else {
                (finalizing_at_db, completed_at, failed_at)
            }
        } else {
            if is_actually_terminal
                && (completed_at.is_some() || failed_at.is_some() || cancelled_at.is_some())
            {
                sqlx::query!(
                    r#"
                    UPDATE batches
                    SET completed_requests = $2,
                        failed_requests = $3,
                        canceled_requests = $4,
                        counts_frozen_at = NOW()
                    WHERE id = $1
                      AND retry_version = $5
                      AND counts_frozen_at IS NULL
                      AND (completed_at IS NOT NULL OR failed_at IS NOT NULL OR cancelled_at IS NOT NULL)
                      AND NOT EXISTS (
                          SELECT 1 FROM requests r
                          WHERE r.batch_id = $1
                            AND r.state NOT IN ('completed', 'failed', 'canceled')
                      )
                    "#,
                    *batch_id as Uuid,
                    completed_requests,
                    failed_requests,
                    canceled_actual,
                    retry_version,
                )
                .execute(self.write_executor())
                .await
                .map_err(|e| {
                    FusilladeError::Other(anyhow!("Failed to freeze batch counts: {}", e))
                })?;
            }
            (finalizing_at_db, completed_at, failed_at)
        };

        Ok(Batch {
            id: BatchId(row.get("id")),
            file_id: row.get::<Option<Uuid>, _>("file_id").map(FileId),
            created_at: row.get("created_at"),
            metadata: row.get("metadata"),
            service_tier: row.get("service_tier"),
            completion_window: row.get("completion_window"),
            endpoint: row.get("endpoint"),
            output_file_id: row.get::<Option<Uuid>, _>("output_file_id").map(FileId),
            error_file_id: row.get::<Option<Uuid>, _>("error_file_id").map(FileId),
            created_by: row.get("created_by"),
            expires_at: row.get("expires_at"),
            cancelling_at: row.get("cancelling_at"),
            errors: row.get("errors"),
            total_requests,
            pending_requests,
            in_progress_requests,
            completed_requests,
            failed_requests,
            canceled_requests,
            requests_started_at: row.get("requests_started_at"),
            finalizing_at,
            completed_at,
            failed_at,
            cancelled_at,
            deleted_at: row.get("deleted_at"),
            notification_sent_at: row.get("notification_sent_at"),
            api_key_id: row.get::<Option<Uuid>, _>("api_key_id"),
        })
    }

    pub async fn claim_batch_notifications(&self) -> Result<Vec<BatchNotification>> {
        let rows = sqlx::query_file!("src/postgres/sql/claim_batch_notifications.sql")
            .fetch_all(self.write_executor())
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to poll completed batches: {}", e))
            })?;

        Ok(rows
            .into_iter()
            .map(|row| BatchNotification {
                batch: Batch {
                    id: BatchId(row.id),
                    file_id: row.file_id.map(FileId),
                    endpoint: row.endpoint,
                    service_tier: row.service_tier,
                    completion_window: row.completion_window,
                    metadata: row.metadata,
                    output_file_id: row.output_file_id.map(FileId),
                    error_file_id: row.error_file_id.map(FileId),
                    created_by: row.created_by,
                    created_at: row.created_at,
                    expires_at: row.expires_at,
                    cancelling_at: row.cancelling_at,
                    errors: row.errors,
                    total_requests: row.total_requests,
                    requests_started_at: row.requests_started_at,
                    finalizing_at: row.finalizing_at,
                    completed_at: row.completed_at,
                    failed_at: row.failed_at,
                    cancelled_at: row.cancelled_at,
                    deleted_at: row.deleted_at,
                    notification_sent_at: row.notification_sent_at,
                    api_key_id: row.api_key_id,
                    pending_requests: row.pending_requests,
                    in_progress_requests: row.in_progress_requests,
                    completed_requests: row.completed_requests,
                    failed_requests: row.failed_requests,
                    canceled_requests: row.canceled_requests,
                },
                model: row.model.unwrap_or_default(),
                input_file_name: row.input_file_name,
                input_file_description: row.input_file_description,
            })
            .collect())
    }

    pub async fn failed_batch_sample_classes(
        &self,
        batch_ids: &[BatchId],
    ) -> Result<std::collections::HashMap<BatchId, String>> {
        if batch_ids.is_empty() {
            return Ok(Default::default());
        }
        let ids: Vec<Uuid> = batch_ids.iter().map(|id| **id).collect();
        let rows = sqlx::query(
            r#"
            SELECT t.id AS batch_id,
                   COALESCE(l.response_status, a.response_status) AS response_status,
                   COALESCE(l.error, a.error) AS error
            FROM batches t
            LEFT JOIN LATERAL (
                SELECT true AS found, r.response_status, r.error
                FROM requests r
                WHERE r.batch_id = t.id AND r.state = 'failed'
                LIMIT 1
            ) l ON true
            LEFT JOIN LATERAL (
                SELECT true AS found, x.response_status, x.error
                FROM batch_requests_archive x
                WHERE l.found IS NULL
                  AND x.archive_bucket = t.archive_bucket
                  AND x.batch_id = t.id
                  AND x.state = 'failed'
                LIMIT 1
            ) a ON true
            WHERE t.id = ANY($1)
              AND COALESCE(l.found, a.found, false)
            "#,
        )
        .bind(&ids)
        .fetch_all(self.read_executor())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to sample batch failures: {}", e)))?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let status = row.get::<Option<i16>, _>("response_status");
                let error = row.get::<Option<String>, _>("error");
                (
                    BatchId(row.get::<Uuid, _>("batch_id")),
                    failure_class(status, error.as_deref()),
                )
            })
            .collect())
    }

    async fn insert_template_batch(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        file_id: Uuid,
        templates: &[(RequestTemplateInput, i32)],
    ) -> Result<()> {
        if templates.is_empty() {
            return Ok(());
        }

        let custom_ids: Vec<Option<&str>> = templates
            .iter()
            .map(|(t, _)| t.custom_id.as_deref())
            .collect();
        let endpoints: Vec<&str> = templates.iter().map(|(t, _)| t.endpoint.as_str()).collect();
        let methods: Vec<&str> = templates.iter().map(|(t, _)| t.method.as_str()).collect();
        let paths: Vec<&str> = templates.iter().map(|(t, _)| t.path.as_str()).collect();
        let stored_bodies: Vec<std::borrow::Cow<'_, str>> = templates
            .iter()
            .map(|(t, _)| sanitize_outbound_body(&t.body))
            .collect();
        let bodies: Vec<&str> = stored_bodies.iter().map(AsRef::as_ref).collect();
        let models: Vec<&str> = templates.iter().map(|(t, _)| t.model.as_str()).collect();
        let line_numbers: Vec<i32> = templates.iter().map(|(_, line)| *line).collect();
        let body_byte_sizes: Vec<i64> = stored_bodies.iter().map(|b| b.len() as i64).collect();

        sqlx::query(
            r#"
            WITH inserted AS (
                INSERT INTO request_templates_g2 (
                    created_on, file_id, custom_id, endpoint, method, path,
                    body, model, line_number, body_byte_size
                )
                SELECT (transaction_timestamp() AT TIME ZONE 'UTC')::date,
                       $1, custom_id, endpoint, method, path, body, model,
                       line_number, body_byte_size
                FROM UNNEST(
                    $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
                    $7::text[], $8::int[], $9::bigint[]
                ) AS t(custom_id, endpoint, method, path, body, model,
                       line_number, body_byte_size)
                RETURNING id, created_on
            )
            INSERT INTO request_template_routes (template_id, week_start)
            SELECT id, date_trunc('week', created_on)::date FROM inserted
            "#,
        )
        .bind(file_id)
        .bind(&custom_ids as &[Option<&str>])
        .bind(&endpoints as &[&str])
        .bind(&methods as &[&str])
        .bind(&paths as &[&str])
        .bind(&bodies as &[&str])
        .bind(&models as &[&str])
        .bind(&line_numbers as &[i32])
        .bind(&body_byte_sizes as &[i64])
        .execute(&mut **tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to batch insert templates: {}", e)))?;

        Ok(())
    }

    async fn stream_request_templates(
        pools: P,
        retry_config: crate::DbRetryConfig,
        query_schema: Option<Arc<str>>,
        file_id: FileId,
        offset: i64,
        search: Option<String>,
        tx: mpsc::Sender<Result<FileContentItem>>,
    ) {
        const BATCH_SIZE: i64 = 1000;
        let mut last_line_number: i32 = -1;
        let mut is_first_batch = true;
        let search_pattern = search.map(|s| format!("%{}%", s.to_lowercase()));

        loop {
            let (line_filter, offset_val) = if is_first_batch {
                (-1i32, offset)
            } else {
                (last_line_number, 0i64)
            };
            is_first_batch = false;

            let template_batch = sqlx::query!(
                r#"
                SELECT custom_id,
                       endpoint AS "endpoint!",
                       method AS "method!",
                       path AS "path!",
                       body AS "body!",
                       model AS "model!",
                       line_number AS "line_number!"
                FROM request_templates_all
                WHERE file_id = $1 AND ($2 = -1 OR line_number > $2)
                  AND ($5::text IS NULL OR LOWER(custom_id) LIKE $5)
                ORDER BY line_number ASC
                OFFSET $3
                LIMIT $4
                "#,
                *file_id as Uuid,
                line_filter,
                offset_val,
                BATCH_SIZE,
                search_pattern.as_deref(),
            )
            .fetch_all(
                crate::db::RetryingPgPool::new(&pools.read(), &retry_config)
                    .with_schema(query_schema.clone()),
            )
            .await;

            match template_batch {
                Ok(templates) => {
                    if templates.is_empty() {
                        break;
                    }

                    tracing::debug!(
                        "Fetched batch of {} templates, line_numbers {}-{}",
                        templates.len(),
                        templates.first().map(|r| r.line_number).unwrap_or(0),
                        templates.last().map(|r| r.line_number).unwrap_or(0)
                    );

                    for row in templates {
                        last_line_number = row.line_number;

                        let template = RequestTemplateInput {
                            custom_id: row.custom_id,
                            endpoint: row.endpoint,
                            method: row.method,
                            path: row.path,
                            body: row.body,
                            model: row.model,
                        };
                        if tx
                            .send(Ok(FileContentItem::Template(template)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(FusilladeError::Other(anyhow!(
                            "Failed to fetch template batch: {}",
                            e
                        ))))
                        .await;
                    return;
                }
            }
        }
    }

    async fn stream_batch_output(
        pools: P,
        retry_config: crate::DbRetryConfig,
        query_schema: Option<Arc<str>>,
        file_id: FileId,
        offset: i64,
        search: Option<String>,
        tx: mpsc::Sender<Result<FileContentItem>>,
    ) {
        let batch_result = sqlx::query!(
            r#"
            SELECT id,
                   COALESCE(
                       archive_bucket,
                       date_trunc('week', created_at AT TIME ZONE 'UTC')::date
                   ) AS "bucket!"
            FROM batches
            WHERE output_file_id = $1
            "#,
            *file_id as Uuid,
        )
        .fetch_one(
            crate::db::RetryingPgPool::new(&pools.read(), &retry_config)
                .with_schema(query_schema.clone()),
        )
        .await;

        let (batch_id, bucket) = match batch_result {
            Ok(row) => (row.id, row.bucket),
            Err(e) => {
                let _ = tx
                    .send(Err(FusilladeError::Other(anyhow!(
                        "Failed to find batch for output file: {}",
                        e
                    ))))
                    .await;
                return;
            }
        };

        const BATCH_SIZE: i64 = 1000;
        let mut last_completed_at: Option<chrono::DateTime<chrono::Utc>> = None;
        let mut last_id: Uuid = Uuid::nil();
        let mut is_first_batch = true;
        let search_pattern = search.map(|s| format!("%{}%", s.to_lowercase()));

        loop {
            let (cursor_time, cursor_id, offset_val) = if is_first_batch {
                (None, Uuid::nil(), offset)
            } else {
                (last_completed_at, last_id, 0i64)
            };
            is_first_batch = false;

            let request_batch = sqlx::query!(
                r#"
                SELECT id AS "id!", custom_id, response_status, response_body, completed_at
                FROM (
                    SELECT id, custom_id, response_status, response_body, completed_at
                    FROM requests
                    WHERE batch_id = $1
                      AND state = 'completed'
                      AND ($2::TIMESTAMPTZ IS NULL OR completed_at > $2 OR (completed_at = $2 AND id > $3))
                      AND ($6::text IS NULL OR LOWER(custom_id) LIKE $6)
                    UNION ALL
                    SELECT id, custom_id, response_status, response_body, completed_at
                    FROM batch_requests_archive
                    WHERE archive_bucket = $7
                      AND batch_id = $1
                      AND state = 'completed'
                      AND ($2::TIMESTAMPTZ IS NULL OR completed_at > $2 OR (completed_at = $2 AND id > $3))
                      AND ($6::text IS NULL OR LOWER(custom_id) LIKE $6)
                ) u
                ORDER BY completed_at ASC, id ASC
                OFFSET $4
                LIMIT $5
                "#,
                batch_id,
                cursor_time,
                cursor_id,
                offset_val,
                BATCH_SIZE,
                search_pattern.as_deref(),
                bucket,
            )
            .fetch_all(crate::db::RetryingPgPool::new(&pools.read(), &retry_config).with_schema(query_schema.clone()))
            .await;

            match request_batch {
                Ok(requests) => {
                    if requests.is_empty() {
                        break;
                    }

                    tracing::debug!("Fetched batch of {} completed requests", requests.len());

                    for row in requests {
                        last_completed_at = row.completed_at;
                        last_id = row.id;

                        let response_body: serde_json::Value = match &row.response_body {
                            Some(body) => match serde_json::from_str(body) {
                                Ok(json) => json,
                                Err(e) => {
                                    tracing::warn!(
                                        line = e.line(),
                                        column = e.column(),
                                        category = ?e.classify(),
                                        "Failed to parse response body as JSON"
                                    );
                                    serde_json::Value::String(body.to_string())
                                }
                            },
                            None => serde_json::Value::Null,
                        };

                        let output_item = BatchOutputItem {
                            id: format!("batch_req_{}", row.id),
                            custom_id: row.custom_id,
                            response: BatchResponseDetails {
                                status_code: row.response_status.unwrap_or(200),
                                request_id: None,
                                body: response_body,
                            },
                            error: None,
                        };

                        if tx
                            .send(Ok(FileContentItem::Output(output_item)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(FusilladeError::Other(anyhow!(
                            "Failed to fetch completed requests: {}",
                            e
                        ))))
                        .await;
                    return;
                }
            }
        }
    }

    async fn stream_batch_error(
        pools: P,
        retry_config: crate::DbRetryConfig,
        query_schema: Option<Arc<str>>,
        file_id: FileId,
        offset: i64,
        search: Option<String>,
        tx: mpsc::Sender<Result<FileContentItem>>,
    ) {
        let batch_result = sqlx::query!(
            r#"
            SELECT id, expires_at,
                   COALESCE(
                       archive_bucket,
                       date_trunc('week', created_at AT TIME ZONE 'UTC')::date
                   ) AS "bucket!"
            FROM batches
            WHERE error_file_id = $1
            "#,
            *file_id as Uuid,
        )
        .fetch_one(
            crate::db::RetryingPgPool::new(&pools.read(), &retry_config)
                .with_schema(query_schema.clone()),
        )
        .await;

        let (batch_id, bucket, _expires_at) = match batch_result {
            Ok(row) => (row.id, row.bucket, row.expires_at),
            Err(e) => {
                let _ = tx
                    .send(Err(FusilladeError::Other(anyhow!(
                        "Failed to find batch for error file: {}",
                        e
                    ))))
                    .await;
                return;
            }
        };

        const BATCH_SIZE: i64 = 1000;
        let mut last_failed_at: Option<chrono::DateTime<chrono::Utc>> = None;
        let mut last_id: Uuid = Uuid::nil();
        let mut is_first_batch = true;
        let search_pattern = search.map(|s| format!("%{}%", s.to_lowercase()));

        loop {
            let (cursor_time, cursor_id, offset_val) = if is_first_batch {
                (None, Uuid::nil(), offset)
            } else {
                (last_failed_at, last_id, 0i64)
            };
            is_first_batch = false;

            let mut query_builder = QueryBuilder::new(
                r#"
                SELECT id, custom_id, error, failed_at FROM (
                SELECT id, custom_id, error, failed_at
                FROM requests
                WHERE batch_id = "#,
            );
            for source in ["live", "archive"] {
                if source == "archive" {
                    query_builder.push(
                        r#"
                UNION ALL
                SELECT id, custom_id, error, failed_at
                FROM batch_requests_archive
                WHERE archive_bucket = "#,
                    );
                    query_builder.push_bind(bucket);
                    query_builder.push(" AND batch_id = ");
                }
                query_builder.push_bind(batch_id);
                query_builder.push(" AND state = 'failed' AND (");
                query_builder.push_bind(cursor_time);
                query_builder.push("::TIMESTAMPTZ IS NULL OR failed_at > ");
                query_builder.push_bind(cursor_time);
                query_builder.push(" OR (failed_at = ");
                query_builder.push_bind(cursor_time);
                query_builder.push(" AND id > ");
                query_builder.push_bind(cursor_id);
                query_builder.push(")) AND (");
                query_builder.push_bind(search_pattern.as_deref());
                query_builder.push("::text IS NULL OR LOWER(custom_id) LIKE ");
                query_builder.push_bind(search_pattern.as_deref());
                query_builder.push(")");
            }
            query_builder.push(" ) u ORDER BY failed_at ASC, id ASC OFFSET ");
            query_builder.push_bind(offset_val);
            query_builder.push(" LIMIT ");
            query_builder.push_bind(BATCH_SIZE);

            let request_batch = query_builder
                .build()
                .fetch_all(
                    crate::db::RetryingPgPool::new(&pools.read(), &retry_config)
                        .with_schema(query_schema.clone()),
                )
                .await;

            match request_batch {
                Ok(requests) => {
                    if requests.is_empty() {
                        break;
                    }

                    tracing::debug!("Fetched batch of {} failed requests", requests.len());

                    for row in requests {
                        let id: Uuid = row.get("id");
                        let custom_id: Option<String> = row.get("custom_id");
                        let error: Option<String> = row.get("error");
                        let failed_at: Option<DateTime<Utc>> = row.get("failed_at");

                        last_failed_at = failed_at;
                        last_id = id;

                        let error_item = BatchErrorItem {
                            id: format!("batch_req_{}", id),
                            custom_id,
                            response: None,
                            error: BatchErrorDetails {
                                code: None,
                                message: error.unwrap_or_else(|| "Unknown error".to_string()),
                            },
                        };

                        if tx
                            .send(Ok(FileContentItem::Error(error_item)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(FusilladeError::Other(anyhow!(
                            "Failed to fetch failed requests: {}",
                            e
                        ))))
                        .await;
                    return;
                }
            }
        }
    }

    async fn stream_batch_results(
        pools: P,
        retry_config: crate::DbRetryConfig,
        query_schema: Option<Arc<str>>,
        batch_id: BatchId,
        offset: i64,
        filters: (Option<String>, Option<String>),
        tx: mpsc::Sender<Result<crate::batch::BatchResultItem>>,
    ) {
        use crate::batch::{BatchResultItem, BatchResultStatus};
        let (search, status) = filters;

        let (file_id, archive_bucket) = match sqlx::query!(
            r#"
            SELECT
                b.archive_bucket,
                CASE WHEN f.status <> 'expired'
                       AND (f.expires_at IS NULL OR f.expires_at > NOW())
                       AND NOT EXISTS (
                           SELECT 1 FROM request_template_buckets bucket
                           WHERE bucket.week_start = date_trunc('week', f.created_at AT TIME ZONE 'UTC')::date
                             AND bucket.state <> 'active'
                       )
                     THEN f.id
                END AS input_file_id
            FROM batches b
            LEFT JOIN files f ON f.id = b.file_id
            WHERE b.id = $1 AND b.deleted_at IS NULL
            "#,
            *batch_id as Uuid,
        )
        .fetch_optional(
            crate::db::RetryingPgPool::new(&pools.read(), &retry_config)
                .with_schema(query_schema.clone()),
        )
        .await
        {
            Ok(Some(row)) => (row.input_file_id, row.archive_bucket),
            Ok(None) => {
                let _ = tx
                    .send(Err(FusilladeError::Other(anyhow!("Batch not found"))))
                    .await;
                return;
            }
            Err(e) => {
                let _ = tx
                    .send(Err(FusilladeError::Other(anyhow!(
                        "Failed to fetch batch: {}",
                        e
                    ))))
                    .await;
                return;
            }
        };

        const BATCH_SIZE: i64 = 1000;
        let mut last_line_number: i32 = -1;
        let mut is_first_batch = true;
        let search_pattern = search.map(|s| format!("%{}%", s.to_lowercase()));

        let state_filter: Option<Vec<String>> = status.map(|s| match s.as_str() {
            "in_progress" => vec!["claimed".to_string(), "processing".to_string()],
            other => vec![other.to_string()],
        });

        let mut last_created_at: Option<DateTime<Utc>> = None;
        let mut last_id: Uuid = Uuid::nil();

        loop {
            let (line_filter, cursor_time, cursor_id, offset_val) = if is_first_batch {
                (-1i32, None, Uuid::nil(), offset)
            } else {
                (last_line_number, last_created_at, last_id, 0i64)
            };
            is_first_batch = false;

            let push_requests_union = |qb: &mut QueryBuilder<'_, sqlx::Postgres>| {
                qb.push(
                    r#"
                    SELECT id, custom_id, model, state, response_body, error, template_id, created_at
                    FROM requests
                    WHERE batch_id = "#,
                );
                qb.push_bind(*batch_id as Uuid);
                qb.push(
                    r#"
                    UNION ALL
                    SELECT id, custom_id, model, state, response_body, error, template_id, created_at
                    FROM batch_requests_archive
                    WHERE "#,
                );
                qb.push_bind(archive_bucket);
                qb.push(
                    r#"::date IS NOT NULL
                      AND archive_bucket = "#,
                );
                qb.push_bind(archive_bucket);
                qb.push(" AND batch_id = ");
                qb.push_bind(*batch_id as Uuid);
            };

            let mut query_builder = QueryBuilder::new(
                r#"
                SELECT
                    r.id,
                    r.custom_id,
                    r.model,
                    r.state,
                    t.body as input_body,
                    r.response_body,
                    r.error,
                    t.line_number,
                    r.created_at
                FROM "#,
            );
            if let Some(file_id) = file_id {
                query_builder.push("request_templates_all t JOIN (");
                push_requests_union(&mut query_builder);
                query_builder.push(") r ON r.template_id = t.id WHERE t.file_id = ");
                query_builder.push_bind(file_id);
                query_builder.push(" AND (");
                query_builder.push_bind(line_filter);
                query_builder.push(" = -1 OR t.line_number > ");
                query_builder.push_bind(line_filter);
                query_builder.push(")");
            } else {
                query_builder.push("(");
                push_requests_union(&mut query_builder);
                query_builder.push(
                    r#") r
                CROSS JOIN (SELECT NULL::text AS body, NULL::integer AS line_number) t
                WHERE ("#,
                );
                query_builder.push_bind(cursor_time);
                query_builder.push("::timestamptz IS NULL OR r.created_at > ");
                query_builder.push_bind(cursor_time);
                query_builder.push(" OR (r.created_at = ");
                query_builder.push_bind(cursor_time);
                query_builder.push(" AND r.id > ");
                query_builder.push_bind(cursor_id);
                query_builder.push("))");
            }
            query_builder.push(" AND (");
            query_builder.push_bind(search_pattern.as_deref());
            query_builder.push("::text IS NULL OR LOWER(r.custom_id) LIKE ");
            query_builder.push_bind(search_pattern.as_deref());
            query_builder.push(") AND (");
            query_builder.push_bind(state_filter.as_deref());
            query_builder.push("::text[] IS NULL OR r.state = ANY(");
            query_builder.push_bind(state_filter.as_deref());
            query_builder.push("))");
            if file_id.is_some() {
                query_builder.push(" ORDER BY t.line_number ASC OFFSET ");
            } else {
                query_builder.push(" ORDER BY r.created_at ASC, r.id ASC OFFSET ");
            }
            query_builder.push_bind(offset_val);
            query_builder.push(" LIMIT ");
            query_builder.push_bind(BATCH_SIZE);

            let request_batch = query_builder
                .build()
                .fetch_all(
                    crate::db::RetryingPgPool::new(&pools.read(), &retry_config)
                        .with_schema(query_schema.clone()),
                )
                .await;

            match request_batch {
                Ok(requests) => {
                    if requests.is_empty() {
                        break;
                    }

                    tracing::debug!("Fetched batch of {} results", requests.len());

                    for row in requests {
                        let line_number: Option<i32> = row.get("line_number");
                        let created_at: DateTime<Utc> = row.get("created_at");
                        let id: Uuid = row.get("id");
                        last_line_number = line_number.unwrap_or(last_line_number);
                        last_created_at = Some(created_at);
                        last_id = id;

                        let input_body_str: Option<String> = row.get("input_body");
                        let response_body_opt: Option<String> = row.get("response_body");
                        let state: String = row.get("state");
                        let custom_id: Option<String> = row.get("custom_id");
                        let model: String = row.get("model");
                        let error: Option<String> = row.get("error");

                        let input_body: serde_json::Value = match input_body_str {
                            Some(s) => {
                                serde_json::from_str(&s).unwrap_or(serde_json::Value::String(s))
                            }
                            None => serde_json::Value::Null,
                        };

                        let response_body: Option<serde_json::Value> =
                            response_body_opt.as_ref().map(|body| {
                                serde_json::from_str(body)
                                    .unwrap_or_else(|_| serde_json::Value::String(body.to_string()))
                            });

                        let status = match state.as_str() {
                            "completed" => BatchResultStatus::Completed,
                            "failed" => BatchResultStatus::Failed,
                            "pending" => BatchResultStatus::Pending,
                            "claimed" | "processing" => BatchResultStatus::InProgress,
                            _ => BatchResultStatus::Pending,
                        };

                        let result_item = BatchResultItem {
                            id: id.to_string(),
                            custom_id,
                            model,
                            input_body,
                            response_body,
                            error,
                            status,
                        };

                        if tx.send(Ok(result_item)).await.is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(FusilladeError::Other(anyhow!(
                            "Failed to fetch batch results: {}",
                            e
                        ))))
                        .await;
                    return;
                }
            }
        }
    }

    async fn create_virtual_output_file(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        batch_id: Uuid,
        created_by: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<Uuid> {
        let name = format!("batch-{}-output.jsonl", batch_id);
        let description = format!("Output file for batch {}", batch_id);

        let file_id = sqlx::query_scalar!(
            r#"
            INSERT INTO files (name, description, size_bytes, size_finalized, status, purpose, uploaded_by, expires_at)
            VALUES ($1, $2, 0, FALSE, 'processed', 'batch_output', NULLIF($3, ''), $4)
            RETURNING id
            "#,
            name,
            description,
            created_by,
            expires_at,
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to create output file: {}", e)))?;

        Ok(file_id)
    }

    async fn create_virtual_error_file(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        batch_id: Uuid,
        created_by: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<Uuid> {
        let name = format!("batch-{}-error.jsonl", batch_id);
        let description = format!("Error file for batch {}", batch_id);

        let file_id = sqlx::query_scalar!(
            r#"
            INSERT INTO files (name, description, size_bytes, size_finalized, status, purpose, uploaded_by, expires_at)
            VALUES ($1, $2, 0, FALSE, 'processed', 'batch_error', NULLIF($3, ''), $4)
            RETURNING id
            "#,
            name,
            description,
            created_by,
            expires_at,
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to create error file: {}", e)))?;

        Ok(file_id)
    }
}

const PURGE_DELETED_BATCH_REQUESTS: &str = r#"
            DELETE FROM requests
            WHERE id IN (
                SELECT r.id
                FROM (SELECT id FROM batches
                      WHERE deleted_at IS NOT NULL
                        AND retention_expired_at IS NULL) b,
                LATERAL (
                    SELECT id FROM requests
                    WHERE batch_id = b.id
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                ) r
                LIMIT $1
            )
            "#;

const PURGE_DELETED_BATCH_ARCHIVED_REQUESTS: &str = r#"
            DELETE FROM batch_requests_archive
            WHERE (id, archive_bucket) IN (
                SELECT a.id, a.archive_bucket
                FROM (SELECT id, archive_bucket FROM batches
                      WHERE deleted_at IS NOT NULL
                        AND retention_expired_at IS NULL
                        AND archive_bucket IS NOT NULL) b,
                LATERAL (
                    SELECT id, archive_bucket FROM batch_requests_archive
                    WHERE archive_bucket = b.archive_bucket AND batch_id = b.id
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                ) a
                LIMIT $1
            )
            "#;

const PURGE_DELETED_FILE_TEMPLATES: &str = r#"
            WITH doomed AS (
                SELECT template.created_on, template.id
                FROM (SELECT id FROM files
                      WHERE deleted_at IS NOT NULL
                        AND retention_expired_at IS NULL) file,
                LATERAL (
                    SELECT created_on, id
                    FROM request_templates_g2
                    WHERE file_id = file.id
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                ) template
                LIMIT $1
            ), removed AS (
                DELETE FROM request_templates_g2 template
                USING doomed
                WHERE template.created_on = doomed.created_on
                  AND template.id = doomed.id
                RETURNING template.id
            )
            DELETE FROM request_template_routes route
            USING removed
            WHERE route.template_id = removed.id
            "#;

#[async_trait]
impl<P: PoolProvider> DaemonStorage for PostgresRequestManager<P> {
    fn supports_retained_response_lifecycle(&self) -> bool {
        true
    }

    fn supports_retained_response_partition_retirement(&self) -> bool {
        self.partition_maintenance_pool.is_some() && self.partition_maintenance_attested
    }

    fn supports_retained_response_route_cleanup(&self) -> bool {
        self.retained_response_fence_seconds.is_some()
    }

    async fn retire_expired_response_partition(
        &self,
        select_new: bool,
    ) -> Result<RetainedResponseRetirementOutcome> {
        retained_response_retirement::retire_expired_response_partition(self, select_new).await
    }

    async fn cleanup_retained_response_routes(&self, limit: i64) -> Result<u64> {
        retained_response_retirement::cleanup_retained_response_routes(self, limit).await
    }

    async fn cleanup_expired_response_fences(&self, limit: i64) -> Result<u64> {
        retained_response_retirement::cleanup_expired_response_fences(self, limit).await
    }

    async fn retire_expired_batch_archive_partition(
        &self,
        select_new: bool,
        retention_days: i32,
    ) -> Result<RetainedResponseRetirementOutcome> {
        batch_archive_retirement::retire_expired_batch_archive_partition(
            self,
            select_new,
            retention_days,
        )
        .await
    }

    async fn retire_expired_template_partition(
        &self,
        select_new: bool,
        retention_days: i32,
    ) -> Result<RetainedResponseRetirementOutcome> {
        template_retirement::retire_expired_template_partition(self, select_new, retention_days)
            .await
    }

    async fn cleanup_retired_template_routes(&self, limit: i64) -> Result<u64> {
        template_retirement::cleanup_retired_template_routes(self, limit).await
    }

    async fn retained_response_archive_index_ready(&self) -> Result<bool> {
        sqlx::query_scalar("SELECT retained_response_archive_index_ready(current_schema())")
            .fetch_one(self.read_executor())
            .await
            .map_err(|error| {
                FusilladeError::Other(anyhow!(
                    "Failed to inspect retained response archive index readiness: {}",
                    error
                ))
            })
    }

    async fn ensure_retained_response_partitions(
        &self,
        policy: &RetentionPolicy,
        days_ahead: i32,
    ) -> Result<RetainedResponsePartitionRunway> {
        policy.validate().map_err(FusilladeError::ValidationError)?;
        if days_ahead < 0 {
            return Err(FusilladeError::ValidationError(
                "retained response partition runway must not be negative".to_string(),
            ));
        }
        let Some(max_retention_seconds) = policy
            .batchless_seconds_by_service_tier
            .values()
            .copied()
            .max()
        else {
            return Ok(RetainedResponsePartitionRunway::default());
        };

        let observed_at: DateTime<Utc> = sqlx::query_scalar("SELECT statement_timestamp()")
            .fetch_one(self.write_executor())
            .await
            .map_err(|error| {
                FusilladeError::Other(anyhow!(
                    "Failed to read the retained response partition clock: {}",
                    error
                ))
            })?;
        let first_delete_on = observed_at.date_naive().succ_opt().ok_or_else(|| {
            FusilladeError::ValidationError(
                "retained response partition start is out of range".to_string(),
            )
        })?;
        let retention_horizon = RetentionPolicy::delete_on(observed_at, max_retention_seconds)
            .map_err(FusilladeError::ValidationError)?;
        let last_delete_on = retention_horizon
            .checked_add_signed(chrono::Duration::days(i64::from(days_ahead)))
            .ok_or_else(|| {
                FusilladeError::ValidationError(
                    "retained response partition runway is out of range".to_string(),
                )
            })?;
        let offsets = (last_delete_on - first_delete_on).num_days();
        let offsets = i32::try_from(offsets).map_err(|_| {
            FusilladeError::ValidationError(
                "retained response partition runway is too large".to_string(),
            )
        })?;

        let created: i32 = sqlx::query_scalar("SELECT ensure_retained_response_partitions($1, $2)")
            .bind(first_delete_on)
            .bind(offsets)
            .fetch_one(self.write_executor())
            .await
            .map_err(|error| {
                FusilladeError::Other(anyhow!(
                    "Failed to ensure retained response partitions: {}",
                    error
                ))
            })?;

        let contiguous: i64 = sqlx::query_scalar(
            r#"
            WITH expected AS (
                SELECT day.delete_on::date AS delete_on
                FROM generate_series($1::date, $2::date, INTERVAL '1 day')
                    AS day(delete_on)
            ), readiness AS (
                SELECT expected.delete_on,
                       EXISTS (
                           SELECT 1
                           FROM retained_response_buckets bucket
                           JOIN pg_class child ON child.oid = bucket.partition_oid
                           JOIN pg_namespace namespace ON namespace.oid = child.relnamespace
                           JOIN pg_inherits inheritance ON inheritance.inhrelid = child.oid
                           WHERE bucket.delete_on = expected.delete_on
                             AND bucket.state = 'active'
                             AND bucket.partition_schema = current_schema()
                             AND bucket.partition_table = 'retained_response_objects_d'
                                 || to_char(expected.delete_on, 'YYYYMMDD')
                             AND namespace.nspname = bucket.partition_schema
                             AND child.relname = bucket.partition_table
                             AND inheritance.inhparent = 'retained_response_objects'::regclass
                             AND NOT inheritance.inhdetachpending
                             AND pg_get_expr(child.relpartbound, child.oid) = format(
                                 'FOR VALUES FROM (%L) TO (%L)',
                                 expected.delete_on,
                                 expected.delete_on + 1
                             )
                       ) AS ready
                FROM expected
            ), prefix AS (
                SELECT bool_and(ready) OVER (
                           ORDER BY delete_on ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                       ) AS contiguous
                FROM readiness
            )
            SELECT COUNT(*) FILTER (WHERE contiguous)::BIGINT FROM prefix
            "#,
        )
        .bind(first_delete_on)
        .bind(last_delete_on)
        .fetch_one(self.write_executor())
        .await
        .map_err(|error| {
            FusilladeError::Other(anyhow!(
                "Failed to inspect retained response partition runway: {}",
                error
            ))
        })?;

        Ok(RetainedResponsePartitionRunway {
            created: i64::from(created),
            contiguous_ahead: contiguous,
            required: i64::from(offsets) + 1,
        })
    }

    async fn archive_terminal_batchless_responses(
        &self,
        policy: &RetentionPolicy,
        cutoffs: &RetainedResponseArchiveCutoffs,
        max_groups: i64,
        max_bytes: i64,
    ) -> Result<RetainedResponseArchiveOutcome> {
        retained_response::archive_terminal_batchless_responses(
            self, policy, cutoffs, max_groups, max_bytes,
        )
        .await
    }

    async fn archive_overdue_batchless_responses(
        &self,
        policy: &RetentionPolicy,
        cutoffs: &RetainedResponseArchiveCutoffs,
        max_groups: i64,
        max_bytes: i64,
        concurrency: usize,
    ) -> Result<RetainedResponseArchiveOutcome> {
        retained_response::archive_overdue_batchless_responses(
            self,
            policy,
            cutoffs,
            max_groups,
            max_bytes,
            concurrency,
        )
        .await
    }

    async fn persist_daemon<T: DaemonState + Clone>(&self, record: &DaemonRecord<T>) -> Result<()>
    where
        AnyDaemonRecord: From<DaemonRecord<T>>,
    {
        let any_daemon = AnyDaemonRecord::from(record.clone());

        match any_daemon {
            AnyDaemonRecord::Initializing(daemon) => {
                sqlx::query!(
                    r#"
                    INSERT INTO daemons (
                        id, status, hostname, pid, version, config_snapshot,
                        started_at, last_heartbeat, stopped_at,
                        requests_processed, requests_failed, requests_in_flight
                    ) VALUES ($1, 'initializing', $2, $3, $4, $5, $6, NULL, NULL, 0, 0, 0)
                    ON CONFLICT (id) DO UPDATE SET
                        status = 'initializing',
                        started_at = $6,
                        updated_at = NOW()
                    "#,
                    *daemon.data.id as Uuid,
                    daemon.data.hostname,
                    daemon.data.pid,
                    daemon.data.version,
                    daemon.data.config_snapshot,
                    daemon.state.started_at,
                )
                .execute(self.write_executor())
                .await
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to persist daemon: {}", e)))?;
            }
            AnyDaemonRecord::Running(daemon) => {
                sqlx::query!(
                    r#"
                    INSERT INTO daemons (
                        id, status, hostname, pid, version, config_snapshot,
                        started_at, last_heartbeat, stopped_at,
                        requests_processed, requests_failed, requests_in_flight
                    ) VALUES ($1, 'running', $2, $3, $4, $5, $6, $7, NULL, $8, $9, $10)
                    ON CONFLICT (id) DO UPDATE SET
                        status = 'running',
                        last_heartbeat = $7,
                        requests_processed = $8,
                        requests_failed = $9,
                        requests_in_flight = $10,
                        updated_at = NOW()
                    "#,
                    *daemon.data.id as Uuid,
                    daemon.data.hostname,
                    daemon.data.pid,
                    daemon.data.version,
                    daemon.data.config_snapshot,
                    daemon.state.started_at,
                    daemon.state.last_heartbeat,
                    daemon.state.stats.requests_processed as i64,
                    daemon.state.stats.requests_failed as i64,
                    daemon.state.stats.requests_in_flight as i32,
                )
                .execute(self.write_executor())
                .await
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to persist daemon: {}", e)))?;
            }
            AnyDaemonRecord::Dead(daemon) => {
                sqlx::query!(
                    r#"
                    INSERT INTO daemons (
                        id, status, hostname, pid, version, config_snapshot,
                        started_at, last_heartbeat, stopped_at,
                        requests_processed, requests_failed, requests_in_flight
                    ) VALUES ($1, 'dead', $2, $3, $4, $5, $6, NULL, $7, $8, $9, $10)
                    ON CONFLICT (id) DO UPDATE SET
                        status = 'dead',
                        stopped_at = $7,
                        requests_processed = $8,
                        requests_failed = $9,
                        requests_in_flight = $10,
                        updated_at = NOW()
                    "#,
                    *daemon.data.id as Uuid,
                    daemon.data.hostname,
                    daemon.data.pid,
                    daemon.data.version,
                    daemon.data.config_snapshot,
                    daemon.state.started_at,
                    daemon.state.stopped_at,
                    daemon.state.final_stats.requests_processed as i64,
                    daemon.state.final_stats.requests_failed as i64,
                    daemon.state.final_stats.requests_in_flight as i32,
                )
                .execute(self.write_executor())
                .await
                .map_err(|e| FusilladeError::Other(anyhow!("Failed to persist daemon: {}", e)))?;
            }
        }

        Ok(())
    }

    async fn get_daemon(&self, daemon_id: DaemonId) -> Result<AnyDaemonRecord> {
        let row = sqlx::query!(
            r#"
            SELECT
                id, status, hostname, pid, version, config_snapshot,
                started_at, last_heartbeat, stopped_at,
                requests_processed, requests_failed, requests_in_flight
            FROM daemons
            WHERE id = $1
            "#,
            *daemon_id as Uuid,
        )
        .fetch_one(self.read_executor())
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => FusilladeError::Other(anyhow!("Daemon not found")),
            _ => FusilladeError::Other(anyhow!("Failed to fetch daemon: {}", e)),
        })?;

        let data = DaemonData {
            id: DaemonId(row.id),
            hostname: row.hostname,
            pid: row.pid,
            version: row.version,
            config_snapshot: row.config_snapshot,
        };

        let any_daemon = match row.status.as_str() {
            "initializing" => AnyDaemonRecord::Initializing(DaemonRecord {
                data,
                state: Initializing {
                    started_at: row.started_at,
                },
            }),
            "running" => AnyDaemonRecord::Running(DaemonRecord {
                data,
                state: Running {
                    started_at: row.started_at,
                    last_heartbeat: row.last_heartbeat.ok_or_else(|| {
                        FusilladeError::Other(anyhow!("Running daemon missing last_heartbeat"))
                    })?,
                    stats: crate::daemon::DaemonStats {
                        requests_processed: row.requests_processed as u64,
                        requests_failed: row.requests_failed as u64,
                        requests_in_flight: row.requests_in_flight as usize,
                    },
                },
            }),
            "dead" => AnyDaemonRecord::Dead(DaemonRecord {
                data,
                state: Dead {
                    started_at: row.started_at,
                    stopped_at: row.stopped_at.ok_or_else(|| {
                        FusilladeError::Other(anyhow!("Dead daemon missing stopped_at"))
                    })?,
                    final_stats: crate::daemon::DaemonStats {
                        requests_processed: row.requests_processed as u64,
                        requests_failed: row.requests_failed as u64,
                        requests_in_flight: row.requests_in_flight as usize,
                    },
                },
            }),
            _ => {
                return Err(FusilladeError::Other(anyhow!(
                    "Unknown daemon status: {}",
                    row.status
                )));
            }
        };

        Ok(any_daemon)
    }

    async fn list_daemons(
        &self,
        status_filter: Option<DaemonStatus>,
    ) -> Result<Vec<AnyDaemonRecord>> {
        let status_str = status_filter.as_ref().map(|s| s.as_str());

        let rows = sqlx::query!(
            r#"
            SELECT
                id, status, hostname, pid, version, config_snapshot,
                started_at, last_heartbeat, stopped_at,
                requests_processed, requests_failed, requests_in_flight
            FROM daemons
            WHERE ($1::text IS NULL OR status = $1)
            ORDER BY created_at DESC
            "#,
            status_str,
        )
        .fetch_all(self.read_executor())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to list daemons: {}", e)))?;

        let mut daemons = Vec::new();

        for row in rows {
            let data = DaemonData {
                id: DaemonId(row.id),
                hostname: row.hostname,
                pid: row.pid,
                version: row.version,
                config_snapshot: row.config_snapshot,
            };

            let any_daemon = match row.status.as_str() {
                "initializing" => AnyDaemonRecord::Initializing(DaemonRecord {
                    data,
                    state: Initializing {
                        started_at: row.started_at,
                    },
                }),
                "running" => {
                    if let Some(last_heartbeat) = row.last_heartbeat {
                        AnyDaemonRecord::Running(DaemonRecord {
                            data,
                            state: Running {
                                started_at: row.started_at,
                                last_heartbeat,
                                stats: crate::daemon::DaemonStats {
                                    requests_processed: row.requests_processed as u64,
                                    requests_failed: row.requests_failed as u64,
                                    requests_in_flight: row.requests_in_flight as usize,
                                },
                            },
                        })
                    } else {
                        continue;
                    }
                }
                "dead" => {
                    if let Some(stopped_at) = row.stopped_at {
                        AnyDaemonRecord::Dead(DaemonRecord {
                            data,
                            state: Dead {
                                started_at: row.started_at,
                                stopped_at,
                                final_stats: crate::daemon::DaemonStats {
                                    requests_processed: row.requests_processed as u64,
                                    requests_failed: row.requests_failed as u64,
                                    requests_in_flight: row.requests_in_flight as usize,
                                },
                            },
                        })
                    } else {
                        continue;
                    }
                }
                _ => {
                    continue;
                }
            };

            daemons.push(any_daemon);
        }

        Ok(daemons)
    }

    async fn purge_orphaned_rows(&self, batch_size: i64) -> Result<u64> {
        let requests_deleted = sqlx::query(PURGE_DELETED_BATCH_REQUESTS)
            .bind(batch_size)
            .execute(self.write_executor())
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to purge orphaned requests: {}", e))
            })?
            .rows_affected() as i64;

        let archived_deleted = sqlx::query(PURGE_DELETED_BATCH_ARCHIVED_REQUESTS)
            .bind(batch_size)
            .execute(self.write_executor())
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to purge orphaned archived requests: {}", e))
            })?
            .rows_affected() as i64;

        let templates_deleted = sqlx::query(PURGE_DELETED_FILE_TEMPLATES)
            .bind(batch_size)
            .execute(self.write_executor())
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to purge orphaned request templates: {e}"))
            })?
            .rows_affected() as i64;
        let total = (requests_deleted + archived_deleted + templates_deleted) as u64;
        if total > 0 {
            tracing::info!(
                requests_deleted,
                archived_deleted,
                templates_deleted,
                "Purged orphaned rows"
            );
        }

        Ok(total)
    }

    async fn archive_batch(&self, batch_id: BatchId) -> Result<ArchiveOutcome> {
        request_maintenance::before_archive(self).await?;
        let deadline = self.maintenance_deadline();
        let mut tx = self
            .begin_maintenance_write_until(deadline)
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to begin transaction: {}", e)))?;

        let batch = sqlx::query!(
            r#"
            SELECT retry_version,
                   location,
                   counts_frozen_at,
                   COALESCE(
                       archive_bucket,
                       -- A batch that outlived its own week's retirement (it
                       -- was blocked from moving until after the week was
                       -- dropped) is routed into the current week instead:
                       -- a retired week never receives rows, and a later
                       -- retirement date is the safe direction.
                       CASE
                           WHEN EXISTS (
                               SELECT 1
                               FROM batch_archive_buckets bucket
                               WHERE bucket.week_start =
                                     date_trunc('week', created_at AT TIME ZONE 'UTC')::date
                                 AND bucket.state <> 'active'
                           )
                           THEN date_trunc('week', now() AT TIME ZONE 'UTC')::date
                           ELSE date_trunc('week', created_at AT TIME ZONE 'UTC')::date
                       END
                   ) AS "bucket!"
            FROM batches
            WHERE id = $1 AND deleted_at IS NULL
            FOR UPDATE SKIP LOCKED
            "#,
            *batch_id as Uuid,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to lock batch for archive: {}", e)))?;

        let Some(batch) = batch else {
            bound_to_deadline(&mut tx, deadline).await.map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to bound archive move: {}", e))
            })?;
            let exists = sqlx::query_scalar!(
                r#"SELECT EXISTS(SELECT 1 FROM batches WHERE id = $1 AND deleted_at IS NULL) AS "exists!""#,
                *batch_id as Uuid,
            )
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to check batch existence: {}", e))
            })?;
            return Ok(if exists {
                metrics::counter!("fusillade_archive_contended_total").increment(1);
                ArchiveOutcome::SkippedNotLive
            } else {
                ArchiveOutcome::SkippedNotFound
            });
        };
        if batch.location == "archive" {
            return Ok(ArchiveOutcome::SkippedNotLive);
        }
        if batch.counts_frozen_at.is_none() {
            return Ok(ArchiveOutcome::SkippedNotFrozen);
        }

        bound_to_deadline(&mut tx, deadline)
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to bound archive move: {}", e)))?;
        let bucket_state: Option<String> = sqlx::query_scalar(
            "SELECT state FROM batch_archive_buckets WHERE week_start = $1 FOR SHARE",
        )
        .bind(batch.bucket)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to lock archive bucket: {}", e)))?;
        if bucket_state.is_some_and(|state| state != "active") {
            return Ok(ArchiveOutcome::SkippedNoPartition);
        }

        bound_to_deadline(&mut tx, deadline)
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to bound archive move: {}", e)))?;
        let partition_exists = sqlx::query_scalar!(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM pg_inherits inheritance
                JOIN pg_class parent ON parent.oid = inheritance.inhparent
                JOIN pg_class child ON child.oid = inheritance.inhrelid
                JOIN pg_namespace namespace ON namespace.oid = child.relnamespace
                WHERE parent.oid = 'batch_requests_archive'::regclass
                  AND namespace.nspname = current_schema()
                  AND child.relname =
                      'batch_requests_archive_y' || to_char($1::date, 'IYYY')
                          || 'w' || to_char($1::date, 'IW')
                  AND NOT inheritance.inhdetachpending
            ) AS "exists!"
            "#,
            batch.bucket,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to check archive partition: {}", e)))?;
        if !partition_exists {
            return Ok(ArchiveOutcome::SkippedNoPartition);
        }

        bound_to_deadline(&mut tx, deadline)
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to bound archive move: {}", e)))?;
        let inserted = sqlx::query(
            r#"
            INSERT INTO batch_requests_archive
            SELECT r.*, $2::date
            FROM requests r
            WHERE r.batch_id = $1
            ON CONFLICT (id, archive_bucket) DO NOTHING
            "#,
        )
        .bind(*batch_id as Uuid)
        .bind(batch.bucket)
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to copy rows to archive: {}", e)))?
        .rows_affected();

        bound_to_deadline(&mut tx, deadline)
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to bound archive move: {}", e)))?;
        let deleted = sqlx::query!(
            r#"
            DELETE FROM requests r
            WHERE r.batch_id = $1
              AND EXISTS (
                  SELECT 1 FROM batch_requests_archive a
                  WHERE a.id = r.id AND a.archive_bucket = $2
              )
            "#,
            *batch_id as Uuid,
            batch.bucket,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to delete archived rows: {}", e)))?
        .rows_affected();

        bound_to_deadline(&mut tx, deadline)
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to bound archive move: {}", e)))?;
        let left_behind = sqlx::query_scalar!(
            r#"SELECT COUNT(*) AS "count!" FROM requests WHERE batch_id = $1"#,
            *batch_id as Uuid,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to verify move completeness: {}", e)))?;
        if left_behind != 0 {
            return Err(FusilladeError::Other(anyhow!(
                "archive move for batch {batch_id} would leave {left_behind} rows in live \
                 (inserted {inserted}, deleted {deleted}); aborted to preserve the \
                 exactly-one-table invariant"
            )));
        }

        bound_to_deadline(&mut tx, deadline)
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to bound archive move: {}", e)))?;
        let stamped = sqlx::query!(
            r#"
            UPDATE batches
            SET location = 'archive', archive_bucket = $2
            WHERE id = $1
              AND retry_version = $3
              AND counts_frozen_at IS NOT NULL
              AND location IN ('live', 'split')
            "#,
            *batch_id as Uuid,
            batch.bucket,
            batch.retry_version,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to stamp batch location: {}", e)))?
        .rows_affected();
        if stamped == 0 {
            return Ok(ArchiveOutcome::SkippedRetryRaced);
        }

        tx.commit()
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to commit archive move: {}", e)))?;

        tracing::info!(%batch_id, rows = deleted, "Archived batch rows");
        Ok(ArchiveOutcome::Archived { rows: deleted })
    }

    async fn list_archivable_batches(
        &self,
        limit: i64,
        oldest_first: bool,
        cancel_grace_secs: f64,
        min_frozen_age_secs: f64,
    ) -> Result<Vec<BatchId>> {
        let rows = if oldest_first {
            sqlx::query_scalar!(
                r#"
                SELECT id FROM batches b
                WHERE b.location IN ('live', 'split') AND b.counts_frozen_at IS NOT NULL AND b.deleted_at IS NULL
                  AND b.counts_frozen_at <= NOW() - make_interval(secs => $3)
                  AND NOT EXISTS (
                      SELECT 1 FROM requests r
                      WHERE r.batch_id = b.id
                        AND r.state = 'canceled'
                        AND r.claimed_at IS NOT NULL
                        AND r.canceled_at > NOW() - make_interval(secs => $2)
                  )
                ORDER BY b.created_at ASC, b.id ASC
                LIMIT $1
                "#,
                limit,
                cancel_grace_secs,
                min_frozen_age_secs,
            )
            .fetch_all(self.read_executor())
            .await
        } else {
            sqlx::query_scalar!(
                r#"
                SELECT id FROM batches b
                WHERE b.location IN ('live', 'split') AND b.counts_frozen_at IS NOT NULL AND b.deleted_at IS NULL
                  AND b.counts_frozen_at <= NOW() - make_interval(secs => $3)
                  AND NOT EXISTS (
                      SELECT 1 FROM requests r
                      WHERE r.batch_id = b.id
                        AND r.state = 'canceled'
                        AND r.claimed_at IS NOT NULL
                        AND r.canceled_at > NOW() - make_interval(secs => $2)
                  )
                ORDER BY b.created_at DESC, b.id DESC
                LIMIT $1
                "#,
                limit,
                cancel_grace_secs,
                min_frozen_age_secs,
            )
            .fetch_all(self.read_executor())
            .await
        }
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to list archivable batches: {}", e)))?;

        Ok(rows.into_iter().map(BatchId).collect())
    }

    async fn count_archivable_batches(&self, cancel_grace_secs: f64) -> Result<i64> {
        sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) AS "count!" FROM batches b
            WHERE b.location IN ('live', 'split') AND b.counts_frozen_at IS NOT NULL AND b.deleted_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM requests r
                  WHERE r.batch_id = b.id
                    AND r.state = 'canceled'
                    AND r.claimed_at IS NOT NULL
                    AND r.canceled_at > NOW() - make_interval(secs => $1)
              )
            "#,
            cancel_grace_secs,
        )
        .fetch_one(self.read_executor())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to count archivable batches: {}", e)))
    }

    async fn count_unfrozen_terminal_batches(&self) -> Result<i64> {
        sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) AS "count!" FROM batches
            WHERE deleted_at IS NULL AND counts_frozen_at IS NULL
              AND (completed_at IS NOT NULL OR failed_at IS NOT NULL OR cancelled_at IS NOT NULL)
            "#,
        )
        .fetch_one(self.read_executor())
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to count unfrozen terminal batches: {}", e))
        })
    }

    async fn finalize_terminal_batches(&self) -> Result<i64> {
        let result = sqlx::query!(
            r#"
            WITH candidates AS (
                SELECT b.id, b.retry_version,
                       COALESCE(counts.completed, 0) + COALESCE(arch.completed, 0) AS completed,
                       COALESCE(counts.failed, 0) + COALESCE(arch.failed, 0) AS failed,
                       COALESCE(counts.canceled, 0) + COALESCE(arch.canceled, 0) AS canceled
                FROM batches b
                LEFT JOIN LATERAL (
                    SELECT
                        COUNT(*) FILTER (WHERE state = 'completed') AS completed,
                        COUNT(*) FILTER (WHERE state = 'failed') AS failed,
                        COUNT(*) FILTER (WHERE state = 'canceled') AS canceled,
                        COUNT(*) FILTER (WHERE state NOT IN ('completed', 'failed', 'canceled')) AS live
                    FROM requests WHERE batch_id = b.id
                ) counts ON TRUE
                -- Split batches: archived (already-done) rows count toward
                -- terminal-by-count, same as in get_batch.
                LEFT JOIN LATERAL (
                    SELECT
                        COUNT(*) FILTER (WHERE state = 'completed') AS completed,
                        COUNT(*) FILTER (WHERE state = 'failed') AS failed,
                        COUNT(*) FILTER (WHERE state = 'canceled') AS canceled
                    FROM batch_requests_archive a
                    WHERE b.location = 'split'
                      AND a.archive_bucket = b.archive_bucket
                      AND a.batch_id = b.id
                ) arch ON TRUE
                WHERE b.counts_frozen_at IS NULL
                  AND b.deleted_at IS NULL
                  AND b.cancelling_at IS NULL
                  AND b.total_requests > 0
                  -- Most unfrozen batches still have work in flight. Rule
                  -- them out on a live row before the lateral counts above
                  -- read every row of every candidate; `state_check` makes
                  -- these three the complement of the terminal states, and
                  -- the live count below stays the authoritative check.
                  AND NOT EXISTS (
                      SELECT 1 FROM requests r
                      WHERE r.batch_id = b.id
                        AND r.state IN ('pending', 'claimed', 'processing')
                  )
                  AND COALESCE(counts.live, 0) = 0
                  AND COALESCE(counts.completed, 0) + COALESCE(counts.failed, 0) + COALESCE(counts.canceled, 0)
                      + COALESCE(arch.completed, 0) + COALESCE(arch.failed, 0) + COALESCE(arch.canceled, 0)
                      = b.total_requests
            )
            UPDATE batches b
            SET finalizing_at = COALESCE(b.finalizing_at, NOW()),
                completed_at = COALESCE(b.completed_at,
                    CASE WHEN c.completed > 0 THEN NOW() END),
                failed_at = COALESCE(b.failed_at,
                    CASE WHEN c.completed = 0 THEN NOW() END),
                completed_requests = c.completed,
                failed_requests = c.failed,
                canceled_requests = c.canceled,
                counts_frozen_at = NOW()
            FROM candidates c
            WHERE b.id = c.id
              -- Re-checks against concurrent writers: get_batch's lazy freeze
              -- (counts_frozen_at) and batch retry (retry_version CAS; a
              -- target-row condition, so it holds even when this UPDATE
              -- resumes from a lock wait).
              AND b.counts_frozen_at IS NULL
              AND b.retry_version = c.retry_version
            "#,
        )
        .execute(self.write_executor())
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to finalize terminal batches: {}", e))
        })?;
        Ok(result.rows_affected() as i64)
    }

    async fn finalize_cancelled_batches(&self, grace_secs: f64, limit: i64) -> Result<i64> {
        let candidates = sqlx::query_scalar!(
            r#"
            SELECT id FROM batches
            WHERE cancelled_at IS NOT NULL
              AND counts_frozen_at IS NULL
              AND deleted_at IS NULL
              AND cancelled_at < NOW() - make_interval(secs => $1)
            ORDER BY cancelled_at
            LIMIT $2
            "#,
            grace_secs,
            limit,
        )
        .fetch_all(self.read_executor())
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!(
                "Failed to list cancelled finalizer candidates: {}",
                e
            ))
        })?;

        let mut finalized = 0i64;
        let deadline = self.maintenance_deadline();
        for batch_id in candidates {
            if std::time::Instant::now() >= deadline {
                break;
            }
            let mut tx = self
                .begin_maintenance_write_until(deadline)
                .await
                .map_err(|e| {
                    FusilladeError::Other(anyhow!("Failed to begin transaction: {}", e))
                })?;

            let batch = sqlx::query!(
                r#"
                SELECT retry_version, total_requests
                FROM batches
                WHERE id = $1
                  AND cancelled_at IS NOT NULL
                  AND counts_frozen_at IS NULL
                  AND deleted_at IS NULL
                FOR UPDATE
                "#,
                batch_id,
            )
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| FusilladeError::Other(anyhow!("Failed to lock cancelled batch: {}", e)))?;
            let Some(batch) = batch else {
                continue;
            };

            bound_to_deadline(&mut tx, deadline).await.map_err(|e| {
                FusilladeError::Other(anyhow!(
                    "Failed to bound cancelled batch finalization: {}",
                    e
                ))
            })?;
            sqlx::query!(
                r#"
                UPDATE requests
                SET state = 'canceled',
                    canceled_at = COALESCE(canceled_at, NOW())
                WHERE batch_id = $1
                  AND state IN ('pending', 'claimed', 'processing')
                "#,
                batch_id,
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to settle cancelled batch rows: {}", e))
            })?;

            bound_to_deadline(&mut tx, deadline).await.map_err(|e| {
                FusilladeError::Other(anyhow!(
                    "Failed to bound cancelled batch finalization: {}",
                    e
                ))
            })?;
            let froze = sqlx::query!(
                r#"
                UPDATE batches b
                SET finalizing_at = COALESCE(b.finalizing_at, NOW()),
                    completed_requests = c.completed,
                    failed_requests = c.failed,
                    canceled_requests = c.canceled,
                    counts_frozen_at = NOW(),
                    notification_sent_at = COALESCE(b.notification_sent_at, NOW())
                FROM (
                    SELECT
                        COALESCE(live.completed, 0) + COALESCE(arch.completed, 0) AS completed,
                        COALESCE(live.failed, 0) + COALESCE(arch.failed, 0) AS failed,
                        COALESCE(live.canceled, 0) + COALESCE(arch.canceled, 0) AS canceled
                    FROM (
                        SELECT
                            COUNT(*) FILTER (WHERE state = 'completed') AS completed,
                            COUNT(*) FILTER (WHERE state = 'failed') AS failed,
                            COUNT(*) FILTER (WHERE state = 'canceled') AS canceled
                        FROM requests WHERE batch_id = $1
                    ) live
                    LEFT JOIN LATERAL (
                        SELECT
                            COUNT(*) FILTER (WHERE state = 'completed') AS completed,
                            COUNT(*) FILTER (WHERE state = 'failed') AS failed,
                            COUNT(*) FILTER (WHERE state = 'canceled') AS canceled
                        FROM batch_requests_archive a, batches bb
                        WHERE bb.id = $1
                          AND bb.location = 'split'
                          AND a.archive_bucket = bb.archive_bucket
                          AND a.batch_id = $1
                    ) arch ON TRUE
                ) c
                WHERE b.id = $1
                  AND b.retry_version = $2
                  AND b.counts_frozen_at IS NULL
                  AND b.cancelled_at IS NOT NULL
                  AND c.completed + c.failed + c.canceled = b.total_requests
                "#,
                batch_id,
                batch.retry_version,
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                FusilladeError::Other(anyhow!("Failed to freeze cancelled batch: {}", e))
            })?;

            if froze.rows_affected() > 0 {
                tx.commit().await.map_err(|e| {
                    FusilladeError::Other(anyhow!("Failed to commit finalization: {}", e))
                })?;
                finalized += 1;
            } else {
                tx.rollback().await.map_err(|e| {
                    FusilladeError::Other(anyhow!("Failed to roll back finalization: {}", e))
                })?;
            }
        }
        Ok(finalized)
    }

    async fn ensure_archive_partitions(&self, weeks_ahead: i32) -> Result<(i64, i64)> {
        let row = sqlx::query!(
            r#"
            SELECT ensure_archive_partitions($1) AS "created!",
                   (SELECT COUNT(*) FROM generate_series(0, $1) AS w(i)
                    WHERE to_regclass(
                        'batch_requests_archive_y'
                        || to_char(date_trunc('week', now() AT TIME ZONE 'UTC')::date + (w.i * 7), 'IYYY')
                        || 'w'
                        || to_char(date_trunc('week', now() AT TIME ZONE 'UTC')::date + (w.i * 7), 'IW')
                    ) IS NOT NULL) AS "ahead!"
            "#,
            weeks_ahead,
        )
        .fetch_one(self.write_executor())
        .await
        .map_err(|e| {
            FusilladeError::Other(anyhow!("Failed to ensure archive partitions: {}", e))
        })?;
        Ok((i64::from(row.created), row.ahead))
    }

    async fn ensure_request_template_partitions(&self, weeks_ahead: i32) -> Result<(i64, i64)> {
        let map_err = |e: sqlx::Error| {
            FusilladeError::Other(anyhow!(
                "Failed to ensure request template partitions: {}",
                e
            ))
        };
        let mut tx = self.begin_maintenance_write().await.map_err(map_err)?;
        sqlx::query("SET LOCAL lock_timeout = '1s'")
            .execute(&mut *tx)
            .await
            .map_err(map_err)?;
        let (created, ahead): (i32, i64) = sqlx::query_as(
            r#"
            SELECT ensure_request_template_partitions(
                       (date_trunc('week', now() AT TIME ZONE 'UTC'))::date, $1
                   ),
                   (SELECT COUNT(*) FROM generate_series(0, $1) AS w(i)
                    WHERE to_regclass(
                        'request_templates_g2_y'
                        || to_char(date_trunc('week', now() AT TIME ZONE 'UTC')::date + (w.i * 7), 'IYYY')
                        || 'w'
                        || to_char(date_trunc('week', now() AT TIME ZONE 'UTC')::date + (w.i * 7), 'IW')
                    ) IS NOT NULL)
            "#,
        )
        .bind(weeks_ahead)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_err)?;
        tx.commit().await.map_err(map_err)?;
        Ok((i64::from(created), ahead))
    }

    async fn purge_model_filter_events(
        &self,
        batch_size: i64,
        keep_per_model: i64,
        retention_secs: f64,
    ) -> Result<u64> {
        let keep = keep_per_model.max(1);
        let deleted = sqlx::query!(
            r#"
            WITH ranked AS (
                SELECT
                    id,
                    created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY model
                        ORDER BY created_at DESC, id DESC
                    ) AS rn
                FROM model_filters
            ),
            eligible AS (
                SELECT id
                FROM ranked
                WHERE rn > $2
                  AND created_at < now() - make_interval(secs => $3)
                ORDER BY id ASC
                LIMIT $1
            )
            DELETE FROM model_filters
            WHERE id IN (SELECT id FROM eligible)
            "#,
            batch_size,
            keep,
            retention_secs,
        )
        .execute(self.write_executor())
        .await
        .map_err(|e| FusilladeError::Other(anyhow!("Failed to purge model_filters events: {}", e)))?
        .rows_affected();

        if deleted > 0 {
            tracing::debug!(deleted, "Purged old model_filters events");
        }

        Ok(deleted)
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use crate::TestDbPools;
    use crate::batch::FileStreamResult;
    use crate::daemon::{
        AnyDaemonRecord, DaemonConfig, DaemonData, DaemonRecord, DaemonStats, DaemonStatus, Dead,
        Initializing, Running,
    };
    use chrono::Timelike;

    #[derive(Debug, Clone, Default)]
    struct MockHttpClient;

    impl MockHttpClient {
        fn new() -> Self {
            Self
        }
    }

    #[tokio::test]
    async fn zero_state_write_limit_is_unbounded() {
        let limiter = StateWriteLimiter::new(0);
        let (first, second, third) = tokio::join!(
            limiter.acquire("first"),
            limiter.acquire("second"),
            limiter.acquire("third")
        );
        let permits = (first.unwrap(), second.unwrap(), third.unwrap());

        assert_eq!(limiter.counts(), (0, 3));
        drop(permits);
        assert_eq!(limiter.counts(), (0, 0));
    }

    fn expect_stream_success(result: FileStreamResult) -> FileId {
        match result {
            FileStreamResult::Success(file_id) => file_id,
            FileStreamResult::Aborted => panic!("Expected stream creation success, got abort"),
        }
    }

    async fn mark_models_live_for_test(
        manager: &PostgresRequestManager<TestDbPools>,
        models: impl IntoIterator<Item = impl AsRef<str>>,
    ) {
        let filters: Vec<ModelFilter> = models
            .into_iter()
            .map(|model| ModelFilter {
                model: model.as_ref().to_string(),
                state: ModelFilterState::Live,
                expected_ready_at: None,
            })
            .collect();

        manager.append_model_filter_events(&filters).await.unwrap();
    }

    async fn create_background_batch_for_test(
        manager: &PostgresRequestManager<TestDbPools>,
        model: &str,
        owner: &str,
    ) -> Batch {
        let file_id = manager
            .create_file(
                format!("{model}-background"),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some(format!("{model}-batch")),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: model.to_string(),
                }],
            )
            .await
            .unwrap();

        manager
            .create_background_batch(BackgroundBatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                metadata: None,
                created_by: Some(owner.to_string()),
                api_key_id: None,
                total_requests: Some(1),
            })
            .await
            .unwrap()
    }

    async fn create_background_request_for_test(
        manager: &PostgresRequestManager<TestDbPools>,
        model: &str,
        owner: &str,
    ) -> RequestId {
        let request_id = Uuid::new_v4();
        manager
            .create_background(CreateBackgroundInput {
                request_id,
                body: "{}".to_string(),
                model: model.to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: owner.to_string(),
                metadata: None,
            })
            .await
            .unwrap()
    }

    async fn claim_batch_requests_for_test(
        manager: &PostgresRequestManager<TestDbPools>,
        limit: usize,
        batch_limit: usize,
        daemon_id: DaemonId,
        available_capacity: &HashMap<String, usize>,
        user_active_counts: &HashMap<String, usize>,
    ) -> Vec<Request<Claimed>> {
        mark_models_live_for_test(manager, available_capacity.keys()).await;
        manager
            .unclaim_stale_requests()
            .await
            .expect("unclaim stale failed");
        manager
            .claim_batch_requests(
                limit,
                batch_limit,
                daemon_id,
                available_capacity,
                user_active_counts,
            )
            .await
            .expect("batch claim failed")
    }

    fn key_id_of(request: &Request<Claimed>) -> Option<Uuid> {
        request
            .data
            .batch_metadata
            .get(API_KEY_ID_METADATA_KEY)
            .map(|id| id.parse().unwrap())
    }

    #[sqlx::test]
    async fn batchless_requests_record_their_key_id(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(DaemonConfig {
            background_concurrency_limit: 10,
            ..Default::default()
        });
        let flex_key = Uuid::new_v4();
        let background_key = Uuid::new_v4();
        let by_id = manager
            .create_flex(CreateFlexInput {
                request_id: Uuid::new_v4(),
                body: "{}".to_string(),
                model: "key-id-model".to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                api_key_id: flex_key,
                created_by: "owner".to_string(),
                metadata: None,
            })
            .await
            .unwrap();
        let background_id = manager
            .create_background(CreateBackgroundInput {
                request_id: Uuid::new_v4(),
                body: "{}".to_string(),
                model: "key-id-model".to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                api_key_id: background_key,
                created_by: "owner".to_string(),
                metadata: None,
            })
            .await
            .unwrap();

        let stored: HashMap<Uuid, Option<Uuid>> = sqlx::query_as::<_, (Uuid, Option<Uuid>)>(
            "SELECT r.id, t.api_key_id FROM requests r
             JOIN request_templates_all t ON t.id = r.template_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap()
        .into_iter()
        .collect();
        assert_eq!(stored[&by_id], Some(flex_key));
        assert_eq!(stored[&background_id], Some(background_key));

        mark_models_live_for_test(&manager, ["key-id-model"]).await;
        let capacity = HashMap::from([("key-id-model".to_string(), 10)]);
        let daemon_id = DaemonId::from(Uuid::new_v4());
        let claimed: HashMap<RequestId, (String, Option<Uuid>)> = manager
            .claim_batchless_requests(10, daemon_id, &capacity, &HashMap::new(), &HashSet::new())
            .await
            .unwrap()
            .iter()
            .map(|r| (r.data.id, (r.data.api_key.clone(), key_id_of(r))))
            .collect();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[&by_id], (String::new(), Some(flex_key)));

        let background = manager
            .claim_background_batchless_requests(1, daemon_id, &capacity, &HashMap::new())
            .await
            .unwrap();
        assert_eq!(background.len(), 1);
        assert_eq!(background[0].data.id, background_id);
        assert_eq!(background[0].data.api_key, "");
        assert_eq!(key_id_of(&background[0]), Some(background_key));
    }

    #[sqlx::test]
    async fn batch_requests_report_the_batch_key_id(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let file_id = manager
            .create_file(
                "key-id.jsonl".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "batch-key-model".to_string(),
                }],
            )
            .await
            .unwrap();
        let batch_key = Uuid::new_v4();
        let batch_input = |api_key_id| crate::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: Some("owner".to_string()),
            api_key_id,
            total_requests: None,
        };
        let by_id = manager
            .create_batch(batch_input(Some(batch_key)))
            .await
            .unwrap();
        let legacy = manager.create_batch(batch_input(None)).await.unwrap();

        let capacity = HashMap::from([("batch-key-model".to_string(), 10)]);
        let claimed = claim_batch_requests_for_test(
            &manager,
            10,
            10,
            DaemonId::from(Uuid::new_v4()),
            &capacity,
            &HashMap::new(),
        )
        .await;
        assert_eq!(claimed.len(), 2);
        let by_batch: HashMap<BatchId, (String, Option<Uuid>)> = claimed
            .iter()
            .map(|r| {
                (
                    r.data.batch_id.unwrap(),
                    (r.data.api_key.clone(), key_id_of(r)),
                )
            })
            .collect();
        assert_eq!(by_batch[&by_id.id], (String::new(), Some(batch_key)));
        assert_eq!(by_batch[&legacy.id], (String::new(), None));
    }

    #[sqlx::test]
    async fn caller_metadata_cannot_name_the_dispatch_key(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(DaemonConfig {
            batch_metadata_fields: vec![API_KEY_ID_METADATA_KEY.to_string(), "id".to_string()],
            ..Default::default()
        });
        let spoofed = serde_json::json!({ API_KEY_ID_METADATA_KEY: Uuid::new_v4().to_string() });
        let file_id = manager
            .create_file(
                "spoof.jsonl".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "spoof-model".to_string(),
                }],
            )
            .await
            .unwrap();
        let batch_key = Uuid::new_v4();
        let batch_input = |api_key_id| crate::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: Some(spoofed.clone()),
            created_by: Some("owner".to_string()),
            api_key_id,
            total_requests: None,
        };
        let legacy_batch = manager.create_batch(batch_input(None)).await.unwrap();
        let keyed_batch = manager
            .create_batch(batch_input(Some(batch_key)))
            .await
            .unwrap();
        let flex_key = Uuid::new_v4();
        let keyed_flex = manager
            .create_flex(CreateFlexInput {
                request_id: Uuid::new_v4(),
                body: "{}".to_string(),
                model: "spoof-model".to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                api_key_id: flex_key,
                created_by: "owner".to_string(),
                metadata: Some(spoofed.clone()),
            })
            .await
            .unwrap();

        let capacity = HashMap::from([("spoof-model".to_string(), 10)]);
        let batch_rows = claim_batch_requests_for_test(
            &manager,
            10,
            10,
            DaemonId::from(Uuid::new_v4()),
            &capacity,
            &HashMap::new(),
        )
        .await;
        let by_batch: HashMap<BatchId, Option<Uuid>> = batch_rows
            .iter()
            .map(|r| (r.data.batch_id.unwrap(), key_id_of(r)))
            .collect();
        assert_eq!(by_batch.len(), 2);
        assert_eq!(by_batch[&legacy_batch.id], None);
        assert_eq!(by_batch[&keyed_batch.id], Some(batch_key));

        let flex = manager
            .claim_batchless_requests(
                10,
                DaemonId::from(Uuid::new_v4()),
                &capacity,
                &HashMap::new(),
                &HashSet::new(),
            )
            .await
            .unwrap();
        assert_eq!(flex.len(), 1);
        assert_eq!(flex[0].data.id, keyed_flex);
        assert_eq!(key_id_of(&flex[0]), Some(flex_key));
    }

    #[sqlx::test]
    async fn pending_request_counts_statement_timeout_uses_manager_config(pool: sqlx::PgPool) {
        let config = DaemonConfig {
            pending_request_counts_timeout_ms: 12_345,
            ..DaemonConfig::default()
        };
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(config);

        assert_eq!(manager.pending_counts_statement_timeout_ms(), 12_345);
    }

    async fn statement_timeout_ms(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> i64 {
        sqlx::query_scalar(
            "SELECT setting::bigint FROM pg_settings WHERE name = 'statement_timeout'",
        )
        .fetch_one(&mut **tx)
        .await
        .unwrap()
    }

    #[sqlx::test]
    async fn maintenance_budget_follows_the_client_timeout(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let mut tx = manager.begin_maintenance_write().await.unwrap();
        let budget = statement_timeout_ms(&mut tx).await;
        assert!((149_000..=150_000).contains(&budget), "{budget}");
        tx.commit().await.unwrap();

        let session: String = sqlx::query_scalar("SELECT current_setting('statement_timeout')")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(session, "0");

        let manager = manager.with_maintenance_query_timeout(std::time::Duration::from_secs(60));
        let mut tx = manager.begin_maintenance_write().await.unwrap();
        let budget = statement_timeout_ms(&mut tx).await;
        assert!((49_000..=50_000).contains(&budget), "{budget}");
        tx.rollback().await.unwrap();
    }

    #[sqlx::test]
    async fn later_maintenance_statements_share_one_deadline(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let now = std::time::Instant::now();

        let mut tx = manager
            .begin_maintenance_write_until(now + std::time::Duration::from_secs(150))
            .await
            .unwrap();
        bound_to_deadline(&mut tx, now + std::time::Duration::from_secs(20))
            .await
            .unwrap();
        let budget = statement_timeout_ms(&mut tx).await;
        assert!((19_000..=20_000).contains(&budget), "{budget}");
        tx.rollback().await.unwrap();

        let mut tx = manager.begin_maintenance_write().await.unwrap();
        bound_to_deadline(&mut tx, now).await.unwrap();
        let error = sqlx::query("SELECT pg_sleep(1)")
            .execute(&mut *tx)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("statement timeout"), "{error}");
    }

    const GET_CANCELLED_BATCH_IDS_SQL: &str =
        include_str!("postgres/sql/get_cancelled_batch_ids.sql");
    const CLAIM_BATCH_NOTIFICATIONS_SQL: &str =
        include_str!("postgres/sql/claim_batch_notifications.sql");

    fn plan_scans_index(plan: &serde_json::Value, node_type: &str, index_name: &str) -> bool {
        match plan {
            serde_json::Value::Array(values) => values
                .iter()
                .any(|value| plan_scans_index(value, node_type, index_name)),
            serde_json::Value::Object(fields) => {
                (fields.get("Node Type").and_then(|value| value.as_str()) == Some(node_type)
                    && fields.get("Index Name").and_then(|value| value.as_str())
                        == Some(index_name))
                    || fields
                        .values()
                        .any(|value| plan_scans_index(value, node_type, index_name))
            }
            _ => false,
        }
    }

    #[sqlx::test]
    async fn cancellation_poll_probes_only_cancelling_batches(pool: sqlx::PgPool) {
        sqlx::query(
            r#"
            INSERT INTO batches (id, endpoint, completion_window, created_by,
                                 created_at, expires_at, cancelling_at, deleted_at)
            SELECT md5(i::text)::uuid, '/v1/chat/completions', '24h', 'owner',
                   '2026-01-01'::timestamptz + i * interval '1 second',
                   '2026-01-02'::timestamptz,
                   CASE WHEN i IN (7, 4000, 9000) THEN '2026-01-01'::timestamptz END,
                   CASE WHEN i = 9000 THEN '2026-01-01'::timestamptz END
            FROM generate_series(1, 10000) i
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("VACUUM (ANALYZE) batches")
            .execute(&pool)
            .await
            .unwrap();
        let polled: Vec<Uuid> =
            sqlx::query_scalar("SELECT md5(i::text)::uuid FROM generate_series(1, 10000) i")
                .fetch_all(&pool)
                .await
                .unwrap();

        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL plan_cache_mode = force_generic_plan")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::raw_sql(&format!(
            "PREPARE cancel_poll(uuid[]) AS {GET_CANCELLED_BATCH_IDS_SQL}"
        ))
        .execute(&mut *tx)
        .await
        .unwrap();
        let ids = polled
            .iter()
            .map(Uuid::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let plan: serde_json::Value = sqlx::query_scalar(&format!(
            "EXPLAIN (FORMAT JSON) EXECUTE cancel_poll('{{{ids}}}'::uuid[])"
        ))
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert!(
            plan_scans_index(&plan, "Index Only Scan", "idx_batches_cancelling"),
            "the cancellation poll must be an index-only scan of idx_batches_cancelling: {plan}"
        );
        tx.rollback().await.unwrap();

        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let polled: Vec<BatchId> = polled.into_iter().map(BatchId).collect();
        let expected: Vec<Uuid> = sqlx::query_scalar(
            "SELECT md5(i::text)::uuid FROM unnest(ARRAY[7, 4000]) i ORDER BY 1",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        let mut cancelled: Vec<Uuid> = manager
            .get_cancelled_batch_ids(&polled)
            .await
            .unwrap()
            .into_iter()
            .map(|id| *id)
            .collect();
        cancelled.sort();
        assert_eq!(cancelled, expected);
    }


    #[test]
    fn test_sanitize_outbound_body_strips_both_fields() {
        let raw = r#"{"model":"gpt-4","input":"hi","service_tier":"flex","background":true}"#;
        let cleaned = sanitize_outbound_body(raw);
        let parsed: serde_json::Value = serde_json::from_str(cleaned.as_ref()).expect("valid JSON");
        assert_eq!(parsed["model"], "gpt-4");
        assert_eq!(parsed["input"], "hi");
        assert!(parsed.get("service_tier").is_none());
        assert!(parsed.get("background").is_none());
        assert!(matches!(cleaned, std::borrow::Cow::Owned(_)));
    }

    #[test]
    fn test_sanitize_outbound_body_borrows_when_nothing_to_strip() {
        let raw = r#"{"model":"gpt-4","input":"hi"}"#;
        let cleaned = sanitize_outbound_body(raw);
        assert_eq!(cleaned.as_ref(), raw);
        assert!(matches!(cleaned, std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn test_sanitize_outbound_body_non_object_passthrough() {
        for raw in ["[1,2,3]", "\"plain string\"", "42", "not json at all"] {
            let cleaned = sanitize_outbound_body(raw);
            assert_eq!(cleaned.as_ref(), raw);
            assert!(matches!(cleaned, std::borrow::Cow::Borrowed(_)));
        }
    }

    #[test]
    fn test_sanitize_outbound_body_substring_in_value_is_safe_passthrough() {
        let raw = r#"{"model":"gpt-4","input":"tell me about service_tier and background"}"#;
        let cleaned = sanitize_outbound_body(raw);
        assert_eq!(cleaned.as_ref(), raw);
        assert!(matches!(cleaned, std::borrow::Cow::Borrowed(_)));
    }

    #[sqlx::test]
    async fn test_create_flex_strips_routing_fields_from_stored_body(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let request_id = uuid::Uuid::new_v4();
        manager
            .create_flex(crate::request::CreateFlexInput {
                request_id,
                body: r#"{"model":"gpt-4","input":"hi","service_tier":"flex","background":true}"#
                    .to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: "user-123".to_string(),
                metadata: None,
            })
            .await
            .expect("create_flex should succeed");

        let detail = manager
            .get_request_detail(crate::request::RequestId(request_id))
            .await
            .expect("request should exist");
        let stored = detail.body.expect("stored body should be present");
        let parsed: serde_json::Value =
            serde_json::from_str(&stored).expect("stored body parses as JSON");
        assert_eq!(parsed["model"], "gpt-4");
        assert_eq!(parsed["input"], "hi");
        assert!(
            parsed.get("service_tier").is_none(),
            "service_tier should be stripped before storage; got: {stored}"
        );
        assert!(
            parsed.get("background").is_none(),
            "background should be stripped before storage; got: {stored}"
        );
    }


    #[sqlx::test]
    async fn test_create_and_get_file(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "test-file".to_string(),
                Some("A test file".to_string()),
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/completions".to_string(),
                        body: r#"{"model":"gpt-4"}"#.to_string(),
                        model: "gpt-4".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/completions".to_string(),
                        body: r#"{"model":"gpt-3.5"}"#.to_string(),
                        model: "gpt-3.5".to_string(),
                    },
                ],
            )
            .await
            .expect("Failed to create file");

        let file = manager.get_file(file_id).await.expect("Failed to get file");

        assert_eq!(file.id, file_id);
        assert_eq!(file.name, "test-file");
        assert_eq!(file.description, Some("A test file".to_string()));

        let content = manager
            .get_file_content(file_id)
            .await
            .expect("Failed to get content");

        assert_eq!(content.len(), 2);
        match &content[0] {
            FileContentItem::Template(t) => assert_eq!(t.model, "gpt-4"),
            _ => panic!("Expected template"),
        }
        match &content[1] {
            FileContentItem::Template(t) => assert_eq!(t.model, "gpt-3.5"),
            _ => panic!("Expected template"),
        }
    }


    #[sqlx::test]
    async fn create_file_does_not_deadlock_with_partition_creation(pool: sqlx::PgPool) {
        let no_partitions: bool =
            sqlx::query_scalar("SELECT NOT EXISTS (SELECT 1 FROM request_template_buckets)")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            no_partitions,
            "the upload must need to create its partition"
        );
        let mut creator = pool.begin().await.unwrap();
        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended(
                 'request_templates_g2.partition:' || current_schema() || ':'
                     || to_char(date_trunc('week', now() AT TIME ZONE 'UTC')::date, 'YYYYMMDD'),
                 0))",
        )
        .execute(&mut *creator)
        .await
        .unwrap();

        let manager = Arc::new(PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        ));
        let upload = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager
                    .create_file(
                        "upload".to_string(),
                        None,
                        vec![RequestTemplateInput {
                            custom_id: None,
                            endpoint: "https://api.example.com".to_string(),
                            method: "POST".to_string(),
                            path: "/v1/completions".to_string(),
                            body: "{}".to_string(),
                            model: "test-model".to_string(),
                        }],
                    )
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS (
                         SELECT 1 FROM pg_stat_activity
                         WHERE datname = current_database()
                           AND wait_event_type = 'Lock' AND wait_event = 'advisory')",
                )
                .fetch_one(&pool)
                .await
                .unwrap();
                if waiting {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("upload never queued behind the partition lock");

        sqlx::query(
            "SELECT ensure_request_template_partition(
                 date_trunc('week', now() AT TIME ZONE 'UTC')::date, NULL)",
        )
        .execute(&mut *creator)
        .await
        .expect("partition creation must not deadlock with the upload");
        creator.commit().await.unwrap();
        upload
            .await
            .unwrap()
            .expect("upload must not deadlock with partition creation");
    }

    #[sqlx::test]
    async fn test_batched_insert_small_file(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let templates: Vec<RequestTemplateInput> = (0..10)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("batch-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: format!(r#"{{"prompt":"test {}"}}"#, i),
                model: "gpt-4".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file(
                "batched-small".to_string(),
                Some("Small file for batched insert test".to_string()),
                templates,
            )
            .await
            .expect("Failed to create file");

        let content = manager
            .get_file_content(file_id)
            .await
            .expect("Failed to get content");

        assert_eq!(content.len(), 10, "Should have 10 templates");

        for (i, item) in content.iter().enumerate() {
            match item {
                FileContentItem::Template(t) => {
                    assert_eq!(t.custom_id, Some(format!("batch-{}", i)));
                    assert_eq!(t.body, format!(r#"{{"prompt":"test {}"}}"#, i));
                }
                _ => panic!("Expected template"),
            }
        }
    }

    #[sqlx::test]
    async fn test_batched_insert_large_file(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let template_count = 15_000;
        let templates: Vec<RequestTemplateInput> = (0..template_count)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("large-batch-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: format!(r#"{{"prompt":"test {}","data":{}}}"#, i, "x".repeat(100)),
                model: "gpt-4".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file(
                "batched-large".to_string(),
                Some("Large file for batched insert test".to_string()),
                templates,
            )
            .await
            .expect("Failed to create file");

        let content = manager
            .get_file_content(file_id)
            .await
            .expect("Failed to get content");

        assert_eq!(
            content.len(),
            template_count,
            "Should have {} templates",
            template_count
        );

        match &content[0] {
            FileContentItem::Template(t) => {
                assert_eq!(t.custom_id, Some("large-batch-0".to_string()));
            }
            _ => panic!("Expected template"),
        }

        match &content[7500] {
            FileContentItem::Template(t) => {
                assert_eq!(t.custom_id, Some("large-batch-7500".to_string()));
            }
            _ => panic!("Expected template"),
        }

        match &content[14999] {
            FileContentItem::Template(t) => {
                assert_eq!(t.custom_id, Some("large-batch-14999".to_string()));
            }
            _ => panic!("Expected template"),
        }
    }

    #[sqlx::test]
    async fn test_batched_insert_preserves_line_numbers(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        )
        .with_batch_insert_strategy(BatchInsertStrategy::Batched { batch_size: 50 });

        let template_count = 150;
        let templates: Vec<RequestTemplateInput> = (0..template_count)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("line-{}", i)),
                endpoint: "https://api.openai.com/v1".to_string(),
                method: "POST".to_string(),
                path: "/chat/completions".to_string(),
                body: format!(
                    r#"{{"model":"gpt-4","messages":[{{"role":"user","content":"line {}"}}]}}"#,
                    i
                ),
                model: "gpt-4".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file("test-batched-lines".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        let rows = sqlx::query!(
            r#"
            SELECT custom_id, line_number AS "line_number!"
            FROM request_templates_all
            WHERE file_id = $1
            ORDER BY line_number
            "#,
            *file_id as Uuid,
        )
        .fetch_all(&pool)
        .await
        .expect("Failed to query templates");

        assert_eq!(rows.len(), template_count);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(
                row.line_number, i as i32,
                "Line number {} should be sequential",
                i
            );
            assert_eq!(row.custom_id.as_ref().unwrap(), &format!("line-{}", i));
        }
    }

    #[sqlx::test]
    async fn test_batched_insert_with_stream(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let mut items = vec![FileStreamItem::Metadata(FileMetadata {
            filename: Some("streamed-batched".to_string()),
            description: Some("Batched insert via stream".to_string()),
            purpose: None,
            expires_after_anchor: None,
            expires_after_seconds: None,
            size_bytes: None,
            uploaded_by: Some("test-user".to_string()),
            api_key_id: None,
            ..Default::default()
        })];

        for i in 0..8000 {
            items.push(FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some(format!("stream-batch-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: format!(r#"{{"n":{}}}"#, i),
                model: "gpt-4".to_string(),
            }));
        }

        let stream = stream::iter(items);
        let file_id = expect_stream_success(
            manager
                .create_file_stream(stream)
                .await
                .expect("Failed to create file from stream"),
        );

        let file = manager.get_file(file_id).await.expect("Failed to get file");
        assert_eq!(file.name, "streamed-batched");

        let content = manager
            .get_file_content(file_id)
            .await
            .expect("Failed to get content");

        assert_eq!(content.len(), 8000);

        match &content[4999] {
            FileContentItem::Template(t) => {
                assert_eq!(t.custom_id, Some("stream-batch-4999".to_string()));
            }
            _ => panic!("Expected template"),
        }

        match &content[5000] {
            FileContentItem::Template(t) => {
                assert_eq!(t.custom_id, Some("stream-batch-5000".to_string()));
            }
            _ => panic!("Expected template"),
        }
    }

    #[sqlx::test]
    async fn test_batched_insert_empty_batches(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let items = vec![FileStreamItem::Metadata(FileMetadata {
            filename: Some("empty-file".to_string()),
            description: Some("File with no templates".to_string()),
            purpose: None,
            expires_after_anchor: None,
            expires_after_seconds: None,
            size_bytes: None,
            uploaded_by: None,
            api_key_id: None,
            ..Default::default()
        })];

        let stream = stream::iter(items);
        let file_id = expect_stream_success(
            manager
                .create_file_stream(stream)
                .await
                .expect("Failed to create empty file"),
        );

        let file = manager.get_file(file_id).await.expect("Failed to get file");
        assert_eq!(file.name, "empty-file");

        let content = manager
            .get_file_content(file_id)
            .await
            .expect("Failed to get content");

        assert_eq!(content.len(), 0, "Should have no templates");
    }

    #[sqlx::test]
    #[allow(deprecated)]
    async fn test_batched_insert_transactional_rollback(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let mut items = vec![FileStreamItem::Metadata(FileMetadata {
            filename: Some("rollback-test".to_string()),
            description: Some("Should rollback on error".to_string()),
            purpose: None,
            expires_after_anchor: None,
            expires_after_seconds: None,
            size_bytes: None,
            uploaded_by: None,
            api_key_id: None,
            ..Default::default()
        })];

        for i in 0..3000 {
            items.push(FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some(format!("rollback-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: format!(r#"{{"n":{}}}"#, i),
                model: "gpt-4".to_string(),
            }));
        }

        items.push(FileStreamItem::Error("Simulated parse error".to_string()));

        for i in 3000..3100 {
            items.push(FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some(format!("rollback-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: format!(r#"{{"n":{}}}"#, i),
                model: "gpt-4".to_string(),
            }));
        }

        let stream = stream::iter(items);
        let result = manager.create_file_stream(stream).await;

        assert!(result.is_err());

        let files =
            sqlx::query!(r#"SELECT COUNT(*) as count FROM files WHERE name = 'rollback-test'"#)
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(files.count, Some(0), "File should not exist after rollback");
    }

    #[sqlx::test]
    async fn test_batched_insert_body_byte_size_calculation(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let unicode_body = r#"{"data":"hé🌍"}"#;
        let templates = vec![
            RequestTemplateInput {
                custom_id: Some("small".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: r#"{"a":1}"#.to_string(),
                model: "gpt-4".to_string(),
            },
            RequestTemplateInput {
                custom_id: Some("large".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: format!(r#"{{"data":"{}"}}"#, "x".repeat(5000)),
                model: "gpt-4".to_string(),
            },
            RequestTemplateInput {
                custom_id: Some("unicode".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: unicode_body.to_string(),
                model: "gpt-4".to_string(),
            },
        ];

        let file_id = manager
            .create_file("byte-size-test".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        let rows = sqlx::query!(
            r#"
            SELECT custom_id,
                   body_byte_size AS "body_byte_size!",
                   octet_length(body) as actual_bytes
            FROM request_templates_all
            WHERE file_id = $1
            ORDER BY line_number ASC
            "#,
            *file_id as Uuid,
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(rows.len(), 3);

        assert_eq!(rows[0].custom_id, Some("small".to_string()));
        assert_eq!(rows[0].body_byte_size, 7);
        assert_eq!(rows[0].actual_bytes, Some(7));

        assert_eq!(rows[1].custom_id, Some("large".to_string()));
        assert_eq!(rows[1].body_byte_size, rows[1].actual_bytes.unwrap() as i64);
        assert!(rows[1].body_byte_size > 5000);

        assert_eq!(rows[2].custom_id, Some("unicode".to_string()));
        assert_eq!(rows[2].body_byte_size, unicode_body.len() as i64);
        assert_eq!(rows[2].body_byte_size, rows[2].actual_bytes.unwrap() as i64);
    }

    #[sqlx::test]
    async fn test_batched_insert_performance_comparison(pool: sqlx::PgPool) {
        use std::time::Instant;

        let http_client = Arc::new(MockHttpClient::new());

        let manager_batched = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        );

        let template_count = 1000;
        let templates: Vec<RequestTemplateInput> = (0..template_count)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("perf-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: format!(r#"{{"prompt":"test {}","data":{}}}"#, i, "x".repeat(50)),
                model: "gpt-4".to_string(),
            })
            .collect();

        let start = Instant::now();
        let file_id = manager_batched
            .create_file("perf-test-batched".to_string(), None, templates)
            .await
            .expect("Failed to create file");
        let batched_duration = start.elapsed();

        let content = manager_batched
            .get_file_content(file_id)
            .await
            .expect("Failed to get content");

        assert_eq!(content.len(), template_count);

        println!(
            "Batched insert of {} templates took: {:?}",
            template_count, batched_duration
        );

        assert!(
            batched_duration.as_secs() < 2,
            "Batched insert should be fast"
        );
    }

    #[sqlx::test]
    #[should_panic(expected = "batch_size must be greater than 0")]
    async fn test_batched_insert_rejects_zero_batch_size(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let _manager =
            PostgresRequestManager::with_client(TestDbPools::new(pool).await.unwrap(), http_client)
                .with_batch_insert_strategy(BatchInsertStrategy::Batched { batch_size: 0 });
    }

    #[sqlx::test]
    async fn test_batched_insert_valid_batch_sizes(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        let manager1 = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_batch_insert_strategy(BatchInsertStrategy::Batched { batch_size: 1 });

        let templates: Vec<RequestTemplateInput> = (0..5)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("req-{}", i)),
                endpoint: "/v1/chat/completions".to_string(),
                method: "POST".to_string(),
                path: "/v1/chat/completions".to_string(),
                body: format!(
                    r#"{{"model":"gpt-4","messages":[{{"role":"user","content":"test {}"}}]}}"#,
                    i
                ),
                model: "gpt-4".to_string(),
            })
            .collect();

        let file_id1 = manager1
            .create_file("batch-size-1".to_string(), None, templates.clone())
            .await
            .expect("Failed to create file with batch_size=1");

        let content1 = manager1
            .get_file_content(file_id1)
            .await
            .expect("Failed to get content");
        assert_eq!(
            content1.len(),
            5,
            "Should have 5 templates with batch_size=1"
        );

        let manager100 = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_batch_insert_strategy(BatchInsertStrategy::Batched { batch_size: 100 });

        let file_id100 = manager100
            .create_file("batch-size-100".to_string(), None, templates.clone())
            .await
            .expect("Failed to create file with batch_size=100");

        let content100 = manager100
            .get_file_content(file_id100)
            .await
            .expect("Failed to get content");
        assert_eq!(
            content100.len(),
            5,
            "Should have 5 templates with batch_size=100"
        );

        let manager5000 =
            PostgresRequestManager::with_client(TestDbPools::new(pool).await.unwrap(), http_client)
                .with_batch_insert_strategy(BatchInsertStrategy::Batched { batch_size: 5000 });

        let file_id5000 = manager5000
            .create_file("batch-size-5000".to_string(), None, templates)
            .await
            .expect("Failed to create file with batch_size=5000");

        let content5000 = manager5000
            .get_file_content(file_id5000)
            .await
            .expect("Failed to get content");
        assert_eq!(
            content5000.len(),
            5,
            "Should have 5 templates with batch_size=5000"
        );
    }


    #[sqlx::test]
    async fn test_create_batch_and_get_status(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "batch-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"1"}"#.to_string(),
                        model: "gpt-4".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"2"}"#.to_string(),
                        model: "gpt-4".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"3"}"#.to_string(),
                        model: "gpt-4".to_string(),
                    },
                ],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .expect("Failed to create batch");

        let status = manager
            .get_batch_status(batch.id)
            .await
            .expect("Failed to get batch status");

        assert_eq!(status.batch_id, batch.id);
        assert_eq!(status.file_id, Some(file_id));
        assert_eq!(status.file_name, Some("batch-test".to_string()));
        assert_eq!(status.total_requests, 3);
        assert_eq!(status.pending_requests, 3);
        assert_eq!(status.completed_requests, 0);
        assert_eq!(status.failed_requests, 0);

        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get batch requests");

        assert_eq!(requests.len(), 3);
        for request in requests {
            assert!(request.is_pending());
        }
    }

    #[sqlx::test]
    async fn background_creation_batch_populates_without_a_deadline(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let file_id = manager
            .create_file(
                "background-batch.jsonl".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("background-1".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/responses".to_string(),
                    body: r#"{"model":"model-a","input":"hello"}"#.to_string(),
                    model: "model-a".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_background_batch(crate::batch::BackgroundBatchInput {
                file_id,
                endpoint: "/v1/responses".to_string(),
                metadata: Some(serde_json::json!({"kind": "background"})),
                created_by: Some("user-a".to_string()),
                api_key_id: None,
                total_requests: Some(1),
            })
            .await
            .expect("background batch should be created");

        assert_eq!(batch.service_tier.as_deref(), Some("background"));
        assert_eq!(batch.completion_window, None);
        assert_eq!(batch.expires_at, None);
        assert!(batch.output_file_id.is_some());
        assert!(batch.error_file_id.is_some());
        assert_eq!(batch.total_requests, 1);
        assert_eq!(batch.pending_requests, 1);

        let stored = manager.get_batch(batch.id).await.unwrap();
        assert_eq!(stored.service_tier.as_deref(), Some("background"));
        assert_eq!(stored.completion_window, None);
        assert_eq!(stored.expires_at, None);

        let tiers: Vec<Option<String>> = sqlx::query_scalar(
            "SELECT service_tier FROM requests WHERE batch_id = $1 ORDER BY created_at, id",
        )
        .bind(*batch.id as Uuid)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(tiers, vec![Some("background".to_string())]);
    }

    #[sqlx::test]
    async fn background_creation_rejects_mixed_batch_deadline_fields(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let file_id = manager
            .create_file("background-constraint.jsonl".to_string(), None, vec![])
            .await
            .unwrap();
        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/responses".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: Some("user-a".to_string()),
                api_key_id: None,
                total_requests: Some(0),
            })
            .await;
        assert!(batch.is_err(), "empty source file should fail population");

        let batch_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM batches WHERE file_id = $1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(*file_id as Uuid)
        .fetch_one(&pool)
        .await
        .unwrap();

        let result = sqlx::query("UPDATE batches SET service_tier = 'background' WHERE id = $1")
            .bind(batch_id)
            .execute(&pool)
            .await;
        assert!(
            result.is_err(),
            "background tier with an SLA deadline must violate the cross-field constraint"
        );
    }


    async fn populate_guard_setup(
        pool: sqlx::PgPool,
    ) -> (Arc<PostgresRequestManager<TestDbPools>>, FileId, BatchId) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = Arc::new(PostgresRequestManager::with_client(
            TestDbPools::new(pool).await.unwrap(),
            http_client,
        ));

        let file_id = manager
            .create_file(
                "populate-guard".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"1"}"#.to_string(),
                    model: "gpt-4".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch_record(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        (manager, file_id, batch.id)
    }

    #[sqlx::test]
    async fn test_populate_batch_rejects_when_source_file_deleted(pool: sqlx::PgPool) {
        let (manager, file_id, batch_id) = populate_guard_setup(pool).await;

        manager.delete_file(file_id).await.unwrap();

        manager
            .populate_batch(batch_id, file_id)
            .await
            .expect("populate_batch is a no-op after source file deleted");
    }

    #[sqlx::test]
    async fn test_populate_batch_rejects_when_batch_cancelled(pool: sqlx::PgPool) {
        let (manager, file_id, batch_id) = populate_guard_setup(pool).await;

        manager.cancel_batch(batch_id).await.unwrap();

        manager
            .populate_batch(batch_id, file_id)
            .await
            .expect("populate_batch is a no-op after batch cancelled");
    }

    #[sqlx::test]
    async fn test_populate_batch_rejects_when_batch_deleted(pool: sqlx::PgPool) {
        let (manager, file_id, batch_id) = populate_guard_setup(pool).await;

        manager.delete_batch(batch_id).await.unwrap();

        manager
            .populate_batch(batch_id, file_id)
            .await
            .expect("populate_batch is a no-op after batch deleted");
    }

    #[sqlx::test]
    async fn test_populate_batch_is_idempotent(pool: sqlx::PgPool) {
        let (manager, file_id, batch_id) = populate_guard_setup(pool).await;

        manager.populate_batch(batch_id, file_id).await.unwrap();
        let after_first = manager.get_batch(batch_id).await.unwrap();
        assert_eq!(
            after_first.total_requests, 1,
            "first populate should snapshot the one template"
        );
        assert_eq!(
            manager.get_batch_requests(batch_id).await.unwrap().len(),
            1,
            "first populate should create exactly one request row"
        );

        manager.populate_batch(batch_id, file_id).await.expect(
            "re-running populate_batch on an already-populated batch should succeed as a no-op",
        );

        let after_second = manager.get_batch(batch_id).await.unwrap();
        assert_eq!(
            after_second.total_requests, 1,
            "re-population must not change total_requests"
        );
        assert_eq!(
            manager.get_batch_requests(batch_id).await.unwrap().len(),
            1,
            "re-population must not create duplicate request rows"
        );
    }


    #[sqlx::test]
    async fn background_claims_require_explicit_live_and_separate_modalities(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(DaemonConfig {
            background_concurrency_limit: 10,
            ..Default::default()
        });
        let batch =
            create_background_batch_for_test(&manager, "background-model", "batch-owner").await;
        let batchless =
            create_background_request_for_test(&manager, "background-model", "request-owner").await;
        let capacity = HashMap::from([("background-model".to_string(), 10)]);
        let daemon_id = DaemonId::from(Uuid::new_v4());

        let unmanaged = manager
            .claim_background_batchless_requests(10, daemon_id, &capacity, &HashMap::new())
            .await
            .expect("background claim should be supported");
        assert!(
            unmanaged.is_empty(),
            "a missing liveness event is ineligible"
        );

        manager
            .append_model_filter_events(&[ModelFilter {
                model: "background-model".to_string(),
                state: ModelFilterState::Coming,
                expected_ready_at: None,
            }])
            .await
            .unwrap();
        let coming = manager
            .claim_background_batch_requests(10, 10, daemon_id, &capacity, &HashMap::new())
            .await
            .unwrap();
        assert!(coming.is_empty());

        mark_models_live_for_test(&manager, ["background-model"]).await;
        let batchless_claimed = manager
            .claim_background_batchless_requests(10, daemon_id, &capacity, &HashMap::new())
            .await
            .unwrap();
        assert_eq!(batchless_claimed.len(), 1);
        assert_eq!(batchless_claimed[0].data.id, batchless);
        assert_eq!(batchless_claimed[0].data.batch_id, None);

        let batch_claimed = manager
            .claim_background_batch_requests(10, 10, daemon_id, &capacity, &HashMap::new())
            .await
            .unwrap();
        assert_eq!(batch_claimed.len(), 1);
        assert_eq!(batch_claimed[0].data.batch_id, Some(batch.id));

        assert!(
            batchless_claimed
                .iter()
                .chain(batch_claimed.iter())
                .all(|request| request.state.batch_expires_at.is_none())
        );
        assert!(
            batchless_claimed
                .iter()
                .chain(batch_claimed.iter())
                .all(|request| {
                    request
                        .data
                        .batch_metadata
                        .get("completion_window")
                        .map(String::as_str)
                        == Some("background")
                })
        );
    }

    #[sqlx::test]
    async fn background_batch_constraint_rejects_missing_tier_and_deadline(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let file_id = manager
            .create_file("invalid-background-shape".to_string(), None, Vec::new())
            .await
            .unwrap();

        let error = sqlx::query!(
            r#"
            INSERT INTO batches (
                file_id, endpoint, created_by, service_tier,
                completion_window, expires_at
            )
            VALUES ($1, '/v1/responses', 'owner', NULL, NULL, NULL)
            "#,
            *file_id as Uuid,
        )
        .execute(&pool)
        .await
        .expect_err("an SLA batch without a completion window or expiry must be rejected");

        let constraint = error
            .as_database_error()
            .and_then(|error| error.constraint());
        assert_eq!(constraint, Some("batches_background_deadline_check"));
    }

    #[sqlx::test]
    async fn background_claim_threshold_counts_only_foreground_work(pool: sqlx::PgPool) {
        let config = DaemonConfig {
            background_concurrency_limit: 1,
            ..Default::default()
        };
        let first_manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(config.clone());
        let second_manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(config);

        create_background_request_for_test(&first_manager, "global-model", "owner-a").await;
        create_background_request_for_test(&first_manager, "global-model", "owner-b").await;
        mark_models_live_for_test(&first_manager, ["global-model"]).await;

        let capacity = HashMap::from([("global-model".to_string(), 1)]);
        let first = first_manager
            .claim_background_batchless_requests(
                10,
                DaemonId::from(Uuid::new_v4()),
                &capacity,
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(first.len(), 1);

        let second = second_manager
            .claim_background_batchless_requests(
                10,
                DaemonId::from(Uuid::new_v4()),
                &capacity,
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            second.len(),
            1,
            "active background work must not consume foreground headroom"
        );

        create_background_request_for_test(
            &first_manager,
            "foreground-saturated-model",
            "background-owner",
        )
        .await;
        first_manager
            .create_flex(CreateFlexInput {
                request_id: Uuid::new_v4(),
                body: "{}".to_string(),
                model: "foreground-saturated-model".to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: "foreground-owner".to_string(),
                metadata: None,
            })
            .await
            .unwrap();
        mark_models_live_for_test(&first_manager, ["foreground-saturated-model"]).await;
        let foreground_capacity = HashMap::from([("foreground-saturated-model".to_string(), 1)]);
        let foreground = first_manager
            .claim_batchless_requests(
                1,
                DaemonId::from(Uuid::new_v4()),
                &foreground_capacity,
                &HashMap::new(),
                &HashSet::new(),
            )
            .await
            .unwrap();
        assert_eq!(foreground.len(), 1);

        let blocked = second_manager
            .claim_background_batchless_requests(
                10,
                DaemonId::from(Uuid::new_v4()),
                &foreground_capacity,
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert!(
            blocked.is_empty(),
            "active foreground work must consume background-admission headroom"
        );
    }

    #[sqlx::test]
    async fn background_claim_waits_for_due_sla_work(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(DaemonConfig {
            background_concurrency_limit: 2,
            ..Default::default()
        });
        let background_id =
            create_background_request_for_test(&manager, "shared-model", "background-owner").await;
        let flex_id = manager
            .create_flex(CreateFlexInput {
                request_id: Uuid::new_v4(),
                body: "{}".to_string(),
                model: "shared-model".to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: "sla-owner".to_string(),
                metadata: None,
            })
            .await
            .unwrap();
        mark_models_live_for_test(&manager, ["shared-model"]).await;
        let capacity = HashMap::from([("shared-model".to_string(), 1)]);
        let daemon_id = DaemonId::from(Uuid::new_v4());

        let blocked = manager
            .claim_background_requests(10, 10, daemon_id, &capacity, &HashMap::new())
            .await
            .unwrap();
        assert!(
            blocked.is_empty(),
            "due SLA work must block background claims"
        );

        let sla = manager
            .claim_batchless_requests(10, daemon_id, &capacity, &HashMap::new(), &HashSet::new())
            .await
            .unwrap();
        assert_eq!(sla.len(), 1);
        assert_eq!(sla[0].data.id, flex_id);

        let background = manager
            .claim_background_requests(10, 10, daemon_id, &capacity, &HashMap::new())
            .await
            .unwrap();
        assert_eq!(background.len(), 1);
        assert_eq!(background[0].data.id, background_id);
    }

    #[sqlx::test]
    async fn background_claim_ignores_future_sla_retry_but_not_due_batch(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(DaemonConfig {
            background_concurrency_limit: 1,
            ..Default::default()
        });
        let background_id =
            create_background_request_for_test(&manager, "retry-model", "background-owner").await;
        let future_flex = manager
            .create_flex(CreateFlexInput {
                request_id: Uuid::new_v4(),
                body: "{}".to_string(),
                model: "retry-model".to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: "sla-owner".to_string(),
                metadata: None,
            })
            .await
            .unwrap();
        sqlx::query!(
            "UPDATE requests SET not_before = NOW() + interval '1 hour' WHERE id = $1",
            *future_flex as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();
        mark_models_live_for_test(&manager, ["retry-model"]).await;
        let capacity = HashMap::from([("retry-model".to_string(), 1)]);

        let claimed = manager
            .claim_background_requests(
                10,
                10,
                DaemonId::from(Uuid::new_v4()),
                &capacity,
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].data.id, background_id);

        create_background_request_for_test(&manager, "batch-blocked-model", "background-owner")
            .await;
        let file_id = manager
            .create_file(
                "due-sla-batch".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "batch-blocked-model".to_string(),
                }],
            )
            .await
            .unwrap();
        manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: Some("sla-owner".to_string()),
                api_key_id: None,
                total_requests: Some(1),
            })
            .await
            .unwrap();
        mark_models_live_for_test(&manager, ["batch-blocked-model"]).await;
        let blocked = manager
            .claim_background_requests(
                10,
                10,
                DaemonId::from(Uuid::new_v4()),
                &HashMap::from([("batch-blocked-model".to_string(), 1)]),
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert!(blocked.is_empty());
    }

    #[sqlx::test]
    async fn background_claim_enforces_capacity_batch_bound_and_owner_fairness(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(DaemonConfig {
            background_concurrency_limit: 10,
            ..Default::default()
        });
        let batch =
            create_background_batch_for_test(&manager, "bounded-model", "batch-owner").await;
        let busy_owner =
            create_background_request_for_test(&manager, "bounded-model", "busy-owner").await;
        let idle_owner =
            create_background_request_for_test(&manager, "bounded-model", "idle-owner").await;
        mark_models_live_for_test(&manager, ["bounded-model"]).await;
        let capacity = HashMap::from([("bounded-model".to_string(), 1)]);
        let active = HashMap::from([("busy-owner".to_string(), 5)]);

        let first = manager
            .claim_background_requests(10, 0, DaemonId::from(Uuid::new_v4()), &capacity, &active)
            .await
            .unwrap();
        assert_eq!(first.len(), 1, "per-model capacity must cap the claim");
        assert_eq!(first[0].data.id, idle_owner, "the less-active owner wins");
        assert_eq!(
            first[0].data.batch_id, None,
            "batch_limit=0 excludes batches"
        );

        let second = manager
            .claim_background_requests(
                1,
                10,
                DaemonId::from(Uuid::new_v4()),
                &HashMap::from([("bounded-model".to_string(), 10)]),
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(second.len(), 1, "the global cycle limit must be enforced");
        assert!(second[0].data.id == busy_owner || second[0].data.batch_id == Some(batch.id));
    }

    #[sqlx::test]
    async fn combined_background_claim_shares_model_capacity_between_modalities(
        pool: sqlx::PgPool,
    ) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(DaemonConfig {
            background_concurrency_limit: 10,
            ..Default::default()
        });
        create_background_request_for_test(&manager, "shared-capacity-model", "request-owner")
            .await;
        create_background_batch_for_test(&manager, "shared-capacity-model", "batch-owner").await;
        mark_models_live_for_test(&manager, ["shared-capacity-model"]).await;

        let claimed = manager
            .claim_background_requests(
                10,
                10,
                DaemonId::from(Uuid::new_v4()),
                &HashMap::from([("shared-capacity-model".to_string(), 1)]),
                &HashMap::new(),
            )
            .await
            .unwrap();

        assert_eq!(
            claimed.len(),
            1,
            "the combined claim must share per-model capacity between modalities"
        );
    }

    #[sqlx::test]
    async fn ordinary_claims_never_return_background_rows(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        create_background_batch_for_test(&manager, "isolated-model", "batch-owner").await;
        create_background_request_for_test(&manager, "isolated-model", "request-owner").await;
        mark_models_live_for_test(&manager, ["isolated-model"]).await;
        let capacity = HashMap::from([("isolated-model".to_string(), 10)]);
        let daemon_id = DaemonId::from(Uuid::new_v4());

        let batchless = manager
            .claim_batchless_requests(10, daemon_id, &capacity, &HashMap::new(), &HashSet::new())
            .await
            .unwrap();
        assert!(batchless.is_empty());

        let batched = manager
            .claim_batch_requests(10, 10, daemon_id, &capacity, &HashMap::new())
            .await
            .unwrap();
        assert!(batched.is_empty());
    }

    #[sqlx::test]
    async fn background_pending_counts_are_explicit_and_isolated(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(DaemonConfig {
            background_concurrency_limit: 10,
            ..Default::default()
        });
        create_background_batch_for_test(&manager, "count-model", "batch-owner").await;
        create_background_request_for_test(&manager, "count-model", "request-owner").await;
        mark_models_live_for_test(&manager, ["count-model"]).await;

        let windows = vec![("24h".to_string(), None, 86_400)];
        let states = vec!["pending".to_string()];
        let ordinary = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &states,
                &[],
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert!(!ordinary.contains_key("count-model"));

        let background = manager
            .get_pending_request_counts_by_model_and_window(
                &[],
                &states,
                &[],
                &ServiceTierFilter::Include(vec![Some("background".to_string())]),
                false,
            )
            .await
            .unwrap();
        assert_eq!(background["count-model"]["background"], 2);
    }

    #[sqlx::test]
    async fn background_pending_counts_include_non_live_models(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(DaemonConfig {
            background_concurrency_limit: 0,
            ..Default::default()
        });
        create_background_request_for_test(&manager, "absent-model", "request-owner").await;

        let result = manager
            .get_pending_request_counts_by_model_and_window(
                &[],
                &["pending".to_string()],
                &[],
                &ServiceTierFilter::Include(vec![Some("background".to_string())]),
                false,
            )
            .await
            .unwrap();

        assert_eq!(result["absent-model"]["background"], 1);
    }

    #[sqlx::test]
    async fn background_pending_counts_ignore_dispatch_capacity_and_due_sla(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(DaemonConfig {
            background_concurrency_limit: 1,
            ..Default::default()
        });

        for model in [
            "available-model",
            "background-active-model",
            "full-model",
            "sla-blocked-model",
        ] {
            create_background_request_for_test(&manager, model, "background-owner").await;
        }
        create_background_request_for_test(
            &manager,
            "background-active-model",
            "second-background-owner",
        )
        .await;
        manager
            .create_realtime(CreateRealtimeInput {
                request_id: Uuid::new_v4(),
                body: "{}".to_string(),
                model: "full-model".to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                created_by: "active-owner".to_string(),
            })
            .await
            .unwrap();
        manager
            .create_flex(CreateFlexInput {
                request_id: Uuid::new_v4(),
                body: "{}".to_string(),
                model: "sla-blocked-model".to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: "sla-owner".to_string(),
                metadata: None,
            })
            .await
            .unwrap();
        mark_models_live_for_test(
            &manager,
            [
                "available-model",
                "background-active-model",
                "full-model",
                "sla-blocked-model",
            ],
        )
        .await;
        let claimed_background = manager
            .claim_background_batchless_requests(
                1,
                DaemonId::from(Uuid::new_v4()),
                &HashMap::from([("background-active-model".to_string(), 1)]),
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(claimed_background.len(), 1);

        let result = manager
            .get_pending_request_counts_by_model_and_window(
                &[],
                &["pending".to_string()],
                &[],
                &ServiceTierFilter::Include(vec![Some("background".to_string())]),
                false,
            )
            .await
            .unwrap();

        assert_eq!(result["available-model"]["background"], 1);
        assert_eq!(
            result["background-active-model"]["background"], 1,
            "active background work must not hide remaining background demand"
        );
        assert_eq!(result["full-model"]["background"], 1);
        assert_eq!(result["sla-blocked-model"]["background"], 1);
    }

    #[sqlx::test]
    async fn test_claim_batchless_requests_ignores_batched_rows(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        let batch_file = manager
            .create_file(
                "batched-row".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("batched".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();
        manager
            .create_batch(crate::batch::BatchInput {
                file_id: batch_file,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let flex_uuid = Uuid::new_v4();
        let flex_id = manager
            .create_flex(CreateFlexInput {
                request_id: flex_uuid,
                body: r#"{"model":"test","service_tier":"flex"}"#.to_string(),
                model: "test".to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: "user".to_string(),
                metadata: None,
            })
            .await
            .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("test".to_string(), 10)]);
        let claimed = manager
            .claim_batchless_requests(10, daemon_id, &capacity, &HashMap::new(), &HashSet::new())
            .await
            .expect("Failed to claim batchless requests");

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].data.id, flex_id);
        assert_eq!(claimed[0].data.batch_id, None);
    }

    #[sqlx::test]
    async fn batchless_claim_forwards_listed_template_metadata(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(PostgresStorageConfig {
            batch_metadata_fields: vec![
                "completion_window".to_string(),
                "dw_user_agent".to_string(),
            ],
            ..Default::default()
        });

        let request_id = Uuid::new_v4();
        manager
            .create_flex(CreateFlexInput {
                request_id,
                body: r#"{"model":"test","service_tier":"flex"}"#.to_string(),
                model: "test".to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: "user".to_string(),
                metadata: Some(serde_json::json!({
                    "dw_user_agent": "claude-cli/1.2.3",
                    "dw_unlisted": "must not be forwarded",
                })),
            })
            .await
            .unwrap();

        let claimed = manager
            .claim_batchless_requests(
                10,
                DaemonId::from(Uuid::new_v4()),
                &HashMap::from([("test".to_string(), 10)]),
                &HashMap::new(),
                &HashSet::new(),
            )
            .await
            .expect("Failed to claim batchless requests");

        assert_eq!(claimed.len(), 1);
        let metadata = &claimed[0].data.batch_metadata;
        assert_eq!(
            metadata.get("dw_user_agent").map(String::as_str),
            Some("claude-cli/1.2.3"),
            "a listed template metadata key must reach the dispatched request"
        );
        assert!(
            !metadata.contains_key("dw_unlisted"),
            "an unlisted key must not be forwarded: the allow-list is the contract"
        );
        assert_eq!(
            metadata.get("completion_window").map(String::as_str),
            Some("1h")
        );
    }

    #[sqlx::test]
    async fn batch_claim_forwards_listed_batch_metadata(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(PostgresStorageConfig {
            batch_metadata_fields: vec!["id".to_string(), "dw_user_agent".to_string()],
            ..Default::default()
        });

        let file_id = manager
            .create_file(
                "ua-batch".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("ua".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();
        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: Some(serde_json::json!({
                    "dw_user_agent": "OpenAI/Python 1.2.3",
                    "dw_unlisted": "must not be forwarded",
                })),
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let claimed = manager
            .claim_batch_requests(
                10,
                10,
                DaemonId::from(Uuid::new_v4()),
                &HashMap::from([("test".to_string(), 10)]),
                &HashMap::new(),
            )
            .await
            .expect("Failed to claim batch requests");

        assert_eq!(claimed.len(), 1);
        let metadata = &claimed[0].data.batch_metadata;
        assert_eq!(
            metadata.get("dw_user_agent").map(String::as_str),
            Some("OpenAI/Python 1.2.3"),
            "a listed batch metadata key must reach the dispatched request"
        );
        assert!(
            !metadata.contains_key("dw_unlisted"),
            "an unlisted key must not be forwarded: the allow-list is the contract"
        );
    }

    #[sqlx::test]
    async fn batchless_claim_without_metadata_keeps_synthesized_fields(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        manager
            .create_flex(CreateFlexInput {
                request_id: Uuid::new_v4(),
                body: r#"{"model":"test","service_tier":"flex"}"#.to_string(),
                model: "test".to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: "user".to_string(),
                metadata: None,
            })
            .await
            .unwrap();

        let claimed = manager
            .claim_batchless_requests(
                10,
                DaemonId::from(Uuid::new_v4()),
                &HashMap::from([("test".to_string(), 10)]),
                &HashMap::new(),
                &HashSet::new(),
            )
            .await
            .expect("Failed to claim batchless requests");

        assert_eq!(claimed.len(), 1);
        let metadata = &claimed[0].data.batch_metadata;
        assert!(!metadata.contains_key("dw_user_agent"));
        assert_eq!(
            metadata.get("completion_window").map(String::as_str),
            Some("1h")
        );
        assert!(
            metadata.contains_key("created_at"),
            "submission epoch drives the SLO metrics"
        );
    }

    #[sqlx::test]
    async fn test_claim_batch_requests_leaks_one_per_bucket(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        sqlx::query("ALTER TABLE batches DROP CONSTRAINT batches_background_deadline_check")
            .execute(&pool)
            .await
            .unwrap();
        for (user, model, window) in [
            ("alice", "large-a", "24h"),
            ("alice", "large-a", ""),
            ("bob", "large-a", "24h"),
            ("alice", "large-b", "24h"),
            ("alice", "large-a", "1h"),
        ] {
            let file = manager
                .create_file(
                    "tiny-batch".into(),
                    Some(user.into()),
                    (0..3)
                        .map(|n| RequestTemplateInput {
                            custom_id: Some(n.to_string()),
                            endpoint: "https://api.example.com".into(),
                            method: "POST".into(),
                            path: "/test".into(),
                            body: "{}".into(),
                            model: model.into(),
                        })
                        .collect(),
                )
                .await
                .unwrap();
            manager
                .create_batch(crate::batch::BatchInput {
                    file_id: file,
                    endpoint: "/v1/chat/completions".into(),
                    completion_window: if window.is_empty() { "24h" } else { window }.into(),
                    metadata: None,
                    created_by: Some(user.into()),
                    api_key_id: None,
                    total_requests: None,
                })
                .await
                .unwrap();
            if window.is_empty() {
                sqlx::query("UPDATE batches SET completion_window = NULL WHERE file_id = $1")
                    .bind(*file)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
        }
        for model in ["large-a", "large-b"] {
            mark_not_live(&manager, model).await;
        }
        let capacity = HashMap::from([("large-a".into(), 100), ("large-b".into(), 100)]);
        let daemon = DaemonId::from(Uuid::new_v4());
        for _ in 0..2 {
            let legacy = manager
                .claim_batch_requests(100, 100, daemon, &capacity, &HashMap::new())
                .await
                .unwrap();
            assert!(
                legacy.is_empty(),
                "the legacy method must not leak on any call"
            );
        }
        let first = manager
            .claim_batch_requests_with_cooldown(
                100,
                100,
                daemon,
                &capacity,
                &HashMap::new(),
                &std::collections::HashSet::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            first.len(),
            4,
            "one immediate request per bucket, not per batch"
        );
        let cooldown: std::collections::HashSet<_> = first
            .iter()
            .map(|r| {
                let stamp = r
                    .state
                    .leak
                    .as_ref()
                    .expect("unavailable batches must stamp cooldown");
                (
                    r.data.created_by.clone(),
                    stamp.window_class.clone(),
                    r.data.model.clone(),
                )
            })
            .collect();
        assert_eq!(cooldown.len(), 4);
        let blocked = manager
            .claim_batch_requests_with_cooldown(
                100,
                100,
                daemon,
                &capacity,
                &HashMap::new(),
                &cooldown,
            )
            .await
            .unwrap();
        assert!(
            blocked.is_empty(),
            "no further claims before the interval elapses"
        );
        let next = manager
            .claim_batch_requests_with_cooldown(
                100,
                100,
                daemon,
                &capacity,
                &HashMap::new(),
                &std::collections::HashSet::new(),
            )
            .await
            .unwrap();
        assert_eq!(next.len(), 4);
        assert!(next.iter().all(|r| r.state.leak.is_some()));
    }

    #[sqlx::test]
    async fn test_claim_batch_requests_ramp_precedes_leaks_at_every_limit(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(DaemonConfig {
            urgency_weight: 0.5,
            ..Default::default()
        });
        for user in ["busy", "idle-1", "idle-2", "idle-3", "idle-4"] {
            let file_id = manager
                .create_file(
                    user.into(),
                    Some(user.into()),
                    (0..3)
                        .map(|n| RequestTemplateInput {
                            custom_id: Some(n.to_string()),
                            endpoint: "https://api.example.com".into(),
                            method: "POST".into(),
                            path: "/test".into(),
                            body: "{}".into(),
                            model: "large".into(),
                        })
                        .collect(),
                )
                .await
                .unwrap();
            manager
                .create_batch(crate::batch::BatchInput {
                    file_id,
                    endpoint: "/v1/chat/completions".into(),
                    completion_window: "24h".into(),
                    metadata: None,
                    created_by: Some(user.into()),
                    api_key_id: None,
                    total_requests: None,
                })
                .await
                .unwrap();
        }
        mark_not_live(&manager, "large").await;
        sqlx::query("UPDATE batches SET created_at = NOW() - INTERVAL '4 hours', expires_at = NOW() + INTERVAL '20 hours'")
            .execute(&pool).await.unwrap();
        sqlx::query("UPDATE batches SET created_at = NOW() - INTERVAL '23 hours', expires_at = NOW() + INTERVAL '5 minutes' WHERE created_by = 'busy'")
            .execute(&pool).await.unwrap();
        for (batch_limit, capacity, limit) in [(4, 100, 100), (5, 1, 100), (5, 100, 1)] {
            let claimed = manager
                .claim_batch_requests_with_cooldown(
                    limit,
                    batch_limit,
                    DaemonId::from(Uuid::new_v4()),
                    &HashMap::from([("large".into(), capacity)]),
                    &HashMap::from([("busy".into(), 100)]),
                    &HashSet::new(),
                )
                .await
                .unwrap();
            let rescued: Vec<_> = claimed
                .iter()
                .filter(|r| r.data.created_by == "busy")
                .collect();
            assert_eq!(
                rescued.len(),
                3.min(capacity).min(limit),
                "ramp claims must survive batch_limit={batch_limit}, capacity={capacity}, limit={limit}"
            );
            assert!(rescued.iter().all(|r| r.state.leak.is_none()));
            sqlx::query(
                "UPDATE requests SET state = 'pending', daemon_id = NULL, claimed_at = NULL",
            )
            .execute(&pool)
            .await
            .unwrap();
        }
    }

    #[sqlx::test]
    async fn test_claim_batch_requests_live_and_unmanaged_bypass_cooldown(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        for model in ["live-model", "coming-model", "unmanaged-model"] {
            let file_id = manager
                .create_file(
                    format!("{model}-batch"),
                    None,
                    vec![RequestTemplateInput {
                        custom_id: Some(model.to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: model.to_string(),
                    }],
                )
                .await
                .unwrap();
            manager
                .create_batch(crate::batch::BatchInput {
                    file_id,
                    endpoint: "/v1/chat/completions".to_string(),
                    completion_window: "24h".to_string(),
                    metadata: None,
                    created_by: None,
                    api_key_id: None,
                    total_requests: None,
                })
                .await
                .unwrap();
        }

        manager
            .append_model_filter_events(&[
                ModelFilter {
                    model: "live-model".to_string(),
                    state: ModelFilterState::Live,
                    expected_ready_at: None,
                },
                ModelFilter {
                    model: "coming-model".to_string(),
                    state: ModelFilterState::Coming,
                    expected_ready_at: Some(Utc::now() + chrono::Duration::minutes(10)),
                },
            ])
            .await
            .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([
            ("live-model".to_string(), 10),
            ("coming-model".to_string(), 10),
            ("unmanaged-model".to_string(), 10),
        ]);

        let cooldown = ["live-model", "coming-model", "unmanaged-model"]
            .into_iter()
            .map(|model| (String::new(), "24h".to_string(), model.to_string()))
            .collect();

        let strict_manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(DaemonConfig {
            batch_claim_require_live: true,
            ..Default::default()
        });
        let claimed = strict_manager
            .claim_batch_requests_with_cooldown(
                10,
                1,
                daemon_id,
                &capacity,
                &HashMap::new(),
                &cooldown,
            )
            .await
            .expect("Failed to claim batch requests");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].data.model, "live-model");
        assert!(claimed[0].data.batch_id.is_some());

        let claimed = manager
            .claim_batch_requests_with_cooldown(
                10,
                1,
                daemon_id,
                &capacity,
                &HashMap::new(),
                &cooldown,
            )
            .await
            .expect("Failed to claim batch requests");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].data.model, "unmanaged-model");
        assert!(claimed[0].data.batch_id.is_some());
    }

    #[sqlx::test]
    async fn test_claim_batch_requests_deadline_ramp_escape_hatch(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        let file_id = manager
            .create_file(
                "ramp-batch".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("ramp".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "ramp-model".to_string(),
                }],
            )
            .await
            .unwrap();
        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        manager
            .append_model_filter_events(&[ModelFilter {
                model: "ramp-model".to_string(),
                state: ModelFilterState::Coming,
                expected_ready_at: Some(Utc::now() + chrono::Duration::minutes(30)),
            }])
            .await
            .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("ramp-model".to_string(), 10)]);

        let cooldown = std::collections::HashSet::from([(
            String::new(),
            "1h".to_string(),
            "ramp-model".to_string(),
        )]);

        let claimed = manager
            .claim_batch_requests_with_cooldown(
                10,
                1,
                daemon_id,
                &capacity,
                &HashMap::new(),
                &cooldown,
            )
            .await
            .expect("Failed to claim batch requests");
        assert!(
            claimed.is_empty(),
            "not-live batch outside ramp must not be claimed"
        );

        sqlx::query(
            "UPDATE batches
             SET created_at = NOW() - interval '56 minutes',
                 expires_at = NOW() + interval '4 minutes'
             WHERE id IN (SELECT DISTINCT batch_id FROM requests WHERE model = 'ramp-model')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let claimed = manager
            .claim_batch_requests_with_cooldown(
                10,
                1,
                daemon_id,
                &capacity,
                &HashMap::new(),
                &cooldown,
            )
            .await
            .expect("Failed to claim batch requests");
        assert_eq!(
            claimed.len(),
            1,
            "within-ramp batch must be claimable despite not-live model"
        );
        assert_eq!(claimed[0].data.model, "ramp-model");
        assert!(claimed[0].state.leak.is_none());
    }

    #[sqlx::test]
    async fn test_claim_batch_requests_limits_batches_per_live_model(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        for model in ["live-a", "live-b"] {
            for batch_index in 0..2 {
                let file_id = manager
                    .create_file(
                        format!("{model}-batch-{batch_index}"),
                        None,
                        vec![RequestTemplateInput {
                            custom_id: Some(format!("{model}-{batch_index}")),
                            endpoint: "https://api.example.com".to_string(),
                            method: "POST".to_string(),
                            path: "/test".to_string(),
                            body: "{}".to_string(),
                            model: model.to_string(),
                        }],
                    )
                    .await
                    .unwrap();
                manager
                    .create_batch(crate::batch::BatchInput {
                        file_id,
                        endpoint: "/v1/chat/completions".to_string(),
                        completion_window: "24h".to_string(),
                        metadata: None,
                        created_by: None,
                        api_key_id: None,
                        total_requests: None,
                    })
                    .await
                    .unwrap();
            }
        }

        manager
            .append_model_filter_events(&[
                ModelFilter {
                    model: "live-a".to_string(),
                    state: ModelFilterState::Live,
                    expected_ready_at: None,
                },
                ModelFilter {
                    model: "live-b".to_string(),
                    state: ModelFilterState::Live,
                    expected_ready_at: None,
                },
            ])
            .await
            .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("live-a".to_string(), 10), ("live-b".to_string(), 10)]);
        let claimed = manager
            .claim_batch_requests(10, 1, daemon_id, &capacity, &HashMap::new())
            .await
            .expect("Failed to claim batch requests");

        let mut claims_by_model = HashMap::new();
        for request in claimed {
            assert!(request.data.batch_id.is_some());
            *claims_by_model.entry(request.data.model).or_insert(0) += 1;
        }

        assert_eq!(
            claims_by_model,
            HashMap::from([("live-a".to_string(), 1), ("live-b".to_string(), 1)])
        );
    }

    #[sqlx::test]
    async fn test_claim_fails_pending_requests_whose_template_is_gone(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let stranded = setup_freeze_test_batch(&manager, "claim-stranded", 2).await;
        let healthy = setup_freeze_test_batch(&manager, "claim-healthy", 1).await;
        sqlx::query(
            "WITH removed AS (
                 DELETE FROM request_templates_g2
                 WHERE id IN (SELECT template_id FROM requests WHERE batch_id = $1)
                 RETURNING id
             )
             DELETE FROM request_template_routes route
             USING removed
             WHERE route.template_id = removed.id",
        )
        .bind(*stranded as Uuid)
        .execute(&pool)
        .await
        .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("freeze-test".to_string(), 10)]);
        let claimed = manager
            .claim_batch_requests(10, 5, daemon_id, &capacity, &HashMap::new())
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1, "only the healthy request is claimable");
        assert_eq!(claimed[0].data.batch_id, Some(healthy));

        let states: Vec<(String, Option<String>)> =
            sqlx::query_as("SELECT state, error FROM requests WHERE batch_id = $1 ORDER BY id")
                .bind(*stranded as Uuid)
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(states.len(), 2);
        assert!(
            states.iter().all(|(state, error)| state == "failed"
                && error.as_deref() == Some("request template no longer exists")),
            "stranded rows must be failed at claim time: {states:?}"
        );

        let again = manager
            .claim_batch_requests(10, 5, daemon_id, &capacity, &HashMap::new())
            .await
            .unwrap();
        assert!(again.is_empty());
    }

    #[sqlx::test]
    async fn background_claim_fails_a_request_whose_template_is_gone(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(DaemonConfig {
            background_concurrency_limit: 10,
            ..Default::default()
        });
        let request =
            create_background_request_for_test(&manager, "background-model", "owner").await;
        manager
            .append_model_filter_events(&[ModelFilter {
                model: "background-model".to_string(),
                state: ModelFilterState::Live,
                expected_ready_at: None,
            }])
            .await
            .unwrap();
        sqlx::query(
            "WITH removed AS (
                DELETE FROM request_templates_g2 WHERE id = (SELECT template_id FROM requests WHERE id = $1)
                RETURNING id
             ) DELETE FROM request_template_routes route USING removed WHERE route.template_id = removed.id",
        ).bind(*request as Uuid).execute(&pool).await.unwrap();
        let claimed = manager
            .claim_background_requests_by_kind(
                BackgroundClaimKind::Batchless,
                10,
                5,
                DaemonId::from(Uuid::new_v4()),
                &HashMap::from([("background-model".to_string(), 10)]),
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert!(claimed.is_empty());
        let (state, error): (String, Option<String>) =
            sqlx::query_as("SELECT state, error FROM requests WHERE id = $1")
                .bind(*request as Uuid)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "failed");
        assert_eq!(error.as_deref(), Some("request template no longer exists"));
    }

    #[sqlx::test]
    async fn test_claim_requests(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "claim-test".to_string(),
                None,
                (0..2)
                    .map(|i| RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let mut flex_ids = Vec::new();
        for i in 0..5 {
            let flex_id = manager
                .create_flex(CreateFlexInput {
                    request_id: Uuid::new_v4(),
                    body: format!(r#"{{"model":"test","n":{i}}}"#),
                    model: "test".to_string(),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    api_key_id: uuid::Uuid::new_v4(),
                    created_by: "user".to_string(),
                    metadata: None,
                })
                .await
                .unwrap();
            flex_ids.push(flex_id);
        }

        let daemon_id = DaemonId::from(Uuid::new_v4());

        let capacity = HashMap::from([("test".to_string(), 10)]);
        let claimed = manager
            .claim_requests(3, daemon_id, &capacity, &HashMap::new(), &HashSet::new())
            .await
            .expect("Failed to claim requests");

        assert_eq!(claimed.len(), 3);
        for request in &claimed {
            assert_eq!(request.state.daemon_id, daemon_id);
            assert_eq!(request.state.retry_attempt, 0);
            assert!(request.data.batch_id.is_none());
            assert!(flex_ids.contains(&request.data.id));
        }

        let claimed2 = manager
            .claim_requests(10, daemon_id, &capacity, &HashMap::new(), &HashSet::new())
            .await
            .expect("Failed to claim requests");

        assert_eq!(claimed2.len(), 2);
        for request in &claimed2 {
            assert!(request.data.batch_id.is_none());
            assert!(flex_ids.contains(&request.data.id));
        }

        let status = manager.get_batch_status(batch.id).await.unwrap();
        assert_eq!(status.total_requests, 2);
        assert_eq!(status.pending_requests, 2);
        assert_eq!(status.in_progress_requests, 0);
    }

    async fn setup_processing_request(
        pool: &sqlx::PgPool,
        daemon_id: DaemonId,
    ) -> (
        PostgresRequestManager<TestDbPools>,
        RequestId,
        DateTime<Utc>,
    ) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        let file_id = manager
            .create_file(
                "retry-fence-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{"n":0}"#.to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let capacity = HashMap::from([("test".to_string(), 10)]);
        let claimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon_id, &capacity, &HashMap::new())
                .await;
        assert_eq!(claimed.len(), 1);
        let request_id = claimed[0].data.id;
        let claimed_at = claimed[0].state.claimed_at;

        sqlx::query("UPDATE requests SET state = 'processing', started_at = NOW() WHERE id = $1")
            .bind(*request_id)
            .execute(pool)
            .await
            .unwrap();

        (manager, request_id, claimed_at)
    }

    async fn read_request_row(pool: &sqlx::PgPool, request_id: RequestId) -> (String, i32) {
        sqlx::query_as::<_, (String, i32)>(
            "SELECT state, retry_attempt FROM requests WHERE id = $1",
        )
        .bind(*request_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }


    struct MarkingTransformer {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl crate::transform::ResponseTransformer for MarkingTransformer {
        async fn transform(&self, _request: &RequestData, body: &str) -> Result<String> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(format!("XFORM:{body}"))
        }
    }

    struct RequestEchoTransformer {
        seen: Arc<std::sync::Mutex<Option<Uuid>>>,
    }

    #[async_trait]
    impl crate::transform::ResponseTransformer for RequestEchoTransformer {
        async fn transform(&self, request: &RequestData, body: &str) -> Result<String> {
            *self.seen.lock().unwrap() = Some(request.id.0);
            Ok(format!("{}:{body}", request.id.0))
        }
    }

    async fn claim_one_processing(
        pool: &sqlx::PgPool,
        transformer: Option<Arc<dyn crate::transform::ResponseTransformer>>,
    ) -> (PostgresRequestManager<TestDbPools>, Request<Claimed>) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        if let Some(t) = transformer {
            manager.set_response_transformer(t).unwrap();
        }

        let file_id = manager
            .create_file(
                "transformer-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{"n":0}"#.to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();
        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("test".to_string(), 10)]);
        let mut claimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon_id, &capacity, &HashMap::new())
                .await;
        assert_eq!(claimed.len(), 1);
        let request = claimed.pop().unwrap();

        sqlx::query("UPDATE requests SET state = 'processing', started_at = NOW() WHERE id = $1")
            .bind(*request.data.id)
            .execute(pool)
            .await
            .unwrap();

        (manager, request)
    }

    fn completed_from(req: &Request<Claimed>, body: &str) -> Request<Completed> {
        let now = chrono::Utc::now();
        Request {
            data: req.data.clone(),
            state: Completed {
                response_status: 200,
                response_body: body.to_string(),
                claimed_at: req.state.claimed_at,
                started_at: now,
                completed_at: now,
                routed_model: req.data.model.clone(),
            },
        }
    }

    fn failed_from(req: &Request<Claimed>, reason: FailureReason) -> Request<Failed> {
        Request {
            data: req.data.clone(),
            state: Failed {
                reason,
                failed_at: chrono::Utc::now(),
                claimed_at: Some(req.state.claimed_at),
                retry_attempt: 0,
                batch_expires_at: req.state.batch_expires_at,
                routed_model: req.data.model.clone(),
            },
        }
    }

    #[sqlx::test]
    async fn request_state_writes_respect_configured_concurrency(pool: sqlx::PgPool) {
        let config = crate::PostgresStorageConfig {
            max_concurrent_state_writes: 2,
            ..Default::default()
        };
        let manager = Arc::new(PostgresRequestManager::new(
            TestDbPools::new(pool.clone()).await.unwrap(),
            config,
        ));

        let templates = (0..3)
            .map(|index| RequestTemplateInput {
                custom_id: Some(format!("state-write-{index}")),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: "{}".to_string(),
                model: "test".to_string(),
            })
            .collect();
        let file_id = manager
            .create_file("state-write-limit-test".to_string(), None, templates)
            .await
            .unwrap();
        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("test".to_string(), 3)]);
        let claimed =
            claim_batch_requests_for_test(&manager, 3, 1, daemon_id, &capacity, &HashMap::new())
                .await;
        assert_eq!(claimed.len(), 3);

        let claimed_ids: Vec<Uuid> = claimed.iter().map(|request| *request.data.id).collect();
        sqlx::query(
            "UPDATE requests SET state = 'processing', started_at = NOW() WHERE id = ANY($1)",
        )
        .bind(&claimed_ids)
        .execute(&pool)
        .await
        .unwrap();

        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE requests IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *blocker)
            .await
            .unwrap();

        let mut handles = Vec::new();
        {
            let manager = manager.clone();
            let request_id = claimed[0].data.id;
            let claimed_at = claimed[0].state.claimed_at;
            handles.push(tokio::spawn(async move {
                let rescheduled = manager
                    .reschedule_for_retry(request_id, daemon_id, claimed_at, 1, None)
                    .await?;
                assert!(rescheduled);
                Ok::<(), FusilladeError>(())
            }));
        }
        handles.extend(claimed[1..].iter().map(|request| {
            let manager = manager.clone();
            let completed = completed_from(request, "done");
            tokio::spawn(async move { manager.persist(&completed).await.map(|_| ()) })
        }));

        let blocked_writes = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let counts = manager.state_write_limiter.counts();
                if counts == (1, 2) {
                    let blocked = sqlx::query_scalar::<_, i64>(
                        r#"
                        SELECT COUNT(*)
                        FROM pg_stat_activity
                        WHERE datname = current_database()
                          AND state = 'active'
                          AND wait_event_type = 'Lock'
                          AND query LIKE '%requests%'
                          AND query NOT LIKE 'LOCK TABLE requests%'
                        "#,
                    )
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                    if blocked == 2 {
                        break blocked;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("state writes never reached the database lock");

        assert_eq!(
            blocked_writes, 2,
            "only the configured number of state writes may enter Postgres"
        );

        blocker.rollback().await.unwrap();
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    }

    #[sqlx::test]
    async fn completed_persist_preserves_durable_processing_start(pool: sqlx::PgPool) {
        let (manager, req) = claim_one_processing(&pool, None).await;
        let durable_started_at = (chrono::Utc::now() - chrono::Duration::minutes(5))
            .with_nanosecond(0)
            .unwrap();
        sqlx::query("UPDATE requests SET state = 'processing', started_at = $2 WHERE id = $1")
            .bind(*req.data.id)
            .bind(durable_started_at)
            .execute(&pool)
            .await
            .unwrap();

        manager
            .persist(&completed_from(&req, "done"))
            .await
            .unwrap();

        let stored_started_at: Option<DateTime<Utc>> =
            sqlx::query_scalar("SELECT started_at FROM requests WHERE id = $1")
                .bind(*req.data.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(stored_started_at, Some(durable_started_at));
    }

    #[sqlx::test]
    async fn persist_completed_supersedes_canceled_row(pool: sqlx::PgPool) {
        let (manager, req) = claim_one_processing(&pool, None).await;

        sqlx::query(
            "UPDATE requests SET state = 'canceled', canceled_at = NOW(), \
             daemon_id = NULL, started_at = NULL WHERE id = $1",
        )
        .bind(*req.data.id)
        .execute(&pool)
        .await
        .unwrap();

        manager
            .persist(&completed_from(&req, "late result"))
            .await
            .unwrap();

        let row = sqlx::query!(
            r#"SELECT state, response_body, canceled_at, completed_at
               FROM requests WHERE id = $1"#,
            *req.data.id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            row.state, "completed",
            "billed result supersedes best-effort cancel"
        );
        assert_eq!(row.response_body.as_deref(), Some("late result"));
        assert!(row.canceled_at.is_none(), "cancel residue must be cleared");
        assert!(row.completed_at.is_some());
    }

    #[sqlx::test]
    async fn persist_failed_supersedes_canceled_row(pool: sqlx::PgPool) {
        let (manager, req) = claim_one_processing(&pool, None).await;
        sqlx::query("UPDATE requests SET state = 'canceled', canceled_at = NOW() WHERE id = $1")
            .bind(*req.data.id)
            .execute(&pool)
            .await
            .unwrap();

        manager
            .persist(&failed_from(
                &req,
                FailureReason::NonRetriableHttpStatus {
                    status: 500,
                    body: "late failure".to_string(),
                },
            ))
            .await
            .unwrap();

        let row = sqlx::query!(
            r#"SELECT state, error, canceled_at FROM requests WHERE id = $1"#,
            *req.data.id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            row.state, "failed",
            "truthful failure record supersedes cancel"
        );
        assert!(row.error.is_some());
        assert!(row.canceled_at.is_none());
    }

    #[sqlx::test]
    async fn duplicate_terminal_persist_is_dropped(pool: sqlx::PgPool) {
        let (manager, req) = claim_one_processing(&pool, None).await;
        manager
            .persist(&completed_from(&req, "first result"))
            .await
            .unwrap();
        manager
            .persist(&completed_from(&req, "replayed result"))
            .await
            .unwrap();
        let body = stored_response_body(&pool, req.data.id).await;
        assert_eq!(body.as_deref(), Some("first result"));
    }

    #[sqlx::test]
    async fn late_completion_repairs_frozen_counts(pool: sqlx::PgPool) {
        let (manager, req) = claim_one_processing(&pool, None).await;
        let batch_id = req.data.batch_id.expect("claimed row belongs to a batch");

        manager.cancel_batch(batch_id).await.unwrap();
        sqlx::query("UPDATE requests SET state = 'canceled', canceled_at = NOW() WHERE id = $1")
            .bind(*req.data.id)
            .execute(&pool)
            .await
            .unwrap();

        manager.get_batch(batch_id).await.unwrap();
        let frozen = sqlx::query!(
            r#"SELECT counts_frozen_at IS NOT NULL AS "frozen!", canceled_requests
               FROM batches WHERE id = $1"#,
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            frozen.frozen,
            "batch must freeze once the cascade settled it"
        );
        assert_eq!(frozen.canceled_requests, 1);

        manager
            .persist(&completed_from(&req, "too late"))
            .await
            .unwrap();

        let check = sqlx::query!(
            r#"SELECT b.canceled_requests, b.completed_requests,
                      b.counts_frozen_at IS NOT NULL AS "still_frozen!",
                      (SELECT COUNT(*) FROM requests WHERE batch_id = b.id AND state = 'canceled') AS "live_canceled!",
                      (SELECT COUNT(*) FROM requests WHERE batch_id = b.id AND state = 'completed') AS "live_completed!"
               FROM batches b WHERE b.id = $1"#,
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(check.still_frozen, "supersede must not unfreeze the batch");
        assert_eq!((check.canceled_requests, check.completed_requests), (0, 1));
        assert_eq!(
            (check.canceled_requests, check.completed_requests),
            (check.live_canceled, check.live_completed),
            "frozen counters must match the rows exactly after the repair"
        );
    }

    #[sqlx::test]
    async fn persist_pending_still_repends_failed_row(pool: sqlx::PgPool) {
        let (manager, req) = claim_one_processing(&pool, None).await;
        manager
            .persist(&failed_from(
                &req,
                FailureReason::NonRetriableHttpStatus {
                    status: 500,
                    body: "boom".to_string(),
                },
            ))
            .await
            .unwrap();

        let pending = Request {
            data: req.data.clone(),
            state: fusillade_core::request::Pending {
                retry_attempt: 0,
                not_before: None,
                batch_expires_at: req.state.batch_expires_at,
            },
        };
        manager.persist(&pending).await.unwrap();

        let state: String = sqlx::query_scalar("SELECT state FROM requests WHERE id = $1")
            .bind(*req.data.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            state, "pending",
            "manual retry terminal->pending must keep working"
        );
    }

    #[sqlx::test]
    async fn persist_claimed_does_not_resurrect_terminal_row(pool: sqlx::PgPool) {
        let (manager, req) = claim_one_processing(&pool, None).await;
        manager
            .persist(&failed_from(
                &req,
                FailureReason::NonRetriableHttpStatus {
                    status: 500,
                    body: "terminal".to_string(),
                },
            ))
            .await
            .unwrap();

        let stale_pending = Request {
            data: req.data.clone(),
            state: fusillade_core::request::Pending {
                retry_attempt: req.state.retry_attempt,
                not_before: None,
                batch_expires_at: req.state.batch_expires_at,
            },
        };
        let error = stale_pending
            .claim(DaemonId::from(Uuid::new_v4()), &manager)
            .await
            .expect_err("a stale pending value must not claim a terminal row");
        assert!(matches!(
            error,
            FusilladeError::LostOwnership(id) if id == req.data.id
        ));

        let (state, _) = read_request_row(&pool, req.data.id).await;
        assert_eq!(state, "failed");
    }

    #[sqlx::test]
    async fn persist_completed_accepts_current_claimed_generation(pool: sqlx::PgPool) {
        let (manager, req) = claim_one_processing(&pool, None).await;
        sqlx::query("UPDATE requests SET state = 'claimed', started_at = NULL WHERE id = $1")
            .bind(*req.data.id)
            .execute(&pool)
            .await
            .unwrap();

        manager
            .persist(&completed_from(&req, "direct completion"))
            .await
            .unwrap();

        let state: String = sqlx::query_scalar("SELECT state FROM requests WHERE id = $1")
            .bind(*req.data.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(state, "completed");
    }

    #[sqlx::test]
    async fn persist_canceled_stale_generation_loses_ownership(pool: sqlx::PgPool) {
        let (manager, req) = claim_one_processing(&pool, None).await;
        let new_claimed_at = req.state.claimed_at + chrono::Duration::seconds(1);
        sqlx::query("UPDATE requests SET claimed_at = $2 WHERE id = $1")
            .bind(*req.data.id)
            .bind(new_claimed_at)
            .execute(&pool)
            .await
            .unwrap();

        let canceled = Request {
            data: req.data.clone(),
            state: Canceled {
                canceled_at: chrono::Utc::now(),
                claimed_at: Some(req.state.claimed_at),
            },
        };
        let error = manager
            .persist(&canceled)
            .await
            .expect_err("stale cancellation must be fenced out");
        assert!(matches!(
            error,
            FusilladeError::LostOwnership(id) if id == req.data.id
        ));

        let (state, _) = read_request_row(&pool, req.data.id).await;
        assert_eq!(state, "processing");
    }

    async fn stored_response_body(pool: &sqlx::PgPool, id: RequestId) -> Option<String> {
        sqlx::query_scalar::<_, Option<String>>("SELECT response_body FROM requests WHERE id = $1")
            .bind(*id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn stored_failure_reason(pool: &sqlx::PgPool, id: RequestId) -> FailureReason {
        let error_json: String = sqlx::query_scalar("SELECT error FROM requests WHERE id = $1")
            .bind(*id)
            .fetch_one(pool)
            .await
            .unwrap();
        serde_json::from_str(&error_json).unwrap()
    }

    #[sqlx::test]
    async fn response_transformer_rewrites_completed_body(pool: sqlx::PgPool) {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (manager, req) = claim_one_processing(
            &pool,
            Some(Arc::new(MarkingTransformer {
                calls: calls.clone(),
            })),
        )
        .await;

        manager
            .persist(&completed_from(&req, "hello"))
            .await
            .unwrap();

        assert_eq!(
            stored_response_body(&pool, req.data.id).await.as_deref(),
            Some("XFORM:hello"),
        );
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "transform must run exactly once per persist (it sits before the DB retry loop)",
        );
    }

    #[sqlx::test]
    async fn response_transformer_receives_the_persisted_request(pool: sqlx::PgPool) {
        let seen = Arc::new(std::sync::Mutex::new(None));
        let (manager, req) = claim_one_processing(
            &pool,
            Some(Arc::new(RequestEchoTransformer { seen: seen.clone() })),
        )
        .await;

        manager
            .persist(&completed_from(&req, "hello"))
            .await
            .unwrap();

        assert_eq!(
            *seen.lock().unwrap(),
            Some(req.data.id.0),
            "transformer must be handed the request being persisted",
        );
        let expected = format!("{}:hello", req.data.id.0);
        assert_eq!(
            stored_response_body(&pool, req.data.id).await.as_deref(),
            Some(expected.as_str()),
            "a field read from the passed request influenced the stored body",
        );
    }

    #[sqlx::test]
    async fn response_transformer_rewrites_http_status_failure_body(pool: sqlx::PgPool) {
        for reason in [
            FailureReason::RetriableHttpStatus {
                status: 503,
                body: "boom".to_string(),
            },
            FailureReason::NonRetriableHttpStatus {
                status: 400,
                body: "bad".to_string(),
            },
        ] {
            let original_body = match &reason {
                FailureReason::RetriableHttpStatus { body, .. }
                | FailureReason::NonRetriableHttpStatus { body, .. } => body.clone(),
                _ => unreachable!(),
            };
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let (manager, req) = claim_one_processing(
                &pool,
                Some(Arc::new(MarkingTransformer {
                    calls: calls.clone(),
                })),
            )
            .await;

            manager.persist(&failed_from(&req, reason)).await.unwrap();

            let stored_body = match stored_failure_reason(&pool, req.data.id).await {
                FailureReason::RetriableHttpStatus { body, .. }
                | FailureReason::NonRetriableHttpStatus { body, .. } => body,
                other => panic!("unexpected reason variant: {other:?}"),
            };
            assert_eq!(stored_body, format!("XFORM:{original_body}"));
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        }
    }

    #[sqlx::test]
    async fn response_transformer_skips_non_http_failure(pool: sqlx::PgPool) {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (manager, req) = claim_one_processing(
            &pool,
            Some(Arc::new(MarkingTransformer {
                calls: calls.clone(),
            })),
        )
        .await;

        manager
            .persist(&failed_from(
                &req,
                FailureReason::Timeout {
                    error: "timed out".to_string(),
                },
            ))
            .await
            .unwrap();

        assert_eq!(
            stored_failure_reason(&pool, req.data.id).await,
            FailureReason::Timeout {
                error: "timed out".to_string(),
            },
        );
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "internal-error variants have no user body to transform",
        );
    }

    #[sqlx::test]
    async fn response_transformer_absent_stores_body_verbatim(pool: sqlx::PgPool) {
        let (manager, req) = claim_one_processing(&pool, None).await;

        manager
            .persist(&completed_from(&req, "plaintext"))
            .await
            .unwrap();

        assert_eq!(
            stored_response_body(&pool, req.data.id).await.as_deref(),
            Some("plaintext"),
        );
    }

    #[sqlx::test]
    async fn test_reschedule_for_retry_owner_succeeds(pool: sqlx::PgPool) {
        let daemon_id = DaemonId::from(Uuid::new_v4());
        let (manager, request_id, claimed_at) = setup_processing_request(&pool, daemon_id).await;

        let not_before = chrono::Utc::now() + chrono::Duration::seconds(30);
        let rescheduled = manager
            .reschedule_for_retry(request_id, daemon_id, claimed_at, 1, Some(not_before))
            .await
            .unwrap();

        assert!(
            rescheduled,
            "owner should reschedule its own processing row"
        );
        let (state, retry) = read_request_row(&pool, request_id).await;
        assert_eq!(state, "pending");
        assert_eq!(retry, 1);
    }

    #[sqlx::test]
    async fn test_reschedule_for_retry_accepts_claimed_state(pool: sqlx::PgPool) {
        let daemon_id = DaemonId::from(Uuid::new_v4());
        let (manager, request_id, claimed_at) = setup_processing_request(&pool, daemon_id).await;
        sqlx::query("UPDATE requests SET state = 'claimed', started_at = NULL WHERE id = $1")
            .bind(*request_id)
            .execute(&pool)
            .await
            .unwrap();

        let rescheduled = manager
            .reschedule_for_retry(request_id, daemon_id, claimed_at, 1, None)
            .await
            .unwrap();

        assert!(rescheduled);
        let (state, retry) = read_request_row(&pool, request_id).await;
        assert_eq!((state.as_str(), retry), ("pending", 1));
    }

    #[sqlx::test]
    async fn test_reschedule_for_retry_does_not_resurrect_terminal(pool: sqlx::PgPool) {
        let daemon_id = DaemonId::from(Uuid::new_v4());
        let (manager, request_id, claimed_at) = setup_processing_request(&pool, daemon_id).await;

        sqlx::query(
            "UPDATE requests SET state = 'failed', failed_at = NOW(), error = 'terminal', daemon_id = NULL WHERE id = $1",
        )
        .bind(*request_id)
        .execute(&pool)
        .await
        .unwrap();

        let not_before = chrono::Utc::now() + chrono::Duration::seconds(30);
        let rescheduled = manager
            .reschedule_for_retry(request_id, daemon_id, claimed_at, 1, Some(not_before))
            .await
            .unwrap();

        assert!(!rescheduled, "must not reschedule a terminalized row");
        let (state, _) = read_request_row(&pool, request_id).await;
        assert_eq!(
            state, "failed",
            "row must stay terminal, not resurrect to pending"
        );
    }

    #[sqlx::test]
    async fn test_reschedule_for_retry_wrong_owner_skips(pool: sqlx::PgPool) {
        let owner = DaemonId::from(Uuid::new_v4());
        let (manager, request_id, claimed_at) = setup_processing_request(&pool, owner).await;

        let other = DaemonId::from(Uuid::new_v4());
        let rescheduled = manager
            .reschedule_for_retry(request_id, other, claimed_at, 1, None)
            .await
            .unwrap();

        assert!(!rescheduled, "non-owning daemon must not reschedule");
        let (state, _) = read_request_row(&pool, request_id).await;
        assert_eq!(
            state, "processing",
            "row must remain the owner's in-flight claim"
        );
    }

    #[sqlx::test]
    async fn test_reschedule_for_retry_stale_generation_same_owner_skips(pool: sqlx::PgPool) {
        let owner = DaemonId::from(Uuid::new_v4());
        let (manager, request_id, old_claimed_at) = setup_processing_request(&pool, owner).await;
        let new_claimed_at = old_claimed_at + chrono::Duration::seconds(1);

        let new_claimed_at: DateTime<Utc> = sqlx::query_scalar(
            "UPDATE requests SET claimed_at = $2 WHERE id = $1 AND state = 'processing' \
             RETURNING claimed_at",
        )
        .bind(*request_id)
        .bind(new_claimed_at)
        .fetch_one(&pool)
        .await
        .unwrap();

        let rescheduled = manager
            .reschedule_for_retry(request_id, owner, old_claimed_at, 1, None)
            .await
            .unwrap();

        assert!(!rescheduled, "stale claim generation must be fenced out");
        let row_claimed_at: DateTime<Utc> =
            sqlx::query_scalar("SELECT claimed_at FROM requests WHERE id = $1")
                .bind(*request_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(row_claimed_at, new_claimed_at);
    }

    #[sqlx::test]
    async fn test_terminal_persist_stale_generation_loses_ownership(pool: sqlx::PgPool) {
        let (manager, request) = claim_one_processing(&pool, None).await;
        let new_claimed_at = request.state.claimed_at + chrono::Duration::seconds(1);

        sqlx::query("UPDATE requests SET claimed_at = $2 WHERE id = $1 AND state = 'processing'")
            .bind(*request.data.id)
            .bind(new_claimed_at)
            .execute(&pool)
            .await
            .unwrap();

        let error = manager
            .persist(&completed_from(&request, "stale response"))
            .await
            .expect_err("old generation must not terminalize the request");
        assert!(matches!(
            error,
            FusilladeError::LostOwnership(id) if id == request.data.id
        ));

        let (state, _) = read_request_row(&pool, request.data.id).await;
        assert_eq!(state, "processing");
        let response_body: Option<String> =
            sqlx::query_scalar("SELECT response_body FROM requests WHERE id = $1")
                .bind(*request.data.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(response_body, None);
    }

    #[sqlx::test]
    async fn test_pending_counts_exclude_terminal_batches(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        let file_id = manager
            .create_file(
                "terminal-count-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{"n":0}"#.to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let now = chrono::Utc::now();
        sqlx::query!(
            "UPDATE batches SET expires_at = $1 WHERE id = $2",
            now + chrono::Duration::minutes(30),
            *batch.id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let windows = vec![("1h".to_string(), None, 3600)];
        let states = vec!["pending".to_string()];
        let model_filter: Vec<String> = vec![];

        let before = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert_eq!(*before.get("test").unwrap().get("1h").unwrap(), 1);

        sqlx::query!(
            "UPDATE batches SET finalizing_at = $1, completed_at = $1 WHERE id = $2",
            now,
            *batch.id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let after = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert_eq!(
            after
                .get("test")
                .and_then(|m| m.get("1h"))
                .copied()
                .unwrap_or(0),
            0,
            "pending request under a completed batch must not be counted"
        );
    }


    async fn setup_freeze_test_batch(
        manager: &PostgresRequestManager<TestDbPools>,
        name: &str,
        n: usize,
    ) -> BatchId {
        let templates: Vec<RequestTemplateInput> = (0..n)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("{name}-{i}")),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: "{}".to_string(),
                model: "freeze-test".to_string(),
            })
            .collect();
        let file_id = manager
            .create_file(format!("{name}-file"), None, templates)
            .await
            .unwrap();
        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap()
            .id
    }

    async fn frozen_at(pool: &sqlx::PgPool, batch_id: BatchId) -> Option<DateTime<Utc>> {
        sqlx::query_scalar!(
            "SELECT counts_frozen_at FROM batches WHERE id = $1",
            *batch_id as Uuid
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[sqlx::test]
    async fn test_frozen_counts_survive_request_row_deletion(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "freeze-survive", 3).await;

        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1 AND id IN (SELECT id FROM requests WHERE batch_id = $1 LIMIT 2)",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query!(
            "UPDATE requests SET state = 'failed', failed_at = NOW(), error = 'test-error' WHERE batch_id = $1 AND state <> 'completed'",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let batch = manager.get_batch(batch_id).await.unwrap();
        assert_eq!(batch.completed_requests, 2);
        assert_eq!(batch.failed_requests, 1);
        assert!(batch.completed_at.is_some(), "completed>0 => completed_at");
        assert!(
            frozen_at(&pool, batch_id).await.is_some(),
            "lazy finalization must freeze counts in the same transition"
        );

        sqlx::query!(
            "DELETE FROM requests WHERE batch_id = $1",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let batch = manager.get_batch(batch_id).await.unwrap();
        assert_eq!(
            (
                batch.completed_requests,
                batch.failed_requests,
                batch.canceled_requests
            ),
            (2, 1, 0),
            "get_batch must serve frozen counts without request rows"
        );
        assert_eq!((batch.pending_requests, batch.in_progress_requests), (0, 0));

        let status = manager.get_batch_status(batch_id).await.unwrap();
        assert_eq!(status.completed_requests, 2);
        assert_eq!(status.failed_requests, 1);

        let file_id = batch.file_id.expect("freeze test batch has a file_id");
        let file_batches = manager.list_file_batches(file_id).await.unwrap();
        let file_row = file_batches
            .iter()
            .find(|b| b.batch_id == batch_id)
            .unwrap();
        assert_eq!(
            (
                file_row.completed_requests,
                file_row.failed_requests,
                file_row.canceled_requests
            ),
            (2, 1, 0),
            "list_file_batches must serve frozen counts without request rows"
        );

        let output_file_id = manager
            .create_file("freeze-survive-output".to_string(), None, vec![])
            .await
            .unwrap();
        sqlx::query!(
            "UPDATE batches SET output_file_id = $2 WHERE id = $1",
            *batch_id as Uuid,
            *output_file_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        let by_output = manager
            .get_batch_by_output_file_id(output_file_id, OutputFileType::Output)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_output.id, batch_id);
        assert_eq!(
            (
                by_output.completed_requests,
                by_output.failed_requests,
                by_output.canceled_requests
            ),
            (2, 1, 0),
            "get_batch_by_output_file_id must serve frozen counts without request rows"
        );

        let listed = manager
            .list_batches(crate::batch::ListBatchesFilter::default())
            .await
            .unwrap();
        let row = listed.iter().find(|b| b.id == batch_id).unwrap();
        assert_eq!(
            (row.completed_requests, row.failed_requests),
            (2, 1),
            "list_batches must serve frozen counts without request rows"
        );
    }

    #[sqlx::test]
    async fn test_cancelled_batch_freezes_only_after_rows_settle(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "freeze-cancel", 2).await;

        manager.cancel_batch(batch_id).await.unwrap();

        let batch = manager.get_batch(batch_id).await.unwrap();
        assert_eq!(batch.canceled_requests, 2, "projected cancel count");
        assert!(
            frozen_at(&pool, batch_id).await.is_none(),
            "must not freeze while rows are still transitioning"
        );

        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1 AND id IN (SELECT id FROM requests WHERE batch_id = $1 LIMIT 1)",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query!(
            "UPDATE requests SET state = 'canceled', canceled_at = NOW() WHERE batch_id = $1 AND state <> 'completed'",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let batch = manager.get_batch(batch_id).await.unwrap();
        assert_eq!((batch.completed_requests, batch.canceled_requests), (1, 1));
        assert!(frozen_at(&pool, batch_id).await.is_some());

        sqlx::query!(
            "DELETE FROM requests WHERE batch_id = $1",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        let batch = manager.get_batch(batch_id).await.unwrap();
        assert_eq!((batch.completed_requests, batch.canceled_requests), (1, 1));
    }

    #[sqlx::test]
    async fn test_retry_unfreezes_then_refreezes(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "freeze-retry", 2).await;

        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1 AND id IN (SELECT id FROM requests WHERE batch_id = $1 LIMIT 1)",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query!(
            "UPDATE requests SET state = 'failed', failed_at = NOW(), error = 'test-error' WHERE batch_id = $1 AND state <> 'completed'",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let batch = manager.get_batch(batch_id).await.unwrap();
        assert_eq!((batch.completed_requests, batch.failed_requests), (1, 1));
        assert!(frozen_at(&pool, batch_id).await.is_some());

        let version_before: i64 = sqlx::query_scalar!(
            "SELECT retry_version FROM batches WHERE id = $1",
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let retried = manager
            .retry_failed_requests_for_batch(batch_id)
            .await
            .unwrap();
        assert_eq!(retried, 1);
        assert!(
            frozen_at(&pool, batch_id).await.is_none(),
            "retry must clear the freeze"
        );

        let version_after: i64 = sqlx::query_scalar!(
            "SELECT retry_version FROM batches WHERE id = $1",
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            version_after,
            version_before + 1,
            "retry must bump retry_version"
        );

        let stale = sqlx::query!(
            r#"
            UPDATE batches
            SET completed_requests = 999, failed_requests = 999, canceled_requests = 999,
                counts_frozen_at = NOW()
            WHERE id = $1 AND retry_version = $2 AND counts_frozen_at IS NULL
            "#,
            *batch_id as Uuid,
            version_before,
        )
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            stale.rows_affected(),
            0,
            "stale-retry_version freeze must not land after a retry"
        );
        let batch = manager.get_batch(batch_id).await.unwrap();
        assert_eq!(
            (
                batch.completed_requests,
                batch.failed_requests,
                batch.pending_requests
            ),
            (1, 0, 1),
            "post-retry reads must count live, not serve stale frozen values"
        );

        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1 AND state = 'pending'",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        let batch = manager.get_batch(batch_id).await.unwrap();
        assert_eq!((batch.completed_requests, batch.failed_requests), (2, 0));
        assert!(
            frozen_at(&pool, batch_id).await.is_some(),
            "re-terminalization must re-freeze"
        );
    }

    #[sqlx::test]
    async fn test_single_request_retry_unfreezes_parent_batch(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "freeze-single-retry", 2).await;

        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1 AND id IN (SELECT id FROM requests WHERE batch_id = $1 LIMIT 1)",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query!(
            "UPDATE requests SET state = 'failed', failed_at = NOW(), error = 'test-error' WHERE batch_id = $1 AND state <> 'completed'",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let batch = manager.get_batch(batch_id).await.unwrap();
        assert_eq!((batch.completed_requests, batch.failed_requests), (1, 1));
        assert!(frozen_at(&pool, batch_id).await.is_some());
        let version_before: i64 = sqlx::query_scalar!(
            "SELECT retry_version FROM batches WHERE id = $1",
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let failed_id = sqlx::query_scalar!(
            "SELECT id FROM requests WHERE batch_id = $1 AND state = 'failed'",
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let results = manager
            .retry_failed_requests(batch_id, vec![RequestId(failed_id)])
            .await
            .unwrap();
        assert!(results.iter().all(|r| r.is_ok()));

        assert!(
            frozen_at(&pool, batch_id).await.is_none(),
            "single-request retry must clear the parent batch freeze"
        );
        let version_after: i64 = sqlx::query_scalar!(
            "SELECT retry_version FROM batches WHERE id = $1",
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(version_after, version_before + 1);
        let batch = manager.get_batch(batch_id).await.unwrap();
        assert!(batch.completed_at.is_none() && batch.failed_at.is_none());
        assert_eq!(
            (
                batch.completed_requests,
                batch.failed_requests,
                batch.pending_requests
            ),
            (1, 0, 1),
            "reads must count live again and show the retried row"
        );
    }

    #[sqlx::test]
    async fn test_retry_failed_requests_ignores_ids_from_other_batches(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let own_batch = setup_freeze_test_batch(&manager, "retry-scope-own", 1).await;
        let other_batch = setup_freeze_test_batch(&manager, "retry-scope-other", 1).await;
        for batch_id in [own_batch, other_batch] {
            sqlx::query(
                "UPDATE requests SET state = 'failed', failed_at = NOW(), error = 'test-error' WHERE batch_id = $1",
            )
            .bind(*batch_id)
            .execute(&pool)
            .await
            .unwrap();
        }
        let failed_id = |batch_id: BatchId| {
            sqlx::query_scalar::<_, Uuid>("SELECT id FROM requests WHERE batch_id = $1")
                .bind(*batch_id)
                .fetch_one(&pool)
        };
        let own_id = failed_id(own_batch).await.unwrap();
        let other_id = failed_id(other_batch).await.unwrap();
        let other_version_before: i64 =
            sqlx::query_scalar("SELECT retry_version FROM batches WHERE id = $1")
                .bind(*other_batch)
                .fetch_one(&pool)
                .await
                .unwrap();

        let results = manager
            .retry_failed_requests(own_batch, vec![RequestId(other_id), RequestId(own_id)])
            .await
            .unwrap();
        assert!(
            matches!(
                results.as_slice(),
                [Err(FusilladeError::RequestNotFound(id)), Ok(())] if **id == other_id
            ),
            "out-of-batch id must be reported as not found, in-batch id retried: {results:?}"
        );

        let state = |id: Uuid| {
            sqlx::query_scalar::<_, String>("SELECT state FROM requests WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
        };
        assert_eq!(state(other_id).await.unwrap(), "failed");
        assert_eq!(state(own_id).await.unwrap(), "pending");
        let other_version_after: i64 =
            sqlx::query_scalar("SELECT retry_version FROM batches WHERE id = $1")
                .bind(*other_batch)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            other_version_after, other_version_before,
            "another batch must not be reset by a retry scoped elsewhere"
        );
    }

    #[sqlx::test]
    async fn test_batch_retry_uncancels_and_resumes(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "freeze-uncancel", 3).await;

        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1 AND id IN (SELECT id FROM requests WHERE batch_id = $1 LIMIT 1)",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query!(
            "UPDATE requests SET state = 'failed', failed_at = NOW(), error = 'test-error' WHERE batch_id = $1 AND id IN (SELECT id FROM requests WHERE batch_id = $1 AND state = 'pending' LIMIT 1)",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        manager.cancel_batch(batch_id).await.unwrap();
        sqlx::query!(
            "UPDATE requests SET state = 'canceled', canceled_at = NOW() WHERE batch_id = $1 AND state = 'pending'",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        let batch = manager.get_batch(batch_id).await.unwrap();
        assert_eq!(
            (
                batch.completed_requests,
                batch.failed_requests,
                batch.canceled_requests
            ),
            (1, 1, 1)
        );
        assert!(batch.cancelled_at.is_some());
        assert!(frozen_at(&pool, batch_id).await.is_some());
        let version_before: i64 = sqlx::query_scalar!(
            "SELECT retry_version FROM batches WHERE id = $1",
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let retried = manager
            .retry_failed_requests_for_batch(batch_id)
            .await
            .unwrap();
        assert_eq!(retried, 2, "failed and canceled rows re-pend");

        let batch = manager.get_batch(batch_id).await.unwrap();
        assert!(
            batch.cancelled_at.is_none() && batch.cancelling_at.is_none(),
            "retry must overturn cancellation"
        );
        assert!(frozen_at(&pool, batch_id).await.is_none());
        assert_eq!(
            (
                batch.completed_requests,
                batch.pending_requests,
                batch.failed_requests,
                batch.canceled_requests
            ),
            (1, 2, 0, 0),
            "completed work kept; failed + canceled rows pending again"
        );
        let version_after: i64 = sqlx::query_scalar!(
            "SELECT retry_version FROM batches WHERE id = $1",
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(version_after, version_before + 1);

        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1 AND state = 'pending'",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        let batch = manager.get_batch(batch_id).await.unwrap();
        assert_eq!(batch.completed_requests, 3);
        assert!(batch.completed_at.is_some());
        assert!(frozen_at(&pool, batch_id).await.is_some());
    }

    #[sqlx::test]
    async fn test_single_request_retry_uncancels_parent(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "freeze-single-uncancel", 2).await;

        sqlx::query!(
            "UPDATE requests SET state = 'failed', failed_at = NOW(), error = 'test-error' WHERE batch_id = $1 AND id IN (SELECT id FROM requests WHERE batch_id = $1 LIMIT 1)",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        manager.cancel_batch(batch_id).await.unwrap();
        sqlx::query!(
            "UPDATE requests SET state = 'canceled', canceled_at = NOW() WHERE batch_id = $1 AND state <> 'failed'",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        let _ = manager.get_batch(batch_id).await.unwrap();
        assert!(frozen_at(&pool, batch_id).await.is_some());

        let failed_id = sqlx::query_scalar!(
            "SELECT id FROM requests WHERE batch_id = $1 AND state = 'failed'",
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let results = manager
            .retry_failed_requests(batch_id, vec![RequestId(failed_id)])
            .await
            .unwrap();
        assert!(results.iter().all(|r| r.is_ok()));

        let batch = manager.get_batch(batch_id).await.unwrap();
        assert!(
            batch.cancelled_at.is_none() && batch.cancelling_at.is_none(),
            "per-request retry must also overturn cancellation"
        );
        assert!(frozen_at(&pool, batch_id).await.is_none());
        assert_eq!(
            (batch.pending_requests, batch.canceled_requests),
            (1, 1),
            "only the retried row re-pends; other canceled rows stay canceled"
        );
    }

    #[sqlx::test]
    async fn test_retry_immediately_after_cancel_resumes_batch(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "freeze-fast-resume", 2).await;

        manager.cancel_batch(batch_id).await.unwrap();
        let retried = manager
            .retry_failed_requests_for_batch(batch_id)
            .await
            .unwrap();
        assert_eq!(retried, 0, "nothing to re-pend yet");

        let batch = manager.get_batch(batch_id).await.unwrap();
        assert!(
            batch.cancelled_at.is_none() && batch.cancelling_at.is_none(),
            "immediate retry must still un-cancel (pause-resume)"
        );
        assert_eq!(
            batch.pending_requests, 2,
            "rows are pending (claimable) again, not projected as canceled"
        );
        assert!(frozen_at(&pool, batch_id).await.is_none());

        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        let batch = manager.get_batch(batch_id).await.unwrap();
        assert!(batch.completed_at.is_some());
        assert!(frozen_at(&pool, batch_id).await.is_some());
        let version: i64 = sqlx::query_scalar!(
            "SELECT retry_version FROM batches WHERE id = $1",
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let retried = manager
            .retry_failed_requests_for_batch(batch_id)
            .await
            .unwrap();
        assert_eq!(retried, 0);
        let batch = manager.get_batch(batch_id).await.unwrap();
        assert!(
            batch.completed_at.is_some() && frozen_at(&pool, batch_id).await.is_some(),
            "no-op retry of a completed batch must not un-terminalize"
        );
        let version_after: i64 = sqlx::query_scalar!(
            "SELECT retry_version FROM batches WHERE id = $1",
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(version_after, version, "no retry_version churn on no-op");
    }

    #[sqlx::test]
    async fn notification_claim_reads_only_pending_batches(pool: sqlx::PgPool) {
        sqlx::query(
            r#"
            INSERT INTO batches (id, endpoint, completion_window, created_by, created_at,
                                 completed_at, counts_frozen_at, notification_sent_at,
                                 total_requests, expires_at)
            SELECT md5(i::text)::uuid, '/v1/chat/completions', '24h', 'owner',
                   '2026-01-01'::timestamptz + i * interval '1 second',
                   '2026-02-01'::timestamptz,
                   '2026-02-01'::timestamptz + i * interval '1 second',
                   CASE WHEN i <= 20000 THEN '2026-02-02'::timestamptz END,
                   1, '2026-02-02'::timestamptz
            FROM generate_series(1, 20005) i
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("ANALYZE batches").execute(&pool).await.unwrap();

        let (usable, predicate): (bool, String) = sqlx::query_as(
            "SELECT i.indisvalid AND i.indisready, pg_get_expr(i.indpred, i.indrelid)
             FROM pg_index i WHERE i.indexrelid = 'idx_batches_notification_due'::regclass",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            usable,
            "idx_batches_notification_due must be valid and ready"
        );
        assert_eq!(
            predicate,
            "((counts_frozen_at IS NOT NULL) AND (notification_sent_at IS NULL) AND (cancelling_at IS NULL) AND (deleted_at IS NULL) AND (total_requests > 0))"
        );

        let plan: serde_json::Value = sqlx::query_scalar(&format!(
            "EXPLAIN (FORMAT JSON) {CLAIM_BATCH_NOTIFICATIONS_SQL}"
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            plan_scans_index(&plan, "Index Scan", "idx_batches_notification_due"),
            "claim candidates must come from an ordered scan of idx_batches_notification_due: {plan}"
        );

        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let claimed = manager.claim_batch_notifications().await.unwrap();
        assert_eq!(claimed.len(), 5);
        assert!(
            manager
                .claim_batch_notifications()
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[sqlx::test]
    async fn finalizer_counts_only_batches_without_live_requests(pool: sqlx::PgPool) {
        sqlx::raw_sql(
            r#"
            INSERT INTO batches (id, endpoint, completion_window, created_by, created_at,
                                 total_requests, expires_at)
            SELECT md5('b' || i)::uuid, '/v1/chat/completions', '24h', 'owner', now(), 20,
                   now() + interval '1 day'
            FROM generate_series(1, 2005) i;
            INSERT INTO requests (batch_id, model, state, response_status, response_body,
                                  completed_at)
            SELECT md5('b' || i)::uuid, 'finalizer-test', s.state,
                   CASE WHEN s.state = 'completed' THEN 200 END,
                   CASE WHEN s.state = 'completed' THEN '{}' END,
                   CASE WHEN s.state = 'completed' THEN now() END
            FROM generate_series(1, 2005) i, generate_series(1, 20) j,
                 LATERAL (SELECT CASE WHEN i <= 2000 AND j = 20 THEN 'pending'
                                      ELSE 'completed' END AS state) s;
            ANALYZE batches;
            ANALYZE requests;
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let plan: serde_json::Value = sqlx::query_scalar(
            r#"
            EXPLAIN (ANALYZE, FORMAT JSON)
            SELECT b.id
            FROM batches b
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE state = 'completed') AS completed,
                    COUNT(*) FILTER (WHERE state = 'failed') AS failed,
                    COUNT(*) FILTER (WHERE state = 'canceled') AS canceled,
                    COUNT(*) FILTER (WHERE state NOT IN ('completed', 'failed', 'canceled')) AS live
                FROM requests WHERE batch_id = b.id
            ) counts ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE state = 'completed') AS completed,
                    COUNT(*) FILTER (WHERE state = 'failed') AS failed,
                    COUNT(*) FILTER (WHERE state = 'canceled') AS canceled
                FROM batch_requests_archive a
                WHERE b.location = 'split'
                  AND a.archive_bucket = b.archive_bucket
                  AND a.batch_id = b.id
            ) arch ON TRUE
            WHERE b.counts_frozen_at IS NULL
              AND b.deleted_at IS NULL
              AND b.cancelling_at IS NULL
              AND b.total_requests > 0
              AND NOT EXISTS (
                  SELECT 1 FROM requests r
                  WHERE r.batch_id = b.id
                    AND r.state IN ('pending', 'claimed', 'processing')
              )
              AND COALESCE(counts.live, 0) = 0
              AND COALESCE(counts.completed, 0) + COALESCE(counts.failed, 0) + COALESCE(counts.canceled, 0)
                  + COALESCE(arch.completed, 0) + COALESCE(arch.failed, 0) + COALESCE(arch.canceled, 0)
                  = b.total_requests
            "#,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        fn request_rows_visited(node: &serde_json::Value) -> f64 {
            match node {
                serde_json::Value::Array(values) => values.iter().map(request_rows_visited).sum(),
                serde_json::Value::Object(fields) => {
                    let visited = if fields.get("Relation Name").and_then(|v| v.as_str())
                        == Some("requests")
                    {
                        [
                            "Actual Rows",
                            "Rows Removed by Filter",
                            "Rows Removed by Index Recheck",
                        ]
                        .iter()
                        .map(|key| fields.get(*key).and_then(|v| v.as_f64()).unwrap_or(0.0))
                        .sum::<f64>()
                            * fields
                                .get("Actual Loops")
                                .and_then(|v| v.as_f64())
                                .unwrap_or(0.0)
                    } else {
                        0.0
                    };
                    visited + fields.values().map(request_rows_visited).sum::<f64>()
                }
                _ => 0.0,
            }
        }
        let read = request_rows_visited(&plan);
        assert!(
            read < 10_000.0,
            "finalizer read {read} request rows: {plan}"
        );

        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        assert_eq!(manager.finalize_terminal_batches().await.unwrap(), 5);
        let frozen: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM batches WHERE counts_frozen_at IS NOT NULL AND completed_requests = 20",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(frozen, 5);
    }

    #[sqlx::test]
    async fn test_finalizer_freezes_counts_and_notifier_claims(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "freeze-poller", 2).await;

        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let premature = manager.claim_batch_notifications().await.unwrap();
        assert!(
            !premature.iter().any(|n| n.batch.id == batch_id),
            "notifier must not see unfinalized batches"
        );

        let finalized = manager.finalize_terminal_batches().await.unwrap();
        assert!(finalized >= 1, "finalizer must pick up the settled batch");
        assert!(
            frozen_at(&pool, batch_id).await.is_some(),
            "finalizer must freeze counts"
        );

        let notifications = manager.claim_batch_notifications().await.unwrap();
        let n = notifications
            .iter()
            .find(|n| n.batch.id == batch_id)
            .unwrap();
        assert_eq!(n.batch.completed_requests, 2);

        let again = manager.claim_batch_notifications().await.unwrap();
        assert!(!again.iter().any(|n| n.batch.id == batch_id));

        sqlx::query!(
            "DELETE FROM requests WHERE batch_id = $1",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        let batch = manager.get_batch(batch_id).await.unwrap();
        assert_eq!(batch.completed_requests, 2);
    }

    #[sqlx::test]
    async fn test_finalizer_settles_and_freezes_cancelled_batch(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "freeze-cancelled", 3).await;

        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE id = (SELECT id FROM requests WHERE batch_id = $1 LIMIT 1)",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        manager.cancel_batch(batch_id).await.unwrap();

        let finalized = manager
            .finalize_cancelled_batches(3600.0, 10)
            .await
            .unwrap();
        assert_eq!(finalized, 0, "grace window must be respected");
        assert!(frozen_at(&pool, batch_id).await.is_none());

        let finalized = manager.finalize_cancelled_batches(0.0, 10).await.unwrap();
        assert_eq!(finalized, 1);
        assert!(
            frozen_at(&pool, batch_id).await.is_some(),
            "cancelled batch must freeze once settled"
        );
        let stuck: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) AS "count!" FROM requests WHERE batch_id = $1 AND state IN ('pending', 'claimed', 'processing')"#,
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stuck, 0, "leftover rows must be physically settled");

        let batch = manager.get_batch(batch_id).await.unwrap();
        assert_eq!(batch.completed_requests, 1);
        assert_eq!(batch.canceled_requests, 2);

        let notifications = manager.claim_batch_notifications().await.unwrap();
        assert!(!notifications.iter().any(|n| n.batch.id == batch_id));
    }

    #[sqlx::test]
    async fn test_finalizer_skips_populate_race_zombie(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "freeze-zombie", 2).await;
        manager.cancel_batch(batch_id).await.unwrap();
        sqlx::query!(
            "DELETE FROM requests WHERE batch_id = $1",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let finalized = manager.finalize_cancelled_batches(0.0, 10).await.unwrap();
        assert_eq!(finalized, 0, "zombie must be skipped, not frozen");
        assert!(frozen_at(&pool, batch_id).await.is_none());
    }

    #[sqlx::test]
    async fn test_failed_batch_sample_classes(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let failed = setup_freeze_test_batch(&manager, "sample-failed", 2).await;
        let ok = setup_freeze_test_batch(&manager, "sample-ok", 2).await;
        let reason = serde_json::to_string(&FailureReason::NonRetriableHttpStatus {
            status: 422,
            body: "secret detail".to_string(),
        })
        .unwrap();
        sqlx::query!(
            "UPDATE requests SET state = 'failed', failed_at = NOW(), error = $2 WHERE batch_id = $1",
            *failed as Uuid,
            reason
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1",
            *ok as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        assert!(
            manager
                .failed_batch_sample_classes(&[])
                .await
                .unwrap()
                .is_empty()
        );
        let samples = manager
            .failed_batch_sample_classes(&[failed, ok])
            .await
            .unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[&failed], "http_422");

        let reason = serde_json::to_string(&FailureReason::Timeout {
            error: "secret".to_string(),
        })
        .unwrap();
        sqlx::query!(
            "UPDATE requests SET error = $2 WHERE batch_id = $1",
            *failed as Uuid,
            reason
        )
        .execute(&pool)
        .await
        .unwrap();
        let samples = manager
            .failed_batch_sample_classes(&[failed])
            .await
            .unwrap();
        assert_eq!(samples[&failed], "timeout");

        manager.get_batch(failed).await.unwrap();
        assert_eq!(
            manager.archive_batch(failed).await.unwrap(),
            ArchiveOutcome::Archived { rows: 2 }
        );
        let (_, _, live, archived) = archive_state(&pool, failed).await;
        assert_eq!((live, archived), (0, 2));
        let samples = manager
            .failed_batch_sample_classes(&[failed, ok])
            .await
            .unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[&failed], "timeout");
    }

    #[sqlx::test]
    async fn test_poller_returns_background_batch_without_sla_fields(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch = create_background_batch_for_test(&manager, "background-poller", "owner").await;

        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1",
            *batch.id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        manager.finalize_terminal_batches().await.unwrap();
        let notifications = manager.claim_batch_notifications().await.unwrap();
        let notification = notifications
            .iter()
            .find(|notification| notification.batch.id == batch.id)
            .expect("background batch should be claimed once finalized");

        assert_eq!(
            notification.batch.service_tier.as_deref(),
            Some("background")
        );
        assert!(notification.batch.completion_window.is_none());
        assert!(notification.batch.expires_at.is_none());
    }

    async fn backfill_chunk(
        pool: &sqlx::PgPool,
        before: Option<(DateTime<Utc>, Uuid)>,
        chunk_size: i64,
    ) -> (i64, Option<(DateTime<Utc>, Uuid)>) {
        assert!(chunk_size > 0, "chunk_size must be > 0");
        let (before_created_at, before_id) = match before {
            Some((c, i)) => (Some(c), Some(i)),
            None => (None, None),
        };
        let row = sqlx::query(
            r#"
            WITH todo AS (
                SELECT b.id, b.created_at, b.retry_version
                FROM batches b
                WHERE b.counts_frozen_at IS NULL
                  AND b.deleted_at IS NULL
                  AND (b.completed_at IS NOT NULL OR b.failed_at IS NOT NULL OR b.cancelled_at IS NOT NULL)
                  AND ($1::TIMESTAMPTZ IS NULL OR (b.created_at, b.id) < ($1, $3::UUID))
                ORDER BY b.created_at DESC, b.id DESC
                LIMIT $2
            ),
            counted AS (
                SELECT t.id, t.retry_version,
                       COALESCE(c.completed, 0) AS completed,
                       COALESCE(c.failed, 0) AS failed,
                       COALESCE(c.canceled, 0) AS canceled,
                       COALESCE(c.non_terminal, 0) AS non_terminal
                FROM todo t
                LEFT JOIN LATERAL (
                    SELECT COUNT(*) FILTER (WHERE state = 'completed') AS completed,
                           COUNT(*) FILTER (WHERE state = 'failed') AS failed,
                           COUNT(*) FILTER (WHERE state = 'canceled') AS canceled,
                           COUNT(*) FILTER (WHERE state NOT IN ('completed', 'failed', 'canceled')) AS non_terminal
                    FROM requests r WHERE r.batch_id = t.id
                ) c ON TRUE
            ),
            frozen AS (
                UPDATE batches b
                SET completed_requests = counted.completed,
                    failed_requests = counted.failed,
                    canceled_requests = counted.canceled,
                    counts_frozen_at = NOW()
                FROM counted
                WHERE b.id = counted.id
                  AND counted.non_terminal = 0
                  AND b.counts_frozen_at IS NULL
                  AND b.retry_version = counted.retry_version
                RETURNING b.id
            )
            SELECT (SELECT COUNT(*) FROM frozen) AS frozen,
                   (SELECT t.created_at FROM todo t ORDER BY t.created_at ASC, t.id ASC LIMIT 1) AS cursor_created_at,
                   (SELECT t.id FROM todo t ORDER BY t.created_at ASC, t.id ASC LIMIT 1) AS cursor_id
            "#,
        )
        .bind(before_created_at)
        .bind(chunk_size)
        .bind(before_id)
        .fetch_one(pool)
        .await
        .unwrap();
        let frozen: i64 = row.get("frozen");
        let cursor = match (
            row.get::<Option<DateTime<Utc>>, _>("cursor_created_at"),
            row.get::<Option<Uuid>, _>("cursor_id"),
        ) {
            (Some(c), Some(i)) => Some((c, i)),
            _ => None,
        };
        (frozen, cursor)
    }

    #[sqlx::test]
    async fn test_backfill_frozen_counts(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        let mut historic = Vec::new();
        for i in 0..3 {
            let batch_id = setup_freeze_test_batch(&manager, &format!("backfill-{i}"), 2).await;
            sqlx::query!(
                "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1",
                *batch_id as Uuid
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query!(
                "UPDATE batches SET finalizing_at = NOW(), completed_at = NOW() WHERE id = $1",
                *batch_id as Uuid
            )
            .execute(&pool)
            .await
            .unwrap();
            historic.push(batch_id);
        }

        let straggler = setup_freeze_test_batch(&manager, "backfill-straggler", 2).await;
        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1 AND id IN (SELECT id FROM requests WHERE batch_id = $1 LIMIT 1)",
            *straggler as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query!(
            "UPDATE batches SET finalizing_at = NOW(), completed_at = NOW() WHERE id = $1",
            *straggler as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query!(
            "UPDATE batches SET created_at = '2020-01-01T00:00:00Z' WHERE id = ANY($1)",
            &historic
                .iter()
                .map(|b| **b as Uuid)
                .chain(std::iter::once(*straggler as Uuid))
                .collect::<Vec<_>>()
        )
        .execute(&pool)
        .await
        .unwrap();

        let mut cursor = None;
        let mut total_frozen = 0i64;
        let mut iterations = 0;
        loop {
            let (frozen, next) = backfill_chunk(&pool, cursor, 2).await;
            total_frozen += frozen;
            iterations += 1;
            assert!(iterations < 20, "backfill must terminate");
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        assert_eq!(total_frozen, 3, "all historic batches frozen");
        for batch_id in &historic {
            assert!(frozen_at(&pool, *batch_id).await.is_some());
            let batch = manager.get_batch(*batch_id).await.unwrap();
            assert_eq!(batch.completed_requests, 2);
        }
        assert!(
            frozen_at(&pool, straggler).await.is_none(),
            "straggler with non-terminal rows must be skipped"
        );

        let (frozen_again, _) = backfill_chunk(&pool, None, 100).await;
        assert_eq!(frozen_again, 0);
    }

    async fn setup_frozen_batch(
        manager: &PostgresRequestManager<TestDbPools>,
        pool: &sqlx::PgPool,
        name: &str,
        n: usize,
    ) -> BatchId {
        let batch_id = setup_freeze_test_batch(manager, name, n).await;
        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1",
            *batch_id as Uuid
        )
        .execute(pool)
        .await
        .unwrap();
        manager.get_batch(batch_id).await.unwrap();
        let frozen: bool = sqlx::query_scalar!(
            r#"SELECT counts_frozen_at IS NOT NULL AS "f!" FROM batches WHERE id = $1"#,
            *batch_id as Uuid
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert!(frozen, "freeze-on-read must have frozen the batch");
        batch_id
    }

    async fn archive_state(
        pool: &sqlx::PgPool,
        batch_id: BatchId,
    ) -> (String, Option<chrono::NaiveDate>, i64, i64) {
        let b = sqlx::query!(
            r#"SELECT location, archive_bucket,
                      (SELECT COUNT(*) FROM requests WHERE batch_id = $1) AS "live!",
                      (SELECT COUNT(*) FROM batch_requests_archive WHERE batch_id = $1) AS "archived!"
               FROM batches WHERE id = $1"#,
            *batch_id as Uuid
        )
        .fetch_one(pool)
        .await
        .unwrap();
        (b.location, b.archive_bucket, b.live, b.archived)
    }

    #[sqlx::test]
    async fn test_archive_batch_moves_rows_and_stamps_location(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_frozen_batch(&manager, &pool, "arch-happy", 3).await;
        let frozen_before = sqlx::query!(
            "SELECT completed_requests, counts_frozen_at FROM batches WHERE id = $1",
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let outcome = manager.archive_batch(batch_id).await.unwrap();
        assert_eq!(outcome, ArchiveOutcome::Archived { rows: 3 });

        let (location, bucket, live, archived) = archive_state(&pool, batch_id).await;
        assert_eq!(location, "archive");
        assert!(bucket.is_some(), "archive_bucket must be stamped");
        assert_eq!((live, archived), (0, 3), "rows live in exactly one table");

        let frozen_after = sqlx::query!(
            "SELECT completed_requests, counts_frozen_at FROM batches WHERE id = $1",
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            frozen_after.completed_requests,
            frozen_before.completed_requests
        );
        assert_eq!(
            frozen_after.counts_frozen_at,
            frozen_before.counts_frozen_at
        );

        let again = manager.archive_batch(batch_id).await.unwrap();
        assert_eq!(again, ArchiveOutcome::SkippedNotLive);
    }

    #[sqlx::test]
    async fn test_archive_batch_bounces_off_held_lock(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_frozen_batch(&manager, &pool, "arch-contend", 3).await;

        let mut holder = pool.begin().await.unwrap();
        sqlx::query!(
            "SELECT id FROM batches WHERE id = $1 FOR UPDATE",
            *batch_id as Uuid
        )
        .fetch_one(&mut *holder)
        .await
        .unwrap();

        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            manager.archive_batch(batch_id),
        )
        .await
        .expect("archive_batch must not queue behind a held batch lock")
        .unwrap();
        assert_eq!(outcome, ArchiveOutcome::SkippedNotLive);

        let (location, _, live, archived) = archive_state(&pool, batch_id).await;
        assert_eq!((location.as_str(), live, archived), ("live", 3, 0));

        holder.rollback().await.unwrap();
        let outcome = manager.archive_batch(batch_id).await.unwrap();
        assert_eq!(outcome, ArchiveOutcome::Archived { rows: 3 });
    }

    #[sqlx::test]
    async fn test_concurrent_archive_batch_exactly_one_wins(pool: sqlx::PgPool) {
        let manager = Arc::new(PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        ));
        let batch_id = setup_frozen_batch(&manager, &pool, "arch-race", 3).await;

        let (a, b) = tokio::join!(
            {
                let m = manager.clone();
                async move { m.archive_batch(batch_id).await.unwrap() }
            },
            {
                let m = manager.clone();
                async move { m.archive_batch(batch_id).await.unwrap() }
            }
        );

        let archived_count = [&a, &b]
            .iter()
            .filter(|o| matches!(o, ArchiveOutcome::Archived { rows: 3 }))
            .count();
        assert_eq!(
            archived_count, 1,
            "exactly one mover must win: {a:?} / {b:?}"
        );
        let loser = if matches!(a, ArchiveOutcome::Archived { .. }) {
            &b
        } else {
            &a
        };
        assert!(
            matches!(loser, ArchiveOutcome::SkippedNotLive),
            "loser must skip benignly, got {loser:?}"
        );

        let (location, _, live, archived) = archive_state(&pool, batch_id).await;
        assert_eq!((location.as_str(), live, archived), ("archive", 0, 3));
    }

    #[sqlx::test]
    async fn test_archive_batch_refuses_unfrozen_and_active(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let active = setup_freeze_test_batch(&manager, "arch-active", 2).await;
        assert_eq!(
            manager.archive_batch(active).await.unwrap(),
            ArchiveOutcome::SkippedNotFrozen
        );
        let (_, _, live, archived) = archive_state(&pool, active).await;
        assert_eq!(
            (live, archived),
            (2, 0),
            "nothing may move for an active batch"
        );

        let unfrozen = setup_freeze_test_batch(&manager, "arch-unfrozen", 2).await;
        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}' WHERE batch_id = $1",
            *unfrozen as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            manager.archive_batch(unfrozen).await.unwrap(),
            ArchiveOutcome::SkippedNotFrozen
        );

        assert_eq!(
            manager
                .archive_batch(BatchId(Uuid::new_v4()))
                .await
                .unwrap(),
            ArchiveOutcome::SkippedNotFound
        );
    }

    #[sqlx::test]
    async fn test_archive_batch_missing_partition_degrades_gracefully(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_frozen_batch(&manager, &pool, "arch-nopart", 2).await;
        sqlx::query!(
            "UPDATE batches SET created_at = '2020-01-06T12:00:00Z' WHERE id = $1",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE batch_requests_archive_y2020w02 (LIKE batch_requests_archive INCLUDING ALL)",
        )
        .execute(&pool)
        .await
        .unwrap();

        assert_eq!(
            manager.archive_batch(batch_id).await.unwrap(),
            ArchiveOutcome::SkippedNoPartition
        );
        let (location, _, live, archived) = archive_state(&pool, batch_id).await;
        assert_eq!(location, "live", "batch must stay fully live and served");
        assert_eq!((live, archived), (2, 0));
    }

    #[sqlx::test]
    async fn test_archive_batch_outliving_its_retired_week_lands_in_the_current_week(
        pool: sqlx::PgPool,
    ) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_frozen_batch(&manager, &pool, "arch-retired-week", 2).await;
        sqlx::query!(
            "UPDATE batches SET created_at = '2020-01-06T12:00:00Z' WHERE id = $1",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO batch_archive_buckets (week_start, partition_schema, partition_table, partition_oid, state) \
             VALUES ('2020-01-06', current_schema(), 'batch_requests_archive_y2020w02', 'batches'::regclass::oid, 'retired')",
        )
        .execute(&pool)
        .await
        .unwrap();

        assert!(matches!(
            manager.archive_batch(batch_id).await.unwrap(),
            ArchiveOutcome::Archived { .. }
        ));
        let (location, bucket, live, archived) = archive_state(&pool, batch_id).await;
        assert_eq!(location, "archive");
        assert_eq!((live, archived), (0, 2));
        let current_week: chrono::NaiveDate =
            sqlx::query_scalar("SELECT date_trunc('week', now() AT TIME ZONE 'UTC')::date")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            bucket,
            Some(current_week),
            "the batch must be stamped into the current week"
        );
    }

    #[sqlx::test]
    async fn test_archive_batch_in_a_retiring_week_lands_in_the_current_week(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_frozen_batch(&manager, &pool, "arch-retiring-week", 2).await;
        sqlx::query!(
            "UPDATE batches SET created_at = '2020-01-06T12:00:00Z' WHERE id = $1",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("SELECT ensure_archive_partitions_for('2020-01-06'::date)")
            .execute(&pool)
            .await
            .ok();
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS batch_requests_archive_y2020w02 PARTITION OF batch_requests_archive FOR VALUES FROM ('2020-01-06') TO ('2020-01-13')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO batch_archive_buckets (week_start, partition_schema, partition_table, partition_oid, state) \
             VALUES ('2020-01-06', current_schema(), 'batch_requests_archive_y2020w02', 'batch_requests_archive_y2020w02'::regclass::oid, 'retiring') \
             ON CONFLICT (week_start) DO UPDATE SET state = 'retiring'",
        )
        .execute(&pool)
        .await
        .unwrap();

        assert!(matches!(
            manager.archive_batch(batch_id).await.unwrap(),
            ArchiveOutcome::Archived { .. }
        ));
        let (location, bucket, live, archived) = archive_state(&pool, batch_id).await;
        assert_eq!(location, "archive");
        assert_eq!((live, archived), (0, 2));
        let current_week: chrono::NaiveDate =
            sqlx::query_scalar("SELECT date_trunc('week', now() AT TIME ZONE 'UTC')::date")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(bucket, Some(current_week));
        let landed_in_fenced_week: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM batch_requests_archive_y2020w02")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            landed_in_fenced_week, 0,
            "a fenced week must never receive rows"
        );
    }

    #[sqlx::test]
    async fn test_purge_leaves_pending_requests_without_deletion_tombstones(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let stranded = setup_freeze_test_batch(&manager, "purge-stranded", 2).await;
        let healthy = setup_freeze_test_batch(&manager, "purge-healthy", 1).await;
        sqlx::query(
            "WITH doomed AS (
                 SELECT template_id FROM requests WHERE batch_id = $1
             ), removed AS (
                 DELETE FROM request_templates_g2 template
                 USING doomed WHERE template.id = doomed.template_id
             )
             DELETE FROM request_template_routes route
             USING doomed WHERE route.template_id = doomed.template_id",
        )
        .bind(*stranded as Uuid)
        .execute(&pool)
        .await
        .unwrap();

        let purged = manager.purge_orphaned_rows(100).await.unwrap();
        assert_eq!(purged, 0, "no deletion tombstones means no purge work");
        let states: Vec<(String, Option<String>)> =
            sqlx::query_as("SELECT state, error FROM requests WHERE batch_id = $1 ORDER BY id")
                .bind(*stranded as Uuid)
                .fetch_all(&pool)
                .await
                .unwrap();
        assert!(
            states
                .iter()
                .all(|(state, error)| state == "pending" && error.is_none())
        );
        let healthy_pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM requests WHERE batch_id = $1 AND state = 'pending'",
        )
        .bind(*healthy as Uuid)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            healthy_pending, 1,
            "requests with a live template are untouched"
        );
    }

    #[sqlx::test]
    async fn test_archive_batch_crash_resume_is_idempotent(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_frozen_batch(&manager, &pool, "arch-resume", 3).await;

        sqlx::query(
            "INSERT INTO batch_requests_archive
             SELECT r.*, date_trunc('week', (SELECT created_at FROM batches WHERE id = $1) AT TIME ZONE 'UTC')::date
             FROM requests r WHERE r.batch_id = $1
             ORDER BY r.id LIMIT 1",
        )
        .bind(*batch_id as Uuid)
        .execute(&pool)
        .await
        .unwrap();

        let outcome = manager.archive_batch(batch_id).await.unwrap();
        assert_eq!(outcome, ArchiveOutcome::Archived { rows: 3 });
        let (location, _, live, archived) = archive_state(&pool, batch_id).await;
        assert_eq!(location, "archive");
        assert_eq!((live, archived), (0, 3));
    }

    #[sqlx::test]
    async fn test_archived_batch_reads_retry_and_purge(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "reads-arch", 3).await;
        sqlx::query!(
            "UPDATE requests SET state = 'completed', claimed_at = NOW(), started_at = NOW(),
             completed_at = NOW(), response_status = 200, response_body = '{}',
             daemon_id = gen_random_uuid()
             WHERE batch_id = $1 AND id IN (SELECT id FROM requests WHERE batch_id = $1 ORDER BY id LIMIT 2)",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query!(
            "UPDATE requests SET state = 'failed', failed_at = NOW(),
             error = '{\"type\":\"non_retriable_http_status\",\"status\":500,\"body\":\"boom\"}'
             WHERE batch_id = $1 AND state <> 'completed'",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        manager.get_batch(batch_id).await.unwrap();
        assert_eq!(
            manager.archive_batch(batch_id).await.unwrap(),
            ArchiveOutcome::Archived { rows: 3 }
        );

        let rows = manager.get_batch_requests(batch_id).await.unwrap();
        assert_eq!(rows.len(), 3, "archived rows must remain listable");
        let failed_id = rows
            .iter()
            .find_map(|r| match r {
                AnyRequest::Failed(req) => Some(req.data.id),
                _ => None,
            })
            .expect("the failed row must be visible");

        let fetched = manager.get_requests(vec![failed_id]).await.unwrap();
        assert!(matches!(fetched[0], Ok(AnyRequest::Failed(_))));

        let results = manager
            .retry_failed_requests(batch_id, vec![failed_id])
            .await
            .unwrap();
        assert!(
            results[0].is_ok(),
            "archived failed row must be retryable: {:?}",
            results[0]
        );
        let b = sqlx::query!(
            r#"SELECT location, retry_version, counts_frozen_at,
                      (SELECT COUNT(*) FROM requests WHERE batch_id = $1 AND state = 'pending') AS "pending!",
                      (SELECT COUNT(*) FROM batch_requests_archive WHERE batch_id = $1) AS "archived!"
               FROM batches WHERE id = $1"#,
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(b.location, "split");
        assert_eq!((b.pending, b.archived), (1, 2));
        assert!(b.counts_frozen_at.is_none());
        assert_eq!(b.retry_version, 1);

        sqlx::query!(
            "UPDATE requests SET state = 'completed', claimed_at = NOW(), started_at = NOW(),
             completed_at = NOW(), response_status = 200, response_body = '{}',
             error = NULL, failed_at = NULL, daemon_id = gen_random_uuid()
             WHERE batch_id = $1 AND state = 'pending'",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        manager.get_batch(batch_id).await.unwrap();
        assert_eq!(
            manager.archive_batch(batch_id).await.unwrap(),
            ArchiveOutcome::Archived { rows: 1 }
        );
        sqlx::query!(
            "UPDATE batches SET deleted_at = NOW() WHERE id = $1",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        let purged = manager.purge_orphaned_rows(100).await.unwrap();
        assert!(purged >= 3, "purge must erase archived rows, got {purged}");
        let remaining: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) AS "c!" FROM batch_requests_archive WHERE batch_id = $1"#,
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(remaining, 0, "right-to-erasure must reach the archive");
    }

    #[sqlx::test]
    async fn test_batch_results_stream_serves_archived_batches(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "results-arch", 3).await;
        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200,
             response_body = '{\"ok\":true}' WHERE batch_id = $1",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        manager.get_batch(batch_id).await.unwrap();

        let live: Vec<_> = manager
            .get_batch_results_stream(batch_id, 0, None, None)
            .collect()
            .await;
        assert_eq!(live.len(), 3, "pre-archive stream serves all rows");
        for item in live {
            item.expect("live result item must be Ok");
        }

        assert_eq!(
            manager.archive_batch(batch_id).await.unwrap(),
            ArchiveOutcome::Archived { rows: 3 }
        );

        let archived: Vec<_> = manager
            .get_batch_results_stream(batch_id, 0, None, None)
            .collect()
            .await;
        assert_eq!(
            archived.len(),
            3,
            "post-archive stream must serve the same rows from the archive"
        );
        for item in archived {
            item.expect("archived result item must be Ok");
        }
    }

    #[sqlx::test]
    async fn test_download_streams_serve_archived_batches(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "dl-arch", 3).await;
        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200,
             response_body = '{\"ok\":true}'
             WHERE batch_id = $1 AND id IN (SELECT id FROM requests WHERE batch_id = $1 ORDER BY id LIMIT 2)",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query!(
            "UPDATE requests SET state = 'failed', failed_at = NOW(), error = '{\"type\":\"test\"}'
             WHERE batch_id = $1 AND state <> 'completed'",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        manager.get_batch(batch_id).await.unwrap();
        assert_eq!(
            manager.archive_batch(batch_id).await.unwrap(),
            ArchiveOutcome::Archived { rows: 3 }
        );

        let b = sqlx::query!(
            r#"SELECT output_file_id AS "o!", error_file_id AS "e!" FROM batches WHERE id = $1"#,
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let output: Vec<_> = manager
            .get_file_content_stream(FileId(b.o), 0, None)
            .collect()
            .await;
        assert_eq!(output.len(), 2, "archived completions must stream");
        for item in &output {
            assert!(matches!(item.as_ref().unwrap(), FileContentItem::Output(_)));
        }

        let errors: Vec<_> = manager
            .get_file_content_stream(FileId(b.e), 0, None)
            .collect()
            .await;
        assert_eq!(errors.len(), 1, "archived failures must stream");
    }

    #[sqlx::test]
    async fn test_download_continues_across_archive_move(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "dl-flip", 3).await;
        sqlx::query!(
            r#"
            UPDATE requests r SET state = 'completed', response_status = 200,
                   response_body = '{"n":' || o.rn || '}',
                   completed_at = NOW() - (10 - o.rn) * INTERVAL '1 second'
            FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn
                  FROM requests WHERE batch_id = $1) o
            WHERE r.id = o.id
            "#,
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        manager.get_batch(batch_id).await.unwrap();

        let output_file = sqlx::query_scalar!(
            r#"SELECT output_file_id AS "o!" FROM batches WHERE id = $1"#,
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let first: Vec<_> = manager
            .get_file_content_stream(FileId(output_file), 0, None)
            .take(2)
            .collect()
            .await;
        assert_eq!(
            manager.archive_batch(batch_id).await.unwrap(),
            ArchiveOutcome::Archived { rows: 3 }
        );
        let rest: Vec<_> = manager
            .get_file_content_stream(FileId(output_file), 2, None)
            .collect()
            .await;
        assert_eq!(
            rest.len(),
            1,
            "exactly the one remaining row, no repeats or gaps"
        );

        let mut ids: Vec<String> = first
            .iter()
            .chain(rest.iter())
            .map(|r| match r.as_ref().unwrap() {
                FileContentItem::Output(o) => o.id.clone(),
                other => panic!("expected output item, got {other:?}"),
            })
            .collect();
        let total = ids.len();
        ids.dedup();
        assert_eq!(ids.len(), 3, "3 distinct rows across the flip");
        assert_eq!(total, 3);
    }

    #[sqlx::test]
    async fn test_retry_of_archived_batch_splits_resumes_and_rearchives(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "roundtrip", 3).await;
        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}'
             WHERE batch_id = $1 AND id IN (SELECT id FROM requests WHERE batch_id = $1 ORDER BY id LIMIT 2)",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query!(
            "UPDATE requests SET state = 'failed', failed_at = NOW(), error = 'boom'
             WHERE batch_id = $1 AND state <> 'completed'",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        manager.get_batch(batch_id).await.unwrap();
        assert_eq!(
            manager.archive_batch(batch_id).await.unwrap(),
            ArchiveOutcome::Archived { rows: 3 }
        );

        let retried = manager
            .retry_failed_requests_for_batch(batch_id)
            .await
            .unwrap();
        assert_eq!(retried, 1);
        let b = sqlx::query!(
            r#"SELECT location, archive_bucket, counts_frozen_at, retry_version,
                      (SELECT COUNT(*) FROM requests WHERE batch_id = $1) AS "live!",
                      (SELECT COUNT(*) FROM requests WHERE batch_id = $1 AND state = 'pending') AS "live_pending!",
                      (SELECT COUNT(*) FROM batch_requests_archive WHERE batch_id = $1) AS "archived!"
               FROM batches WHERE id = $1"#,
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(b.location, "split");
        assert!(
            b.archive_bucket.is_some(),
            "bucket stamp survives the split"
        );
        assert!(b.counts_frozen_at.is_none(), "retry unfreezes");
        assert_eq!(b.retry_version, 1);
        assert_eq!(
            (b.live, b.live_pending, b.archived),
            (1, 1, 2),
            "failed row re-pended live; completed rows stay archived"
        );

        let mid = manager.get_batch(batch_id).await.unwrap();
        assert_eq!(mid.completed_requests, 2, "archived completions must count");
        assert_eq!(mid.pending_requests, 1);

        sqlx::query!(
            "UPDATE requests SET state = 'completed', completed_at = NOW(), response_status = 200, response_body = '{}',
             daemon_id = NULL, claimed_at = NULL, started_at = NULL
             WHERE batch_id = $1 AND state = 'pending'",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        let done = manager.get_batch(batch_id).await.unwrap();
        assert_eq!(done.completed_requests, 3);
        let frozen = sqlx::query!(
            r#"SELECT counts_frozen_at IS NOT NULL AS "f!", completed_requests, failed_requests
               FROM batches WHERE id = $1"#,
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(frozen.f, "split batch must re-freeze once resumed");
        assert_eq!((frozen.completed_requests, frozen.failed_requests), (3, 0));

        let candidates = manager
            .list_archivable_batches(50, true, 0.0, 0.0)
            .await
            .unwrap();
        assert!(
            candidates.contains(&batch_id),
            "re-frozen split batch must be a sweep candidate"
        );
        assert_eq!(
            manager.archive_batch(batch_id).await.unwrap(),
            ArchiveOutcome::Archived { rows: 1 }
        );
        let after = sqlx::query!(
            r#"SELECT location,
                      (SELECT COUNT(*) FROM requests WHERE batch_id = $1) AS "live!",
                      (SELECT COUNT(*) FROM batch_requests_archive WHERE batch_id = $1) AS "archived!",
                      (SELECT COUNT(DISTINCT archive_bucket) FROM batch_requests_archive WHERE batch_id = $1) AS "buckets!"
               FROM batches WHERE id = $1"#,
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(after.location, "archive");
        assert_eq!((after.live, after.archived), (0, 3));
        assert_eq!(after.buckets, 1, "a batch never scatters across partitions");
    }

    #[sqlx::test]
    async fn test_retry_of_fully_failed_archived_batch_returns_live(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch_id = setup_freeze_test_batch(&manager, "allfailed", 2).await;
        sqlx::query!(
            "UPDATE requests SET state = 'failed', failed_at = NOW(), error = 'boom' WHERE batch_id = $1",
            *batch_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        manager.get_batch(batch_id).await.unwrap();
        assert_eq!(
            manager.archive_batch(batch_id).await.unwrap(),
            ArchiveOutcome::Archived { rows: 2 }
        );

        assert_eq!(
            manager
                .retry_failed_requests_for_batch(batch_id)
                .await
                .unwrap(),
            2
        );
        let b = sqlx::query!(
            r#"SELECT location, archive_bucket,
                      (SELECT COUNT(*) FROM requests WHERE batch_id = $1 AND state = 'pending') AS "pending!",
                      (SELECT COUNT(*) FROM batch_requests_archive WHERE batch_id = $1) AS "archived!"
               FROM batches WHERE id = $1"#,
            *batch_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            b.location, "live",
            "empty archive side means fully live again"
        );
        assert!(b.archive_bucket.is_some(), "bucket stamp is never cleared");
        assert_eq!((b.pending, b.archived), (2, 0));
    }

    #[sqlx::test]
    async fn test_archive_candidates_respect_cancel_grace(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        let in_flight = setup_freeze_test_batch(&manager, "grace-inflight", 1).await;
        sqlx::query!(
            "UPDATE requests SET state = 'canceled', canceled_at = NOW() - INTERVAL '5 minutes',
             claimed_at = NOW() - INTERVAL '6 minutes' WHERE batch_id = $1",
            *in_flight as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        manager.get_batch(in_flight).await.unwrap();

        let pending_cancel = setup_freeze_test_batch(&manager, "grace-pending", 1).await;
        sqlx::query!(
            "UPDATE requests SET state = 'canceled', canceled_at = NOW() - INTERVAL '5 minutes',
             claimed_at = NULL WHERE batch_id = $1",
            *pending_cancel as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();
        manager.get_batch(pending_cancel).await.unwrap();

        let candidates = manager
            .list_archivable_batches(10, true, 600.0, 0.0)
            .await
            .unwrap();
        assert!(
            !candidates.contains(&in_flight),
            "in-flight-canceled row inside the grace must hold the batch back"
        );
        assert!(
            candidates.contains(&pending_cancel),
            "pending-canceled rows never delay archiving"
        );

        let candidates = manager
            .list_archivable_batches(10, true, 120.0, 0.0)
            .await
            .unwrap();
        assert!(
            candidates.contains(&in_flight),
            "once the grace passes, the batch archives normally"
        );
    }

    #[sqlx::test]
    async fn test_list_archivable_batches_membership_and_order(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let old = setup_frozen_batch(&manager, &pool, "list-old", 1).await;
        let mid = setup_frozen_batch(&manager, &pool, "list-mid", 1).await;
        let new = setup_frozen_batch(&manager, &pool, "list-new", 1).await;
        for (id, days) in [(old, 30), (mid, 20), (new, 10)] {
            sqlx::query(&format!(
                "UPDATE batches SET created_at = NOW() - INTERVAL '{days} days' WHERE id = $1"
            ))
            .bind(*id as Uuid)
            .execute(&pool)
            .await
            .unwrap();
        }
        let _active = setup_freeze_test_batch(&manager, "list-active", 1).await;
        let deleted = setup_frozen_batch(&manager, &pool, "list-deleted", 1).await;
        sqlx::query!(
            "UPDATE batches SET deleted_at = NOW() WHERE id = $1",
            *deleted as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let oldest = manager
            .list_archivable_batches(10, true, 0.0, 0.0)
            .await
            .unwrap();
        assert_eq!(oldest, vec![old, mid, new], "oldest-first backfill order");
        let newest = manager
            .list_archivable_batches(10, false, 0.0, 0.0)
            .await
            .unwrap();
        assert_eq!(newest, vec![new, mid, old], "newest-first sweeper order");
        let limited = manager
            .list_archivable_batches(1, true, 0.0, 0.0)
            .await
            .unwrap();
        assert_eq!(limited, vec![old]);
        sqlx::query(
            r#"DO $$
            DECLARE target date; part text;
            BEGIN
                FOR i IN 0..6 LOOP
                    target := date_trunc('week', now() AT TIME ZONE 'UTC')::date - (i * 7);
                    part := 'batch_requests_archive_y' || to_char(target,'IYYY') || 'w' || to_char(target,'IW');
                    IF to_regclass(part) IS NULL THEN
                        EXECUTE format(
                            'CREATE TABLE %I PARTITION OF batch_requests_archive FOR VALUES FROM (%L) TO (%L)',
                            part, target, target + 7);
                    END IF;
                END LOOP;
            END $$"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            manager.archive_batch(old).await.unwrap(),
            ArchiveOutcome::Archived { rows: 1 }
        );
        let after = manager
            .list_archivable_batches(10, true, 0.0, 0.0)
            .await
            .unwrap();
        assert_eq!(after, vec![mid, new]);
    }

    #[sqlx::test]
    async fn test_cancel_batch(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "cancel-test".to_string(),
                None,
                (0..3)
                    .map(|i| RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let status_before = manager.get_batch_status(batch.id).await.unwrap();
        assert_eq!(status_before.pending_requests, 3);
        assert_eq!(status_before.canceled_requests, 0);

        manager.cancel_batch(batch.id).await.unwrap();

        let status_after = manager.get_batch_status(batch.id).await.unwrap();
        assert_eq!(status_after.pending_requests, 0);
        assert_eq!(status_after.canceled_requests, 3);

        let requests = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests.len(), 3);
        for request in requests {
            assert!(matches!(request, AnyRequest::Pending(_)));
        }
    }

    #[sqlx::test]
    async fn test_cascade_batch_state_to_requests(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "cascade-test".to_string(),
                None,
                (0..7)
                    .map(|i| RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let requests = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests.len(), 7);
        let req_ids: Vec<RequestId> = requests.iter().map(|r| r.id()).collect();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = '{"ok":true}',
                claimed_at = NOW(),
                started_at = NOW(),
                completed_at = NOW()
            WHERE id = $1
            "#,
            *req_ids[0] as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'failed',
                error = 'test error',
                failed_at = NOW(),
                retry_attempt = 1
            WHERE id = $1
            "#,
            *req_ids[1] as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'claimed',
                daemon_id = gen_random_uuid(),
                claimed_at = NOW()
            WHERE id = $1
            "#,
            *req_ids[2] as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'processing',
                daemon_id = gen_random_uuid(),
                claimed_at = NOW(),
                started_at = NOW()
            WHERE id = $1
            "#,
            *req_ids[3] as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();


        let rows_updated = manager
            .cascade_batch_state_to_requests(batch.id, CascadeTargetState::Canceled)
            .await
            .unwrap();
        assert_eq!(rows_updated, 5);

        let requests = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests.len(), 7);

        let mut completed_count = 0;
        let mut failed_count = 0;
        let mut canceled_count = 0;
        for req in &requests {
            match req {
                AnyRequest::Completed(_) => completed_count += 1,
                AnyRequest::Failed(_) => failed_count += 1,
                AnyRequest::Canceled(_) => canceled_count += 1,
                other => panic!("Unexpected state: {:?}", other.id()),
            }
        }
        assert_eq!(completed_count, 1);
        assert_eq!(failed_count, 1);
        assert_eq!(canceled_count, 5);
    }

    #[sqlx::test]
    async fn test_cascade_batch_state_to_requests_failed(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "cascade-failed-test".to_string(),
                None,
                (0..4)
                    .map(|i| RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let requests = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests.len(), 4);
        let req_ids: Vec<RequestId> = requests.iter().map(|r| r.id()).collect();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = '{"ok":true}',
                claimed_at = NOW(),
                started_at = NOW(),
                completed_at = NOW()
            WHERE id = $1
            "#,
            *req_ids[0] as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();


        let rows_updated = manager
            .cascade_batch_state_to_requests(batch.id, CascadeTargetState::Failed)
            .await
            .unwrap();
        assert_eq!(rows_updated, 3);

        let requests = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests.len(), 4);

        let mut completed_count = 0;
        let mut failed_count = 0;
        for req in &requests {
            match req {
                AnyRequest::Completed(_) => completed_count += 1,
                AnyRequest::Failed(r) => {
                    assert_eq!(r.state.reason, FailureReason::BatchTerminated);
                    failed_count += 1;
                }
                other => panic!("Unexpected state for request {:?}", other.id()),
            }
        }
        assert_eq!(completed_count, 1);
        assert_eq!(failed_count, 3);

        let row = sqlx::query!(
            "SELECT response_size FROM requests WHERE batch_id = $1 AND state = 'failed'",
            *batch.id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let expected_size = serde_json::to_string(&FailureReason::BatchTerminated)
            .unwrap()
            .len() as i64;
        assert_eq!(row.response_size, expected_size);

        let rows_updated = manager
            .cascade_batch_state_to_requests(batch.id, CascadeTargetState::Failed)
            .await
            .unwrap();
        assert_eq!(rows_updated, 0);
    }

    #[sqlx::test]
    async fn test_cascade_is_idempotent(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "cascade-idempotent-test".to_string(),
                None,
                (0..3)
                    .map(|i| RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let rows_updated = manager
            .cascade_batch_state_to_requests(batch.id, CascadeTargetState::Canceled)
            .await
            .unwrap();
        assert_eq!(rows_updated, 3);

        let rows_updated = manager
            .cascade_batch_state_to_requests(batch.id, CascadeTargetState::Canceled)
            .await
            .unwrap();
        assert_eq!(rows_updated, 0);
    }

    #[sqlx::test]
    async fn test_delete_batch(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "delete-batch-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":1}"#.to_string(),
                        model: "test".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":2}"#.to_string(),
                        model: "test".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();
        let output_file_id = batch
            .output_file_id
            .expect("batch should have an output file");
        let error_file_id = batch
            .error_file_id
            .expect("batch should have an error file");

        let batch_before = manager.get_batch(batch.id).await;
        assert!(batch_before.is_ok());

        assert!(manager.get_file(output_file_id).await.is_ok());
        assert!(manager.get_file(error_file_id).await.is_ok());

        let requests_before = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests_before.len(), 2);

        manager.delete_batch(batch.id).await.unwrap();

        let batch_after = manager.get_batch(batch.id).await;
        assert!(batch_after.is_err());

        let requests_after = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests_after.len(), 0);

        let file_after = manager.get_file(file_id).await;
        assert!(file_after.is_ok());

        assert!(manager.get_file(output_file_id).await.is_err());
        assert!(manager.get_file(error_file_id).await.is_err());

        let delete_result = manager.delete_batch(batch.id).await;
        assert!(delete_result.is_err());
    }

    #[sqlx::test]
    async fn test_cancel_individual_requests(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "individual-cancel-test".to_string(),
                None,
                (0..5)
                    .map(|i| RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let requests = manager.get_batch_requests(batch.id).await.unwrap();
        let request_ids: Vec<_> = requests.iter().map(|r| r.id()).collect();

        let results = manager
            .cancel_requests(request_ids[0..3].to_vec())
            .await
            .unwrap();

        for result in results {
            assert!(result.is_ok());
        }

        let status = manager.get_batch_status(batch.id).await.unwrap();
        assert_eq!(status.pending_requests, 2);
        assert_eq!(status.canceled_requests, 3);

        let all_requests = manager.get_batch_requests(batch.id).await.unwrap();
        let canceled_count = all_requests
            .iter()
            .filter(|r| matches!(r, AnyRequest::Canceled(_)))
            .count();
        assert_eq!(canceled_count, 3);
    }

    #[sqlx::test]
    async fn test_list_files(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file1_id = manager
            .create_file("file1".to_string(), Some("First".to_string()), vec![])
            .await
            .unwrap();

        let file2_id = manager
            .create_file("file2".to_string(), Some("Second".to_string()), vec![])
            .await
            .unwrap();

        let file3_id = manager
            .create_file("file3".to_string(), None, vec![])
            .await
            .unwrap();

        let files = manager
            .list_files(crate::batch::FileFilter::default())
            .await
            .unwrap();

        assert!(files.len() >= 3);

        let file_ids: Vec<_> = files.iter().map(|f| f.id).collect();
        assert!(file_ids.contains(&file1_id));
        assert!(file_ids.contains(&file2_id));
        assert!(file_ids.contains(&file3_id));

        let file1 = files.iter().find(|f| f.id == file1_id).unwrap();
        assert_eq!(file1.name, "file1");
        assert_eq!(file1.description, Some("First".to_string()));

        let file3 = files.iter().find(|f| f.id == file3_id).unwrap();
        assert_eq!(file3.name, "file3");
        assert_eq!(file3.description, None);
    }

    #[sqlx::test]
    async fn test_list_file_batches(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "batch-list-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch1 = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();
        let batch2 = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();
        let batch3 = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let batches = manager.list_file_batches(file_id).await.unwrap();

        assert_eq!(batches.len(), 3);

        let batch_ids: Vec<_> = batches.iter().map(|b| b.batch_id).collect();
        assert!(batch_ids.contains(&batch1.id));
        assert!(batch_ids.contains(&batch2.id));
        assert!(batch_ids.contains(&batch3.id));

        for batch in batches {
            assert_eq!(batch.total_requests, 1);
            assert_eq!(batch.pending_requests, 1);
        }
    }

    #[sqlx::test]
    async fn test_delete_file_cascade(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "delete-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":1}"#.to_string(),
                        model: "test".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":2}"#.to_string(),
                        model: "test".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let batch_before = manager.get_batch(batch.id).await.unwrap();
        assert_eq!(batch_before.file_id, Some(file_id));
        assert!(batch_before.cancelling_at.is_none());
        assert!(batch_before.cancelled_at.is_none());
        assert_eq!(batch_before.pending_requests, 2);

        let requests_before = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests_before.len(), 2);

        manager.delete_file(file_id).await.unwrap();

        let file_result = manager.get_file(file_id).await;
        assert!(file_result.is_err());

        let batch_after = manager.get_batch(batch.id).await.unwrap();
        assert_eq!(batch_after.file_id, None);
        assert!(batch_after.cancelling_at.is_some());
        assert!(batch_after.cancelled_at.is_some());
        assert_eq!(batch_after.canceled_requests, 2);

        let requests_after = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests_after.len(), 0);
    }

    #[sqlx::test]
    async fn test_delete_file_twice_is_file_not_found(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "double-delete".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{"n":1}"#.to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        manager.delete_file(file_id).await.unwrap();

        let second = manager.delete_file(file_id).await;
        assert!(
            matches!(second, Err(FusilladeError::FileNotFound(id)) if id == file_id),
            "expected FileNotFound on the second delete, got {second:?}"
        );
    }


    #[sqlx::test]
    async fn test_unclaim_stale_claimed_requests(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        let config = crate::daemon::DaemonConfig {
            claim_timeout_ms: 1000,
            processing_timeout_ms: 60000,
            ..Default::default()
        };
        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client,
            )
            .with_config(config),
        );

        let file_id = manager
            .create_file(
                "stale-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let capacity = HashMap::from([("test".to_string(), 10)]);
        let daemon1_id = DaemonId::from(Uuid::new_v4());
        let claimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon1_id, &capacity, &HashMap::new())
                .await;
        assert_eq!(claimed.len(), 1);
        let request_id = claimed[0].data.id;

        sqlx::query!(
            "UPDATE requests SET claimed_at = NOW() - INTERVAL '3 seconds' WHERE id = $1",
            *request_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let daemon2_id = DaemonId::from(Uuid::new_v4());
        let reclaimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon2_id, &capacity, &HashMap::new())
                .await;

        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].data.id, request_id);
        assert_eq!(reclaimed[0].state.daemon_id, daemon2_id);

        let status = manager.get_batch_status(batch.id).await.unwrap();
        assert_eq!(status.in_progress_requests, 1);
    }

    #[sqlx::test]
    async fn test_unclaim_stale_processing_requests(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        let config = crate::daemon::DaemonConfig {
            claim_timeout_ms: 60000,
            processing_timeout_ms: 1000,
            ..Default::default()
        };
        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client,
            )
            .with_config(config),
        );

        let file_id = manager
            .create_file(
                "stale-processing-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let capacity = HashMap::from([("test".to_string(), 10)]);
        let daemon1_id = DaemonId::from(Uuid::new_v4());
        let claimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon1_id, &capacity, &HashMap::new())
                .await;
        assert_eq!(claimed.len(), 1);
        let request_id = claimed[0].data.id;

        sqlx::query!(
            r#"
            UPDATE requests
            SET
                state = 'processing',
                started_at = NOW() - INTERVAL '3 seconds'
            WHERE id = $1
            "#,
            *request_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let status_before = manager.get_batch_status(batch.id).await.unwrap();
        assert_eq!(status_before.in_progress_requests, 1);

        let daemon2_id = DaemonId::from(Uuid::new_v4());
        let reclaimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon2_id, &capacity, &HashMap::new())
                .await;

        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].data.id, request_id);
        assert_eq!(reclaimed[0].state.daemon_id, daemon2_id);
    }

    #[sqlx::test]
    async fn test_unclaim_requests_from_dead_daemon(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        let config = crate::daemon::DaemonConfig {
            claim_timeout_ms: 600000,
            processing_timeout_ms: 600000,
            pending_request_counts_timeout_ms: 60_000,
            stale_daemon_threshold_ms: 1000,
            ..Default::default()
        };
        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client,
            )
            .with_config(config),
        );

        let file_id = manager
            .create_file(
                "dead-daemon-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let daemon1_id = DaemonId::from(Uuid::new_v4());
        let daemon1 = DaemonRecord {
            data: DaemonData {
                id: daemon1_id,
                hostname: "test-host".to_string(),
                pid: 1234,
                version: "test".to_string(),
                config_snapshot: serde_json::json!({}),
            },
            state: Dead {
                started_at: Utc::now() - chrono::Duration::minutes(10),
                stopped_at: Utc::now(),
                final_stats: DaemonStats::default(),
            },
        };
        manager.persist_daemon(&daemon1).await.unwrap();

        let capacity = HashMap::from([("test".to_string(), 10)]);
        let claimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon1_id, &capacity, &HashMap::new())
                .await;
        assert_eq!(claimed.len(), 1);
        let request_id = claimed[0].data.id;

        sqlx::query!(
            "UPDATE requests SET state = 'processing', started_at = NOW() WHERE id = $1",
            *request_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let status = manager.get_batch_status(batch.id).await.unwrap();
        assert_eq!(status.in_progress_requests, 1);

        let daemon2_id = DaemonId::from(Uuid::new_v4());
        let reclaimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon2_id, &capacity, &HashMap::new())
                .await;

        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].data.id, request_id);
        assert_eq!(reclaimed[0].state.daemon_id, daemon2_id);
    }

    #[sqlx::test]
    async fn test_unclaim_requests_from_stale_heartbeat_daemon(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        let config = crate::daemon::DaemonConfig {
            claim_timeout_ms: 600000,
            processing_timeout_ms: 600000,
            pending_request_counts_timeout_ms: 60_000,
            stale_daemon_threshold_ms: 1000,
            ..Default::default()
        };
        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client,
            )
            .with_config(config),
        );

        let file_id = manager
            .create_file(
                "stale-heartbeat-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let daemon1_id = DaemonId::from(Uuid::new_v4());
        let daemon1 = DaemonRecord {
            data: DaemonData {
                id: daemon1_id,
                hostname: "test-host".to_string(),
                pid: 1234,
                version: "test".to_string(),
                config_snapshot: serde_json::json!({}),
            },
            state: Running {
                started_at: Utc::now() - chrono::Duration::minutes(10),
                last_heartbeat: Utc::now() - chrono::Duration::milliseconds(1500),
                stats: DaemonStats::default(),
            },
        };
        manager.persist_daemon(&daemon1).await.unwrap();

        let capacity = HashMap::from([("test".to_string(), 10)]);
        let claimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon1_id, &capacity, &HashMap::new())
                .await;
        assert_eq!(claimed.len(), 1);
        let request_id = claimed[0].data.id;

        sqlx::query!(
            "UPDATE requests SET state = 'processing', started_at = NOW() WHERE id = $1",
            *request_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let status = manager.get_batch_status(batch.id).await.unwrap();
        assert_eq!(status.in_progress_requests, 1);

        let daemon2_id = DaemonId::from(Uuid::new_v4());
        let reclaimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon2_id, &capacity, &HashMap::new())
                .await;

        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].data.id, request_id);
        assert_eq!(reclaimed[0].state.daemon_id, daemon2_id);
    }

    #[sqlx::test]
    async fn test_dont_unclaim_requests_from_healthy_daemon(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        let config = crate::daemon::DaemonConfig {
            claim_timeout_ms: 600000,
            processing_timeout_ms: 600000,
            pending_request_counts_timeout_ms: 60_000,
            stale_daemon_threshold_ms: 60000,
            ..Default::default()
        };
        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client,
            )
            .with_config(config),
        );

        let file_id = manager
            .create_file(
                "healthy-daemon-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":1}"#.to_string(),
                        model: "test".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":2}"#.to_string(),
                        model: "test".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let daemon1_id = DaemonId::from(Uuid::new_v4());
        let daemon1 = DaemonRecord {
            data: DaemonData {
                id: daemon1_id,
                hostname: "test-host".to_string(),
                pid: 1234,
                version: "test".to_string(),
                config_snapshot: serde_json::json!({}),
            },
            state: Running {
                started_at: Utc::now() - chrono::Duration::minutes(10),
                last_heartbeat: Utc::now(),
                stats: DaemonStats::default(),
            },
        };
        manager.persist_daemon(&daemon1).await.unwrap();

        let capacity = HashMap::from([("test".to_string(), 10)]);
        let claimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon1_id, &capacity, &HashMap::new())
                .await;
        assert_eq!(claimed.len(), 1);
        let request_id = claimed[0].data.id;

        sqlx::query!(
            "UPDATE requests SET state = 'processing', started_at = NOW() WHERE id = $1",
            *request_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let daemon2_id = DaemonId::from(Uuid::new_v4());
        let claimed2 =
            claim_batch_requests_for_test(&manager, 1, 1, daemon2_id, &capacity, &HashMap::new())
                .await;

        assert_eq!(claimed2.len(), 1);
        assert_ne!(claimed2[0].data.id, request_id);

        let results = manager.get_requests(vec![request_id]).await.unwrap();
        assert!(
            matches!(&results[0], Ok(crate::AnyRequest::Processing(_))),
            "Request should still be in processing state"
        );
    }

    #[sqlx::test]
    async fn test_dont_unclaim_recent_requests(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        let config = crate::daemon::DaemonConfig {
            claim_timeout_ms: 60000,
            processing_timeout_ms: 600000,
            ..Default::default()
        };
        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client,
            )
            .with_config(config),
        );

        let file_id = manager
            .create_file(
                "recent-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":1}"#.to_string(),
                        model: "test".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":2}"#.to_string(),
                        model: "test".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let capacity = HashMap::from([("test".to_string(), 10)]);
        let daemon1_id = DaemonId::from(Uuid::new_v4());
        let claimed1 =
            claim_batch_requests_for_test(&manager, 1, 1, daemon1_id, &capacity, &HashMap::new())
                .await;
        assert_eq!(claimed1.len(), 1);

        let daemon2_id = DaemonId::from(Uuid::new_v4());
        let claimed2 =
            claim_batch_requests_for_test(&manager, 1, 1, daemon2_id, &capacity, &HashMap::new())
                .await;
        assert_eq!(claimed2.len(), 1);

        assert_ne!(claimed1[0].data.id, claimed2[0].data.id);

        let results = manager
            .get_requests(vec![claimed1[0].data.id])
            .await
            .unwrap();
        if let Ok(crate::AnyRequest::Claimed(req)) = &results[0] {
            assert_eq!(req.state.daemon_id, daemon1_id);
        } else {
            panic!("Request should still be claimed by daemon1");
        }
    }

    #[sqlx::test]
    async fn test_preserve_retry_attempt_on_unclaim(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        let config = crate::daemon::DaemonConfig {
            claim_timeout_ms: 1000,
            processing_timeout_ms: 60000,
            ..Default::default()
        };
        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client,
            )
            .with_config(config),
        );

        let file_id = manager
            .create_file(
                "retry-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        sqlx::query!(
            r#"
            UPDATE requests
            SET
                retry_attempt = 2,
                state = 'claimed',
                daemon_id = $1,
                claimed_at = NOW() - INTERVAL '3 seconds'
            WHERE id IN (SELECT id FROM requests WHERE state = 'pending' LIMIT 1)
            RETURNING id
            "#,
            Uuid::new_v4()
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let capacity = HashMap::from([("test".to_string(), 10)]);
        let daemon_id = DaemonId::from(Uuid::new_v4());
        let claimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon_id, &capacity, &HashMap::new())
                .await;

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].state.retry_attempt, 2);
    }


    #[sqlx::test]
    async fn test_batch_output_and_error_streaming(pool: sqlx::PgPool) {
        use futures::StreamExt;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "streaming-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("req-1".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/chat/completions".to_string(),
                        body: r#"{"prompt":"first"}"#.to_string(),
                        model: "gpt-4".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req-2".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/chat/completions".to_string(),
                        body: r#"{"prompt":"second"}"#.to_string(),
                        model: "gpt-4".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req-3".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/chat/completions".to_string(),
                        body: r#"{"prompt":"third"}"#.to_string(),
                        model: "gpt-4".to_string(),
                    },
                ],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: Some("test-user".to_string()),
                api_key_id: None,
                total_requests: None,
            })
            .await
            .expect("Failed to create batch");

        assert!(batch.output_file_id.is_some());
        assert!(batch.error_file_id.is_some());
        let output_file_id = batch.output_file_id.unwrap();
        let error_file_id = batch.error_file_id.unwrap();

        let output_file = manager
            .get_file(output_file_id)
            .await
            .expect("Failed to get output file");
        let error_file = manager
            .get_file(error_file_id)
            .await
            .expect("Failed to get error file");

        assert_eq!(
            output_file.name,
            format!("batch-{}-output.jsonl", batch.id.0)
        );
        assert_eq!(error_file.name, format!("batch-{}-error.jsonl", batch.id.0));

        let requests = manager
            .get_batch_requests(batch.id)
            .await
            .expect("Failed to get requests");
        assert_eq!(requests.len(), 3);

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = $2,
                completed_at = NOW()
            WHERE id = $1
            "#,
            *requests[0].id() as Uuid,
            r#"{"id":"chatcmpl-123","choices":[{"message":{"content":"Response 1"}}]}"#,
        )
        .execute(&pool)
        .await
        .expect("Failed to mark request as completed");

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = $2,
                completed_at = NOW()
            WHERE id = $1
            "#,
            *requests[1].id() as Uuid,
            r#"{"id":"chatcmpl-456","choices":[{"message":{"content":"Response 2"}}]}"#,
        )
        .execute(&pool)
        .await
        .expect("Failed to mark request as completed");

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'failed',
                error = $2,
                failed_at = NOW()
            WHERE id = $1
            "#,
            *requests[2].id() as Uuid,
            "Rate limit exceeded",
        )
        .execute(&pool)
        .await
        .expect("Failed to mark request as failed");

        let output_stream = manager.get_file_content_stream(output_file_id, 0, None);
        let output_items: Vec<_> = output_stream.collect().await;

        assert_eq!(output_items.len(), 2, "Should have 2 output items");

        let mut found_custom_ids = Vec::new();
        for item_result in output_items.iter() {
            let item = item_result.as_ref().expect("Output item should be Ok");

            match item {
                FileContentItem::Output(output) => {
                    found_custom_ids.push(output.custom_id.clone());

                    assert_eq!(output.response.status_code, 200);
                    assert!(output.response.body.is_object());
                    assert!(output.error.is_none());

                    assert!(output.id.starts_with("batch_req_"));
                }
                _ => panic!("Expected FileContentItem::Output, got different type"),
            }
        }

        found_custom_ids.sort();
        assert_eq!(
            found_custom_ids,
            vec![Some("req-1".to_string()), Some("req-2".to_string())]
        );

        let error_stream = manager.get_file_content_stream(error_file_id, 0, None);
        let error_items: Vec<_> = error_stream.collect().await;

        assert_eq!(error_items.len(), 1, "Should have 1 error item");

        let error_result = &error_items[0];
        let error_item = error_result.as_ref().expect("Error item should be Ok");

        match error_item {
            FileContentItem::Error(error) => {
                assert_eq!(error.custom_id, Some("req-3".to_string()));
                assert_eq!(error.error.message, "Rate limit exceeded");
                assert!(error.response.is_none());
                assert!(error.id.starts_with("batch_req_"));
            }
            _ => panic!("Expected FileContentItem::Error, got different type"),
        }

        let input_stream = manager.get_file_content_stream(file_id, 0, None);
        let input_items: Vec<_> = input_stream.collect().await;

        assert_eq!(input_items.len(), 3, "Input file should have 3 templates");

        for item_result in input_items {
            let item = item_result.expect("Input item should be Ok");
            match item {
                FileContentItem::Template(_) => {
                }
                _ => panic!("Expected FileContentItem::Template for input file"),
            }
        }
    }


    #[sqlx::test]
    async fn test_daemon_persist_and_get(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let daemon_id = DaemonId(Uuid::new_v4());
        let daemon_data = DaemonData {
            id: daemon_id,
            hostname: "test-host".to_string(),
            pid: 12345,
            version: "1.0.0".to_string(),
            config_snapshot: serde_json::json!({"test": "config"}),
        };

        let initializing = DaemonRecord {
            data: daemon_data.clone(),
            state: Initializing {
                started_at: Utc::now(),
            },
        };

        manager.persist_daemon(&initializing).await.unwrap();

        let retrieved = manager.get_daemon(daemon_id).await.unwrap();
        match retrieved {
            AnyDaemonRecord::Initializing(d) => {
                assert_eq!(d.data.id, daemon_id);
                assert_eq!(d.data.hostname, "test-host");
            }
            _ => panic!("Expected Initializing state"),
        }

        let running = DaemonRecord {
            data: daemon_data.clone(),
            state: Running {
                started_at: Utc::now(),
                last_heartbeat: Utc::now(),
                stats: DaemonStats {
                    requests_processed: 10,
                    requests_failed: 2,
                    requests_in_flight: 3,
                },
            },
        };

        manager.persist_daemon(&running).await.unwrap();

        let retrieved = manager.get_daemon(daemon_id).await.unwrap();
        match retrieved {
            AnyDaemonRecord::Running(d) => {
                assert_eq!(d.data.id, daemon_id);
                assert_eq!(d.state.stats.requests_processed, 10);
                assert_eq!(d.state.stats.requests_failed, 2);
                assert_eq!(d.state.stats.requests_in_flight, 3);
            }
            _ => panic!("Expected Running state"),
        }

        let dead = DaemonRecord {
            data: daemon_data,
            state: Dead {
                started_at: Utc::now() - chrono::Duration::hours(1),
                stopped_at: Utc::now(),
                final_stats: DaemonStats {
                    requests_processed: 100,
                    requests_failed: 5,
                    requests_in_flight: 0,
                },
            },
        };

        manager.persist_daemon(&dead).await.unwrap();

        let retrieved = manager.get_daemon(daemon_id).await.unwrap();
        match retrieved {
            AnyDaemonRecord::Dead(d) => {
                assert_eq!(d.data.id, daemon_id);
                assert_eq!(d.state.final_stats.requests_processed, 100);
                assert_eq!(d.state.final_stats.requests_failed, 5);
            }
            _ => panic!("Expected Dead state"),
        }
    }

    #[sqlx::test]
    async fn test_daemon_list_all(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let daemon1 = DaemonRecord {
            data: DaemonData {
                id: DaemonId(Uuid::new_v4()),
                hostname: "host1".to_string(),
                pid: 1001,
                version: "1.0.0".to_string(),
                config_snapshot: serde_json::json!({}),
            },
            state: Running {
                started_at: Utc::now(),
                last_heartbeat: Utc::now(),
                stats: DaemonStats::default(),
            },
        };

        let daemon2 = DaemonRecord {
            data: DaemonData {
                id: DaemonId(Uuid::new_v4()),
                hostname: "host2".to_string(),
                pid: 1002,
                version: "1.0.0".to_string(),
                config_snapshot: serde_json::json!({}),
            },
            state: Running {
                started_at: Utc::now(),
                last_heartbeat: Utc::now(),
                stats: DaemonStats::default(),
            },
        };

        let daemon3 = DaemonRecord {
            data: DaemonData {
                id: DaemonId(Uuid::new_v4()),
                hostname: "host3".to_string(),
                pid: 1003,
                version: "1.0.0".to_string(),
                config_snapshot: serde_json::json!({}),
            },
            state: Dead {
                started_at: Utc::now() - chrono::Duration::hours(1),
                stopped_at: Utc::now(),
                final_stats: DaemonStats::default(),
            },
        };

        manager.persist_daemon(&daemon1).await.unwrap();
        manager.persist_daemon(&daemon2).await.unwrap();
        manager.persist_daemon(&daemon3).await.unwrap();

        let all = manager.list_daemons(None).await.unwrap();
        assert_eq!(all.len(), 3);

        let running = manager
            .list_daemons(Some(DaemonStatus::Running))
            .await
            .unwrap();
        assert_eq!(running.len(), 2);

        let dead = manager
            .list_daemons(Some(DaemonStatus::Dead))
            .await
            .unwrap();
        assert_eq!(dead.len(), 1);
    }

    #[sqlx::test]
    async fn test_daemon_heartbeat_updates(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let daemon_id = DaemonId(Uuid::new_v4());
        let daemon_data = DaemonData {
            id: daemon_id,
            hostname: "test-host".to_string(),
            pid: 12345,
            version: "1.0.0".to_string(),
            config_snapshot: serde_json::json!({}),
        };

        let running = DaemonRecord {
            data: daemon_data,
            state: Running {
                started_at: Utc::now(),
                last_heartbeat: Utc::now(),
                stats: DaemonStats {
                    requests_processed: 0,
                    requests_failed: 0,
                    requests_in_flight: 0,
                },
            },
        };

        manager.persist_daemon(&running).await.unwrap();

        for i in 1..=3 {
            let updated = DaemonRecord {
                data: running.data.clone(),
                state: Running {
                    started_at: running.state.started_at,
                    last_heartbeat: Utc::now(),
                    stats: DaemonStats {
                        requests_processed: i * 10,
                        requests_failed: i,
                        requests_in_flight: i as usize,
                    },
                },
            };
            manager.persist_daemon(&updated).await.unwrap();
        }

        let retrieved = manager.get_daemon(daemon_id).await.unwrap();
        match retrieved {
            AnyDaemonRecord::Running(d) => {
                assert_eq!(d.state.stats.requests_processed, 30);
                assert_eq!(d.state.stats.requests_failed, 3);
                assert_eq!(d.state.stats.requests_in_flight, 3);
            }
            _ => panic!("Expected Running state"),
        }
    }


    #[sqlx::test]
    async fn test_create_file_stream_with_metadata_and_templates(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let items = vec![
            FileStreamItem::Metadata(FileMetadata {
                filename: Some("streamed-file".to_string()),
                description: Some("A file created via streaming".to_string()),
                purpose: None,
                expires_after_anchor: None,
                expires_after_seconds: None,
                size_bytes: None,
                uploaded_by: Some("test-user".to_string()),
                api_key_id: None,
                ..Default::default()
            }),
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some("stream-1".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: r#"{"prompt":"first"}"#.to_string(),
                model: "gpt-4".to_string(),
            }),
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some("stream-2".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: r#"{"prompt":"second"}"#.to_string(),
                model: "gpt-3.5".to_string(),
            }),
        ];

        let stream = stream::iter(items);

        let file_id = expect_stream_success(
            manager
                .create_file_stream(stream)
                .await
                .expect("Failed to create file from stream"),
        );

        let file = manager.get_file(file_id).await.expect("Failed to get file");
        assert_eq!(file.name, "streamed-file");
        assert_eq!(
            file.description,
            Some("A file created via streaming".to_string())
        );

        let content = manager
            .get_file_content(file_id)
            .await
            .expect("Failed to get content");
        assert_eq!(content.len(), 2);

        match &content[0] {
            FileContentItem::Template(t) => {
                assert_eq!(t.custom_id, Some("stream-1".to_string()));
                assert_eq!(t.model, "gpt-4");
            }
            _ => panic!("Expected template"),
        }
        match &content[1] {
            FileContentItem::Template(t) => {
                assert_eq!(t.custom_id, Some("stream-2".to_string()));
                assert_eq!(t.model, "gpt-3.5");
            }
            _ => panic!("Expected template"),
        }
    }

    #[sqlx::test]
    async fn test_create_file_stream_templates_before_metadata(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let items = vec![
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: r#"{"n":1}"#.to_string(),
                model: "test".to_string(),
            }),
            FileStreamItem::Metadata(FileMetadata {
                filename: Some("late-metadata".to_string()),
                description: Some("Metadata came late".to_string()),
                purpose: None,
                expires_after_anchor: None,
                expires_after_seconds: None,
                size_bytes: None,
                uploaded_by: Some("test-user".to_string()),
                api_key_id: None,
                ..Default::default()
            }),
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: r#"{"n":2}"#.to_string(),
                model: "test".to_string(),
            }),
        ];

        let stream = stream::iter(items);
        let file_id = expect_stream_success(
            manager
                .create_file_stream(stream)
                .await
                .expect("Failed to create file from stream"),
        );

        let file = manager.get_file(file_id).await.unwrap();
        assert_eq!(file.name, "late-metadata");
        assert_eq!(file.description, Some("Metadata came late".to_string()));

        let content = manager.get_file_content(file_id).await.unwrap();
        assert_eq!(content.len(), 2);
    }

    #[sqlx::test]
    async fn test_create_file_stream_abort_handling(pool: sqlx::PgPool) {
        use crate::batch::FileStreamItem;
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let items = vec![
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: r#"{"n":1}"#.to_string(),
                model: "test".to_string(),
            }),
            FileStreamItem::Abort,
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: r#"{"n":2}"#.to_string(),
                model: "test".to_string(),
            }),
        ];

        let stream = stream::iter(items);
        let result = manager.create_file_stream(stream).await;

        match result {
            Ok(FileStreamResult::Aborted) => {}
            _ => panic!("Expected Aborted"),
        }

        let files = sqlx::query(r#"SELECT COUNT(*) as count FROM files"#)
            .fetch_one(&pool)
            .await
            .unwrap();
        let count: i64 = files.get("count");
        assert_eq!(count, 0, "Aborted stream should roll back inserts");
    }

    #[sqlx::test]
    #[allow(deprecated)]
    async fn test_create_file_stream_deprecated_error_handling(pool: sqlx::PgPool) {
        use crate::batch::FileStreamItem;
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let items = vec![
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: r#"{"n":1}"#.to_string(),
                model: "test".to_string(),
            }),
            FileStreamItem::Error("Invalid JSON on line 2".to_string()),
        ];

        let result = manager.create_file_stream(stream::iter(items)).await;

        match result {
            Err(FusilladeError::ValidationError(msg)) => {
                assert_eq!(msg, "Invalid JSON on line 2");
            }
            _ => panic!("Expected ValidationError"),
        }
    }


    #[sqlx::test]
    async fn test_get_batch(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "batch-retrieval-test".to_string(),
                Some("Test file for batch retrieval".to_string()),
                vec![RequestTemplateInput {
                    custom_id: Some("req-1".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/completions".to_string(),
                    body: r#"{"prompt":"test"}"#.to_string(),
                    model: "gpt-4".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch_input = crate::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: Some(serde_json::json!({"project": "test"})),
            created_by: Some("test-user".to_string()),
            api_key_id: None,
            total_requests: None,
        };

        let created_batch = manager.create_batch(batch_input).await.unwrap();

        let retrieved_batch = manager
            .get_batch(created_batch.id)
            .await
            .expect("Failed to get batch");

        assert_eq!(retrieved_batch.id, created_batch.id);
        assert_eq!(retrieved_batch.file_id, Some(file_id));
        assert_eq!(retrieved_batch.endpoint, "/v1/chat/completions");
        assert_eq!(retrieved_batch.completion_window.as_deref(), Some("24h"));
        assert_eq!(
            retrieved_batch.metadata,
            Some(serde_json::json!({"project": "test"}))
        );
        assert_eq!(retrieved_batch.created_by, "test-user");
        assert!(retrieved_batch.output_file_id.is_some());
        assert!(retrieved_batch.error_file_id.is_some());
        assert_eq!(retrieved_batch.total_requests, 1);
        assert_eq!(retrieved_batch.pending_requests, 1);
        assert_eq!(retrieved_batch.completed_requests, 0);
    }

    #[sqlx::test]
    async fn test_get_batch_not_found(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let fake_batch_id = BatchId(Uuid::new_v4());
        let result = manager.get_batch(fake_batch_id).await;

        assert!(result.is_err());
        match result {
            Err(FusilladeError::Other(e)) => {
                assert!(e.to_string().contains("Batch not found"));
            }
            _ => panic!("Expected Other error with 'Batch not found' message"),
        }
    }

    #[sqlx::test]
    async fn test_get_batch_with_progress(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "progress-test".to_string(),
                None,
                (0..5)
                    .map(|i| RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let capacity = HashMap::from([("test".to_string(), 10)]);
        let daemon_id = DaemonId::from(Uuid::new_v4());
        let claimed =
            claim_batch_requests_for_test(&manager, 2, 1, daemon_id, &capacity, &HashMap::new())
                .await;

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = '{"result":"ok"}',
                completed_at = NOW()
            WHERE id = $1
            "#,
            *claimed[0].data.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let retrieved = manager.get_batch(batch.id).await.unwrap();
        assert_eq!(retrieved.total_requests, 5);
        assert_eq!(retrieved.pending_requests, 3);
        assert_eq!(retrieved.in_progress_requests, 1);
        assert_eq!(retrieved.completed_requests, 1);
        assert_eq!(retrieved.failed_requests, 0);
        assert_eq!(retrieved.canceled_requests, 0);
    }

    #[sqlx::test]
    async fn test_get_batch_lazy_finalization(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "lazy-finalization-test".to_string(),
                None,
                (0..3)
                    .map(|i| RequestTemplateInput {
                        custom_id: Some(format!("req-{}", i)),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = '{"result":"ok"}',
                completed_at = NOW()
            WHERE batch_id = $1
            "#,
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let retrieved = manager.get_batch(batch.id).await.unwrap();

        assert_eq!(retrieved.total_requests, 3);
        assert_eq!(retrieved.completed_requests, 3);
        assert_eq!(retrieved.pending_requests, 0);
        assert_eq!(retrieved.failed_requests, 0);

        assert!(
            retrieved.finalizing_at.is_some(),
            "finalizing_at should be set by lazy finalization"
        );
        assert!(
            retrieved.completed_at.is_some(),
            "completed_at should be set by lazy finalization"
        );
        assert!(
            retrieved.failed_at.is_none(),
            "failed_at should be None for completed batch"
        );

        let retrieved_again = manager.get_batch(batch.id).await.unwrap();

        let truncate_nanos = |ts: Option<chrono::DateTime<chrono::Utc>>| {
            ts.map(|t| t.with_nanosecond(t.nanosecond() / 1000 * 1000).unwrap())
        };

        assert_eq!(
            truncate_nanos(retrieved.finalizing_at),
            truncate_nanos(retrieved_again.finalizing_at)
        );
        assert_eq!(
            truncate_nanos(retrieved.completed_at),
            truncate_nanos(retrieved_again.completed_at)
        );
    }

    #[sqlx::test]
    async fn test_get_requests_various_states(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "get-requests-test".to_string(),
                None,
                (0..5)
                    .map(|i| RequestTemplateInput {
                        custom_id: Some(format!("req-{}", i)),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: format!(r#"{{"n":{}}}"#, i),
                        model: "test".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let all_requests = manager.get_batch_requests(batch.id).await.unwrap();
        let request_ids: Vec<_> = all_requests.iter().map(|r| r.id()).collect();

        let capacity = HashMap::from([("test".to_string(), 10)]);
        let daemon_id = DaemonId::from(Uuid::new_v4());
        let claimed =
            claim_batch_requests_for_test(&manager, 2, 1, daemon_id, &capacity, &HashMap::new())
                .await;
        let claimed_ids: Vec<_> = claimed.iter().map(|r| r.data.id).collect();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                started_at = NOW() - INTERVAL '1 minute',
                response_status = 200,
                response_body = '{"done":true}',
                completed_at = NOW()
            WHERE id = $1
            "#,
            *claimed_ids[0] as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let pending_id = request_ids
            .iter()
            .find(|id| !claimed_ids.contains(id))
            .unwrap();
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'failed',
                error = 'Rate limit exceeded',
                failed_at = NOW()
            WHERE id = $1
            "#,
            **pending_id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let results = manager.get_requests(request_ids.clone()).await.unwrap();
        assert_eq!(results.len(), 5);

        let states: Vec<_> = results
            .iter()
            .map(|r| match r {
                Ok(AnyRequest::Pending(_)) => "pending",
                Ok(AnyRequest::Claimed(_)) => "claimed",
                Ok(AnyRequest::Processing(_)) => "processing",
                Ok(AnyRequest::Completed(_)) => "completed",
                Ok(AnyRequest::Failed(_)) => "failed",
                Ok(AnyRequest::Canceled(_)) => "canceled",
                Err(_) => "error",
            })
            .collect();

        assert_eq!(states.iter().filter(|&&s| s == "completed").count(), 1);
        assert_eq!(states.iter().filter(|&&s| s == "failed").count(), 1);
        assert_eq!(states.iter().filter(|&&s| s == "claimed").count(), 1);
        assert_eq!(states.iter().filter(|&&s| s == "pending").count(), 2);
    }

    #[sqlx::test]
    async fn test_get_requests_preserves_custom_ids(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "custom-id-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("my-custom-id-1".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"test":1}"#.to_string(),
                        model: "test".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("my-custom-id-2".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"test":2}"#.to_string(),
                        model: "test".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let all_requests = manager.get_batch_requests(batch.id).await.unwrap();
        let request_ids: Vec<_> = all_requests.iter().map(|r| r.id()).collect();

        let results = manager.get_requests(request_ids).await.unwrap();
        assert_eq!(results.len(), 2);

        for result in results {
            let request = result.expect("Request should be Ok");
            let custom_id = match &request {
                AnyRequest::Pending(r) => &r.data.custom_id,
                _ => panic!("Expected Pending"),
            };

            assert!(
                custom_id == &Some("my-custom-id-1".to_string())
                    || custom_id == &Some("my-custom-id-2".to_string())
            );
        }
    }

    #[sqlx::test]
    async fn test_get_requests_with_nonexistent_ids(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "mixed-ids-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{}"#.to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let all_requests = manager.get_batch_requests(batch.id).await.unwrap();
        let real_id = all_requests[0].id();

        let mixed_ids = vec![
            real_id,
            RequestId(Uuid::new_v4()),
            RequestId(Uuid::new_v4()),
        ];

        let results = manager.get_requests(mixed_ids).await.unwrap();

        assert_eq!(results.len(), 3);
        assert!(results[0].is_ok());
        assert!(results[1].is_err());
        assert!(results[2].is_err());
    }


    #[sqlx::test]
    async fn test_per_daemon_limit_allows_independent_claiming(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        let config = crate::daemon::DaemonConfig::default();

        let manager = Arc::new(
            PostgresRequestManager::with_client(
                TestDbPools::new(pool.clone()).await.unwrap(),
                http_client,
            )
            .with_config(config),
        );

        let mut templates = Vec::new();
        for model in &["model-a", "model-b"] {
            for n in 1..=10 {
                templates.push(RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: format!(r#"{{"model":"{}","n":{}}}"#, model, n),
                    model: model.to_string(),
                });
            }
        }

        let file_id = manager
            .create_file("multi-daemon-test".to_string(), None, templates)
            .await
            .unwrap();

        let _batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let daemon1_id = DaemonId::from(Uuid::new_v4());
        let daemon2_id = DaemonId::from(Uuid::new_v4());
        let daemon3_id = DaemonId::from(Uuid::new_v4());

        let full_capacity: std::collections::HashMap<String, usize> =
            [("model-a".to_string(), 3), ("model-b".to_string(), 3)].into();

        let claimed1 = claim_batch_requests_for_test(
            &manager,
            10,
            1,
            daemon1_id,
            &full_capacity,
            &HashMap::new(),
        )
        .await;
        let claimed2 = claim_batch_requests_for_test(
            &manager,
            10,
            1,
            daemon2_id,
            &full_capacity,
            &HashMap::new(),
        )
        .await;
        let claimed3 = claim_batch_requests_for_test(
            &manager,
            10,
            1,
            daemon3_id,
            &full_capacity,
            &HashMap::new(),
        )
        .await;

        let mut per_daemon_model_counts: Vec<std::collections::HashMap<String, i32>> = Vec::new();
        for claimed in [&claimed1, &claimed2, &claimed3] {
            let mut counts = std::collections::HashMap::new();
            for request in claimed {
                *counts.entry(request.data.model.clone()).or_insert(0) += 1;
            }
            per_daemon_model_counts.push(counts);
        }

        for (i, counts) in per_daemon_model_counts.iter().enumerate() {
            for (model, count) in counts {
                assert!(
                    *count <= 3,
                    "Daemon {} claimed {} requests for {}, exceeding per-daemon limit of 3",
                    i + 1,
                    count,
                    model,
                );
            }
        }

        let total = claimed1.len() + claimed2.len() + claimed3.len();
        assert!(
            total <= 18,
            "Total claimed should not exceed 18 (3 per model × 2 models × 3 daemons), got {}",
            total,
        );
    }


    #[sqlx::test]
    async fn test_auto_generated_filename_then_real_filename_differs(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let items = vec![
            FileStreamItem::Metadata(FileMetadata {
                uploaded_by: Some("user1".to_string()),
                ..Default::default()
            }),
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some("test-1".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: "{}".to_string(),
                model: "test".to_string(),
            }),
            FileStreamItem::Metadata(FileMetadata {
                filename: Some("updated-filename.jsonl".to_string()),
                uploaded_by: Some("user1".to_string()),
                ..Default::default()
            }),
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some("test-2".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: "{}".to_string(),
                model: "test".to_string(),
            }),
        ];

        let file_id = expect_stream_success(
            manager
                .create_file_stream(stream::iter(items))
                .await
                .expect("Should create file successfully"),
        );

        let file = manager.get_file(file_id).await.unwrap();
        assert_eq!(file.name, "updated-filename.jsonl");
        assert_eq!(file.uploaded_by, Some("user1".to_string()));

        let content = manager.get_file_content(file_id).await.unwrap();
        assert_eq!(content.len(), 2);
    }

    #[sqlx::test]
    async fn test_multiple_metadata_updates_last_wins(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let items = vec![
            FileStreamItem::Metadata(FileMetadata {
                filename: Some("first-name.jsonl".to_string()),
                description: Some("First description".to_string()),
                uploaded_by: Some("user1".to_string()),
                ..Default::default()
            }),
            FileStreamItem::Metadata(FileMetadata {
                filename: Some("second-name.jsonl".to_string()),
                description: Some("Second description".to_string()),
                uploaded_by: Some("user1".to_string()),
                ..Default::default()
            }),
            FileStreamItem::Metadata(FileMetadata {
                filename: Some("final-name.jsonl".to_string()),
                description: Some("Final description".to_string()),
                uploaded_by: Some("user1".to_string()),
                ..Default::default()
            }),
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: "{}".to_string(),
                model: "test".to_string(),
            }),
        ];

        let file_id = expect_stream_success(
            manager
                .create_file_stream(stream::iter(items))
                .await
                .expect("Should create file"),
        );

        let file = manager.get_file(file_id).await.unwrap();
        assert_eq!(file.name, "final-name.jsonl");
        assert_eq!(file.description, Some("Final description".to_string()));
    }

    #[sqlx::test]
    async fn test_empty_file_no_templates_but_with_filename(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let items = vec![FileStreamItem::Metadata(FileMetadata {
            filename: Some("empty-file.jsonl".to_string()),
            description: Some("A file with no templates".to_string()),
            uploaded_by: Some("user1".to_string()),
            ..Default::default()
        })];

        let file_id = expect_stream_success(
            manager
                .create_file_stream(stream::iter(items))
                .await
                .expect("Should create empty file"),
        );

        let file = manager.get_file(file_id).await.unwrap();
        assert_eq!(file.name, "empty-file.jsonl");
        assert_eq!(
            file.description,
            Some("A file with no templates".to_string())
        );

        let content = manager.get_file_content(file_id).await.unwrap();
        assert_eq!(content.len(), 0);
    }

    #[cfg(any())]
    async fn wait_for<F, Fut>(mut check: F, timeout: std::time::Duration) -> bool
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if check().await {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        false
    }

    #[cfg(any())]
    #[sqlx::test]
    async fn test_batch_cancellation_with_stream(pool: sqlx::PgPool) {
        use crate::request::HttpResponse;
        use std::time::Duration;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = Arc::new(PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        ));

        let file_id = manager
            .create_file(
                "test_cancellation".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("req1".to_string()),
                        endpoint: "http://example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"test": 1}"#.to_string(),
                        model: "model-a".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req2".to_string()),
                        endpoint: "http://example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"test": 2}"#.to_string(),
                        model: "model-a".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                created_by: Some("test-user".to_string()),
                completion_window: "24h".to_string(),
                endpoint: "/v1/chat/completions".to_string(),
                metadata: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();
        mark_models_live_for_test(manager.as_ref(), ["model-a"]).await;

        http_client.clear_calls();
        let trigger1 = http_client.add_response_with_trigger(
            "POST /test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: "ok".to_string(),
            }),
        );
        let _trigger2 = http_client.add_response_with_trigger(
            "POST /test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: "ok".to_string(),
            }),
        );

        let shutdown_token = tokio_util::sync::CancellationToken::new();
        let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
        model_concurrency_limits.insert("model-a".to_string(), 5);
        let config = crate::daemon::DaemonConfig {
            claim_batch_size: 10,
            model_concurrency_limits,
            claim_interval_ms: 10,
            max_retries: Some(10_000),
            stop_before_deadline_ms: Some(900_000),
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 1000,
            should_retry: Arc::new(|_| false),
            claim_timeout_ms: 5000,
            processing_timeout_ms: 10000,
            ..Default::default()
        };

        let daemon = Arc::new(crate::daemon::Daemon::new(
            manager.clone(),
            http_client.clone(),
            config,
            shutdown_token.clone(),
        ));

        let daemon_handle = tokio::spawn({
            let daemon = daemon.clone();
            async move { daemon.run().await }
        });

        let requests = manager.get_batch_requests(batch.id).await.unwrap();
        assert_eq!(requests.len(), 2);

        let manager_clone = manager.clone();
        let batch_id = batch.id;
        let reached_processing = wait_for(
            || async {
                if let Ok(reqs) = manager_clone.get_batch_requests(batch_id).await {
                    reqs.iter().all(|r| matches!(r, AnyRequest::Processing(_)))
                } else {
                    false
                }
            },
            Duration::from_secs(3),
        )
        .await;
        assert!(
            reached_processing,
            "Both requests should reach processing state"
        );

        manager.cancel_batch(batch.id).await.unwrap();

        let manager_clone = manager.clone();
        let batch_shows_canceled = wait_for(
            || async {
                if let Ok(status) = manager_clone.get_batch_status(batch_id).await {
                    return status.canceled_requests == 2 && status.in_progress_requests == 0;
                }
                false
            },
            Duration::from_secs(2),
        )
        .await;
        assert!(
            batch_shows_canceled,
            "Batch should show 2 canceled requests and 0 in_progress"
        );

        shutdown_token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), daemon_handle).await;

        drop(trigger1);
    }


    #[sqlx::test]
    async fn test_virtual_files_lazy_finalized_via_get_file(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "lazy-finalize-get-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("req-1".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req-2".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req-3".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/test".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let output_file_id = batch.output_file_id.unwrap();
        let error_file_id = batch.error_file_id.unwrap();

        let requests = manager.get_batch_requests(batch.id).await.unwrap();
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = $2,
                response_size = $3,
                completed_at = NOW()
            WHERE id = $1
            "#,
            *requests[0].id() as Uuid,
            r#"{"result":"success"}"#,
            19i64,
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'failed',
                error = $2,
                response_size = $3,
                failed_at = NOW()
            WHERE id = $1
            "#,
            *requests[1].id() as Uuid,
            r#"{"code":"rate_limit","message":"Too many requests"}"#,
            52i64,
        )
        .execute(&pool)
        .await
        .unwrap();

        let output_file = manager.get_file(output_file_id).await.unwrap();
        assert!(
            !output_file.size_finalized,
            "Output file should not be finalized (batch incomplete)"
        );
        assert!(
            output_file.size_bytes > 0,
            "Output file should have estimated size > 0"
        );

        let error_file = manager.get_file(error_file_id).await.unwrap();
        assert!(
            !error_file.size_finalized,
            "Error file should not be finalized (batch incomplete)"
        );
        assert!(
            error_file.size_bytes > 0,
            "Error file should have estimated size > 0"
        );

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = '{"done":true}',
                response_size = 14,
                completed_at = NOW()
            WHERE id = $1
            "#,
            *requests[2].id() as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let output_file_after = manager.get_file(output_file_id).await.unwrap();
        assert!(
            output_file_after.size_finalized,
            "Output file should be finalized after batch complete"
        );

        let error_file_after = manager.get_file(error_file_id).await.unwrap();
        assert!(
            error_file_after.size_finalized,
            "Error file should be finalized after batch complete"
        );
    }

    #[sqlx::test]
    async fn test_virtual_files_lazy_finalized_via_list_files(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "lazy-finalize-list-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/test".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: Some("user1".to_string()),
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let output_file_id = batch.output_file_id.unwrap();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = '{"ok":true}',
                response_size = 12,
                completed_at = NOW()
            WHERE batch_id = $1
            "#,
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let files = manager
            .list_files(crate::batch::FileFilter {
                purpose: Some(crate::batch::Purpose::BatchOutput.to_string()),
                uploaded_by: Some("user1".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let output_file = files.iter().find(|f| f.id == output_file_id).unwrap();
        assert!(output_file.size_bytes > 0, "Should have calculated size");

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let db_file = sqlx::query!(
            "SELECT size_finalized FROM files WHERE id = $1",
            *output_file_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(db_file.size_finalized);
    }

    #[sqlx::test]
    async fn test_list_files_respects_pagination_only_updates_current_page(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let mut output_file_ids = Vec::new();

        for i in 0..3 {
            let file_id = manager
                .create_file(
                    format!("batch-{}", i),
                    None,
                    vec![RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                    }],
                )
                .await
                .unwrap();

            let batch = manager
                .create_batch(BatchInput {
                    file_id,
                    endpoint: "/v1/test".to_string(),
                    completion_window: "24h".to_string(),
                    metadata: None,
                    created_by: Some("user1".to_string()),
                    api_key_id: None,
                    total_requests: None,
                })
                .await
                .unwrap();

            output_file_ids.push(batch.output_file_id.unwrap());

            sqlx::query!(
                r#"
                UPDATE requests
                SET state = 'completed',
                    response_status = 200,
                    response_body = '{"done":true}',
                    response_size = 14,
                    completed_at = NOW()
                WHERE batch_id = $1
                "#,
                *batch.id as Uuid,
            )
            .execute(&pool)
            .await
            .unwrap();
        }

        let page1 = manager
            .list_files(crate::batch::FileFilter {
                purpose: Some(crate::batch::Purpose::BatchOutput.to_string()),
                uploaded_by: Some("user1".to_string()),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(page1.len(), 2, "First page should have 2 files");

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let finalized_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM files
            WHERE id = ANY($1) AND size_finalized = TRUE
            "#,
            &output_file_ids
                .iter()
                .map(|id| **id as Uuid)
                .collect::<Vec<_>>(),
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(finalized_count, 2, "Only page 1 files should be finalized");

        let page2 = manager
            .list_files(crate::batch::FileFilter {
                purpose: Some(crate::batch::Purpose::BatchOutput.to_string()),
                uploaded_by: Some("user1".to_string()),
                after: Some(page1.last().unwrap().id),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(page2.len(), 1, "Second page should have 1 file");

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let all_finalized = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM files
            WHERE id = ANY($1) AND size_finalized = TRUE
            "#,
            &output_file_ids
                .iter()
                .map(|id| **id as Uuid)
                .collect::<Vec<_>>(),
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(all_finalized, 3, "All files should now be finalized");
    }

    #[sqlx::test]
    async fn test_incomplete_batch_gives_estimate_not_finalized(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "incomplete-batch-test".to_string(),
                None,
                (0..3)
                    .map(|_| RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/test".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let output_file_id = batch.output_file_id.unwrap();
        let error_file_id = batch.error_file_id.unwrap();

        let requests = manager.get_batch_requests(batch.id).await.unwrap();
        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = '{"partial":true}',
                response_size = 17,
                completed_at = NOW()
            WHERE id = $1
            "#,
            *requests[0].id() as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'failed',
                error = '{"error":"test"}',
                response_size = 16,
                failed_at = NOW()
            WHERE id = $1
            "#,
            *requests[1].id() as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        for _ in 0..3 {
            let output_file = manager.get_file(output_file_id).await.unwrap();
            assert!(
                !output_file.size_finalized,
                "Output file should NOT be finalized (batch incomplete)"
            );
            assert!(output_file.size_bytes > 0, "Should have non-zero estimate");

            let error_file = manager.get_file(error_file_id).await.unwrap();
            assert!(
                !error_file.size_finalized,
                "Error file should NOT be finalized (batch incomplete)"
            );
            assert!(error_file.size_bytes > 0, "Should have non-zero estimate");
        }

        let output_db = sqlx::query!(
            "SELECT size_finalized FROM files WHERE id = $1",
            *output_file_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(!output_db.size_finalized);

        let error_db = sqlx::query!(
            "SELECT size_finalized FROM files WHERE id = $1",
            *error_file_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(!error_db.size_finalized);
    }

    #[sqlx::test]
    async fn test_finalized_file_uses_cached_value_no_recomputation(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "cached-value-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/test".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let output_file_id = batch.output_file_id.unwrap();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'completed',
                response_status = 200,
                response_body = '{"cached":true}',
                response_size = 16,
                completed_at = NOW()
            WHERE batch_id = $1
            "#,
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let file1 = manager.get_file(output_file_id).await.unwrap();
        assert!(file1.size_finalized);
        let finalized_size = file1.size_bytes;

        sqlx::query!(
            r#"
            UPDATE requests
            SET response_size = 999999
            WHERE batch_id = $1
            "#,
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let file2 = manager.get_file(output_file_id).await.unwrap();
        assert!(file2.size_finalized);
        assert_eq!(
            file2.size_bytes, finalized_size,
            "Should use cached finalized value, not recalculate"
        );

        let files = manager
            .list_files(crate::batch::FileFilter {
                purpose: Some(crate::batch::Purpose::BatchOutput.to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let listed_file = files.iter().find(|f| f.id == output_file_id).unwrap();
        assert_eq!(
            listed_file.size_bytes, finalized_size,
            "List should use cached value too"
        );
    }

    #[sqlx::test]
    async fn test_normal_files_finalized_immediately_no_calculation(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "normal-file-test".to_string(),
                Some("A normal input file".to_string()),
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let file = manager.get_file(file_id).await.unwrap();

        assert!(
            file.size_finalized,
            "Normal input files should be finalized immediately"
        );
        assert_eq!(file.size_bytes, 0, "Input files have size 0 by default");

        let db_file = sqlx::query!(
            "SELECT size_finalized, purpose FROM files WHERE id = $1",
            *file_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(db_file.size_finalized, "Should be finalized in DB");
        assert!(
            db_file.purpose.is_none() || db_file.purpose.as_deref() == Some("batch"),
            "Should not be a virtual output/error file"
        );
    }


    #[sqlx::test]
    async fn test_sla_based_claim_priority(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );


        let file3 = manager
            .create_file(
                "no-sla-batch".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("no-sla-1".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{"no_sla":true}"#.to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch3 = manager
            .create_batch(crate::batch::BatchInput {
                file_id: file3,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();


        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let file2 = manager
            .create_file(
                "medium-batch".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("medium-1".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{"medium":true}"#.to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch2 = manager
            .create_batch(crate::batch::BatchInput {
                file_id: file2,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        sqlx::query!(
            "UPDATE batches SET expires_at = NOW() + INTERVAL '2 hours' WHERE id = $1",
            *batch2.id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let file1 = manager
            .create_file(
                "urgent-batch".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("urgent-1".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{"urgent":true}"#.to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch1 = manager
            .create_batch(crate::batch::BatchInput {
                file_id: file1,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        sqlx::query!(
            "UPDATE batches SET expires_at = NOW() + INTERVAL '30 minutes' WHERE id = $1",
            *batch1.id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("test".to_string(), 10)]);

        let claimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon_id, &capacity, &HashMap::new())
                .await;

        assert_eq!(claimed.len(), 1);
        assert_eq!(
            claimed[0].data.batch_id,
            Some(batch1.id),
            "First claim should be from most urgent batch (30 min expiration)"
        );
        assert_eq!(claimed[0].data.custom_id, Some("urgent-1".to_string()));

        let claimed2 =
            claim_batch_requests_for_test(&manager, 1, 1, daemon_id, &capacity, &HashMap::new())
                .await;

        assert_eq!(claimed2.len(), 1);
        assert_eq!(
            claimed2[0].data.batch_id,
            Some(batch2.id),
            "Second claim should be from medium priority batch (2 hour expiration)"
        );
        assert_eq!(claimed2[0].data.custom_id, Some("medium-1".to_string()));

        let claimed3 =
            claim_batch_requests_for_test(&manager, 1, 1, daemon_id, &capacity, &HashMap::new())
                .await;

        assert_eq!(claimed3.len(), 1);
        assert_eq!(
            claimed3[0].data.batch_id,
            Some(batch3.id),
            "Third claim should be from no-SLA batch (NULL expiration)"
        );
        assert_eq!(claimed3[0].data.custom_id, Some("no-sla-1".to_string()));
    }

    #[sqlx::test]
    async fn test_claim_drains_earliest_batch_first(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let mut filler_ids = Vec::new();
        for i in 0..9 {
            let file = manager
                .create_file(
                    format!("filler-{i}"),
                    None,
                    (0..3)
                        .map(|j| RequestTemplateInput {
                            custom_id: Some(format!("filler-{i}-{j}")),
                            endpoint: "https://api.example.com".to_string(),
                            method: "POST".to_string(),
                            path: "/test".to_string(),
                            body: "{}".to_string(),
                            model: "test-fifo".to_string(),
                        })
                        .collect(),
                )
                .await
                .unwrap();

            let batch = manager
                .create_batch(crate::batch::BatchInput {
                    file_id: file,
                    endpoint: "/v1/chat/completions".to_string(),
                    completion_window: "24h".to_string(),
                    metadata: None,
                    created_by: None,
                    api_key_id: None,
                    total_requests: None,
                })
                .await
                .unwrap();

            sqlx::query!(
                "UPDATE batches SET expires_at = NOW() + INTERVAL '6 hours' WHERE id = $1",
                *batch.id as Uuid
            )
            .execute(&pool)
            .await
            .unwrap();

            filler_ids.push(batch.id);
        }

        let urgent_file = manager
            .create_file(
                "urgent".to_string(),
                None,
                (0..3)
                    .map(|j| RequestTemplateInput {
                        custom_id: Some(format!("urgent-{j}")),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test-fifo".to_string(),
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let urgent_batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id: urgent_file,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        sqlx::query!(
            "UPDATE batches SET expires_at = NOW() + INTERVAL '30 minutes' WHERE id = $1",
            *urgent_batch.id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("test-fifo".to_string(), 3)]);

        let claimed =
            claim_batch_requests_for_test(&manager, 3, 1, daemon_id, &capacity, &HashMap::new())
                .await;

        assert_eq!(claimed.len(), 3);
        for req in &claimed {
            assert_eq!(
                req.data.batch_id,
                Some(urgent_batch.id),
                "All claimed requests should come from the urgent batch (earliest expires_at), \
                 but got one from a filler batch — indicates non-FIFO ordering"
            );
        }
    }

    #[sqlx::test]
    async fn test_per_user_fair_scheduling(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        for user in &["user-a", "user-b", "user-c"] {
            let file_id = manager
                .create_file(
                    format!("{}-file", user),
                    None,
                    vec![
                        RequestTemplateInput {
                            custom_id: Some(format!("{}-req-1", user)),
                            endpoint: "https://api.example.com".to_string(),
                            method: "POST".to_string(),
                            path: "/test".to_string(),
                            body: "{}".to_string(),
                            model: "fair-test".to_string(),
                        },
                        RequestTemplateInput {
                            custom_id: Some(format!("{}-req-2", user)),
                            endpoint: "https://api.example.com".to_string(),
                            method: "POST".to_string(),
                            path: "/test".to_string(),
                            body: "{}".to_string(),
                            model: "fair-test".to_string(),
                        },
                    ],
                )
                .await
                .unwrap();

            let _batch = manager
                .create_batch(BatchInput {
                    file_id,
                    endpoint: "/v1/chat/completions".to_string(),
                    completion_window: "24h".to_string(),
                    metadata: None,
                    created_by: Some(user.to_string()),
                    api_key_id: None,
                    total_requests: None,
                })
                .await
                .unwrap();
        }

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("fair-test".to_string(), 6)]);

        let claimed =
            claim_batch_requests_for_test(&manager, 6, 3, daemon_id, &capacity, &HashMap::new())
                .await;

        assert_eq!(
            claimed.len(),
            6,
            "Should claim all 6 requests on cold start"
        );

        sqlx::query!("UPDATE requests SET state = 'pending', daemon_id = NULL, claimed_at = NULL WHERE state = 'claimed'")
            .execute(&pool)
            .await
            .unwrap();

        let user_counts = HashMap::from([("user-a".to_string(), 5usize)]);
        let claimed =
            claim_batch_requests_for_test(&manager, 6, 3, daemon_id, &capacity, &user_counts).await;

        assert_eq!(claimed.len(), 6, "Should still claim all 6 requests");

        let mut per_user: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, req) in claimed.iter().enumerate() {
            per_user
                .entry(req.data.created_by.clone())
                .or_default()
                .push(i);
        }

        let user_a_first = per_user.get("user-a").map(|v| v[0]).unwrap_or(0);
        let user_b_first = per_user.get("user-b").map(|v| v[0]).unwrap_or(usize::MAX);
        let user_c_first = per_user.get("user-c").map(|v| v[0]).unwrap_or(usize::MAX);

        assert!(
            user_b_first < user_a_first,
            "user-b (0 active) should be prioritised over user-a (5 active), \
             but user-b first index={} vs user-a first index={}",
            user_b_first,
            user_a_first
        );
        assert!(
            user_c_first < user_a_first,
            "user-c (0 active) should be prioritised over user-a (5 active), \
             but user-c first index={} vs user-a first index={}",
            user_c_first,
            user_a_first
        );
    }

    #[sqlx::test]
    async fn test_per_user_deadline_ordering_preserved(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id_urgent = manager
            .create_file(
                "urgent-file".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("urgent-req".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "deadline-test".to_string(),
                }],
            )
            .await
            .unwrap();

        let file_id_relaxed = manager
            .create_file(
                "relaxed-file".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("relaxed-req".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: "deadline-test".to_string(),
                }],
            )
            .await
            .unwrap();

        let urgent_batch = manager
            .create_batch(BatchInput {
                file_id: file_id_urgent,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1h".to_string(),
                metadata: None,
                created_by: Some("same-user".to_string()),
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let _relaxed_batch = manager
            .create_batch(BatchInput {
                file_id: file_id_relaxed,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "7d".to_string(),
                metadata: None,
                created_by: Some("same-user".to_string()),
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("deadline-test".to_string(), 2)]);

        let user_counts = HashMap::from([("same-user".to_string(), 0usize)]);
        let claimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon_id, &capacity, &user_counts).await;

        assert_eq!(claimed.len(), 1);
        assert_eq!(
            claimed[0].data.batch_id,
            Some(urgent_batch.id),
            "Urgent batch (earlier deadline) should be claimed first when user priority is equal"
        );
    }

    #[sqlx::test]
    async fn test_urgency_weighted_scheduling(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());


        async fn setup_user_batch(
            manager: &PostgresRequestManager<TestDbPools>,
            user: &str,
            completion_window: &str,
        ) -> BatchId {
            let file_id = manager
                .create_file(
                    format!("{}-file", user),
                    None,
                    vec![RequestTemplateInput {
                        custom_id: Some(format!("{}-req", user)),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "urgency-test".to_string(),
                    }],
                )
                .await
                .unwrap();

            let batch = manager
                .create_batch(BatchInput {
                    file_id,
                    endpoint: "/v1/chat/completions".to_string(),
                    completion_window: completion_window.to_string(),
                    metadata: None,
                    created_by: Some(user.to_string()),
                    api_key_id: None,
                    total_requests: None,
                })
                .await
                .unwrap();
            batch.id
        }

        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_config(DaemonConfig {
            urgency_weight: 0.5,
            ..DaemonConfig::default()
        });

        let batch_a = setup_user_batch(&manager, "user-a", "1h").await;
        let _batch_b = setup_user_batch(&manager, "user-b", "24h").await;

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("urgency-test".to_string(), 2)]);

        let user_counts = HashMap::from([
            ("user-a".to_string(), 1usize),
            ("user-b".to_string(), 1usize),
        ]);

        let claimed =
            claim_batch_requests_for_test(&manager, 1, 1, daemon_id, &capacity, &user_counts).await;

        assert_eq!(claimed.len(), 1);
        assert_eq!(
            claimed[0].data.batch_id,
            Some(batch_a),
            "With urgency_weight=0.5 and equal user activity, \
             the 1hr SLA batch should be claimed before the 24hr batch"
        );

        sqlx::query!(
            "UPDATE requests SET state = 'pending', daemon_id = NULL, claimed_at = NULL WHERE state = 'claimed'"
        )
        .execute(&pool)
        .await
        .unwrap();

        let manager_no_urgency = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_config(DaemonConfig {
            urgency_weight: 0.0,
            ..DaemonConfig::default()
        });

        let user_counts_skewed = HashMap::from([
            ("user-a".to_string(), 5usize),
            ("user-b".to_string(), 0usize),
        ]);

        let claimed = claim_batch_requests_for_test(
            &manager_no_urgency,
            1,
            1,
            daemon_id,
            &capacity,
            &user_counts_skewed,
        )
        .await;

        assert_eq!(claimed.len(), 1);
        assert_eq!(
            claimed[0].data.created_by, "user-b",
            "With urgency_weight=0.0, user-b (0 active) should beat user-a (5 active) \
             despite user-a having a more urgent 1hr SLA"
        );
    }

    #[sqlx::test]
    async fn test_claim_ranks_batches_and_pulls_from_winners(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client.clone(),
        )
        .with_config(DaemonConfig {
            urgency_weight: 1.0,
            ..DaemonConfig::default()
        });

        async fn setup_batch_n(
            manager: &PostgresRequestManager<TestDbPools>,
            pool: &sqlx::PgPool,
            label: &str,
            model: &str,
            n: usize,
            expires_at: DateTime<Utc>,
        ) -> BatchId {
            let templates: Vec<RequestTemplateInput> = (0..n)
                .map(|i| RequestTemplateInput {
                    custom_id: Some(format!("{label}-req-{i}")),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: "{}".to_string(),
                    model: model.to_string(),
                })
                .collect();
            let file_id = manager
                .create_file(format!("{label}-file"), None, templates)
                .await
                .unwrap();
            let batch = manager
                .create_batch(BatchInput {
                    file_id,
                    endpoint: "/v1/chat/completions".to_string(),
                    completion_window: "24h".to_string(),
                    metadata: None,
                    created_by: Some(format!("{label}-user")),
                    api_key_id: None,
                    total_requests: None,
                })
                .await
                .unwrap();
            sqlx::query!(
                "UPDATE batches SET expires_at = $1 WHERE id = $2",
                expires_at,
                *batch.id as Uuid,
            )
            .execute(pool)
            .await
            .unwrap();
            batch.id
        }

        let now = Utc::now();
        let _far = setup_batch_n(
            &manager,
            &pool,
            "far",
            "rank-test",
            3,
            now + chrono::Duration::hours(24),
        )
        .await;
        let mid = setup_batch_n(
            &manager,
            &pool,
            "mid",
            "rank-test",
            3,
            now + chrono::Duration::hours(2),
        )
        .await;
        let soon = setup_batch_n(
            &manager,
            &pool,
            "soon",
            "rank-test",
            3,
            now + chrono::Duration::minutes(30),
        )
        .await;

        let daemon_id = DaemonId::from(Uuid::new_v4());

        let cap3 = HashMap::from([("rank-test".to_string(), 3)]);
        let claimed =
            claim_batch_requests_for_test(&manager, 3, 1, daemon_id, &cap3, &HashMap::new()).await;
        assert_eq!(claimed.len(), 3);
        assert!(
            claimed.iter().all(|r| r.data.batch_id == Some(soon)),
            "with capacity 3, all rows should be pulled from the soonest-expiring batch"
        );

        sqlx::query!(
            "UPDATE requests SET state='pending', daemon_id=NULL, claimed_at=NULL WHERE state='claimed'"
        )
        .execute(&pool)
        .await
        .unwrap();

        let cap5 = HashMap::from([("rank-test".to_string(), 5)]);
        let claimed =
            claim_batch_requests_for_test(&manager, 5, 3, daemon_id, &cap5, &HashMap::new()).await;
        assert_eq!(claimed.len(), 5);
        let from_soon = claimed
            .iter()
            .filter(|r| r.data.batch_id == Some(soon))
            .count();
        let from_mid = claimed
            .iter()
            .filter(|r| r.data.batch_id == Some(mid))
            .count();
        let from_far = claimed
            .iter()
            .filter(|r| r.data.batch_id == Some(_far))
            .count();
        assert_eq!(
            from_soon, 3,
            "the whole soonest batch should be claimed first"
        );
        assert_eq!(
            from_mid, 2,
            "the remainder should come from the next-soonest batch"
        );
        assert_eq!(
            from_far, 0,
            "the farthest-deadline batch should not be touched"
        );
    }


    async fn setup_filter_request(
        manager: &PostgresRequestManager<TestDbPools>,
        pool: &sqlx::PgPool,
        user: &str,
        model: &str,
        expires_at: DateTime<Utc>,
    ) -> RequestId {
        let request_uuid = Uuid::new_v4();
        let request_id = manager
            .create_flex(CreateFlexInput {
                request_id: request_uuid,
                body: format!(r#"{{"model":"{model}","service_tier":"flex"}}"#),
                model: model.to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: user.to_string(),
                metadata: None,
            })
            .await
            .unwrap();

        sqlx::query!(
            "UPDATE requests SET created_at = $1, service_tier = NULL WHERE id = $2",
            expires_at - chrono::Duration::hours(24),
            *request_id as Uuid,
        )
        .execute(pool)
        .await
        .unwrap();

        request_id
    }

    fn filter_test_config() -> DaemonConfig {
        DaemonConfig {
            claim_ramp_exponent: 0.56,
            leaks_per_window: 60.0,
            ..Default::default()
        }
    }

    async fn set_batch_window(
        pool: &sqlx::PgPool,
        request_id: RequestId,
        created_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) {
        let desired_window = expires_at - created_at;
        let (service_tier, effective_window) = if desired_window <= chrono::Duration::hours(2) {
            (Some("flex"), chrono::Duration::hours(1))
        } else {
            (None, chrono::Duration::hours(24))
        };
        sqlx::query!(
            "UPDATE requests SET created_at = $1, service_tier = $2 WHERE id = $3",
            expires_at - effective_window,
            service_tier,
            *request_id as Uuid,
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn mark_not_live(manager: &PostgresRequestManager<TestDbPools>, model: &str) {
        manager
            .append_model_filter_event(&ModelFilter {
                model: model.to_string(),
                state: ModelFilterState::Coming,
                expected_ready_at: None,
            })
            .await
            .unwrap();
    }

    #[sqlx::test]
    async fn test_live_model_claims_full_capacity(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(filter_test_config());

        let expires_at = Utc::now() + chrono::Duration::hours(12);
        setup_filter_request(&manager, &pool, "u1", "live-model", expires_at).await;
        setup_filter_request(&manager, &pool, "u2", "live-model", expires_at).await;
        manager
            .append_model_filter_event(&ModelFilter {
                model: "live-model".to_string(),
                state: ModelFilterState::Live,
                expected_ready_at: None,
            })
            .await
            .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("live-model".to_string(), 5)]);
        let claimed = manager
            .claim_requests(10, daemon_id, &capacity, &HashMap::new(), &HashSet::new())
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            2,
            "a live model drains its backlog at full capacity"
        );
        assert!(
            claimed.iter().all(|r| r.state.leak.is_none()),
            "full-capacity (Source A) claims are never leaked"
        );
    }

    #[sqlx::test]
    async fn test_live_overrides_cooldown(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(filter_test_config());

        let expires_at = Utc::now() + chrono::Duration::hours(12);
        setup_filter_request(&manager, &pool, "heavy", "live-model", expires_at).await;
        manager
            .append_model_filter_event(&ModelFilter {
                model: "live-model".to_string(),
                state: ModelFilterState::Live,
                expected_ready_at: None,
            })
            .await
            .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("live-model".to_string(), 5)]);
        let cooldown = HashSet::from([(
            "heavy".to_string(),
            "default".to_string(),
            "live-model".to_string(),
        )]);
        let claimed = manager
            .claim_requests(10, daemon_id, &capacity, &HashMap::new(), &cooldown)
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            1,
            "a live model claims regardless of cooldown"
        );
        assert!(claimed[0].state.leak.is_none());
    }

    #[sqlx::test]
    async fn test_becoming_live_releases_throttled_backlog(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(filter_test_config());

        let expires_at = Utc::now() + chrono::Duration::hours(12);
        for _ in 0..3 {
            setup_filter_request(&manager, &pool, "u", "m", expires_at).await;
        }
        mark_not_live(&manager, "m").await;

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("m".to_string(), 5)]);

        let throttled = manager
            .claim_requests(10, daemon_id, &capacity, &HashMap::new(), &HashSet::new())
            .await
            .unwrap();
        assert_eq!(throttled.len(), 1, "not-live model trickles one per cycle");
        assert!(throttled[0].state.leak.is_some());

        manager
            .append_model_filter_event(&ModelFilter {
                model: "m".to_string(),
                state: ModelFilterState::Live,
                expected_ready_at: None,
            })
            .await
            .unwrap();
        let cooldown = HashSet::from([("u".to_string(), "default".to_string(), "m".to_string())]);
        let released = manager
            .claim_requests(10, daemon_id, &capacity, &HashMap::new(), &cooldown)
            .await
            .unwrap();
        assert_eq!(
            released.len(),
            2,
            "becoming live releases the full remaining backlog at capacity"
        );
        assert!(
            released.iter().all(|r| r.state.leak.is_none()),
            "live claims are full-capacity, never leaked"
        );
    }

    #[sqlx::test]
    async fn test_not_live_leaky_bucket_one_per_user_window(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(filter_test_config());

        let expires_at = Utc::now() + chrono::Duration::hours(12);
        setup_filter_request(&manager, &pool, "u", "nl-model", expires_at).await;
        setup_filter_request(&manager, &pool, "u", "nl-model", expires_at).await;
        mark_not_live(&manager, "nl-model").await;

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("nl-model".to_string(), 5)]);
        let claimed = manager
            .claim_requests(10, daemon_id, &capacity, &HashMap::new(), &HashSet::new())
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            1,
            "≤ 1 leak per (user, window-class) per cycle"
        );
        let leak = claimed[0]
            .state
            .leak
            .as_ref()
            .expect("a not-live before-ramp claim is leaked");
        assert_eq!(leak.window_class.as_str(), "default");

        let cooldown = HashSet::from([(
            "u".to_string(),
            "default".to_string(),
            "nl-model".to_string(),
        )]);
        let claimed = manager
            .claim_requests(10, daemon_id, &capacity, &HashMap::new(), &cooldown)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 0, "a bucket in cooldown does not leak");
    }

    #[sqlx::test]
    async fn test_two_users_each_leak_one(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(filter_test_config());

        let expires_at = Utc::now() + chrono::Duration::hours(12);
        setup_filter_request(&manager, &pool, "a", "nl-model", expires_at).await;
        setup_filter_request(&manager, &pool, "b", "nl-model", expires_at).await;
        mark_not_live(&manager, "nl-model").await;

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("nl-model".to_string(), 5)]);
        let claimed = manager
            .claim_requests(10, daemon_id, &capacity, &HashMap::new(), &HashSet::new())
            .await
            .unwrap();
        assert_eq!(claimed.len(), 2, "two distinct buckets each leak one");
        assert!(claimed.iter().all(|r| r.state.leak.is_some()));
    }

    #[sqlx::test]
    async fn test_cooldown_is_per_model(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(filter_test_config());

        let expires_at = Utc::now() + chrono::Duration::hours(12);
        setup_filter_request(&manager, &pool, "u", "model-a", expires_at).await;
        setup_filter_request(&manager, &pool, "u", "model-b", expires_at).await;
        mark_not_live(&manager, "model-a").await;
        mark_not_live(&manager, "model-b").await;

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("model-a".to_string(), 5), ("model-b".to_string(), 5)]);

        let cooldown = HashSet::from([(
            "u".to_string(),
            "default".to_string(),
            "model-a".to_string(),
        )]);
        let claimed = manager
            .claim_requests(10, daemon_id, &capacity, &HashMap::new(), &cooldown)
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            1,
            "a per-model cooldown leaves the other model's bucket free to leak"
        );
        let leaked = &claimed[0];
        assert_eq!(
            leaked.data.model.as_str(),
            "model-b",
            "the leaked row is the model NOT in cooldown"
        );
        assert!(leaked.state.leak.is_some(), "the model-b claim is leaked");
    }

    #[sqlx::test]
    async fn test_not_live_within_ramp_claims_full_capacity(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(filter_test_config());

        let created_at = Utc::now() - chrono::Duration::hours(2);
        let expires_at = Utc::now() + chrono::Duration::minutes(5);
        let bid = setup_filter_request(&manager, &pool, "u", "nl-ramp", Utc::now()).await;
        set_batch_window(&pool, bid, created_at, expires_at).await;
        mark_not_live(&manager, "nl-ramp").await;

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("nl-ramp".to_string(), 5)]);
        let cooldown =
            HashSet::from([("u".to_string(), "flex".to_string(), "nl-ramp".to_string())]);
        let claimed = manager
            .claim_requests(10, daemon_id, &capacity, &HashMap::new(), &cooldown)
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            1,
            "a within-ramp request is claimed at full capacity"
        );
        assert!(
            claimed[0].state.leak.is_none(),
            "a ramped claim consumes no leaky-bucket token"
        );
    }

    #[sqlx::test]
    async fn test_expired_deadline_claims_at_full_capacity(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(filter_test_config());

        let created_at = Utc::now() - chrono::Duration::hours(1);
        let expires_at = Utc::now() - chrono::Duration::minutes(5);
        let bid = setup_filter_request(&manager, &pool, "u", "nl-expired", Utc::now()).await;
        set_batch_window(&pool, bid, created_at, expires_at).await;
        mark_not_live(&manager, "nl-expired").await;

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("nl-expired".to_string(), 5)]);
        let cooldown = HashSet::from([(
            "u".to_string(),
            "flex".to_string(),
            "nl-expired".to_string(),
        )]);
        let claimed = manager
            .claim_requests(10, daemon_id, &capacity, &HashMap::new(), &cooldown)
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            1,
            "an overdue request is claimed immediately"
        );
        assert!(
            claimed[0].state.leak.is_none(),
            "an overdue (within-ramp) claim is full-capacity, not leaked"
        );
    }

    #[sqlx::test]
    async fn test_ramp_scales_with_window(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(filter_test_config());

        let now = Utc::now();
        let long_bid = setup_filter_request(&manager, &pool, "long", "ramp-model", now).await;
        set_batch_window(
            &pool,
            long_bid,
            now - chrono::Duration::hours(23) - chrono::Duration::minutes(10),
            now + chrono::Duration::minutes(50),
        )
        .await;
        let short_bid = setup_filter_request(&manager, &pool, "short", "ramp-model", now).await;
        set_batch_window(
            &pool,
            short_bid,
            now - chrono::Duration::minutes(10),
            now + chrono::Duration::minutes(50),
        )
        .await;
        mark_not_live(&manager, "ramp-model").await;

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("ramp-model".to_string(), 5)]);
        let claimed = manager
            .claim_requests(10, daemon_id, &capacity, &HashMap::new(), &HashSet::new())
            .await
            .unwrap();
        assert_eq!(claimed.len(), 2, "both requests are claimed this cycle");
        let leaked_by_user: HashMap<String, bool> = claimed
            .iter()
            .map(|r| (r.data.created_by.clone(), r.state.leak.is_some()))
            .collect();
        assert_eq!(
            leaked_by_user.get("long"),
            Some(&false),
            "the 24h-window request is within ramp => full-capacity (not leaked)"
        );
        assert_eq!(
            leaked_by_user.get("short"),
            Some(&true),
            "the 1h-window request is before ramp => leaked (Source B)"
        );
    }

    #[sqlx::test]
    async fn test_append_model_filter_events(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        manager
            .append_model_filter_events(&[
                ModelFilter {
                    model: "m-live".to_string(),
                    state: ModelFilterState::Live,
                    expected_ready_at: None,
                },
                ModelFilter {
                    model: "m-coming".to_string(),
                    state: ModelFilterState::Coming,
                    expected_ready_at: Some(Utc::now() + chrono::Duration::minutes(5)),
                },
            ])
            .await
            .unwrap();

        let mut listed = manager.list_model_filters().await.unwrap();
        listed.sort_by(|a, b| a.model.cmp(&b.model));
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].model, "m-coming");
        assert_eq!(listed[0].state, ModelFilterState::Coming);
        assert!(listed[0].expected_ready_at.is_some());
        assert_eq!(listed[1].model, "m-live");
        assert_eq!(listed[1].state, ModelFilterState::Live);

        manager
            .append_model_filter_events(&[
                ModelFilter {
                    model: "m-live".to_string(),
                    state: ModelFilterState::Coming,
                    expected_ready_at: Some(Utc::now() + chrono::Duration::minutes(2)),
                },
                ModelFilter {
                    model: "m-new".to_string(),
                    state: ModelFilterState::Live,
                    expected_ready_at: None,
                },
            ])
            .await
            .unwrap();
        let mut listed = manager.list_model_filters().await.unwrap();
        listed.sort_by(|a, b| a.model.cmp(&b.model));
        assert_eq!(
            listed.len(),
            3,
            "all three models have a live/coming latest"
        );
        assert_eq!(listed[1].model, "m-live");
        assert_eq!(
            listed[1].state,
            ModelFilterState::Coming,
            "m-live's latest event is now coming"
        );

        manager
            .append_model_filter_event(&ModelFilter {
                model: "m-new".to_string(),
                state: ModelFilterState::Coming,
                expected_ready_at: Some(Utc::now()),
            })
            .await
            .unwrap();
        let listed = manager.list_model_filters().await.unwrap();
        let m_new = listed.iter().find(|m| m.model == "m-new").unwrap();
        assert_eq!(m_new.state, ModelFilterState::Coming);

        let total: i64 = sqlx::query_scalar!("SELECT COUNT(*) AS \"c!\" FROM model_filters")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(total, 5, "append-only: every event is retained in the log");
    }

    #[test]
    fn test_model_filter_state_roundtrip() {
        for s in [
            ModelFilterState::Live,
            ModelFilterState::Coming,
            ModelFilterState::Leaving,
            ModelFilterState::Absent,
        ] {
            assert_eq!(
                ModelFilterState::parse_state(s.as_str()),
                Some(s),
                "{s:?} must round-trip through as_str/parse_state"
            );
        }
        assert_eq!(ModelFilterState::Leaving.as_str(), "leaving");
        assert_eq!(ModelFilterState::parse_state("nonsense"), None);
    }

    #[sqlx::test]
    async fn test_current_filter_states_and_leaving(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        manager
            .append_model_filter_events(&[
                ModelFilter {
                    model: "m-drain".to_string(),
                    state: ModelFilterState::Live,
                    expected_ready_at: None,
                },
                ModelFilter {
                    model: "m-coming".to_string(),
                    state: ModelFilterState::Coming,
                    expected_ready_at: None,
                },
                ModelFilter {
                    model: "m-gone".to_string(),
                    state: ModelFilterState::Absent,
                    expected_ready_at: None,
                },
            ])
            .await
            .unwrap();
        manager
            .append_model_filter_event(&ModelFilter {
                model: "m-drain".to_string(),
                state: ModelFilterState::Leaving,
                expected_ready_at: None,
            })
            .await
            .unwrap();

        let listed = manager.list_model_filters().await.unwrap();
        let drain = listed.iter().find(|m| m.model == "m-drain").unwrap();
        assert_eq!(
            drain.state,
            ModelFilterState::Leaving,
            "leaving round-trips through the append-only log"
        );
        assert!(
            listed.iter().all(|m| m.model != "m-gone"),
            "absent tombstone is excluded from list_model_filters"
        );

        let states = manager.current_filter_states().await.unwrap();
        assert_eq!(
            states.get("m-drain").map(|(s, _)| *s),
            Some(ModelFilterState::Leaving)
        );
        assert_eq!(
            states.get("m-coming").map(|(s, _)| *s),
            Some(ModelFilterState::Coming)
        );
        assert_eq!(
            states.get("m-gone").map(|(s, _)| *s),
            Some(ModelFilterState::Absent),
            "current_filter_states keeps the latest event even when it is a tombstone"
        );

        let (_, drain_since) = states["m-drain"];
        let now = Utc::now();
        assert!(
            drain_since <= now && now - drain_since < chrono::Duration::minutes(1),
            "since reflects the recent leaving-event timestamp"
        );
    }

    #[sqlx::test]
    async fn test_leaving_treated_as_not_live(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(filter_test_config());

        let expires_at = Utc::now() + chrono::Duration::hours(12);
        setup_filter_request(&manager, &pool, "u", "drain-model", expires_at).await;
        setup_filter_request(&manager, &pool, "u", "drain-model", expires_at).await;
        manager
            .append_model_filter_event(&ModelFilter {
                model: "drain-model".to_string(),
                state: ModelFilterState::Leaving,
                expected_ready_at: None,
            })
            .await
            .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("drain-model".to_string(), 5)]);
        let claimed = manager
            .claim_requests(10, daemon_id, &capacity, &HashMap::new(), &HashSet::new())
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            1,
            "a leaving model is not-live: leaky-bucket trickles ≤1, not full capacity"
        );
        assert!(
            claimed[0].state.leak.is_some(),
            "the leaving model's claim is leaked (not-live path)"
        );
    }

    #[sqlx::test]
    async fn test_tombstone_absent_hides_model(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        manager
            .append_model_filter_event(&ModelFilter {
                model: "m-tomb".to_string(),
                state: ModelFilterState::Live,
                expected_ready_at: None,
            })
            .await
            .unwrap();
        assert_eq!(manager.list_model_filters().await.unwrap().len(), 1);

        manager
            .append_model_filter_event(&ModelFilter {
                model: "m-tomb".to_string(),
                state: ModelFilterState::Absent,
                expected_ready_at: None,
            })
            .await
            .unwrap();

        let listed = manager.list_model_filters().await.unwrap();
        assert!(
            listed.iter().all(|m| m.model != "m-tomb"),
            "a model whose latest event is an absent tombstone is treated as absent"
        );

        let total: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) AS \"c!\" FROM model_filters WHERE model = 'm-tomb'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(total, 2);
    }

    #[sqlx::test]
    async fn test_tombstone_absent_model_is_claimed(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_config(filter_test_config());

        let expires_at = Utc::now() + chrono::Duration::hours(12);
        setup_filter_request(&manager, &pool, "idle-user", "tomb-model", expires_at).await;

        manager
            .append_model_filter_event(&ModelFilter {
                model: "tomb-model".to_string(),
                state: ModelFilterState::Coming,
                expected_ready_at: Some(Utc::now() + chrono::Duration::seconds(30)),
            })
            .await
            .unwrap();
        manager
            .append_model_filter_event(&ModelFilter {
                model: "tomb-model".to_string(),
                state: ModelFilterState::Absent,
                expected_ready_at: None,
            })
            .await
            .unwrap();

        let daemon_id = DaemonId::from(Uuid::new_v4());
        let capacity = HashMap::from([("tomb-model".to_string(), 5)]);
        let claimed = manager
            .claim_requests(10, daemon_id, &capacity, &HashMap::new(), &HashSet::new())
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            1,
            "latest event is absent tombstone => claim (route to OR), not hold"
        );
    }

    #[sqlx::test]
    async fn test_purge_model_filter_events_retention(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        let old = Utc::now() - chrono::Duration::hours(2);
        for i in 0..5 {
            sqlx::query!(
                "INSERT INTO model_filters (model, state, created_at) \
                 VALUES ('m-purge', 'coming', $1)",
                old + chrono::Duration::seconds(i),
            )
            .execute(&pool)
            .await
            .unwrap();
        }
        manager
            .append_model_filter_event(&ModelFilter {
                model: "m-purge".to_string(),
                state: ModelFilterState::Live,
                expected_ready_at: None,
            })
            .await
            .unwrap();

        let deleted = manager
            .purge_model_filter_events(1000, 1, 3600.0)
            .await
            .unwrap();
        assert_eq!(deleted, 5, "the 5 old events should be purged");

        let listed = manager.list_model_filters().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].model, "m-purge");
        assert_eq!(listed[0].state, ModelFilterState::Live);

        let deleted = manager
            .purge_model_filter_events(1000, 0, 0.0)
            .await
            .unwrap();
        assert_eq!(deleted, 0, "nothing left beyond the protected latest event");
        assert_eq!(manager.list_model_filters().await.unwrap().len(), 1);
    }

    #[sqlx::test]
    async fn test_empty_virtual_files_finalized_at_zero(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "all-fail-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/test".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let output_file_id = batch.output_file_id.unwrap();
        let error_file_id = batch.error_file_id.unwrap();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'failed',
                error = '{"error":"all failed"}',
                response_size = 22,
                failed_at = NOW()
            WHERE batch_id = $1
            "#,
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let output_file = manager.get_file(output_file_id).await.unwrap();
        assert!(
            output_file.size_finalized,
            "Empty output file should be finalized"
        );
        assert_eq!(
            output_file.size_bytes, 0,
            "Output file with no completions should have size 0"
        );

        let error_file = manager.get_file(error_file_id).await.unwrap();
        assert!(error_file.size_finalized, "Error file should be finalized");
        assert!(
            error_file.size_bytes > 0,
            "Error file should have size > 0 (2 failed requests)"
        );
    }

    #[sqlx::test]
    async fn test_retry_failed_requests_for_batch(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = Arc::new(PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        ));

        let file_id = manager
            .create_file(
                "retry-batch-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("req-1".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req-2".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req-3".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'failed',
                error = 'test error',
                failed_at = NOW(),
                retry_attempt = 3,
                daemon_id = '00000000-0000-0000-0000-000000000001',
                claimed_at = NOW() - INTERVAL '1 hour',
                started_at = NOW() - INTERVAL '30 minutes'
            WHERE batch_id = $1
            AND id IN (
                SELECT id FROM requests WHERE batch_id = $1 ORDER BY created_at LIMIT 2
            )
            "#,
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query!(
            r#"
            UPDATE batches
            SET failed_at = NOW(),
                finalizing_at = NOW(),
                notification_sent_at = NOW()
            WHERE id = $1
            "#,
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let requests_before = manager.get_batch_requests(batch.id).await.unwrap();
        let failed_count = requests_before
            .iter()
            .filter(|r| matches!(r, AnyRequest::Failed(_)))
            .count();
        let pending_count = requests_before
            .iter()
            .filter(|r| matches!(r, AnyRequest::Pending(_)))
            .count();
        assert_eq!(failed_count, 2);
        assert_eq!(pending_count, 1);

        let retried = manager
            .retry_failed_requests_for_batch(batch.id)
            .await
            .unwrap();
        assert_eq!(retried, 2, "Should have retried 2 failed requests");

        let requests_after = manager.get_batch_requests(batch.id).await.unwrap();
        let pending_after = requests_after
            .iter()
            .filter(|r| matches!(r, AnyRequest::Pending(_)))
            .count();
        assert_eq!(
            pending_after, 3,
            "All requests should be pending after retry"
        );

        let batch_after = sqlx::query!(
            r#"
            SELECT completed_at, failed_at, finalizing_at, notification_sent_at
            FROM batches WHERE id = $1
            "#,
            *batch.id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            batch_after.completed_at.is_none(),
            "completed_at should be cleared after retry"
        );
        assert!(
            batch_after.failed_at.is_none(),
            "failed_at should be cleared after retry"
        );
        assert!(
            batch_after.finalizing_at.is_none(),
            "finalizing_at should be cleared after retry"
        );
        assert!(
            batch_after.notification_sent_at.is_none(),
            "notification_sent_at should be cleared after retry"
        );

        for req in &requests_after {
            if let AnyRequest::Pending(r) = req {
                assert_eq!(
                    r.state.retry_attempt, 0,
                    "retry_attempt should be reset to 0"
                );
            }
        }

        let cleared_check = sqlx::query!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM requests
            WHERE batch_id = $1
                AND (daemon_id IS NOT NULL OR claimed_at IS NOT NULL OR started_at IS NOT NULL)
            "#,
            *batch.id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            cleared_check.count, 0,
            "daemon_id, claimed_at, and started_at should all be NULL after retry"
        );

        let retried_again = manager
            .retry_failed_requests_for_batch(batch.id)
            .await
            .unwrap();
        assert_eq!(retried_again, 0, "No failed requests to retry");
    }

    #[sqlx::test]
    async fn batch_retry_rejects_an_expired_source_without_unfreezing(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let batch = create_background_batch_for_test(&manager, "expired-retry", "owner").await;
        sqlx::query(
            "UPDATE requests SET state = 'failed', error = 'failed', failed_at = NOW() WHERE batch_id = $1",
        )
        .bind(*batch.id)
        .execute(&pool)
        .await
        .unwrap();
        let request_id: Uuid =
            sqlx::query_scalar("SELECT id FROM requests WHERE batch_id = $1 LIMIT 1")
                .bind(*batch.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        sqlx::query(
            "UPDATE batches SET failed_at = NOW(), counts_frozen_at = NOW(), failed_requests = total_requests WHERE id = $1",
        )
        .bind(*batch.id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE files SET expires_at = NOW() - INTERVAL '1 second' WHERE id = $1")
            .bind(*batch.file_id.unwrap())
            .execute(&pool)
            .await
            .unwrap();

        let error = manager
            .retry_failed_requests_for_batch(batch.id)
            .await
            .expect_err("retry must not reactivate content after its source deadline");
        assert!(matches!(error, FusilladeError::ValidationError(_)));
        let state = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64)>(
            "SELECT counts_frozen_at, (SELECT count(*) FROM requests WHERE batch_id = $1 AND state = 'failed') FROM batches WHERE id = $1",
        )
        .bind(*batch.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(state.0.is_some(), "rejected retry must remain frozen");
        assert_eq!(state.1, 1, "failed work must not be re-pended");

        let individual = manager
            .retry_failed_requests(batch.id, vec![RequestId(request_id)])
            .await
            .unwrap();
        assert!(individual[0].is_err());
        let state: String = sqlx::query_scalar("SELECT state FROM requests WHERE id = $1")
            .bind(request_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(state, "failed", "individual retry must also be rejected");
    }

    #[sqlx::test]
    async fn batch_retry_serializes_with_source_expiration(pool: sqlx::PgPool) {
        let manager = Arc::new(PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        ));
        let batch = create_background_batch_for_test(&manager, "retry-race", "owner").await;
        sqlx::query(
            "UPDATE requests SET state = 'failed', error = 'failed', failed_at = NOW() WHERE batch_id = $1",
        )
        .bind(*batch.id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE batches SET failed_at = NOW(), counts_frozen_at = NOW(), failed_requests = total_requests WHERE id = $1",
        )
        .bind(*batch.id)
        .execute(&pool)
        .await
        .unwrap();

        let file_id = *batch.file_id.unwrap();
        let mut expiration = pool.begin().await.unwrap();
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM files WHERE id = $1 FOR UPDATE")
            .bind(file_id)
            .fetch_one(&mut *expiration)
            .await
            .unwrap();
        sqlx::query("UPDATE files SET deleted_at = NOW(), status = 'expired' WHERE id = $1")
            .bind(file_id)
            .execute(&mut *expiration)
            .await
            .unwrap();

        let retry_manager = manager.clone();
        let batch_id = batch.id;
        let mut retry = tokio::spawn(async move {
            retry_manager
                .retry_failed_requests_for_batch(batch_id)
                .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut retry)
                .await
                .is_err(),
            "retry should wait for the source-file deletion lock"
        );

        expiration.commit().await.unwrap();
        let error = retry
            .await
            .unwrap()
            .expect_err("the committed expiration must win the retry race");
        assert!(matches!(error, FusilladeError::ValidationError(_)));
        let state = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64)>(
            "SELECT counts_frozen_at, (SELECT count(*) FROM requests WHERE batch_id = $1 AND state = 'failed') FROM batches WHERE id = $1",
        )
        .bind(*batch.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(state.0.is_some());
        assert_eq!(state.1, 1);
    }


    #[sqlx::test]
    async fn results_files_expire_the_default_lifetime_after_the_batch(pool: sqlx::PgPool) {
        let manager = Arc::new(PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        ));
        let batch = create_background_batch_for_test(&manager, "expiry", "owner").await;

        for file_id in [batch.output_file_id.unwrap(), batch.error_file_id.unwrap()] {
            let file = manager.get_file(file_id).await.unwrap();
            assert_eq!(
                file.expires_at,
                Some(batch.created_at + FileExpiry::default().default),
                "results files carry the batch's clock"
            );
        }
    }

    #[sqlx::test]
    async fn results_files_without_an_expiry_read_as_the_default(pool: sqlx::PgPool) {
        let manager = Arc::new(PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        ));
        let batch = create_background_batch_for_test(&manager, "legacy", "owner").await;
        let output_file_id = batch.output_file_id.unwrap();
        sqlx::query("UPDATE files SET expires_at = NULL WHERE id = $1")
            .bind(*output_file_id)
            .execute(&pool)
            .await
            .unwrap();

        let file = manager.get_file(output_file_id).await.unwrap();
        assert_eq!(
            file.expires_at,
            Some(file.created_at + FileExpiry::default().default)
        );
        let listed = manager
            .list_files(crate::batch::FileFilter::default())
            .await
            .unwrap()
            .into_iter()
            .find(|f| f.id == output_file_id)
            .unwrap();
        assert_eq!(listed.expires_at, file.expires_at);

        let input = manager.get_file(batch.file_id.unwrap()).await.unwrap();
        sqlx::query("UPDATE files SET expires_at = NULL WHERE id = $1")
            .bind(*input.id)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(manager.get_file(input.id).await.unwrap().expires_at, None);
    }

    #[sqlx::test]
    async fn upload_refuses_an_expiry_beyond_the_maximum(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_file_expiry(FileExpiry {
            default: chrono::Duration::days(1),
            max: chrono::Duration::days(2),
        });
        let metadata = |seconds: Option<i64>| {
            FileStreamItem::Metadata(FileMetadata {
                filename: Some("expiry".to_string()),
                expires_after_seconds: seconds,
                uploaded_by: Some("test-user".to_string()),
                ..Default::default()
            })
        };
        let template = || {
            FileStreamItem::Template(RequestTemplateInput {
                custom_id: Some("expiry".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/completions".to_string(),
                body: "{}".to_string(),
                model: "gpt-4".to_string(),
            })
        };

        let error = manager
            .create_file_stream(stream::iter(vec![
                metadata(Some(3 * 24 * 60 * 60)),
                template(),
            ]))
            .await
            .unwrap_err();
        assert!(
            matches!(error, FusilladeError::ValidationError(ref message) if message.contains("172800")),
            "unexpected error: {error:?}"
        );
        let files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM files")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(files, 0, "a refused upload leaves no file behind");

        let before = Utc::now();
        let file_id = expect_stream_success(
            manager
                .create_file_stream(stream::iter(vec![metadata(None), template()]))
                .await
                .unwrap(),
        );
        let expires_at = manager.get_file(file_id).await.unwrap().expires_at.unwrap();
        let lifetime = expires_at - before;
        assert!(
            lifetime >= chrono::Duration::days(1)
                && lifetime < chrono::Duration::days(1) + chrono::Duration::minutes(1),
            "default lifetime applies, got {lifetime}"
        );
    }


    #[sqlx::test]
    async fn batch_creation_rejects_a_due_source_file(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let file_id = manager
            .create_file("due-source".to_string(), None, vec![])
            .await
            .unwrap();
        sqlx::query("UPDATE files SET expires_at = NOW() - INTERVAL '1 second' WHERE id = $1")
            .bind(*file_id)
            .execute(&pool)
            .await
            .unwrap();

        let error = manager
            .create_background_batch_record(BackgroundBatchInput {
                file_id,
                endpoint: "/test".to_string(),
                metadata: None,
                created_by: Some("owner".to_string()),
                api_key_id: None,
                total_requests: Some(0),
            })
            .await
            .expect_err("an expired source cannot start new work");
        assert!(matches!(error, FusilladeError::ValidationError(_)));
        let batch_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM batches WHERE file_id = $1")
                .bind(*file_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(batch_count, 0);
    }

    #[sqlx::test]
    async fn purge_driving_sets_come_from_explicit_deletion_indexes(pool: sqlx::PgPool) {
        sqlx::query(
            r#"
            INSERT INTO batches (id, endpoint, completion_window, created_by, created_at,
                                 deleted_at, retention_expired_at, archive_bucket, expires_at)
            SELECT md5('b' || i)::uuid, '/v1/chat/completions', '24h', 'owner',
                   '2026-01-01'::timestamptz + i * interval '1 second',
                   CASE WHEN i % 1000 = 0 THEN '2026-02-01'::timestamptz END,
                   CASE WHEN i % 2000 = 0 THEN '2026-02-02'::timestamptz END,
                   CASE WHEN i % 3 = 0 THEN '2026-01-05'::date END,
                   '2026-02-02'::timestamptz
            FROM generate_series(1, 20000) i
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO files (id, name, deleted_at, retention_expired_at)
            SELECT md5('f' || i)::uuid, 'file-' || i,
                   CASE WHEN i % 1000 = 0 THEN '2026-02-01'::timestamptz END,
                   CASE WHEN i % 2000 = 0 THEN '2026-02-02'::timestamptz END
            FROM generate_series(1, 20000) i
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("ANALYZE batches").execute(&pool).await.unwrap();
        sqlx::query("ANALYZE files").execute(&pool).await.unwrap();

        let statements = [
            (
                "idx_batches_explicitly_deleted",
                PURGE_DELETED_BATCH_REQUESTS,
            ),
            (
                "idx_batches_explicitly_deleted",
                PURGE_DELETED_BATCH_ARCHIVED_REQUESTS,
            ),
            ("idx_files_explicitly_deleted", PURGE_DELETED_FILE_TEMPLATES),
        ];
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL plan_cache_mode = force_generic_plan")
            .execute(&mut *tx)
            .await
            .unwrap();
        for (index, statement) in statements {
            sqlx::raw_sql(&format!("PREPARE purge_step(bigint) AS {statement}"))
                .execute(&mut *tx)
                .await
                .unwrap();
            let plan: serde_json::Value =
                sqlx::query_scalar("EXPLAIN (FORMAT JSON) EXECUTE purge_step(1000)")
                    .fetch_one(&mut *tx)
                    .await
                    .unwrap();
            sqlx::raw_sql("DEALLOCATE purge_step")
                .execute(&mut *tx)
                .await
                .unwrap();
            let plan = plan.to_string();
            assert!(
                plan.contains(&format!("\"Index Name\":\"{index}\"")),
                "purge driving set must come from {index}: {plan}"
            );
        }
        tx.rollback().await.unwrap();

        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        assert_eq!(manager.purge_orphaned_rows(1000).await.unwrap(), 0);
    }

    #[sqlx::test]
    async fn test_purge_orphaned_templates_after_file_delete(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "purge-templates-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":1}"#.to_string(),
                        model: "test".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":2}"#.to_string(),
                        model: "test".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let count_before: i64 = sqlx::query_scalar!(
            "SELECT count(*) as \"count!\" FROM request_templates_all WHERE file_id = $1",
            *file_id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count_before, 2);

        manager.delete_file(file_id).await.unwrap();

        let deleted = manager.purge_orphaned_rows(1000).await.unwrap();
        assert!(deleted >= 2, "Should have deleted at least 2 templates");

        let count_after: i64 = sqlx::query_scalar!(
            "SELECT count(*) as \"count!\" FROM request_templates_all WHERE file_id = $1",
            *file_id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count_after, 0);

        let deleted_again = manager.purge_orphaned_rows(1000).await.unwrap();
        assert_eq!(deleted_again, 0);
    }

    #[sqlx::test]
    async fn test_purge_orphaned_requests_after_batch_delete(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "purge-requests-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":1}"#.to_string(),
                        model: "test".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/test".to_string(),
                        body: r#"{"n":2}"#.to_string(),
                        model: "test".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let count_before: i64 = sqlx::query_scalar!(
            "SELECT count(*) as \"count!\" FROM requests WHERE batch_id = $1",
            *batch.id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count_before, 2);

        manager.delete_batch(batch.id).await.unwrap();

        let deleted = manager.purge_orphaned_rows(1000).await.unwrap();
        assert!(deleted >= 2, "Should have deleted at least 2 requests");

        let count_after: i64 = sqlx::query_scalar!(
            "SELECT count(*) as \"count!\" FROM requests WHERE batch_id = $1",
            *batch.id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count_after, 0);
    }

    #[sqlx::test]
    async fn purge_never_deletes_rows_for_retention_expired_parents(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let template = |n: u8| RequestTemplateInput {
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: format!("{{\"n\":{n}}}"),
            model: "test".to_string(),
        };
        let mut batches = Vec::new();
        for name in ["purge-explicit", "purge-retention"] {
            let file_id = manager
                .create_file(name.to_string(), None, vec![template(1), template(2)])
                .await
                .unwrap();
            let batch = manager
                .create_batch(crate::batch::BatchInput {
                    file_id,
                    endpoint: "/v1/chat/completions".to_string(),
                    completion_window: "24h".to_string(),
                    metadata: None,
                    created_by: None,
                    api_key_id: None,
                    total_requests: None,
                })
                .await
                .unwrap();
            batches.push((file_id, batch.id));
        }
        let (explicit_file, explicit_batch) = batches[0];
        let (retention_file, retention_batch) = batches[1];
        manager.delete_batch(explicit_batch).await.unwrap();
        manager.delete_file(explicit_file).await.unwrap();
        sqlx::query(
            "UPDATE batches SET deleted_at = NOW(), retention_expired_at = NOW() WHERE id = $1",
        )
        .bind(*retention_batch)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE files SET deleted_at = NOW(), retention_expired_at = NOW() WHERE id = $1",
        )
        .bind(*retention_file)
        .execute(&pool)
        .await
        .unwrap();

        while manager.purge_orphaned_rows(1000).await.unwrap() > 0 {}

        let count = |query: &'static str, id: Uuid| {
            let pool = pool.clone();
            async move {
                sqlx::query_scalar::<_, i64>(query)
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
            }
        };
        assert_eq!(
            count(
                "SELECT count(*) FROM requests WHERE batch_id = $1",
                *explicit_batch
            )
            .await,
            0,
            "explicit deletion must purge live batch requests"
        );
        assert_eq!(
            count(
                "SELECT count(*) FROM request_templates_all WHERE file_id = $1",
                *explicit_file
            )
            .await,
            0,
            "explicit deletion must purge file templates"
        );
        assert_eq!(
            count(
                "SELECT count(*) FROM requests WHERE batch_id = $1",
                *retention_batch
            )
            .await,
            2,
            "a scheduled-retention tombstone must never flow through the explicit purge"
        );
        assert_eq!(
            count(
                "SELECT count(*) FROM request_templates_all WHERE file_id = $1",
                *retention_file
            )
            .await,
            2,
            "scheduled file expiry must never purge templates through the explicit path"
        );
    }

    #[sqlx::test]
    async fn test_purge_does_not_delete_active_rows(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "purge-active-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{"n":1}"#.to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let deleted = manager.purge_orphaned_rows(1000).await.unwrap();
        assert_eq!(deleted, 0, "Should not delete any active rows");

        let template_count: i64 = sqlx::query_scalar!(
            "SELECT count(*) as \"count!\" FROM request_templates_all WHERE file_id = $1",
            *file_id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(template_count, 1);

        let request_count: i64 = sqlx::query_scalar!(
            "SELECT count(*) as \"count!\" FROM requests WHERE batch_id = $1",
            *batch.id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(request_count, 1);
    }

    #[sqlx::test]
    async fn test_purge_returns_zero_when_empty(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let deleted = manager.purge_orphaned_rows(1000).await.unwrap();
        assert_eq!(deleted, 0);
    }

    #[sqlx::test]
    async fn test_purge_respects_batch_size(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let templates: Vec<RequestTemplateInput> = (0..10)
            .map(|i| RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: format!(r#"{{"n":{}}}"#, i),
                model: "test".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file("purge-batch-size-test".to_string(), None, templates)
            .await
            .unwrap();

        manager.delete_file(file_id).await.unwrap();

        let deleted_first = manager.purge_orphaned_rows(3).await.unwrap();
        assert_eq!(deleted_first, 3, "Should delete exactly 3 templates");

        let remaining: i64 = sqlx::query_scalar!(
            "SELECT count(*) as \"count!\" FROM request_templates_all WHERE file_id = $1",
            *file_id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(remaining, 7);

        let deleted_second = manager.purge_orphaned_rows(3).await.unwrap();
        assert_eq!(deleted_second, 3);

        let deleted_third = manager.purge_orphaned_rows(3).await.unwrap();
        assert_eq!(deleted_third, 3);

        let deleted_fourth = manager.purge_orphaned_rows(3).await.unwrap();
        assert_eq!(deleted_fourth, 1, "Only 1 remaining");

        let deleted_fifth = manager.purge_orphaned_rows(3).await.unwrap();
        assert_eq!(deleted_fifth, 0, "Nothing left to purge");
    }

    #[sqlx::test]
    async fn test_purge_deletes_templates_after_file_delete_without_waiting_for_batch(
        pool: sqlx::PgPool,
    ) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "purge-safety-guard-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{"n":1}"#.to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        manager.delete_file(file_id).await.unwrap();

        let deleted = manager.purge_orphaned_rows(1000).await.unwrap();
        assert!(deleted >= 1, "Should delete orphaned templates");

        let template_count: i64 = sqlx::query_scalar!(
            "SELECT count(*) as \"count!\" FROM request_templates_all WHERE file_id = $1",
            *file_id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(template_count, 0);

        let batch_after = manager.get_batch(batch.id).await.unwrap();
        assert!(
            batch_after.cancelling_at.is_some(),
            "Batch should be cancelled"
        );
        assert_eq!(
            batch_after.file_id, None,
            "Batch file_id should be NULL after file deletion"
        );

        let request_count: i64 = sqlx::query_scalar!(
            "SELECT count(*) as \"count!\" FROM requests WHERE batch_id = $1",
            *batch.id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(request_count, 1, "Request should still exist");

        let dangling_count: i64 = sqlx::query_scalar!(
            "SELECT count(*) as \"count!\" FROM requests r \
             LEFT JOIN request_templates_all t ON t.id = r.template_id \
             WHERE r.batch_id = $1 AND r.template_id IS NOT NULL AND t.id IS NULL",
            *batch.id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            dangling_count, 1,
            "the deleted template must resolve to absent, not break the request"
        );

        let input_file = manager.get_file(file_id).await;
        assert!(
            input_file.is_err(),
            "Input file should not be accessible after deletion"
        );
    }

    #[sqlx::test]
    async fn test_purge_batch_size_applies_independently_to_requests_and_templates(
        pool: sqlx::PgPool,
    ) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let templates: Vec<RequestTemplateInput> = (0..5)
            .map(|i| RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/test".to_string(),
                body: format!(r#"{{"n":{}}}"#, i),
                model: "test".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file("purge-independent-limit-test".to_string(), None, templates)
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        manager.delete_batch(batch.id).await.unwrap();
        manager.delete_file(file_id).await.unwrap();

        let deleted = manager.purge_orphaned_rows(3).await.unwrap();
        assert!(
            deleted > 3,
            "batch_size should apply independently: expected >3, got {}",
            deleted
        );

        let mut total_deleted = deleted;
        loop {
            let d = manager.purge_orphaned_rows(3).await.unwrap();
            if d == 0 {
                break;
            }
            total_deleted += d;
        }
        assert_eq!(
            total_deleted, 10,
            "Should delete all 5 requests + 5 templates"
        );
    }


    #[sqlx::test]
    async fn test_delete_request_batchless(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let request_id = manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id: uuid::Uuid::new_v4(),
                body: r#"{"model":"gpt-4","messages":[]}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "user-1".to_string(),
            })
            .await
            .unwrap();

        manager.delete_request(request_id).await.unwrap();

        let count: i64 = sqlx::query_scalar!(
            r#"SELECT count(*) as "count!" FROM requests WHERE id = $1"#,
            *request_id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 0, "Request row should be hard-deleted");
    }

    #[sqlx::test]
    async fn test_delete_request_not_found(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let bogus = crate::request::RequestId(uuid::Uuid::new_v4());
        let err = manager
            .delete_request(bogus)
            .await
            .expect_err("delete on non-existent id should error");

        assert!(
            matches!(err, FusilladeError::RequestNotFound(id) if id == bogus),
            "expected RequestNotFound, got {err:?}",
        );
    }

    #[sqlx::test]
    async fn test_delete_request_leaves_unrelated_requests_intact(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let req_a = manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id: uuid::Uuid::new_v4(),
                body: r#"{"r":"a"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "user-1".to_string(),
            })
            .await
            .unwrap();
        let req_b = manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id: uuid::Uuid::new_v4(),
                body: r#"{"r":"b"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "user-1".to_string(),
            })
            .await
            .unwrap();

        manager.delete_request(req_a).await.unwrap();

        let req_a_count: i64 = sqlx::query_scalar!(
            r#"SELECT count(*) as "count!" FROM requests WHERE id = $1"#,
            *req_a as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let req_b_count: i64 = sqlx::query_scalar!(
            r#"SELECT count(*) as "count!" FROM requests WHERE id = $1"#,
            *req_b as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(req_a_count, 0, "req_a should be hard-deleted");
        assert_eq!(req_b_count, 1, "unrelated req_b must survive");
    }

    #[sqlx::test]
    async fn test_delete_request_removes_batchless_template(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let request_id = manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id: uuid::Uuid::new_v4(),
                body: r#"{"prompt":"erase me"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "user-1".to_string(),
            })
            .await
            .unwrap();

        let template_id: uuid::Uuid = sqlx::query_scalar!(
            "SELECT template_id FROM requests WHERE id = $1",
            *request_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .expect("realtime requests always have a template at creation");

        manager.delete_request(request_id).await.unwrap();

        let req_count: i64 = sqlx::query_scalar!(
            r#"SELECT count(*) as "count!" FROM requests WHERE id = $1"#,
            *request_id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(req_count, 0);

        let tpl_count: i64 = sqlx::query_scalar!(
            r#"SELECT count(*) as "count!" FROM request_templates_all WHERE id = $1"#,
            template_id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(tpl_count, 0, "batchless template should be hard-deleted");
    }

    #[sqlx::test]
    async fn test_delete_request_preserves_file_backed_template(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "preserve-template-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/test".to_string(),
                    body: r#"{"n":1}"#.to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let row = sqlx::query!(
            "SELECT id, template_id FROM requests WHERE batch_id = $1 LIMIT 1",
            *batch.id as Uuid,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let request_id = crate::request::RequestId(row.id);
        let template_id = row.template_id.expect("batched request has a template");

        manager.delete_request(request_id).await.unwrap();

        let tpl_count: i64 = sqlx::query_scalar!(
            r#"SELECT count(*) as "count!" FROM request_templates_all WHERE id = $1"#,
            template_id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            tpl_count, 1,
            "file-backed template must survive single-request deletion",
        );
    }


    #[sqlx::test]
    async fn test_pending_request_counts_by_model_and_window_basic(pool: sqlx::PgPool) {
        use chrono::Duration;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id_a = manager
            .create_file(
                "file-a".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("a1".to_string()),
                        endpoint: "/v1/chat/completions".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/chat/completions".to_string(),
                        body: r#"{"input":"a1"}"#.to_string(),
                        model: "model-a".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("a2".to_string()),
                        endpoint: "/v1/chat/completions".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/chat/completions".to_string(),
                        body: r#"{"input":"a2"}"#.to_string(),
                        model: "model-a".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch_a = manager
            .create_batch(BatchInput {
                file_id: file_id_a,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let file_id_b = manager
            .create_file(
                "file-b".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("b1".to_string()),
                        endpoint: "/v1/chat/completions".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/chat/completions".to_string(),
                        body: r#"{"input":"b1"}"#.to_string(),
                        model: "model-b".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("b2".to_string()),
                        endpoint: "/v1/chat/completions".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/chat/completions".to_string(),
                        body: r#"{"input":"b2"}"#.to_string(),
                        model: "model-b".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch_b = manager
            .create_batch(BatchInput {
                file_id: file_id_b,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let now = Utc::now();
        sqlx::query!(
            "UPDATE batches SET expires_at = $1 WHERE id = $2",
            now + Duration::minutes(30),
            *batch_a.id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query!(
            "UPDATE batches SET expires_at = $1 WHERE id = $2",
            now + Duration::hours(3),
            *batch_b.id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let windows = vec![
            ("1h".to_string(), None, 3600),
            ("4h".to_string(), None, 14_400),
        ];
        let states = vec!["pending".to_string()];
        let model_filter: Vec<String> = vec![];

        let counts = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();

        assert_eq!(*counts.get("model-a").unwrap().get("1h").unwrap(), 2);
        assert_eq!(*counts.get("model-a").unwrap().get("4h").unwrap(), 2);

        assert_eq!(*counts.get("model-b").unwrap().get("1h").unwrap(), 0);
        assert_eq!(*counts.get("model-b").unwrap().get("4h").unwrap(), 2);

        let disjoint = manager
            .get_pending_request_counts_by_model_and_window(
                &[("1h:4h".to_string(), Some(3600), 14_400)],
                &states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert_eq!(*disjoint.get("model-a").unwrap().get("1h:4h").unwrap(), 0);
        assert_eq!(*disjoint.get("model-b").unwrap().get("1h:4h").unwrap(), 2);

        let err = manager
            .get_pending_request_counts_by_model_and_window(
                &[("bad".to_string(), Some(7200), 3600)],
                &states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("start"));

        sqlx::query!(
            "UPDATE batches SET expires_at = $1 WHERE id = $2",
            now - Duration::minutes(10),
            *batch_a.id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let incl_overdue = manager
            .get_pending_request_counts_by_model_and_window(
                &[("1h".to_string(), None, 3600)],
                &states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert_eq!(*incl_overdue.get("model-a").unwrap().get("1h").unwrap(), 2);

        let excl_overdue = manager
            .get_pending_request_counts_by_model_and_window(
                &[("1h".to_string(), Some(0), 3600)],
                &states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert_eq!(*excl_overdue.get("model-a").unwrap().get("1h").unwrap(), 0);
    }

    #[sqlx::test]
    async fn test_pending_request_counts_respects_states_models_and_cancelling(pool: sqlx::PgPool) {
        use chrono::Duration;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "file-mixed".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("a1".to_string()),
                        endpoint: "/v1/chat/completions".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/chat/completions".to_string(),
                        body: r#"{"input":"a1"}"#.to_string(),
                        model: "model-a".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("b1".to_string()),
                        endpoint: "/v1/chat/completions".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/chat/completions".to_string(),
                        body: r#"{"input":"b1"}"#.to_string(),
                        model: "model-b".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch = manager
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
            .unwrap();

        let now = Utc::now();
        sqlx::query!(
            "UPDATE batches SET expires_at = $1 WHERE id = $2",
            now + Duration::minutes(10),
            *batch.id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query!(
            r#"
            UPDATE requests
            SET state = 'claimed',
                daemon_id = $1,
                claimed_at = NOW()
            WHERE batch_id = $2 AND model = 'model-b'
            "#,
            Uuid::new_v4(),
            *batch.id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let windows = vec![("15m".to_string(), None, 900)];
        let states = vec!["pending".to_string()];
        let model_filter = vec!["model-a".to_string()];

        let counts = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();

        assert_eq!(*counts.get("model-a").unwrap().get("15m").unwrap(), 1);
        assert!(!counts.contains_key("model-b"));

        let states = vec!["pending".to_string(), "claimed".to_string()];
        let model_filter: Vec<String> = vec![];

        let counts_all = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();

        assert_eq!(*counts_all.get("model-a").unwrap().get("15m").unwrap(), 1);
        assert_eq!(*counts_all.get("model-b").unwrap().get("15m").unwrap(), 1);

        sqlx::query!(
            "UPDATE batches SET cancelling_at = NOW() WHERE id = $1",
            *batch.id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let counts_cancelled = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();

        assert!(counts_cancelled.is_empty());
    }

    #[sqlx::test]
    async fn test_pending_request_counts_empty_inputs(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager =
            PostgresRequestManager::with_client(TestDbPools::new(pool).await.unwrap(), http_client);

        let counts = manager
            .get_pending_request_counts_by_model_and_window(
                &[],
                &["pending".to_string()],
                &[],
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert!(counts.is_empty());

        let counts = manager
            .get_pending_request_counts_by_model_and_window(
                &[("1h".to_string(), None, 3600)],
                &[],
                &[],
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert!(counts.is_empty());
    }

    #[sqlx::test]
    async fn test_pending_request_counts_service_tier_filter(pool: sqlx::PgPool) {
        use chrono::Duration;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let mut batch_ids = Vec::new();
        for (label, completion_window) in [("batch", "24h"), ("flex", "1h"), ("priority", "0s")] {
            let file_id = manager
                .create_file(
                    format!("file-{label}"),
                    None,
                    vec![RequestTemplateInput {
                        custom_id: Some(label.to_string()),
                        endpoint: "/v1/chat/completions".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/chat/completions".to_string(),
                        body: r#"{"input":"x"}"#.to_string(),
                        model: "model-a".to_string(),
                    }],
                )
                .await
                .unwrap();

            let batch = manager
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
                .unwrap();
            batch_ids.push(batch.id);
        }

        let now = Utc::now();
        for batch_id in &batch_ids {
            sqlx::query!(
                "UPDATE batches SET expires_at = $1 WHERE id = $2",
                now + Duration::minutes(30),
                **batch_id as Uuid
            )
            .execute(&pool)
            .await
            .unwrap();
        }

        let windows = vec![("1h".to_string(), None, 3600)];
        let states = vec!["pending".to_string()];
        let model_filter: Vec<String> = vec![];

        let counts = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert_eq!(*counts.get("model-a").unwrap().get("1h").unwrap(), 3);

        let counts = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &states,
                &model_filter,
                &ServiceTierFilter::Exclude(vec![Some("priority".to_string())]),
                false,
            )
            .await
            .unwrap();
        assert_eq!(*counts.get("model-a").unwrap().get("1h").unwrap(), 2);

        let counts = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &states,
                &model_filter,
                &ServiceTierFilter::Include(vec![None]),
                false,
            )
            .await
            .unwrap();
        assert_eq!(*counts.get("model-a").unwrap().get("1h").unwrap(), 1);

        let counts = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &states,
                &model_filter,
                &ServiceTierFilter::Include(vec![None, Some("flex".to_string())]),
                false,
            )
            .await
            .unwrap();
        assert_eq!(*counts.get("model-a").unwrap().get("1h").unwrap(), 2);

        let counts = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &states,
                &model_filter,
                &ServiceTierFilter::Exclude(vec![None]),
                false,
            )
            .await
            .unwrap();
        assert_eq!(*counts.get("model-a").unwrap().get("1h").unwrap(), 2);

        let counts = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &states,
                &model_filter,
                &ServiceTierFilter::Include(vec![]),
                false,
            )
            .await
            .unwrap();
        assert!(counts.is_empty());
    }

    #[sqlx::test]
    async fn test_pending_counts_by_tier_carries_tier_and_folds_to_flat(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let flex_id = Uuid::new_v4();
        manager
            .create_flex(CreateFlexInput {
                request_id: flex_id,
                body: r#"{"input":"flex"}"#.to_string(),
                model: "tier-model".to_string(),
                endpoint: "http://localhost".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: "queue-user".to_string(),
                metadata: None,
            })
            .await
            .unwrap();

        let windows = vec![("1h".to_string(), None, 3600)];
        let states = vec!["pending".to_string()];
        let model_filter: Vec<String> = vec![];

        let rows = manager
            .get_pending_request_counts_by_model_window_and_tier(
                &windows,
                &states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        let row = rows
            .iter()
            .find(|row| row.model == "tier-model" && row.count > 0)
            .expect("flex row must appear in tier-grouped counts");
        assert_eq!(row.window_label, "1h");
        assert_eq!(row.service_tier.as_deref(), Some("flex"));
        assert_eq!(row.outcome, "pending");
        assert_eq!(row.count, 1);

        let flat = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert_eq!(*flat.get("tier-model").unwrap().get("1h").unwrap(), 1);
    }

    #[sqlx::test]
    async fn test_pending_request_counts_includes_batchless_flex(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let flex_id = Uuid::new_v4();
        manager
            .create_flex(CreateFlexInput {
                request_id: flex_id,
                body: r#"{"input":"flex"}"#.to_string(),
                model: "flex-model".to_string(),
                endpoint: "http://localhost".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: "queue-user".to_string(),
                metadata: None,
            })
            .await
            .unwrap();

        sqlx::query("UPDATE requests SET created_at = NOW() - INTERVAL '5 minutes' WHERE id = $1")
            .bind(flex_id)
            .execute(&pool)
            .await
            .unwrap();

        let windows = vec![
            ("1h".to_string(), None, 3600),
            ("24h".to_string(), None, 86_400),
        ];
        let pending = vec!["pending".to_string()];
        let model_filter: Vec<String> = vec![];

        let counts = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &pending,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        let flex_counts = counts
            .get("flex-model")
            .expect("batchless flex request must appear in pending counts");
        assert_eq!(*flex_counts.get("1h").unwrap(), 1);
        assert_eq!(*flex_counts.get("24h").unwrap(), 1);

        let bounded = manager
            .get_pending_request_counts_by_model_and_window(
                &[("1h".to_string(), Some(0), 3600)],
                &pending,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert_eq!(*bounded.get("flex-model").unwrap().get("1h").unwrap(), 1);

        sqlx::query("UPDATE requests SET created_at = NOW() - INTERVAL '2 hours' WHERE id = $1")
            .bind(flex_id)
            .execute(&pool)
            .await
            .unwrap();
        let overdue = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &pending,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert_eq!(*overdue.get("flex-model").unwrap().get("1h").unwrap(), 1);
        let overdue_bounded = manager
            .get_pending_request_counts_by_model_and_window(
                &[("1h".to_string(), Some(0), 3600)],
                &pending,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert_eq!(
            *overdue_bounded
                .get("flex-model")
                .unwrap()
                .get("1h")
                .unwrap(),
            0
        );
        sqlx::query("UPDATE requests SET created_at = NOW() - INTERVAL '5 minutes' WHERE id = $1")
            .bind(flex_id)
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query("UPDATE requests SET state = 'claimed', daemon_id = gen_random_uuid(), claimed_at = NOW() WHERE id = $1")
            .bind(flex_id)
            .execute(&pool)
            .await
            .unwrap();
        let pending_only = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &pending,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert!(!pending_only.contains_key("flex-model"));

        let active_states = vec![
            "pending".to_string(),
            "claimed".to_string(),
            "processing".to_string(),
        ];
        let active = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &active_states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert_eq!(*active.get("flex-model").unwrap().get("1h").unwrap(), 1);

        let realtime_id = Uuid::new_v4();
        manager
            .create_realtime(CreateRealtimeInput {
                request_id: realtime_id,
                body: r#"{"input":"rt"}"#.to_string(),
                model: "rt-model".to_string(),
                endpoint: "http://localhost".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "queue-user".to_string(),
            })
            .await
            .unwrap();
        sqlx::query("UPDATE requests SET created_at = NOW() - INTERVAL '1 minute' WHERE id = $1")
            .bind(realtime_id)
            .execute(&pool)
            .await
            .unwrap();

        let excl_priority = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &active_states,
                &model_filter,
                &ServiceTierFilter::Exclude(vec![Some("priority".to_string())]),
                false,
            )
            .await
            .unwrap();
        assert_eq!(
            *excl_priority.get("flex-model").unwrap().get("1h").unwrap(),
            1
        );
        assert!(!excl_priority.contains_key("rt-model"));

        let any_tier = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &active_states,
                &model_filter,
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert_eq!(*any_tier.get("rt-model").unwrap().get("24h").unwrap(), 1);
        assert_eq!(*any_tier.get("rt-model").unwrap().get("1h").unwrap(), 0);

        let excl_flex = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &active_states,
                &model_filter,
                &ServiceTierFilter::Exclude(vec![Some("flex".to_string())]),
                false,
            )
            .await
            .unwrap();
        assert!(!excl_flex.contains_key("flex-model"));

        let filtered = manager
            .get_pending_request_counts_by_model_and_window(
                &windows,
                &active_states,
                &["other-model".to_string()],
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();
        assert!(!filtered.contains_key("flex-model"));
    }


    #[sqlx::test]
    async fn test_list_batches_filter_by_api_key_id(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "api-key-filter-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let key_a = uuid::Uuid::new_v4();
        let key_b = uuid::Uuid::new_v4();

        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: Some(key_a),
                total_requests: None,
            })
            .await
            .unwrap();

        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: Some(key_b),
                total_requests: None,
            })
            .await
            .unwrap();

        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                api_key_ids: Some(vec![key_a]),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].api_key_id, Some(key_a));

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                api_key_ids: Some(vec![key_a, key_b]),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        let returned_keys: std::collections::HashSet<_> =
            results.iter().filter_map(|b| b.api_key_id).collect();
        assert!(returned_keys.contains(&key_a));
        assert!(returned_keys.contains(&key_b));

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                api_key_ids: Some(vec![]),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 0);

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[sqlx::test]
    async fn test_list_batches_filter_by_status_completed(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "status-filter-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                status: Some("completed".to_string()),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(results.iter().all(|b| b.id != batch.id));

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                status: Some("in_progress".to_string()),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(results.iter().any(|b| b.id == batch.id));
    }

    #[sqlx::test]
    async fn test_list_batches_filter_unknown_status_returns_error(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let result = manager
            .list_batches(crate::batch::ListBatchesFilter {
                status: Some("nonexistent_status".to_string()),
                limit: Some(100),
                ..Default::default()
            })
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Unknown batch status filter"),
            "Expected error about unknown status, got: {}",
            err
        );

        let result = manager
            .list_batches(crate::batch::ListBatchesFilter {
                api_key_ids: Some(vec![]),
                status: Some("not_a_status".to_string()),
                limit: Some(100),
                ..Default::default()
            })
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Unknown batch status filter"),
            "Expected status error even with empty api_key_ids, got: {}",
            err
        );
    }

    #[sqlx::test]
    async fn test_list_batches_filter_by_time_range(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "time-filter-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let batch_created_at = batch.created_at;

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                created_after: Some(batch_created_at + chrono::Duration::seconds(1)),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(results.is_empty());

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                created_before: Some(batch_created_at - chrono::Duration::seconds(1)),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(results.is_empty());

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                created_after: Some(batch_created_at - chrono::Duration::seconds(1)),
                created_before: Some(batch_created_at + chrono::Duration::seconds(1)),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(!results.is_empty());
    }

    #[sqlx::test]
    async fn test_list_batches_search_by_batch_id(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "id-search-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let id_str = batch.id.0.to_string();
        let search_term = &id_str[..8];

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                search: Some(search_term.to_string()),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(results.iter().any(|b| b.id == batch.id));

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                search: Some("zzz_no_match_zzz".to_string()),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(results.iter().all(|b| b.id != batch.id));
    }

    #[sqlx::test]
    async fn test_list_batches_filter_by_status_in_progress_includes_validating(
        pool: sqlx::PgPool,
    ) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "validating-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        sqlx::query!(
            "UPDATE batches SET total_requests = 0 WHERE id = $1",
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                status: Some("in_progress".to_string()),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(results.iter().any(|b| b.id == batch.id));
    }

    #[sqlx::test]
    async fn test_list_batches_filter_by_status_cancelled(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "cancelled-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch_a = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let batch_b = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        manager.cancel_batch(batch_a.id).await.unwrap();

        sqlx::query!(
            "UPDATE batches SET cancelling_at = NOW() WHERE id = $1",
            *batch_b.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                status: Some("cancelled".to_string()),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(
            results.iter().any(|b| b.id == batch_a.id),
            "fully cancelled batch should match"
        );
        assert!(
            results.iter().any(|b| b.id == batch_b.id),
            "cancelling batch should also match"
        );

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                status: Some("in_progress".to_string()),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(results.iter().all(|b| b.id != batch_a.id));
        assert!(results.iter().all(|b| b.id != batch_b.id));
    }

    #[sqlx::test]
    async fn test_list_batches_filter_by_status_failed(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "failed-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        sqlx::query!(
            "UPDATE batches SET failed_at = NOW() WHERE id = $1",
            *batch.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                status: Some("failed".to_string()),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(results.iter().any(|b| b.id == batch.id));

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                status: Some("in_progress".to_string()),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(results.iter().all(|b| b.id != batch.id));
    }

    #[sqlx::test]
    async fn test_list_batches_filter_by_status_expired(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "expired-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let batch_a = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();
        sqlx::query!(
            "UPDATE batches SET expires_at = NOW() - INTERVAL '1 hour' WHERE id = $1",
            *batch_a.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let batch_b = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();
        sqlx::query!(
            "UPDATE batches SET expires_at = NOW() - INTERVAL '2 hours', completed_at = NOW() - INTERVAL '1 hour' WHERE id = $1",
            *batch_b.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let batch_c = manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();
        sqlx::query!(
            "UPDATE batches SET expires_at = NOW() + INTERVAL '1 hour', completed_at = NOW() WHERE id = $1",
            *batch_c.id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                status: Some("expired".to_string()),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();

        assert!(
            results.iter().any(|b| b.id == batch_a.id),
            "overdue in-progress batch should match"
        );
        assert!(
            results.iter().any(|b| b.id == batch_b.id),
            "completed-after-deadline batch should match"
        );
        assert!(
            results.iter().all(|b| b.id != batch_c.id),
            "on-time completed batch should not match"
        );
    }


    #[sqlx::test]
    async fn test_list_files_filter_by_api_key_id(pool: sqlx::PgPool) {
        use crate::batch::{FileMetadata, FileStreamItem};
        use futures::stream;

        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let key_a = uuid::Uuid::new_v4();
        let key_b = uuid::Uuid::new_v4();

        let file_a = manager
            .create_file_stream(stream::iter(vec![
                FileStreamItem::Metadata(FileMetadata {
                    filename: Some("file-a.jsonl".to_string()),
                    api_key_id: Some(key_a),
                    ..Default::default()
                }),
                FileStreamItem::Template(RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }),
            ]))
            .await
            .map(expect_stream_success)
            .unwrap();

        let file_b = manager
            .create_file_stream(stream::iter(vec![
                FileStreamItem::Metadata(FileMetadata {
                    filename: Some("file-b.jsonl".to_string()),
                    api_key_id: Some(key_b),
                    ..Default::default()
                }),
                FileStreamItem::Template(RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }),
            ]))
            .await
            .map(expect_stream_success)
            .unwrap();

        let results = manager
            .list_files(crate::batch::FileFilter {
                api_key_ids: Some(vec![key_a]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, file_a);

        let results = manager
            .list_files(crate::batch::FileFilter {
                api_key_ids: Some(vec![key_b]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, file_b);

        let results = manager
            .list_files(crate::batch::FileFilter {
                api_key_ids: Some(vec![key_a, key_b]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        let ids: std::collections::HashSet<_> = results.iter().map(|f| f.id).collect();
        assert!(ids.contains(&file_a));
        assert!(ids.contains(&file_b));

        let results = manager
            .list_files(crate::batch::FileFilter {
                api_key_ids: Some(vec![]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 0);

        let results = manager
            .list_files(crate::batch::FileFilter::default())
            .await
            .unwrap();
        let ids: Vec<_> = results.iter().map(|f| f.id).collect();
        assert!(ids.contains(&file_a));
        assert!(ids.contains(&file_b));
    }

    #[sqlx::test]
    async fn test_list_batches_active_first_sorting(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "active-first-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let mut batch_ids = Vec::new();
        for i in 0..5 {
            let batch = manager
                .create_batch(crate::batch::BatchInput {
                    file_id,
                    endpoint: "/v1/chat/completions".to_string(),
                    completion_window: "24h".to_string(),
                    metadata: None,
                    created_by: None,
                    api_key_id: None,
                    total_requests: None,
                })
                .await
                .unwrap();
            sqlx::query("UPDATE batches SET created_at = NOW() + ($1 || ' seconds')::INTERVAL WHERE id = $2")
                .bind(i.to_string())
                .bind(*batch.id as Uuid)
                .execute(&pool)
                .await
                .unwrap();
            batch_ids.push(batch.id);
        }

        sqlx::query("UPDATE batches SET completed_at = NOW() WHERE id = $1")
            .bind(*batch_ids[2] as Uuid)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE batches SET cancelling_at = NOW(), cancelled_at = NOW() WHERE id = $1")
            .bind(*batch_ids[3] as Uuid)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE batches SET cancelling_at = NOW() WHERE id = $1")
            .bind(*batch_ids[4] as Uuid)
            .execute(&pool)
            .await
            .unwrap();

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                active_first: false,
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        let result_ids: Vec<_> = results.iter().map(|b| b.id).collect();
        assert_eq!(
            result_ids,
            vec![
                batch_ids[4],
                batch_ids[3],
                batch_ids[2],
                batch_ids[1],
                batch_ids[0]
            ]
        );

        let results = manager
            .list_batches(crate::batch::ListBatchesFilter {
                active_first: true,
                limit: Some(100),
                ..Default::default()
            })
            .await
            .unwrap();
        let result_ids: Vec<_> = results.iter().map(|b| b.id).collect();
        assert_eq!(
            result_ids,
            vec![
                batch_ids[1],
                batch_ids[0],
                batch_ids[4],
                batch_ids[3],
                batch_ids[2]
            ],
            "Active batches should sort before newer terminal ones"
        );
    }

    #[sqlx::test]
    async fn test_list_batches_active_first_cursor_pagination(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let file_id = manager
            .create_file(
                "cursor-test".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: "{}".to_string(),
                    model: "test".to_string(),
                }],
            )
            .await
            .unwrap();

        let mut batch_ids = Vec::new();
        for i in 0..4 {
            let batch = manager
                .create_batch(crate::batch::BatchInput {
                    file_id,
                    endpoint: "/v1/chat/completions".to_string(),
                    completion_window: "24h".to_string(),
                    metadata: None,
                    created_by: None,
                    api_key_id: None,
                    total_requests: None,
                })
                .await
                .unwrap();
            sqlx::query("UPDATE batches SET created_at = NOW() + ($1 || ' seconds')::INTERVAL WHERE id = $2")
                .bind(i.to_string())
                .bind(*batch.id as Uuid)
                .execute(&pool)
                .await
                .unwrap();
            batch_ids.push(batch.id);
        }

        sqlx::query("UPDATE batches SET completed_at = NOW() WHERE id = $1")
            .bind(*batch_ids[2] as Uuid)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE batches SET completed_at = NOW() WHERE id = $1")
            .bind(*batch_ids[3] as Uuid)
            .execute(&pool)
            .await
            .unwrap();


        let page1 = manager
            .list_batches(crate::batch::ListBatchesFilter {
                active_first: true,
                limit: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].id, batch_ids[1], "newest active batch first");
        assert_eq!(page1[1].id, batch_ids[0], "oldest active batch second");

        let page2 = manager
            .list_batches(crate::batch::ListBatchesFilter {
                active_first: true,
                limit: Some(2),
                after: Some(page1.last().unwrap().id),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page2.len(), 2);
        assert_eq!(page2[0].id, batch_ids[3], "newest terminal batch first");
        assert_eq!(page2[1].id, batch_ids[2], "oldest terminal batch second");

        let page3 = manager
            .list_batches(crate::batch::ListBatchesFilter {
                active_first: true,
                limit: Some(2),
                after: Some(page2.last().unwrap().id),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(
            page3.is_empty(),
            "Should have no more results after last page"
        );

        let all_ids: Vec<_> = page1.iter().chain(page2.iter()).map(|b| b.id).collect();
        assert_eq!(
            all_ids,
            vec![batch_ids[1], batch_ids[0], batch_ids[3], batch_ids[2]],
            "Full pagination should return all batches in active-first order"
        );
    }

    #[sqlx::test]
    async fn test_list_batches_completion_windows(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let template = RequestTemplateInput {
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/v1/chat/completions".to_string(),
            body: "{}".to_string(),
            model: "gpt-4".to_string(),
        };

        let file_1h = manager
            .create_file("1h-file".to_string(), None, vec![template.clone()])
            .await
            .unwrap();
        let batch_1h = manager
            .create_batch(crate::batch::BatchInput {
                file_id: file_1h,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let file_24h = manager
            .create_file("24h-file".to_string(), None, vec![template.clone()])
            .await
            .unwrap();
        manager
            .create_batch(crate::batch::BatchInput {
                file_id: file_24h,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let result = manager
            .list_batches(crate::batch::ListBatchesFilter {
                completion_windows: Some(vec!["1h".to_string()]),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, batch_1h.id);
        assert_eq!(result[0].completion_window.as_deref(), Some("1h"));

        let result = manager
            .list_batches(crate::batch::ListBatchesFilter {
                completion_windows: Some(vec!["24h".to_string()]),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].completion_window.as_deref(), Some("24h"));

        let result = manager
            .list_batches(crate::batch::ListBatchesFilter {
                completion_windows: Some(vec!["1h".to_string(), "24h".to_string()]),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 2);

        let result = manager
            .list_batches(crate::batch::ListBatchesFilter::default())
            .await
            .unwrap();

        assert_eq!(result.len(), 2);

        let result = manager
            .list_batches(crate::batch::ListBatchesFilter {
                completion_windows: Some(vec![]),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 0);
    }


    #[sqlx::test]
    async fn test_list_requests_empty(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let result = manager
            .list_requests(crate::request::ListRequestsFilter {
                limit: 10,
                ..Default::default()
            })
            .await
            .expect("list_requests should succeed on empty DB");

        assert_eq!(result.total_count, 0);
        assert!(result.data.is_empty());
    }

    #[sqlx::test]
    async fn test_list_requests_returns_requests(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        for body in [
            r#"{"model":"gpt-4","messages":[{"role":"user","content":"hello"}]}"#,
            r#"{"model":"gpt-4","messages":[{"role":"user","content":"world"}]}"#,
        ] {
            manager
                .create_realtime(crate::request::CreateRealtimeInput {
                    request_id: uuid::Uuid::new_v4(),
                    body: body.to_string(),
                    model: "gpt-4".to_string(),
                    endpoint: "http://localhost:3001/ai".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/responses".to_string(),
                    created_by: "test-user".to_string(),
                })
                .await
                .expect("create_realtime should succeed");
        }

        let result = manager
            .list_requests(crate::request::ListRequestsFilter {
                limit: 10,
                ..Default::default()
            })
            .await
            .expect("list_requests should succeed");

        assert_eq!(result.total_count, 2);
        assert_eq!(result.data.len(), 2);
        assert!(result.data.iter().all(|r| r.model == "gpt-4"));
        assert!(result.data.iter().all(|r| r.status == "processing"));
    }

    #[sqlx::test]
    async fn test_list_requests_pagination(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        for i in 0..5 {
            manager
                .create_realtime(crate::request::CreateRealtimeInput {
                    request_id: uuid::Uuid::new_v4(),
                    body: format!(r#"{{"model":"gpt-4","prompt":"{}"}}"#, i),
                    model: "gpt-4".to_string(),
                    endpoint: "http://localhost".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/responses".to_string(),
                    created_by: "pagination-user".to_string(),
                })
                .await
                .unwrap();
        }

        let page1 = manager
            .list_requests(crate::request::ListRequestsFilter {
                limit: 2,
                skip: 0,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page1.total_count, 5);
        assert_eq!(page1.data.len(), 2);

        let page2 = manager
            .list_requests(crate::request::ListRequestsFilter {
                limit: 2,
                skip: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page2.total_count, 5);
        assert_eq!(page2.data.len(), 2);

        let page3 = manager
            .list_requests(crate::request::ListRequestsFilter {
                limit: 2,
                skip: 4,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page3.total_count, 5);
        assert_eq!(page3.data.len(), 1);
    }

    #[sqlx::test]
    async fn test_list_requests_active_first_ordering(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let mut req_ids: Vec<uuid::Uuid> = Vec::new();
        for _ in 0..4 {
            let id = uuid::Uuid::new_v4();
            manager
                .create_realtime(crate::request::CreateRealtimeInput {
                    request_id: id,
                    body: "{}".to_string(),
                    model: "gpt-4".to_string(),
                    endpoint: "http://localhost".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/responses".to_string(),
                    created_by: "af-user".to_string(),
                })
                .await
                .unwrap();
            req_ids.push(id);
        }

        sqlx::query("UPDATE requests SET state='completed', response_status=200, response_body='{}', completed_at=NOW() WHERE id=$1")
            .bind(req_ids[0])
            .execute(&pool).await.unwrap();
        sqlx::query("UPDATE requests SET state='claimed', daemon_id=gen_random_uuid(), claimed_at=NOW(), started_at=NULL WHERE id=$1")
            .bind(req_ids[2])
            .execute(&pool).await.unwrap();
        sqlx::query("UPDATE requests SET state='pending', daemon_id=NULL, claimed_at=NULL, started_at=NULL WHERE id=$1")
            .bind(req_ids[3])
            .execute(&pool).await.unwrap();

        let result = manager
            .list_requests(crate::request::ListRequestsFilter {
                active_first: true,
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(result.data.len(), 4);
        let states: Vec<&str> = result.data.iter().map(|r| r.status.as_str()).collect();
        assert_eq!(
            states,
            vec!["processing", "claimed", "pending", "completed"],
            "active_first must order by state priority"
        );
    }

    #[sqlx::test]
    async fn test_get_request_detail(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let request_id = uuid::Uuid::new_v4();
        manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id,
                body: r#"{"model":"gpt-4","messages":[{"role":"user","content":"test"}]}"#
                    .to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "test-user-id".to_string(),
            })
            .await
            .unwrap();

        let detail = manager
            .get_request_detail(crate::request::RequestId(request_id))
            .await
            .expect("get_request_detail should succeed");

        assert_eq!(detail.model, "gpt-4");
        assert_eq!(detail.status, "processing");
        assert_eq!(detail.created_by, "test-user-id");
        assert!(detail.body.as_deref().unwrap().contains("gpt-4"));
    }

    #[sqlx::test]
    async fn test_terminal_poll_does_not_read_pending_template(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::new(
            TestDbPools::new(pool.clone()).await.unwrap(),
            PostgresStorageConfig::default(),
        );
        let id = Uuid::new_v4();
        manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id: id,
                body: "{}".into(),
                model: "test".into(),
                endpoint: "http://localhost".into(),
                method: "POST".into(),
                path: "/".into(),
                created_by: "owner".into(),
            })
            .await
            .unwrap();
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE request_template_routes IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *blocker)
            .await
            .unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            manager.get_terminal_request_detail(crate::request::RequestId(id)),
        )
        .await;
        blocker.rollback().await.unwrap();
        assert!(
            result
                .expect("pending polls must not wait for template access")
                .unwrap()
                .is_none()
        );
        sqlx::query(
            "UPDATE requests SET state = 'completed', response_body = 'done', response_status = 200, completed_at = now() WHERE id = $1",
        )
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
        let detail = manager
            .get_terminal_request_detail(crate::request::RequestId(id))
            .await
            .unwrap()
            .expect("completed request must return its payload");
        assert_eq!(detail.response_body.as_deref(), Some("done"));
    }

    #[sqlx::test]
    async fn test_purge_preserves_batchless_request_detail(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let request_id = uuid::Uuid::new_v4();
        manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id,
                body: r#"{"model":"gpt-4","messages":[{"role":"user","content":"test"}]}"#
                    .to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "test-user".to_string(),
            })
            .await
            .unwrap();

        let purged = manager
            .purge_orphaned_rows(1000)
            .await
            .expect("purge should succeed");
        assert_eq!(
            purged, 0,
            "purge must not touch batchless realtime templates"
        );

        let detail = manager
            .get_request_detail(crate::request::RequestId(request_id))
            .await
            .expect("get_request_detail should succeed after purge");

        assert_eq!(detail.model, "gpt-4");
        assert!(
            detail.body.is_some(),
            "body should still be present after purge"
        );
    }

    #[sqlx::test]
    async fn test_get_request_detail_not_found(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let result = manager
            .get_request_detail(crate::request::RequestId(uuid::Uuid::new_v4()))
            .await;

        assert!(result.is_err());
    }


    #[sqlx::test]
    async fn test_create_flex_pending(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let request_id = uuid::Uuid::new_v4();
        manager
            .create_flex(crate::request::CreateFlexInput {
                request_id,
                body: r#"{"model":"gpt-4","input":"hi"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: "user-123".to_string(),
                metadata: None,
            })
            .await
            .expect("create_flex should succeed");

        let detail = manager
            .get_request_detail(crate::request::RequestId(request_id))
            .await
            .expect("request should exist");
        assert_eq!(detail.status, "pending");
        assert_eq!(detail.service_tier, Some("flex".to_string()));
        assert_eq!(detail.batch_id, None);
        assert_eq!(detail.created_by, "user-123");
        assert!(detail.body.is_some());
    }

    #[sqlx::test]
    async fn background_creation_batchless_persists_pending_request(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let request_id = Uuid::new_v4();

        manager
            .create_background(crate::request::CreateBackgroundInput {
                request_id,
                body: r#"{"model":"model-a","input":"hello","service_tier":"flex"}"#.to_string(),
                model: "model-a".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: "user-a".to_string(),
                metadata: None,
            })
            .await
            .expect("background request should be created");

        let detail = manager
            .get_request_detail(crate::request::RequestId(request_id))
            .await
            .unwrap();
        assert_eq!(detail.status, "pending");
        assert_eq!(detail.service_tier.as_deref(), Some("background"));
        assert_eq!(detail.batch_id, None);
        assert_eq!(detail.created_by, "user-a");
        assert!(
            !detail.body.unwrap().contains("service_tier"),
            "routing fields must be stripped from the persisted outbound body"
        );
    }

    #[sqlx::test]
    async fn background_creation_is_exposed_by_its_own_batch_filter(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let created =
            create_background_batch_for_test(&manager, "filter-model", "filter-owner").await;
        let sla = manager
            .create_batch(BatchInput {
                file_id: created.file_id.unwrap(),
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: Some("filter-owner".to_string()),
                api_key_id: None,
                total_requests: Some(1),
            })
            .await
            .unwrap();

        let background = manager
            .list_batches(ListBatchesFilter {
                service_tiers: Some(vec!["background".to_string()]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(background.len(), 1);
        assert_eq!(background[0].id, created.id);
        assert_eq!(background[0].service_tier.as_deref(), Some("background"));
        assert_eq!(background[0].completion_window, None);
        assert_eq!(background[0].expires_at, None);

        let sla_window = manager
            .list_batches(ListBatchesFilter {
                completion_windows: Some(vec!["24h".to_string()]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(sla_window.len(), 1);
        assert_eq!(sla_window[0].id, sla.id);

        let mixed = manager
            .list_batches(ListBatchesFilter {
                completion_windows: Some(vec!["24h".to_string()]),
                service_tiers: Some(vec!["background".to_string()]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(mixed.len(), 2);
        assert!(mixed.iter().any(|batch| batch.id == created.id));
        assert!(mixed.iter().any(|batch| batch.id == sla.id));

        let error = manager
            .list_batches(ListBatchesFilter {
                service_tiers: Some(vec!["unknown".to_string()]),
                ..Default::default()
            })
            .await
            .expect_err("unknown service tiers must be rejected");
        assert!(matches!(error, FusilladeError::ValidationError(_)));
    }

    #[sqlx::test]
    async fn test_trailing_demand_window_validation(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let rows = manager
            .get_completed_request_counts_by_model_and_window(
                &[],
                &[],
                &crate::request::ServiceTierFilter::Any,
            )
            .await
            .unwrap();
        assert!(rows.is_empty());

        for (start, end) in [(-10, 10), (0, 0), (-5, -10), (-5, -5), (5, 10)] {
            let error = manager
                .get_completed_request_counts_by_model_and_window(
                    &[("w".to_string(), start, end)],
                    &[],
                    &crate::request::ServiceTierFilter::Any,
                )
                .await
                .expect_err("trailing window with start >= end or end > 0 must be rejected");
            assert!(matches!(error, FusilladeError::ValidationError(_)));
        }
    }

    #[sqlx::test]
    async fn test_create_realtime_processing(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let request_id = uuid::Uuid::new_v4();
        manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id,
                body: r#"{"model":"gpt-4","input":"hi"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "user-456".to_string(),
            })
            .await
            .expect("create_realtime should succeed");

        let detail = manager
            .get_request_detail(crate::request::RequestId(request_id))
            .await
            .expect("request should exist");
        assert_eq!(detail.status, "processing");
        assert_eq!(detail.service_tier, Some("priority".to_string()));
        assert_eq!(detail.batch_id, None);
        assert_eq!(detail.created_by, "user-456");
    }

    #[sqlx::test]
    async fn test_create_realtime_complete_lifecycle(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let request_id = uuid::Uuid::new_v4();
        manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id,
                body: r#"{"input":"test"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "user-789".to_string(),
            })
            .await
            .expect("create should succeed");

        manager
            .complete_request(
                crate::request::RequestId(request_id),
                r#"{"output":"done"}"#,
                200,
            )
            .await
            .expect("complete should succeed");

        let detail = manager
            .get_request_detail(crate::request::RequestId(request_id))
            .await
            .expect("request should exist");
        assert_eq!(detail.status, "completed");
        assert_eq!(
            detail.response_body,
            Some(r#"{"output":"done"}"#.to_string())
        );
        assert!(detail.completed_at.is_some());
    }

    #[sqlx::test]
    async fn test_create_realtime_fail_lifecycle(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let request_id = uuid::Uuid::new_v4();
        manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id,
                body: r#"{"input":"test"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "user-789".to_string(),
            })
            .await
            .expect("create should succeed");

        manager
            .fail_request(
                crate::request::RequestId(request_id),
                "upstream timeout",
                504,
            )
            .await
            .expect("fail should succeed");

        let detail = manager
            .get_request_detail(crate::request::RequestId(request_id))
            .await
            .expect("request should exist");
        assert_eq!(detail.status, "failed");
        assert!(detail.failed_at.is_some());

        let error: crate::request::FailureReason =
            serde_json::from_str(detail.error.as_ref().unwrap())
                .expect("error should be valid FailureReason JSON");
        assert!(matches!(
            error,
            crate::request::FailureReason::NonRetriableHttpStatus { status: 504, .. }
        ));
    }

    #[sqlx::test]
    async fn test_create_realtime_shows_in_list_requests(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let request_id = uuid::Uuid::new_v4();
        manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id,
                body: r#"{"input":"test"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "user-abc".to_string(),
            })
            .await
            .expect("create should succeed");

        let result = manager
            .list_requests(crate::request::ListRequestsFilter {
                service_tiers: Some(vec!["flex".to_string(), "priority".to_string()]),
                ..Default::default()
            })
            .await
            .expect("list should succeed");

        assert!(result.data.iter().any(|r| r.id == request_id));
        let found = result.data.iter().find(|r| r.id == request_id).unwrap();
        assert_eq!(found.service_tier, Some("priority".to_string()));
        assert_eq!(found.created_by, "user-abc");
    }


    #[sqlx::test]
    async fn test_persist_completed_realtime_batch_empty_is_noop(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        manager
            .persist_completed_realtime_batch(&[])
            .await
            .expect("empty batch should succeed");
    }

    #[sqlx::test]
    async fn test_persist_completed_realtime_batch_inserts_when_no_row(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let started_at = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let completed_at = started_at + chrono::Duration::milliseconds(1500);
        let request_id = uuid::Uuid::new_v4();
        manager
            .persist_completed_realtime_batch(&[crate::request::PersistCompletedRealtimeInput {
                request_id,
                response_body: r#"{"output":"done"}"#.to_string(),
                status_code: 200,
                request_body: r#"{"input":"hi"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "user-1".to_string(),
                started_at,
                completed_at,
            }])
            .await
            .expect("batch should succeed");

        let detail = manager
            .get_request_detail(crate::request::RequestId(request_id))
            .await
            .expect("request should exist");
        assert_eq!(detail.status, "completed");
        assert_eq!(detail.service_tier, Some("priority".to_string()));
        assert_eq!(detail.batch_id, None);
        assert_eq!(detail.created_by, "user-1");
        assert_eq!(
            detail.response_body,
            Some(r#"{"output":"done"}"#.to_string())
        );
        assert_eq!(detail.response_status, Some(200));
        assert!(detail.body.is_some());
        assert_eq!(
            detail.completed_at,
            Some(completed_at),
            "completed_at must be the real completion instant"
        );
        let duration_ms = detail.duration_ms.expect("duration_ms should be populated");
        assert!(
            (duration_ms - 1500.0).abs() < 1.0,
            "duration_ms should be ~1500 (real latency), got {duration_ms}"
        );
        assert_eq!(detail.created_at, started_at);
    }

    #[sqlx::test]
    async fn test_persist_completed_realtime_batch_updates_existing_processing_row(
        pool: sqlx::PgPool,
    ) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let request_id = uuid::Uuid::new_v4();
        manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id,
                body: r#"{"input":"hi"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "user-2".to_string(),
            })
            .await
            .expect("create_realtime should succeed");

        let pre = manager
            .get_request_detail(crate::request::RequestId(request_id))
            .await
            .expect("pre-detail should fetch");
        assert_eq!(pre.status, "processing");
        assert!(pre.completed_at.is_none());

        manager
            .persist_completed_realtime_batch(&[crate::request::PersistCompletedRealtimeInput {
                request_id,
                response_body: r#"{"output":"done"}"#.to_string(),
                status_code: 200,
                request_body: r#"{"input":"this should not be stored"}"#.to_string(),
                model: "this-model-should-be-ignored".to_string(),
                endpoint: String::new(),
                method: String::new(),
                path: String::new(),
                created_by: "this-user-should-be-ignored".to_string(),
                started_at: Utc::now(),
                completed_at: Utc::now(),
            }])
            .await
            .expect("batch should succeed");

        let detail = manager
            .get_request_detail(crate::request::RequestId(request_id))
            .await
            .expect("post-detail should fetch");
        assert_eq!(detail.status, "completed");
        assert_eq!(detail.created_by, "user-2");
        assert_eq!(
            detail.response_body,
            Some(r#"{"output":"done"}"#.to_string())
        );
        assert_eq!(detail.response_status, Some(200));
        assert!(detail.completed_at.is_some());
    }

    #[sqlx::test]
    async fn test_persist_completed_realtime_batch_mixed_insert_and_update(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let existing_id = uuid::Uuid::new_v4();
        manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id: existing_id,
                body: r#"{"input":"bg"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "user-bg".to_string(),
            })
            .await
            .expect("create_realtime should succeed");

        let new_id = uuid::Uuid::new_v4();
        manager
            .persist_completed_realtime_batch(&[
                crate::request::PersistCompletedRealtimeInput {
                    request_id: existing_id,
                    response_body: r#"{"output":"bg done"}"#.to_string(),
                    status_code: 200,
                    request_body: String::new(),
                    model: "ignored".to_string(),
                    endpoint: String::new(),
                    method: String::new(),
                    path: String::new(),
                    created_by: String::new(),
                    started_at: Utc::now(),
                    completed_at: Utc::now(),
                },
                crate::request::PersistCompletedRealtimeInput {
                    request_id: new_id,
                    response_body: r#"{"output":"non-bg done"}"#.to_string(),
                    status_code: 200,
                    request_body: r#"{"input":"non-bg"}"#.to_string(),
                    model: "gpt-4".to_string(),
                    endpoint: "http://localhost:3001/ai".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/responses".to_string(),
                    created_by: "user-nonbg".to_string(),
                    started_at: Utc::now(),
                    completed_at: Utc::now(),
                },
            ])
            .await
            .expect("mixed batch should succeed");

        let existing_detail = manager
            .get_request_detail(crate::request::RequestId(existing_id))
            .await
            .expect("existing row should be readable");
        assert_eq!(existing_detail.status, "completed");
        assert_eq!(existing_detail.created_by, "user-bg");
        assert_eq!(
            existing_detail.response_body,
            Some(r#"{"output":"bg done"}"#.to_string())
        );

        let new_detail = manager
            .get_request_detail(crate::request::RequestId(new_id))
            .await
            .expect("new row should be readable");
        assert_eq!(new_detail.status, "completed");
        assert_eq!(new_detail.created_by, "user-nonbg");
        assert_eq!(
            new_detail.response_body,
            Some(r#"{"output":"non-bg done"}"#.to_string())
        );
    }

    #[sqlx::test]
    async fn test_persist_completed_realtime_batch_idempotent_on_terminal_row(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let request_id = uuid::Uuid::new_v4();
        manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id,
                body: r#"{"input":"first"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "user-3".to_string(),
            })
            .await
            .expect("create should succeed");
        manager
            .complete_request(
                crate::request::RequestId(request_id),
                r#"{"output":"first"}"#,
                200,
            )
            .await
            .expect("first complete should succeed");

        manager
            .persist_completed_realtime_batch(&[crate::request::PersistCompletedRealtimeInput {
                request_id,
                response_body: r#"{"output":"second-should-not-overwrite"}"#.to_string(),
                status_code: 500,
                request_body: r#"{"input":"second"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost:3001/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "user-3".to_string(),
                started_at: Utc::now(),
                completed_at: Utc::now(),
            }])
            .await
            .expect("batch should succeed");

        let detail = manager
            .get_request_detail(crate::request::RequestId(request_id))
            .await
            .expect("request should exist");
        assert_eq!(detail.status, "completed");
        assert_eq!(
            detail.response_body,
            Some(r#"{"output":"first"}"#.to_string())
        );
        assert_eq!(detail.response_status, Some(200));
    }

    #[sqlx::test]
    async fn test_list_requests_excludes_batched_rows(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        );

        let template = RequestTemplateInput {
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/v1/chat/completions".to_string(),
            body: r#"{"model":"gpt-4"}"#.to_string(),
            model: "gpt-4".to_string(),
        };
        let file_id = manager
            .create_file("batched".to_string(), None, vec![template])
            .await
            .unwrap();
        manager
            .create_batch(crate::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1h".to_string(),
                metadata: None,
                created_by: Some("batched-user".to_string()),
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let realtime_id = uuid::Uuid::new_v4();
        manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id: realtime_id,
                body: r#"{"input":"x"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "realtime-user".to_string(),
            })
            .await
            .unwrap();

        let result = manager
            .list_requests(crate::request::ListRequestsFilter::default())
            .await
            .unwrap();
        assert_eq!(result.total_count, 1);
        assert_eq!(result.data[0].id, realtime_id);
    }


    async fn make_realtime(
        manager: &PostgresRequestManager<TestDbPools>,
        user: &str,
    ) -> uuid::Uuid {
        let id = uuid::Uuid::new_v4();
        manager
            .create_realtime(crate::request::CreateRealtimeInput {
                request_id: id,
                body: r#"{"model":"gpt-4","input":"secret prompt"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "http://localhost/ai".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: user.to_string(),
            })
            .await
            .expect("create_realtime should succeed");
        id
    }

    async fn count(pool: &sqlx::PgPool, sql_one_arg: &str, arg: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(sql_one_arg)
            .bind(arg)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[sqlx::test]
    async fn test_bulk_delete_data_erases_batchless_requests_and_templates(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_retained_response_fence_seconds(Some(3_600));

        make_realtime(&manager, "user-A").await;
        make_realtime(&manager, "user-A").await;
        make_realtime(&manager, "user-B").await;

        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM requests WHERE created_by = $1",
                "user-A"
            )
            .await,
            2
        );
        let templates_before: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "c!" FROM request_templates_all WHERE file_id IS NULL"#
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(templates_before, 3);

        let total = manager.bulk_delete_data("user-A", 100).await.unwrap();
        assert_eq!(total, 2, "2 batchless requests erased, no batches/files");

        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM requests WHERE created_by = $1",
                "user-A"
            )
            .await,
            0
        );
        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM requests WHERE created_by = $1",
                "user-B"
            )
            .await,
            1
        );
        let templates_after: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "c!" FROM request_templates_all WHERE file_id IS NULL"#
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(templates_after, 1);
    }

    #[sqlx::test]
    async fn test_bulk_delete_data_soft_deletes_batches_and_files_nullifying_metadata(
        pool: sqlx::PgPool,
    ) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        async fn file_and_batch(
            manager: &PostgresRequestManager<TestDbPools>,
            pool: &sqlx::PgPool,
            user: &str,
        ) -> (Uuid, Uuid) {
            let file_id = manager
                .create_file(
                    format!("{user}.jsonl"),
                    None,
                    vec![RequestTemplateInput {
                        custom_id: None,
                        endpoint: "/v1/chat/completions".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/chat/completions".to_string(),
                        body: "{}".to_string(),
                        model: "test".to_string(),
                    }],
                )
                .await
                .unwrap();
            sqlx::query!(
                "UPDATE files SET uploaded_by = $1 WHERE id = $2",
                user,
                *file_id as Uuid
            )
            .execute(pool)
            .await
            .unwrap();
            let batch = manager
                .create_batch(crate::batch::BatchInput {
                    file_id,
                    endpoint: "/v1/chat/completions".to_string(),
                    completion_window: "24h".to_string(),
                    metadata: None,
                    created_by: Some(user.to_string()),
                    api_key_id: None,
                    total_requests: None,
                })
                .await
                .unwrap();
            sqlx::query!(
                r#"UPDATE batches SET metadata = '{"email":"someone@x.com"}'::jsonb WHERE id = $1"#,
                *batch.id as Uuid
            )
            .execute(pool)
            .await
            .unwrap();
            (*file_id as Uuid, *batch.id as Uuid)
        }

        let (file_a, batch_a) = file_and_batch(&manager, &pool, "user-A").await;
        let (file_b, batch_b) = file_and_batch(&manager, &pool, "user-B").await;

        let total = manager.bulk_delete_data("user-A", 100).await.unwrap();
        let soft_batches = count(
            &pool,
            "SELECT COUNT(*) FROM batches WHERE created_by = $1 AND deleted_at IS NOT NULL",
            "user-A",
        )
        .await;
        let soft_files = count(
            &pool,
            "SELECT COUNT(*) FROM files WHERE uploaded_by = $1 AND deleted_at IS NOT NULL",
            "user-A",
        )
        .await;
        assert_eq!(total, (soft_batches + soft_files) as u64);
        assert!(total >= 2, "at least the batch + input file");
        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM files WHERE uploaded_by = $1 AND deleted_at IS NULL",
                "user-A"
            )
            .await,
            0
        );

        let a_batch: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "c!" FROM batches WHERE id = $1 AND deleted_at IS NOT NULL AND metadata IS NULL AND cancelled_at IS NOT NULL"#,
            batch_a,
        ).fetch_one(&pool).await.unwrap();
        assert_eq!(a_batch, 1);
        let a_file: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "c!" FROM files WHERE id = $1 AND deleted_at IS NOT NULL AND status = 'deleted'"#,
            file_a,
        ).fetch_one(&pool).await.unwrap();
        assert_eq!(a_file, 1);

        let b_untouched: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "c!" FROM batches WHERE id = $1 AND deleted_at IS NULL AND metadata IS NOT NULL"#,
            batch_b,
        ).fetch_one(&pool).await.unwrap();
        assert_eq!(b_untouched, 1);
        let b_file: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "c!" FROM files WHERE id = $1 AND deleted_at IS NULL"#,
            file_b
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(b_file, 1);
    }

    #[sqlx::test]
    async fn test_bulk_delete_data_respects_batch_size_and_drains_to_zero(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        )
        .with_retained_response_fence_seconds(Some(3_600));
        for _ in 0..5 {
            make_realtime(&manager, "user-A").await;
        }

        assert_eq!(manager.bulk_delete_data("user-A", 2).await.unwrap(), 2);
        assert_eq!(manager.bulk_delete_data("user-A", 2).await.unwrap(), 2);
        assert_eq!(manager.bulk_delete_data("user-A", 2).await.unwrap(), 1);
        assert_eq!(
            manager.bulk_delete_data("user-A", 2).await.unwrap(),
            0,
            "idempotent once drained"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM requests WHERE created_by = $1",
                "user-A"
            )
            .await,
            0
        );
    }

    #[sqlx::test]
    async fn test_bulk_delete_data_nonpositive_batch_size_is_noop(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        make_realtime(&manager, "user-A").await;

        assert_eq!(manager.bulk_delete_data("user-A", 0).await.unwrap(), 0);
        assert_eq!(manager.bulk_delete_data("user-A", -5).await.unwrap(), 0);
        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM requests WHERE created_by = $1",
                "user-A"
            )
            .await,
            1
        );
    }

    #[sqlx::test]
    async fn test_bulk_delete_data_leaves_file_templates_for_purge_daemon(pool: sqlx::PgPool) {
        let manager = PostgresRequestManager::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );
        let file_id = manager
            .create_file(
                "a.jsonl".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "/v1/c".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/c".to_string(),
                        body: "{}".to_string(),
                        model: "m".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "/v1/c".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/c".to_string(),
                        body: "{}".to_string(),
                        model: "m".to_string(),
                    },
                ],
            )
            .await
            .unwrap();
        sqlx::query!(
            "UPDATE files SET uploaded_by = $1 WHERE id = $2",
            "user-A",
            *file_id as Uuid
        )
        .execute(&pool)
        .await
        .unwrap();

        let file_templates: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "c!" FROM request_templates_all WHERE file_id = $1"#,
            *file_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(file_templates, 2);

        assert_eq!(manager.bulk_delete_data("user-A", 100).await.unwrap(), 1);
        let still_there: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "c!" FROM request_templates_all WHERE file_id = $1"#,
            *file_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(still_there, 2, "templates linger until the purge pass");

        manager.purge_orphaned_rows(1000).await.unwrap();
        let purged: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "c!" FROM request_templates_all WHERE file_id = $1"#,
            *file_id as Uuid
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(purged, 0, "purge daemon erases the file's templates");
    }
}
