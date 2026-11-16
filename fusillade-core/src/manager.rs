
use crate::batch::{
    BackgroundBatchInput, Batch, BatchId, BatchInput, BatchStatus, File, FileContentItem,
    FileFilter, FileId, FileStreamItem, FileStreamResult, ListBatchesFilter, OutputFileType,
    RequestTemplateInput,
};
use crate::daemon_record::{AnyDaemonRecord, DaemonRecord, DaemonState, DaemonStatus};
use crate::error::Result;
use crate::request::{
    AnyRequest, CascadeTargetState, Claimed, CreateBackgroundInput, CreateFlexInput,
    CreateRealtimeInput, DaemonId, ListRequestsFilter, PersistCompletedRealtimeInput, Request,
    RequestDetail, RequestId, RequestListResult, RequestState, ServiceTierFilter,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::Stream;
use std::collections::HashMap;
use std::pin::Pin;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrailingDemandCount {
    pub model: String,
    pub window_label: String,
    pub service_tier: Option<String>,
    pub outcome: String,
    pub count: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetentionPolicy {
    pub expire_files: bool,
    pub terminal_batch_seconds: Option<u64>,
    pub batchless_seconds_by_service_tier: HashMap<String, u64>,
    pub max_late_writer_seconds: Option<u64>,
}

impl RetentionPolicy {
    pub fn is_enabled(&self) -> bool {
        self.expire_files
            || self.terminal_batch_seconds.is_some()
            || !self.batchless_seconds_by_service_tier.is_empty()
    }

    pub fn batchless_retention_bounds_seconds(&self) -> Option<(u64, u64)> {
        let mut non_background = self
            .batchless_seconds_by_service_tier
            .iter()
            .filter(|(tier, _)| tier.as_str() != "background")
            .map(|(_, seconds)| *seconds);
        let first = non_background.next()?;
        let (min, max) = non_background.fold((first, first), |(min, max), seconds| {
            (min.min(seconds), max.max(seconds))
        });
        Some((min, max))
    }

    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.terminal_batch_seconds == Some(0)
            || self.max_late_writer_seconds == Some(0)
            || self
                .batchless_seconds_by_service_tier
                .values()
                .any(|seconds| *seconds == 0)
        {
            return Err("automated retention periods must be greater than zero".to_string());
        }
        let periods = self
            .terminal_batch_seconds
            .into_iter()
            .chain(self.max_late_writer_seconds)
            .chain(self.batchless_seconds_by_service_tier.values().copied());
        if periods.into_iter().any(|seconds| {
            i64::try_from(seconds)
                .ok()
                .and_then(chrono::Duration::try_seconds)
                .is_none()
        }) {
            return Err("automated retention period is too large".to_string());
        }
        if self
            .batchless_seconds_by_service_tier
            .keys()
            .any(|tier| !matches!(tier.as_str(), "priority" | "flex" | "background"))
        {
            return Err("batchless retention contains an unsupported service tier".to_string());
        }
        if !self.batchless_seconds_by_service_tier.is_empty()
            && self.max_late_writer_seconds.is_none()
        {
            return Err(
                "batchless retention requires an explicit late-writer fence period".to_string(),
            );
        }
        Ok(())
    }

    pub fn delete_on(
        terminal_at: DateTime<Utc>,
        seconds: u64,
    ) -> std::result::Result<chrono::NaiveDate, String> {
        let seconds = i64::try_from(seconds)
            .map_err(|_| "automated retention period is too large".to_string())?;
        let duration = chrono::Duration::try_seconds(seconds)
            .ok_or_else(|| "automated retention period is too large".to_string())?;
        let expiry = terminal_at
            .checked_add_signed(duration)
            .ok_or_else(|| "automated retention expiry is out of range".to_string())?;
        expiry
            .date_naive()
            .succ_opt()
            .ok_or_else(|| "automated retention deletion date is out of range".to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RetainedResponseMaintenanceError {
    #[error("Retained response maintenance is not supported by this storage backend")]
    Disabled,
    #[error("Retained response graph is incomplete")]
    IncompleteGraph,
    #[error("Retained response resurrection fence period is not configured")]
    FencePolicyMissing,
    #[error("Retained response partition maintenance pool is not configured")]
    PartitionMaintenancePoolMissing,
    #[error("Retained response partition retirement identity is inconsistent")]
    RetirementIdentityMismatch,
    #[error("Retained response partition retirement failed")]
    RetirementFailed,
    #[error("Retained response partition retirement is pending")]
    RetirementPending,
}

impl RetainedResponseMaintenanceError {
    pub fn from_fusillade_error(error: &crate::error::FusilladeError) -> Option<Self> {
        match error {
            crate::error::FusilladeError::Other(error) => error.downcast_ref::<Self>().copied(),
            _ => None,
        }
    }

    pub fn into_fusillade_error(self) -> crate::error::FusilladeError {
        crate::error::FusilladeError::Other(anyhow::Error::new(self))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RetainedResponseWriteError {
    #[error("Response is already retained")]
    AlreadyRetained,
    #[error("Response content is no longer available")]
    NotFound,
}

impl RetainedResponseWriteError {
    pub fn from_fusillade_error(error: &crate::error::FusilladeError) -> Option<Self> {
        match error {
            crate::error::FusilladeError::Other(error) => error.downcast_ref::<Self>().copied(),
            _ => None,
        }
    }

    pub fn into_fusillade_error(self) -> crate::error::FusilladeError {
        crate::error::FusilladeError::Other(anyhow::Error::new(self))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetainedResponseArchiveCutoffs {
    observed_at: DateTime<Utc>,
    terminal_before: DateTime<Utc>,
    cancel_grace_before: DateTime<Utc>,
}

impl RetainedResponseArchiveCutoffs {
    pub fn new(
        observed_at: DateTime<Utc>,
        terminal_before: DateTime<Utc>,
        cancel_grace_before: DateTime<Utc>,
    ) -> std::result::Result<Self, String> {
        if terminal_before > observed_at {
            return Err("terminal movement cutoff cannot be after its observation time".to_owned());
        }
        if cancel_grace_before > observed_at {
            return Err(
                "cancellation grace cutoff cannot be after its observation time".to_owned(),
            );
        }
        Ok(Self {
            observed_at,
            terminal_before,
            cancel_grace_before,
        })
    }

    pub fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }

    pub fn terminal_before(&self) -> DateTime<Utc> {
        self.terminal_before
    }

    pub fn cancel_grace_before(&self) -> DateTime<Utc> {
        self.cancel_grace_before
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetainedResponseArchiveOutcome {
    pub groups_archived: u64,
    pub requests_archived: u64,
    pub templates_archived: u64,
    pub bytes_archived: u64,
    pub skipped_locked: bool,
    pub may_have_more: bool,
    pub incomplete_skipped: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetainedResponsePartitionRunway {
    pub created: i64,
    pub contiguous_ahead: i64,
    pub required: i64,
}

impl RetainedResponsePartitionRunway {
    pub fn is_complete(self) -> bool {
        self.required > 0 && self.contiguous_ahead == self.required
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainedResponseRetirementOutcome {
    NoCandidate,
    Retryable,
    Retired,
}

#[cfg(test)]
mod retention_policy_tests {
    use super::*;

    #[test]
    fn retention_bounds_span_non_background_tiers_only() {
        let mut policy = RetentionPolicy::default();
        assert_eq!(policy.batchless_retention_bounds_seconds(), None);

        policy
            .batchless_seconds_by_service_tier
            .insert("background".to_owned(), 86_400);
        assert_eq!(policy.batchless_retention_bounds_seconds(), None);

        policy
            .batchless_seconds_by_service_tier
            .insert("flex".to_owned(), 30 * 86_400);
        policy
            .batchless_seconds_by_service_tier
            .insert("priority".to_owned(), 60 * 86_400);
        assert_eq!(
            policy.batchless_retention_bounds_seconds(),
            Some((30 * 86_400, 60 * 86_400))
        );
    }

    #[test]
    fn batchless_delete_on_is_the_utc_day_after_exact_expiry() {
        let terminal_at = DateTime::parse_from_rfc3339("2026-08-14T15:30:00Z")
            .unwrap()
            .to_utc();
        assert_eq!(
            RetentionPolicy::delete_on(terminal_at, 90 * 86_400).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2026, 11, 13).unwrap()
        );
    }

    #[test]
    fn batchless_delete_on_moves_exact_midnight_expiry_to_the_next_utc_day() {
        let terminal_at = DateTime::parse_from_rfc3339("2026-08-14T00:00:00Z")
            .unwrap()
            .to_utc();
        assert_eq!(
            RetentionPolicy::delete_on(terminal_at, 90 * 86_400).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2026, 11, 13).unwrap()
        );
    }

    #[test]
    fn batchless_delete_on_rejects_timestamp_overflow() {
        assert!(RetentionPolicy::delete_on(DateTime::<Utc>::MAX_UTC, 1).is_err());
    }

    #[test]
    fn batchless_delete_on_rejects_deletion_date_overflow() {
        assert!(RetentionPolicy::delete_on(DateTime::<Utc>::MAX_UTC, 0).is_err());
    }

    struct DefaultDisabledStorage;

    #[async_trait]
    impl DaemonStorage for DefaultDisabledStorage {
        async fn persist_daemon<T: DaemonState + Clone>(
            &self,
            _record: &DaemonRecord<T>,
        ) -> Result<()>
        where
            AnyDaemonRecord: From<DaemonRecord<T>>,
        {
            Ok(())
        }

        async fn get_daemon(&self, _daemon_id: DaemonId) -> Result<AnyDaemonRecord> {
            Err(crate::error::FusilladeError::ValidationError(
                "not used by this test storage".to_string(),
            ))
        }

        async fn list_daemons(
            &self,
            _status_filter: Option<DaemonStatus>,
        ) -> Result<Vec<AnyDaemonRecord>> {
            Ok(Vec::new())
        }

        async fn purge_orphaned_rows(&self, _batch_size: i64) -> Result<u64> {
            Ok(0)
        }

        async fn archive_batch(&self, _batch_id: BatchId) -> Result<ArchiveOutcome> {
            Ok(ArchiveOutcome::SkippedNotFound)
        }

        async fn list_archivable_batches(
            &self,
            _limit: i64,
            _oldest_first: bool,
            _cancel_grace_secs: f64,
            _min_frozen_age_secs: f64,
        ) -> Result<Vec<BatchId>> {
            Ok(Vec::new())
        }

        async fn count_archivable_batches(&self, _cancel_grace_secs: f64) -> Result<i64> {
            Ok(0)
        }

        async fn count_unfrozen_terminal_batches(&self) -> Result<i64> {
            Ok(0)
        }

        async fn finalize_terminal_batches(&self) -> Result<i64> {
            Ok(0)
        }

        async fn finalize_cancelled_batches(&self, _grace_secs: f64, _limit: i64) -> Result<i64> {
            Ok(0)
        }

        async fn ensure_archive_partitions(&self, _weeks_ahead: i32) -> Result<(i64, i64)> {
            Ok((0, 0))
        }

        async fn ensure_request_template_partitions(
            &self,
            _weeks_ahead: i32,
        ) -> Result<(i64, i64)> {
            Ok((0, 0))
        }

        async fn purge_model_filter_events(
            &self,
            _batch_size: i64,
            _keep_per_model: i64,
            _retention_secs: f64,
        ) -> Result<u64> {
            Ok(0)
        }
    }

    fn assert_maintenance_is_disabled<T>(result: Result<T>) {
        let error = match result {
            Ok(_) => panic!("retained response maintenance unexpectedly succeeded"),
            Err(error) => error,
        };
        assert_eq!(
            RetainedResponseMaintenanceError::from_fusillade_error(&error),
            Some(RetainedResponseMaintenanceError::Disabled)
        );
    }

    #[tokio::test]
    async fn retained_response_maintenance_is_disabled_without_backend_opt_in() {
        let storage = DefaultDisabledStorage;
        let policy = RetentionPolicy::default();
        let now = DateTime::parse_from_rfc3339("2026-08-14T15:30:00Z")
            .unwrap()
            .to_utc();
        let cutoffs = RetainedResponseArchiveCutoffs::new(now, now, now).unwrap();

        assert!(!storage.supports_retained_response_lifecycle());
        assert!(!storage.supports_retained_response_partition_retirement());
        assert!(!storage.supports_retained_response_route_cleanup());
        assert_maintenance_is_disabled(storage.retained_response_archive_index_ready().await);

        assert_maintenance_is_disabled(
            storage
                .archive_terminal_batchless_responses(&policy, &cutoffs, 1, 1)
                .await,
        );

        assert_maintenance_is_disabled(
            storage
                .archive_overdue_batchless_responses(&policy, &cutoffs, 1, 1, 1)
                .await,
        );
        assert_maintenance_is_disabled(
            storage
                .ensure_retained_response_partitions(&policy, 1)
                .await,
        );
        assert_maintenance_is_disabled(storage.retire_expired_response_partition(false).await);
        assert_maintenance_is_disabled(storage.cleanup_retained_response_routes(1).await);
        assert_maintenance_is_disabled(storage.cleanup_expired_response_fences(1).await);
        assert_maintenance_is_disabled(
            storage
                .retire_expired_batch_archive_partition(false, 365)
                .await,
        );
    }

    #[test]
    fn retained_response_archive_cutoffs_reject_future_boundaries() {
        let observed_at = DateTime::parse_from_rfc3339("2026-08-14T15:30:00Z")
            .unwrap()
            .to_utc();
        let earlier = observed_at - chrono::Duration::hours(1);
        let later = observed_at + chrono::Duration::nanoseconds(1);

        let cutoffs = RetainedResponseArchiveCutoffs::new(observed_at, earlier, earlier).unwrap();
        assert_eq!(cutoffs.observed_at(), observed_at);
        assert_eq!(cutoffs.terminal_before(), earlier);
        assert_eq!(cutoffs.cancel_grace_before(), earlier);
        assert!(RetainedResponseArchiveCutoffs::new(observed_at, later, earlier).is_err());
        assert!(RetainedResponseArchiveCutoffs::new(observed_at, earlier, later).is_err());
    }

    #[test]
    fn retained_response_error_classifies_an_incomplete_graph_without_payload() {
        let error = RetainedResponseMaintenanceError::IncompleteGraph.into_fusillade_error();
        assert_eq!(
            RetainedResponseMaintenanceError::from_fusillade_error(&error),
            Some(RetainedResponseMaintenanceError::IncompleteGraph)
        );
    }

    #[test]
    fn retention_is_disabled_by_default() {
        let policy = RetentionPolicy::default();
        assert!(!policy.is_enabled());
        assert_eq!(policy.max_late_writer_seconds, None);
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn late_writer_window_is_independent_from_content_retention() {
        let policy = RetentionPolicy {
            max_late_writer_seconds: Some(600),
            ..Default::default()
        };

        assert!(!policy.is_enabled());
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn batchless_retention_requires_an_explicit_late_writer_window() {
        let policy = RetentionPolicy {
            batchless_seconds_by_service_tier: HashMap::from([("flex".to_owned(), 600)]),
            ..Default::default()
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn retention_rejects_ambiguous_or_immediate_rules() {
        for tier in ["", "unknown"] {
            let policy = RetentionPolicy {
                batchless_seconds_by_service_tier: HashMap::from([(tier.to_string(), 1)]),
                ..Default::default()
            };
            assert!(policy.validate().is_err());
        }

        let realtime = RetentionPolicy {
            batchless_seconds_by_service_tier: HashMap::from([("priority".to_string(), 1)]),
            max_late_writer_seconds: Some(1),
            ..Default::default()
        };
        assert!(
            realtime.validate().is_ok(),
            "persisted realtime content must be configurable for retention"
        );

        let batch = RetentionPolicy {
            terminal_batch_seconds: Some(0),
            ..Default::default()
        };
        assert!(batch.validate().is_err());

        let batchless = RetentionPolicy {
            batchless_seconds_by_service_tier: HashMap::from([("flex".to_string(), 0)]),
            max_late_writer_seconds: Some(1),
            ..Default::default()
        };
        assert!(batchless.validate().is_err());

        let oversized = RetentionPolicy {
            terminal_batch_seconds: Some(i64::MAX as u64),
            ..Default::default()
        };
        assert!(oversized.validate().is_err());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveOutcome {
    Archived { rows: u64 },
    SkippedNotFound,
    SkippedNotLive,
    SkippedNotFrozen,
    SkippedNoPartition,
    SkippedRetryRaced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelFilterState {
    Live,
    Coming,
    Leaving,
    Absent,
}

impl ModelFilterState {
    pub fn as_str(self) -> &'static str {
        match self {
            ModelFilterState::Live => "live",
            ModelFilterState::Coming => "coming",
            ModelFilterState::Leaving => "leaving",
            ModelFilterState::Absent => "absent",
        }
    }

    pub fn parse_state(s: &str) -> Option<Self> {
        match s {
            "live" => Some(ModelFilterState::Live),
            "coming" => Some(ModelFilterState::Coming),
            "leaving" => Some(ModelFilterState::Leaving),
            "absent" => Some(ModelFilterState::Absent),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ModelFilter {
    pub model: String,
    pub state: ModelFilterState,
    pub expected_ready_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundClaimKind {
    Batchless,
    Batch,
}

#[async_trait]
pub trait Storage: Send + Sync {
    async fn create_file(
        &self,
        name: String,
        description: Option<String>,
        templates: Vec<RequestTemplateInput>,
    ) -> Result<FileId>;

    async fn create_file_stream<S: Stream<Item = FileStreamItem> + Send + Unpin>(
        &self,
        stream: S,
    ) -> Result<FileStreamResult>;

    async fn get_file(&self, file_id: FileId) -> Result<File>;

    async fn get_file_from_primary_pool(&self, file_id: FileId) -> Result<File>;

    async fn list_files(&self, filter: FileFilter) -> Result<Vec<File>>;

    async fn get_file_content(&self, file_id: FileId) -> Result<Vec<FileContentItem>>;

    fn get_file_content_stream(
        &self,
        file_id: FileId,
        offset: usize,
        search: Option<String>,
    ) -> Pin<Box<dyn Stream<Item = Result<FileContentItem>> + Send>>;

    async fn get_file_template_stats(
        &self,
        file_id: FileId,
    ) -> Result<Vec<crate::batch::ModelTemplateStats>>;

    async fn delete_file(&self, file_id: FileId) -> Result<()>;

    async fn create_batch(&self, input: BatchInput) -> Result<Batch>;

    async fn create_batch_record(&self, input: BatchInput) -> Result<Batch>;

    async fn create_background_batch(&self, input: BackgroundBatchInput) -> Result<Batch> {
        let _ = input;
        Err(crate::error::FusilladeError::Other(anyhow::anyhow!(
            "background batch creation is not implemented for this storage backend"
        )))
    }

    async fn create_background_batch_record(&self, input: BackgroundBatchInput) -> Result<Batch> {
        let _ = input;
        Err(crate::error::FusilladeError::Other(anyhow::anyhow!(
            "background batch creation is not implemented for this storage backend"
        )))
    }

    async fn populate_batch(&self, batch_id: BatchId, file_id: FileId) -> Result<()>;

    async fn get_batch(&self, batch_id: BatchId) -> Result<Batch>;

    async fn get_batch_status(&self, batch_id: BatchId) -> Result<BatchStatus>;

    async fn list_file_batches(&self, file_id: FileId) -> Result<Vec<BatchStatus>>;

    async fn list_batches(&self, filter: ListBatchesFilter) -> Result<Vec<Batch>>;

    async fn get_batch_by_output_file_id(
        &self,
        file_id: FileId,
        file_type: OutputFileType,
    ) -> Result<Option<Batch>>;

    async fn get_batch_requests(&self, batch_id: BatchId) -> Result<Vec<AnyRequest>>;

    fn get_batch_results_stream(
        &self,
        batch_id: BatchId,
        offset: usize,
        search: Option<String>,
        status: Option<String>,
    ) -> Pin<Box<dyn Stream<Item = Result<crate::batch::BatchResultItem>> + Send>>;

    async fn get_cancelled_batch_ids(&self, batch_ids: &[BatchId]) -> Result<Vec<BatchId>>;

    async fn cancel_batch(&self, batch_id: BatchId) -> Result<()>;

    async fn cascade_batch_state_to_requests(
        &self,
        batch_id: BatchId,
        target_state: CascadeTargetState,
    ) -> Result<u64>;

    async fn delete_batch(&self, batch_id: BatchId) -> Result<()>;

    #[deprecated(
        note = "use delete_response_group or delete_owned_response_group to erase the complete logical response graph"
    )]
    async fn delete_request(&self, request_id: RequestId) -> Result<()>;

    async fn delete_response_group(&self, response_id: uuid::Uuid) -> Result<u64> {
        let _ = response_id;
        Err(RetainedResponseMaintenanceError::Disabled.into_fusillade_error())
    }

    async fn delete_owned_response_group(
        &self,
        response_id: uuid::Uuid,
        creator_id: &str,
    ) -> Result<u64> {
        let _ = (response_id, creator_id);
        Err(RetainedResponseMaintenanceError::Disabled.into_fusillade_error())
    }

    async fn bulk_delete_data(&self, creator_id: &str, batch_size: i64) -> Result<u64>;

    async fn retry_failed_requests(
        &self,
        batch_id: BatchId,
        ids: Vec<RequestId>,
    ) -> Result<Vec<Result<()>>>;

    async fn retry_failed_requests_for_batch(&self, batch_id: BatchId) -> Result<u64>;

    async fn get_pending_request_counts_by_model_and_window(
        &self,
        windows: &[(String, Option<i64>, i64)],
        states: &[String],
        model_filter: &[String],
        service_tier_filter: &ServiceTierFilter,
        strict: bool,
    ) -> Result<HashMap<String, HashMap<String, i64>>>;

    async fn get_pending_request_counts_by_model_window_and_tier(
        &self,
        windows: &[(String, Option<i64>, i64)],
        states: &[String],
        model_filter: &[String],
        service_tier_filter: &ServiceTierFilter,
        strict: bool,
    ) -> Result<Vec<TrailingDemandCount>>;

    async fn get_completed_request_counts_by_model_and_window(
        &self,
        windows: &[(String, i64, i64)],
        model_filter: &[String],
        service_tier_filter: &ServiceTierFilter,
    ) -> Result<Vec<TrailingDemandCount>>;

    async fn sum_owner_batch_requests_in_window(
        &self,
        owner: &str,
        completion_window: &str,
        cutoff: DateTime<Utc>,
        strict: bool,
    ) -> Result<i64>;

    async fn count_owner_flex_requests_since(
        &self,
        owner: &str,
        cutoff: DateTime<Utc>,
        strict: bool,
    ) -> Result<i64>;

    async fn count_owner_flex_requests_since_capped(
        &self,
        owner: &str,
        cutoff: DateTime<Utc>,
        strict: bool,
        limit: i64,
    ) -> Result<i64> {
        Ok(self
            .count_owner_flex_requests_since(owner, cutoff, strict)
            .await?
            .min(limit.max(0)))
    }
    #[tracing::instrument(skip(self, ids), fields(count = ids.len()))]
    async fn cancel_requests(&self, ids: Vec<RequestId>) -> Result<Vec<Result<()>>> {
        tracing::debug!(count = ids.len(), "Cancelling requests");

        let mut results = Vec::new();

        for id in ids {
            let get_results = self.get_requests(vec![id]).await?;
            let request_result = get_results.into_iter().next().unwrap();

            let result = match request_result {
                Ok(any_request) => match any_request {
                    AnyRequest::Pending(req) => {
                        req.cancel(self).await?;
                        Ok(())
                    }
                    AnyRequest::Claimed(req) => {
                        req.cancel(self).await?;
                        Ok(())
                    }
                    AnyRequest::Processing(req) => {
                        req.cancel(self).await?;
                        Ok(())
                    }
                    AnyRequest::Completed(_) | AnyRequest::Failed(_) | AnyRequest::Canceled(_) => {
                        Err(crate::error::FusilladeError::InvalidState(
                            id,
                            "terminal state".to_string(),
                            "cancellable state".to_string(),
                        ))
                    }
                },
                Err(e) => Err(e),
            };

            results.push(result);
        }

        Ok(results)
    }

    async fn get_requests(&self, ids: Vec<RequestId>) -> Result<Vec<Result<AnyRequest>>>;


    async fn claim_batchless_requests(
        &self,
        limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
        leak_cooldown: &std::collections::HashSet<(String, String, String)>,
    ) -> Result<Vec<Request<Claimed>>> {
        self.claim_requests(
            limit,
            daemon_id,
            available_capacity,
            user_active_counts,
            leak_cooldown,
        )
        .await
    }

    async fn claim_requests(
        &self,
        limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
        leak_cooldown: &std::collections::HashSet<(String, String, String)>,
    ) -> Result<Vec<Request<Claimed>>>;

    async fn claim_batch_requests(
        &self,
        limit: usize,
        batch_limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
    ) -> Result<Vec<Request<Claimed>>> {
        let _ = (
            limit,
            batch_limit,
            daemon_id,
            available_capacity,
            user_active_counts,
        );
        Err(crate::error::FusilladeError::Other(anyhow::anyhow!(
            "claim_batch_requests is not implemented for this storage backend \
             (override it, or return false from supports_batch_claims to run \
             the daemon request-only)"
        )))
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
        let _ = leak_cooldown;
        self.claim_batch_requests(
            limit,
            batch_limit,
            daemon_id,
            available_capacity,
            user_active_counts,
        )
        .await
    }

    fn supports_batch_claims(&self) -> bool {
        true
    }

    #[doc(hidden)]
    async fn claim_background_requests_by_kind(
        &self,
        kind: BackgroundClaimKind,
        limit: usize,
        batch_limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
    ) -> Result<Vec<Request<Claimed>>> {
        let _ = (
            kind,
            limit,
            batch_limit,
            daemon_id,
            available_capacity,
            user_active_counts,
        );
        Err(crate::error::FusilladeError::Other(anyhow::anyhow!(
            "background claims are not implemented for this storage backend"
        )))
    }

    async fn claim_background_batchless_requests(
        &self,
        limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
    ) -> Result<Vec<Request<Claimed>>> {
        self.claim_background_requests_by_kind(
            BackgroundClaimKind::Batchless,
            limit,
            0,
            daemon_id,
            available_capacity,
            user_active_counts,
        )
        .await
    }

    async fn claim_background_batch_requests(
        &self,
        limit: usize,
        batch_limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
    ) -> Result<Vec<Request<Claimed>>> {
        self.claim_background_requests_by_kind(
            BackgroundClaimKind::Batch,
            limit,
            batch_limit,
            daemon_id,
            available_capacity,
            user_active_counts,
        )
        .await
    }

    async fn claim_background_requests(
        &self,
        limit: usize,
        batch_limit: usize,
        daemon_id: DaemonId,
        available_capacity: &std::collections::HashMap<String, usize>,
        user_active_counts: &std::collections::HashMap<String, usize>,
    ) -> Result<Vec<Request<Claimed>>> {
        let mut claimed = self
            .claim_background_batchless_requests(
                limit,
                daemon_id,
                available_capacity,
                user_active_counts,
            )
            .await?;
        let remaining = limit.saturating_sub(claimed.len());
        let mut remaining_capacity = available_capacity.clone();
        for request in &claimed {
            if let Some(capacity) = remaining_capacity.get_mut(&request.data.model) {
                *capacity = capacity.saturating_sub(1);
            }
        }
        if remaining > 0
            && batch_limit > 0
            && remaining_capacity.values().any(|capacity| *capacity > 0)
        {
            claimed.extend(
                self.claim_background_batch_requests(
                    remaining,
                    batch_limit,
                    daemon_id,
                    &remaining_capacity,
                    user_active_counts,
                )
                .await?,
            );
        }
        Ok(claimed)
    }

    fn supports_background_claims(&self) -> bool {
        false
    }

    async fn append_model_filter_event(&self, entry: &ModelFilter) -> Result<()>;

    async fn append_model_filter_events(&self, entries: &[ModelFilter]) -> Result<()>;

    async fn list_model_filters(&self) -> Result<Vec<ModelFilter>>;

    async fn current_filter_states(
        &self,
    ) -> Result<std::collections::HashMap<String, (ModelFilterState, chrono::DateTime<chrono::Utc>)>>;

    async fn persist<T: RequestState + Clone>(
        &self,
        request: &Request<T>,
    ) -> Result<Option<RequestId>>
    where
        AnyRequest: From<Request<T>>;

    async fn reschedule_for_retry(
        &self,
        request_id: RequestId,
        owner: DaemonId,
        claimed_at: chrono::DateTime<chrono::Utc>,
        retry_attempt: u32,
        not_before: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<bool>;

    async fn list_requests(&self, filter: ListRequestsFilter) -> Result<RequestListResult>;

    async fn get_request_detail(&self, request_id: RequestId) -> Result<RequestDetail>;

    async fn create_realtime(&self, input: CreateRealtimeInput) -> Result<RequestId>;

    async fn create_flex(&self, input: CreateFlexInput) -> Result<RequestId>;

    async fn create_background(&self, input: CreateBackgroundInput) -> Result<RequestId> {
        let _ = input;
        Err(crate::error::FusilladeError::Other(anyhow::anyhow!(
            "background request creation is not implemented for this storage backend"
        )))
    }

    async fn complete_request(
        &self,
        request_id: RequestId,
        response_body: &str,
        status_code: u16,
    ) -> Result<()>;

    async fn fail_request(
        &self,
        request_id: RequestId,
        error: &str,
        status_code: u16,
    ) -> Result<()>;

    async fn persist_completed_realtime_batch(
        &self,
        records: &[PersistCompletedRealtimeInput],
    ) -> Result<()>;
}

#[async_trait]
pub trait DaemonStorage: Send + Sync {
    async fn persist_daemon<T: DaemonState + Clone>(&self, record: &DaemonRecord<T>) -> Result<()>
    where
        AnyDaemonRecord: From<DaemonRecord<T>>;

    async fn get_daemon(&self, daemon_id: DaemonId) -> Result<AnyDaemonRecord>;

    async fn list_daemons(
        &self,
        status_filter: Option<DaemonStatus>,
    ) -> Result<Vec<AnyDaemonRecord>>;

    async fn purge_orphaned_rows(&self, batch_size: i64) -> Result<u64>;

    fn supports_retained_response_lifecycle(&self) -> bool {
        false
    }

    fn supports_retained_response_partition_retirement(&self) -> bool {
        false
    }

    fn supports_retained_response_route_cleanup(&self) -> bool {
        false
    }

    async fn archive_terminal_batchless_responses(
        &self,
        _policy: &RetentionPolicy,
        _cutoffs: &RetainedResponseArchiveCutoffs,
        _max_groups: i64,
        _max_bytes: i64,
    ) -> Result<RetainedResponseArchiveOutcome> {
        Err(RetainedResponseMaintenanceError::Disabled.into_fusillade_error())
    }

    async fn archive_overdue_batchless_responses(
        &self,
        _policy: &RetentionPolicy,
        _cutoffs: &RetainedResponseArchiveCutoffs,
        _max_groups: i64,
        _max_bytes: i64,
        _concurrency: usize,
    ) -> Result<RetainedResponseArchiveOutcome> {
        Err(RetainedResponseMaintenanceError::Disabled.into_fusillade_error())
    }

    async fn retained_response_archive_index_ready(&self) -> Result<bool> {
        Err(RetainedResponseMaintenanceError::Disabled.into_fusillade_error())
    }

    async fn ensure_retained_response_partitions(
        &self,
        _policy: &RetentionPolicy,
        _days_ahead: i32,
    ) -> Result<RetainedResponsePartitionRunway> {
        Err(RetainedResponseMaintenanceError::Disabled.into_fusillade_error())
    }

    async fn retire_expired_response_partition(
        &self,
        _select_new: bool,
    ) -> Result<RetainedResponseRetirementOutcome> {
        Err(RetainedResponseMaintenanceError::Disabled.into_fusillade_error())
    }

    async fn cleanup_retained_response_routes(&self, _limit: i64) -> Result<u64> {
        Err(RetainedResponseMaintenanceError::Disabled.into_fusillade_error())
    }

    async fn retire_expired_batch_archive_partition(
        &self,
        _select_new: bool,
        _retention_days: i32,
    ) -> Result<RetainedResponseRetirementOutcome> {
        Err(RetainedResponseMaintenanceError::Disabled.into_fusillade_error())
    }

    async fn retire_expired_template_partition(
        &self,
        _select_new: bool,
        _retention_days: i32,
    ) -> Result<RetainedResponseRetirementOutcome> {
        Err(RetainedResponseMaintenanceError::Disabled.into_fusillade_error())
    }

    async fn cleanup_retired_template_routes(&self, _limit: i64) -> Result<u64> {
        Err(RetainedResponseMaintenanceError::Disabled.into_fusillade_error())
    }

    async fn cleanup_expired_response_fences(&self, _limit: i64) -> Result<u64> {
        Err(RetainedResponseMaintenanceError::Disabled.into_fusillade_error())
    }

    async fn archive_batch(&self, batch_id: BatchId) -> Result<ArchiveOutcome>;

    async fn list_archivable_batches(
        &self,
        limit: i64,
        oldest_first: bool,
        cancel_grace_secs: f64,
        min_frozen_age_secs: f64,
    ) -> Result<Vec<BatchId>>;

    async fn count_archivable_batches(&self, cancel_grace_secs: f64) -> Result<i64>;

    async fn count_unfrozen_terminal_batches(&self) -> Result<i64>;

    async fn finalize_terminal_batches(&self) -> Result<i64>;

    async fn finalize_cancelled_batches(&self, grace_secs: f64, limit: i64) -> Result<i64>;

    async fn ensure_archive_partitions(&self, weeks_ahead: i32) -> Result<(i64, i64)>;

    async fn ensure_request_template_partitions(&self, weeks_ahead: i32) -> Result<(i64, i64)>;

    async fn purge_model_filter_events(
        &self,
        batch_size: i64,
        keep_per_model: i64,
        retention_secs: f64,
    ) -> Result<u64>;
}
