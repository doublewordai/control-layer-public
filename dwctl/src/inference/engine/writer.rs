
use crate::metrics::errors::component::RESPONSES_WRITER;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use fusillade::{PersistCompletedRealtimeInput, Storage};
use fusillade_arsenal::PostgresRequestManager;
use metrics::{counter, gauge, histogram};
use sqlx_pool_router::PoolProvider;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, warn};
use uuid::Uuid;

const CHANNEL_BUFFER_SIZE: usize = 10_000;

const DEFAULT_MAX_RETRIES: u32 = 3;

const DEFAULT_RETRY_BASE_DELAY_MS: u64 = 100;

#[derive(Debug, Clone)]
pub struct RawCompletedRequest {
    pub request_id: Uuid,
    pub status_code: u16,
    pub response_body: String,
    pub request_body: String,
    pub model: String,
    pub endpoint: String,
    pub created_by: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
}

fn body_bytes(record: &RawCompletedRequest) -> i64 {
    (record.request_body.capacity() + record.response_body.capacity()) as i64
}

#[derive(Debug, Default)]
struct WriterAccounting {
    queued_records: AtomicI64,
    queued_body_bytes: AtomicI64,
    batch_records: AtomicI64,
    batch_body_bytes: AtomicI64,
    publish: Mutex<()>,
}

impl WriterAccounting {
    fn publish_queued(&self) {
        let _publish = self.publish.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        gauge!("dwctl_requests_writer_queued_records").set(self.queued_records.load(Ordering::Relaxed) as f64);
        gauge!("dwctl_requests_writer_queued_body_bytes").set(self.queued_body_bytes.load(Ordering::Relaxed) as f64);
    }

    fn publish_batch(&self) {
        let _publish = self.publish.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        gauge!("dwctl_requests_writer_batch_records").set(self.batch_records.load(Ordering::Relaxed) as f64);
        gauge!("dwctl_requests_writer_batch_body_bytes").set(self.batch_body_bytes.load(Ordering::Relaxed) as f64);
    }
}

fn describe_writer_gauges() {
    metrics::describe_gauge!(
        "dwctl_requests_writer_queued_records",
        "Completed-response records enqueued into the writer channel and not yet taken by the writer task"
    );
    metrics::describe_gauge!(
        "dwctl_requests_writer_queued_body_bytes",
        "String::capacity() of request_body + response_body for records queued in the writer channel"
    );
    metrics::describe_gauge!(
        "dwctl_requests_writer_batch_records",
        "Records taken from the writer channel and held until the flush handling them finishes"
    );
    metrics::describe_gauge!(
        "dwctl_requests_writer_batch_body_bytes",
        "Body bytes of buffered records plus persistence body-clone bytes while the clones are alive"
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Queued,
    Batch,
}

struct AccountedRecord {
    record: Option<RawCompletedRequest>,
    accounting: Arc<WriterAccounting>,
    body_bytes: i64,
    stage: Stage,
}

impl AccountedRecord {
    fn new_queued(record: RawCompletedRequest, accounting: Arc<WriterAccounting>) -> Self {
        let body_bytes = body_bytes(&record);
        accounting.queued_records.fetch_add(1, Ordering::Relaxed);
        accounting.queued_body_bytes.fetch_add(body_bytes, Ordering::Relaxed);
        accounting.publish_queued();
        Self {
            record: Some(record),
            accounting,
            body_bytes,
            stage: Stage::Queued,
        }
    }

    #[cfg(test)]
    fn new_batch(record: RawCompletedRequest, accounting: Arc<WriterAccounting>) -> Self {
        let body_bytes = body_bytes(&record);
        accounting.batch_records.fetch_add(1, Ordering::Relaxed);
        accounting.batch_body_bytes.fetch_add(body_bytes, Ordering::Relaxed);
        accounting.publish_batch();
        Self {
            record: Some(record),
            accounting,
            body_bytes,
            stage: Stage::Batch,
        }
    }

    fn record(&self) -> &RawCompletedRequest {
        self.record.as_ref().expect("accounted record always holds its payload")
    }

    fn into_batch(mut self) -> Self {
        debug_assert_eq!(self.stage, Stage::Queued);
        self.accounting.queued_records.fetch_sub(1, Ordering::Relaxed);
        self.accounting.queued_body_bytes.fetch_sub(self.body_bytes, Ordering::Relaxed);
        self.accounting.batch_records.fetch_add(1, Ordering::Relaxed);
        self.accounting.batch_body_bytes.fetch_add(self.body_bytes, Ordering::Relaxed);
        self.stage = Stage::Batch;
        self.accounting.publish_queued();
        self.accounting.publish_batch();
        self
    }

    fn into_record(mut self) -> RawCompletedRequest {
        self.record.take().expect("accounted record always holds its payload")
    }
}

impl Drop for AccountedRecord {
    fn drop(&mut self) {
        match self.stage {
            Stage::Queued => {
                self.accounting.queued_records.fetch_sub(1, Ordering::Relaxed);
                self.accounting.queued_body_bytes.fetch_sub(self.body_bytes, Ordering::Relaxed);
                self.accounting.publish_queued();
            }
            Stage::Batch => {
                self.accounting.batch_records.fetch_sub(1, Ordering::Relaxed);
                self.accounting.batch_body_bytes.fetch_sub(self.body_bytes, Ordering::Relaxed);
                self.accounting.publish_batch();
            }
        }
    }
}

struct CloneBodyGuard<'a> {
    accounting: &'a WriterAccounting,
    bytes: i64,
}

impl<'a> CloneBodyGuard<'a> {
    fn new(accounting: &'a WriterAccounting) -> Self {
        Self { accounting, bytes: 0 }
    }

    fn add(&mut self, bytes: i64) {
        self.bytes += bytes;
        self.accounting.batch_body_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.accounting.publish_batch();
    }
}

impl Drop for CloneBodyGuard<'_> {
    fn drop(&mut self) {
        if self.bytes != 0 {
            self.accounting.batch_body_bytes.fetch_sub(self.bytes, Ordering::Relaxed);
            self.accounting.publish_batch();
        }
    }
}

#[derive(Clone)]
pub struct RequestsWriterSender {
    sender: mpsc::Sender<AccountedRecord>,
    accounting: Arc<WriterAccounting>,
}

impl RequestsWriterSender {
    #[allow(clippy::result_large_err)]
    pub async fn send(&self, record: RawCompletedRequest) -> Result<(), mpsc::error::SendError<RawCompletedRequest>> {
        let accounted = AccountedRecord::new_queued(record, self.accounting.clone());
        self.sender
            .send(accounted)
            .await
            .map_err(|error| mpsc::error::SendError(error.0.into_record()))
    }

    #[allow(clippy::result_large_err)]
    pub fn try_send(&self, record: RawCompletedRequest) -> Result<(), mpsc::error::TrySendError<RawCompletedRequest>> {
        let accounted = AccountedRecord::new_queued(record, self.accounting.clone());
        self.sender.try_send(accounted).map_err(|error| match error {
            mpsc::error::TrySendError::Full(record) => mpsc::error::TrySendError::Full(record.into_record()),
            mpsc::error::TrySendError::Closed(record) => mpsc::error::TrySendError::Closed(record.into_record()),
        })
    }

    #[cfg(test)]
    pub(crate) fn full_for_test(record: RawCompletedRequest) -> (Self, Box<dyn std::any::Any + Send>) {
        let accounting = Arc::new(WriterAccounting::default());
        let (sender, receiver) = mpsc::channel::<AccountedRecord>(1);
        let sender = Self { sender, accounting };
        sender.try_send(record).expect("an empty single-slot channel accepts one record");
        (sender, Box::new(receiver))
    }

    #[cfg(test)]
    pub(crate) fn queued_records(&self) -> i64 {
        self.accounting.queued_records.load(Ordering::Relaxed)
    }
}

pub struct RequestsWriter<P: PoolProvider + Clone + Send + Sync + 'static> {
    request_manager: Arc<PostgresRequestManager<P>>,
    receiver: mpsc::Receiver<AccountedRecord>,
    batch_size: usize,
    max_retries: u32,
    retry_base_delay: Duration,
    accounting: Arc<WriterAccounting>,
}

impl<P: PoolProvider + Clone + Send + Sync + 'static> RequestsWriter<P> {
    pub fn new(request_manager: Arc<PostgresRequestManager<P>>, batch_size: usize) -> (Self, RequestsWriterSender) {
        let (sender, receiver) = mpsc::channel(CHANNEL_BUFFER_SIZE);
        let accounting = Arc::new(WriterAccounting::default());
        describe_writer_gauges();
        accounting.publish_queued();
        accounting.publish_batch();
        let writer = Self {
            request_manager,
            receiver,
            batch_size: batch_size.max(1),
            max_retries: DEFAULT_MAX_RETRIES,
            retry_base_delay: Duration::from_millis(DEFAULT_RETRY_BASE_DELAY_MS),
            accounting: accounting.clone(),
        };
        let sender = RequestsWriterSender { sender, accounting };
        (writer, sender)
    }

    pub async fn run(mut self, shutdown_token: CancellationToken) {
        info!(
            batch_size = self.batch_size,
            max_retries = self.max_retries,
            "Responses writer started"
        );

        let mut buffer: Vec<AccountedRecord> = Vec::with_capacity(self.batch_size);

        loop {
            tokio::select! {
                biased;

                _ = shutdown_token.cancelled() => {
                    let pre_buffered = buffer.len();
                    info!(pre_buffered, "Shutdown signal received, draining responses writer channel");
                    self.receiver.close();
                    let mut drained = 0usize;
                    while let Some(record) = self.receiver.recv().await {
                        buffer.push(record.into_batch());
                        drained += 1;
                        if buffer.len() >= self.batch_size {
                            self.flush_batch(&mut buffer).await;
                        }
                    }
                    if !buffer.is_empty() {
                        self.flush_batch(&mut buffer).await;
                    }
                    let total_flushed = pre_buffered + drained;
                    counter!("dwctl_requests_writer_shutdown_records_flushed").increment(total_flushed as u64);
                    info!(
                        pre_buffered,
                        drained,
                        total_flushed,
                        "Responses writer shutdown complete"
                    );
                    break;
                }

                maybe_record = self.receiver.recv() => {
                    match maybe_record {
                        Some(record) => buffer.push(record.into_batch()),
                        None => {
                            info!("Responses writer channel closed, shutting down");
                            if !buffer.is_empty() {
                                self.flush_batch(&mut buffer).await;
                            }
                            break;
                        }
                    }
                }
            }

            while buffer.len() < self.batch_size {
                match self.receiver.try_recv() {
                    Ok(record) => buffer.push(record.into_batch()),
                    Err(_) => break,
                }
            }

            gauge!("dwctl_requests_writer_channel_depth").set(self.receiver.len() as f64);
            self.flush_batch(&mut buffer).await;
        }
    }

    async fn flush_batch(&self, buffer: &mut Vec<AccountedRecord>) {
        if buffer.is_empty() {
            return;
        }

        let batch_size = buffer.len();
        let span = info_span!("dwctl.flush_responses_batch", batch_size);

        async {
            let start = Instant::now();
            histogram!("dwctl_requests_writer_flush_size").record(batch_size as f64);

            let mut clone_body_guard = CloneBodyGuard::new(&self.accounting);
            let inputs: Vec<PersistCompletedRealtimeInput> = buffer
                .iter()
                .map(|record| {
                    let record = record.record();
                    PersistCompletedRealtimeInput {
                        request_id: record.request_id,
                        response_body: record.response_body.clone(),
                        status_code: record.status_code,
                        request_body: record.request_body.clone(),
                        model: record.model.clone(),
                        endpoint: String::new(),
                        method: "POST".to_string(),
                        path: record.endpoint.clone(),
                        created_by: record.created_by.clone(),
                        started_at: record.started_at,
                        completed_at: record.completed_at,
                    }
                })
                .collect();
            clone_body_guard.add(
                inputs
                    .iter()
                    .map(|input| (input.request_body.capacity() + input.response_body.capacity()) as i64)
                    .sum(),
            );

            let mut persisted = 0_usize;
            let mut unavailable = 0_usize;
            let mut dropped = 0_usize;
            match self.persist_with_retries(&inputs).await {
                Ok(()) => persisted = inputs.len(),
                Err(error) if retained_write_error(&error) == Some(fusillade::RetainedResponseWriteError::NotFound) && inputs.len() > 1 => {
                    for input in &inputs {
                        match self.persist_with_retries(std::slice::from_ref(input)).await {
                            Ok(()) => persisted += 1,
                            Err(error) if retained_write_error(&error) == Some(fusillade::RetainedResponseWriteError::NotFound) => {
                                unavailable += 1;
                            }
                            Err(error) => {
                                dropped += 1;
                                crate::background_error!(
                                    RESPONSES_WRITER,
                                    "flush_drop",
                                    Error,
                                    error_class = response_write_error_class(&error),
                                    batch_size = 1,
                                    attempts = self.max_retries + 1,
                                    "Failed to flush one response record after retries"
                                );
                            }
                        }
                    }
                }
                Err(error) if retained_write_error(&error) == Some(fusillade::RetainedResponseWriteError::NotFound) => {
                    unavailable = inputs.len();
                }
                Err(error) => {
                    dropped = inputs.len();
                    crate::background_error!(
                        RESPONSES_WRITER,
                        "flush_drop",
                        Error,
                        error_class = response_write_error_class(&error),
                        batch_size,
                        attempts = self.max_retries + 1,
                        "Failed to flush responses batch after retries"
                    );
                }
            }

            let duration = start.elapsed();
            histogram!("dwctl_requests_writer_flush_duration_seconds").record(duration.as_secs_f64());
            counter!("dwctl_requests_writer_records_total").increment(persisted as u64);
            counter!("dwctl_requests_writer_terminal_total", "outcome" => "unavailable").increment(unavailable as u64);

            debug!(
                batch_size,
                persisted,
                unavailable,
                dropped,
                duration_ms = duration.as_millis() as u64,
                "Finished responses batch flush"
            );

            drop(inputs);
            drop(clone_body_guard);
            buffer.clear();
        }
        .instrument(span)
        .await;
    }

    async fn persist_with_retries(&self, inputs: &[PersistCompletedRealtimeInput]) -> Result<(), fusillade::FusilladeError> {
        let batch_size = inputs.len();
        for attempt in 0..=self.max_retries {
            match self.request_manager.persist_completed_realtime_batch(inputs).await {
                Ok(()) => {
                    if attempt > 0 {
                        debug!(attempt, batch_size, "Responses batch flush succeeded after retry");
                        counter!("dwctl_requests_writer_retries_total", "outcome" => "success").increment(1);
                    }
                    return Ok(());
                }
                Err(error) if retained_write_error(&error) == Some(fusillade::RetainedResponseWriteError::NotFound) => {
                    return Err(error);
                }
                Err(error) if attempt < self.max_retries => {
                    let delay = self.retry_base_delay * 2u32.pow(attempt);
                    warn!(
                        error_class = response_write_error_class(&error),
                        attempt = attempt + 1,
                        max_retries = self.max_retries,
                        delay_ms = delay.as_millis() as u64,
                        batch_size,
                        "Responses batch flush failed, retrying"
                    );
                    counter!("dwctl_requests_writer_retries_total", "outcome" => "retry").increment(1);
                    tokio::time::sleep(delay).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("the inclusive retry loop always returns")
    }
}

fn retained_write_error(error: &fusillade::FusilladeError) -> Option<fusillade::RetainedResponseWriteError> {
    fusillade::RetainedResponseWriteError::from_fusillade_error(error)
}

fn response_write_error_class(error: &fusillade::FusilladeError) -> &'static str {
    if retained_write_error(error).is_some() {
        "retained_lifecycle"
    } else {
        match error {
            fusillade::FusilladeError::RequestNotFound(_) => "request_not_found",
            fusillade::FusilladeError::RequestStateConflict { .. } => "state_conflict",
            fusillade::FusilladeError::ValidationError(_) => "validation",
            _ => "storage",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::time::Duration;

    use fusillade::RequestId;
    use fusillade::ReqwestHttpClient;
    use fusillade_arsenal::PostgresRequestManager;
    use sqlx_pool_router::TestDbPools;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::*;
    use crate::{BackgroundShutdownTokens, RESPONSES_WRITER_TASK_NAME};

    #[derive(Clone)]
    struct CaptureWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    impl Write for CaptureWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
        type Writer = CaptureWriter;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    async fn build_writer(
        pool: sqlx::PgPool,
    ) -> (
        RequestsWriter<TestDbPools>,
        RequestsWriterSender,
        Arc<PostgresRequestManager<TestDbPools>>,
    ) {
        let fusillade_pool = crate::test::utils::setup_fusillade_pool(&pool).await;
        let pools = TestDbPools::new(fusillade_pool).await.unwrap();
        let http_client = Arc::new(ReqwestHttpClient::default());
        let manager = Arc::new(PostgresRequestManager::with_client(pools, http_client));
        let (writer, sender) = RequestsWriter::new(manager.clone(), 8);
        (writer, sender, manager)
    }

    async fn wait_until_completed(
        manager: &PostgresRequestManager<TestDbPools>,
        request_id: Uuid,
        timeout_secs: u64,
    ) -> fusillade::RequestDetail {
        let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
        loop {
            match Storage::get_request_detail(manager, RequestId(request_id)).await {
                Ok(detail) if detail.status == "completed" => return detail,
                Ok(_) | Err(fusillade::FusilladeError::RequestNotFound(_)) => {}
                Err(e) => panic!("get_request_detail failed: {e}"),
            }
            if std::time::Instant::now() >= deadline {
                panic!("timed out waiting for request {request_id} to complete");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn test_record(request_id: Uuid) -> RawCompletedRequest {
        let now = Utc::now();
        RawCompletedRequest {
            request_id,
            status_code: 200,
            response_body: r#"{"output":"done"}"#.to_owned(),
            request_body: r#"{"input":"hi"}"#.to_owned(),
            model: "test-model".to_owned(),
            endpoint: "/v1/responses".to_owned(),
            created_by: "test-owner".to_owned(),
            started_at: now,
            completed_at: now,
        }
    }

    fn assert_accounting_zero(accounting: &WriterAccounting) {
        assert_eq!(accounting.queued_records.load(Ordering::Relaxed), 0, "queued_records");
        assert_eq!(accounting.queued_body_bytes.load(Ordering::Relaxed), 0, "queued_body_bytes");
        assert_eq!(accounting.batch_records.load(Ordering::Relaxed), 0, "batch_records");
        assert_eq!(accounting.batch_body_bytes.load(Ordering::Relaxed), 0, "batch_body_bytes");
    }

    #[tokio::test]
    async fn test_accounted_record_moves_queued_to_batch_and_back_to_zero() {
        let accounting = Arc::new(WriterAccounting::default());
        let record = test_record(Uuid::new_v4());
        let expected_bytes = body_bytes(&record);

        let accounted = AccountedRecord::new_queued(record, accounting.clone());
        assert_eq!(accounting.queued_records.load(Ordering::Relaxed), 1);
        assert_eq!(accounting.queued_body_bytes.load(Ordering::Relaxed), expected_bytes);
        assert_eq!(accounting.batch_records.load(Ordering::Relaxed), 0);

        let accounted = accounted.into_batch();
        assert_eq!(accounting.queued_records.load(Ordering::Relaxed), 0);
        assert_eq!(accounting.queued_body_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(accounting.batch_records.load(Ordering::Relaxed), 1);
        assert_eq!(accounting.batch_body_bytes.load(Ordering::Relaxed), expected_bytes);

        drop(accounted);
        assert_accounting_zero(&accounting);
    }

    #[tokio::test]
    async fn test_failed_send_returns_queued_accounting_to_zero() {
        let accounting = Arc::new(WriterAccounting::default());
        let (sender, receiver) = mpsc::channel::<AccountedRecord>(1);
        let sender = RequestsWriterSender {
            sender,
            accounting: accounting.clone(),
        };
        drop(receiver);

        let error = sender
            .send(test_record(Uuid::new_v4()))
            .await
            .expect_err("send to a closed channel must fail");
        let _ = error.0;

        assert_accounting_zero(&accounting);
    }

    #[tokio::test]
    async fn test_try_send_to_a_full_channel_fails_without_waiting() {
        let accounting = Arc::new(WriterAccounting::default());
        let (sender, mut receiver) = mpsc::channel::<AccountedRecord>(1);
        let sender = RequestsWriterSender {
            sender,
            accounting: accounting.clone(),
        };

        sender
            .try_send(test_record(Uuid::new_v4()))
            .expect("an empty channel accepts one record");
        let rejected = sender
            .try_send(test_record(Uuid::new_v4()))
            .expect_err("a full channel must reject the record immediately");
        assert!(matches!(rejected, mpsc::error::TrySendError::Full(_)));
        drop(rejected);
        assert_eq!(accounting.queued_records.load(Ordering::Relaxed), 1);

        let taken = receiver.recv().await.expect("the queued record is still there");
        drop(taken);
        sender
            .try_send(test_record(Uuid::new_v4()))
            .expect("a drained channel accepts a record");
        assert_eq!(accounting.queued_records.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_dropping_receiver_with_queued_records_returns_to_zero() {
        let accounting = Arc::new(WriterAccounting::default());
        let (sender, receiver) = mpsc::channel::<AccountedRecord>(4);
        let sender = RequestsWriterSender {
            sender,
            accounting: accounting.clone(),
        };
        sender.send(test_record(Uuid::new_v4())).await.expect("send should succeed");
        assert_eq!(accounting.queued_records.load(Ordering::Relaxed), 1);

        drop(receiver);
        assert_accounting_zero(&accounting);
    }

    #[tokio::test]
    async fn test_clone_body_guard_accounts_and_releases_copy_bytes() {
        let accounting = WriterAccounting::default();
        {
            let mut guard = CloneBodyGuard::new(&accounting);
            guard.add(128);
            assert_eq!(accounting.batch_body_bytes.load(Ordering::Relaxed), 128);
        }
        assert_eq!(accounting.batch_body_bytes.load(Ordering::Relaxed), 0);
    }

    #[dwctl_test_macros::test]
    async fn test_writer_persists_completed_record(pool: sqlx::PgPool) {
        let (writer, sender, manager) = build_writer(pool).await;
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(writer.run(shutdown.clone()));

        let request_id = Uuid::new_v4();
        let started_at = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let completed_at = started_at + chrono::Duration::seconds(2);
        sender
            .send(RawCompletedRequest {
                request_id,
                status_code: 200,
                response_body: r#"{"output":"done"}"#.to_string(),
                request_body: r#"{"input":"hi"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "/v1/responses".to_string(),
                created_by: "user-test".to_string(),
                started_at,
                completed_at,
            })
            .await
            .expect("send should succeed");

        let detail = wait_until_completed(&manager, request_id, 5).await;
        assert_eq!(detail.status, "completed");
        assert_eq!(detail.service_tier, Some("priority".to_string()));
        assert_eq!(detail.response_body, Some(r#"{"output":"done"}"#.to_string()));
        assert_eq!(detail.response_status, Some(200));
        assert_eq!(detail.completed_at, Some(completed_at));
        let duration_ms = detail.duration_ms.expect("duration_ms should be populated");
        assert!(
            (duration_ms - 2000.0).abs() < 1.0,
            "duration_ms should be ~2000 (real latency), got {duration_ms}"
        );

        shutdown.cancel();
        timeout(Duration::from_secs(5), handle)
            .await
            .expect("writer should shut down within 5s")
            .expect("writer task should not panic");

        assert_accounting_zero(&sender.accounting);
    }

    #[dwctl_test_macros::test]
    async fn test_writer_batches_multiple_records_in_one_flush(pool: sqlx::PgPool) {
        let (writer, sender, manager) = build_writer(pool).await;
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(writer.run(shutdown.clone()));

        let request_ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        for id in &request_ids {
            sender
                .send(RawCompletedRequest {
                    request_id: *id,
                    status_code: 200,
                    response_body: format!(r#"{{"output":"{id}"}}"#),
                    request_body: r#"{"input":"hi"}"#.to_string(),
                    model: "gpt-4".to_string(),
                    endpoint: "/v1/responses".to_string(),
                    created_by: "user-test".to_string(),
                    started_at: Utc::now(),
                    completed_at: Utc::now(),
                })
                .await
                .expect("send should succeed");
        }

        for id in &request_ids {
            let detail = wait_until_completed(&manager, *id, 5).await;
            assert_eq!(detail.status, "completed");
        }

        shutdown.cancel();
        timeout(Duration::from_secs(5), handle)
            .await
            .expect("writer should shut down within 5s")
            .expect("writer task should not panic");
    }

    #[dwctl_test_macros::test]
    async fn test_writer_splits_unavailable_record_without_losing_valid_sibling(pool: sqlx::PgPool) {
        let fusillade_pool = crate::test::utils::setup_fusillade_pool(&pool).await;
        let unavailable_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO retained_response_resurrection_fences \
             (object_id, reason, expires_at) \
             VALUES ($1, 'erased', NOW() + INTERVAL '1 hour')",
        )
        .bind(unavailable_id)
        .execute(&fusillade_pool)
        .await
        .unwrap();
        let (writer, _sender, manager) = build_writer(pool).await;
        let valid_id = Uuid::new_v4();
        let now = Utc::now();
        let record = |request_id| RawCompletedRequest {
            request_id,
            status_code: 200,
            response_body: r#"{"output":"done"}"#.to_owned(),
            request_body: r#"{"input":"safe"}"#.to_owned(),
            model: "test-model".to_owned(),
            endpoint: "/v1/responses".to_owned(),
            created_by: "test-owner".to_owned(),
            started_at: now,
            completed_at: now,
        };
        let mut records = vec![record(unavailable_id), record(valid_id)]
            .into_iter()
            .map(|record| AccountedRecord::new_batch(record, writer.accounting.clone()))
            .collect::<Vec<_>>();

        writer.flush_batch(&mut records).await;

        assert!(records.is_empty());
        assert_accounting_zero(&writer.accounting);
        let valid = manager
            .get_request_detail(RequestId(valid_id))
            .await
            .expect("the valid sibling must persist after batch splitting");
        assert_eq!(valid.status, "completed");
        assert!(matches!(
            manager.get_request_detail(RequestId(unavailable_id)).await,
            Err(fusillade::FusilladeError::RequestNotFound(_))
        ));
    }

    #[dwctl_test_macros::test]
    async fn test_writer_failure_logs_are_content_free(pool: sqlx::PgPool) {
        const REQUEST_SENTINEL: &str = "private-request-payload-b83b1a";
        const RESPONSE_SENTINEL: &str = "private-response-payload-66f319";
        const MODEL_SENTINEL: &str = "private-model-c13fa2";
        const OWNER_SENTINEL: &str = "private-owner-d34fb4";
        const DATABASE_SENTINEL: &str = "raw-database-error-566e2d";
        const POSITIVE_CONTROL: &str = "writer-log-capture-positive-control";

        let (mut writer, _sender, _manager) = build_writer(pool.clone()).await;
        writer.max_retries = 0;

        sqlx::query(
            r#"
            CREATE OR REPLACE FUNCTION fusillade.reject_writer_fixture()
            RETURNS trigger LANGUAGE plpgsql AS $function$
            BEGIN
                RAISE EXCEPTION 'raw-database-error-566e2d';
            END
            $function$
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TRIGGER reject_writer_fixture
            BEFORE INSERT ON fusillade.request_templates_g2
            FOR EACH ROW EXECUTE FUNCTION fusillade.reject_writer_fixture()
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let log_bytes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .with_writer(CaptureWriter(log_bytes.clone()))
            .finish();
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);
        tracing::info!(POSITIVE_CONTROL);

        let request_id = Uuid::new_v4();
        let now = Utc::now();
        let mut records = vec![RawCompletedRequest {
            request_id,
            status_code: 200,
            response_body: RESPONSE_SENTINEL.to_owned(),
            request_body: REQUEST_SENTINEL.to_owned(),
            model: MODEL_SENTINEL.to_owned(),
            endpoint: "/v1/responses".to_owned(),
            created_by: OWNER_SENTINEL.to_owned(),
            started_at: now,
            completed_at: now,
        }]
        .into_iter()
        .map(|record| AccountedRecord::new_batch(record, writer.accounting.clone()))
        .collect::<Vec<_>>();
        writer.flush_batch(&mut records).await;

        assert_accounting_zero(&writer.accounting);

        let logs = String::from_utf8(log_bytes.lock().unwrap().clone()).unwrap();
        assert!(logs.contains(POSITIVE_CONTROL), "the capture subscriber must be active");
        assert!(logs.contains("Failed to flush responses batch after retries"));
        for secret in [
            REQUEST_SENTINEL,
            RESPONSE_SENTINEL,
            MODEL_SENTINEL,
            OWNER_SENTINEL,
            DATABASE_SENTINEL,
        ] {
            assert!(!logs.contains(secret), "sensitive value leaked into tracing output");
        }
        assert!(
            !logs.contains(&request_id.to_string()),
            "request identity leaked into tracing output"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_writer_accepts_completion_while_http_server_drains(pool: sqlx::PgPool) {
        let (writer, sender, manager) = build_writer(pool).await;
        let shutdown = BackgroundShutdownTokens::new(CancellationToken::new());
        let writer_shutdown = shutdown.after_http_drain();
        let handle = tokio::spawn(writer.run(writer_shutdown.clone()));

        shutdown.begin_http_drain();
        assert!(shutdown.task_is_shutting_down("ordinary-background-service"));
        assert!(!writer_shutdown.is_cancelled());
        assert!(!shutdown.task_is_shutting_down(RESPONSES_WRITER_TASK_NAME));

        let request_id = Uuid::new_v4();
        sender
            .send(test_record(request_id))
            .await
            .expect("completion should be accepted during HTTP drain");

        let detail = wait_until_completed(&manager, request_id, 5).await;
        assert_eq!(detail.status, "completed");

        shutdown.finish_http_drain();
        assert!(shutdown.task_is_shutting_down(RESPONSES_WRITER_TASK_NAME));
        timeout(Duration::from_secs(5), handle)
            .await
            .expect("writer should shut down after HTTP drain")
            .expect("writer task should not panic");
        assert_accounting_zero(&sender.accounting);
    }

    #[dwctl_test_macros::test]
    async fn test_writer_drains_channel_on_shutdown(pool: sqlx::PgPool) {
        let (writer, sender, manager) = build_writer(pool).await;
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(writer.run(shutdown.clone()));

        let request_id = Uuid::new_v4();
        sender
            .send(RawCompletedRequest {
                request_id,
                status_code: 200,
                response_body: r#"{"output":"shutdown-test"}"#.to_string(),
                request_body: r#"{"input":"hi"}"#.to_string(),
                model: "gpt-4".to_string(),
                endpoint: "/v1/responses".to_string(),
                created_by: "user-test".to_string(),
                started_at: Utc::now(),
                completed_at: Utc::now(),
            })
            .await
            .expect("send should succeed");

        shutdown.cancel();

        timeout(Duration::from_secs(5), handle)
            .await
            .expect("writer should shut down within 5s")
            .expect("writer task should not panic");

        let detail = Storage::get_request_detail(&*manager, RequestId(request_id))
            .await
            .expect("request should exist after drain");
        assert_eq!(detail.status, "completed");
        assert_eq!(detail.response_body, Some(r#"{"output":"shutdown-test"}"#.to_string()));

        assert_accounting_zero(&sender.accounting);

        drop(sender);
    }
}
