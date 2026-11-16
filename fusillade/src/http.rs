
use crate::error::Result;
pub use crate::request::HttpResponse;
use crate::request::RequestData;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use opentelemetry::trace::TraceContextExt;
use std::sync::Arc;
use std::time::Duration;
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[async_trait]
pub trait HttpClient: Send + Sync + Clone {
    async fn execute(&self, request: &RequestData, api_key: &str) -> Result<HttpResponse>;
}


#[derive(Clone)]
pub struct ReqwestHttpClient {
    client: reqwest::Client,
    first_chunk_timeout: Duration,
    chunk_timeout: Duration,
    body_timeout: Duration,
    upload_stall_timeout: Duration,
    upload_chunk_bytes: usize,
    upload_stall_poll: Duration,
}

pub(crate) const DEFAULT_UPLOAD_STALL_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) const DEFAULT_UPLOAD_CHUNK_BYTES: usize = 64 * 1024;

pub(crate) const DEFAULT_UPLOAD_STALL_POLL: Duration = Duration::from_millis(100);

impl ReqwestHttpClient {
    pub fn new(
        first_chunk_timeout: Duration,
        chunk_timeout: Duration,
        body_timeout: Duration,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            first_chunk_timeout,
            chunk_timeout,
            body_timeout,
            upload_stall_timeout: DEFAULT_UPLOAD_STALL_TIMEOUT,
            upload_chunk_bytes: DEFAULT_UPLOAD_CHUNK_BYTES,
            upload_stall_poll: DEFAULT_UPLOAD_STALL_POLL,
        }
    }

    pub fn with_upload_stall_timeout(mut self, timeout: Duration) -> Self {
        self.upload_stall_timeout = timeout;
        self
    }

    pub fn with_upload_chunk_bytes(mut self, chunk_bytes: usize) -> Self {
        assert!(
            chunk_bytes > 0,
            "upload chunk size must be greater than zero"
        );
        self.upload_chunk_bytes = chunk_bytes;
        self
    }

    pub fn with_upload_stall_poll_interval(mut self, interval: Duration) -> Self {
        assert!(
            !interval.is_zero(),
            "upload stall poll interval must be greater than zero"
        );
        self.upload_stall_poll = interval;
        self
    }
}

impl Default for ReqwestHttpClient {
    fn default() -> Self {
        Self::new(ONE_DAY_DURATION, ONE_DAY_DURATION, ONE_DAY_DURATION)
    }
}

const ONE_DAY_DURATION: Duration = Duration::from_secs(86_400);

fn map_reqwest_error(error: reqwest::Error) -> crate::error::FusilladeError {
    if error.is_builder() {
        crate::error::FusilladeError::HttpRequestBuilder(error.to_string())
    } else if error.is_timeout() {
        crate::error::FusilladeError::HttpClientTimeout(error.to_string())
    } else {
        crate::error::FusilladeError::HttpClient(error.to_string())
    }
}

#[async_trait]
impl HttpClient for ReqwestHttpClient {
    #[tracing::instrument(name = "fusillade.execute", skip(self, request, api_key), fields(
        otel.name = %format!("{} {}", request.method, request.path),
        x_request_id = tracing::field::Empty,
    ))]
    async fn execute(&self, request: &RequestData, api_key: &str) -> Result<HttpResponse> {
        let url = format!("{}{}", request.endpoint, request.path);
        let span = tracing::Span::current();
        span.set_attribute("otel.kind", "Client");
        span.set_attribute("http.request.method", request.method.clone());
        span.set_attribute("url.path", request.path.clone());
        span.set_attribute("url.full", url.clone());

        tracing::debug!(
            url.full = %url,
            upload_stall_timeout_ms = self.upload_stall_timeout.as_millis() as u64,
            upload_chunk_bytes = self.upload_chunk_bytes,
            upload_stall_poll_ms = self.upload_stall_poll.as_millis() as u64,
            first_chunk_timeout_ms = self.first_chunk_timeout.as_millis() as u64,
            chunk_timeout_ms = self.chunk_timeout.as_millis() as u64,
            body_timeout_ms = self.body_timeout.as_millis() as u64,
            "Executing HTTP request"
        );

        let mut req = self.client.request(
            request.method.parse().map_err(|e| {
                tracing::error!(method = %request.method, error = %e, "Invalid HTTP method");
                anyhow::anyhow!("Invalid HTTP method '{}': {}", request.method, e)
            })?,
            &url,
        );

        if !api_key.is_empty() {
            req = req.header("Authorization", format!("Bearer {}", api_key));
            tracing::trace!(request_id = %request.id, "Added Authorization header");
        }

        req = req.header("X-Fusillade-Request-Id", request.id.0.to_string());

        for (key, value) in &request.batch_metadata {
            let header_name = format!("x-fusillade-batch-{}", key.replace('_', "-"));
            req = req.header(&header_name, value);
        }

        if let Some(custom_id) = &request.custom_id {
            req = req.header("X-Fusillade-Custom-Id", custom_id.clone());
            tracing::trace!(request_id = %request.id, custom_id = %custom_id, "Added X-Fusillade-Custom-Id header");
        }

        let ctx = tracing::Span::current().context();
        let span_ref = ctx.span();
        let span_ctx = span_ref.span_context();
        if span_ctx.is_valid() {
            let traceparent = format!(
                "00-{}-{}-{:02x}",
                span_ctx.trace_id(),
                span_ctx.span_id(),
                span_ctx.trace_flags().to_u8()
            );
            req = req.header("traceparent", &traceparent);
            tracing::trace!(request_id = %request.id, traceparent = %traceparent, "Added traceparent header for distributed tracing");
        }

        let mut upload: Option<Arc<UploadProgress>> = None;
        let method_upper = request.method.to_uppercase();
        if method_upper != "GET"
            && method_upper != "HEAD"
            && method_upper != "DELETE"
            && !request.body.is_empty()
        {
            let body = bytes::Bytes::from(request.body.clone().into_bytes());
            let progress = UploadProgress::new(body.len() as u64);
            req = req
                .header("Content-Type", "application/json")
                .body(reqwest::Body::wrap(ProgressBody {
                    remaining: body,
                    progress: progress.clone(),
                    chunk_bytes: self.upload_chunk_bytes,
                }));
            upload = Some(progress);
            tracing::trace!(
                request_id = %request.id,
                body_len = request.body.len(),
                "Added request body"
            );
        }

        self.execute_non_streaming(request, req, &url, upload).await
    }
}

struct UploadProgress {
    started: std::time::Instant,
    last_progress_ms: std::sync::atomic::AtomicU64,
    sent: std::sync::atomic::AtomicU64,
    total: u64,
}

impl UploadProgress {
    fn new(total: u64) -> Arc<Self> {
        Arc::new(Self {
            started: std::time::Instant::now(),
            last_progress_ms: std::sync::atomic::AtomicU64::new(0),
            sent: std::sync::atomic::AtomicU64::new(0),
            total,
        })
    }

    fn record(&self, bytes: usize) {
        use std::sync::atomic::Ordering;
        self.sent.fetch_add(bytes as u64, Ordering::Relaxed);
        self.last_progress_ms
            .store(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    fn is_complete(&self) -> bool {
        self.sent_bytes() >= self.total
    }

    fn stalled_for(&self) -> Duration {
        let last = Duration::from_millis(
            self.last_progress_ms
                .load(std::sync::atomic::Ordering::Relaxed),
        );
        self.started.elapsed().saturating_sub(last)
    }

    fn sent_bytes(&self) -> u64 {
        self.sent.load(std::sync::atomic::Ordering::Relaxed)
    }
}

struct ProgressBody {
    remaining: bytes::Bytes,
    progress: Arc<UploadProgress>,
    chunk_bytes: usize,
}

struct TrackedUploadChunk {
    bytes: bytes::Bytes,
    progress: Arc<UploadProgress>,
}

impl AsRef<[u8]> for TrackedUploadChunk {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

impl Drop for TrackedUploadChunk {
    fn drop(&mut self) {
        self.progress.record(self.bytes.len());
    }
}

impl http_body::Body for ProgressBody {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<std::result::Result<http_body::Frame<Self::Data>, Self::Error>>>
    {
        let this = self.get_mut();
        if this.remaining.is_empty() {
            return std::task::Poll::Ready(None);
        }
        let take = this.remaining.len().min(this.chunk_bytes);
        let chunk = this.remaining.split_to(take);
        let tracked = bytes::Bytes::from_owner(TrackedUploadChunk {
            bytes: chunk,
            progress: this.progress.clone(),
        });
        std::task::Poll::Ready(Some(Ok(http_body::Frame::data(tracked))))
    }

    fn is_end_stream(&self) -> bool {
        self.remaining.is_empty()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.remaining.len() as u64)
    }
}

async fn race_upload_stall<T>(
    send: impl std::future::Future<Output = T>,
    upload: Option<Arc<UploadProgress>>,
    stall_timeout: Duration,
    poll_interval: Duration,
    url: &str,
) -> Result<T> {
    let Some(progress) = upload else {
        return Ok(send.await);
    };
    tokio::pin!(send);
    loop {
        tokio::select! {
            out = &mut send => return Ok(out),
            _ = tokio::time::sleep(poll_interval) => {
                if progress.is_complete() {
                    return Ok(send.await);
                }
                if progress.stalled_for() >= stall_timeout {
                    return Err(crate::error::FusilladeError::UploadStallTimeout(format!(
                        "request upload to {} stalled: {} of {} bytes accepted by the transport writer, no progress for {}ms",
                        url,
                        progress.sent_bytes(),
                        progress.total,
                        stall_timeout.as_millis(),
                    )));
                }
            }
        }
    }
}

pub(crate) fn submission_time(request: &RequestData) -> Option<DateTime<Utc>> {
    request
        .batch_metadata
        .get("created_at")?
        .parse::<DateTime<Utc>>()
        .ok()
}

fn record_submission_ttft(request: &RequestData, status: u16) {
    if let Some((seconds, completion_window)) = submission_ttft_sample(request, status, Utc::now())
    {
        metrics::histogram!(
            "fusillade_request_time_to_first_token_seconds",
            "model" => request.model.clone(),
            "completion_window" => completion_window,
        )
        .record(seconds);
    }
}

fn submission_ttft_sample(
    request: &RequestData,
    status: u16,
    now: DateTime<Utc>,
) -> Option<(f64, String)> {
    if !(200..300).contains(&status) {
        return None;
    }
    let created_at = submission_time(request)?;
    let elapsed_ms = (now - created_at).num_milliseconds();
    if elapsed_ms < 0 {
        return None;
    }
    let completion_window = request
        .batch_metadata
        .get("completion_window")
        .cloned()
        .unwrap_or_default();
    Some((elapsed_ms as f64 / 1000.0, completion_window))
}

impl ReqwestHttpClient {
    async fn execute_non_streaming(
        &self,
        request: &RequestData,
        req: reqwest::RequestBuilder,
        url: &str,
        upload: Option<Arc<UploadProgress>>,
    ) -> Result<HttpResponse> {
        let total_timeout = self.first_chunk_timeout + self.body_timeout;
        let response = race_upload_stall(
            req.timeout(total_timeout).send(),
            upload,
            self.upload_stall_timeout,
            self.upload_stall_poll,
            url,
        )
        .await
        .inspect_err(|e| {
            tracing::error!(
                request_id = %request.id,
                url.full = %url,
                error = %e,
                "HTTP request upload stalled"
            );
        })?
        .map_err(|e| {
            if e.is_builder() {
                tracing::error!(
                    request_id = %request.id,
                    url.full = %url,
                    error = %e.to_string(),
                    custom_id = ?request.custom_id,
                    batch_metadata_keys = ?request.batch_metadata.keys().collect::<Vec<_>>(),
                    "Failed to build HTTP request (not retriable) - likely invalid header value"
                );
            } else {
                tracing::error!(
                    request_id = %request.id,
                    url.full = %url,
                    error = %e,
                    "HTTP request failed"
                );
            }
            map_reqwest_error(e)
        })?;

        let status = response.status().as_u16();
        let x_request_id = response
            .headers()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        if let Some(id) = &x_request_id {
            tracing::Span::current().record("x_request_id", tracing::field::display(id));
        }

        record_submission_ttft(request, status);
        let body = response.text().await.map_err(|e| {
            tracing::warn!(
                request_id = %request.id,
                status = status,
                error = %e,
                "HTTP response body read failed"
            );
            map_reqwest_error(e)
        })?;

        tracing::debug!(
            request_id = %request.id,
            status = status,
            response_len = body.len(),
            "HTTP request completed"
        );

        Ok(HttpResponse {
            status,
            body,
            x_request_id,
        })
    }
}


use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::oneshot;

#[derive(Clone)]
pub struct MockHttpClient {
    responses: Arc<Mutex<HashMap<String, Vec<MockResponse>>>>,
    calls: Arc<Mutex<Vec<MockCall>>>,
    in_flight: Arc<AtomicUsize>,
}

enum MockResponse {
    Immediate(Result<HttpResponse>),
    Triggered {
        response: Result<HttpResponse>,
        trigger: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    },
}

#[derive(Debug, Clone)]
pub struct MockCall {
    pub method: String,
    pub endpoint: String,
    pub path: String,
    pub body: String,
    pub api_key: String,
    pub batch_metadata: std::collections::HashMap<String, String>,
}

impl MockHttpClient {
    pub fn new() -> Self {
        Self {
            responses: Arc::new(Mutex::new(HashMap::new())),
            calls: Arc::new(Mutex::new(Vec::new())),
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn add_response(&self, key: &str, response: Result<HttpResponse>) {
        self.responses
            .lock()
            .entry(key.to_string())
            .or_default()
            .push(MockResponse::Immediate(response));
    }

    pub fn add_response_with_trigger(
        &self,
        key: &str,
        response: Result<HttpResponse>,
    ) -> oneshot::Sender<()> {
        let (tx, rx) = oneshot::channel();
        self.responses
            .lock()
            .entry(key.to_string())
            .or_default()
            .push(MockResponse::Triggered {
                response,
                trigger: Arc::new(Mutex::new(Some(rx))),
            });
        tx
    }

    pub fn get_calls(&self) -> Vec<MockCall> {
        self.calls.lock().clone()
    }

    pub fn clear_calls(&self) {
        self.calls.lock().clear();
    }

    pub fn call_count(&self) -> usize {
        self.calls.lock().len()
    }

    pub fn in_flight_count(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }
}

impl Default for MockHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HttpClient for MockHttpClient {
    async fn execute(&self, request: &RequestData, api_key: &str) -> Result<HttpResponse> {
        self.in_flight.fetch_add(1, Ordering::SeqCst);

        let in_flight = self.in_flight.clone();
        let _guard = InFlightGuard { in_flight };

        self.calls.lock().push(MockCall {
            method: request.method.clone(),
            endpoint: request.endpoint.clone(),
            path: request.path.clone(),
            body: request.body.clone(),
            api_key: api_key.to_string(),
            batch_metadata: request.batch_metadata.clone(),
        });

        let key = format!("{} {}", request.method, request.path);
        let mock_response = {
            let mut responses = self.responses.lock();
            if let Some(response_queue) = responses.get_mut(&key) {
                if !response_queue.is_empty() {
                    Some(response_queue.remove(0))
                } else {
                    None
                }
            } else {
                None
            }
        };

        match mock_response {
            Some(MockResponse::Immediate(response)) => response,
            Some(MockResponse::Triggered { response, trigger }) => {
                let rx = {
                    let mut trigger_guard = trigger.lock();
                    trigger_guard.take()
                };

                if let Some(rx) = rx {
                    let _ = rx.await;
                }

                response
            }
            None => {
                Err(crate::error::FusilladeError::Other(anyhow::anyhow!(
                    "No mock response configured for {} {}",
                    request.method,
                    request.path
                )))
            }
        }
    }
}

struct InFlightGuard {
    in_flight: Arc<AtomicUsize>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::RequestId;

    fn ttft_test_request(metadata: &[(&str, &str)]) -> RequestData {
        RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: None,
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: "{}".to_string(),
            model: "test-model".to_string(),
            api_key: "test-key".to_string(),
            created_by: String::new(),
            batch_metadata: metadata
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn submission_ttft_sample_gates_and_measures() {
        let now: DateTime<Utc> = "2026-07-23T12:00:30Z".parse().unwrap();
        let with_meta = ttft_test_request(&[
            ("created_at", "2026-07-23T12:00:00.000000Z"),
            ("completion_window", "1h"),
        ]);

        let (seconds, window) =
            submission_ttft_sample(&with_meta, 200, now).expect("should record");
        assert!((seconds - 30.0).abs() < 0.001, "elapsed = {seconds}");
        assert_eq!(window, "1h");

        assert_eq!(submission_ttft_sample(&with_meta, 429, now), None);
        assert_eq!(submission_ttft_sample(&with_meta, 500, now), None);

        assert_eq!(
            submission_ttft_sample(&ttft_test_request(&[]), 200, now),
            None
        );
        assert_eq!(
            submission_ttft_sample(
                &ttft_test_request(&[("created_at", "not a time")]),
                200,
                now
            ),
            None
        );

        let future = ttft_test_request(&[("created_at", "2026-07-23T12:05:00Z")]);
        assert_eq!(submission_ttft_sample(&future, 200, now), None);

        let no_window = ttft_test_request(&[("created_at", "2026-07-23T12:00:00Z")]);
        let (_, window) = submission_ttft_sample(&no_window, 200, now).unwrap();
        assert_eq!(window, "");
    }

    #[test]
    fn upload_completes_only_after_transport_releases_final_chunk() {
        use http_body::Body as _;

        let progress = UploadProgress::new(3);
        let mut body = std::pin::pin!(ProgressBody {
            remaining: bytes::Bytes::from_static(b"abc"),
            progress: progress.clone(),
            chunk_bytes: DEFAULT_UPLOAD_CHUNK_BYTES,
        });
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());

        let frame = match body.as_mut().poll_frame(&mut context) {
            std::task::Poll::Ready(Some(Ok(frame))) => frame.into_data().unwrap(),
            other => panic!("expected a data frame, got {other:?}"),
        };

        assert_eq!(progress.sent_bytes(), 0);
        drop(frame);
        assert_eq!(progress.sent_bytes(), 3);
    }

    #[test]
    fn configurable_upload_chunk_size_controls_progress_frames() {
        use http_body::Body as _;

        let progress = UploadProgress::new(6);
        let mut body = std::pin::pin!(ProgressBody {
            remaining: bytes::Bytes::from_static(b"abcdef"),
            progress: progress.clone(),
            chunk_bytes: 3,
        });
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());

        let first = match body.as_mut().poll_frame(&mut context) {
            std::task::Poll::Ready(Some(Ok(frame))) => frame.into_data().unwrap(),
            other => panic!("expected first data frame, got {other:?}"),
        };
        assert_eq!(first.len(), 3);
        drop(first);
        assert_eq!(progress.sent_bytes(), 3);

        let second = match body.as_mut().poll_frame(&mut context) {
            std::task::Poll::Ready(Some(Ok(frame))) => frame.into_data().unwrap(),
            other => panic!("expected second data frame, got {other:?}"),
        };
        assert_eq!(second.len(), 3);
        drop(second);
        assert_eq!(progress.sent_bytes(), 6);
    }

    #[test]
    fn upload_watchdog_client_configuration_has_defaults_and_overrides() {
        let client = ReqwestHttpClient::new(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        assert_eq!(client.upload_chunk_bytes, 64 * 1024);
        assert_eq!(client.upload_stall_poll, Duration::from_millis(100));

        let client = client
            .with_upload_chunk_bytes(8 * 1024)
            .with_upload_stall_poll_interval(Duration::from_millis(25));
        assert_eq!(client.upload_chunk_bytes, 8 * 1024);
        assert_eq!(client.upload_stall_poll, Duration::from_millis(25));
    }

    #[test]
    #[should_panic(expected = "upload chunk size must be greater than zero")]
    fn zero_upload_chunk_size_is_rejected() {
        let _ = ReqwestHttpClient::new(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .with_upload_chunk_bytes(0);
    }

    #[test]
    #[should_panic(expected = "upload stall poll interval must be greater than zero")]
    fn zero_upload_stall_poll_interval_is_rejected() {
        let _ = ReqwestHttpClient::new(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .with_upload_stall_poll_interval(Duration::ZERO);
    }

    #[tokio::test]
    async fn configurable_upload_stall_poll_controls_first_check() {
        let result = tokio::time::timeout(
            Duration::from_millis(250),
            race_upload_stall(
                std::future::pending::<()>(),
                Some(UploadProgress::new(1)),
                Duration::from_millis(1),
                Duration::from_secs(5),
                "http://example.test",
            ),
        )
        .await;

        assert!(
            result.is_err(),
            "watchdog checked before the configured poll interval"
        );
    }

    #[tokio::test]
    async fn upload_watchdog_aborts_when_progress_stops() {
        let stall_timeout = Duration::from_millis(250);
        let progress = UploadProgress::new(1);

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            race_upload_stall(
                std::future::pending::<()>(),
                Some(progress),
                stall_timeout,
                DEFAULT_UPLOAD_STALL_POLL,
                "http://example.test",
            ),
        )
        .await
        .expect("watchdog did not enforce the stall timeout")
        .unwrap_err();

        assert!(matches!(
            result,
            crate::error::FusilladeError::UploadStallTimeout(_)
        ));
    }

    #[tokio::test]
    async fn upload_watchdog_allows_progress_across_multiple_stall_windows() {
        const PROGRESS_STEPS: u64 = 24;
        let stall_timeout = Duration::from_millis(250);
        let progress = UploadProgress::new(PROGRESS_STEPS);
        let watchdog_progress = progress.clone();
        let (send_complete_tx, send_complete_rx) = tokio::sync::oneshot::channel();
        let watchdog = tokio::spawn(async move {
            race_upload_stall(
                async move { send_complete_rx.await.unwrap() },
                Some(watchdog_progress),
                stall_timeout,
                DEFAULT_UPLOAD_STALL_POLL,
                "http://example.test",
            )
            .await
        });

        let started = std::time::Instant::now();
        let mut pacing = tokio::time::interval(Duration::from_millis(25));
        pacing.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        for _ in 0..PROGRESS_STEPS {
            pacing.tick().await;
            progress.record(1);
            assert!(
                !watchdog.is_finished(),
                "watchdog aborted despite continuing upload progress"
            );
        }
        assert!(
            started.elapsed() > stall_timeout * 2,
            "test did not span multiple complete stall windows"
        );

        send_complete_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), watchdog)
            .await
            .expect("watchdog did not finish after upload and send completion")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_mock_client_basic() {
        let mock = MockHttpClient::new();
        mock.add_response(
            "POST /test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: "success".to_string(),
            }),
        );

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: Some(crate::batch::BatchId::from(uuid::Uuid::new_v4())),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: "{}".to_string(),
            model: "test-model".to_string(),
            api_key: "test-key".to_string(),
            created_by: String::new(),
            batch_metadata: std::collections::HashMap::new(),
        };

        let response = mock.execute(&request, "test-key").await.unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, "success");

        let calls = mock.get_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].method, "POST");
        assert_eq!(calls[0].path, "/test");
        assert_eq!(calls[0].api_key, "test-key");
    }

    #[tokio::test]
    async fn test_mock_client_multiple_responses() {
        let mock = MockHttpClient::new();
        mock.add_response(
            "GET /status",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: "first".to_string(),
            }),
        );
        mock.add_response(
            "GET /status",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: "second".to_string(),
            }),
        );

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: Some(crate::batch::BatchId::from(uuid::Uuid::new_v4())),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "GET".to_string(),
            path: "/status".to_string(),
            body: "".to_string(),
            model: "test-model".to_string(),
            api_key: "test-key".to_string(),
            created_by: String::new(),
            batch_metadata: std::collections::HashMap::new(),
        };

        let response1 = mock.execute(&request, "key").await.unwrap();
        assert_eq!(response1.body, "first");

        let response2 = mock.execute(&request, "key").await.unwrap();
        assert_eq!(response2.body, "second");

        assert_eq!(mock.call_count(), 2);
    }

    #[tokio::test]
    async fn test_mock_client_no_response() {
        let mock = MockHttpClient::new();

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: Some(crate::batch::BatchId::from(uuid::Uuid::new_v4())),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/unknown".to_string(),
            body: "{}".to_string(),
            model: "test-model".to_string(),
            api_key: "test-key".to_string(),
            created_by: String::new(),
            batch_metadata: std::collections::HashMap::new(),
        };

        let result = mock.execute(&request, "key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_client_with_trigger() {
        let mock = MockHttpClient::new();

        let trigger = mock.add_response_with_trigger(
            "POST /test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: "triggered".to_string(),
            }),
        );

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: Some(crate::batch::BatchId::from(uuid::Uuid::new_v4())),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: "{}".to_string(),
            model: "test-model".to_string(),
            api_key: "test-key".to_string(),
            created_by: String::new(),
            batch_metadata: std::collections::HashMap::new(),
        };

        let mock_clone = mock.clone();
        let handle = tokio::spawn(async move { mock_clone.execute(&request, "key").await });

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert!(!handle.is_finished());

        trigger.send(()).unwrap();

        let response = handle.await.unwrap().unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, "triggered");
    }

    #[tokio::test]
    async fn test_mock_client_records_batch_metadata() {
        let mock = MockHttpClient::new();
        mock.add_response(
            "POST /test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: "success".to_string(),
            }),
        );

        let mut batch_metadata = std::collections::HashMap::new();
        batch_metadata.insert("id".to_string(), "batch-123".to_string());
        batch_metadata.insert(
            "endpoint".to_string(),
            "https://api.example.com".to_string(),
        );
        batch_metadata.insert("created_at".to_string(), "2025-12-19T12:00:00Z".to_string());
        batch_metadata.insert("completion_window".to_string(), "2s".to_string());

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: Some(crate::batch::BatchId::from(uuid::Uuid::new_v4())),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: r#"{"key":"value"}"#.to_string(),
            model: "test-model".to_string(),
            api_key: "test-key".to_string(),
            created_by: String::new(),
            batch_metadata: batch_metadata.clone(),
        };

        let response = mock.execute(&request, "test-key").await.unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, "success");

        let calls = mock.get_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].batch_metadata.len(), 4);
        assert_eq!(
            calls[0].batch_metadata.get("id"),
            Some(&"batch-123".to_string())
        );
        assert_eq!(
            calls[0].batch_metadata.get("endpoint"),
            Some(&"https://api.example.com".to_string())
        );
        assert_eq!(
            calls[0].batch_metadata.get("created_at"),
            Some(&"2025-12-19T12:00:00Z".to_string())
        );
        assert_eq!(
            calls[0].batch_metadata.get("completion_window"),
            Some(&"2s".to_string())
        );
    }

    #[tokio::test]
    async fn test_reqwest_client_sets_batch_metadata_headers() {
        use axum::{Router, extract::Request, http::StatusCode, routing::post};

        let app = Router::new().route(
            "/test",
            post(|request: Request| async move {
                let headers = request.headers();

                assert_eq!(
                    headers
                        .get("x-fusillade-batch-id")
                        .and_then(|h| h.to_str().ok()),
                    Some("batch-456"),
                    "Missing or incorrect x-fusillade-batch-id header"
                );
                assert_eq!(
                    headers
                        .get("x-fusillade-batch-endpoint")
                        .and_then(|h| h.to_str().ok()),
                    Some("/v1/completions"),
                    "Missing or incorrect x-fusillade-batch-endpoint header"
                );
                assert_eq!(
                    headers
                        .get("x-fusillade-batch-created-at")
                        .and_then(|h| h.to_str().ok()),
                    Some("2025-12-19T13:00:00Z"),
                    "Missing or incorrect x-fusillade-batch-created-at header"
                );
                assert_eq!(
                    headers
                        .get("x-fusillade-batch-completion-window")
                        .and_then(|h| h.to_str().ok()),
                    Some("24h"),
                    "Missing or incorrect x-fusillade-batch-completion-window header"
                );
                assert_eq!(
                    headers
                        .get("x-fusillade-batch-dw-user-agent")
                        .and_then(|h| h.to_str().ok()),
                    Some("claude-cli/1.2.3"),
                    "Missing or incorrect x-fusillade-batch-dw-user-agent header"
                );
                assert!(
                    headers.get("user-agent").is_none(),
                    "the dispatching client must not add a User-Agent of its own — if it \
                     ever does, analytics must keep preferring the batch header"
                );

                assert_eq!(
                    headers.get("authorization").and_then(|h| h.to_str().ok()),
                    Some("Bearer test-api-key"),
                    "Missing or incorrect authorization header"
                );
                assert!(
                    headers.get("x-fusillade-request-id").is_some(),
                    "Missing x-fusillade-request-id header"
                );

                (StatusCode::OK, r#"{"result":"ok"}"#)
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut batch_metadata = std::collections::HashMap::new();
        batch_metadata.insert("id".to_string(), "batch-456".to_string());
        batch_metadata.insert("endpoint".to_string(), "/v1/completions".to_string());
        batch_metadata.insert("created_at".to_string(), "2025-12-19T13:00:00Z".to_string());
        batch_metadata.insert("completion_window".to_string(), "24h".to_string());
        batch_metadata.insert("dw_user_agent".to_string(), "claude-cli/1.2.3".to_string());

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: Some(crate::batch::BatchId::from(uuid::Uuid::new_v4())),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: format!("http://{}", addr),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: r#"{"prompt":"test"}"#.to_string(),
            model: "test-model".to_string(),
            api_key: "test-api-key".to_string(),
            created_by: String::new(),
            batch_metadata,
        };

        let client = ReqwestHttpClient::default();
        let response = client.execute(&request, "test-api-key").await.unwrap();

        assert_eq!(response.status, 200);
        assert_eq!(response.body, r#"{"result":"ok"}"#);
    }

    #[tokio::test]
    async fn test_response_x_request_id_is_captured() {
        use axum::{Router, http::StatusCode, routing::post};

        let app = Router::new().route(
            "/test",
            post(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    [("x-request-id", "server-minted-id")],
                    "nope",
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: Some(crate::batch::BatchId::from(uuid::Uuid::new_v4())),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: format!("http://{}", addr),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: "{}".to_string(),
            model: "test-model".to_string(),
            api_key: "k".to_string(),
            created_by: String::new(),
            batch_metadata: std::collections::HashMap::new(),
        };
        let response = ReqwestHttpClient::default()
            .execute(&request, "k")
            .await
            .unwrap();

        assert_eq!(response.status, 400);
        assert_eq!(response.x_request_id.as_deref(), Some("server-minted-id"));
    }

    #[derive(Clone, Default)]
    struct LogBuffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl LogBuffer {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    impl std::io::Write for LogBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
        type Writer = LogBuffer;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[derive(Clone, Default)]
    struct ExecuteSpanRequestId(std::sync::Arc<std::sync::Mutex<Option<String>>>);

    impl<S> tracing_subscriber::Layer<S> for ExecuteSpanRequestId
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_record(
            &self,
            id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Visit<'a>(&'a mut Option<String>);
            impl tracing::field::Visit for Visit<'_> {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "x_request_id" {
                        *self.0 = Some(format!("{value:?}"));
                    }
                }
            }
            if ctx
                .span(id)
                .is_some_and(|s| s.name() == "fusillade.execute")
            {
                values.record(&mut Visit(&mut self.0.lock().unwrap()));
            }
        }
    }

    #[tokio::test]
    async fn test_x_request_id_recorded_when_body_read_fails() {
        use axum::{Router, body::Body, http::Response, routing::post};
        use tracing_subscriber::layer::SubscriberExt;

        const CHILD: &str = "FUSILLADE_EXECUTE_REQUEST_ID_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "http::tests::test_x_request_id_recorded_when_body_read_fails",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .expect("start isolated tracing test");
            assert!(
                output.status.success(),
                "isolated tracing test failed:\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("1 passed"),
                "child test filter must execute the tracing test"
            );
            return;
        }

        let captured = ExecuteSpanRequestId::default();
        let logs = LogBuffer::default();
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(captured.clone()).with(
                tracing_subscriber::fmt::layer()
                    .compact()
                    .with_ansi(false)
                    .with_writer(logs.clone()),
            ),
        )
        .expect("isolated tracing test owns its subscriber");

        let app = Router::new().route(
            "/test",
            post(|| async {
                let chunks = futures::stream::unfold(0u8, |step| async move {
                    match step {
                        0 => Some((Ok(bytes::Bytes::from_static(b"{\"partial\":")), 1)),
                        1 => {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            Some((Err(std::io::Error::other("connection reset mid-body")), 2))
                        }
                        _ => None,
                    }
                });
                Response::builder()
                    .header("x-request-id", "server-minted-id")
                    .body(Body::from_stream(chunks))
                    .unwrap()
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: None,
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: format!("http://{}", addr),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: "{}".to_string(),
            model: "test-model".to_string(),
            api_key: "k".to_string(),
            created_by: String::new(),
            batch_metadata: std::collections::HashMap::new(),
        };
        assert_eq!(*captured.0.lock().unwrap(), None);
        let result = ReqwestHttpClient::default().execute(&request, "k").await;

        assert!(result.is_err(), "body read should fail");
        assert_eq!(
            captured.0.lock().unwrap().as_deref(),
            Some("server-minted-id")
        );
        let text = logs.text();
        assert!(
            text.lines()
                .any(|line| line.contains("HTTP response body read failed")
                    && line.contains("x_request_id=server-minted-id")),
            "body-read failure logged with x_request_id:\n{text}"
        );
    }

    #[tokio::test]
    async fn test_custom_id_with_newline_is_not_retriable() {
        use crate::request::types::FailureReason;

        let request = RequestData {
            id: RequestId::from(uuid::Uuid::new_v4()),
            batch_id: Some(crate::batch::BatchId::from(uuid::Uuid::new_v4())),
            template_id: crate::batch::TemplateId::from(uuid::Uuid::new_v4()),
            custom_id: Some("invalid\ncustom_id".to_string()),
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/test".to_string(),
            body: "{}".to_string(),
            model: "test-model".to_string(),
            api_key: "test-key".to_string(),
            created_by: String::new(),
            batch_metadata: std::collections::HashMap::new(),
        };

        let client = ReqwestHttpClient::default();
        let result = client.execute(&request, "test-key").await;
        let err = result.expect_err("Expected builder error for invalid header value");

        let reason = match err {
            crate::error::FusilladeError::HttpRequestBuilder(error) => {
                FailureReason::RequestBuilderError { error }
            }
            _ => panic!("Expected HttpClient builder error, got: {:?}", err),
        };

        assert!(
            !reason.is_retriable(),
            "Builder errors should not be retriable"
        );
    }
}
