
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::HeaderMap;
use axum::routing::{any, get};
use axum_prometheus::{
    AXUM_HTTP_REQUESTS_DURATION_SECONDS, GenericMetricLayer, Handle,
    PREFIXED_HTTP_REQUESTS_DURATION_SECONDS, PrometheusMetricLayerBuilder,
    metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle},
    utils::SECONDS_DURATION_BUCKETS,
};
use std::borrow::Cow;
use std::sync::Arc;
use tracing::{info, instrument};

pub mod affinity;
pub mod aimd;
pub mod auth;
pub mod client;
pub mod config;
pub mod errors;
pub mod handlers;
pub mod inflight;
pub mod load_balancer;
pub mod models;
pub mod reasoning;
pub mod rejections;
pub mod response_id;
pub mod response_sanitizer;
pub mod serving;
pub mod sse;
pub mod strict;
pub mod target;
pub mod telemetry;
pub mod unsupported_params;

use client::{HttpClient, HyperClient};
pub use errors::ErrorOrigin;
pub use handlers::{AuthenticatedApiKeyId, HeaderExtractor, ServedBy};
use handlers::{models as models_handler, target_message_handler};
use models::ExtractedModel;
pub use rejections::RequestIdentity;
pub use serving::{
    AccountServing, ProviderKind, ServingClass, ServingClassOutcome, ServingHints, ServingOverlay,
};

pub type ResponseTransformFn = Arc<
    dyn Fn(&str, &HeaderMap, &[u8], Option<&str>) -> Result<Option<axum::body::Bytes>, String>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct AppState<T: HttpClient> {
    pub http_client: T,
    pub targets: target::Targets,
    pub response_transform_fn: Option<ResponseTransformFn>,
    pub upstream_rate_limit_message: Option<String>,
    pub response_id_header: Option<String>,
    pub body_limit: usize,
    pub sse_buffer_limit: usize,
    pub first_token_timeout: Option<std::time::Duration>,
    pub first_token_timeout_exempt_header: Option<String>,
    pub rejected_params: Vec<&'static str>,
    pub model_allowed_params: std::collections::HashMap<String, Vec<&'static str>>,
    pub inflight_limiter: Arc<dyn inflight::InflightLimiter>,
    pub batch_inflight_enforce: bool,
    pub batch_inflight_limiter: Arc<dyn inflight::InflightLimiter>,
}

pub const DEFAULT_BODY_LIMIT: usize = 32 * 1024 * 1024;

impl<T: HttpClient> std::fmt::Debug for AppState<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("http_client", &self.http_client)
            .field("targets", &self.targets)
            .field(
                "response_transform_fn",
                &self.response_transform_fn.as_ref().map(|_| "<function>"),
            )
            .field(
                "upstream_rate_limit_message",
                &self.upstream_rate_limit_message,
            )
            .field("response_id_header", &self.response_id_header)
            .field("body_limit", &self.body_limit)
            .field("sse_buffer_limit", &self.sse_buffer_limit)
            .field("first_token_timeout", &self.first_token_timeout)
            .field(
                "first_token_timeout_exempt_header",
                &self.first_token_timeout_exempt_header,
            )
            .field("rejected_params", &self.rejected_params)
            .field("model_allowed_params", &self.model_allowed_params)
            .field("inflight_limiter", &self.inflight_limiter)
            .field("batch_inflight_enforce", &self.batch_inflight_enforce)
            .field("batch_inflight_limiter", &self.batch_inflight_limiter)
            .finish()
    }
}

impl AppState<HyperClient> {
    pub fn new(targets: target::Targets) -> Self {
        let (max_idle, timeout) = targets
            .http_pool_config
            .as_ref()
            .map(|p| (p.max_idle_per_host, p.idle_timeout_secs))
            .unwrap_or((100, 90));

        let http_client = client::create_hyper_client(max_idle, timeout);
        Self {
            http_client,
            targets,
            response_transform_fn: None,
            upstream_rate_limit_message: None,
            response_id_header: None,
            body_limit: DEFAULT_BODY_LIMIT,
            sse_buffer_limit: sse::DEFAULT_SSE_BUFFER_LIMIT,
            first_token_timeout: None,
            first_token_timeout_exempt_header: None,
            rejected_params: Vec::new(),
            model_allowed_params: std::collections::HashMap::new(),
            inflight_limiter: Arc::new(inflight::LocalInflightLimiter::default()),
            batch_inflight_enforce: false,
            batch_inflight_limiter: Arc::new(inflight::LocalInflightLimiter::default()),
        }
    }
}

impl<T: HttpClient> AppState<T> {
    pub fn with_client(targets: target::Targets, http_client: T) -> Self {
        Self {
            http_client,
            targets,
            response_transform_fn: None,
            upstream_rate_limit_message: None,
            response_id_header: None,
            body_limit: DEFAULT_BODY_LIMIT,
            sse_buffer_limit: sse::DEFAULT_SSE_BUFFER_LIMIT,
            first_token_timeout: None,
            first_token_timeout_exempt_header: None,
            rejected_params: Vec::new(),
            model_allowed_params: std::collections::HashMap::new(),
            inflight_limiter: Arc::new(inflight::LocalInflightLimiter::default()),
            batch_inflight_enforce: false,
            batch_inflight_limiter: Arc::new(inflight::LocalInflightLimiter::default()),
        }
    }

    pub fn with_upstream_rate_limit_message(mut self, message: impl Into<String>) -> Self {
        self.upstream_rate_limit_message = Some(message.into());
        self
    }

    pub fn with_response_id_header(mut self, header: impl Into<String>) -> Self {
        self.response_id_header = Some(header.into());
        self
    }

    pub fn with_first_token_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.first_token_timeout = Some(timeout);
        self
    }

    pub fn with_first_token_timeout_exempt_header(mut self, header: impl Into<String>) -> Self {
        self.first_token_timeout_exempt_header = Some(header.into());
        self
    }

    pub fn with_rejected_params<I, S>(mut self, params: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut rejected = Vec::new();
        for name in params {
            let name = name.as_ref();
            let param = unsupported_params::known_param(name)
                .ok_or_else(|| format!("unknown parameter in the reject list: {name}"))?;
            if !rejected.contains(&param) {
                rejected.push(param);
            }
        }
        self.rejected_params = rejected;
        Ok(self)
    }

    pub fn with_model_allowed_params<I, M, P, S>(mut self, models: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = (M, P)>,
        M: Into<String>,
        P: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut allowed = std::collections::HashMap::new();
        for (model, params) in models {
            let model = model.into();
            let mut known: Vec<&'static str> = Vec::new();
            for name in params {
                let name = name.as_ref();
                let param = unsupported_params::known_param(name).ok_or_else(|| {
                    format!("unknown parameter in the allow list for {model}: {name}")
                })?;
                if !known.contains(&param) {
                    known.push(param);
                }
            }
            allowed.insert(model, known);
        }
        self.model_allowed_params = allowed;
        Ok(self)
    }

    pub fn rejects_param(&self, model: &str, param: &str) -> bool {
        self.rejected_params.contains(&param)
            && !self
                .model_allowed_params
                .get(model)
                .is_some_and(|allowed| allowed.contains(&param))
    }

    pub fn with_inflight_limiter(mut self, limiter: Arc<dyn inflight::InflightLimiter>) -> Self {
        self.inflight_limiter = limiter;
        self
    }

    pub fn with_batch_inflight_limiter(
        mut self,
        limiter: Arc<dyn inflight::InflightLimiter>,
    ) -> Self {
        self.batch_inflight_limiter = limiter;
        self
    }

    pub fn with_batch_inflight_enforce(mut self, enforce: bool) -> Self {
        if enforce && self.first_token_timeout_exempt_header.is_none() {
            tracing::warn!(
                "batch in-flight cap enforcement is on, but no batch marker header is configured \
                 (with_first_token_timeout_exempt_header); no request will be counted as batch"
            );
        }
        self.batch_inflight_enforce = enforce;
        self
    }

    pub fn with_response_transform(mut self, transform_fn: ResponseTransformFn) -> Self {
        self.response_transform_fn = Some(transform_fn);
        self
    }

    pub fn with_sse_buffer_limit(mut self, limit: usize) -> Self {
        self.sse_buffer_limit = limit;
        self
    }

    pub fn with_body_limit(mut self, limit: usize) -> Self {
        self.body_limit = limit;
        self
    }
}

pub fn extract_model_from_request(headers: &HeaderMap, body_bytes: &[u8]) -> Option<String> {
    const MODEL_OVERRIDE_HEADER: &str = "model-override";

    match headers.get(MODEL_OVERRIDE_HEADER) {
        Some(header_value) => {
            let model_str = header_value.to_str().ok()?;
            Some(model_str.to_string())
        }
        None => {
            let extracted: ExtractedModel = serde_json::from_slice(body_bytes).ok()?;
            Some(extracted.model.into_owned())
        }
    }
}

pub fn create_openai_sanitizer() -> ResponseTransformFn {
    Arc::new(|path, headers, body, original_model| {
        let sanitizer = response_sanitizer::ResponseSanitizer {
            original_model: original_model.map(String::from),
        };
        sanitizer.sanitize(path, headers, body)
    })
}

#[instrument(skip(state))]
pub fn build_router<T: HttpClient + Clone + Send + Sync + 'static>(state: AppState<T>) -> Router {
    info!("Building router");
    Router::new()
        .route("/models", get(models_handler))
        .route("/v1/models", get(models_handler))
        .route("/{*path}", any(target_message_handler))
        .layer(DefaultBodyLimit::max(state.body_limit))
        .with_state(state)
}

#[instrument(skip(handle))]
pub fn build_metrics_router(handle: PrometheusHandle) -> Router {
    info!("Building metrics router");
    Router::new().route(
        "/metrics",
        axum::routing::get(move || async move { handle.render() }),
    )
}

pub const FIRST_TOKEN_SECONDS_BUCKETS: &[f64] = &[
    0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 7.5, 10.0, 15.0, 20.0, 30.0, 60.0, 120.0,
];

type MetricsLayerAndHandle = (
    GenericMetricLayer<'static, PrometheusHandle, Handle>,
    PrometheusHandle,
);

pub fn build_metrics_layer_and_handle(
    prefix: impl Into<Cow<'static, str>>,
) -> MetricsLayerAndHandle {
    info!("Building metrics layer");
    PrometheusMetricLayerBuilder::new()
        .with_prefix(prefix)
        .enable_response_body_size(true)
        .with_endpoint_label_type(axum_prometheus::EndpointLabel::Exact)
        .with_metrics_from_fn(|| {
            let recorder = PrometheusBuilder::new()
                .set_buckets_for_metric(
                    Matcher::Full(
                        PREFIXED_HTTP_REQUESTS_DURATION_SECONDS
                            .get()
                            .map_or(AXUM_HTTP_REQUESTS_DURATION_SECONDS, |s| s.as_str())
                            .to_string(),
                    ),
                    SECONDS_DURATION_BUCKETS,
                )
                .expect("valid HTTP duration buckets")
                .set_buckets_for_metric(
                    Matcher::Full("onwards_first_token_seconds".to_string()),
                    FIRST_TOKEN_SECONDS_BUCKETS,
                )
                .expect("valid first-token buckets")
                .build_recorder();
            let handle = recorder.handle();
            let upkeep = handle.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    upkeep.run_upkeep();
                }
            });
            metrics::set_global_recorder(recorder).expect("Failed to set global recorder");
            handle
        })
        .build_pair()
}

pub mod test_utils {
    use super::*;
    use async_trait::async_trait;
    use axum::http::StatusCode;
    use std::sync::{Arc, Mutex};

    pub fn declared_field_names(source: &str) -> Vec<String> {
        let source = source.split("#[cfg(test)]").next().unwrap_or(source);
        let mut names = Vec::new();
        for line in source.lines().map(str::trim) {
            for marker in ["rename = \"", "alias = \""] {
                let mut rest = line;
                while let Some(start) = rest.find(marker) {
                    rest = &rest[start + marker.len()..];
                    if let Some(end) = rest.find('"') {
                        names.push(rest[..end].to_string());
                    }
                }
            }
            if line.starts_with("//") || line.starts_with("let ") {
                continue;
            }
            let field = line
                .strip_prefix("pub(crate) ")
                .or_else(|| line.strip_prefix("pub "))
                .unwrap_or(line);
            let field = field.strip_prefix("r#").unwrap_or(field);
            let len = field
                .find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
                .unwrap_or(field.len());
            let after = field[len..].trim_start();
            if len > 0
                && field.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
                && after.starts_with(':')
                && !after.starts_with("::")
            {
                names.push(field[..len].to_string());
            }
        }
        names
    }

    pub struct MockHttpClient {
        pub requests: Arc<Mutex<Vec<MockRequest>>>,
        response_builder: Arc<dyn Fn() -> axum::response::Response + Send + Sync>,
        custom_headers: Arc<Mutex<Vec<(String, String)>>>,
    }

    #[derive(Debug, Clone)]
    pub struct MockRequest {
        pub method: String,
        pub uri: String,
        pub headers: Vec<(String, String)>,
        pub body: Vec<u8>,
    }

    impl MockHttpClient {
        pub fn new(status: StatusCode, body: &str) -> Self {
            let body = body.to_string();
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                response_builder: Arc::new(move || {
                    axum::response::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(body.clone()))
                        .unwrap()
                }),
                custom_headers: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub fn new_streaming(status: StatusCode, chunks: Vec<String>) -> Self {
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                response_builder: Arc::new(move || {
                    use axum::body::Body;
                    use futures_util::stream;

                    let stream = stream::iter(
                        chunks
                            .clone()
                            .into_iter()
                            .map(|chunk| Ok::<_, std::io::Error>(chunk.into_bytes())),
                    );

                    axum::response::Response::builder()
                        .status(status)
                        .header("content-type", "text/event-stream")
                        .header("cache-control", "no-cache")
                        .header("connection", "keep-alive")
                        .body(Body::from_stream(stream))
                        .unwrap()
                }),
                custom_headers: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub fn new_streaming_sequence(status: StatusCode, responses: Vec<Vec<String>>) -> Self {
            let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                custom_headers: Arc::new(Mutex::new(Vec::new())),
                response_builder: Arc::new(move || {
                    use axum::body::Body;
                    use futures_util::stream;

                    let idx = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let chunks = responses.get(idx).cloned().unwrap_or_default();

                    let stream = stream::iter(
                        chunks
                            .into_iter()
                            .map(|chunk| Ok::<_, std::io::Error>(chunk.into_bytes())),
                    );

                    axum::response::Response::builder()
                        .status(status)
                        .header("content-type", "text/event-stream")
                        .header("cache-control", "no-cache")
                        .header("connection", "keep-alive")
                        .body(Body::from_stream(stream))
                        .unwrap()
                }),
            }
        }

        pub fn new_delayed_streaming_sequence(
            status: StatusCode,
            responses: Vec<(std::time::Duration, Vec<String>)>,
        ) -> Self {
            let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                custom_headers: Arc::new(Mutex::new(Vec::new())),
                response_builder: Arc::new(move || {
                    use axum::body::Body;

                    let idx = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let (delay, chunks) = responses.get(idx).cloned().unwrap_or_default();

                    let stream = async_stream::stream! {
                        tokio::time::sleep(delay).await;
                        for chunk in chunks {
                            yield Ok::<_, std::io::Error>(chunk.into_bytes());
                        }
                    };

                    axum::response::Response::builder()
                        .status(status)
                        .header("content-type", "text/event-stream")
                        .header("cache-control", "no-cache")
                        .header("connection", "keep-alive")
                        .body(Body::from_stream(stream))
                        .unwrap()
                }),
            }
        }

        pub fn new_timed_streaming_sequence(
            status: StatusCode,
            responses: Vec<Vec<(std::time::Duration, String)>>,
        ) -> Self {
            let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                custom_headers: Arc::new(Mutex::new(Vec::new())),
                response_builder: Arc::new(move || {
                    use axum::body::Body;

                    let idx = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let chunks = responses.get(idx).cloned().unwrap_or_default();

                    let stream = async_stream::stream! {
                        for (delay, chunk) in chunks {
                            tokio::time::sleep(delay).await;
                            yield Ok::<_, std::io::Error>(chunk.into_bytes());
                        }
                    };

                    axum::response::Response::builder()
                        .status(status)
                        .header("content-type", "text/event-stream")
                        .header("cache-control", "no-cache")
                        .header("connection", "keep-alive")
                        .body(Body::from_stream(stream))
                        .unwrap()
                }),
            }
        }

        pub fn get_requests(&self) -> Vec<MockRequest> {
            self.requests.lock().unwrap().clone()
        }

        pub fn set_header(&mut self, name: &str, value: String) {
            self.custom_headers
                .lock()
                .unwrap()
                .push((name.to_string(), value));
        }
    }

    impl std::fmt::Debug for MockHttpClient {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockHttpClient")
                .field("requests", &self.requests)
                .field("response_builder", &"<closure>")
                .finish()
        }
    }

    impl Clone for MockHttpClient {
        fn clone(&self) -> Self {
            Self {
                requests: Arc::clone(&self.requests),
                response_builder: Arc::clone(&self.response_builder),
                custom_headers: Arc::clone(&self.custom_headers),
            }
        }
    }

    #[async_trait]
    impl HttpClient for MockHttpClient {
        async fn request(
            &self,
            req: axum::extract::Request,
        ) -> Result<axum::response::Response, Box<dyn std::error::Error + Send + Sync>> {
            let method = req.method().to_string();
            let uri = req.uri().to_string();
            let headers = req
                .headers()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();

            let body = axum::body::to_bytes(req.into_body(), usize::MAX)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
                .to_vec();

            let mock_request = MockRequest {
                method,
                uri,
                headers,
                body,
            };
            self.requests.lock().unwrap().push(mock_request);

            let mut response = (self.response_builder)();
            let custom_headers = self.custom_headers.lock().unwrap();
            for (name, value) in custom_headers.iter() {
                response.headers_mut().insert(
                    axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                    axum::http::HeaderValue::from_str(value).unwrap(),
                );
            }

            Ok(response)
        }
    }

    pub struct TriggeredMockHttpClient {
        pub requests: Arc<Mutex<Vec<MockRequest>>>,
        response_builder: Arc<dyn Fn() -> axum::response::Response + Send + Sync>,
        triggers: Arc<Mutex<Vec<tokio::sync::oneshot::Sender<()>>>>,
    }

    impl TriggeredMockHttpClient {
        pub fn new(status: StatusCode, body: &str) -> Self {
            let body = body.to_string();
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                response_builder: Arc::new(move || {
                    axum::response::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(body.clone()))
                        .unwrap()
                }),
                triggers: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub fn get_requests(&self) -> Vec<MockRequest> {
            self.requests.lock().unwrap().clone()
        }

        pub fn complete_request(&self, index: usize) -> bool {
            let mut triggers = self.triggers.lock().unwrap();
            if index < triggers.len() {
                let trigger = triggers.remove(index);
                let _ = trigger.send(());
                true
            } else {
                false
            }
        }
    }

    impl std::fmt::Debug for TriggeredMockHttpClient {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TriggeredMockHttpClient")
                .field("requests", &self.requests)
                .field("pending_triggers", &self.triggers.lock().unwrap().len())
                .field("response_builder", &"<closure>")
                .finish()
        }
    }

    impl Clone for TriggeredMockHttpClient {
        fn clone(&self) -> Self {
            Self {
                requests: Arc::clone(&self.requests),
                response_builder: Arc::clone(&self.response_builder),
                triggers: Arc::clone(&self.triggers),
            }
        }
    }

    #[async_trait]
    impl HttpClient for TriggeredMockHttpClient {
        async fn request(
            &self,
            req: axum::extract::Request,
        ) -> Result<axum::response::Response, Box<dyn std::error::Error + Send + Sync>> {
            let method = req.method().to_string();
            let uri = req.uri().to_string();
            let headers = req
                .headers()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();

            let body = axum::body::to_bytes(req.into_body(), usize::MAX)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
                .to_vec();

            let mock_request = MockRequest {
                method,
                uri,
                headers,
                body,
            };
            self.requests.lock().unwrap().push(mock_request);

            let (tx, rx) = tokio::sync::oneshot::channel();
            self.triggers.lock().unwrap().push(tx);

            let _ = rx.await;

            Ok((self.response_builder)())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::KeyHash;
    use crate::load_balancer::{Provider, ProviderPool};
    use crate::target::{ConcurrencyLimiter, Target, Targets};
    use axum::http::StatusCode;
    use axum_test::TestServer;
    use dashmap::DashMap;
    use serde_json::json;
    use std::sync::Arc;
    use test_utils::MockHttpClient;

    fn pool(target: Target) -> target::TargetPools {
        target.into_pool()
    }

    #[test]
    fn test_extract_model_decodes_json_escapes() {
        let headers = HeaderMap::new();
        for body in [
            br#"{"model":"zai-org/GLM-5.2-FP8","messages":[]}"#.as_slice(),
            br#"{"model":"zai-org\/GLM-5.2-FP8","messages":[]}"#.as_slice(),
            br#"{"model":"zai-org\u002FGLM-5.2-FP8","messages":[]}"#.as_slice(),
        ] {
            assert_eq!(
                extract_model_from_request(&headers, body).as_deref(),
                Some("zai-org/GLM-5.2-FP8"),
                "body: {}",
                String::from_utf8_lossy(body)
            );
        }
    }

    #[test]
    fn test_extract_model_decodes_escaped_backslash() {
        let headers = HeaderMap::new();
        let body = br#"{"model":"weird\\model","messages":[]}"#;
        assert_eq!(
            extract_model_from_request(&headers, body).as_deref(),
            Some(r"weird\model")
        );
    }

    #[tokio::test]
    async fn test_empty_targets_returns_404() {
        let targets = target::Targets {
            targets: Arc::new(DashMap::new()),
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let mock_client = MockHttpClient::new(StatusCode::OK, "{}");
        let app_state = AppState::with_client(targets, mock_client);
        let router = build_router(app_state);
        let server = TestServer::new(router).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 404);
    }

    #[tokio::test]
    async fn test_non_strict_router_rejects_body_over_configured_limit() {
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "gpt-4".to_string(),
            pool(
                target::Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .build(),
            ),
        );

        let targets = target::Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let mock_client = MockHttpClient::new(StatusCode::OK, "{}");
        let app_state = AppState::with_client(targets, mock_client).with_body_limit(1024);
        let router = build_router(app_state);
        let server = TestServer::new(router).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4",
                "messages": [{"role": "user", "content": "x".repeat(2048)}]
            }))
            .await;

        assert_eq!(response.status_code(), StatusCode::PAYLOAD_TOO_LARGE);
        let body: serde_json::Value = response.json();
        assert_eq!(body["error"]["code"], "payload_too_large");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("1024 bytes")
        );
    }

    #[tokio::test]
    async fn test_multiple_targets_routing() {
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "gpt-4".to_string(),
            pool(
                target::Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .onwards_key("sk-test-key".to_string())
                    .build(),
            ),
        );
        targets_map.insert(
            "claude-3".to_string(),
            pool(
                target::Target::builder()
                    .url("https://api.anthropic.com".parse().unwrap())
                    .onwards_key("sk-ant-test-key".to_string())
                    .build(),
            ),
        );

        let targets = target::Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let mock_client = MockHttpClient::new(
            StatusCode::OK,
            r#"{"choices": [{"message": {"content": "Hello!"}}]}"#,
        );
        let app_state = AppState::with_client(targets, mock_client.clone());
        let router = build_router(app_state);
        let server = TestServer::new(router).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 200);

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "claude-3",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 200);

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "non-existent-model",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 404);

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 2);

        assert!(requests[0].uri.contains("api.openai.com"));
        assert!(requests[1].uri.contains("api.anthropic.com"));
    }

    #[tokio::test]
    async fn test_presented_key_extension_authenticates_request() {
        use crate::auth::{KeyHash, PresentedKey};
        use std::collections::{HashMap, HashSet};
        use tower::ServiceExt;

        let api_key_id = uuid::Uuid::new_v4();
        let api_key = "test-api-key";
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "gpt-4".to_string(),
            pool(
                target::Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .keys(HashSet::from([KeyHash::of(api_key)]))
                    .build(),
            ),
        );
        let key_labels = Arc::new(DashMap::new());
        key_labels.insert(
            KeyHash::of(api_key),
            HashMap::from([("api_key_id".to_string(), api_key_id.to_string())]),
        );
        let targets = target::Targets {
            targets: targets_map,
            key_labels,
            ..Default::default()
        };
        let router = build_router(AppState::with_client(
            targets,
            MockHttpClient::new(StatusCode::OK, r#"{"choices": []}"#),
        ));

        let request = |presented: Option<&str>| {
            let mut request = axum::http::Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .header("authorization", "Bearer not-a-configured-key")
                .body(axum::body::Body::from(
                    json!({"model": "gpt-4", "messages": [{"role": "user", "content": "Hi"}]})
                        .to_string(),
                ))
                .unwrap();
            if let Some(key) = presented {
                request
                    .extensions_mut()
                    .insert(PresentedKey(KeyHash::of(key)));
            }
            request
        };

        let response = router.clone().oneshot(request(None)).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "bearer alone");

        let response = router
            .clone()
            .oneshot(request(Some(api_key)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "pre-authenticated key");
        assert_eq!(
            response.extensions().get::<crate::AuthenticatedApiKeyId>(),
            Some(&crate::AuthenticatedApiKeyId(api_key_id))
        );

        let response = router.oneshot(request(Some("unknown-key"))).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "unknown presented key"
        );
    }

    #[tokio::test]
    async fn test_served_by_extension_set_on_success() {
        use crate::auth::KeyHash;
        use std::collections::{HashMap, HashSet};
        use tower::ServiceExt;

        let api_key_id = uuid::Uuid::new_v4();
        let api_key = "test-api-key";
        let mut keys = HashSet::new();
        keys.insert(KeyHash::from(api_key.to_string()));
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "gpt-4".to_string(),
            pool(
                target::Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .keys(keys)
                    .onwards_model("gpt-4-upstream".to_string())
                    .build(),
            ),
        );
        let key_labels = Arc::new(DashMap::new());
        key_labels.insert(
            KeyHash::of(api_key),
            HashMap::from([("api_key_id".to_string(), api_key_id.to_string())]),
        );
        let targets = target::Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels,
            key_ids: Default::default(),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };
        let mock_client = MockHttpClient::new(
            StatusCode::OK,
            r#"{"choices": [{"message": {"content": "Hello!"}}]}"#,
        );
        let app_state = AppState::with_client(targets, mock_client);
        let router = build_router(app_state);

        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {api_key}"))
            .body(axum::body::Body::from(
                json!({
                    "model": "gpt-4",
                    "messages": [{"role": "user", "content": "Hello"}]
                })
                .to_string(),
            ))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let served_by = response
            .extensions()
            .get::<crate::ServedBy>()
            .expect("success response must carry ServedBy");
        assert_eq!(served_by.url, "https://api.openai.com/");
        assert_eq!(served_by.onwards_model.as_deref(), Some("gpt-4-upstream"));
        assert_eq!(served_by.compute_pool, None);
        assert_eq!(
            response.extensions().get::<crate::AuthenticatedApiKeyId>(),
            Some(&crate::AuthenticatedApiKeyId(api_key_id))
        );
    }

    #[tokio::test]
    async fn test_compute_pool_header_captured_in_served_by_and_preserved_for_chained_hops() {
        use crate::auth::KeyHash;
        use std::collections::HashSet;
        use tower::ServiceExt;

        let api_key = "test-api-key";
        let mut keys = HashSet::new();
        keys.insert(KeyHash::from(api_key.to_string()));
        let make_targets = |strict_mode: bool| {
            let targets_map = Arc::new(DashMap::new());
            targets_map.insert(
                "gpt-4".to_string(),
                pool(
                    target::Target::builder()
                        .url("https://inference.example.com".parse().unwrap())
                        .keys(keys.clone())
                        .build(),
                ),
            );
            target::Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Default::default(),
                accounts: Arc::new(DashMap::new()),
                strict_mode,
                http_pool_config: None,
            }
        };

        let mut mock_client = MockHttpClient::new(
            StatusCode::OK,
            r#"{"choices": [{"message": {"content": "Hello!"}}]}"#,
        );
        mock_client.set_header("X-Dynamo-Compute-Pool", "  gpu-pool-a  ".to_string());
        mock_client.set_header("x-other-upstream", "preserved".to_string());
        let router = build_router(AppState::with_client(make_targets(false), mock_client));

        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {api_key}"))
            .body(axum::body::Body::from(
                json!({
                    "model": "gpt-4",
                    "messages": [{"role": "user", "content": "Hello"}]
                })
                .to_string(),
            ))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("x-dynamo-compute-pool").unwrap(),
            "  gpu-pool-a  ",
            "onwards must preserve x-dynamo-compute-pool across chained internal hops"
        );
        assert_eq!(
            response.headers().get("x-other-upstream").unwrap(),
            "preserved"
        );
        let served_by = response
            .extensions()
            .get::<crate::ServedBy>()
            .expect("success response must carry ServedBy");
        assert_eq!(served_by.compute_pool.as_deref(), Some("gpu-pool-a"));

        let sse_chunk = format!(
            "data: {}\n\n",
            json!({
                "id": "chatcmpl-1",
                "object": "chat.completion.chunk",
                "created": 1700000000,
                "model": "gpt-4",
                "choices": [{"index": 0, "delta": {"content": "Hi"}, "finish_reason": "stop"}]
            })
        );
        let mut stream_client = MockHttpClient::new_streaming(
            StatusCode::OK,
            vec![sse_chunk, "data: [DONE]\n\n".to_string()],
        );
        stream_client.set_header("x-dynamo-compute-pool", "spot-pool-1".to_string());
        let router = build_router(AppState::with_client(make_targets(true), stream_client));

        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {api_key}"))
            .body(axum::body::Body::from(
                json!({
                    "model": "gpt-4",
                    "stream": true,
                    "messages": [{"role": "user", "content": "Hello"}]
                })
                .to_string(),
            ))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("x-dynamo-compute-pool").unwrap(),
            "spot-pool-1",
            "onwards must preserve x-dynamo-compute-pool on streaming responses for chained internal hops"
        );
        let served_by = response
            .extensions()
            .get::<crate::ServedBy>()
            .expect("streaming response must carry ServedBy");
        assert_eq!(served_by.compute_pool.as_deref(), Some("spot-pool-1"));
    }

    #[tokio::test]
    async fn test_authenticated_api_key_id_survives_models_and_strict_error() {
        use crate::auth::KeyHash;
        use crate::target::{RateLimitExceeded, RateLimiter};
        use std::collections::{HashMap, HashSet};
        use tower::ServiceExt;

        #[derive(Debug)]
        struct BlockingRateLimiter;

        impl RateLimiter for BlockingRateLimiter {
            fn check(&self) -> Result<(), RateLimitExceeded> {
                Err(RateLimitExceeded)
            }
        }

        let api_key_id = uuid::Uuid::new_v4();
        let api_key = "test-api-key";
        let mut keys = HashSet::new();
        keys.insert(KeyHash::from(api_key.to_string()));
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "gpt-4".to_string(),
            pool(
                target::Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .keys(keys.clone())
                    .build(),
            ),
        );
        targets_map.insert(
            "blocked-model".to_string(),
            pool(
                target::Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .keys(keys)
                    .limiter(Arc::new(BlockingRateLimiter) as Arc<dyn RateLimiter>)
                    .build(),
            ),
        );
        let key_labels = Arc::new(DashMap::new());
        key_labels.insert(
            KeyHash::of(api_key),
            HashMap::from([("api_key_id".to_string(), api_key_id.to_string())]),
        );
        let targets = target::Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels,
            key_ids: Default::default(),
            accounts: Default::default(),
            strict_mode: true,
            http_pool_config: None,
        };
        let mock_client = MockHttpClient::new(
            StatusCode::BAD_REQUEST,
            r#"{"error":{"message":"provider detail"}}"#,
        );
        let router = build_router(AppState::with_client(targets, mock_client));

        let models_request = axum::http::Request::builder()
            .method("GET")
            .uri("/v1/models")
            .header("authorization", format!("Bearer {api_key}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let models_response = router.clone().oneshot(models_request).await.unwrap();
        assert_eq!(
            models_response
                .extensions()
                .get::<crate::AuthenticatedApiKeyId>(),
            Some(&crate::AuthenticatedApiKeyId(api_key_id))
        );

        let chat_request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {api_key}"))
            .body(axum::body::Body::from(
                json!({
                    "model": "gpt-4",
                    "messages": [{"role": "user", "content": "Hello"}]
                })
                .to_string(),
            ))
            .unwrap();
        let chat_response = router.clone().oneshot(chat_request).await.unwrap();
        assert_eq!(chat_response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            chat_response
                .extensions()
                .get::<crate::AuthenticatedApiKeyId>(),
            Some(&crate::AuthenticatedApiKeyId(api_key_id))
        );

        let rejected_request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {api_key}"))
            .body(axum::body::Body::from(
                json!({
                    "model": "blocked-model",
                    "messages": [{"role": "user", "content": "Hello"}]
                })
                .to_string(),
            ))
            .unwrap();
        let rejected_response = router.oneshot(rejected_request).await.unwrap();
        assert_eq!(rejected_response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            rejected_response
                .extensions()
                .get::<crate::AuthenticatedApiKeyId>(),
            Some(&crate::AuthenticatedApiKeyId(api_key_id))
        );
    }

    #[tokio::test]
    async fn test_strict_refusals_before_forwarding_keep_the_caller_and_code() {
        use std::collections::HashMap;
        use tower::ServiceExt;

        let refusal = |body: String, body_limit: usize| async move {
            let targets = fallback_targets("gpt-4", 1, vec![]);
            targets.key_labels.insert(
                KeyHash::of("sk-acct"),
                HashMap::from([(serving::ACCOUNT_LABEL.to_string(), "acct-1".to_string())]),
            );
            let state = AppState::with_client(targets, MockHttpClient::new(StatusCode::OK, "{}"))
                .with_body_limit(body_limit);
            let request = axum::http::Request::builder()
                .method("POST")
                .uri("/chat/completions")
                .header("content-type", "application/json")
                .header("authorization", "Bearer sk-acct")
                .body(axum::body::Body::from(body))
                .unwrap();
            let response = crate::strict::build_strict_router(state)
                .oneshot(request)
                .await
                .unwrap();
            let account = response
                .extensions()
                .get::<crate::RequestIdentity>()
                .and_then(|identity| identity.account_id.clone());
            (
                response.status().as_u16(),
                account,
                response.extensions().get::<crate::ErrorOrigin>().cloned(),
            )
        };
        let expected = |status, code: &str| {
            (
                status,
                Some("acct-1".to_string()),
                Some(crate::ErrorOrigin::Gateway {
                    code: code.to_string(),
                }),
            )
        };

        let wrong_shape = json!({"model": "gpt-4", "messages": "hello"}).to_string();
        assert_eq!(
            refusal(wrong_shape, 1024).await,
            expected(422, "schema_mismatch")
        );
        assert_eq!(
            refusal("{not json".to_string(), 1024).await,
            expected(400, "invalid_json")
        );
        let chat = json!({"model": "gpt-4", "messages": [{"role": "user", "content": "Hello"}]});
        assert_eq!(
            refusal(chat.to_string(), 8).await,
            expected(413, "payload_too_large")
        );
    }

    #[tokio::test]
    async fn test_strict_refusals_attribute_a_presented_key() {
        use crate::auth::PresentedKey;
        use std::collections::HashMap;
        use tower::ServiceExt;

        let targets = fallback_targets("gpt-4", 1, vec![]);
        targets.key_labels.insert(
            KeyHash::of("sk-acct"),
            HashMap::from([(serving::ACCOUNT_LABEL.to_string(), "acct-1".to_string())]),
        );
        let state = AppState::with_client(targets, MockHttpClient::new(StatusCode::OK, "{}"));
        let mut request = axum::http::Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", "Bearer dwi-not-a-configured-key")
            .body(axum::body::Body::from("{not json"))
            .unwrap();
        request
            .extensions_mut()
            .insert(PresentedKey(KeyHash::of("sk-acct")));
        let response = crate::strict::build_strict_router(state)
            .oneshot(request)
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 400);
        assert_eq!(
            response
                .extensions()
                .get::<crate::RequestIdentity>()
                .and_then(|identity| identity.account_id.clone()),
            Some("acct-1".to_string())
        );
    }

    #[tokio::test]
    async fn test_strict_sanitized_errors_keep_the_caller_and_name_the_provider() {
        use std::collections::HashMap;
        use tower::ServiceExt;

        let attribution = |mock: MockHttpClient| async move {
            let targets = fallback_targets("gpt-4", 1, vec![]);
            targets.key_labels.insert(
                KeyHash::of("sk-acct"),
                HashMap::from([(serving::ACCOUNT_LABEL.to_string(), "acct-1".to_string())]),
            );
            let request = axum::http::Request::builder()
                .method("POST")
                .uri("/chat/completions")
                .header("content-type", "application/json")
                .header("authorization", "Bearer sk-acct")
                .body(axum::body::Body::from(
                    json!({"model": "gpt-4", "messages": [{"role": "user", "content": "Hello"}]})
                        .to_string(),
                ))
                .unwrap();
            let response = crate::strict::build_strict_router(AppState::with_client(targets, mock))
                .oneshot(request)
                .await
                .unwrap();
            let account = response
                .extensions()
                .get::<crate::RequestIdentity>()
                .and_then(|identity| identity.account_id.clone());
            (
                response.status().as_u16(),
                account,
                response.extensions().get::<crate::ErrorOrigin>().cloned(),
            )
        };
        let expected = |status| {
            (
                status,
                Some("acct-1".to_string()),
                Some(crate::ErrorOrigin::Provider),
            )
        };

        let malformed = MockHttpClient::new(StatusCode::OK, "not a completion");
        assert_eq!(attribution(malformed).await, expected(502));

        let rejected = MockHttpClient::new(
            StatusCode::BAD_REQUEST,
            r#"{"error":{"message":"provider detail"}}"#,
        );
        assert_eq!(attribution(rejected).await, expected(400));
    }

    fn fallback_targets(alias: &str, n: usize, on_status: Vec<u16>) -> target::Targets {
        fallback_targets_with(
            alias,
            n,
            target::FallbackConfig {
                enabled: true,
                on_status,
                ..Default::default()
            },
        )
    }

    fn fallback_targets_with(
        alias: &str,
        n: usize,
        fallback: target::FallbackConfig,
    ) -> target::Targets {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target};

        let providers = (0..n)
            .map(|i| {
                let t = Target::builder()
                    .url(format!("https://p{i}.example.com/").parse().unwrap())
                    .request_timeout_secs(5)
                    .build();
                Provider::new(t, 1)
            })
            .collect();
        let pool = ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            Some(fallback),
            LoadBalanceStrategy::Priority,
            false,
            Vec::new(),
        );
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(alias.to_string(), pool.into());
        target::Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        }
    }

    fn embedded_error_targets(alias: &str, n: usize) -> target::Targets {
        fallback_targets(alias, n, vec![429])
    }

    fn completions_pool_targets(alias: &str, accepts_priority: &[bool]) -> target::Targets {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{FallbackConfig, LoadBalanceStrategy, Target, TargetPools};

        let make_pool = |members: Vec<(String, bool)>| {
            let providers = members
                .into_iter()
                .map(|(u, accepts)| {
                    let t = Target::builder()
                        .url(u.parse().unwrap())
                        .request_timeout_secs(5)
                        .accepts_scheduling_priority(accepts)
                        .build();
                    Provider::new(t, 1)
                })
                .collect();
            ProviderPool::with_config(
                providers,
                None,
                None,
                None,
                Some(FallbackConfig {
                    enabled: true,
                    on_status: vec![429],
                    ..Default::default()
                }),
                LoadBalanceStrategy::Priority,
                false,
                Vec::new(),
            )
        };
        let default_pool = make_pool(vec![("https://chat.example.com/".to_string(), false)]);
        let completions_pool = make_pool(
            accepts_priority
                .iter()
                .enumerate()
                .map(|(i, accepts)| (format!("https://c{i}.example.com/"), *accepts))
                .collect(),
        );
        let mut extra = std::collections::HashMap::new();
        extra.insert("completions".to_string(), completions_pool);
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            alias.to_string(),
            TargetPools::with_pools(default_pool, extra),
        );
        target::Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        }
    }

    #[tokio::test]
    async fn completions_member_sees_priority_iff_it_accepts_it() {
        let error_frame =
            "data: {\"error\":{\"code\":429,\"message\":\"Provider returned error\"}}\n\n"
                .to_string();
        let mock = MockHttpClient::new_streaming_sequence(
            StatusCode::OK,
            vec![
                vec![error_frame],
                vec![OK_CONTENT_FRAME.to_string()],
            ],
        );
        let app_state = AppState::with_client(
            completions_pool_targets("gpt-4", &[true, false]),
            mock.clone(),
        );
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/completions")
            .json(&json!({
                "model": "gpt-4", "prompt": [1, 2, 3], "stream": true, "priority": 100,
                "nvext": {"agent_hints": {"priority": 100}}
            }))
            .await;

        assert_eq!(response.status_code(), 200);
        let requests = mock.get_requests();
        assert_eq!(requests.len(), 2, "primary fails, fallback serves");
        let first: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        let second: serde_json::Value = serde_json::from_slice(&requests[1].body).unwrap();
        assert_eq!(
            first["nvext"]["agent_hints"]["priority"], 100,
            "a member that accepts scheduling fields receives the nvext carrier"
        );
        assert_eq!(first["priority"], 100, "and the legacy top-level field");
        assert!(
            second.get("priority").is_none() && second.get("nvext").is_none(),
            "a member that does not accept them must see neither carrier"
        );
        assert_eq!(
            second["prompt"],
            json!([1, 2, 3]),
            "the rest of the body is untouched"
        );
    }

    #[tokio::test]
    async fn a_non_accepting_primary_never_sees_priority_either() {
        let mock =
            MockHttpClient::new_streaming(StatusCode::OK, vec![OK_CONTENT_FRAME.to_string()]);
        let app_state =
            AppState::with_client(completions_pool_targets("gpt-4", &[false]), mock.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/completions")
            .json(&json!({
                "model": "gpt-4", "prompt": [1, 2, 3], "stream": true,
                "nvext": {"agent_hints": {"priority": 100}}
            }))
            .await;

        assert_eq!(response.status_code(), 200);
        let requests = mock.get_requests();
        assert_eq!(requests.len(), 1);
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert!(
            body.get("nvext").is_none(),
            "index 0 is not 'dynamo'; a non-accepting primary is stripped too"
        );
        assert_eq!(body["prompt"], json!([1, 2, 3]));
    }

    #[tokio::test]
    async fn default_pool_fallback_keeps_priority_on_every_attempt() {
        let error_frame =
            "data: {\"error\":{\"code\":429,\"message\":\"Provider returned error\"}}\n\n"
                .to_string();
        let mock = MockHttpClient::new_streaming_sequence(
            StatusCode::OK,
            vec![vec![error_frame], vec![OK_CONTENT_FRAME.to_string()]],
        );
        let app_state = AppState::with_client(embedded_error_targets("gpt-4", 2), mock.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4", "stream": true, "priority": -1754812800,
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 200);
        let requests = mock.get_requests();
        assert_eq!(requests.len(), 2);
        for (i, req) in requests.iter().enumerate() {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            assert_eq!(
                body["priority"], -1754812800i64,
                "attempt {i}: default-pool members all receive the deadline priority"
            );
        }
    }


    #[tokio::test]
    async fn test_embedded_error_preserves_only_trusted_client_details() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::LoadBalanceStrategy;

        for (provider_trust, pool_trust, preserve) in [
            (Some(true), false, true),
            (Some(false), true, false),
            (None, true, true),
            (None, false, false),
        ] {
            for streaming in [false, true] {
                let body = r#"{"error":{"code":400,"message":"image_url.detail must be auto, low or high","type":"invalid_image_error","param":"image_url.detail","metadata":{"private":"not part of the public error"}}}"#;
                let mock = if streaming {
                    MockHttpClient::new_streaming(
                        StatusCode::OK,
                        vec![": keep-alive\n\n".to_string(), format!("data:{body}\n\n")],
                    )
                } else {
                    MockHttpClient::new(StatusCode::OK, body)
                };
                let target = Target::builder()
                    .url("https://provider.example.com/".parse().unwrap())
                    .maybe_trusted(provider_trust)
                    .build();
                let provider_pool = ProviderPool::with_config(
                    vec![Provider::new(target, 1)],
                    None,
                    None,
                    None,
                    None,
                    LoadBalanceStrategy::Priority,
                    pool_trust,
                    Vec::new(),
                );
                let targets = embedded_error_targets("gpt-4", 1);
                targets
                    .targets
                    .insert("gpt-4".to_string(), provider_pool.into());
                let server =
                    TestServer::new(build_router(AppState::with_client(targets, mock.clone())))
                        .unwrap();
                let response = server
                    .post("/v1/chat/completions")
                    .json(&json!({
                        "model":"gpt-4", "stream":streaming,
                        "messages":[{"role":"user","content":"hello"}]
                    }))
                    .await;
                assert_eq!(response.status_code(), 400);
                let error = response.json::<serde_json::Value>();
                if preserve {
                    assert_eq!(
                        error["error"]["message"],
                        "image_url.detail must be auto, low or high"
                    );
                    assert_eq!(error["error"]["param"], "image_url.detail");
                    assert_eq!(error["error"]["code"], "400");
                    assert_eq!(error["error"]["type"], "invalid_image_error");
                } else {
                    assert_eq!(
                        error["error"]["message"],
                        "The upstream provider rejected the request."
                    );
                    assert!(error["error"]["param"].is_null());
                    assert_eq!(error["error"]["type"], "invalid_request_error");
                }
                assert!(error["error"].get("metadata").is_none());
                assert_eq!(mock.get_requests().len(), 1);
            }
        }
    }

    #[tokio::test]
    async fn test_upstream_rate_limit_preserves_status_and_custom_message() {
        for strict in [false, true] {
            for embedded in [false, true] {
                for streaming in [false, true] {
                    for retry in [false, true] {
                        let body = r#"{"error":{"code":429,"message":"private upstream details"}}"#;
                        let status = if embedded {
                            StatusCode::OK
                        } else {
                            StatusCode::TOO_MANY_REQUESTS
                        };
                        let mock = if streaming {
                            MockHttpClient::new_streaming(status, vec![format!("data: {body}\n\n")])
                        } else {
                            MockHttpClient::new(status, body)
                        };
                        let targets =
                            fallback_targets("gpt-4", 2, if retry { vec![429] } else { vec![] });
                        let state = AppState::with_client(targets, mock.clone())
                            .with_upstream_rate_limit_message(
                                "Contact support for a dedicated deployment.",
                            );
                        let router = if strict {
                            crate::strict::build_strict_router(state)
                        } else {
                            build_router(state)
                        };
                        let server = TestServer::new(router).unwrap();
                        let response = server
                            .post(if strict {
                                "/chat/completions"
                            } else {
                                "/v1/chat/completions"
                            })
                            .json(&json!({
                                "model": "gpt-4", "stream": streaming,
                                "messages": [{"role": "user", "content": "Hello"}]
                            }))
                            .await;
                        assert_eq!(
                            response.status_code(),
                            429,
                            "strict={strict}, embedded={embedded}, streaming={streaming}, retry={retry}"
                        );
                        let error = response.json::<serde_json::Value>();
                        assert_eq!(error["error"]["type"], "rate_limit_error");
                        assert_eq!(error["error"]["code"], "upstream_rate_limit");
                        assert_eq!(
                            error["error"]["message"],
                            "Contact support for a dedicated deployment."
                        );
                        assert!(!response.text().contains("private upstream details"));
                        assert_eq!(mock.get_requests().len(), if retry { 2 } else { 1 });
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_trusted_embedded_error_keeps_server_and_rate_limit_details_private() {
        for code in [429, 500, 501, 503] {
            for streaming in [false, true] {
                let body =
                    json!({"error":{"code":code,"message":"private upstream failure"}}).to_string();
                let mock = if streaming {
                    MockHttpClient::new_streaming(StatusCode::OK, vec![format!("data: {body}\n\n")])
                } else {
                    MockHttpClient::new(StatusCode::OK, &body)
                };
                let targets = embedded_error_targets("gpt-4", 1);
                targets.targets.insert(
                    "gpt-4".to_string(),
                    pool(
                        Target::builder()
                            .url("https://provider.example.com/".parse().unwrap())
                            .trusted(true)
                            .build(),
                    ),
                );
                let server =
                    TestServer::new(build_router(AppState::with_client(targets, mock))).unwrap();
                let response = server
                    .post("/v1/chat/completions")
                    .json(&json!({
                        "model":"gpt-4", "stream":streaming,
                        "messages":[{"role":"user","content":"hello"}]
                    }))
                    .await;
                let expected = match code {
                    429 => 429,
                    503 => 529,
                    _ => 503,
                };
                assert_eq!(response.status_code(), expected);
                assert!(!response.text().contains("private upstream failure"));
            }
        }
    }

    #[tokio::test]
    async fn test_streaming_embedded_error_retries_then_exhausts_to_429() {
        let error_frame =
            "data: {\"error\":{\"code\":429,\"message\":\"Provider returned error\"}}\n\n"
                .to_string();
        let mock = MockHttpClient::new_streaming(StatusCode::OK, vec![error_frame]);
        let app_state = AppState::with_client(embedded_error_targets("gpt-4", 2), mock.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4", "stream": true,
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(
            response.status_code(),
            429,
            "exhausted retries must preserve the upstream 429"
        );
        assert_eq!(
            mock.get_requests().len(),
            2,
            "both providers should be tried"
        );
    }

    #[tokio::test]
    async fn test_streaming_embedded_error_retries_then_succeeds() {
        let error_frame =
            "data: {\"error\":{\"code\":429,\"message\":\"Provider returned error\"}}\n\n"
                .to_string();
        let ok_chunk = "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-4\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n".to_string();
        let done = "data: [DONE]\n\n".to_string();
        let mock = MockHttpClient::new_streaming_sequence(
            StatusCode::OK,
            vec![vec![error_frame], vec![ok_chunk, done]],
        );
        let app_state = AppState::with_client(embedded_error_targets("gpt-4", 2), mock.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4", "stream": true,
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(
            response.status_code(),
            200,
            "a successful retry must be served transparently"
        );
        assert_eq!(
            mock.get_requests().len(),
            2,
            "first attempt errored, second succeeded"
        );
    }

    #[tokio::test]
    async fn test_unary_embedded_rate_limit_preserves_429() {
        let body = r#"{"error":{"code":429,"message":"Provider returned error"}}"#;
        let mock = MockHttpClient::new(StatusCode::OK, body);
        let app_state = AppState::with_client(embedded_error_targets("gpt-4", 2), mock.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 429);
        assert_eq!(
            mock.get_requests().len(),
            2,
            "both providers should be tried"
        );
    }

    #[tokio::test]
    async fn test_streaming_keepalive_before_error_is_still_detected() {
        let keepalive = ": keep-alive\n\n".to_string();
        let error_frame =
            "data: {\"error\":{\"code\":429,\"message\":\"Provider returned error\"}}\n\n"
                .to_string();
        let mock = MockHttpClient::new_streaming(StatusCode::OK, vec![keepalive, error_frame]);
        let app_state = AppState::with_client(embedded_error_targets("gpt-4", 2), mock.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4", "stream": true,
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(
            response.status_code(),
            429,
            "an error after a keep-alive frame must still be detected"
        );
        assert_eq!(mock.get_requests().len(), 2);
    }


    const OK_CONTENT_FRAME: &str = "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n";

    #[tokio::test]
    async fn test_streaming_empty_body_retries_then_succeeds() {
        let mock = MockHttpClient::new_streaming_sequence(
            StatusCode::OK,
            vec![
                vec![],
                vec![OK_CONTENT_FRAME.to_string()],
            ],
        );
        let app_state =
            AppState::with_client(fallback_targets("gpt-4", 2, vec![502]), mock.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4", "stream": true,
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 200, "the retry's success is served");
        assert!(
            response.text().contains("hi"),
            "client receives the healthy provider's content"
        );
        assert_eq!(
            mock.get_requests().len(),
            2,
            "empty stream must trigger a retry"
        );
    }

    #[tokio::test]
    async fn test_slow_streaming_empty_body_retries_then_succeeds() {
        let mock = MockHttpClient::new_delayed_streaming_sequence(
            StatusCode::OK,
            vec![
                (std::time::Duration::from_millis(20), vec![]),
                (
                    std::time::Duration::ZERO,
                    vec![OK_CONTENT_FRAME.to_string()],
                ),
            ],
        );
        let app_state =
            AppState::with_client(fallback_targets("gpt-4", 2, vec![502]), mock.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4", "stream": true,
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 200, "the retry's success is served");
        assert!(
            response.text().contains("hi"),
            "client receives the healthy provider's content"
        );
        assert_eq!(
            mock.get_requests().len(),
            2,
            "slow empty stream must trigger a retry"
        );
    }

    fn first_token_app(mock: MockHttpClient) -> AppState<MockHttpClient> {
        AppState::with_client(fallback_targets("gpt-4", 2, vec![502]), mock)
            .with_first_token_timeout(std::time::Duration::from_millis(100))
    }

    fn chat_request(stream: bool) -> serde_json::Value {
        json!({
            "model": "gpt-4", "stream": stream,
            "messages": [{"role": "user", "content": "Hello"}]
        })
    }

    fn slow_stream(delay: std::time::Duration) -> (std::time::Duration, Vec<String>) {
        (delay, vec![OK_CONTENT_FRAME.to_string()])
    }

    #[tokio::test]
    async fn test_first_token_timeout_fails_over_stalled_stream() {
        let mock = MockHttpClient::new_delayed_streaming_sequence(
            StatusCode::OK,
            vec![
                slow_stream(std::time::Duration::from_secs(30)),
                slow_stream(std::time::Duration::ZERO),
            ],
        );
        let server = TestServer::new(build_router(first_token_app(mock.clone()))).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&chat_request(true))
            .await;

        assert_eq!(response.status_code(), 200);
        assert!(
            response.text().contains("hi"),
            "served by the healthy provider"
        );
        let requests = mock.get_requests();
        assert_eq!(requests.len(), 2, "the stalled stream must fail over");
        assert!(
            requests[1].uri.starts_with("https://p1.example.com/"),
            "failover goes to the next provider, got {}",
            requests[1].uri
        );
    }

    #[tokio::test]
    async fn test_first_token_timeout_ignores_keep_alive_comments() {
        let keep_alive = |ms: u64| {
            (
                std::time::Duration::from_millis(ms),
                ": keep-alive\n\n".to_string(),
            )
        };
        let mock = MockHttpClient::new_timed_streaming_sequence(
            StatusCode::OK,
            vec![
                vec![
                    keep_alive(0),
                    keep_alive(20),
                    keep_alive(20),
                    keep_alive(20),
                    keep_alive(20),
                    (
                        std::time::Duration::from_secs(30),
                        OK_CONTENT_FRAME.to_string(),
                    ),
                ],
                vec![(std::time::Duration::ZERO, OK_CONTENT_FRAME.to_string())],
            ],
        );
        let server = TestServer::new(build_router(first_token_app(mock.clone()))).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&chat_request(true))
            .await;

        assert_eq!(response.status_code(), 200);
        assert!(response.text().contains("hi"));
        assert_eq!(
            mock.get_requests().len(),
            2,
            "keep-alive comments alone must not count as a first token"
        );
    }

    #[tokio::test]
    async fn test_first_token_timeout_spares_the_final_attempt() {
        let slow = slow_stream(std::time::Duration::from_millis(300));
        let mock = MockHttpClient::new_delayed_streaming_sequence(
            StatusCode::OK,
            vec![slow.clone(), slow],
        );
        let server = TestServer::new(build_router(first_token_app(mock.clone()))).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&chat_request(true))
            .await;

        assert_eq!(response.status_code(), 200);
        assert!(
            response.text().contains("hi"),
            "the final attempt is served"
        );
        assert_eq!(mock.get_requests().len(), 2);
    }

    #[tokio::test]
    async fn test_first_token_timeout_exempt_header_opts_out() {
        let mock = MockHttpClient::new_delayed_streaming_sequence(
            StatusCode::OK,
            vec![slow_stream(std::time::Duration::from_millis(300))],
        );
        let app = first_token_app(mock.clone())
            .with_first_token_timeout_exempt_header("x-fusillade-batch-created-at");
        let server = TestServer::new(build_router(app)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .add_header("x-fusillade-batch-created-at", "2026-09-15T00:00:00Z")
            .json(&chat_request(true))
            .await;

        assert_eq!(response.status_code(), 200);
        assert!(response.text().contains("hi"));
        assert_eq!(
            mock.get_requests().len(),
            1,
            "a request carrying the exempt header is never failed over on time"
        );
    }

    #[tokio::test]
    async fn test_first_token_timeout_leaves_non_streaming_alone() {
        let mock = MockHttpClient::new_delayed_streaming_sequence(
            StatusCode::OK,
            vec![slow_stream(std::time::Duration::from_millis(300))],
        );
        let server = TestServer::new(build_router(first_token_app(mock.clone()))).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&chat_request(false))
            .await;

        assert_eq!(response.status_code(), 200);
        assert_eq!(mock.get_requests().len(), 1);
    }

    #[tokio::test]
    async fn test_done_sentinel_is_not_a_first_token_but_is_not_empty() {
        let mock =
            MockHttpClient::new_streaming(StatusCode::OK, vec!["data: [DONE]\n\n".to_string()]);
        let server = TestServer::new(build_router(first_token_app(mock.clone()))).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&chat_request(true))
            .await;

        assert_eq!(response.status_code(), 200);
        assert_eq!(
            mock.get_requests().len(),
            1,
            "a [DONE]-only stream is not empty, so it must not be retried"
        );
    }

    #[tokio::test]
    async fn test_first_token_timeout_needs_a_different_provider() {
        let mock = MockHttpClient::new_delayed_streaming_sequence(
            StatusCode::OK,
            vec![slow_stream(std::time::Duration::from_millis(300))],
        );
        let targets = fallback_targets_with(
            "gpt-4",
            1,
            target::FallbackConfig {
                enabled: true,
                on_status: vec![502],
                max_attempts: Some(3),
                ..Default::default()
            },
        );
        let app = AppState::with_client(targets, mock.clone())
            .with_first_token_timeout(std::time::Duration::from_millis(100));
        let server = TestServer::new(build_router(app)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&chat_request(true))
            .await;

        assert_eq!(response.status_code(), 200);
        assert_eq!(mock.get_requests().len(), 1);
    }

    #[tokio::test]
    async fn test_pool_first_token_timeout_overrides_default() {
        let stalled_then_healthy = || {
            MockHttpClient::new_delayed_streaming_sequence(
                StatusCode::OK,
                vec![
                    slow_stream(std::time::Duration::from_millis(300)),
                    slow_stream(std::time::Duration::ZERO),
                ],
            )
        };
        let pool_timeout = |ms: u64| {
            fallback_targets_with(
                "gpt-4",
                2,
                target::FallbackConfig {
                    enabled: true,
                    on_status: vec![502],
                    first_token_timeout_ms: Some(ms),
                    aimd: None,
                    ..Default::default()
                },
            )
        };

        let mock = stalled_then_healthy();
        let server = TestServer::new(build_router(AppState::with_client(
            pool_timeout(100),
            mock.clone(),
        )))
        .unwrap();
        server
            .post("/v1/chat/completions")
            .json(&chat_request(true))
            .await
            .assert_status_ok();
        assert_eq!(mock.get_requests().len(), 2, "pool timeout arms failover");

        let mock = stalled_then_healthy();
        let app = AppState::with_client(pool_timeout(0), mock.clone())
            .with_first_token_timeout(std::time::Duration::from_millis(100));
        let server = TestServer::new(build_router(app)).unwrap();
        server
            .post("/v1/chat/completions")
            .json(&chat_request(true))
            .await
            .assert_status_ok();
        assert_eq!(mock.get_requests().len(), 1, "pool timeout of 0 disarms it");
    }

    #[tokio::test]
    async fn test_slow_streaming_content_is_not_retried() {
        let mock = MockHttpClient::new_delayed_streaming_sequence(
            StatusCode::OK,
            vec![(
                std::time::Duration::from_millis(20),
                vec![OK_CONTENT_FRAME.to_string()],
            )],
        );
        let app_state =
            AppState::with_client(fallback_targets("gpt-4", 2, vec![502]), mock.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4", "stream": true,
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 200);
        assert!(
            response.text().contains("hi"),
            "client receives the slow provider's content"
        );
        assert_eq!(
            mock.get_requests().len(),
            1,
            "slow content must not trigger a retry"
        );
    }

    #[tokio::test]
    async fn test_idle_stream_after_decisive_wait_is_forwarded_not_retried() {
        let mock = MockHttpClient::new_delayed_streaming_sequence(
            StatusCode::OK,
            vec![(
                std::time::Duration::from_millis(75),
                vec![OK_CONTENT_FRAME.to_string()],
            )],
        );
        let app_state =
            AppState::with_client(fallback_targets("gpt-4", 2, vec![502]), mock.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4", "stream": true,
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 200);
        assert!(
            response.text().contains("hi"),
            "client receives the original provider's delayed content"
        );
        assert_eq!(
            mock.get_requests().len(),
            1,
            "idle-but-open stream must not retry before a terminal empty is observed"
        );
    }

    #[tokio::test]
    async fn test_streaming_empty_body_exhausts_to_503() {
        let mock = MockHttpClient::new_streaming(StatusCode::OK, vec![]);
        let app_state =
            AppState::with_client(fallback_targets("gpt-4", 2, vec![502]), mock.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4", "stream": true,
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 503);
        assert_eq!(
            mock.get_requests().len(),
            2,
            "both providers should be tried"
        );
    }

    #[tokio::test]
    async fn test_unary_empty_body_retries_and_exhausts_to_503() {
        let mock = MockHttpClient::new(StatusCode::OK, "");
        let app_state =
            AppState::with_client(fallback_targets("gpt-4", 2, vec![502]), mock.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 503);
        assert_eq!(
            mock.get_requests().len(),
            2,
            "empty unary body must trigger a retry"
        );
    }

    #[tokio::test]
    async fn test_errors_standing_in_for_an_upstream_are_attributed_to_the_provider() {
        use tower::ServiceExt;

        let origin = |mock: MockHttpClient, targets: target::Targets, model: &'static str| async move {
            let request = axum::http::Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({"model": model, "messages": [{"role": "user", "content": "Hello"}]})
                        .to_string(),
                ))
                .unwrap();
            let response = build_router(AppState::with_client(targets, mock))
                .oneshot(request)
                .await
                .unwrap();
            (
                response.status().as_u16(),
                response.extensions().get::<crate::ErrorOrigin>().cloned(),
            )
        };

        let body = json!({"error": {"code": 500, "message": "upstream failure"}}).to_string();
        let embedded = MockHttpClient::new(StatusCode::OK, &body);
        assert_eq!(
            origin(embedded, embedded_error_targets("gpt-4", 1), "gpt-4").await,
            (503, Some(crate::ErrorOrigin::Provider))
        );

        let body = json!({"error": {"code": 503, "message": "no room"}}).to_string();
        let embedded = MockHttpClient::new(StatusCode::OK, &body);
        assert_eq!(
            origin(embedded, embedded_error_targets("gpt-4", 1), "gpt-4").await,
            (529, Some(crate::ErrorOrigin::Provider))
        );

        let empty = MockHttpClient::new(StatusCode::OK, "");
        assert_eq!(
            origin(empty, fallback_targets("gpt-4", 2, vec![502]), "gpt-4").await,
            (503, Some(crate::ErrorOrigin::Provider))
        );

        let unused = MockHttpClient::new(StatusCode::OK, "");
        assert_eq!(
            origin(unused, embedded_error_targets("gpt-4", 1), "no-such-model").await,
            (
                404,
                Some(crate::ErrorOrigin::Gateway {
                    code: "model_not_found".to_string()
                })
            )
        );
    }

    fn overloaded_mock() -> MockHttpClient {
        MockHttpClient::new(
            StatusCode::from_u16(529).unwrap(),
            r#"{"error":{"message":"overloaded"}}"#,
        )
    }

    fn retrying_fallback(on_status: Vec<u16>, max_attempts: usize) -> target::FallbackConfig {
        target::FallbackConfig {
            enabled: true,
            on_status,
            max_attempts: Some(max_attempts),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_overloaded_members_are_asked_once_and_caller_gets_529() {
        let mock = overloaded_mock();
        let app_state = AppState::with_client(
            fallback_targets_with("gpt-4", 2, retrying_fallback(vec![529], 6)),
            mock.clone(),
        );
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 529);
        assert_eq!(response.header("retry-after"), "1");
        let body: serde_json::Value = response.json();
        assert_eq!(body["error"]["code"], "overloaded");
        assert_eq!(
            mock.get_requests().len(),
            2,
            "each overloaded member is asked once"
        );
    }

    #[tokio::test]
    async fn test_single_overloaded_member_is_not_reasked() {
        let mock = overloaded_mock();
        let app_state = AppState::with_client(
            fallback_targets_with("gpt-4", 1, retrying_fallback(vec![529], 3)),
            mock.clone(),
        );
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 529);
        assert_eq!(mock.get_requests().len(), 1);
    }

    async fn chat(app_state: AppState<MockHttpClient>) -> axum_test::TestResponse {
        TestServer::new(build_router(app_state))
            .unwrap()
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await
    }

    #[tokio::test]
    async fn test_members_answering_503_are_asked_once_and_caller_gets_529() {
        let mock = MockHttpClient::new(StatusCode::SERVICE_UNAVAILABLE, "{}");
        let response = chat(AppState::with_client(
            fallback_targets_with("gpt-4", 2, retrying_fallback(vec![503], 6)),
            mock.clone(),
        ))
        .await;

        assert_eq!(response.status_code(), 529);
        let body: serde_json::Value = response.json();
        assert_eq!(body["error"]["code"], "overloaded");
        assert_eq!(mock.get_requests().len(), 2);
    }

    #[tokio::test]
    async fn test_capacity_refusal_without_failover_reaches_caller_as_529() {
        let mock = MockHttpClient::new(StatusCode::SERVICE_UNAVAILABLE, "{}");
        let response = chat(AppState::with_client(
            fallback_targets("gpt-4", 1, vec![]),
            mock.clone(),
        ))
        .await;

        assert_eq!(response.status_code(), 529);
        assert_eq!(response.header("retry-after"), "1");
    }

    #[tokio::test]
    async fn test_model_with_no_provider_answers_529_no_capacity() {
        let mock = MockHttpClient::new(StatusCode::OK, "{}");
        let response = chat(AppState::with_client(
            fallback_targets_with("gpt-4", 0, retrying_fallback(vec![529], 1)),
            mock.clone(),
        ))
        .await;

        assert_eq!(response.status_code(), 529);
        assert_eq!(response.header("retry-after"), "30");
        let body: serde_json::Value = response.json();
        assert_eq!(body["error"]["code"], "no_capacity");
        assert!(mock.get_requests().is_empty());
    }

    #[tokio::test]
    async fn test_non_overload_failures_still_spend_the_retry_budget() {
        let mock = MockHttpClient::new(StatusCode::INTERNAL_SERVER_ERROR, "{}");
        let app_state = AppState::with_client(
            fallback_targets_with("gpt-4", 1, retrying_fallback(vec![500], 3)),
            mock.clone(),
        );
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 502);
        assert_eq!(mock.get_requests().len(), 3);
    }

    #[tokio::test]
    async fn test_streaming_keepalive_then_token_is_forwarded_not_retried() {
        let keepalive = ": keep-alive\n\n".to_string();
        let mock = MockHttpClient::new_streaming(
            StatusCode::OK,
            vec![keepalive, OK_CONTENT_FRAME.to_string()],
        );
        let app_state =
            AppState::with_client(fallback_targets("gpt-4", 2, vec![502]), mock.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4", "stream": true,
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 200);
        assert!(response.text().contains("hi"));
        assert_eq!(
            mock.get_requests().len(),
            1,
            "a valid stream must not be retried just because the token followed a keep-alive"
        );
    }

    #[tokio::test]
    async fn test_streaming_only_keepalives_so_far_is_forwarded_not_retried() {
        let keepalive = ": keep-alive\n\n".to_string();
        let mock = MockHttpClient::new_streaming(
            StatusCode::OK,
            vec![
                keepalive.clone(),
                keepalive.clone(),
                keepalive.clone(),
                keepalive.clone(),
                keepalive,
            ],
        );
        let app_state =
            AppState::with_client(fallback_targets("gpt-4", 2, vec![502]), mock.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "gpt-4", "stream": true,
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 200);
        assert_eq!(
            mock.get_requests().len(),
            1,
            "keep-alives without a terminal end must not be treated as empty"
        );
    }

    #[tokio::test]
    async fn test_request_and_response_details() {
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "test-model".to_string(),
            pool(
                Target::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .onwards_key("test-api-key".to_string())
                    .build(),
            ),
        );

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let mock_response_body = r#"{"id": "test-response", "object": "chat.completion", "choices": [{"message": {"content": "Hello from mock!"}}]}"#;
        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response_body);
        let app_state = AppState::with_client(targets, mock_client.clone());
        let router = build_router(app_state);
        let server = TestServer::new(router).unwrap();

        let request_body = json!({
            "model": "test-model",
            "messages": [{"role": "user", "content": "Hello!"}],
            "temperature": 0.7
        });

        let response = server
            .post("/v1/chat/completions")
            .json(&request_body)
            .await;

        assert_eq!(response.status_code(), 200);
        let response_body: serde_json::Value = response.json();
        assert_eq!(response_body["id"], "test-response");
        assert_eq!(
            response_body["choices"][0]["message"]["content"],
            "Hello from mock!"
        );

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 1);

        let request = &requests[0];

        assert_eq!(request.method, "POST");

        assert_eq!(request.uri, "https://api.example.com/v1/chat/completions");

        let auth_header = request
            .headers
            .iter()
            .find(|(key, _)| key == "authorization")
            .map(|(_, value)| value);
        assert_eq!(auth_header, Some(&"Bearer test-api-key".to_string()));

        let host_header = request
            .headers
            .iter()
            .find(|(key, _)| key == "host")
            .map(|(_, value)| value);
        assert_eq!(host_header, Some(&"api.example.com".to_string()));

        let content_type_header = request
            .headers
            .iter()
            .find(|(key, _)| key == "content-type")
            .map(|(_, value)| value);
        assert_eq!(content_type_header, Some(&"application/json".to_string()));

        let forwarded_body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(forwarded_body["model"], "test-model");
        assert_eq!(forwarded_body["messages"][0]["content"], "Hello!");
        assert_eq!(forwarded_body["temperature"], 0.7);
    }

    #[tokio::test]
    async fn test_rewritten_body_keeps_key_order() {
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "test-model".to_string(),
            pool(
                Target::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .onwards_model("upstream-model".to_string())
                    .build(),
            ),
        );
        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };
        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"choices": []}"#);
        let app_state = AppState::with_client(targets, mock_client.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let schema = r#"{"type":"object","properties":{"reasoning":{"type":"string"},"answer":{"type":"string"}},"required":["reasoning","answer"],"additionalProperties":false}"#;
        let body = format!(
            r#"{{"model":"test-model","messages":[{{"role":"user","content":"Hello!"}}],"response_format":{{"type":"json_schema","json_schema":{{"name":"answer","strict":true,"schema":{schema}}}}},"temperature":0.7}}"#
        );
        let response = server
            .post("/v1/chat/completions")
            .content_type("application/json")
            .bytes(body.clone().into())
            .await;
        assert_eq!(response.status_code(), 200);

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 1);
        let forwarded = String::from_utf8(requests[0].body.clone()).unwrap();
        assert_eq!(
            forwarded,
            body.replace(r#""model":"test-model""#, r#""model":"upstream-model""#)
        );
    }

    #[tokio::test]
    async fn test_model_override_header_takes_precedence() {
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "header-model".to_string(),
            pool(
                Target::builder()
                    .url("https://api.header.com".parse().unwrap())
                    .onwards_key("header-key".to_string())
                    .build(),
            ),
        );
        targets_map.insert(
            "body-model".to_string(),
            pool(
                Target::builder()
                    .url("https://api.body.com".parse().unwrap())
                    .onwards_key("body-key".to_string())
                    .build(),
            ),
        );

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let app_state = AppState::with_client(targets, mock_client.clone());
        let router = build_router(app_state);
        let server = TestServer::new(router).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .add_header("model-override", "header-model")
            .json(&json!({
                "model": "body-model",
                "messages": [{"role": "user", "content": "Test"}]
            }))
            .await;

        assert_eq!(response.status_code(), 200);

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 1);

        let request = &requests[0];
        assert!(request.uri.contains("api.header.com"));
        assert!(!request.uri.contains("api.body.com"));

        let auth_header = request
            .headers
            .iter()
            .find(|(key, _)| key == "authorization")
            .map(|(_, value)| value);
        assert_eq!(auth_header, Some(&"Bearer header-key".to_string()));
    }

    #[tokio::test]
    async fn test_models_endpoint_returns_proper_model_list() {
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "gpt-4".to_string(),
            pool(
                Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .onwards_key("sk-openai-key".to_string())
                    .build(),
            ),
        );
        targets_map.insert(
            "claude-3".to_string(),
            pool(
                Target::builder()
                    .url("https://api.anthropic.com".parse().unwrap())
                    .onwards_key("sk-ant-key".to_string())
                    .build(),
            ),
        );
        targets_map.insert(
            "gemini-pro".to_string(),
            pool(
                Target::builder()
                    .url("https://api.google.com".parse().unwrap())
                    .onwards_model("gemini-1.5-pro".to_string())
                    .build(),
            ),
        );

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"unused": "response"}"#);
        let app_state = AppState::with_client(targets, mock_client.clone());
        let router = build_router(app_state);
        let server = TestServer::new(router).unwrap();

        let response = server.get("/v1/models").await;

        assert_eq!(response.status_code(), 200);

        let response_body: serde_json::Value = response.json();

        assert_eq!(response_body["object"], "list");
        assert!(response_body["data"].is_array());

        let models = response_body["data"].as_array().unwrap();
        assert_eq!(models.len(), 3);

        let model_ids: Vec<&str> = models
            .iter()
            .map(|model| model["id"].as_str().unwrap())
            .collect();

        assert!(model_ids.contains(&"gpt-4"));
        assert!(model_ids.contains(&"claude-3"));
        assert!(model_ids.contains(&"gemini-pro"));

        for model in models {
            assert_eq!(model["object"], "model");
            assert_eq!(model["owned_by"], "None");
            assert!(model["id"].is_string());
        }

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 0);
    }

    #[tokio::test]
    async fn test_config_file_keys_authenticate_by_hash() {
        use crate::auth::KeyHash;

        let pre_hashed = serde_json::to_value(KeyHash::of("pre-hashed-key")).unwrap();
        let config: target::ConfigFile = serde_json::from_value(json!({
            "auth": {
                "global_keys": [pre_hashed],
                "key_definitions": {
                    "labelled": { "key": "labelled-key", "labels": { "api_key_id": "00000000-0000-0000-0000-000000000001" } }
                }
            },
            "targets": {
                "gpt-4": { "url": "https://api.openai.com", "keys": ["plain-key", "labelled-key"] }
            }
        }))
        .unwrap();
        let targets = Targets::from_config(config).unwrap();
        assert!(
            targets
                .key_labels
                .contains_key(&KeyHash::of("labelled-key"))
        );

        let app_state = AppState::with_client(targets, MockHttpClient::new(StatusCode::OK, "{}"));
        let server = TestServer::new(build_router(app_state)).unwrap();
        let body = json!({"model": "gpt-4", "messages": [{"role": "user", "content": "Hello"}]});
        for (token, expected) in [
            ("plain-key", 200),
            ("labelled-key", 200),
            ("pre-hashed-key", 200),
            ("wrong-key", 403),
            (pre_hashed.as_str().unwrap(), 403),
        ] {
            let response = server
                .post("/v1/chat/completions")
                .add_header("authorization", format!("Bearer {token}"))
                .json(&body)
                .await;
            assert_eq!(response.status_code(), expected, "token {token}");
        }
    }

    #[tokio::test]
    async fn test_models_endpoint_filters_by_bearer_token() {
        use crate::auth::KeyHash;
        use std::collections::HashSet;

        let mut gpt4_keys = HashSet::new();
        gpt4_keys.insert(KeyHash::from("gpt4-token".to_string()));

        let mut claude_keys = HashSet::new();
        claude_keys.insert(KeyHash::from("claude-token".to_string()));

        let targets_map = Arc::new(DashMap::new());

        targets_map.insert(
            "gpt-4".to_string(),
            pool(
                Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .keys(gpt4_keys)
                    .build(),
            ),
        );

        targets_map.insert(
            "claude-3".to_string(),
            pool(
                Target::builder()
                    .url("https://api.anthropic.com".parse().unwrap())
                    .keys(claude_keys)
                    .build(),
            ),
        );

        targets_map.insert(
            "gemini-pro".to_string(),
            pool(
                Target::builder()
                    .url("https://api.google.com".parse().unwrap())
                    .build(),
            ),
        );

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"unused": "response"}"#);
        let app_state = AppState::with_client(targets, mock_client.clone());
        let router = build_router(app_state);
        let server = TestServer::new(router).unwrap();

        let response = server.get("/v1/models").await;
        assert_eq!(response.status_code(), 200);

        let response_body: serde_json::Value = response.json();
        let models = response_body["data"].as_array().unwrap();
        assert_eq!(models.len(), 1);

        let model_ids: Vec<&str> = models
            .iter()
            .map(|model| model["id"].as_str().unwrap())
            .collect();
        assert!(model_ids.contains(&"gemini-pro"));

        let response = server
            .get("/v1/models")
            .add_header("authorization", "Bearer gpt4-token")
            .await;
        assert_eq!(response.status_code(), 200);

        let response_body: serde_json::Value = response.json();
        let models = response_body["data"].as_array().unwrap();
        assert_eq!(models.len(), 2);

        let model_ids: Vec<&str> = models
            .iter()
            .map(|model| model["id"].as_str().unwrap())
            .collect();
        assert!(model_ids.contains(&"gpt-4"));
        assert!(model_ids.contains(&"gemini-pro"));

        let response = server
            .get("/v1/models")
            .add_header("authorization", "Bearer claude-token")
            .await;
        assert_eq!(response.status_code(), 200);

        let response_body: serde_json::Value = response.json();
        let models = response_body["data"].as_array().unwrap();
        assert_eq!(models.len(), 2);

        let model_ids: Vec<&str> = models
            .iter()
            .map(|model| model["id"].as_str().unwrap())
            .collect();
        assert!(model_ids.contains(&"claude-3"));
        assert!(model_ids.contains(&"gemini-pro"));

        let response = server
            .get("/v1/models")
            .add_header("authorization", "Bearer invalid-token")
            .await;
        assert_eq!(response.status_code(), 200);

        let response_body: serde_json::Value = response.json();
        let models = response_body["data"].as_array().unwrap();
        assert_eq!(models.len(), 1);

        let model_ids: Vec<&str> = models
            .iter()
            .map(|model| model["id"].as_str().unwrap())
            .collect();
        assert!(model_ids.contains(&"gemini-pro"));
    }

    #[tokio::test]
    async fn test_rate_limiting_blocks_requests() {
        use crate::target::{RateLimitExceeded, RateLimiter, Target, Targets};
        use std::sync::Arc;

        #[derive(Debug)]
        struct BlockingRateLimiter;

        impl RateLimiter for BlockingRateLimiter {
            fn check(&self) -> Result<(), RateLimitExceeded> {
                Err(RateLimitExceeded)
            }
        }

        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "rate-limited-model".to_string(),
            pool(
                Target::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .limiter(Arc::new(BlockingRateLimiter) as Arc<dyn RateLimiter>)
                    .build(),
            ),
        );

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let app_state = AppState::with_client(targets, mock_client.clone());
        let router = build_router(app_state);
        let server = TestServer::new(router).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "rate-limited-model",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 429);

        let response_body: serde_json::Value = response.json();
        assert_eq!(response_body["error"]["type"], "rate_limit_error");
        assert_eq!(response_body["error"]["code"], "rate_limit");

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 0);
    }

    #[tokio::test]
    async fn test_rate_limiting_allows_requests() {
        use crate::target::{RateLimitExceeded, RateLimiter, Target, Targets};
        use std::sync::Arc;

        #[derive(Debug)]
        struct AllowingRateLimiter;

        impl RateLimiter for AllowingRateLimiter {
            fn check(&self) -> Result<(), RateLimitExceeded> {
                Ok(())
            }
        }

        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "rate-limited-model".to_string(),
            pool(
                Target::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .limiter(Arc::new(AllowingRateLimiter) as Arc<dyn RateLimiter>)
                    .build(),
            ),
        );

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let app_state = AppState::with_client(targets, mock_client.clone());
        let router = build_router(app_state);
        let server = TestServer::new(router).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "rate-limited-model",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code(), 200);

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].uri.contains("api.example.com"));
    }

    #[tokio::test]
    async fn test_rate_limiting_with_mixed_targets() {
        use crate::target::{RateLimitExceeded, RateLimiter, Target, Targets};
        use std::sync::Arc;

        #[derive(Debug)]
        struct BlockingRateLimiter;
        impl RateLimiter for BlockingRateLimiter {
            fn check(&self) -> Result<(), RateLimitExceeded> {
                Err(RateLimitExceeded)
            }
        }

        #[derive(Debug)]
        struct AllowingRateLimiter;
        impl RateLimiter for AllowingRateLimiter {
            fn check(&self) -> Result<(), RateLimitExceeded> {
                Ok(())
            }
        }

        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "blocked-model".to_string(),
            pool(
                Target::builder()
                    .url("https://blocked.example.com".parse().unwrap())
                    .limiter(Arc::new(BlockingRateLimiter) as Arc<dyn RateLimiter>)
                    .build(),
            ),
        );
        targets_map.insert(
            "allowed-model".to_string(),
            pool(
                Target::builder()
                    .url("https://allowed.example.com".parse().unwrap())
                    .limiter(Arc::new(AllowingRateLimiter) as Arc<dyn RateLimiter>)
                    .build(),
            ),
        );
        targets_map.insert(
            "unlimited-model".to_string(),
            pool(
                Target::builder()
                    .url("https://unlimited.example.com".parse().unwrap())
                    .build(),
            ),
        );

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let app_state = AppState::with_client(targets, mock_client.clone());
        let router = build_router(app_state);
        let server = TestServer::new(router).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "blocked-model",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;
        assert_eq!(response.status_code(), 429);

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "allowed-model",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;
        assert_eq!(response.status_code(), 200);

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "unlimited-model",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;
        assert_eq!(response.status_code(), 200);

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 2);

        let urls: Vec<&str> = requests.iter().map(|r| r.uri.as_str()).collect();
        assert!(urls.contains(&"https://allowed.example.com/v1/chat/completions"));
        assert!(urls.contains(&"https://unlimited.example.com/v1/chat/completions"));
        assert!(!urls.iter().any(|&url| url.contains("blocked.example.com")));
    }

    #[tokio::test]
    async fn test_concurrency_limiting_below_limits() {
        let targets_map = Arc::new(DashMap::new());
        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .build();
        targets_map.insert(
            "limited-model".to_string(),
            ProviderPool::new(vec![Provider::with_concurrency_limit(target, 1, 5)]).into(),
        );

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let app_state = AppState::with_client(targets, mock_client.clone());
        let router = build_router(app_state);
        let server = TestServer::new(router).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "limited-model",
                "messages": [{"role": "user", "content": "Test"}]
            }))
            .await;

        assert_eq!(response.status_code(), 200);

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 1);
    }

    #[tokio::test]
    async fn test_concurrency_limiting_at_limits() {
        use std::rc::Rc;

        let targets_map = Arc::new(DashMap::new());
        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .build();
        targets_map.insert(
            "limited-model".to_string(),
            ProviderPool::with_config(
                vec![Provider::new(target, 1)],
                None,
                None,
                Some(ConcurrencyLimiter::with_limit(1)),
                None,
                target::LoadBalanceStrategy::default(),
                false,
                Vec::new(),
            )
            .into(),
        );

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let mock_client =
            test_utils::TriggeredMockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let app_state = AppState::with_client(targets, mock_client.clone());
        let router = build_router(app_state);
        let server = Rc::new(TestServer::new(router).unwrap());

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let server_clone = Rc::clone(&server);
                let handle1 = tokio::task::spawn_local(async move {
                    server_clone
                        .post("/v1/chat/completions")
                        .json(&json!({"model": "limited-model", "messages": []}))
                        .await
                });

                tokio::time::sleep(std::time::Duration::from_millis(10)).await;

                let response2 = server
                    .post("/v1/chat/completions")
                    .json(&json!({"model": "limited-model", "messages": []}))
                    .await;

                assert_eq!(response2.status_code(), 529);
                let body: serde_json::Value = response2.json();
                assert_eq!(body["error"]["code"], "overloaded");

                mock_client.complete_request(0);
                let response1 = handle1.await.unwrap();
                assert_eq!(response1.status_code(), 200);

                assert_eq!(mock_client.get_requests().len(), 1);
            })
            .await;
    }

    #[tokio::test]
    async fn test_per_key_concurrency_limiting() {
        use std::rc::Rc;

        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "test-model".to_string(),
            pool(
                Target::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .build(),
            ),
        );

        let key_concurrency_limiters = Arc::new(DashMap::new());
        key_concurrency_limiters.insert(
            KeyHash::of("sk-limited-key"),
            ConcurrencyLimiter::with_limit(1),
        );

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters,
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let mock_client =
            test_utils::TriggeredMockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let app_state = AppState::with_client(targets, mock_client.clone());
        let router = build_router(app_state);
        let server = Rc::new(TestServer::new(router).unwrap());

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let server_clone = Rc::clone(&server);
                let handle1 = tokio::task::spawn_local(async move {
                    server_clone
                        .post("/v1/chat/completions")
                        .add_header(
                            axum::http::HeaderName::from_static("authorization"),
                            axum::http::HeaderValue::from_static("Bearer sk-limited-key"),
                        )
                        .json(&json!({"model": "test-model", "messages": []}))
                        .await
                });

                tokio::time::sleep(std::time::Duration::from_millis(10)).await;

                let response2 = server
                    .post("/v1/chat/completions")
                    .add_header(
                        axum::http::HeaderName::from_static("authorization"),
                        axum::http::HeaderValue::from_static("Bearer sk-limited-key"),
                    )
                    .json(&json!({"model": "test-model", "messages": []}))
                    .await;

                assert_eq!(response2.status_code(), 429);
                let body: serde_json::Value = response2.json();
                assert_eq!(body["error"]["code"], "concurrency_limit_exceeded");

                mock_client.complete_request(0);
                let response1 = handle1.await.unwrap();
                assert_eq!(response1.status_code(), 200);

                assert_eq!(mock_client.get_requests().len(), 1);
            })
            .await;
    }

    fn inflight_limited_targets(limits: inflight::InflightLimits) -> Targets {
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "limited-model".to_string(),
            ProviderPool::new(vec![Provider::new(
                Target::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .build(),
                1,
            )])
            .with_inflight_limits(Some(limits))
            .into(),
        );
        let key_labels = Arc::new(DashMap::new());
        for (key, account) in [
            ("sk-acct-a", "acct-a"),
            ("sk-acct-a-second", "acct-a"),
            ("sk-acct-b", "acct-b"),
        ] {
            key_labels.insert(
                KeyHash::of(key),
                std::collections::HashMap::from([(
                    serving::ACCOUNT_LABEL.to_string(),
                    account.to_string(),
                )]),
            );
        }
        Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels,
            key_ids: Default::default(),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        }
    }

    fn limited_chat_request(key: &str) -> axum::extract::Request {
        inflight_chat_request("limited-model", key)
    }

    fn inflight_chat_request(model: &str, key: &str) -> axum::extract::Request {
        axum::extract::Request::builder()
            .uri("/v1/chat/completions")
            .method("POST")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {key}"))
            .body(axum::body::Body::from(
                json!({"model": model, "messages": []}).to_string(),
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn test_inflight_limit_holds_the_slot_until_the_response_body_finishes() {
        use http_body_util::BodyExt;
        use tower::ServiceExt;

        let targets = inflight_limited_targets(inflight::InflightLimits {
            default: 1,
            accounts: std::collections::HashMap::new(),
        });
        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let router = build_router(AppState::with_client(targets, mock_client.clone()));

        let first = router
            .clone()
            .oneshot(limited_chat_request("sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let refused = router
            .clone()
            .oneshot(limited_chat_request("sk-acct-a-second"))
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(refused.headers()["retry-after"], "1");
        let refused_body: serde_json::Value =
            serde_json::from_slice(&refused.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(refused_body["error"]["code"], "inflight_limit_exceeded");

        first.into_body().collect().await.unwrap();

        let after = router
            .clone()
            .oneshot(limited_chat_request("sk-acct-a-second"))
            .await
            .unwrap();
        assert_eq!(after.status(), StatusCode::OK);
        assert_eq!(mock_client.get_requests().len(), 2);
    }

    #[tokio::test]
    async fn test_inflight_limit_counts_each_account_against_its_own_limit() {
        use tower::ServiceExt;

        let targets = inflight_limited_targets(inflight::InflightLimits {
            default: 1,
            accounts: std::collections::HashMap::from([("acct-b".to_string(), 2)]),
        });
        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let router = build_router(AppState::with_client(targets, mock_client.clone()));

        let mut held = Vec::new();
        for key in ["sk-acct-a", "sk-acct-b", "sk-acct-b"] {
            let response = router
                .clone()
                .oneshot(limited_chat_request(key))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            held.push(response);
        }

        for key in ["sk-acct-a", "sk-acct-b"] {
            let response = router
                .clone()
                .oneshot(limited_chat_request(key))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        }
        assert_eq!(mock_client.get_requests().len(), 3);
    }

    #[tokio::test]
    async fn test_inflight_limit_skips_requests_carrying_the_exempt_header() {
        use tower::ServiceExt;

        let targets = inflight_limited_targets(inflight::InflightLimits {
            default: 1,
            accounts: std::collections::HashMap::new(),
        });
        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let router = build_router(
            AppState::with_client(targets, mock_client.clone())
                .with_first_token_timeout_exempt_header("x-dispatched"),
        );
        let exempt_request = || {
            let mut request = limited_chat_request("sk-acct-a");
            request
                .headers_mut()
                .insert("x-dispatched", axum::http::HeaderValue::from_static("1"));
            request
        };

        let mut held = Vec::new();
        for _ in 0..2 {
            let response = router.clone().oneshot(exempt_request()).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            held.push(response);
        }
        let realtime = router
            .clone()
            .oneshot(limited_chat_request("sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(realtime.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_inflight_limit_counts_a_redirected_request_against_the_model_it_named() {
        use crate::target::{LoadBalanceStrategy, RoutingAction, RoutingRule};
        use http_body_util::BodyExt;
        use tower::ServiceExt;

        let provider = || {
            Provider::new(
                Target::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .build(),
                1,
            )
        };
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "named-model".to_string(),
            ProviderPool::with_config(
                vec![provider()],
                None,
                None,
                None,
                None,
                LoadBalanceStrategy::default(),
                false,
                vec![RoutingRule {
                    match_labels: std::collections::HashMap::from([(
                        "purpose".to_string(),
                        "realtime".to_string(),
                    )]),
                    action: RoutingAction::Redirect {
                        target: "served-model".to_string(),
                    },
                }],
            )
            .with_inflight_limits(Some(inflight::InflightLimits {
                default: 1,
                accounts: std::collections::HashMap::new(),
            }))
            .into(),
        );
        targets_map.insert(
            "served-model".to_string(),
            ProviderPool::new(vec![provider()])
                .with_inflight_limits(Some(inflight::InflightLimits {
                    default: 5,
                    accounts: std::collections::HashMap::new(),
                }))
                .into(),
        );
        let key_labels = Arc::new(DashMap::new());
        key_labels.insert(
            KeyHash::of("sk-acct-a"),
            std::collections::HashMap::from([
                (serving::ACCOUNT_LABEL.to_string(), "acct-a".to_string()),
                ("purpose".to_string(), "realtime".to_string()),
            ]),
        );
        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels,
            key_ids: Default::default(),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };
        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let router = build_router(AppState::with_client(targets, mock_client.clone()));

        let redirected = router
            .clone()
            .oneshot(inflight_chat_request("named-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(redirected.status(), StatusCode::OK);

        let refused = router
            .clone()
            .oneshot(inflight_chat_request("named-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
        let refused_body: serde_json::Value =
            serde_json::from_slice(&refused.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert!(
            refused_body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("'named-model'")
        );

        let direct = router
            .clone()
            .oneshot(inflight_chat_request("served-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(direct.status(), StatusCode::OK);
        assert_eq!(mock_client.get_requests().len(), 2);
    }

    fn batch_limited_targets(batch_limit: Option<u32>, realtime_limit: Option<u32>) -> Targets {
        let targets_map = Arc::new(DashMap::new());
        let mut pool = ProviderPool::new(vec![Provider::new(
            Target::builder()
                .url("https://api.example.com".parse().unwrap())
                .build(),
            1,
        )])
        .with_batch_inflight_limit(batch_limit);
        if let Some(limit) = realtime_limit {
            pool = pool.with_inflight_limits(Some(inflight::InflightLimits {
                default: limit,
                accounts: std::collections::HashMap::new(),
            }));
        }
        targets_map.insert("batch-model".to_string(), pool.into());
        let key_labels = Arc::new(DashMap::new());
        key_labels.insert(
            KeyHash::of("sk-acct-a"),
            std::collections::HashMap::from([(
                serving::ACCOUNT_LABEL.to_string(),
                "acct-a".to_string(),
            )]),
        );
        Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels,
            key_ids: Default::default(),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        }
    }

    fn dispatched_chat_request(model: &str, key: &str) -> axum::extract::Request {
        let mut request = inflight_chat_request(model, key);
        request
            .headers_mut()
            .insert("x-dispatched", axum::http::HeaderValue::from_static("1"));
        request
    }

    fn dispatched_request(model: &str) -> axum::extract::Request {
        axum::extract::Request::builder()
            .uri("/v1/chat/completions")
            .method("POST")
            .header("content-type", "application/json")
            .header("x-dispatched", "1")
            .body(axum::body::Body::from(
                json!({"model": model, "messages": []}).to_string(),
            ))
            .unwrap()
    }

    #[derive(Debug, Default)]
    struct CountingBatchLimiter {
        calls: std::sync::Mutex<Vec<(String, String)>>,
    }

    impl CountingBatchLimiter {
        fn calls(&self) -> Vec<(String, String)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl inflight::InflightLimiter for CountingBatchLimiter {
        fn try_acquire<'a>(
            &'a self,
            account: &'a str,
            model: &'a str,
            _limit: u32,
        ) -> futures_util::future::BoxFuture<'a, Option<inflight::InflightSlot>> {
            self.calls
                .lock()
                .unwrap()
                .push((account.to_owned(), model.to_owned()));
            Box::pin(std::future::ready(Some(inflight::InflightSlot::new(()))))
        }
    }

    fn batch_router_with_limiter(
        targets: Targets,
        mock_client: MockHttpClient,
        enforce: bool,
        limiter: Arc<dyn inflight::InflightLimiter>,
    ) -> Router {
        build_router(
            AppState::with_client(targets, mock_client)
                .with_first_token_timeout_exempt_header("x-dispatched")
                .with_batch_inflight_enforce(enforce)
                .with_batch_inflight_limiter(limiter),
        )
    }

    fn fallback_batch_targets(
        alias: &str,
        n: usize,
        fallback: target::FallbackConfig,
        batch_limit: Option<u32>,
    ) -> target::Targets {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target};

        let providers = (0..n)
            .map(|i| {
                let t = Target::builder()
                    .url(format!("https://p{i}.example.com/").parse().unwrap())
                    .request_timeout_secs(5)
                    .build();
                Provider::new(t, 1)
            })
            .collect();
        let pool = ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            Some(fallback),
            LoadBalanceStrategy::Priority,
            false,
            Vec::new(),
        )
        .with_batch_inflight_limit(batch_limit);
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(alias.to_string(), pool.into());
        target::Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        }
    }

    fn batch_router(targets: Targets, mock_client: MockHttpClient, enforce: bool) -> Router {
        build_router(
            AppState::with_client(targets, mock_client)
                .with_first_token_timeout_exempt_header("x-dispatched")
                .with_batch_inflight_enforce(enforce),
        )
    }

    #[tokio::test]
    async fn test_batch_inflight_cap_admits_up_to_the_limit() {
        use tower::ServiceExt;

        let targets = batch_limited_targets(Some(2), None);
        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let router = batch_router(targets, mock_client.clone(), true);

        let mut held = Vec::new();
        for _ in 0..2 {
            let response = router
                .clone()
                .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            held.push(response);
        }
        assert_eq!(mock_client.get_requests().len(), 2);
    }

    #[tokio::test]
    async fn test_batch_inflight_cap_refuses_at_the_cap_with_529() {
        use http_body_util::BodyExt;
        use tower::ServiceExt;

        let targets = batch_limited_targets(Some(1), None);
        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let router = batch_router(targets, mock_client.clone(), true);

        let admitted = router
            .clone()
            .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(admitted.status(), StatusCode::OK);

        let refused = router
            .clone()
            .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::from_u16(529).unwrap());
        assert_eq!(refused.headers()["retry-after"], "1");
        let refused_body: serde_json::Value =
            serde_json::from_slice(&refused.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(refused_body["error"]["type"], "overloaded_error");
        assert_eq!(refused_body["error"]["code"], "batch_capacity_exceeded");
        let message = refused_body["error"]["message"].as_str().unwrap();
        assert!(
            message.contains("'batch-model'"),
            "names the model: {message}"
        );
        assert!(message.contains('1'), "names the cap: {message}");
        assert_eq!(mock_client.get_requests().len(), 1);
    }

    #[tokio::test]
    async fn test_batch_inflight_slot_frees_when_body_finishes_or_drops() {
        use http_body_util::BodyExt;
        use tower::ServiceExt;

        let targets = batch_limited_targets(Some(1), None);
        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let router = batch_router(targets, mock_client.clone(), true);

        let first = router
            .clone()
            .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();
        let refused = router
            .clone()
            .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::from_u16(529).unwrap());
        first.into_body().collect().await.unwrap();

        let after = router
            .clone()
            .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(after.status(), StatusCode::OK);
        drop(after);

        let held = router
            .clone()
            .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(held.status(), StatusCode::OK);
        drop(held);
        let after_drop = router
            .clone()
            .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(after_drop.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_realtime_requests_never_touch_the_batch_cap() {
        use tower::ServiceExt;

        let targets = batch_limited_targets(Some(1), None);
        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let router = batch_router(targets, mock_client.clone(), true);

        let _batch = router
            .clone()
            .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();

        let realtime = router
            .clone()
            .oneshot(inflight_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(realtime.status(), StatusCode::OK);

        let refused = router
            .clone()
            .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::from_u16(529).unwrap());
    }

    #[tokio::test]
    async fn test_batch_inflight_null_cap_is_unlimited() {
        use tower::ServiceExt;

        let targets = batch_limited_targets(None, None);
        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let router = batch_router(targets, mock_client.clone(), true);

        let mut held = Vec::new();
        for _ in 0..5 {
            let response = router
                .clone()
                .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            held.push(response);
        }
        assert_eq!(mock_client.get_requests().len(), 5);
    }

    #[tokio::test]
    async fn test_batch_inflight_enforcement_off_never_refuses() {
        use tower::ServiceExt;

        let targets = batch_limited_targets(Some(1), None);
        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let router = batch_router(targets, mock_client.clone(), false);

        let mut held = Vec::new();
        for _ in 0..3 {
            let response = router
                .clone()
                .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            held.push(response);
        }
        assert_eq!(mock_client.get_requests().len(), 3);
    }

    #[tokio::test]
    async fn test_batch_inflight_enforcement_off_does_not_consult_the_limiter() {
        use tower::ServiceExt;

        let on_limiter = Arc::new(CountingBatchLimiter::default());
        let on_router = batch_router_with_limiter(
            batch_limited_targets(Some(1), None),
            MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#),
            true,
            on_limiter.clone(),
        );
        let _held = on_router
            .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(
            on_limiter.calls().len(),
            1,
            "enforcement on consults the limiter"
        );

        let off_limiter = Arc::new(CountingBatchLimiter::default());
        let off_router = batch_router_with_limiter(
            batch_limited_targets(Some(1), None),
            MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#),
            false,
            off_limiter.clone(),
        );
        for _ in 0..3 {
            let response = off_router
                .clone()
                .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
        assert!(
            off_limiter.calls().is_empty(),
            "enforcement off must never call try_acquire: {:?}",
            off_limiter.calls()
        );
    }

    #[tokio::test]
    async fn test_batch_inflight_counts_a_redirected_request_against_the_model_it_named() {
        use crate::target::{LoadBalanceStrategy, RoutingAction, RoutingRule};
        use http_body_util::BodyExt;
        use tower::ServiceExt;

        let provider = || {
            Provider::new(
                Target::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .build(),
                1,
            )
        };
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "named-model".to_string(),
            ProviderPool::with_config(
                vec![provider()],
                None,
                None,
                None,
                None,
                LoadBalanceStrategy::default(),
                false,
                vec![RoutingRule {
                    match_labels: std::collections::HashMap::from([(
                        "purpose".to_string(),
                        "batch".to_string(),
                    )]),
                    action: RoutingAction::Redirect {
                        target: "served-model".to_string(),
                    },
                }],
            )
            .with_batch_inflight_limit(Some(1))
            .into(),
        );
        targets_map.insert(
            "served-model".to_string(),
            ProviderPool::new(vec![provider()])
                .with_batch_inflight_limit(Some(5))
                .into(),
        );
        let key_labels = Arc::new(DashMap::new());
        key_labels.insert(
            KeyHash::of("sk-acct-a"),
            std::collections::HashMap::from([
                (serving::ACCOUNT_LABEL.to_string(), "acct-a".to_string()),
                ("purpose".to_string(), "batch".to_string()),
            ]),
        );
        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels,
            key_ids: Default::default(),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };
        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let router = batch_router(targets, mock_client.clone(), true);

        let redirected = router
            .clone()
            .oneshot(dispatched_chat_request("named-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(redirected.status(), StatusCode::OK);

        let refused = router
            .clone()
            .oneshot(dispatched_chat_request("named-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::from_u16(529).unwrap());
        let refused_body: serde_json::Value =
            serde_json::from_slice(&refused.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(refused_body["error"]["code"], "batch_capacity_exceeded");
        assert!(
            refused_body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("'named-model'")
        );

        let direct = router
            .clone()
            .oneshot(dispatched_chat_request("served-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(direct.status(), StatusCode::OK);
        assert_eq!(mock_client.get_requests().len(), 2);
    }

    #[tokio::test]
    async fn test_batch_failover_acquires_only_one_slot() {
        use tower::ServiceExt;

        let targets = fallback_batch_targets("gpt-4", 2, retrying_fallback(vec![502], 2), Some(1));
        let mock_client =
            MockHttpClient::new(StatusCode::BAD_GATEWAY, r#"{"error":{"message":"bad"}}"#);
        let limiter = Arc::new(CountingBatchLimiter::default());
        let router = batch_router_with_limiter(targets, mock_client.clone(), true, limiter.clone());

        let response = router.oneshot(dispatched_request("gpt-4")).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(
            mock_client.get_requests().len(),
            2,
            "the request failed over to the second provider"
        );
        assert_eq!(
            limiter.calls().len(),
            1,
            "one slot is held across failover, not one per attempt"
        );
    }

    #[tokio::test]
    async fn test_batch_and_realtime_counts_are_independent() {
        use tower::ServiceExt;

        let targets = batch_limited_targets(Some(1), Some(1));
        let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
        let router = batch_router(targets, mock_client.clone(), true);

        let _realtime = router
            .clone()
            .oneshot(inflight_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();
        let _batch = router
            .clone()
            .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();

        let refused_realtime = router
            .clone()
            .oneshot(inflight_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(refused_realtime.status(), StatusCode::TOO_MANY_REQUESTS);

        let refused_batch = router
            .clone()
            .oneshot(dispatched_chat_request("batch-model", "sk-acct-a"))
            .await
            .unwrap();
        assert_eq!(refused_batch.status(), StatusCode::from_u16(529).unwrap());
    }

    mod metrics {
        use super::*;
        use axum_test::TestServer;
        use dashmap::DashMap;
        use rstest::*;
        use serde_json::json;
        use std::sync::Arc;

        #[fixture]
        #[once]
        fn get_shared_metrics_servers(
            #[default(Arc::new(DashMap::new()))] targets: Arc<DashMap<String, target::TargetPools>>,
        ) -> (TestServer, TestServer) {
            let targets = Targets {
                targets,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let (prometheus_layer, handle) = build_metrics_layer_and_handle("onwards");

            let metrics_router = build_metrics_router(handle);
            let metrics_server = TestServer::new(metrics_router).unwrap();

            let app_state = AppState::new(targets);
            let router = build_router(app_state).layer(prometheus_layer);
            let server = TestServer::new(router).unwrap();

            (server, metrics_server)
        }

        #[rstest]
        #[tokio::test]
        async fn test_metrics_server_for_v1_models(
            get_shared_metrics_servers: &(TestServer, TestServer),
        ) {
            let (server, metrics_server) = get_shared_metrics_servers;

            let initial_response = metrics_server.get("/metrics").await;
            let initial_metrics = initial_response.text();

            let initial_count = initial_metrics
                .lines()
                .find(|line| line.contains("onwards_http_requests_total{method=\"GET\",status=\"200\",endpoint=\"/v1/models\"}"))
                .and_then(|line| line.split_whitespace().last())
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0);

            let response = server.get("/v1/models").await;
            assert_eq!(response.status_code(), 200);

            let response = metrics_server.get("/metrics").await;
            assert_eq!(response.status_code(), 200);
            let metrics_text = response.text();

            let new_count = metrics_text
                .lines()
                .find(|line| line.contains("onwards_http_requests_total{method=\"GET\",status=\"200\",endpoint=\"/v1/models\"}"))
                .and_then(|line| line.split_whitespace().last())
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0);

            assert_eq!(
                new_count,
                initial_count + 1,
                "Metrics should increment by 1"
            );

            for _ in 0..10 {
                let response = server.get("/v1/models").await;
                assert_eq!(response.status_code(), 200);
            }

            let response = metrics_server.get("/metrics").await;
            assert_eq!(response.status_code(), 200);
            let metrics_text = response.text();

            let final_count = metrics_text
                .lines()
                .find(|line| line.contains("onwards_http_requests_total{method=\"GET\",status=\"200\",endpoint=\"/v1/models\"}"))
                .and_then(|line| line.split_whitespace().last())
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0);

            assert_eq!(
                final_count,
                initial_count + 11,
                "Metrics should increment by 11 total"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_metrics_server_for_missing_targets(
            get_shared_metrics_servers: &(TestServer, TestServer),
        ) {
            let (server, metrics_server) = get_shared_metrics_servers;

            let initial_response = metrics_server.get("/metrics").await;
            let initial_metrics = initial_response.text();

            let initial_count = initial_metrics
                .lines()
                .find(|line| line.contains("onwards_http_requests_total{method=\"POST\",status=\"404\",endpoint=\"/v1/chat/completions\"}"))
                .and_then(|line| line.split_whitespace().last())
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0);

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "claude-3",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;
            assert_eq!(response.status_code(), 404);

            let response = metrics_server.get("/metrics").await;

            assert_eq!(response.status_code(), 200);
            let metrics_text = response.text();

            let new_count = metrics_text
                .lines()
                .find(|line| line.contains("onwards_http_requests_total{method=\"POST\",status=\"404\",endpoint=\"/v1/chat/completions\"}"))
                .and_then(|line| line.split_whitespace().last())
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0);

            assert_eq!(
                new_count,
                initial_count + 1,
                "Metrics should increment by 1"
            );

            for _ in 0..10 {
                let response = server
                    .post("/v1/chat/completions")
                    .json(&json!({
                        "model": "claude-3",
                        "messages": [{"role": "user", "content": "Hello"}]
                    }))
                    .await;
                assert_eq!(response.status_code(), 404);
            }

            let response = metrics_server.get("/metrics").await;
            assert_eq!(response.status_code(), 200);
            let metrics_text = response.text();

            let final_count = metrics_text
                .lines()
                .find(|line| line.contains("onwards_http_requests_total{method=\"POST\",status=\"404\",endpoint=\"/v1/chat/completions\"}"))
                .and_then(|line| line.split_whitespace().last())
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0);

            assert_eq!(
                final_count,
                initial_count + 11,
                "Metrics should increment by 11 total"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_onwards_model_inflight_not_created_for_unknown_model(
            get_shared_metrics_servers: &(TestServer, TestServer),
        ) {
            let (server, metrics_server) = get_shared_metrics_servers;
            let model = "unconfigured-cardinality-probe";

            let response = server
                .post("/v1/embeddings")
                .json(&json!({
                    "model": model,
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;
            assert_eq!(response.status_code(), 404);

            let metrics_text = metrics_server.get("/metrics").await.text();
            assert!(
                !metrics_text.contains(&format!("onwards_model_inflight{{model=\"{model}\"}}")),
                "an unknown model must NOT create a per-model series:\n{metrics_text}"
            );
        }
    }

    mod response_headers_pricing {
        use super::*;
        use std::collections::HashMap;
        use target::{Target, Targets};

        #[tokio::test]
        async fn test_pricing_added_to_response_headers_when_configured() {
            let targets_map = Arc::new(DashMap::new());
            let mut response_headers = HashMap::new();
            response_headers.insert("Input-Price-Per-Token".to_string(), "0.00003".to_string());
            response_headers.insert("Output-Price-Per-Token".to_string(), "0.00006".to_string());

            targets_map.insert(
                "gpt-4".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.openai.com".parse().unwrap())
                        .response_headers(response_headers)
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
            let app_state = AppState::with_client(targets, mock_client);
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "gpt-4",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 200);
            assert_eq!(response.header("Input-Price-Per-Token"), "0.00003");
            assert_eq!(response.header("Output-Price-Per-Token"), "0.00006");
        }

        #[tokio::test]
        async fn test_no_pricing_headers_when_not_configured() {
            let targets_map = Arc::new(DashMap::new());
            targets_map.insert(
                "free-model".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.example.com".parse().unwrap())
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
            let app_state = AppState::with_client(targets, mock_client);
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "free-model",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 200);
            assert!(response.maybe_header("Input-Price-Per-Token").is_none());
            assert!(response.maybe_header("Output-Price-Per-Token").is_none());
        }

        #[tokio::test]
        async fn test_pricing_preserved_in_error_response_headers() {
            let targets_map = Arc::new(DashMap::new());
            let mut response_headers = HashMap::new();
            response_headers.insert("Input-Price-Per-Token".to_string(), "0.00001".to_string());
            response_headers.insert("Output-Price-Per-Token".to_string(), "0.00002".to_string());

            targets_map.insert(
                "error-model".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.example.com".parse().unwrap())
                        .response_headers(response_headers)
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let mock_client = MockHttpClient::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"error": "Server error"}"#,
            );
            let app_state = AppState::with_client(targets, mock_client);
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "error-model",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 500);
            assert_eq!(response.header("Input-Price-Per-Token"), "0.00001");
            assert_eq!(response.header("Output-Price-Per-Token"), "0.00002");
        }

        #[tokio::test]
        async fn test_pricing_headers_with_different_models() {
            let targets_map = Arc::new(DashMap::new());

            let mut expensive_headers = HashMap::new();
            expensive_headers.insert("Input-Price-Per-Token".to_string(), "0.0001".to_string());
            expensive_headers.insert("Output-Price-Per-Token".to_string(), "0.0002".to_string());

            targets_map.insert(
                "expensive-model".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.expensive.com".parse().unwrap())
                        .response_headers(expensive_headers)
                        .build(),
                ),
            );

            let mut cheap_headers = HashMap::new();
            cheap_headers.insert("Input-Price-Per-Token".to_string(), "0.000001".to_string());
            cheap_headers.insert("Output-Price-Per-Token".to_string(), "0.000002".to_string());

            targets_map.insert(
                "cheap-model".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.cheap.com".parse().unwrap())
                        .response_headers(cheap_headers)
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
            let app_state = AppState::with_client(targets, mock_client);
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "expensive-model",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 200);
            assert_eq!(response.header("Input-Price-Per-Token"), "0.0001");
            assert_eq!(response.header("Output-Price-Per-Token"), "0.0002");

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "cheap-model",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 200);
            assert_eq!(response.header("Input-Price-Per-Token"), "0.000001");
            assert_eq!(response.header("Output-Price-Per-Token"), "0.000002");
        }

        #[tokio::test]
        async fn test_pricing_header_with_only_input_price() {
            let targets_map = Arc::new(DashMap::new());
            let mut response_headers = HashMap::new();
            response_headers.insert("Input-Price-Per-Token".to_string(), "0.00005".to_string());

            targets_map.insert(
                "input-only-model".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.example.com".parse().unwrap())
                        .response_headers(response_headers)
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
            let app_state = AppState::with_client(targets, mock_client);
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "input-only-model",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 200);
            assert_eq!(response.header("Input-Price-Per-Token"), "0.00005");
            assert!(response.maybe_header("Output-Price-Per-Token").is_none());
        }

        #[tokio::test]
        async fn test_pricing_header_with_only_output_price() {
            let targets_map = Arc::new(DashMap::new());
            let mut response_headers = HashMap::new();
            response_headers.insert("Output-Price-Per-Token".to_string(), "0.00008".to_string());

            targets_map.insert(
                "output-only-model".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.example.com".parse().unwrap())
                        .response_headers(response_headers)
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
            let app_state = AppState::with_client(targets, mock_client);
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "output-only-model",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 200);
            assert!(response.maybe_header("Input-Price-Per-Token").is_none());
            assert_eq!(response.header("Output-Price-Per-Token"), "0.00008");
        }
    }

    mod load_balancing {
        use super::*;
        use crate::load_balancer::{Provider, ProviderPool};

        #[tokio::test]
        async fn test_load_balancing_with_multiple_providers() {
            let providers = vec![
                Provider::new(
                    Target::builder()
                        .url("https://api.provider1.com".parse().unwrap())
                        .onwards_key("key1".to_string())
                        .build(),
                    1,
                ),
                Provider::new(
                    Target::builder()
                        .url("https://api.provider2.com".parse().unwrap())
                        .onwards_key("key2".to_string())
                        .build(),
                    1,
                ),
            ];
            let pool = ProviderPool::new(providers);

            let targets_map = Arc::new(DashMap::new());
            targets_map.insert("test-model".to_string(), pool.into());

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
            let app_state = AppState::with_client(targets, mock_client.clone());
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            for _ in 0..5 {
                let response = server
                    .post("/v1/chat/completions")
                    .json(&json!({
                        "model": "test-model",
                        "messages": [{"role": "user", "content": "Hello"}]
                    }))
                    .await;

                assert_eq!(response.status_code(), 200);
            }

            let requests = mock_client.get_requests();
            assert_eq!(requests.len(), 5);
        }

        #[tokio::test]
        async fn test_load_balancing_with_weighted_providers() {
            let providers = vec![
                Provider::new(
                    Target::builder()
                        .url("https://api.high-weight.com".parse().unwrap())
                        .onwards_key("key-high".to_string())
                        .build(),
                    3,
                ),
                Provider::new(
                    Target::builder()
                        .url("https://api.low-weight.com".parse().unwrap())
                        .onwards_key("key-low".to_string())
                        .build(),
                    1,
                ),
            ];
            let pool = ProviderPool::new(providers);

            let targets_map = Arc::new(DashMap::new());
            targets_map.insert("weighted-model".to_string(), pool.into());

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
            let app_state = AppState::with_client(targets, mock_client.clone());
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            for _ in 0..100 {
                let response = server
                    .post("/v1/chat/completions")
                    .json(&json!({
                        "model": "weighted-model",
                        "messages": [{"role": "user", "content": "Hello"}]
                    }))
                    .await;

                assert_eq!(response.status_code(), 200);
            }

            let requests = mock_client.get_requests();
            assert_eq!(requests.len(), 100);

            let high_weight_count = requests
                .iter()
                .filter(|r| r.uri.contains("api.high-weight.com"))
                .count();
            let low_weight_count = requests
                .iter()
                .filter(|r| r.uri.contains("api.low-weight.com"))
                .count();

            assert!(
                high_weight_count > low_weight_count,
                "Expected high-weight provider ({}) to receive more requests than low-weight ({})",
                high_weight_count,
                low_weight_count
            );
        }

        #[tokio::test]
        async fn test_single_provider_pool_behaves_like_single_target() {
            let pool = ProviderPool::single(
                Target::builder()
                    .url("https://api.single.com".parse().unwrap())
                    .onwards_key("single-key".to_string())
                    .build(),
                1,
            );

            let targets_map = Arc::new(DashMap::new());
            targets_map.insert("single-model".to_string(), pool.into());

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let mock_client = MockHttpClient::new(StatusCode::OK, r#"{"success": true}"#);
            let app_state = AppState::with_client(targets, mock_client.clone());
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "single-model",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 200);

            let requests = mock_client.get_requests();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].uri.contains("api.single.com"));
        }
    }

    #[tokio::test]
    async fn test_provider_reasoning_fields_pass_through_only_in_non_strict_mode() {
        for strict_mode in [false, true] {
            for stream in [false, true] {
                for path in ["/v1/chat/completions", "/v1/responses", "/v1/completions"] {
                    let targets_map = Arc::new(DashMap::new());
                    targets_map.insert(
                        "model".to_string(),
                        pool(
                            Target::builder()
                                .url("https://engine.example.com".parse().unwrap())
                                .build(),
                        ),
                    );
                    let targets = Targets {
                        targets: targets_map,
                        key_rate_limiters: Arc::new(DashMap::new()),
                        key_concurrency_limiters: Arc::new(DashMap::new()),
                        key_labels: Arc::new(DashMap::new()),
                        key_ids: Arc::new(DashMap::new()),
                        accounts: Arc::new(DashMap::new()),
                        strict_mode,
                        http_pool_config: None,
                    };
                    let mock_client = MockHttpClient::new(StatusCode::OK, "{}");
                    let server = TestServer::new(build_router(AppState::with_client(
                        targets,
                        mock_client.clone(),
                    )))
                    .unwrap();
                    let mut body = json!({
                        "model": "model", "messages": [], "input": "hello", "prompt": "hello",
                        "stream": stream,
                        "chat_template_kwargs": {"enable_thinking": true},
                        "thinking_token_budget": 512
                    });
                    if path.ends_with("chat/completions") {
                        body["reasoning_effort"] = json!("high");
                    }
                    let response = server.post(path).json(&body).await;
                    let requests = mock_client.get_requests();
                    if strict_mode {
                        assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
                        assert!(requests.is_empty());
                    } else {
                        assert_eq!(response.status_code(), StatusCode::OK);
                        assert_eq!(requests.len(), 1);
                        let forwarded: serde_json::Value =
                            serde_json::from_slice(&requests[0].body).unwrap();
                        assert_eq!(forwarded, body);
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_provider_reasoning_translation_applies_to_upstream_body() {
        let reasoning_translation = serde_json::from_value(json!({
            "chat_completions": {
                "unsupported_efforts": ["minimal", "low", "medium", "xhigh", "max"],
                "writes": [{
                    "target_path": "/chat_template_kwargs/thinking",
                    "values": {"none": false, "high": true}
                }]
            }
        }))
        .unwrap();
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "kimi-k2.5".to_string(),
            pool(
                Target::builder()
                    .url("https://sglang.example.com".parse().unwrap())
                    .reasoning_translation(reasoning_translation)
                    .build(),
            ),
        );
        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };
        let mock_client = MockHttpClient::new(StatusCode::OK, "{}");
        let app_state = AppState::with_client(targets, mock_client.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "kimi-k2.5",
                "messages": [{"role": "user", "content": "Hello"}],
                "reasoning_effort": "none"
            }))
            .await;

        assert_eq!(response.status_code(), StatusCode::OK);
        let requests = mock_client.get_requests();
        let upstream_body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(
            upstream_body.pointer("/chat_template_kwargs/thinking"),
            Some(&json!(false))
        );
        assert!(upstream_body.get("reasoning_effort").is_none());
    }

    #[tokio::test]
    async fn test_missing_output_limit_returns_422_before_upstream_request() {
        let reasoning_translation = serde_json::from_value(json!({
            "chat_completions": {
                "unsupported_efforts": ["none", "minimal", "low", "medium", "xhigh", "max"],
                "writes": [
                    {
                        "target_path": "/reasoning_effort",
                        "values": {"high": "high"}
                    },
                    {
                        "target_path": "/thinking_token_budget",
                        "values": {"high": 8192}
                    }
                ]
            }
        }))
        .unwrap();
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "reasoning-model".to_string(),
            pool(
                Target::builder()
                    .url("https://vllm.example.com".parse().unwrap())
                    .reasoning_translation(reasoning_translation)
                    .build(),
            ),
        );
        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };
        let mock_client = MockHttpClient::new(StatusCode::OK, "{}");
        let app_state = AppState::with_client(targets, mock_client.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "reasoning-model",
                "messages": [],
                "reasoning_effort": "high"
            }))
            .await;

        assert_eq!(response.status_code(), StatusCode::UNPROCESSABLE_ENTITY);
        let body: serde_json::Value = response.json();
        assert_eq!(body["error"]["param"], "max_completion_tokens");
        assert_eq!(
            body["error"]["code"],
            "reasoning_budget_requires_max_tokens"
        );
        assert!(mock_client.get_requests().is_empty());
    }

    #[tokio::test]
    async fn test_reasoning_budget_is_validated_for_every_pool_provider() {
        let budget_config = |budget| {
            serde_json::from_value(json!({
                "chat_completions": {
                    "unsupported_efforts": ["none", "minimal", "low", "medium", "xhigh", "max"],
                    "writes": [
                        {
                            "target_path": "/reasoning_effort",
                            "values": {"high": "high"}
                        },
                        {
                            "target_path": "/thinking_token_budget",
                            "values": {"high": budget}
                        }
                    ]
                }
            }))
            .unwrap()
        };
        let providers = vec![
            Provider::new(
                Target::builder()
                    .url("https://first.example.com".parse().unwrap())
                    .reasoning_translation(budget_config(4096))
                    .build(),
                1,
            ),
            Provider::new(
                Target::builder()
                    .url("https://fallback.example.com".parse().unwrap())
                    .reasoning_translation(budget_config(8192))
                    .build(),
                1,
            ),
        ];
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "reasoning-model".to_string(),
            ProviderPool::with_config(
                providers,
                None,
                None,
                None,
                None,
                target::LoadBalanceStrategy::Priority,
                false,
                Vec::new(),
            )
            .into(),
        );
        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };
        let mock_client = MockHttpClient::new(StatusCode::OK, "{}");
        let app_state = AppState::with_client(targets, mock_client.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "reasoning-model",
                "messages": [],
                "reasoning_effort": "high",
                "max_completion_tokens": 6000
            }))
            .await;

        assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value = response.json();
        assert_eq!(body["error"]["code"], "reasoning_budget_exceeds_max_tokens");
        assert!(mock_client.get_requests().is_empty());
    }

    #[tokio::test]
    async fn test_non_reasoning_routes_allow_non_json_payloads() {
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "whisper-1".to_string(),
            pool(
                Target::builder()
                    .url("https://audio.example.com".parse().unwrap())
                    .build(),
            ),
        );
        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };
        let mock_client = MockHttpClient::new(StatusCode::OK, "{}");
        let app_state = AppState::with_client(targets, mock_client.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();
        let payload = "--boundary\r\ncontent-disposition: form-data\r\n\r\naudio\r\n--boundary--";

        let response = server
            .post("/v1/audio/transcriptions")
            .add_header("model-override", "whisper-1")
            .add_header("content-type", "multipart/form-data; boundary=boundary")
            .text(payload)
            .await;

        assert_eq!(response.status_code(), StatusCode::OK);
        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].body, payload.as_bytes());
    }

    #[tokio::test]
    async fn test_invalid_reasoning_effort_returns_parameter_specific_400() {
        let reasoning_translation = serde_json::from_value(json!({
            "chat_completions": {
                "unsupported_efforts": ["minimal", "low", "medium", "xhigh", "max"],
                "writes": [{
                    "target_path": "/chat_template_kwargs/thinking",
                    "values": {"none": false, "high": true}
                }]
            }
        }))
        .unwrap();
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "kimi-k2.5".to_string(),
            pool(
                Target::builder()
                    .url("https://sglang.example.com".parse().unwrap())
                    .reasoning_translation(reasoning_translation)
                    .build(),
            ),
        );
        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };
        let mock_client = MockHttpClient::new(StatusCode::OK, "{}");
        let app_state = AppState::with_client(targets, mock_client.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "kimi-k2.5",
                "messages": [],
                "reasoning_effort": "ultra"
            }))
            .await;

        assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value = response.json();
        assert_eq!(body["error"]["param"], "reasoning_effort");
        assert_eq!(body["error"]["code"], "invalid_value");
        assert!(mock_client.get_requests().is_empty());
    }

    #[tokio::test]
    async fn test_reasoning_effort_must_be_supported_by_every_pool_provider() {
        let supports_high = serde_json::from_value(json!({
            "chat_completions": {
                "unsupported_efforts": ["minimal", "low", "medium", "xhigh", "max"],
                "writes": [{
                    "target_path": "/chat_template_kwargs/thinking",
                    "values": {"none": false, "high": true}
                }]
            }
        }))
        .unwrap();
        let binary_disabled_only = serde_json::from_value(json!({
            "chat_completions": {
                "unsupported_efforts": ["minimal", "low", "medium", "high", "xhigh", "max"],
                "writes": [{
                    "target_path": "/thinking",
                    "values": {"none": {"type": "disabled"}}
                }]
            }
        }))
        .unwrap();
        let providers = vec![
            Provider::new(
                Target::builder()
                    .url("https://first.example.com".parse().unwrap())
                    .reasoning_translation(supports_high)
                    .build(),
                1,
            ),
            Provider::new(
                Target::builder()
                    .url("https://fallback.example.com".parse().unwrap())
                    .reasoning_translation(binary_disabled_only)
                    .build(),
                1,
            ),
        ];
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "reasoning-model".to_string(),
            ProviderPool::with_config(
                providers,
                None,
                None,
                None,
                None,
                target::LoadBalanceStrategy::Priority,
                false,
                Vec::new(),
            )
            .into(),
        );
        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };
        let mock_client = MockHttpClient::new(StatusCode::OK, "{}");
        let app_state = AppState::with_client(targets, mock_client.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "reasoning-model",
                "messages": [],
                "reasoning_effort": "high"
            }))
            .await;

        assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value = response.json();
        assert_eq!(body["error"]["code"], "unsupported_value");
        assert!(mock_client.get_requests().is_empty());
    }

    #[tokio::test]
    async fn test_reasoning_translation_is_rebuilt_for_each_fallback_attempt() {
        let sglang = serde_json::from_value(json!({
            "chat_completions": {
                "unsupported_efforts": ["minimal", "low", "medium", "high", "xhigh", "max"],
                "writes": [{
                    "target_path": "/chat_template_kwargs/thinking",
                    "values": {"none": false}
                }]
            }
        }))
        .unwrap();
        let object_shape = serde_json::from_value(json!({
            "chat_completions": {
                "unsupported_efforts": ["minimal", "low", "medium", "high", "xhigh", "max"],
                "writes": [{
                    "target_path": "/thinking",
                    "values": {"none": {"type": "disabled"}}
                }]
            }
        }))
        .unwrap();
        let providers = vec![
            Provider::new(
                Target::builder()
                    .url("https://first.example.com".parse().unwrap())
                    .reasoning_translation(sglang)
                    .build(),
                1,
            ),
            Provider::new(
                Target::builder()
                    .url("https://fallback.example.com".parse().unwrap())
                    .reasoning_translation(object_shape)
                    .build(),
                1,
            ),
        ];
        let fallback = Some(target::FallbackConfig {
            enabled: true,
            on_status: vec![429],
            ..Default::default()
        });
        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "reasoning-model".to_string(),
            ProviderPool::with_config(
                providers,
                None,
                None,
                None,
                fallback,
                target::LoadBalanceStrategy::Priority,
                false,
                Vec::new(),
            )
            .into(),
        );
        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };
        let error_frame =
            "data: {\"error\":{\"code\":429,\"message\":\"Provider returned error\"}}\n\n"
                .to_string();
        let ok_chunk = "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"reasoning-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n".to_string();
        let done = "data: [DONE]\n\n".to_string();
        let mock_client = MockHttpClient::new_streaming_sequence(
            StatusCode::OK,
            vec![vec![error_frame], vec![ok_chunk, done]],
        );
        let app_state = AppState::with_client(targets, mock_client.clone());
        let server = TestServer::new(build_router(app_state)).unwrap();

        let response = server
            .post("/v1/chat/completions")
            .json(&json!({
                "model": "reasoning-model",
                "stream": true,
                "messages": [],
                "reasoning_effort": "none"
            }))
            .await;

        assert_eq!(response.status_code(), StatusCode::OK);
        assert!(response.text().contains("hi"));
        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 2);
        let first: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        let second: serde_json::Value = serde_json::from_slice(&requests[1].body).unwrap();
        assert_eq!(
            first.pointer("/chat_template_kwargs/thinking"),
            Some(&json!(false))
        );
        assert_eq!(second.pointer("/thinking/type"), Some(&json!("disabled")));
        assert!(second.get("chat_template_kwargs").is_none());
        assert!(first.get("reasoning_effort").is_none());
        assert!(second.get("reasoning_effort").is_none());
    }

    mod response_sanitization {
        use super::*;

        #[tokio::test]
        async fn test_sanitize_non_streaming_removes_unknown_fields() {
            let targets_map = Arc::new(DashMap::new());
            targets_map.insert(
                "gpt-4".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.openai.com".parse().unwrap())
                        .onwards_key("sk-test".to_string())
                        .sanitize_response(true)
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let mock_response = r#"{
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "created": 1677652288,
                "model": "gpt-4",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "Hello!"
                    },
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 2,
                    "total_tokens": 11
                },
                "custom_provider_field": "should be removed",
                "another_unknown_field": 12345
            }"#;

            let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
            let app_state = AppState::with_client(targets, mock_client)
                .with_response_transform(create_openai_sanitizer());
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "gpt-4",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 200);

            let body: serde_json::Value = response.json();
            assert!(body.get("id").is_some());
            assert!(body.get("choices").is_some());
            assert!(body.get("usage").is_some());

            assert!(body.get("custom_provider_field").is_none());
            assert!(body.get("another_unknown_field").is_none());
        }

        #[tokio::test]
        async fn test_sanitize_rewrites_model_field() {
            let targets_map = Arc::new(DashMap::new());
            targets_map.insert(
                "gpt-4".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.openai.com".parse().unwrap())
                        .onwards_key("sk-test".to_string())
                        .onwards_model("gpt-4-turbo-2024-04-09".to_string())
                        .sanitize_response(true)
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let mock_response = r#"{
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "created": 1677652288,
                "model": "gpt-4-turbo-2024-04-09",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "Hello!"
                    },
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 2,
                    "total_tokens": 11
                }
            }"#;

            let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
            let app_state = AppState::with_client(targets, mock_client)
                .with_response_transform(create_openai_sanitizer());
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "gpt-4",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 200);

            let body: serde_json::Value = response.json();
            assert_eq!(body["model"], "gpt-4");
        }

        #[tokio::test]
        async fn test_sanitize_streaming_removes_unknown_fields() {
            let targets_map = Arc::new(DashMap::new());
            targets_map.insert(
                "gpt-4".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.openai.com".parse().unwrap())
                        .onwards_key("sk-test".to_string())
                        .sanitize_response(true)
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let streaming_chunks = vec![
                r#"data: {"id":"chatcmpl-123","object":"chat.completion.chunk","created":1677652288,"model":"gpt-4","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}],"custom_field":"remove_me"}

"#
                    .to_string(),
                "data: [DONE]\n\n".to_string(),
            ];

            let mock_client = MockHttpClient::new_streaming(StatusCode::OK, streaming_chunks);
            let app_state = AppState::with_client(targets, mock_client)
                .with_response_transform(create_openai_sanitizer());
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "gpt-4",
                    "messages": [{"role": "user", "content": "Hello"}],
                    "stream": true
                }))
                .await;

            assert_eq!(response.status_code(), 200);

            let body = response.text();
            assert!(body.contains("data: [DONE]"));
            assert!(!body.contains("custom_field"));
            assert!(!body.contains("remove_me"));
        }

        #[tokio::test]
        async fn test_sanitize_streaming_rewrites_model() {
            let targets_map = Arc::new(DashMap::new());
            targets_map.insert(
                "gpt-4".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.openai.com".parse().unwrap())
                        .onwards_key("sk-test".to_string())
                        .onwards_model("gpt-4-turbo".to_string())
                        .sanitize_response(true)
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let streaming_chunks = vec![
                r#"data: {"id":"chatcmpl-123","object":"chat.completion.chunk","created":1677652288,"model":"gpt-4-turbo","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}

"#
                    .to_string(),
                "data: [DONE]\n\n".to_string(),
            ];

            let mock_client = MockHttpClient::new_streaming(StatusCode::OK, streaming_chunks);
            let app_state = AppState::with_client(targets, mock_client)
                .with_response_transform(create_openai_sanitizer());
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "gpt-4",
                    "messages": [{"role": "user", "content": "Hello"}],
                    "stream": true
                }))
                .await;

            assert_eq!(response.status_code(), 200);

            let body = response.text();
            assert!(body.contains(r#""model":"gpt-4""#));
            assert!(!body.contains(r#""model":"gpt-4-turbo""#));
        }

        #[tokio::test]
        async fn test_sanitization_disabled_passes_through() {
            let targets_map = Arc::new(DashMap::new());
            targets_map.insert(
                "gpt-4".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.openai.com".parse().unwrap())
                        .onwards_key("sk-test".to_string())
                        .sanitize_response(false)
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let mock_response = r#"{
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "created": 1677652288,
                "model": "gpt-4",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "Hello!"
                    },
                    "finish_reason": "stop"
                }],
                "custom_provider_field": "should be preserved"
            }"#;

            let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
            let app_state = AppState::with_client(targets, mock_client)
                .with_response_transform(create_openai_sanitizer());
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "gpt-4",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 200);

            let body: serde_json::Value = response.json();
            assert!(body.get("custom_provider_field").is_some());
            assert_eq!(body["custom_provider_field"], "should be preserved");
        }

        #[tokio::test]
        async fn test_sanitization_only_applies_to_chat_completions() {
            let targets_map = Arc::new(DashMap::new());
            targets_map.insert(
                "gpt-4".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.openai.com".parse().unwrap())
                        .onwards_key("sk-test".to_string())
                        .sanitize_response(true)
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let mock_response = r#"{
                "object": "list",
                "data": [{"embedding": [0.1, 0.2]}],
                "model": "text-embedding-ada-002",
                "usage": {"prompt_tokens": 8, "total_tokens": 8},
                "custom_field": "preserved"
            }"#;

            let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
            let app_state = AppState::with_client(targets, mock_client)
                .with_response_transform(create_openai_sanitizer());
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/embeddings")
                .json(&json!({
                    "model": "gpt-4",
                    "input": "Hello"
                }))
                .await;

            assert_eq!(response.status_code(), 200);

            let body: serde_json::Value = response.json();
            assert!(body.get("custom_field").is_some());
        }

        #[tokio::test]
        async fn test_pool_level_sanitization_applies_to_all_providers() {
            use crate::load_balancer::ProviderPool;

            let provider1 = Target::builder()
                .url("https://api1.com".parse().unwrap())
                .onwards_key("key1".to_string())
                .build();

            let provider2 = Target::builder()
                .url("https://api2.com".parse().unwrap())
                .onwards_key("key2".to_string())
                .sanitize_response(false)
                .build();

            let pool = ProviderPool::new(vec![
                crate::load_balancer::Provider::new(provider1, 1),
                crate::load_balancer::Provider::new(provider2, 1),
            ]);

            let targets_map = Arc::new(DashMap::new());
            targets_map.insert("test-model".to_string(), pool.into());

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let mock_response = r#"{
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "created": 1677652288,
                "model": "test-model",
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "Hello!"},
                    "finish_reason": "stop"
                }],
                "custom_field": "value"
            }"#;

            let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
            let app_state = AppState::with_client(targets, mock_client)
                .with_response_transform(create_openai_sanitizer());
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "test-model",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 200);
        }

        #[tokio::test]
        async fn test_error_sanitization_replaces_4xx_body() {
            let targets_map = Arc::new(DashMap::new());
            targets_map.insert(
                "gpt-4".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.openai.com".parse().unwrap())
                        .onwards_key("sk-test".to_string())
                        .sanitize_response(true)
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let upstream_error = r#"{
                "error": {
                    "message": "Internal provider Novita returned error: model not loaded on GPU cluster eu-west-3",
                    "type": "provider_error",
                    "code": "novita_internal_error",
                    "metadata": {"provider": "novita", "region": "eu-west-3"}
                }
            }"#;

            let mock_client = MockHttpClient::new(StatusCode::UNPROCESSABLE_ENTITY, upstream_error);
            let app_state = AppState::with_client(targets, mock_client)
                .with_response_transform(create_openai_sanitizer());
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "gpt-4",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 422);

            let body: serde_json::Value = response.json();
            assert_eq!(
                body["error"]["message"],
                "The upstream provider rejected the request."
            );
            assert_eq!(body["error"]["type"], "invalid_request_error");
            assert_eq!(body["error"]["code"], "upstream_error");
            let body_str = serde_json::to_string(&body).unwrap();
            assert!(!body_str.contains("Novita"));
            assert!(!body_str.contains("novita"));
            assert!(!body_str.contains("eu-west-3"));
        }

        #[tokio::test]
        async fn test_error_sanitization_preserves_5xx_status() {
            let targets_map = Arc::new(DashMap::new());
            targets_map.insert(
                "gpt-4".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.openai.com".parse().unwrap())
                        .onwards_key("sk-test".to_string())
                        .sanitize_response(true)
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let upstream_error = r#"{"error": "worker crashed on gpu-12", "retry_after": 30}"#;

            let mock_client =
                MockHttpClient::new(StatusCode::INTERNAL_SERVER_ERROR, upstream_error);
            let app_state = AppState::with_client(targets, mock_client)
                .with_response_transform(create_openai_sanitizer());
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "gpt-4",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 500);

            let body: serde_json::Value = response.json();
            assert_eq!(
                body["error"]["message"],
                "An internal error occurred. Please try again later."
            );
            assert_eq!(body["error"]["type"], "internal_error");
            assert_eq!(body["error"]["code"], "internal_error");
            let body_str = serde_json::to_string(&body).unwrap();
            assert!(!body_str.contains("GPU cluster"));
            assert!(!body_str.contains("retry_after"));
        }

        #[tokio::test]
        async fn test_error_sanitization_disabled_passes_through() {
            let targets_map = Arc::new(DashMap::new());
            targets_map.insert(
                "gpt-4".to_string(),
                pool(
                    Target::builder()
                        .url("https://api.openai.com".parse().unwrap())
                        .onwards_key("sk-test".to_string())
                        .sanitize_response(false)
                        .build(),
                ),
            );

            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            };

            let upstream_error = r#"{"error": {"message": "provider-specific detail"}}"#;

            let mock_client = MockHttpClient::new(StatusCode::UNPROCESSABLE_ENTITY, upstream_error);
            let app_state = AppState::with_client(targets, mock_client)
                .with_response_transform(create_openai_sanitizer());
            let router = build_router(app_state);
            let server = TestServer::new(router).unwrap();

            let response = server
                .post("/v1/chat/completions")
                .json(&json!({
                    "model": "gpt-4",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
                .await;

            assert_eq!(response.status_code(), 422);

            let body: serde_json::Value = response.json();
            assert_eq!(body["error"]["message"], "provider-specific detail");
        }
    }

    mod completions_pool {
        use super::*;
        use crate::target::{COMPLETIONS_POOL, LoadBalanceStrategy, TargetPools};
        use std::collections::HashMap;

        fn priority_pool(urls: &[&str]) -> ProviderPool {
            ProviderPool::with_config(
                urls.iter()
                    .map(|url| {
                        Provider::new(Target::builder().url(url.parse().unwrap()).build(), 1)
                    })
                    .collect(),
                None,
                None,
                None,
                None,
                LoadBalanceStrategy::Priority,
                false,
                Vec::new(),
            )
        }

        fn flash_pools() -> TargetPools {
            TargetPools::with_pools(
                priority_pool(&[
                    "https://dynamo.example.com",
                    "https://openrouter.example.com",
                ]),
                HashMap::from([(
                    COMPLETIONS_POOL.to_string(),
                    priority_pool(&[
                        "https://dynamo.example.com",
                        "https://fireworks.example.com",
                    ]),
                )]),
            )
        }

        fn server_with(strict_mode: bool, pools: TargetPools) -> (TestServer, MockHttpClient) {
            let targets_map = Arc::new(DashMap::new());
            targets_map.insert("dsv4-flash".to_string(), pools);
            let targets = Targets {
                targets: targets_map,
                key_rate_limiters: Arc::new(DashMap::new()),
                key_concurrency_limiters: Arc::new(DashMap::new()),
                key_labels: Arc::new(DashMap::new()),
                key_ids: Arc::new(DashMap::new()),
                accounts: Arc::new(DashMap::new()),
                strict_mode,
                http_pool_config: None,
            };
            let mock_client = MockHttpClient::new(
                StatusCode::OK,
                r#"{"id":"cmpl-1","object":"text_completion","created":0,"model":"dsv4-flash","choices":[]}"#,
            );
            let app_state = AppState::with_client(targets, mock_client.clone());
            let server = TestServer::new(build_router(app_state)).unwrap();
            (server, mock_client)
        }

        #[tokio::test]
        async fn completions_requests_are_served_by_the_completions_pool() {
            for strict_mode in [false, true] {
                let (server, client) = server_with(strict_mode, flash_pools());
                let response = server
                    .post("/v1/completions")
                    .json(&json!({"model": "dsv4-flash", "prompt": [1, 2, 3]}))
                    .await;
                assert_eq!(
                    response.status_code(),
                    StatusCode::OK,
                    "strict={strict_mode}"
                );

                let requests = client.requests.lock().unwrap();
                assert_eq!(requests.len(), 1, "strict={strict_mode}");
                assert!(
                    requests[0].uri.starts_with("https://dynamo.example.com/"),
                    "strict={strict_mode}: got {}",
                    requests[0].uri
                );
            }
        }

        #[tokio::test]
        async fn chat_requests_never_reach_the_completions_pool() {
            for strict_mode in [false, true] {
                let (server, client) = server_with(strict_mode, flash_pools());
                let response = server
                    .post("/v1/chat/completions")
                    .json(&json!({
                        "model": "dsv4-flash",
                        "messages": [{"role": "user", "content": "hi"}]
                    }))
                    .await;
                assert!(
                    response.status_code().is_success(),
                    "strict={strict_mode}: {}",
                    response.status_code()
                );

                let requests = client.requests.lock().unwrap();
                assert!(
                    requests.iter().all(|r| !r.uri.contains("fireworks")),
                    "strict={strict_mode}: {:?}",
                    requests.iter().map(|r| r.uri.clone()).collect::<Vec<_>>()
                );
            }
        }

        #[tokio::test]
        async fn a_composite_without_a_completions_pool_serves_completions_from_default() {
            for strict_mode in [false, true] {
                let (server, client) = server_with(
                    strict_mode,
                    priority_pool(&["https://dynamo.example.com"]).into(),
                );
                let response = server
                    .post("/v1/completions")
                    .json(&json!({"model": "dsv4-flash", "prompt": "hi"}))
                    .await;
                assert_eq!(
                    response.status_code(),
                    StatusCode::OK,
                    "strict={strict_mode}"
                );

                let requests = client.requests.lock().unwrap();
                assert_eq!(requests.len(), 1, "strict={strict_mode}");
                assert!(
                    requests[0].uri.starts_with("https://dynamo.example.com/"),
                    "strict={strict_mode}: got {}",
                    requests[0].uri
                );
            }
        }

        #[tokio::test]
        async fn an_empty_completions_pool_falls_back_to_the_default() {
            let pools = TargetPools::with_pools(
                priority_pool(&["https://openrouter.example.com"]),
                HashMap::from([(COMPLETIONS_POOL.to_string(), ProviderPool::new(vec![]))]),
            );
            assert!(
                pools
                    .resolved_name(crate::target::RequestClass::Completions)
                    .is_none(),
                "an empty pool is not a picked pool"
            );
            let (server, client) = server_with(false, pools);

            let response = server
                .post("/v1/completions")
                .json(&json!({"model": "dsv4-flash", "prompt": "hi"}))
                .await;
            assert_eq!(response.status_code(), StatusCode::OK);
            let requests = client.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert!(
                requests[0]
                    .uri
                    .starts_with("https://openrouter.example.com/"),
                "got {}",
                requests[0].uri
            );
        }
    }
}
