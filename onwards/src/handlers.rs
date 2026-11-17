
use crate::AppState;
use crate::affinity;
use crate::auth;
use crate::client::HttpClient;
use crate::errors::{ErrorResponseBody, OnwardsErrorResponse};
use crate::inflight::{BATCH_INFLIGHT_SCOPE, InflightSlot};
use crate::models::ListModelResponse;
use crate::rejections::RejectionContext;
use crate::serving::{self, ProviderKind, ServingClassOutcome, ServingResolution};
use crate::sse::SseBufferedStream;
use crate::target::{ConcurrencyGuard, RequestClass, RoutingAction, Target, serving_metric_labels};
use axum::{
    Json,
    extract::Request,
    extract::State,
    http::{
        HeaderMap, HeaderName, HeaderValue, StatusCode, Uri,
        header::{CONTENT_LENGTH, TRANSFER_ENCODING},
    },
    response::{IntoResponse, Response},
};
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use serde_json::map::Entry;
use tracing::{Instrument, debug, error, instrument, trace, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

pub struct HeaderExtractor<'a>(pub &'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(val)) = (key.parse::<HeaderName>(), HeaderValue::from_str(&value)) {
            self.0.insert(name, val);
        }
    }
}

fn record_response_status(status_code: u16) {
    tracing::Span::current().record("http.response.status_code", status_code);
}

fn is_capacity_refusal(status: u16) -> bool {
    matches!(status, 503 | 529)
}

fn embedded_error_status(body: &serde_json::Value) -> Option<u16> {
    let code = body.get("error")?.get("code")?;
    let status = code
        .as_u64()
        .or_else(|| code.as_str().and_then(|s| s.parse::<u64>().ok()))?;
    let status = u16::try_from(status).ok()?;
    (400..600).contains(&status).then_some(status)
}

#[derive(Debug, PartialEq, Eq)]
enum SseEventKind {
    Error(u16, serde_json::Value),
    Data,
    Done,
    Comment,
}

fn classify_sse_event(chunk: &[u8]) -> SseEventKind {
    let Ok(text) = std::str::from_utf8(chunk) else {
        return SseEventKind::Data;
    };
    let mut data = String::new();
    let mut has_data = false;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            has_data = true;
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    if !has_data {
        return SseEventKind::Comment;
    }
    if data.trim() == "[DONE]" {
        return SseEventKind::Done;
    }
    if let Ok(body) = serde_json::from_str::<serde_json::Value>(data.trim())
        && let Some(status) = embedded_error_status(&body)
    {
        SseEventKind::Error(status, body["error"].clone())
    } else {
        SseEventKind::Data
    }
}

fn requests_stream(body: &[u8]) -> bool {
    body.windows(8).any(|w| w == b"\"stream\"")
        && serde_json::from_slice::<serde_json::Value>(body)
            .ok()
            .and_then(|v| v.get("stream")?.as_bool())
            .unwrap_or(false)
}

#[derive(Default)]
struct LeadFrames {
    embedded: Option<(u16, serde_json::Value)>,
    saw_data: bool,
    saw_content: bool,
    stream_ended: bool,
}

async fn read_lead_frames<S, E>(
    events: &mut S,
    peeked: &mut Vec<Result<axum::body::Bytes, E>>,
    lead: &mut LeadFrames,
    max_events: usize,
) where
    S: futures_util::Stream<Item = Result<axum::body::Bytes, E>> + Unpin,
{
    use futures_util::StreamExt;
    for _ in 0..max_events {
        match events.next().await {
            Some(Ok(chunk)) => match classify_sse_event(&chunk) {
                SseEventKind::Error(status, error) => {
                    lead.embedded = Some((status, error));
                    peeked.push(Ok(chunk));
                    return;
                }
                SseEventKind::Data => {
                    lead.saw_data = true;
                    lead.saw_content = true;
                    peeked.push(Ok(chunk));
                    return;
                }
                SseEventKind::Done => {
                    lead.saw_data = true;
                    peeked.push(Ok(chunk));
                    return;
                }
                SseEventKind::Comment => peeked.push(Ok(chunk)),
            },
            Some(Err(e)) => {
                lead.stream_ended = true;
                peeked.push(Err(e));
                return;
            }
            None => {
                lead.stream_ended = true;
                return;
            }
        }
    }
}

enum UpstreamOutcome {
    Timeout,
    Error(Box<dyn std::error::Error + Send + Sync>),
}

struct InflightGuard {
    model: Option<(String, String)>,
}

impl InflightGuard {
    fn set_model(&mut self, model: &str, serving_class: &str) {
        debug_assert!(
            self.model.is_none(),
            "set_model must be called at most once per guard (else the per-model gauge leaks)"
        );
        let labels = (model.to_string(), serving_class.to_string());
        metrics::gauge!("onwards_model_inflight", "model" => labels.0.clone(), "serving_class" => labels.1.clone()).increment(1.0);
        self.model = Some(labels);
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        metrics::gauge!("onwards_requests_inflight").decrement(1.0);
        if let Some((model, serving_class)) = self.model.take() {
            metrics::gauge!("onwards_model_inflight", "model" => model, "serving_class" => serving_class).decrement(1.0);
        }
    }
}

struct GuardedStream<S> {
    inner: S,
    _guard: ConcurrencyGuard,
    _inflight_guard: InflightGuard,
    _inflight_slot: Option<InflightSlot>,
    _batch_inflight_slot: Option<InflightSlot>,
}

impl<S, E> futures_util::Stream for GuardedStream<S>
where
    S: futures_util::Stream<Item = Result<bytes::Bytes, E>> + Unpin,
{
    type Item = Result<bytes::Bytes, E>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }
}

#[derive(Clone, Debug)]
struct OriginalModel(String);

#[derive(Clone, Debug)]
pub(crate) struct ResolvedTrust(pub(crate) bool);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServedBy {
    pub url: String,
    pub onwards_model: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct UpstreamFailureLabels {
    pub(crate) model: String,
    pub(crate) serving_class: String,
    pub(crate) traffic: &'static str,
    pub(crate) served_by: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthenticatedApiKeyId(pub Uuid);

fn resolve_trace_propagation(target: &Target, pool_trusted: bool) -> bool {
    target
        .propagate_trace_context
        .unwrap_or_else(|| target.trusted.unwrap_or(pool_trusted))
}

fn withhold_trace_context(headers: &mut HeaderMap) {
    headers.remove("traceparent");
    headers.remove("tracestate");
}

fn filter_headers_for_upstream(headers: &mut HeaderMap, target: &Target) {
    const HEADERS_TO_STRIP: &[&str] = &[
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "upgrade",
        "authorization",
        "x-api-key",
        "api-key",
        "sec-fetch-site",
        "sec-fetch-mode",
        "sec-fetch-dest",
        "sec-fetch-user",
        "origin",
        "referer",
        "cookie",
        "if-modified-since",
        "if-none-match",
        "if-match",
        "if-unmodified-since",
        "if-range",
        "model-override",
    ];

    for header in HEADERS_TO_STRIP {
        headers.remove(*header);
    }

    let sec_ch_ua_headers: Vec<_> = headers
        .keys()
        .filter(|name| name.as_str().starts_with("sec-ch-ua"))
        .cloned()
        .collect();
    for header in sec_ch_ua_headers {
        headers.remove(header);
    }

    if let Some(key) = &target.onwards_key {
        let header_name_str = target
            .upstream_auth_header_name
            .as_deref()
            .unwrap_or("Authorization");
        let header_name = HeaderName::from_bytes(header_name_str.as_bytes()).unwrap();
        let prefix = target
            .upstream_auth_header_prefix
            .as_deref()
            .unwrap_or("Bearer ");
        let header_value = format!("{}{}", prefix, key);
        debug!(
            "Adding {} header for upstream {}",
            header_name_str, target.url
        );
        headers.insert(header_name, header_value.parse().unwrap());
    } else {
        debug!(
            "No upstream authentication configured for target {}",
            target.url
        );
    }

    headers.insert("x-forwarded-proto", "https".parse().unwrap());
}

pub async fn target_message_handler<T: HttpClient>(
    State(state): State<AppState<T>>,
    mut req: axum::extract::Request,
) -> Result<Response, OnwardsErrorResponse> {
    let span = tracing::info_span!(
        "onwards.request",
        otel.name = "onwards.request",
        gen_ai.request.model = tracing::field::Empty,
        http.response.status_code = tracing::field::Empty,
        onwards.serving.resolved = tracing::field::Empty,
        onwards.serving.priority = tracing::field::Empty,
        onwards.serving.targets_configured = tracing::field::Empty,
    );

    if req.headers().contains_key("traceparent") {
        let propagator = opentelemetry_sdk::propagation::TraceContextPropagator::new();
        let parent_ctx = propagator.extract(&HeaderExtractor(req.headers()));
        let _ = span.set_parent(parent_ctx);
    }

    async move {

    let mut rejection =
        RejectionContext::from_request(&state, req.headers(), auth::presented_key(&req));
    rejection.record_identity();
    let result = async {

    metrics::gauge!("onwards_requests_inflight").increment(1.0);
    let mut inflight_guard = Some(InflightGuard { model: None });

    let body_bytes =
        match axum::body::to_bytes(std::mem::take(req.body_mut()), state.body_limit).await {
            Ok(bytes) => bytes,
            Err(_) => return Err(OnwardsErrorResponse::payload_too_large(state.body_limit)),
        };

    trace!(
        method = %req.method(),
        uri = %req.uri(),
        body_len = body_bytes.len(),
        "Incoming request"
    );

    let model_name = match crate::extract_model_from_request(req.headers(), &body_bytes) {
        Some(model) => model,
        None => {
            record_response_status(400);
            return Err(OnwardsErrorResponse::bad_request(
                "Could not parse onwards model from request. 'model' parameter must be supplied in either the body or in the Model-Override header.",
                Some("model"),
            ));
        }
    };

    tracing::Span::current().record("gen_ai.request.model", &model_name);

    req.extensions_mut()
        .insert(OriginalModel(model_name.clone()));

    trace!("Received request for model: {}", model_name);
    trace!(
        "Available targets: {:?}",
        state
            .targets
            .targets
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>()
    );

    let canonical_request_path = req.uri().path().to_string();

    let request_class = RequestClass::from_path(&canonical_request_path);
    let (mut resolved_pool_name, mut pool, mut alias_serving, alias_inflight, alias_batch_inflight, metric_model, metric_class) = match state.targets.targets.get(&model_name) {
        Some(pools) => {
            let (metric_model, metric_class) = serving_metric_labels(&model_name, pools.default_pool().class_route_of());
            if let Some(guard) = inflight_guard.as_mut() {
                guard.set_model(&metric_model, &metric_class);
            }
            rejection.set_model(&model_name);
            rejection.set_metric_labels(&metric_model, &metric_class);
            (
                pools.resolved_name(request_class),
                pools.resolve(request_class).clone(),
                pools.default_pool().alias_serving().clone(),
                pools.default_pool().inflight_limits().cloned(),
                pools.default_pool().batch_inflight_limit(),
                metric_model,
                metric_class,
            )
        }
        None => {
            debug!("No target found for model: {}", model_name);
            record_response_status(404);
            return Err(OnwardsErrorResponse::model_not_found(model_name.as_str()));
        }
    };

    let bearer_token = auth::presented_key(&req);

    if let Some(keys) = pool.keys() {
        match bearer_token.as_ref() {
            Some(token) => {
                trace!("Validating bearer token against pool keys");
                if auth::validate_bearer_token(keys, token) {
                    debug!("Bearer token validation successful");
                } else {
                    debug!("Bearer token validation failed - token not in key set");
                    record_response_status(403);
                    return Err(OnwardsErrorResponse::forbidden());
                }
            }
            None => {
                debug!("No bearer token found in authorization header");
                record_response_status(401);
                return Err(OnwardsErrorResponse::unauthorized());
            }
        }
    } else {
        debug!(
            "Pool '{}' has no keys configured - allowing request",
            model_name
        );
    }

    let authenticated_api_key_id = bearer_token
        .as_ref()
        .filter(|token| {
            pool.keys()
                .is_some_and(|keys| auth::validate_bearer_token(keys, token))
        })
        .and_then(|token| state.targets.key_labels.get(token))
            .and_then(|labels| labels.get(auth::API_KEY_ID_LABEL).cloned())
        .and_then(|id| id.parse::<Uuid>().ok());

    let result = async move {

    if !pool.routing_rules().is_empty()
        && let Some(token) = bearer_token.as_ref()
    {
            let labels = state
                .targets
                .key_labels
                .get(token)
                .map(|r| r.value().clone())
                .unwrap_or_default();

            let action = pool.evaluate_routing_rules(&labels).cloned();

            if let Some(action) = action {
                match action {
                    RoutingAction::Deny => {
                        debug!(
                            "Routing rule denied request for model '{}' with labels {:?}",
                            model_name, labels
                        );
                        record_response_status(403);
                        return Err(OnwardsErrorResponse::forbidden());
                    }
                    RoutingAction::Redirect {
                        target: ref redirect_alias,
                    } => {
                        debug!(
                            "Routing rule redirecting from '{}' to '{}' with labels {:?}",
                            model_name, redirect_alias, labels
                        );
                        pool = match state.targets.targets.get(redirect_alias) {
                            Some(p) => {
                                resolved_pool_name = p.resolved_name(request_class);
                                alias_serving = p.default_pool().alias_serving().clone();
                                p.resolve(request_class).clone()
                            }
                            None => {
                                debug!("Redirect target '{}' not found", redirect_alias);
                                return Err(OnwardsErrorResponse::bad_gateway());
                            }
                        };
                        if pool.is_empty() {
                            debug!("Redirect target pool '{}' has no providers", redirect_alias);
                            return Err(OnwardsErrorResponse::bad_gateway());
                        }
                    }
                }
            }
    }

    let (account_id, account_name, key_purpose) = bearer_token
        .as_ref()
        .and_then(|token| state.targets.key_labels.get(token))
        .map(|labels| {
            (
                labels.get(serving::ACCOUNT_LABEL).cloned(),
                labels.get(serving::ACCOUNT_NAME_LABEL).cloned(),
                labels.get("purpose").cloned(),
            )
        })
        .unwrap_or((None, None, None));
    let serving_resolution: ServingResolution = {
        let account = account_id
            .as_deref()
            .and_then(|id| state.targets.accounts.get(id).map(|r| r.value().clone()));
        let overlay = account_id
            .as_deref()
            .and_then(|id| alias_serving.overlays().get(id).cloned());
        let resolution = serving::resolve(
            account.as_ref(),
            overlay.as_ref(),
            key_purpose.as_deref(),
        );
        let span = tracing::Span::current();
        span.record("onwards.serving.resolved", metric_class.as_str());
        if let Some(priority) = resolution.hints.priority {
            span.record("onwards.serving.priority", priority);
        }
        span.record(
            "onwards.serving.targets_configured",
            resolution.hints.ttft_ms.is_some() || resolution.hints.itl_ms.is_some(),
        );
        metrics::counter!(
            "onwards_serving_class_requests_total",
            "model" => metric_model.clone(),
            "requested" => "",
            "resolved" => metric_class.clone(),
        )
        .increment(1);
        resolution
    };

    let is_realtime = !state
        .first_token_timeout_exempt_header
        .as_deref()
        .is_some_and(|header| req.headers().contains_key(header));
    let traffic = if is_realtime { "realtime" } else { "dispatched" };

    let body_json = if !body_bytes.is_empty()
        && crate::reasoning::uses_reasoning_contract(&canonical_request_path)
    {
        Some(serde_json::from_slice::<serde_json::Value>(&body_bytes).map_err(|_| {
            OnwardsErrorResponse::bad_request("Request body must be valid JSON.", None)
        })?)
    } else {
        None
    };

    let flagged_params = match body_json.as_ref() {
        Some(body) if state.targets.strict_mode => {
            crate::unsupported_params::request_params(&canonical_request_path, body)
        }
        _ => Vec::new(),
    };
    if !flagged_params.is_empty() {
        let rejected: Vec<&'static str> = flagged_params
            .iter()
            .copied()
            .filter(|param| state.rejects_param(&metric_model, param))
            .collect();
        for param in &flagged_params {
            let action = if rejected.contains(param) {
                "rejected"
            } else if state.rejected_params.contains(param) {
                "allowed"
            } else {
                "logged"
            };
            metrics::counter!(
                "onwards_unsupported_params_total",
                "param" => *param,
                "model" => metric_model.clone(),
                "serving_class" => metric_class.clone(),
                "traffic" => traffic,
                "action" => action,
            )
            .increment(1);
        }
        let span = tracing::Span::current();
        span.set_attribute("onwards.unsupported_params", flagged_params.join(","));
        if let Some(account) = &account_id {
            span.set_attribute("onwards.account", account.clone());
        }
        if let Some(api_key_id) = authenticated_api_key_id {
            span.set_attribute("onwards.api_key_id", api_key_id.to_string());
        }
        if let Some(first) = rejected.first() {
            record_response_status(400);
            return Err(OnwardsErrorResponse::invalid_request(
                &crate::unsupported_params::rejection_message(&rejected),
                Some(first),
                "unsupported_parameter",
            ));
        }
    }

    let canonical_reasoning = if let Some(reasoning) = req
        .extensions()
        .get::<crate::reasoning::CanonicalReasoningRequest>()
    {
        Some(reasoning.clone())
    } else if let Some(body) = body_json.as_ref() {
        crate::reasoning::parse_reasoning_request(
            &canonical_request_path,
            body,
            state.targets.strict_mode,
        ).map_err(|error| OnwardsErrorResponse::reasoning(&error))?
    } else {
        None
    };
    drop(body_json);

    if let Some(reasoning) = canonical_reasoning.as_ref() {
        for provider in pool.providers() {
            if let Some(config) = provider.target.reasoning_translation.as_ref() {
                config
                    .validate_request(&canonical_request_path, reasoning)
                    .map_err(|error| OnwardsErrorResponse::reasoning(&error))?;
            }
        }
    }

    if pool.is_empty() {
        debug!("Pool for model '{}' has no providers", model_name);
        metrics::counter!(
            "onwards_upstream_failed_total",
            "reason" => "no_providers",
            "status" => "529",
            "model" => metric_model.clone(),
            "serving_class" => metric_class.clone(),
            "traffic" => traffic,
        )
        .increment(1);
        record_response_status(529);
        return Err(OnwardsErrorResponse::no_capacity());
    }

    let mut inflight_slot = match (is_realtime, alias_inflight.as_deref(), account_id.as_deref()) {
        (true, Some(limits), Some(account)) => {
            let limit = limits.for_account(account);
            let account_name = account_name.unwrap_or_default();
            metrics::gauge!(
                "onwards_inflight_account_limit",
                "model" => metric_model.clone(),
                "serving_class" => metric_class.clone(),
                "account" => account.to_string(),
                "account_name" => account_name.clone(),
            )
            .set(f64::from(limit));
            metrics::counter!(
                "onwards_inflight_limit_checks_total",
                "model" => metric_model.clone(),
                "serving_class" => metric_class.clone(),
                "account" => account.to_string(),
                "account_name" => account_name.clone(),
            )
            .increment(1);
            match state
                .inflight_limiter
                .try_acquire(account, &model_name, limit)
                .await
            {
                Some(slot) => Some(slot),
                None => {
                    debug!(
                        "In-flight limit of {} reached for account {} on model {}",
                        limit, account, model_name
                    );
                    metrics::counter!(
                        "onwards_inflight_limit_refusals_total",
                        "model" => metric_model.clone(),
                        "serving_class" => metric_class.clone(),
                        "account" => account.to_string(),
                        "account_name" => account_name,
                    )
                    .increment(1);
                    record_response_status(429);
                    return Err(OnwardsErrorResponse::inflight_limited(&model_name, limit));
                }
            }
        }
        _ => None,
    };

    let mut batch_inflight_slot = if !is_realtime
        && state.batch_inflight_enforce
        && let Some(limit) = alias_batch_inflight
    {
        match state
            .batch_inflight_limiter
            .try_acquire(BATCH_INFLIGHT_SCOPE, &model_name, limit)
            .await
        {
            Some(slot) => Some(slot),
            None => {
                debug!(
                    "Batch in-flight cap of {} reached for model {}",
                    limit, model_name
                );
                metrics::counter!(
                    "onwards_batch_inflight_refusals_total",
                    "model" => metric_model.clone(),
                    "serving_class" => metric_class.clone(),
                )
                .increment(1);
                record_response_status(529);
                return Err(OnwardsErrorResponse::batch_capacity_exceeded(&model_name, limit));
            }
        }
    } else {
        None
    };

    {
        let _rate_limit_span = tracing::info_span!("onwards.rate_limit_check", otel.name = "onwards.rate_limit_check", model = %model_name).entered();
        if let Some(limiter) = pool.pool_limiter()
            && limiter.check().is_err()
        {
            debug!("Pool-level rate limit exceeded for model: {}", model_name);
            record_response_status(429);
            return Err(OnwardsErrorResponse::rate_limited());
        }

        if let Some(token) = bearer_token.as_ref()
            && let Some(limiter) = state.targets.key_rate_limiters.get(token)
            && limiter.check().is_err()
        {
            debug!(
                api_key_id = ?authenticated_api_key_id,
                "Per-key rate limit exceeded"
            );
            record_response_status(429);
            return Err(OnwardsErrorResponse::rate_limited());
        }
    }

    let (_pool_concurrency_guard, _key_concurrency_guard) = {
        let _concurrency_span = tracing::info_span!("onwards.acquire_concurrency", otel.name = "onwards.acquire_concurrency", model = %model_name).entered();
        let pool_guard = if let Some(limiter) = pool.pool_concurrency_limiter() {
            match limiter.try_acquire() {
                Some(guard) => Some(guard),
                None => {
                    debug!(
                        "Pool-level concurrency limit exceeded for model: {}",
                        model_name
                    );
                    metrics::counter!(
                        "onwards_upstream_failed_total",
                        "reason" => "model_at_capacity",
                        "status" => "529",
                        "model" => metric_model.clone(),
                        "serving_class" => metric_class.clone(),
                        "traffic" => traffic,
                    )
                    .increment(1);
                    record_response_status(529);
                    return Err(OnwardsErrorResponse::overloaded());
                }
            }
        } else {
            None
        };

        let key_guard = if let Some(token) = bearer_token.as_ref() {
            if let Some(limiter) = state.targets.key_concurrency_limiters.get(token) {
                match limiter.try_acquire() {
                    Some(guard) => Some(guard),
                    None => {
                        debug!(
                            api_key_id = ?authenticated_api_key_id,
                            "Per-key concurrency limit exceeded"
                        );
                        return Err(OnwardsErrorResponse::concurrency_limited());
                    }
                }
            } else {
                None
            }
        } else {
            None
        };
        (pool_guard, key_guard)
    };

    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|v| v.as_str())
        .unwrap_or(req.uri().path())
        .to_string();

    let original_headers = req.headers().clone();
    let method = req.method().clone();

    let mut last_error: Option<OnwardsErrorResponse> = None;
    let mut last_upstream_status: Option<u16> = None;

    let ineligible_members: Vec<usize> = if serving_resolution.self_hosted_only {
        pool.providers()
            .iter()
            .enumerate()
            .filter(|(_, provider)| provider.target.kind == ProviderKind::External)
            .map(|(idx, _)| idx)
            .collect()
    } else {
        Vec::new()
    };
    if !ineligible_members.is_empty() {
        debug!(
            "Excluding {} external provider(s) for a self-hosted-only account",
            ineligible_members.len()
        );
        metrics::counter!(
            "onwards_self_hosted_only_skips_total",
            "model" => metric_model.clone(),
            "serving_class" => metric_class.clone(),
        )
        .increment(ineligible_members.len() as u64);
    }
    let no_eligible_member = !pool.is_empty() && ineligible_members.len() == pool.len();
    let eligible_member_count = pool.len() - ineligible_members.len();

    let mut any_attempted = false;
    let mut attempt_number: u32 = 0;
    let mut total_backoff_ms: u64 = 0;
    let pool_max_attempts = if pool.fallback().is_some_and(|f| f.with_replacement) {
        pool.fallback_max_attempts()
    } else {
        pool.fallback_max_attempts().min(eligible_member_count)
    };

    let first_token_timeout = pool
        .fallback()
        .filter(|f| f.enabled)
        .and_then(|f| {
            f.first_token_timeout_ms
                .map(std::time::Duration::from_millis)
                .or(state.first_token_timeout)
        })
        .filter(|timeout| {
            !timeout.is_zero() && eligible_member_count > 1 && is_realtime && requests_stream(&body_bytes)
        });
    let realtime_failover_attempts = if pool.fallback().is_some_and(|f| f.with_replacement) {
        pool_max_attempts
    } else {
        pool_max_attempts.min(eligible_member_count)
    };
    let listed = |lists: &dyn Fn(u16) -> bool, status: u16| {
        lists(status) || (is_capacity_refusal(status) && (lists(503) || lists(529)))
    };
    let fails_over_on = |status: u16, attempt_number: u32| {
        listed(&|s| pool.should_fallback_on_status(s), status)
            || (is_realtime
                && eligible_member_count > 1
                && (attempt_number as usize) < realtime_failover_attempts
                && listed(&|s| pool.should_fallback_on_realtime_status(s), status))
    };

    let aimd_eligible = pool.aimd_enabled()
        && state.targets.strict_mode
        && requests_stream(&body_bytes)
        && is_realtime;
    let conversation = if pool.affinity_enabled() && is_realtime {
        serde_json::from_slice::<serde_json::Value>(&body_bytes)
            .ok()
            .and_then(|body| {
                let header = original_headers
                    .get(affinity::SESSION_HEADER)
                    .and_then(|v| v.to_str().ok());
                affinity::conversation_key(header, &body)
            })
    } else {
        None
    };
    let billing_prices = req.extensions().get::<serving::BillingPrices>().copied();
    let mut providers = pool
        .select_iter_routed(aimd_eligible, conversation, &model_name, resolved_pool_name.unwrap_or("default"))
        .excluding_members(ineligible_members.iter().copied());
    while let Some((member_idx, target, connection_guard)) = providers.next() {
        any_attempted = true;
        attempt_number += 1;
        let provider_role = if member_idx == 0 {
            "preferred"
        } else {
            "alternate"
        };
        let attempt_start = tokio::time::Instant::now();
        let observation = pool.observe(aimd_eligible, member_idx, &model_name, resolved_pool_name.unwrap_or("default"), attempt_start);
        let first_token_deadline = first_token_timeout
            .filter(|_| (attempt_number as usize) < pool_max_attempts && providers.has_available_alternative(member_idx))
            .map(|timeout| attempt_start + timeout);

        let attempt_span = tracing::info_span!(
            "onwards.provider_attempt",
            otel.name = "onwards.provider_attempt",
            attempt = attempt_number,
            provider.url = %target.url,
            provider.model = target.onwards_model.as_deref().unwrap_or(""),
            provider.timeout_secs = target.request_timeout_secs,
            http.response.status_code = tracing::field::Empty,
            onwards.fallback = tracing::field::Empty,
            onwards.pool = tracing::field::Empty,
            onwards.hints_sent = tracing::field::Empty,
        );
        if let Some(pool_name) = resolved_pool_name {
            attempt_span.record("onwards.pool", pool_name);
        }

        enum LoopAction<T> {
            Continue(Option<OnwardsErrorResponse>),
            Done(T),
        }

        let original_model = req
            .extensions()
            .get::<OriginalModel>()
            .map(|m| m.0.to_string());

        let mut attempt_upstream_status: Option<u16> = None;

        let action = async {

        if let Some(ref limiter) = target.limiter
            && limiter.check().is_err()
        {
            debug!("Provider rate limited: {:?}", target.url);
            tracing::Span::current().record("onwards.fallback", "rate_limited");
            if pool.should_fallback_on_rate_limit() {
                debug!("Fallback on rate limit enabled, trying next provider");
                return LoopAction::Continue(Some(OnwardsErrorResponse::rate_limited()));
            } else {
                return LoopAction::Done(Err(OnwardsErrorResponse::rate_limited()));
            }
        }

        let response_headers = target.response_headers.clone();

        let mut attempt_body = body_bytes.clone();

        let scheduling_fields_refused =
            resolved_pool_name.is_some() && !target.accepts_scheduling_priority;
        if scheduling_fields_refused
            && (attempt_body.windows(10).any(|w| w == b"\"priority\"")
                || attempt_body.windows(7).any(|w| w == b"\"nvext\""))
            && let Ok(mut parsed) = serde_json::from_slice::<serde_json::Value>(&attempt_body)
            && let Some(obj) = parsed.as_object_mut()
        {
            let removed = [obj.shift_remove("priority"), obj.shift_remove("nvext")];
            if removed.iter().any(Option::is_some)
                && let Ok(stripped) = serde_json::to_vec(&parsed)
            {
                debug!("Stripped scheduling fields for a pool member that does not accept them");
                attempt_body = stripped.into();
            }
        }

        if let Some(ref rewrite) = target.onwards_model
            && !attempt_body.is_empty()
        {
            debug!("Rewriting model key to: {}", rewrite);
            let error = OnwardsErrorResponse::bad_request(
                "Could not parse onwards model from request. 'model' parameter must be supplied in either the body or in the Model-Override header.",
                Some("model"),
            );
            let mut body_serialized: serde_json::Value = match serde_json::from_slice(&attempt_body)
            {
                Ok(value) => value,
                Err(_) => return LoopAction::Done(Err(error.clone())),
            };
            let entry = match body_serialized.as_object_mut() {
                Some(obj) => obj.entry("model"),
                None => return LoopAction::Done(Err(error.clone())),
            };
            match entry {
                Entry::Occupied(mut entry) => {
                    entry.insert(serde_json::Value::String(rewrite.clone()));
                }
                Entry::Vacant(_entry) => {
                    return LoopAction::Done(Err(error.clone()));
                }
            }
            attempt_body = match serde_json::to_vec(&body_serialized) {
                Ok(bytes) => axum::body::Bytes::from(bytes),
                Err(_) => return LoopAction::Done(Err(OnwardsErrorResponse::internal())),
            };
        }

        if canonical_reasoning.is_some()
            && let Some(reasoning_translation) = target.reasoning_translation.as_ref()
            && !attempt_body.is_empty()
        {
            let mut body: serde_json::Value = match serde_json::from_slice(&attempt_body) {
                Ok(body) => body,
                Err(_) => {
                    return LoopAction::Done(Err(OnwardsErrorResponse::bad_request(
                        "Request body must be valid JSON.",
                        None,
                    )))
                }
            };
            if let Some(reasoning) = canonical_reasoning.as_ref()
                && let Err(error) = reasoning_translation.apply_request(
                    &canonical_request_path,
                    reasoning,
                    &mut body,
                )
            {
                return LoopAction::Done(Err(OnwardsErrorResponse::reasoning(&error)));
            }
            attempt_body = match serde_json::to_vec(&body) {
                Ok(bytes) => axum::body::Bytes::from(bytes),
                Err(_) => return LoopAction::Done(Err(OnwardsErrorResponse::internal())),
            };
        }

        let stamp_scheduling = !scheduling_fields_refused;
        if target.kind == ProviderKind::Dynamo
            && (stamp_scheduling || billing_prices.is_some())
            && !attempt_body.is_empty()
            && let Ok(mut body) = serde_json::from_slice::<serde_json::Value>(&attempt_body)
            && let Some(object) = body.as_object_mut()
        {
            if stamp_scheduling {
                serving_resolution.hints.stamp(object);
                if !serving_resolution.hints.is_empty() {
                    tracing::Span::current().record("onwards.hints_sent", true);
                }
                serving::stamp_default_priority(object);
            }
            if let Some(prices) = billing_prices {
                prices.stamp(object);
            }
            attempt_body = match serde_json::to_vec(&body) {
                Ok(bytes) => axum::body::Bytes::from(bytes),
                Err(_) => return LoopAction::Done(Err(OnwardsErrorResponse::internal())),
            };
        }

        let request_path = path_and_query.strip_prefix('/').unwrap_or(&path_and_query);
        let target_path = target.url.path().trim_end_matches('/');

        let path_to_join = if !target_path.is_empty() && target_path != "/" {
            let target_path_no_slash = &target_path[1..];
            if let Some(rest) = request_path.strip_prefix(target_path_no_slash) {
                if rest.is_empty() || rest.starts_with('/') {
                    rest.strip_prefix('/').unwrap_or(rest)
                } else {
                    request_path
                }
            } else {
                request_path
            }
        } else {
            request_path
        };

        let upstream_uri = match target.url.join(path_to_join) {
            Ok(url) => url.to_string(),
            Err(_) => return LoopAction::Done(Err(OnwardsErrorResponse::internal())),
        };
        let upstream_uri_parsed = match Uri::try_from(&upstream_uri) {
            Ok(uri) => uri,
            Err(_) => {
                error!("Invalid URI: {}", upstream_uri);
                return LoopAction::Done(Err(OnwardsErrorResponse::internal()));
            }
        };

        let mut attempt_headers = original_headers.clone();

        if let Some(host) = upstream_uri_parsed.host() {
            let host_value = if let Some(port) = upstream_uri_parsed.port_u16() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            };
            attempt_headers.insert("host", host_value.parse().unwrap());
        }

        attempt_headers.insert(
            CONTENT_LENGTH,
            attempt_body
                .len()
                .to_string()
                .parse()
                .expect("Content-Length should be valid"),
        );
        attempt_headers.remove(TRANSFER_ENCODING);

        filter_headers_for_upstream(&mut attempt_headers, target);

        if resolve_trace_propagation(target, pool.is_trusted()) {
            let ctx = tracing::Span::current().context();
            let propagator = opentelemetry_sdk::propagation::TraceContextPropagator::new();
            propagator.inject_context(&ctx, &mut HeaderInjector(&mut attempt_headers));
        } else {
            withhold_trace_context(&mut attempt_headers);
        }

        let attempt_req = axum::extract::Request::builder()
            .method(method.clone())
            .uri(upstream_uri_parsed)
            .body(axum::body::Body::from(attempt_body))
            .unwrap();
        let (mut parts, body) = attempt_req.into_parts();
        parts.headers = attempt_headers;
        let attempt_req = axum::extract::Request::from_parts(parts, body);

        trace!(
            "Outgoing request to provider:\n  URI: {}",
            upstream_uri
        );

        let upstream_span = tracing::info_span!(
            "onwards.upstream_request",
            otel.name = "onwards.upstream_request",
            otel.kind = "Client",
            http.request.method = %method,
            url.full = %upstream_uri,
            server.address = target.url.host_str().unwrap_or_default(),
            http.response.status_code = tracing::field::Empty,
        );
        let request_deadline = target
            .request_timeout_secs
            .map(|secs| tokio::time::Instant::now() + std::time::Duration::from_secs(secs));
        let header_deadline = match (request_deadline, first_token_deadline) {
            (Some(request), Some(first_token)) => Some(request.min(first_token)),
            (request, first_token) => request.or(first_token),
        };
        let request_result = async {
            if let Some(deadline) = header_deadline {
                match tokio::time::timeout_at(deadline, state.http_client.request(attempt_req))
                    .await
                {
                    Err(_) => {
                        debug!("Request to {} timed out waiting for response headers", upstream_uri);
                        Err(UpstreamOutcome::Timeout)
                    }
                    Ok(result) => result.map_err(UpstreamOutcome::Error),
                }
            } else {
                state.http_client.request(attempt_req).await.map_err(UpstreamOutcome::Error)
            }
        }
        .instrument(upstream_span.clone())
        .await;

        let mut response = match request_result {
            Err(UpstreamOutcome::Timeout) => {
                upstream_span.record("http.response.status_code", 504_u16);
                let reason = if header_deadline == first_token_deadline {
                    if let Some(observation) = &observation { observation.deadline(); }
                    metrics::counter!(
                        "onwards_first_token_breaches_total",
                        "model" => metric_model.clone(),
                        "serving_class" => metric_class.clone(),
                        "pool" => resolved_pool_name.unwrap_or("default"),
                        "role" => provider_role,
                    )
                    .increment(1);
                    "first_token_timeout"
                } else {
                    "timeout"
                };
                tracing::Span::current().record("onwards.fallback", reason);
                if pool.fallback_enabled() {
                    return LoopAction::Continue(Some(OnwardsErrorResponse::gateway_timeout()));
                } else {
                    record_response_status(504);
                    return LoopAction::Done(Err(OnwardsErrorResponse::gateway_timeout()));
                }
            }
            Err(UpstreamOutcome::Error(e)) => {
                error!(
                    "Error forwarding request to target url {}: {}",
                    upstream_uri, e
                );
                tracing::Span::current().record("onwards.fallback", "network_error");
                if pool.fallback_enabled() {
                    return LoopAction::Continue(Some(OnwardsErrorResponse::bad_gateway()));
                } else {
                    return LoopAction::Done(Err(OnwardsErrorResponse::bad_gateway()));
                }
            }
            Ok(response) => response,
        };

        let status = response.status().as_u16();
        last_upstream_status = Some(status);
        attempt_upstream_status = Some(status);
        upstream_span.record("http.response.status_code", status);
        tracing::Span::current().record("http.response.status_code", status);
        if !(200..300).contains(&status)
            && let Some(observation) = &observation
        {
            observation.status(status);
        }

        if fails_over_on(status, attempt_number) {
            debug!(
                "Provider returned fallback status {}, trying next: {:?}",
                status, target.url
            );
            tracing::Span::current().record("onwards.fallback", "status_fallback");
            let error = if status == 429 {
                OnwardsErrorResponse::upstream_rate_limited(state.upstream_rate_limit_message.as_deref())
            } else if is_capacity_refusal(status) {
                OnwardsErrorResponse::overloaded()
            } else {
                OnwardsErrorResponse::bad_gateway()
            };
            return LoopAction::Continue(Some(error));
        }

        if is_capacity_refusal(status) {
            record_response_status(529);
            return LoopAction::Done(Err(OnwardsErrorResponse::overloaded()));
        }

        if status == 429
            && (target.sanitize_response || state.upstream_rate_limit_message.is_some())
        {
            record_response_status(429);
            return LoopAction::Done(Err(OnwardsErrorResponse::upstream_rate_limited(
                state.upstream_rate_limit_message.as_deref(),
            )));
        }

        if target.sanitize_response && !(200..300).contains(&status) {
            let error_body = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .ok();

            error!(
                status = status,
                upstream = %target.url,
                body_len = error_body.as_ref().map(|b| b.len()).unwrap_or(0),
                "Upstream provider returned error, sanitizing before forwarding to client"
            );

            let sanitized_error = if (400..500).contains(&status) {
                OnwardsErrorResponse::builder()
                    .body(ErrorResponseBody {
                        message: "The upstream provider rejected the request.".to_string(),
                        r#type: "invalid_request_error".to_string(),
                        param: None,
                        code: "upstream_error".to_string(),
                    })
                    .status(StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST))
                    .from_upstream(true)
                    .build()
            } else {
                OnwardsErrorResponse::builder()
                    .body(ErrorResponseBody {
                        message: "An internal error occurred. Please try again later.".to_string(),
                        r#type: "internal_error".to_string(),
                        param: None,
                        code: "internal_error".to_string(),
                    })
                    .status(StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY))
                    .from_upstream(true)
                    .build()
            };

            record_response_status(status);
            return LoopAction::Done(Err(sanitized_error));
        }

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let is_sse = content_type.contains("text/event-stream");

        enum Scan2xx {
            Clean,
            Embedded(u16, serde_json::Value),
            EmptyBody,
        }
        let scan: Scan2xx = if (200..300).contains(&status)
            && state.targets.strict_mode
        {
            if is_sse {
                use futures_util::StreamExt;
                #[cfg(not(test))]
                const SSE_PEEK_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);
                #[cfg(test)]
                const SSE_PEEK_BUDGET: std::time::Duration = std::time::Duration::from_millis(10);
                #[cfg(not(test))]
                const SSE_DECISIVE_WAIT_BUDGET: std::time::Duration =
                    std::time::Duration::from_secs(30);
                #[cfg(test)]
                const SSE_DECISIVE_WAIT_BUDGET: std::time::Duration =
                    std::time::Duration::from_millis(50);
                const SSE_PEEK_MAX_EVENTS: usize = 4;

                let (parts, body) = response.into_parts();
                let mut stream_observation = observation.clone();
                let mut events = SseBufferedStream::with_limit(body.into_data_stream(), state.sse_buffer_limit).inspect(move |event| {
                    if stream_observation.is_none() { return; }
                    let kind = match event {
                        Ok(bytes) if bytes.ends_with(b"\n\n") => classify_sse_event(bytes),
                        _ => SseEventKind::Done,
                    };
                    if matches!(kind, SseEventKind::Comment) { return; }
                    if let Some(observation) = stream_observation.take() {
                        match kind {
                            SseEventKind::Data => observation.frame(),
                            SseEventKind::Error(status, _) => observation.status(status),
                            _ => observation.unknown(),
                        }
                    }
                });
                let mut peeked = Vec::new();
                let mut lead = LeadFrames::default();
                if let Some(deadline) = first_token_deadline {
                    if tokio::time::timeout_at(
                        deadline,
                        read_lead_frames(&mut events, &mut peeked, &mut lead, usize::MAX),
                    )
                    .await
                    .is_err()
                    {
                        if let Some(observation) = &observation { observation.deadline(); }
                        warn!(
                            upstream = %target.url,
                            "No first token before the failover deadline; trying the next provider"
                        );
                        tracing::Span::current().record("onwards.fallback", "first_token_timeout");
                        metrics::counter!(
                            "onwards_first_token_breaches_total",
                            "model" => metric_model.clone(),
                            "serving_class" => metric_class.clone(),
                            "pool" => resolved_pool_name.unwrap_or("default"),
                            "role" => provider_role,
                        )
                        .increment(1);
                        return LoopAction::Continue(Some(OnwardsErrorResponse::gateway_timeout()));
                    }
                } else if tokio::time::timeout(
                    SSE_PEEK_BUDGET,
                    read_lead_frames(&mut events, &mut peeked, &mut lead, SSE_PEEK_MAX_EVENTS),
                )
                .await
                .is_err()
                {
                    debug!("Timed out peeking SSE lead frames; waiting for first terminal frame");
                    if tokio::time::timeout(
                        SSE_DECISIVE_WAIT_BUDGET,
                        read_lead_frames(&mut events, &mut peeked, &mut lead, SSE_PEEK_MAX_EVENTS),
                    )
                    .await
                    .is_err()
                    {
                        debug!(
                            "Timed out waiting for decisive SSE frame; forwarding stream unmodified"
                        );
                    }
                }
                if lead.saw_content {
                    metrics::histogram!(
                        "onwards_first_token_seconds",
                        "model" => metric_model.clone(),
                        "serving_class" => metric_class.clone(),
                        "pool" => resolved_pool_name.unwrap_or("default"),
                        "role" => provider_role,
                    )
                    .record(attempt_start.elapsed().as_secs_f64());
                }
                let rest = futures_util::stream::iter(peeked).chain(events);
                response = Response::from_parts(parts, axum::body::Body::from_stream(rest));
                if let Some((status, error)) = lead.embedded {
                    Scan2xx::Embedded(status, error)
                } else if lead.stream_ended && !lead.saw_data {
                    Scan2xx::EmptyBody
                } else {
                    Scan2xx::Clean
                }
            } else {
                let (parts, body) = response.into_parts();
                let buffered = match axum::body::to_bytes(body, usize::MAX).await {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        error!("Failed to buffer response body for embedded-error scan: {}", e);
                        return LoopAction::Done(Err(OnwardsErrorResponse::internal()));
                    }
                };
                if buffered.is_empty() {
                    response = Response::from_parts(parts, axum::body::Body::empty());
                    Scan2xx::EmptyBody
                } else {
                    const ERROR_KEY: &[u8] = b"\"error\"";
                    let embedded = if buffered.windows(ERROR_KEY.len()).any(|w| w == ERROR_KEY) {
                        serde_json::from_slice::<serde_json::Value>(&buffered)
                            .ok()
                            .and_then(|body| {
                                embedded_error_status(&body)
                                    .map(|status| (status, body["error"].clone()))
                            })
                    } else {
                        None
                    };
                    let len = buffered.len();
                    response = Response::from_parts(parts, axum::body::Body::from(buffered));
                    response.headers_mut().remove(TRANSFER_ENCODING);
                    response
                        .headers_mut()
                        .insert(CONTENT_LENGTH, HeaderValue::from(len));
                    match embedded {
                        Some((status, error)) => Scan2xx::Embedded(status, error),
                        None => Scan2xx::Clean,
                    }
                }
            }
        } else {
            Scan2xx::Clean
        };

        match scan {
            Scan2xx::Embedded(embedded, provider_error) => {
                warn!(
                    http_status = status,
                    embedded_status = embedded,
                    upstream = %target.url,
                    "Upstream returned 2xx with an embedded provider error; treating it as status {embedded}"
                );
                upstream_span.record("http.response.status_code", embedded);
                tracing::Span::current().record("http.response.status_code", embedded);
                last_upstream_status = Some(embedded);
                attempt_upstream_status = Some(embedded);

                let retryable = fails_over_on(embedded, attempt_number)
                    || (embedded == 429 && pool.should_fallback_on_rate_limit());
                if retryable {
                    tracing::Span::current().record("onwards.fallback", "embedded_error");
                    let error = if embedded == 429 {
                        OnwardsErrorResponse::upstream_rate_limited(state.upstream_rate_limit_message.as_deref())
                    } else if is_capacity_refusal(embedded) {
                        OnwardsErrorResponse::overloaded().reported_by_upstream()
                    } else {
                        OnwardsErrorResponse::service_unavailable().reported_by_upstream()
                    };
                    return LoopAction::Continue(Some(error));
                }

                if is_capacity_refusal(embedded) {
                    record_response_status(529);
                    return LoopAction::Done(Err(OnwardsErrorResponse::overloaded().reported_by_upstream()));
                }
                record_response_status(embedded);
                let trusted = target.trusted.unwrap_or_else(|| pool.is_trusted());
                let err = if embedded == 429 {
                    OnwardsErrorResponse::upstream_rate_limited(state.upstream_rate_limit_message.as_deref())
                } else if embedded >= 500 {
                    OnwardsErrorResponse::service_unavailable().reported_by_upstream()
                } else {
                    OnwardsErrorResponse::builder()
                        .body(ErrorResponseBody {
                            message: trusted
                                .then(|| provider_error["message"].as_str())
                                .flatten()
                                .unwrap_or("The upstream provider rejected the request.")
                                .to_string(),
                            r#type: trusted
                                .then(|| provider_error["type"].as_str())
                                .flatten()
                                .unwrap_or("invalid_request_error")
                                .to_string(),
                            param: trusted
                                .then(|| provider_error["param"].as_str())
                                .flatten()
                                .map(str::to_string),
                            code: if trusted {
                                embedded.to_string()
                            } else {
                                "upstream_error".to_string()
                            },
                        })
                        .status(StatusCode::from_u16(embedded).unwrap_or(StatusCode::BAD_REQUEST))
                        .from_upstream(true)
                        .build()
                };
                return LoopAction::Done(Err(err));
            }
            Scan2xx::EmptyBody => {
                warn!(
                    http_status = status,
                    upstream = %target.url,
                    "Upstream returned 2xx with an empty body before any content; treating it as 502"
                );
                upstream_span.record("http.response.status_code", 502_u16);
                tracing::Span::current().record("http.response.status_code", 502_u16);

                if pool.should_fallback_on_status(502) {
                    tracing::Span::current().record("onwards.fallback", "empty_body");
                    return LoopAction::Continue(Some(OnwardsErrorResponse::service_unavailable().reported_by_upstream()));
                }

                record_response_status(503);
                return LoopAction::Done(Err(OnwardsErrorResponse::service_unavailable().reported_by_upstream()));
            }
            Scan2xx::Clean => {}
        }

        let needs_sse_buffering = !state.targets.strict_mode
            && state.response_transform_fn.is_some()
            && target.sanitize_response
            && (200..300).contains(&status);

        if is_sse && needs_sse_buffering {
            debug!("Wrapping SSE response with buffered stream for non-strict sanitization");
            let (parts, body) = response.into_parts();
            let byte_stream = body.into_data_stream();
            let buffered = SseBufferedStream::with_limit(byte_stream, state.sse_buffer_limit);
            let new_body = axum::body::Body::from_stream(buffered);
            response = Response::from_parts(parts, new_body);
        }

        if let Some(ref transform_fn) = state.response_transform_fn
            && target.sanitize_response
            && (200..300).contains(&status)
            && !state.targets.strict_mode
        {
            debug!(
                "Attempting response sanitization for status {}, path {}",
                status, path_and_query
            );


            if is_sse {
                debug!("Applying streaming sanitization");

                let sanitizer = crate::response_sanitizer::ResponseSanitizer {
                    original_model: original_model.clone(),
                };

                use futures_util::StreamExt;

                let body_stream =
                    http_body_util::BodyExt::into_data_stream(std::mem::take(response.body_mut()));

                let transformed_stream = body_stream.map(move |chunk_result| {
                    match chunk_result {
                        Ok(chunk) => {
                            match sanitizer.sanitize_streaming(&chunk) {
                                Ok(Some(sanitized)) => Ok::<_, std::io::Error>(sanitized),
                                Ok(None) => Ok(chunk),
                                Err(e) => {
                                    tracing::error!("Failed to sanitize streaming chunk: {}", e);
                                    Ok(chunk)
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Stream error: {}", e);
                            Err(std::io::Error::other(e))
                        }
                    }
                });

                *response.body_mut() = axum::body::Body::from_stream(transformed_stream);
            } else {
                debug!("Applying non-streaming sanitization");

                let response_body = match
                    axum::body::to_bytes(std::mem::take(response.body_mut()), usize::MAX)
                        .await
                {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        error!("Failed to buffer response body: {}", e);
                        return LoopAction::Done(Err(OnwardsErrorResponse::internal()));
                    }
                };

                debug!(
                    "Response body buffered: {} bytes, content-type: {}",
                    response_body.len(),
                    content_type
                );

                match transform_fn(
                    &path_and_query,
                    response.headers(),
                    &response_body,
                    original_model.as_deref(),
                ) {
                    Ok(Some(transformed_body)) => {
                        let content_length = transformed_body.len();
                        debug!(
                            "Sanitization successful: {} bytes -> {} bytes",
                            response_body.len(),
                            content_length
                        );
                        *response.body_mut() = axum::body::Body::from(transformed_body);

                        response.headers_mut().remove(TRANSFER_ENCODING);
                        response
                            .headers_mut()
                            .insert(CONTENT_LENGTH, HeaderValue::from(content_length));
                    }
                    Ok(None) => {
                        debug!(
                            "Sanitization returned None, restoring original {} bytes",
                            response_body.len()
                        );
                        let content_length = response_body.len();
                        *response.body_mut() = axum::body::Body::from(response_body);

                        response.headers_mut().remove(TRANSFER_ENCODING);
                        response
                            .headers_mut()
                            .insert(CONTENT_LENGTH, HeaderValue::from(content_length));
                    }
                    Err(e) => {
                        error!("Response sanitization failed: {}", e);
                        return LoopAction::Done(Err(OnwardsErrorResponse::internal()));
                    }
                }
            }
        }

        if let Some(ref header_name) = state.response_id_header
            && crate::response_id::path_supports_id_override(&path_and_query)
            && (200..300).contains(&status)
            && let Some(override_id) =
                crate::response_id::extract_override_id(&original_headers, header_name)
        {
            crate::response_id::patch_response_body_id(&mut response, override_id).await;
        }

        if let Some(headers) = response_headers {
            for (key, value) in headers.iter() {
                if let (Ok(header_name), Ok(header_value)) =
                    (key.parse::<HeaderName>(), value.parse::<HeaderValue>())
                {
                    response.headers_mut().insert(header_name, header_value);
                }
            }
            trace!(
                model = %model_name,
                headers = ?headers,
                "Added custom response headers"
            );
        }

        record_response_status(response.status().as_u16());
        debug!(
            "Returning response with status {}, content-length: {:?}, strict_mode: {}",
            response.status(),
            response.headers().get(CONTENT_LENGTH),
            state.targets.strict_mode
        );
        let resolved_trust = target.trusted.unwrap_or_else(|| pool.is_trusted());
        response
            .extensions_mut()
            .insert(ResolvedTrust(resolved_trust));
        response.extensions_mut().insert(ServedBy {
            url: target.url.to_string(),
            onwards_model: target.onwards_model.clone(),
        });
        response.extensions_mut().insert(UpstreamFailureLabels {
            model: metric_model.clone(),
            serving_class: metric_class.clone(),
            traffic,
            served_by: target.url.host_str().unwrap_or_default().to_string(),
        });
        response
            .extensions_mut()
            .insert::<ServingClassOutcome>(serving_resolution.outcome());
        if let Some(api_key_id) = authenticated_api_key_id {
            response
                .extensions_mut()
                .insert(AuthenticatedApiKeyId(api_key_id));
        }

        let (parts, body) = response.into_parts();
        let guarded = GuardedStream {
            inner: body.into_data_stream(),
            _guard: connection_guard,
            _inflight_guard: inflight_guard.take().expect("inflight_guard taken once on success path"),
            _inflight_slot: inflight_slot.take(),
            _batch_inflight_slot: batch_inflight_slot.take(),
        };
        let response = Response::from_parts(parts, axum::body::Body::from_stream(guarded));

        LoopAction::Done(Ok(response))

        }.instrument(attempt_span).await;

        match action {
            LoopAction::Continue(err) => {
                last_error = err;
                if attempt_upstream_status.is_some_and(|s| s == 429 || is_capacity_refusal(s)) {
                    providers.mark_refused(member_idx);
                }
                if (attempt_number as usize) < pool_max_attempts
                    && let Some(backoff) = pool.fallback().and_then(|f| f.backoff.as_ref())
                {
                    let retry_index = attempt_number;
                    let delay = backoff.delay(retry_index);
                    let delay_ms = delay.as_millis() as u64;
                    let next_total = total_backoff_ms.saturating_add(delay_ms);
                    if let Some(max_total) = pool.fallback().and_then(|f| f.max_total_backoff_ms)
                        && next_total > max_total
                    {
                        debug!(
                            used_ms = total_backoff_ms,
                            next_ms = delay_ms,
                            cap_ms = max_total,
                            "Retry backoff budget exhausted, giving up"
                        );
                        metrics::counter!("onwards_retry_budget_exhausted_total").increment(1);
                        break;
                    }
                    total_backoff_ms = next_total;
                    metrics::counter!("onwards_retries_total").increment(1);
                    debug!(
                        retry = retry_index,
                        delay_ms, "Backing off before next provider attempt"
                    );
                    tokio::time::sleep(delay).await;
                }
                continue;
            }
            LoopAction::Done(result) => return result,
        }
    }

    if any_attempted {
        let final_error = last_error
            .unwrap_or_else(|| OnwardsErrorResponse::model_not_found(model_name.as_str()));
        let status = final_error.status.as_u16();
        metrics::counter!(
            "onwards_upstream_failed_total",
            "reason" => "retries_exhausted",
            "status" => last_upstream_status.unwrap_or(status).to_string(),
            "model" => metric_model.clone(),
            "serving_class" => metric_class.clone(),
            "traffic" => traffic,
        )
        .increment(1);
        record_response_status(status);
        let mut final_error = final_error;
        final_error.serving_outcome = Some(serving_resolution.outcome());
        Err(final_error)
    } else if no_eligible_member {
        metrics::counter!(
            "onwards_upstream_failed_total",
            "reason" => "no_eligible_provider",
            "status" => "503",
            "model" => metric_model.clone(),
            "serving_class" => metric_class.clone(),
            "traffic" => traffic,
        )
        .increment(1);
        record_response_status(503);
        Err(OnwardsErrorResponse::builder()
            .body(ErrorResponseBody {
                message: format!("Model '{model_name}' has no self-hosted provider available to this account."),
                r#type: "server_error".to_string(),
                param: Some("model".to_string()),
                code: "no_eligible_provider".to_string(),
            })
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .serving_outcome(serving_resolution.outcome())
            .build())
    } else if !pool.is_empty() {
        metrics::counter!(
            "onwards_upstream_failed_total",
            "reason" => "all_at_capacity",
            "status" => "529",
            "model" => metric_model.clone(),
            "serving_class" => metric_class.clone(),
            "traffic" => traffic,
        )
        .increment(1);
        record_response_status(529);
        Err(OnwardsErrorResponse::overloaded())
    } else {
        let err = OnwardsErrorResponse::model_not_found(model_name.as_str());
        let status = err.status.as_u16();
        metrics::counter!(
            "onwards_upstream_failed_total",
            "reason" => "empty_pool_late",
            "status" => status.to_string(),
            "model" => metric_model.clone(),
            "serving_class" => metric_class.clone(),
            "traffic" => traffic,
        )
        .increment(1);
        record_response_status(status);
        Err(err)
    }
    }
    .await;

    result.map_err(|error| error.with_authenticated_api_key_id(authenticated_api_key_id))
    }
    .await;

    if let Err(error) = &result {
        rejection.record_error(error);
    }
    rejection.record_identity();
    let identity = rejection.identity();
    let final_status = match &result {
        Ok(response) => response.status().as_u16(),
        Err(error) => error.status.as_u16(),
    };
    record_response_status(final_status);
    result
        .map(|mut response| {
            if let Some(identity) = identity.clone() {
                response.extensions_mut().insert(identity);
            }
            if response.status().as_u16() >= 400
                && response.extensions().get::<crate::ErrorOrigin>().is_none()
            {
                response.extensions_mut().insert(crate::ErrorOrigin::Provider);
            }
            response
        })
        .map_err(|error| error.with_identity(identity))
    }
    .instrument(span)
    .await
}

#[instrument(skip(state, req))]
pub async fn models<T: HttpClient>(State(state): State<AppState<T>>, req: Request) -> Response {
    let bearer_token = auth::presented_key(&req);

    let accessible_models: Vec<String> = state
        .targets
        .targets
        .iter()
        .filter(|entry| {
            let pool = entry.value().default_pool();

            let Some(keys) = pool.keys() else {
                return true;
            };

            let Some(token) = bearer_token.as_ref() else {
                return false;
            };

            auth::validate_bearer_token(keys, token)
        })
        .map(|entry| entry.key().clone())
        .collect();

    let authenticated_api_key_id = bearer_token
        .as_ref()
        .and_then(|token| state.targets.key_labels.get(token))
        .and_then(|labels| labels.get(auth::API_KEY_ID_LABEL).cloned())
        .and_then(|id| id.parse::<Uuid>().ok());

    let mut response =
        Json(ListModelResponse::from_model_names(&accessible_models)).into_response();
    if let Some(api_key_id) = authenticated_api_key_id {
        response
            .extensions_mut()
            .insert(AuthenticatedApiKeyId(api_key_id));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_error_status_detects_provider_envelope() {
        let body =
            serde_json::json!({"error": {"code": 429, "message": "Provider returned error"}});
        assert_eq!(embedded_error_status(&body), Some(429));

        let body = serde_json::json!({
            "id": "gen-x", "object": "chat.completion.chunk", "choices": [],
            "error": {"code": 503, "message": "upstream"}
        });
        assert_eq!(embedded_error_status(&body), Some(503));

        assert_eq!(
            embedded_error_status(&serde_json::json!({"error": {"code": "502"}})),
            Some(502)
        );
    }

    #[test]
    fn embedded_error_status_ignores_normal_and_ambiguous_bodies() {
        let ok = serde_json::json!({
            "id": "chatcmpl-1", "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}}]
        });
        assert_eq!(embedded_error_status(&ok), None);

        assert_eq!(
            embedded_error_status(&serde_json::json!({"error": {"message": "x"}})),
            None
        );
        assert_eq!(
            embedded_error_status(&serde_json::json!({"error": {"code": null}})),
            None
        );
        assert_eq!(
            embedded_error_status(&serde_json::json!({"error": {"code": "rate_limit_exceeded"}})),
            None
        );
        assert_eq!(
            embedded_error_status(&serde_json::json!({"error": {"code": 200}})),
            None
        );
        assert_eq!(
            embedded_error_status(&serde_json::json!({"error": {"code": 999}})),
            None
        );
    }

    #[test]
    fn classify_sse_event_distinguishes_error_data_and_comment() {
        use SseEventKind::{Comment, Data, Done, Error};

        assert_eq!(
            classify_sse_event(b"data: {\"error\":{\"code\":429,\"message\":\"x\"}}\n\n"),
            Error(429, serde_json::json!({"code":429,"message":"x"}))
        );
        assert_eq!(
            classify_sse_event(b"data: {\"id\":\"g\",\"choices\":[],\"error\":{\"code\":502}}\n\n"),
            Error(502, serde_json::json!({"code":502}))
        );
        assert_eq!(
            classify_sse_event(b"data:{\"error\":{\"code\":503}}\n\n"),
            Error(503, serde_json::json!({"code":503}))
        );
        assert_eq!(
            classify_sse_event(b"data: {\"error\":\ndata: {\"code\":429}}\n\n"),
            Error(429, serde_json::json!({"code":429}))
        );

        assert_eq!(
            classify_sse_event(b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"),
            Data
        );
        assert_eq!(classify_sse_event(b"data: [DONE]\n\n"), Done);

        assert_eq!(classify_sse_event(b": keep-alive\n\n"), Comment);
        assert_eq!(classify_sse_event(b": ping\n\n"), Comment);

        assert_eq!(classify_sse_event(&[0xFF, 0xFE]), Data);
    }

    #[test]
    fn test_filter_headers_strips_hop_by_hop_headers() {
        let mut headers = HeaderMap::new();

        headers.insert("connection", "keep-alive".parse().unwrap());
        headers.insert("keep-alive", "timeout=5".parse().unwrap());
        headers.insert("proxy-authenticate", "Basic".parse().unwrap());
        headers.insert("proxy-authorization", "Basic abc123".parse().unwrap());
        headers.insert("te", "trailers".parse().unwrap());
        headers.insert("trailer", "Expires".parse().unwrap());
        headers.insert("upgrade", "HTTP/2.0".parse().unwrap());

        headers.insert("content-type", "application/json".parse().unwrap());

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(!headers.contains_key("connection"));
        assert!(!headers.contains_key("keep-alive"));
        assert!(!headers.contains_key("proxy-authenticate"));
        assert!(!headers.contains_key("proxy-authorization"));
        assert!(!headers.contains_key("te"));
        assert!(!headers.contains_key("trailer"));
        assert!(!headers.contains_key("upgrade"));

        assert!(headers.contains_key("content-type"));
    }

    #[test]
    fn test_filter_headers_strips_auth_headers() {
        let mut headers = HeaderMap::new();

        headers.insert("authorization", "Bearer client-token".parse().unwrap());
        headers.insert("x-api-key", "client-api-key".parse().unwrap());
        headers.insert("api-key", "another-key".parse().unwrap());

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(!headers.contains_key("authorization"));
        assert!(!headers.contains_key("x-api-key"));
        assert!(!headers.contains_key("api-key"));
    }

    #[test]
    fn test_filter_headers_strips_browser_security_headers() {
        let mut headers = HeaderMap::new();

        headers.insert("sec-fetch-site", "cross-site".parse().unwrap());
        headers.insert("sec-fetch-mode", "cors".parse().unwrap());
        headers.insert("sec-fetch-dest", "empty".parse().unwrap());
        headers.insert("sec-fetch-user", "?1".parse().unwrap());

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(!headers.contains_key("sec-fetch-site"));
        assert!(!headers.contains_key("sec-fetch-mode"));
        assert!(!headers.contains_key("sec-fetch-dest"));
        assert!(!headers.contains_key("sec-fetch-user"));
    }

    #[test]
    fn test_filter_headers_strips_all_sec_ch_ua_variants() {
        let mut headers = HeaderMap::new();

        headers.insert("sec-ch-ua", "\"Chrome\";v=\"120\"".parse().unwrap());
        headers.insert("sec-ch-ua-mobile", "?0".parse().unwrap());
        headers.insert("sec-ch-ua-platform", "\"macOS\"".parse().unwrap());
        headers.insert("sec-ch-ua-arch", "\"arm64\"".parse().unwrap());
        headers.insert("sec-ch-ua-model", "\"\"".parse().unwrap());

        headers.insert("user-agent", "Mozilla/5.0...".parse().unwrap());

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(!headers.contains_key("sec-ch-ua"));
        assert!(!headers.contains_key("sec-ch-ua-mobile"));
        assert!(!headers.contains_key("sec-ch-ua-platform"));
        assert!(!headers.contains_key("sec-ch-ua-arch"));
        assert!(!headers.contains_key("sec-ch-ua-model"));

        assert!(headers.contains_key("user-agent"));
    }

    #[test]
    fn test_filter_headers_strips_browser_context_headers() {
        let mut headers = HeaderMap::new();

        headers.insert("origin", "http://localhost:3000".parse().unwrap());
        headers.insert("referer", "http://localhost:3000/chat".parse().unwrap());
        headers.insert("cookie", "session=abc123".parse().unwrap());

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(!headers.contains_key("origin"));
        assert!(!headers.contains_key("referer"));
        assert!(!headers.contains_key("cookie"));
    }

    #[test]
    fn test_filter_headers_strips_caching_headers() {
        let mut headers = HeaderMap::new();

        headers.insert(
            "if-modified-since",
            "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap(),
        );
        headers.insert("if-none-match", "\"abc123\"".parse().unwrap());
        headers.insert("if-match", "\"xyz789\"".parse().unwrap());
        headers.insert(
            "if-unmodified-since",
            "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap(),
        );
        headers.insert("if-range", "\"abc123\"".parse().unwrap());

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(!headers.contains_key("if-modified-since"));
        assert!(!headers.contains_key("if-none-match"));
        assert!(!headers.contains_key("if-match"));
        assert!(!headers.contains_key("if-unmodified-since"));
        assert!(!headers.contains_key("if-range"));
    }

    #[test]
    fn test_filter_headers_strips_model_override_header() {
        let mut headers = HeaderMap::new();

        headers.insert("model-override", "gpt-4".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(!headers.contains_key("model-override"));
        assert!(headers.contains_key("content-type"));
    }

    #[test]
    fn test_filter_headers_keeps_safe_headers() {
        let mut headers = HeaderMap::new();

        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("accept", "application/json".parse().unwrap());
        headers.insert("user-agent", "MyClient/1.0".parse().unwrap());
        headers.insert("accept-language", "en-US,en;q=0.9".parse().unwrap());
        headers.insert("accept-encoding", "gzip, deflate, br".parse().unwrap());
        headers.insert("x-stainless-lang", "js".parse().unwrap());
        headers.insert("x-stainless-os", "macOS".parse().unwrap());

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(headers.contains_key("content-type"));
        assert!(headers.contains_key("accept"));
        assert!(headers.contains_key("user-agent"));
        assert!(headers.contains_key("accept-language"));
        assert!(headers.contains_key("accept-encoding"));
        assert!(headers.contains_key("x-stainless-lang"));
        assert!(headers.contains_key("x-stainless-os"));
    }

    #[test]
    fn test_filter_headers_adds_authorization_when_onwards_key_present() {
        let mut headers = HeaderMap::new();

        headers.insert("authorization", "Bearer client-token".parse().unwrap());

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .onwards_key("sk-upstream-key".to_string())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(headers.contains_key("authorization"));
        assert_eq!(
            headers.get("authorization").unwrap().to_str().unwrap(),
            "Bearer sk-upstream-key"
        );
    }

    #[test]
    fn test_filter_headers_no_authorization_when_onwards_key_absent() {
        let mut headers = HeaderMap::new();

        headers.insert("authorization", "Bearer client-token".parse().unwrap());

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(!headers.contains_key("authorization"));
    }

    #[test]
    fn test_filter_headers_custom_auth_header_name() {
        let mut headers = HeaderMap::new();

        headers.insert("authorization", "Bearer client-token".parse().unwrap());

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .onwards_key("my-api-key-123".to_string())
            .upstream_auth_header_name("X-API-Key".to_string())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(!headers.contains_key("authorization"));

        assert!(headers.contains_key("x-api-key"));
        assert_eq!(
            headers.get("x-api-key").unwrap().to_str().unwrap(),
            "Bearer my-api-key-123"
        );
    }

    #[test]
    fn test_filter_headers_custom_auth_header_prefix() {
        let mut headers = HeaderMap::new();

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .onwards_key("token-xyz".to_string())
            .upstream_auth_header_prefix("ApiKey ".to_string())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(headers.contains_key("authorization"));
        assert_eq!(
            headers.get("authorization").unwrap().to_str().unwrap(),
            "ApiKey token-xyz"
        );
    }

    #[test]
    fn test_filter_headers_empty_auth_header_prefix() {
        let mut headers = HeaderMap::new();

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .onwards_key("plain-api-key-456".to_string())
            .upstream_auth_header_prefix("".to_string())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(headers.contains_key("authorization"));
        assert_eq!(
            headers.get("authorization").unwrap().to_str().unwrap(),
            "plain-api-key-456"
        );
    }

    #[test]
    fn test_filter_headers_custom_header_name_and_prefix() {
        let mut headers = HeaderMap::new();

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .onwards_key("secret-key".to_string())
            .upstream_auth_header_name("X-Custom-Auth".to_string())
            .upstream_auth_header_prefix("Token ".to_string())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(!headers.contains_key("authorization"));
        assert!(headers.contains_key("x-custom-auth"));
        assert_eq!(
            headers.get("x-custom-auth").unwrap().to_str().unwrap(),
            "Token secret-key"
        );
    }

    #[test]
    fn test_filter_headers_adds_x_forwarded_proto() {
        let mut headers = HeaderMap::new();

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(headers.contains_key("x-forwarded-proto"));
        assert_eq!(
            headers.get("x-forwarded-proto").unwrap().to_str().unwrap(),
            "https"
        );
    }

    #[test]
    fn test_filter_headers_comprehensive_browser_request() {
        let mut headers = HeaderMap::new();

        headers.insert("connection", "keep-alive".parse().unwrap());

        headers.insert("authorization", "Bearer client-secret".parse().unwrap());

        headers.insert("sec-fetch-site", "same-origin".parse().unwrap());
        headers.insert("sec-fetch-mode", "cors".parse().unwrap());
        headers.insert("sec-fetch-dest", "empty".parse().unwrap());
        headers.insert("sec-ch-ua", "\"Chrome\";v=\"120\"".parse().unwrap());
        headers.insert("sec-ch-ua-mobile", "?0".parse().unwrap());
        headers.insert("sec-ch-ua-platform", "\"macOS\"".parse().unwrap());

        headers.insert("origin", "http://localhost:5173".parse().unwrap());
        headers.insert(
            "referer",
            "http://localhost:5173/playground".parse().unwrap(),
        );
        headers.insert("cookie", "session=xyz; token=abc".parse().unwrap());

        headers.insert("if-none-match", "\"abc123\"".parse().unwrap());

        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("accept", "application/json".parse().unwrap());
        headers.insert("user-agent", "Mozilla/5.0...".parse().unwrap());

        let target = Target::builder()
            .url("https://api.anthropic.com".parse().unwrap())
            .onwards_key("sk-ant-upstream-key".to_string())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(!headers.contains_key("connection"));
        assert!(!headers.contains_key("sec-fetch-site"));
        assert!(!headers.contains_key("sec-fetch-mode"));
        assert!(!headers.contains_key("sec-fetch-dest"));
        assert!(!headers.contains_key("sec-ch-ua"));
        assert!(!headers.contains_key("sec-ch-ua-mobile"));
        assert!(!headers.contains_key("sec-ch-ua-platform"));
        assert!(!headers.contains_key("origin"));
        assert!(!headers.contains_key("referer"));
        assert!(!headers.contains_key("cookie"));
        assert!(!headers.contains_key("if-none-match"));

        assert!(headers.contains_key("content-type"));
        assert!(headers.contains_key("accept"));
        assert!(headers.contains_key("user-agent"));

        assert_eq!(
            headers.get("authorization").unwrap().to_str().unwrap(),
            "Bearer sk-ant-upstream-key"
        );

        assert_eq!(
            headers.get("x-forwarded-proto").unwrap().to_str().unwrap(),
            "https"
        );
    }

    #[test]
    fn test_filter_headers_preserves_provider_specific_headers() {
        let mut headers = HeaderMap::new();

        headers.insert("anthropic-version", "2023-06-01".parse().unwrap());
        headers.insert("openai-organization", "org-123".parse().unwrap());
        headers.insert("x-stainless-lang", "js".parse().unwrap());
        headers.insert("x-stainless-runtime", "browser:chrome".parse().unwrap());

        let target = Target::builder()
            .url("https://api.anthropic.com".parse().unwrap())
            .build();

        filter_headers_for_upstream(&mut headers, &target);

        assert!(headers.contains_key("anthropic-version"));
        assert!(headers.contains_key("openai-organization"));
        assert!(headers.contains_key("x-stainless-lang"));
        assert!(headers.contains_key("x-stainless-runtime"));
    }

    #[test]
    fn test_path_stripping_with_duplicate_prefix() {

        let target_url = url::Url::parse("https://api.openai.com/v1/").unwrap();
        let target_path = target_url.path().trim_end_matches('/');
        let request_path = "v1/chat/completions";

        let path_to_join = if !target_path.is_empty() && target_path != "/" {
            let target_path_no_slash = &target_path[1..];
            if let Some(rest) = request_path.strip_prefix(target_path_no_slash) {
                if rest.is_empty() || rest.starts_with('/') {
                    rest.strip_prefix('/').unwrap_or(rest)
                } else {
                    request_path
                }
            } else {
                request_path
            }
        } else {
            request_path
        };

        let result = target_url.join(path_to_join).unwrap();
        assert_eq!(
            result.as_str(),
            "https://api.openai.com/v1/chat/completions"
        );
    }

    #[test]
    fn test_path_stripping_without_duplicate() {

        let target_url = url::Url::parse("https://api.openai.com/").unwrap();
        let target_path = target_url.path().trim_end_matches('/');
        let request_path = "v1/chat/completions";

        let path_to_join = if !target_path.is_empty() && target_path != "/" {
            let target_path_no_slash = &target_path[1..];
            if let Some(rest) = request_path.strip_prefix(target_path_no_slash) {
                if rest.is_empty() || rest.starts_with('/') {
                    rest.strip_prefix('/').unwrap_or(rest)
                } else {
                    request_path
                }
            } else {
                request_path
            }
        } else {
            request_path
        };

        let result = target_url.join(path_to_join).unwrap();
        assert_eq!(
            result.as_str(),
            "https://api.openai.com/v1/chat/completions"
        );
    }

    #[test]
    fn test_path_stripping_with_actual_duplicate_paths() {

        let target_url = url::Url::parse("https://api.example.com/").unwrap();
        let target_path = target_url.path().trim_end_matches('/');
        let request_path = "v1/v1/something";

        let path_to_join = if !target_path.is_empty() && target_path != "/" {
            let target_path_no_slash = &target_path[1..];
            if let Some(rest) = request_path.strip_prefix(target_path_no_slash) {
                if rest.is_empty() || rest.starts_with('/') {
                    rest.strip_prefix('/').unwrap_or(rest)
                } else {
                    request_path
                }
            } else {
                request_path
            }
        } else {
            request_path
        };

        let result = target_url.join(path_to_join).unwrap();
        assert_eq!(result.as_str(), "https://api.example.com/v1/v1/something");
    }

    #[test]
    fn test_path_stripping_with_different_prefix() {

        let target_url = url::Url::parse("https://api.example.com/v2/").unwrap();
        let target_path = target_url.path().trim_end_matches('/');
        let request_path = "v1/chat/completions";

        let path_to_join = if !target_path.is_empty() && target_path != "/" {
            let target_path_no_slash = &target_path[1..];
            if let Some(rest) = request_path.strip_prefix(target_path_no_slash) {
                if rest.is_empty() || rest.starts_with('/') {
                    rest.strip_prefix('/').unwrap_or(rest)
                } else {
                    request_path
                }
            } else {
                request_path
            }
        } else {
            request_path
        };

        let result = target_url.join(path_to_join).unwrap();
        assert_eq!(
            result.as_str(),
            "https://api.example.com/v2/v1/chat/completions"
        );
    }

    #[test]
    fn test_path_stripping_with_query_params() {

        let target_url = url::Url::parse("https://api.openai.com/v1/").unwrap();
        let target_path = target_url.path().trim_end_matches('/');
        let request_path = "v1/chat/completions?stream=true";

        let path_to_join = if !target_path.is_empty() && target_path != "/" {
            let target_path_no_slash = &target_path[1..];
            if let Some(rest) = request_path.strip_prefix(target_path_no_slash) {
                if rest.is_empty() || rest.starts_with('/') {
                    rest.strip_prefix('/').unwrap_or(rest)
                } else {
                    request_path
                }
            } else {
                request_path
            }
        } else {
            request_path
        };

        let result = target_url.join(path_to_join).unwrap();
        assert_eq!(
            result.as_str(),
            "https://api.openai.com/v1/chat/completions?stream=true"
        );
    }

    #[test]
    fn test_path_stripping_false_positive() {

        let target_url = url::Url::parse("https://api.example.com/v1/").unwrap();
        let target_path = target_url.path().trim_end_matches('/');
        let request_path = "v1x/something";

        let path_to_join = if !target_path.is_empty() && target_path != "/" {
            let target_path_no_slash = &target_path[1..];
            if let Some(rest) = request_path.strip_prefix(target_path_no_slash) {
                if rest.is_empty() || rest.starts_with('/') {
                    rest.strip_prefix('/').unwrap_or(rest)
                } else {
                    request_path
                }
            } else {
                request_path
            }
        } else {
            request_path
        };

        let result = target_url.join(path_to_join).unwrap();
        assert_eq!(result.as_str(), "https://api.example.com/v1/v1x/something");
    }


    #[test]
    fn test_timeout_config_can_be_set() {
        use crate::target::Target;

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .request_timeout_secs(30)
            .build();

        assert_eq!(target.request_timeout_secs, Some(30));
    }

    #[test]
    fn test_timeout_defaults_to_none() {
        use crate::target::Target;

        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .build();

        assert_eq!(target.request_timeout_secs, None);
    }

    #[test]
    fn test_gateway_timeout_error_response() {
        let error = OnwardsErrorResponse::gateway_timeout();
        let response = error.into_response();

        assert_eq!(response.status().as_u16(), 504);
    }

    #[tokio::test]
    async fn test_gateway_timeout_error_body() {
        use http_body_util::BodyExt;

        let error = OnwardsErrorResponse::gateway_timeout();
        let response = error.into_response();

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

        assert!(body_str.contains("message"));
        assert!(body_str.contains("code"));
        assert!(body_str.contains("gateway_timeout"));
        assert!(body_str.contains("took too long"));
    }

    #[derive(Debug, Clone)]
    struct DelayedMockClient {
        delay: std::time::Duration,
        response_status: u16,
    }

    impl DelayedMockClient {
        fn new(delay: std::time::Duration, response_status: u16) -> Self {
            Self {
                delay,
                response_status,
            }
        }
    }

    #[async_trait::async_trait]
    impl HttpClient for DelayedMockClient {
        async fn request(
            &self,
            _req: axum::extract::Request,
        ) -> Result<axum::response::Response, Box<dyn std::error::Error + Send + Sync>> {
            tokio::time::sleep(self.delay).await;
            Ok(axum::response::Response::builder()
                .status(self.response_status)
                .body(axum::body::Body::empty())
                .unwrap())
        }
    }

    #[tokio::test]
    async fn test_mock_client_timeout_fires() {
        use crate::target::Target;

        let target = Target::builder()
            .url("https://api.example.com/".parse().unwrap())
            .request_timeout_secs(1)
            .build();

        let pool = target.into_pool();

        let mock_client = DelayedMockClient::new(std::time::Duration::from_secs(2), 200);

        let state = AppState {
            targets: crate::target::Targets {
                targets: std::sync::Arc::new(dashmap::DashMap::new()),
                key_rate_limiters: std::sync::Arc::new(dashmap::DashMap::new()),
                key_concurrency_limiters: std::sync::Arc::new(dashmap::DashMap::new()),
                key_labels: std::sync::Arc::new(dashmap::DashMap::new()),
                key_ids: std::sync::Arc::new(dashmap::DashMap::new()),
                accounts: std::sync::Arc::new(dashmap::DashMap::new()),
                strict_mode: false,
                http_pool_config: None,
            },
            http_client: mock_client,
            response_transform_fn: None,
            upstream_rate_limit_message: None,
            response_id_header: None,
            body_limit: crate::DEFAULT_BODY_LIMIT,
            sse_buffer_limit: crate::sse::DEFAULT_SSE_BUFFER_LIMIT,
            first_token_timeout: None,
            first_token_timeout_exempt_header: None,
            rejected_params: Vec::new(),
            model_allowed_params: std::collections::HashMap::new(),
            inflight_limiter: std::sync::Arc::new(crate::inflight::LocalInflightLimiter::default()),
            batch_inflight_enforce: false,
            batch_inflight_limiter: std::sync::Arc::new(
                crate::inflight::LocalInflightLimiter::default(),
            ),
        };

        let req = axum::extract::Request::builder()
            .uri("/v1/chat/completions")
            .method("POST")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(r#"{"model":"gpt-4","messages":[]}"#))
            .unwrap();

        let target = pool.default_pool().first_target().unwrap();
        let timeout_secs = target.request_timeout_secs.unwrap();
        let timeout_duration = std::time::Duration::from_secs(timeout_secs);

        let result = tokio::time::timeout(timeout_duration, state.http_client.request(req)).await;

        assert!(result.is_err(), "Expected timeout but request completed");
    }

    #[test]
    fn test_pool_with_fallback_enabled() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{FallbackConfig, LoadBalanceStrategy, Target};

        let target1 = Target::builder()
            .url("https://provider1.example.com/".parse().unwrap())
            .request_timeout_secs(1)
            .build();

        let target2 = Target::builder()
            .url("https://provider2.example.com/".parse().unwrap())
            .request_timeout_secs(1)
            .build();

        let providers = vec![Provider::new(target1, 1), Provider::new(target2, 1)];

        let fallback_config = Some(FallbackConfig {
            enabled: true,
            on_status: vec![],
            on_rate_limit: false,
            ..Default::default()
        });

        let pool = ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            fallback_config,
            LoadBalanceStrategy::Priority,
            false,
            Vec::new(),
        );

        assert!(pool.fallback_enabled(), "Fallback should be enabled");
        assert_eq!(pool.len(), 2, "Pool should have 2 providers");
    }

    fn target_with_trace_flags(
        trusted: Option<bool>,
        propagate_trace_context: Option<bool>,
    ) -> Target {
        Target {
            url: "https://api.example.com/".parse().unwrap(),
            keys: None,
            onwards_key: None,
            onwards_model: None,
            limiter: None,
            upstream_auth_header_name: None,
            upstream_auth_header_prefix: None,
            response_headers: None,
            sanitize_response: false,
            request_timeout_secs: None,
            trusted,
            propagate_trace_context,
            reasoning_translation: None,
            accepts_scheduling_priority: false,
            kind: Default::default(),
        }
    }

    #[test]
    fn resolve_trace_propagation_explicit_true_overrides_untrusted_pool() {
        let target = target_with_trace_flags(Some(false), Some(true));
        assert!(resolve_trace_propagation(&target, false));
    }

    #[test]
    fn resolve_trace_propagation_explicit_false_overrides_trusted_pool() {
        let target = target_with_trace_flags(Some(true), Some(false));
        assert!(!resolve_trace_propagation(&target, true));
    }

    #[test]
    fn resolve_trace_propagation_inherits_from_target_trusted_when_unset() {
        let target = target_with_trace_flags(Some(true), None);
        assert!(resolve_trace_propagation(&target, false));

        let target = target_with_trace_flags(Some(false), None);
        assert!(!resolve_trace_propagation(&target, true));
    }

    #[test]
    fn resolve_trace_propagation_inherits_from_pool_when_target_unset() {
        let target = target_with_trace_flags(None, None);
        assert!(resolve_trace_propagation(&target, true));
        assert!(!resolve_trace_propagation(&target, false));
    }

    #[test]
    fn withhold_trace_context_strips_inbound_trace_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
                .parse()
                .unwrap(),
        );
        headers.insert("tracestate", "vendor=value".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        withhold_trace_context(&mut headers);

        assert!(
            !headers.contains_key("traceparent"),
            "traceparent must be stripped"
        );
        assert!(
            !headers.contains_key("tracestate"),
            "tracestate must be stripped"
        );
        assert!(
            headers.contains_key("content-type"),
            "non-trace headers must be preserved"
        );
    }

    #[test]
    fn withhold_trace_context_is_noop_when_absent() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        withhold_trace_context(&mut headers);
        assert_eq!(headers.len(), 1);
    }
}
