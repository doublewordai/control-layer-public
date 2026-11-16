
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tracing::warn;

use futures::StreamExt;
use http_body_util::BodyExt;

use super::classifier::{Classifier, ClassifyOutcome, ClassifyRequest};
use super::index::{CacheResult, TierPolicy};
use super::inject::{
    UpstreamCachedTokens, UsageEdit, inject_into_response_nonstreaming, scan_edit_sse, scrub_response_nonstreaming, strip_cache_control,
};
use super::metrics as cache_metrics;
use super::parse::{ParseError, validate_markers};
use super::query::{self, Inject, InvalidBreakpointValue};
use super::sse::SseBufferedStream;
use super::stats::{CacheBilling, CacheReadSource};
use crate::inference::rejections;
use crate::inference::serving_name::ServingName;

const COMMIT_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct CacheLayerState {
    pub classifier: Classifier,
    pub deadline: Duration,
    pub body_limit: usize,
    pub sse_buffer_limit: usize,
}

impl CacheLayerState {
    pub fn new(classifier: Classifier, body_limit: usize, deadline: Duration) -> Self {
        Self {
            classifier,
            deadline,
            body_limit,
            sse_buffer_limit: onwards::sse::DEFAULT_SSE_BUFFER_LIMIT,
        }
    }

    pub fn with_sse_buffer_limit(mut self, limit: usize) -> Self {
        self.sse_buffer_limit = limit;
        self
    }
}

fn is_cacheable(req: &Request) -> bool {
    req.method() == Method::POST && req.uri().path().trim_end_matches('/').ends_with("/completions")
}

pub fn path_is_plain_completions(path: &str) -> bool {
    let path = path.split('?').next().unwrap_or(path).trim_end_matches('/');
    path.ends_with("/completions") && !path.ends_with("/chat/completions")
}

fn marker_rejection_response(e: &ParseError, policy: &TierPolicy) -> Response {
    let reason = match e {
        ParseError::DisabledTier(_) => Some("tier_disabled"),
        ParseError::InvalidTtl(_) => Some("invalid_ttl"),
        ParseError::UnsupportedType(_) => Some("unsupported_type"),
        ParseError::TooManyBreakpoints => Some("too_many_breakpoints"),
        ParseError::MalformedCacheControl => Some("malformed_cache_control"),
        ParseError::AutomaticTtlConflict => Some("automatic_ttl_conflict"),
        ParseError::NoAutomaticSlot => Some("automatic_no_slot"),
        ParseError::Json(_) => None,
    };
    if let Some(r) = reason {
        cache_metrics::record_markers_rejected(r);
    }
    let message = match e {
        ParseError::DisabledTier(_) => format!("{e}; available tiers: {}", policy.enabled_strs().join(", ")),
        _ => e.to_string(),
    };
    let body = serde_json::json!({
        "error": {
            "message": message,
            "type": "invalid_request_error",
            "code": "invalid_cache_control",
            "param": "cache_control",
        }
    });
    (StatusCode::BAD_REQUEST, axum::Json(body)).into_response()
}

fn query_rejection_response(e: &InvalidBreakpointValue) -> Response {
    cache_metrics::record_query_breakpoint("invalid");
    let body = serde_json::json!({
        "error": {
            "message": format!(
                "invalid {} value {:?}; supported values: \"lastUserMessage\"",
                query::CACHE_BREAKPOINT_PARAM, e.0
            ),
            "type": "invalid_request_error",
            "code": "invalid_cache_breakpoint",
            "param": query::CACHE_BREAKPOINT_PARAM,
        }
    });
    (StatusCode::BAD_REQUEST, axum::Json(body)).into_response()
}

pub async fn cache_middleware(State(state): State<CacheLayerState>, request: Request, next: Next) -> Response {
    if !is_cacheable(&request) {
        return next.run(request).await;
    }

    let (mut parts, body) = request.into_parts();
    let body_bytes = match axum::body::to_bytes(body, state.body_limit).await {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "Failed to read request body in cache middleware");
            cache_metrics::record_body_read_failed();
            let body = serde_json::json!({
                "error": {
                    "message": format!("failed to read request body: {e}"),
                    "type": "invalid_request_error",
                    "code": "body_read_failed",
                }
            });
            let response = (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
            return rejections::refuse(&parts, response, "body_read_failed", None, None);
        }
    };

    let mut parsed_body = serde_json::from_slice::<serde_json::Value>(&body_bytes).ok();

    let mut body_bytes = body_bytes;
    let chat_route = !path_is_plain_completions(parts.uri.path());
    if !chat_route {
        if query::breakpoint_marker(parts.uri.query()).is_ok_and(|m| m.is_some()) || query::breakpoint_marker(parts.uri.query()).is_err() {
            parts.uri = query::strip_param(&parts.uri);
            cache_metrics::record_query_breakpoint("non_chat_ignored");
        }
    } else {
        match query::breakpoint_marker(parts.uri.query()) {
            Ok(None) => {}
            Ok(Some(marker)) => {
                parts.uri = query::strip_param(&parts.uri);
                let outcome = match parsed_body.as_mut() {
                    Some(body) => match query::inject_marker(body, marker) {
                        Inject::Applied => match serde_json::to_vec(body) {
                            Ok(b) => {
                                body_bytes = b.into();
                                "applied"
                            }
                            Err(_) => "reserialize_failed",
                        },
                        Inject::BodyFieldWins => "body_field_wins",
                        Inject::NotAnObject => "not_an_object",
                    },
                    None => "not_json",
                };
                cache_metrics::record_query_breakpoint(outcome);
            }
            Err(e) => {
                let model = parsed_body.as_ref().and_then(|body| body["model"].as_str());
                let response = query_rejection_response(&e);
                return rejections::refuse(
                    &parts,
                    response,
                    "invalid_cache_breakpoint",
                    Some(query::CACHE_BREAKPOINT_PARAM),
                    model,
                );
            }
        }
    }

    if let Some(body) = &parsed_body
        && let Err(e) = validate_markers(body, state.classifier.tier_policy(), state.classifier.telemetry_policy())
    {
        let response = marker_rejection_response(&e, state.classifier.tier_policy());
        return rejections::refuse(
            &parts,
            response,
            "invalid_cache_control",
            Some("cache_control"),
            body["model"].as_str(),
        );
    }

    let virtual_model = parsed_body
        .as_ref()
        .and_then(|v| v.get("model").and_then(|m| m.as_str()).map(String::from));
    let bearer = parts
        .headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
        .map(str::trim);
    let api_key = crate::auth::internal_credential::presented_key_hash(&parts.extensions, bearer);

    let model_label = virtual_model.clone().unwrap_or_default();

    let classify_handle = virtual_model.map(|requested| {
        let model = ServingName::of(&parts.extensions, &requested).model;
        let classifier = state.classifier.clone();
        let body = body_bytes.to_vec();
        tokio::spawn(async move {
            classifier
                .classify(ClassifyRequest {
                    virtual_model: &requested,
                    model: &model,
                    body: &body,
                    api_key,
                    principal: None,
                    route_has_blocks: chat_route,
                })
                .await
        })
    });

    let (stripped, had_markers) = strip_cache_control(&body_bytes, state.classifier.telemetry_policy());
    cache_metrics::record_marker_request(had_markers);
    let read_source = match (had_markers, chat_route) {
        (false, _) => Some(CacheReadSource::Engine),
        (true, true) => Some(CacheReadSource::Module),
        (true, false) => None,
    };
    if let Some(source) = read_source {
        parts.extensions.insert(source);
    }
    let forward = stripped.unwrap_or(body_bytes);
    parts.headers.remove(header::TRANSFER_ENCODING);
    parts
        .headers
        .insert(header::CONTENT_LENGTH, axum::http::HeaderValue::from(forward.len() as u64));
    let mut response = next.run(Request::from_parts(parts, Body::from(forward))).await;
    let upstream_cached = UpstreamCachedTokens::default();
    response.extensions_mut().insert(upstream_cached.clone());
    let cache_billing = CacheBilling::default();
    response.extensions_mut().insert(cache_billing.clone());

    let Some(mut handle) = classify_handle else {
        cache_metrics::record_request_outcome("inactive");
        return response;
    };

    if is_streaming(&response) {
        return defer_classify_into_stream(
            response,
            handle,
            state.deadline,
            model_label,
            state.classifier.clone(),
            upstream_cached,
            cache_billing,
            had_markers,
            state.sse_buffer_limit,
        );
    }

    let outcome = join_classify(&mut handle, state.deadline, &model_label).await;
    if !outcome.active {
        return scrub_response_nonstreaming(response, &upstream_cached).await;
    }
    let allow_implicit = !had_markers;
    let (response, billing_ok) = inject_into_response_nonstreaming(response, &outcome.stats, &upstream_cached, allow_implicit).await;
    if !outcome.pending.is_empty() {
        if billing_ok {
            spawn_commit(state.classifier.clone(), outcome.pending);
        } else {
            let reason = if response.status().is_success() { "no_usage" } else { "non_2xx" };
            cache_metrics::record_commit_vetoed(reason);
        }
    }
    response
}

fn is_streaming(response: &Response) -> bool {
    response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|ct| ct.trim().eq_ignore_ascii_case("text/event-stream"))
}

async fn join_classify(
    handle: &mut tokio::task::JoinHandle<CacheResult<ClassifyOutcome>>,
    deadline: Duration,
    model_label: &str,
) -> ClassifyOutcome {
    let outcome = match tokio::time::timeout(deadline, &mut *handle).await {
        Ok(Ok(Ok(result))) => {
            cache_metrics::record_classify("ok");
            result
        }
        Ok(Ok(Err(e))) => {
            cache_metrics::record_classify("error");
            warn!(error = %e, "cache classify failed — billing un-cached");
            ClassifyOutcome::inactive()
        }
        Ok(Err(e)) => {
            cache_metrics::record_classify(if e.is_panic() { "panicked" } else { "error" });
            warn!(error = %e, "cache classify task failed");
            ClassifyOutcome::inactive()
        }
        Err(_) => {
            cache_metrics::record_classify("deadline_exceeded");
            handle.abort();
            ClassifyOutcome::inactive()
        }
    };

    cache_metrics::record_request_outcome(outcome_label(&outcome));
    if outcome.active && !model_label.is_empty() {
        cache_metrics::record_token_volumes(
            model_label,
            outcome.stats.read,
            outcome.stats.creation_5m,
            outcome.stats.creation_1h,
            outcome.stats.creation_24h,
        );
    }
    outcome
}

fn outcome_label(outcome: &ClassifyOutcome) -> &'static str {
    if !outcome.active {
        "inactive"
    } else if outcome.stats.read > 0 && outcome.stats.creation_total() > 0 {
        "read_and_create"
    } else if outcome.stats.read > 0 {
        "read"
    } else if outcome.stats.creation_total() > 0 {
        "create_only"
    } else {
        "zero_active"
    }
}

struct AbortOnDrop<T>(Option<tokio::task::JoinHandle<T>>);

impl<T> AbortOnDrop<T> {
    fn take(&mut self) -> Option<tokio::task::JoinHandle<T>> {
        self.0.take()
    }

    fn as_mut(&mut self) -> Option<&mut tokio::task::JoinHandle<T>> {
        self.0.as_mut()
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(h) = self.0.take() {
            h.abort();
            cache_metrics::record_classify("abandoned");
            cache_metrics::record_request_outcome("aborted");
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn defer_classify_into_stream(
    response: Response,
    handle: tokio::task::JoinHandle<CacheResult<ClassifyOutcome>>,
    deadline: Duration,
    model_label: String,
    classifier: Classifier,
    upstream_cached: UpstreamCachedTokens,
    cache_billing: CacheBilling,
    had_markers: bool,
    sse_buffer_limit: usize,
) -> Response {
    let (parts, body) = response.into_parts();
    let status_ok = parts.status.is_success();
    let body_stream = BodyExt::into_data_stream(body).map(|r| r.map_err(std::io::Error::other));
    let buffered = SseBufferedStream::with_limit(body_stream, sse_buffer_limit);

    let stream = async_stream::stream! {
        futures::pin_mut!(buffered);
        let mut handle = AbortOnDrop(Some(handle));
        let mut outcome: Option<ClassifyOutcome> = None;
        let mut edited = false;
        let mut saw_error = false;
        let mut saw_usage = false;

        while let Some(item) = buffered.next().await {
            let chunk = match item {
                Ok(c) => c,
                Err(e) => {
                    saw_error = true;
                    yield Err(e);
                    continue;
                }
            };
            let probe = scan_edit_sse(&chunk, UsageEdit::Probe);
            if let Some(v) = probe.upstream_cached_tokens {
                upstream_cached.set(v);
            }
            saw_error |= probe.saw_error;
            if probe.saw_usage && outcome.is_none() {
                if let Some(h) = handle.as_mut() {
                    outcome = Some(join_classify(h, deadline, &model_label).await);
                }
                handle.take();
            }
            saw_usage |= probe.saw_usage;
            let out = if !edited && probe.saw_usage {
                let scan = match outcome.as_ref() {
                    Some(o) if o.active => scan_edit_sse(
                        &chunk,
                        UsageEdit::Inject {
                            stats: &o.stats,
                            allow_implicit: !had_markers,
                        },
                    ),
                    _ => scan_edit_sse(&chunk, UsageEdit::Scrub),
                };
                if let Some(billed) = scan.billing_stats {
                    cache_billing.set(billed);
                }
                edited |= scan.rewritten.is_some();
                scan.rewritten.unwrap_or(chunk)
            } else {
                chunk
            };
            yield Ok(out);
        }

        let outcome = match outcome {
            Some(o) => o,
            None => {
                if let Some(h) = handle.as_mut() {
                    let o = join_classify(h, deadline, &model_label).await;
                    handle.take();
                    o
                } else {
                    ClassifyOutcome::inactive()
                }
            }
        };
        if outcome.active && !outcome.pending.is_empty() {
            if status_ok && !saw_error && saw_usage {
                spawn_commit(classifier, outcome.pending);
            } else {
                let reason = if !status_ok {
                    "non_2xx"
                } else if saw_error {
                    "error_frame"
                } else {
                    "no_usage"
                };
                cache_metrics::record_commit_vetoed(reason);
            }
        }
    };

    let mut response = Response::from_parts(parts, Body::from_stream(stream));
    response.headers_mut().remove(header::CONTENT_LENGTH);
    response
}

async fn commit_with_deadline(classifier: &Classifier, pending: &super::stats::PendingWrite) {
    let start = std::time::Instant::now();
    let result = tokio::time::timeout(COMMIT_DEADLINE, classifier.commit(pending)).await;
    cache_metrics::record_commit_duration(start.elapsed().as_secs_f64());
    match result {
        Ok(Ok(())) => cache_metrics::record_commit("ok"),
        Ok(Err(e)) => {
            cache_metrics::record_commit("error");
            warn!(error = %e, "cache index commit failed");
        }
        Err(_) => {
            cache_metrics::record_commit("timeout");
            warn!("cache index commit timed out");
        }
    }
}

fn spawn_commit(classifier: Classifier, pending: super::stats::PendingWrite) {
    tokio::spawn(async move {
        commit_with_deadline(&classifier, &pending).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::models::users::Role;
    use crate::inference::translation::{TranslationRegistry, middleware::translation_middleware, responses::OpenResponses};
    use crate::metrics::errors::component::ANALYTICS_BATCHER;
    use crate::pricing::{CacheMultipliers, TokenCounts, charged_cost};
    use crate::prompt_cache::{
        CacheIndex, IndexScope, ModelConfigResolver, PostgresIndex, PrincipalResolver, TelemetryPolicy, TokenizerClient,
        parse_chat_completions,
    };
    use crate::request_logging::serializers::{extract_cache_tokens, extract_from_last_usage, raw_usage_tokens};
    use crate::test::utils::{create_test_api_key_for_user, create_test_endpoint, create_test_model, create_test_user};
    use axum::http::Extensions;
    use axum::middleware::from_fn_with_state;
    use axum::routing::post;
    use axum::{Json, Router};
    use outlet::ResponseData;
    use rust_decimal::Decimal;
    use sqlx::PgPool;
    use std::sync::{Arc, Mutex};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ALIAS: &str = "layer-model";
    const TOK_VER: &str = "sha256:lv1";

    fn all_tiers() -> TierPolicy {
        TierPolicy::from_config(&["5m".to_string(), "1h".to_string(), "24h".to_string()], "5m")
    }

    #[dwctl_test_macros::test]
    async fn large_fragmented_tool_call_uses_configured_sse_limit(pool: PgPool) {
        use tower::ServiceExt;

        let arguments = "x".repeat(512 * 1024);
        let event = serde_json::json!({"choices": [{"delta": {"tool_calls": [{
            "index": 0, "id": "call_large", "type": "function",
            "function": {"name": "write_file", "arguments": arguments}
        }]}, "finish_reason": "tool_calls"}]});
        let wire = format!(
            "data: {event}\n\ndata: {{\"choices\":[],\"usage\":{{\"prompt_tokens\":10,\"completion_tokens\":20,\"total_tokens\":30,\"prompt_tokens_details\":{{\"cached_tokens\":8}}}}}}\n\ndata: [DONE]\n\n"
        );
        for limit in [64 * 1024, 1024 * 1024] {
            let classifier = Classifier::new(
                PrincipalResolver::new(pool.clone()),
                ModelConfigResolver::new(pool.clone()),
                TokenizerClient::new("http://127.0.0.1:1"),
                Arc::new(PostgresIndex::new(pool.clone(), 1)),
                all_tiers(),
                TelemetryPolicy::default(),
                false,
            );
            let payload = wire.clone();
            let app = Router::new()
                .route(
                    "/v1/chat/completions",
                    post(move || {
                        let payload = payload.clone();
                        async move {
                            let chunks: Vec<_> = payload
                                .as_bytes()
                                .chunks(8192)
                                .map(|b| Ok::<_, std::io::Error>(bytes::Bytes::copy_from_slice(b)))
                                .collect();
                            (
                                [(header::CONTENT_TYPE, "text/event-stream")],
                                Body::from_stream(futures::stream::iter(chunks)),
                            )
                                .into_response()
                        }
                    }),
                )
                .layer(from_fn_with_state(
                    CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)).with_sse_buffer_limit(limit),
                    cache_middleware,
                ));
            let request = Request::builder()
                .method(Method::POST)
                .uri("/v1/chat/completions")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"model": ALIAS, "messages": [{"role": "user", "content": "write a file"}], "stream": true})
                        .to_string(),
                ))
                .unwrap();
            let response = app.oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let result = axum::body::to_bytes(response.into_body(), usize::MAX).await;
            if limit == 64 * 1024 {
                assert!(result.is_err(), "a fragmented event exceeding the configured limit must fail");
            } else {
                let data = String::from_utf8(result.unwrap().to_vec()).unwrap();
                let lines: Vec<_> = data.lines().filter_map(|line| line.strip_prefix("data: ")).collect();
                assert_eq!(lines.last(), Some(&"[DONE]"));
                let tool: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
                assert_eq!(tool, event, "large tool arguments and finish reason are preserved");
                let usage: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
                assert_eq!(usage["usage"]["total_tokens"], 30);
                assert_eq!(
                    usage["usage"]["prompt_tokens_details"]["cached_tokens"], 0,
                    "cache accounting still scrubs untariffed provider hits"
                );
            }
        }
    }

    async fn await_commit(idx: &PostgresIndex, scope: &IndexScope, hash: &crate::prompt_cache::PrefixHash, what: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if !idx.lookup(scope, std::slice::from_ref(hash)).await.unwrap().is_empty() {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "{what}: write did not commit within 5s");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    async fn mock_upstream() -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "id": "chatcmpl-1", "object": "chat.completion",
            "choices": [{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],
            "usage": {"prompt_tokens": 2000, "completion_tokens": 2, "total_tokens": 2002}
        }))
    }

    fn body() -> serde_json::Value {
        serde_json::json!({
            "model": ALIAS,
            "messages": [
                {"role":"system","content":[{"type":"text","text":"static system","cache_control":{"type":"ephemeral","ttl":"1h"}}]},
                {"role":"user","content":"hi"}
            ]
        })
    }

    #[dwctl_test_macros::test]
    async fn end_to_end_injects_then_reads(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;
        let id = create_test_model(&pool, "m", ALIAS, endpoint, user.id).await;
        sqlx::query!(
            r#"INSERT INTO model_cache_tariffs
                 (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, min_prefix_tokens)
               VALUES ($1, 1.25, 2.0, 2.5, 1024)"#,
            id
        )
        .execute(&pool)
        .await
        .unwrap();

        let tok = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [{"alias": ALIAS, "hf_repo": "o/m", "tokenizer_version": TOK_VER}]
            })))
            .mount(&tok)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/tokenize"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "virtual_model": ALIAS, "tokenizer_version": TOK_VER,
                "segment_counts": [1500], "cumulative": [1500], "total": 1500
            })))
            .mount(&tok)
            .await;

        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new(tok.uri()),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let app = Router::new()
            .route("/v1/chat/completions", post(mock_upstream))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ));
        let server = axum_test::TestServer::new(app).unwrap();

        let r1 = server
            .post("/v1/chat/completions")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&body())
            .await;
        r1.assert_status_ok();
        let v1: serde_json::Value = r1.json();
        assert_eq!(v1["usage"]["prompt_tokens"], 2000, "upstream total preserved");
        assert_eq!(v1["usage"]["cache_read_input_tokens"], 0);
        assert_eq!(v1["usage"]["cache_creation_input_tokens"], 1500);
        assert_eq!(v1["usage"]["prompt_tokens_details"]["cached_tokens"], 0);

        let scope = IndexScope {
            principal_id: user.id,
            virtual_model: ALIAS.into(),
            tokenizer_version: TOK_VER.into(),
        };
        let hash = parse_chat_completions(&serde_json::to_vec(&body()).unwrap(), &all_tiers(), &TelemetryPolicy::default())
            .unwrap()
            .cumulative_hashes[0]
            .clone();
        let idx = PostgresIndex::new(pool.clone(), 1);
        await_commit(&idx, &scope, &hash, "the write should have committed after a 2xx").await;

        let r2 = server
            .post("/v1/chat/completions")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&body())
            .await;
        let v2: serde_json::Value = r2.json();
        assert_eq!(
            v2["usage"]["cache_read_input_tokens"], 1500,
            "second request reads the cached prefix"
        );
        assert_eq!(v2["usage"]["cache_creation_input_tokens"], 0);
    }

    async fn mock_upstream_streaming() -> Response {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n\
                   data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2000,\"completion_tokens\":2,\"total_tokens\":2002}}\n\n\
                   data: [DONE]\n\n";
        Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from(sse))
            .unwrap()
    }

    fn body_streaming() -> serde_json::Value {
        serde_json::json!({
            "model": ALIAS,
            "stream": true,
            "messages": [
                {"role":"system","content":[{"type":"text","text":"static system","cache_control":{"type":"ephemeral","ttl":"1h"}}]},
                {"role":"user","content":"hi"}
            ]
        })
    }

    #[dwctl_test_macros::test]
    async fn streaming_defers_classify_then_injects_and_commits(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;
        let id = create_test_model(&pool, "m", ALIAS, endpoint, user.id).await;
        sqlx::query!(
            r#"INSERT INTO model_cache_tariffs
                 (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, min_prefix_tokens)
               VALUES ($1, 1.25, 2.0, 2.5, 1024)"#,
            id
        )
        .execute(&pool)
        .await
        .unwrap();

        let tok = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [{"alias": ALIAS, "hf_repo": "o/m", "tokenizer_version": TOK_VER}]
            })))
            .mount(&tok)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/tokenize"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "virtual_model": ALIAS, "tokenizer_version": TOK_VER,
                "segment_counts": [1500], "cumulative": [1500], "total": 1500
            })))
            .mount(&tok)
            .await;

        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new(tok.uri()),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let app = Router::new()
            .route("/v1/chat/completions", post(mock_upstream_streaming))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ));
        let server = axum_test::TestServer::new(app).unwrap();

        let r1 = server
            .post("/v1/chat/completions")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&body_streaming())
            .await;
        r1.assert_status_ok();
        let t1 = r1.text();
        assert!(t1.contains("\"cache_creation_input_tokens\":1500"), "creation injected: {t1}");
        assert!(t1.contains("\"cache_read_input_tokens\":0"), "no read on first sight: {t1}");
        assert!(t1.contains("data: [DONE]"), "DONE preserved: {t1}");
        assert!(t1.contains("\"content\":\"hi\""), "delta preserved: {t1}");

        let scope = IndexScope {
            principal_id: user.id,
            virtual_model: ALIAS.into(),
            tokenizer_version: TOK_VER.into(),
        };
        let hash = parse_chat_completions(
            &serde_json::to_vec(&body_streaming()).unwrap(),
            &all_tiers(),
            &TelemetryPolicy::default(),
        )
        .unwrap()
        .cumulative_hashes[0]
            .clone();
        let idx = PostgresIndex::new(pool.clone(), 1);
        await_commit(&idx, &scope, &hash, "streaming write commits after a clean usage frame").await;

        let r2 = server
            .post("/v1/chat/completions")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&body_streaming())
            .await;
        let t2 = r2.text();
        assert!(
            t2.contains("\"cache_read_input_tokens\":1500"),
            "second stream reads the prefix: {t2}"
        );
        assert!(t2.contains("\"cache_creation_input_tokens\":0"), "no creation on a read: {t2}");
    }

    async fn mock_upstream_streaming_error() -> Response {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n\
                   data: {\"error\":{\"message\":\"upstream exploded\"}}\n\n";
        Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from(sse))
            .unwrap()
    }

    #[dwctl_test_macros::test]
    async fn streaming_error_frame_vetoes_the_write(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;
        let id = create_test_model(&pool, "m", ALIAS, endpoint, user.id).await;
        sqlx::query!(
            r#"INSERT INTO model_cache_tariffs
                 (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, min_prefix_tokens)
               VALUES ($1, 1.25, 2.0, 2.5, 1024)"#,
            id
        )
        .execute(&pool)
        .await
        .unwrap();

        let tok = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [{"alias": ALIAS, "hf_repo": "o/m", "tokenizer_version": TOK_VER}]
            })))
            .mount(&tok)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/tokenize"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "virtual_model": ALIAS, "tokenizer_version": TOK_VER,
                "segment_counts": [1500], "cumulative": [1500], "total": 1500
            })))
            .mount(&tok)
            .await;

        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new(tok.uri()),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let app = Router::new()
            .route("/v1/chat/completions", post(mock_upstream_streaming_error))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ));
        let server = axum_test::TestServer::new(app).unwrap();

        let r = server
            .post("/v1/chat/completions")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&body_streaming())
            .await;
        let _ = r.text();

        let scope = IndexScope {
            principal_id: user.id,
            virtual_model: ALIAS.into(),
            tokenizer_version: TOK_VER.into(),
        };
        let hash = parse_chat_completions(
            &serde_json::to_vec(&body_streaming()).unwrap(),
            &all_tiers(),
            &TelemetryPolicy::default(),
        )
        .unwrap()
        .cumulative_hashes[0]
            .clone();
        let idx = PostgresIndex::new(pool.clone(), 1);
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert!(
            idx.lookup(&scope, std::slice::from_ref(&hash)).await.unwrap().is_empty(),
            "an unbilled stream (error frame, no usage) must not commit a write"
        );
    }

    #[dwctl_test_macros::test]
    async fn non_cacheable_path_passes_through(pool: PgPool) {
        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new("http://127.0.0.1:1"),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let app = Router::new().route("/v1/embeddings", post(mock_upstream)).layer(from_fn_with_state(
            CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
            cache_middleware,
        ));
        let server = axum_test::TestServer::new(app).unwrap();
        let r = server
            .post("/v1/embeddings")
            .json(&serde_json::json!({"model": "x", "input": "hi"}))
            .await;
        let v: serde_json::Value = r.json();
        assert!(v["usage"].get("cache_read_input_tokens").is_none());
    }

    #[dwctl_test_macros::test]
    async fn disabled_tier_marker_rejected_with_400(pool: PgPool) {
        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new("http://127.0.0.1:1"),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            TierPolicy::from_config(&["5m".to_string()], "5m"),
            TelemetryPolicy::default(),
            false,
        );
        let app = Router::new()
            .route("/v1/chat/completions", post(mock_upstream))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ));
        let server = axum_test::TestServer::new(app).unwrap();

        let r = server
            .post("/v1/chat/completions")
            .add_header("authorization", "Bearer anything")
            .json(&serde_json::json!({
                "model": ALIAS,
                "messages": [{"role": "system", "content": [
                    {"type": "text", "text": "x", "cache_control": {"type": "ephemeral", "ttl": "24h"}}
                ]}]
            }))
            .await;

        r.assert_status(StatusCode::BAD_REQUEST);
        let v: serde_json::Value = r.json();
        assert_eq!(v["error"]["code"], "invalid_cache_control");
        let msg = v["error"]["message"].as_str().unwrap();
        assert!(msg.contains("24h"), "message names the rejected tier: {msg}");
        assert!(msg.contains("available tiers: 5m"), "message names the available tiers: {msg}");
    }

    type Observed = Arc<std::sync::Mutex<Option<UpstreamCachedTokens>>>;
    async fn observe_upstream_cached(
        axum::extract::State(slot): axum::extract::State<Observed>,
        req: axum::extract::Request,
        next: axum::middleware::Next,
    ) -> Response {
        let response = next.run(req).await;
        *slot.lock().unwrap() = response.extensions().get::<UpstreamCachedTokens>().cloned();
        response
    }

    async fn mock_upstream_with_provider_cache() -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "id": "chatcmpl-1", "object": "chat.completion",
            "choices": [{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],
            "usage": {
                "prompt_tokens": 985, "completion_tokens": 2, "total_tokens": 987,
                "prompt_tokens_details": {"cached_tokens": 687},
                "cache_read_input_tokens": 687
            }
        }))
    }

    #[dwctl_test_macros::test]
    async fn inactive_model_scrubs_provider_cache_fields(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new("http://127.0.0.1:1"),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let observed: Observed = Default::default();
        let app = Router::new()
            .route("/v1/chat/completions", post(mock_upstream_with_provider_cache))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ))
            .layer(from_fn_with_state(observed.clone(), observe_upstream_cached));
        let server = axum_test::TestServer::new(app).unwrap();

        let r = server
            .post("/v1/chat/completions")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&serde_json::json!({"model": ALIAS, "messages": [{"role":"user","content":"hi"}]}))
            .await;
        r.assert_status_ok();
        let v: serde_json::Value = r.json();
        assert_eq!(v["usage"]["prompt_tokens"], 985, "token totals untouched");
        assert_eq!(v["usage"]["prompt_tokens_details"]["cached_tokens"], 0, "provider hit zeroed");
        assert!(
            v["usage"].get("cache_read_input_tokens").is_none(),
            "provider extension field removed: {v}"
        );
        let cell = observed.lock().unwrap().clone().expect("cell inserted at head time");
        assert_eq!(cell.get(), Some(687), "the engine's hit survives for analytics");
    }

    async fn mock_upstream_streaming_with_provider_cache() -> Response {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n\
                   data: {\"choices\":[],\"usage\":{\"prompt_tokens\":985,\"completion_tokens\":2,\"total_tokens\":987,\"prompt_tokens_details\":{\"cached_tokens\":687}}}\n\n\
                   data: [DONE]\n\n";
        Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from(sse))
            .unwrap()
    }

    #[dwctl_test_macros::test]
    async fn inactive_model_scrubs_streaming_terminal_frame(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new("http://127.0.0.1:1"),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let observed: Observed = Default::default();
        let app = Router::new()
            .route("/v1/chat/completions", post(mock_upstream_streaming_with_provider_cache))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ))
            .layer(from_fn_with_state(observed.clone(), observe_upstream_cached));
        let server = axum_test::TestServer::new(app).unwrap();

        let r = server
            .post("/v1/chat/completions")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&serde_json::json!({"model": ALIAS, "stream": true, "messages": [{"role":"user","content":"hi"}]}))
            .await;
        r.assert_status_ok();
        let t = r.text();
        assert!(t.contains("\"cached_tokens\":0"), "provider hit zeroed in terminal frame: {t}");
        assert!(!t.contains("687"), "provider value gone: {t}");
        assert!(t.contains("\"content\":\"hi\""), "delta preserved: {t}");
        assert!(t.contains("data: [DONE]"), "DONE preserved: {t}");
        let cell = observed.lock().unwrap().clone().expect("cell inserted at head time");
        assert_eq!(cell.get(), Some(687), "filled at the terminal frame, after the head was cloned");
    }


    async fn activate_alias(pool: &PgPool, user: uuid::Uuid) {
        let endpoint = create_test_endpoint(pool, "ep", user).await;
        let id = create_test_model(pool, "m", ALIAS, endpoint, user).await;
        sqlx::query!(
            r#"INSERT INTO model_cache_tariffs
                 (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, min_prefix_tokens)
               VALUES ($1, 1.25, 2.0, 2.5, 1024)"#,
            id
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[dwctl_test_macros::test]
    async fn tariffed_model_passes_engine_cache_through_for_unmarked_requests(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        activate_alias(&pool, user.id).await;
        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new("http://127.0.0.1:1"),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let app = Router::new()
            .route("/v1/chat/completions", post(mock_upstream_with_provider_cache))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ));
        let server = axum_test::TestServer::new(app).unwrap();

        let r = server
            .post("/v1/chat/completions")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&serde_json::json!({"model": ALIAS, "messages": [{"role":"user","content":"hi"}]}))
            .await;
        r.assert_status_ok();
        let v: serde_json::Value = r.json();
        assert_eq!(v["usage"]["prompt_tokens"], 985, "token totals untouched");
        assert_eq!(
            v["usage"]["prompt_tokens_details"]["cached_tokens"], 687,
            "engine hit passed through"
        );
        assert_eq!(v["usage"]["cache_read_input_tokens"], 687, "billed read = engine hit");
        assert_eq!(v["usage"]["cache_creation_input_tokens"], 0, "implicit caching never writes");
    }

    #[dwctl_test_macros::test]
    async fn tariffed_model_passes_engine_cache_through_on_streams(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        activate_alias(&pool, user.id).await;
        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new("http://127.0.0.1:1"),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let app = Router::new()
            .route("/v1/chat/completions", post(mock_upstream_streaming_with_provider_cache))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ));
        let server = axum_test::TestServer::new(app).unwrap();

        let r = server
            .post("/v1/chat/completions")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&serde_json::json!({"model": ALIAS, "stream": true, "messages": [{"role":"user","content":"hi"}]}))
            .await;
        r.assert_status_ok();
        let t = r.text();
        assert!(t.contains("\"cached_tokens\":687"), "engine hit kept in terminal frame: {t}");
        assert!(t.contains("\"cache_read_input_tokens\":687"), "billed read = engine hit: {t}");
        assert!(t.contains("\"cache_creation_input_tokens\":0"), "no writes on implicit: {t}");
        assert!(t.contains("\"content\":\"hi\""), "delta preserved: {t}");
        assert!(t.contains("data: [DONE]"), "DONE preserved: {t}");
    }


    async fn mock_upstream_completions_with_provider_cache() -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "id": "cmpl-1", "object": "text_completion",
            "model": "m",
            "choices": [{"index":0,"text":" pong","finish_reason":"stop"}],
            "usage": {
                "prompt_tokens": 2008, "completion_tokens": 8, "total_tokens": 2016,
                "prompt_tokens_details": {"cached_tokens": 1792}
            }
        }))
    }

    fn completions_app(pool: &PgPool) -> axum_test::TestServer {
        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new("http://127.0.0.1:1"),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let app = Router::new()
            .route("/v1/completions", post(mock_upstream_completions_with_provider_cache))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ));
        axum_test::TestServer::new(app).unwrap()
    }

    #[dwctl_test_macros::test]
    async fn tariffed_model_passes_engine_cache_through_on_plain_completions(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        activate_alias(&pool, user.id).await;
        let server = completions_app(&pool);

        let r = server
            .post("/v1/completions")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&serde_json::json!({"model": ALIAS, "prompt": "continue this"}))
            .await;
        r.assert_status_ok();
        let v: serde_json::Value = r.json();
        assert_eq!(v["usage"]["prompt_tokens"], 2008, "token totals untouched");
        assert_eq!(
            v["usage"]["prompt_tokens_details"]["cached_tokens"], 1792,
            "engine hit passed through"
        );
        assert_eq!(v["usage"]["cache_read_input_tokens"], 1792, "billed read = engine hit");
        assert_eq!(v["usage"]["cache_creation_input_tokens"], 0, "implicit never writes");
    }

    #[dwctl_test_macros::test]
    async fn marked_plain_completions_stay_deterministically_zero(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        activate_alias(&pool, user.id).await;
        let server = completions_app(&pool);

        let r = server
            .post("/v1/completions")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&serde_json::json!({
                "model": ALIAS, "prompt": "continue this",
                "cache_control": {"type": "ephemeral", "ttl": "1h"}
            }))
            .await;
        r.assert_status_ok();
        let v: serde_json::Value = r.json();
        assert_eq!(v["usage"]["prompt_tokens_details"]["cached_tokens"], 0, "armed → explicit zeros");
        assert_eq!(v["usage"]["cache_read_input_tokens"], 0, "engine hit not billed when armed");
    }

    #[dwctl_test_macros::test]
    async fn chat_shaped_fields_in_completions_bodies_cannot_touch_the_module_cache(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        activate_alias(&pool, user.id).await;
        let server = completions_app(&pool);

        let body = serde_json::json!({
            "model": ALIAS, "prompt": "continue this",
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "a long static prefix the completions engine never reads",
                 "cache_control": {"type": "ephemeral", "ttl": "1h"}}
            ]}]
        });
        for _ in 0..2 {
            let r = server
                .post("/v1/completions")
                .add_header("authorization", format!("Bearer {}", key.secret))
                .json(&body)
                .await;
            r.assert_status_ok();
            let v: serde_json::Value = r.json();
            assert_eq!(
                v["usage"]["cache_creation_input_tokens"], 0,
                "no module creation from smuggled messages"
            );
            assert_eq!(v["usage"]["cache_read_input_tokens"], 0, "no module or engine read billed (armed)");
            assert_eq!(
                v["usage"]["prompt_tokens_details"]["cached_tokens"], 0,
                "engine hit not shown when armed"
            );
        }
        let entries: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM prompt_cache_entries")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(entries, 0, "nothing committed to the module index");
    }

    #[dwctl_test_macros::test]
    async fn untariffed_plain_completions_scrub_provider_cache_fields(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        let server = completions_app(&pool);

        let r = server
            .post("/v1/completions")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&serde_json::json!({"model": ALIAS, "prompt": "continue this"}))
            .await;
        r.assert_status_ok();
        let v: serde_json::Value = r.json();
        assert_eq!(v["usage"]["prompt_tokens_details"]["cached_tokens"], 0, "provider hit zeroed");
        assert!(
            v["usage"].get("cache_read_input_tokens").is_none(),
            "no injected fields when inactive"
        );
    }

    #[test]
    fn is_cacheable_matches_the_onwards_completions_surface() {
        let req = |method: Method, path: &str| {
            let mut r = Request::new(Body::empty());
            *r.method_mut() = method;
            *r.uri_mut() = path.parse().unwrap();
            r
        };
        assert!(is_cacheable(&req(Method::POST, "/v1/chat/completions")));
        assert!(is_cacheable(&req(Method::POST, "/v1/completions")));
        assert!(is_cacheable(&req(Method::POST, "/v1/completions/")));
        assert!(is_cacheable(&req(Method::POST, "/v1/chat/completions/")));
        assert!(!is_cacheable(&req(Method::GET, "/v1/completions")));
        assert!(!is_cacheable(&req(Method::POST, "/v1/embeddings")));
    }

    #[test]
    fn plain_completions_rule_is_shared_with_replay() {
        assert!(path_is_plain_completions("/v1/completions"));
        assert!(path_is_plain_completions("/v1/completions/"));
        assert!(path_is_plain_completions("/v1/completions?cacheBreakpoint=lastUserMessage"));
        assert!(!path_is_plain_completions("/v1/chat/completions"));
        assert!(!path_is_plain_completions("/v1/chat/completions/"));
        assert!(!path_is_plain_completions("/v1/messages"));
        assert!(!path_is_plain_completions("/v1/responses"));
        assert!(!path_is_plain_completions("/v1/embeddings"));
    }

    async fn mock_upstream_completions_asserting_no_query(req: Request) -> Json<serde_json::Value> {
        assert!(req.uri().query().is_none(), "cacheBreakpoint must be stripped before forwarding");
        mock_upstream_completions_with_provider_cache().await
    }

    #[dwctl_test_macros::test]
    async fn cache_breakpoint_param_is_ignored_on_plain_completions(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        activate_alias(&pool, user.id).await;
        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new("http://127.0.0.1:1"),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let app = Router::new()
            .route("/v1/completions", post(mock_upstream_completions_asserting_no_query))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ));
        let server = axum_test::TestServer::new(app).unwrap();

        let r = server
            .post("/v1/completions")
            .add_query_param("cacheBreakpoint", "lastUserMessage")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&serde_json::json!({"model": ALIAS, "prompt": "continue this"}))
            .await;
        r.assert_status_ok();
        let v: serde_json::Value = r.json();
        assert_eq!(v["usage"]["cache_read_input_tokens"], 1792, "param must not suppress implicit");
        assert_eq!(v["usage"]["prompt_tokens_details"]["cached_tokens"], 1792);
    }


    fn body_unmarked() -> serde_json::Value {
        serde_json::json!({
            "model": ALIAS,
            "messages": [
                {"role": "system", "content": "static system"},
                {"role": "user", "content": "hi"}
            ]
        })
    }

    async fn mount_tokenizer_two_segments(tok: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [{"alias": ALIAS, "hf_repo": "o/m", "tokenizer_version": TOK_VER}]
            })))
            .mount(tok)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/tokenize"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "virtual_model": ALIAS, "tokenizer_version": TOK_VER,
                "segment_counts": [1500, 10], "cumulative": [1500, 1510], "total": 1510
            })))
            .mount(tok)
            .await;
    }

    #[dwctl_test_macros::test]
    async fn query_param_end_to_end_creates_then_reads(pool: PgPool) {
        query_param_cache_round_trip(pool, "/v1/chat/completions", false).await;
    }

    #[dwctl_test_macros::test]
    async fn responses_query_param_bills_cache_writes_and_reads(pool: PgPool) {
        query_param_cache_round_trip(pool, "/v1/responses", false).await;
    }

    #[dwctl_test_macros::test]
    async fn responses_streaming_query_param_bills_cache_writes_and_reads(pool: PgPool) {
        query_param_cache_round_trip(pool, "/v1/responses", true).await;
    }

    type ObservedExtensions = Arc<Mutex<Option<Extensions>>>;

    async fn observe_cache_billing(State(slot): State<ObservedExtensions>, req: Request, next: Next) -> Response {
        let response = next.run(req).await;
        let capture = response
            .extensions()
            .get::<CacheBilling>()
            .expect("billing metadata survives translation");
        if is_streaming(&response) {
            assert!(capture.get().is_none(), "streaming counts arrive after the response head");
        }
        *slot.lock().unwrap() = Some(response.extensions().clone());
        response
    }

    async fn query_param_cache_round_trip(pool: PgPool, route: &str, streaming: bool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;
        let id = create_test_model(&pool, "m", ALIAS, endpoint, user.id).await;
        sqlx::query!(
            r#"INSERT INTO model_cache_tariffs
                 (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, min_prefix_tokens)
               VALUES ($1, 1.25, 2.0, 2.5, 1024)"#,
            id
        )
        .execute(&pool)
        .await
        .unwrap();

        let tok = MockServer::start().await;
        mount_tokenizer_two_segments(&tok).await;

        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new(tok.uri()),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let observed: ObservedExtensions = Default::default();
        let app = Router::new()
            .route(route, post(mock_cache_billing_upstream))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ))
            .layer(from_fn_with_state(
                TranslationRegistry::new(vec![Arc::new(OpenResponses::new())]),
                translation_middleware,
            ))
            .layer(from_fn_with_state(observed.clone(), observe_cache_billing));
        let server = axum_test::TestServer::new(app).unwrap();
        let request_body = if route.ends_with("/responses") {
            serde_json::json!({"model": ALIAS, "instructions": "static system", "input": "hi", "stream": streaming})
        } else {
            body_unmarked()
        };

        let r1 = server
            .post(route)
            .add_query_param("cacheBreakpoint", "lastUserMessage")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&request_body)
            .await;
        r1.assert_status_ok();
        assert_cache_billing(&r1.text(), observed.lock().unwrap().clone().unwrap(), 0, 1510, Decimal::from(3516));

        let scope = IndexScope {
            principal_id: user.id,
            virtual_model: ALIAS.into(),
            tokenizer_version: TOK_VER.into(),
        };
        let hash = parse_chat_completions(
            &serde_json::to_vec(&body_unmarked()).unwrap(),
            &all_tiers(),
            &TelemetryPolicy::default(),
        )
        .unwrap()
        .cumulative_hashes[1]
            .clone();
        let idx = PostgresIndex::new(pool.clone(), 1);
        await_commit(&idx, &scope, &hash, "the query-param write should commit after a 2xx").await;

        let r2 = server
            .post(route)
            .add_query_param("cacheBreakpoint", "lastUserMessage")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&request_body)
            .await;
        r2.assert_status_ok();
        assert_cache_billing(&r2.text(), observed.lock().unwrap().clone().unwrap(), 1510, 0, Decimal::from(647));
    }

    async fn mock_cache_billing_upstream(req: Request) -> Response {
        assert_eq!(req.uri().path(), "/v1/chat/completions");
        assert!(req.uri().query().is_none(), "cache query parameter must not reach upstream");
        let body = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap();
        let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(request.get("cache_control").is_none());
        assert!(request.get("input").is_none());
        let usage = serde_json::json!({
            "prompt_tokens": 2000, "completion_tokens": 2, "total_tokens": 2002,
            "prompt_tokens_details": {"cached_tokens": 777}
        });
        if request["stream"] == true {
            let chunk = serde_json::json!({
                "id": "c1", "object": "chat.completion.chunk", "created": 0, "model": ALIAS,
                "choices": [{"index": 0, "delta": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}],
                "usage": usage
            });
            (
                [(header::CONTENT_TYPE, "text/event-stream")],
                format!("data: {chunk}\n\ndata: [DONE]\n\n"),
            )
                .into_response()
        } else {
            Json(serde_json::json!({
                "id": "c1", "object": "chat.completion", "created": 0, "model": ALIAS,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}],
                "usage": usage
            }))
            .into_response()
        }
    }

    fn assert_cache_billing(body: &str, extensions: Extensions, read: i64, creation: i64, expected_cost: Decimal) {
        assert_eq!(extensions.get::<UpstreamCachedTokens>().and_then(|c| c.get()), Some(777));
        let response = ResponseData {
            extensions,
            correlation_id: 1,
            timestamp: std::time::SystemTime::now(),
            status: StatusCode::OK,
            headers: Default::default(),
            body: Some(body.to_string().into()),
            duration: Duration::ZERO,
            duration_to_first_byte: Duration::ZERO,
        };
        let cache = extract_cache_tokens(&response);
        assert_eq!(
            (cache.read, cache.creation_5m, cache.creation_1h, cache.creation_24h),
            (read, 0, creation, 0)
        );
        let displayed = extract_from_last_usage(&response, |usage| {
            usage
                .pointer("/input_tokens_details/cached_tokens")
                .or_else(|| usage.pointer("/prompt_tokens_details/cached_tokens"))
                .and_then(serde_json::Value::as_i64)
        });
        assert_eq!(displayed, Some(read), "displayed reads must match billed reads");
        extract_from_last_usage(&response, |usage| {
            if let Some(details) = usage.get("input_tokens_details") {
                assert_eq!(details["cache_write_tokens"], creation);
                for field in ["cache_read_input_tokens", "cache_creation_input_tokens", "cache_creation"] {
                    assert!(usage.get(field).is_none(), "Responses must not expose {field}");
                }
            } else {
                assert_eq!(usage["cache_read_input_tokens"], read, "Chat Completions stays compatible");
                assert_eq!(usage["cache_creation"]["ephemeral_1h_input_tokens"], creation);
            }
        });
        let tokens = extract_from_last_usage(&response, raw_usage_tokens).expect("response usage");
        let counts = TokenCounts {
            prompt: tokens.prompt,
            completion: tokens.completion,
            cache_read: cache.read,
            cache_creation_5m: cache.creation_5m,
            cache_creation_1h: cache.creation_1h,
            cache_creation_24h: cache.creation_24h,
        };
        let cost = charged_cost(
            &counts,
            Some(ALIAS),
            Some(Decimal::ONE),
            Some(Decimal::from(3)),
            Some(CacheMultipliers::default()),
            ANALYTICS_BATCHER,
        );
        assert_eq!(cost, Some(expected_cost));
    }

    async fn mock_upstream_echoing(req: Request) -> Json<serde_json::Value> {
        let (req_parts, body) = req.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let body_has_cache_control = String::from_utf8_lossy(&bytes).contains("cache_control");
        Json(serde_json::json!({
            "id": "chatcmpl-1", "object": "chat.completion",
            "choices": [{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
            "echo": {"query": req_parts.uri.query(), "body_has_cache_control": body_has_cache_control}
        }))
    }

    #[dwctl_test_macros::test]
    async fn query_param_stripped_before_upstream_others_preserved(pool: PgPool) {
        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new("http://127.0.0.1:1"),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let app = Router::new()
            .route("/v1/chat/completions", post(mock_upstream_echoing))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ));
        let server = axum_test::TestServer::new(app).unwrap();

        let r = server
            .post("/v1/chat/completions")
            .add_query_param("foo", "bar")
            .add_query_param("cacheBreakpoint", "lastUserMessage")
            .json(&body_unmarked())
            .await;
        r.assert_status_ok();
        let v: serde_json::Value = r.json();
        assert_eq!(v["echo"]["query"], "foo=bar", "param stripped, others preserved");
        assert_eq!(v["echo"]["body_has_cache_control"], false, "injected marker stripped from the body");
    }

    #[dwctl_test_macros::test]
    async fn cache_refusals_are_counted_as_rejections(pool: PgPool) {
        use tower::ServiceExt;
        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new("http://127.0.0.1:1"),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            TierPolicy::from_config(&["5m".to_string()], "5m"),
            TelemetryPolicy::default(),
            false,
        );
        let app = Router::new()
            .route("/v1/chat/completions", post(mock_upstream))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ))
            .layer(rejections::test_support::layer(ALIAS));
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let metrics = recorder.handle();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        let marker = serde_json::json!({
            "model": ALIAS,
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "x", "cache_control": {"type": "ephemeral", "ttl": "24h"}}
            ]}]
        });
        for (uri, body, code) in [
            ("/v1/chat/completions", marker, "invalid_cache_control"),
            (
                "/v1/chat/completions?cacheBreakpoint=typo",
                body_unmarked(),
                "invalid_cache_breakpoint",
            ),
        ] {
            let request = axum::http::Request::post(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let rejection = response.extensions().get::<rejections::RejectionCode>().map(|r| r.code());
            assert_eq!(rejection, Some(code));
        }
        let rendered = metrics.render();
        for code in ["invalid_cache_control", "invalid_cache_breakpoint"] {
            assert!(
                rendered
                    .lines()
                    .any(|line| line.starts_with("onwards_rejections_total{") && line.contains(&format!("code=\"{code}\""))),
                "{code} not counted:\n{rendered}"
            );
        }
    }

    #[dwctl_test_macros::test]
    async fn query_param_invalid_value_rejected_400(pool: PgPool) {
        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new("http://127.0.0.1:1"),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let app = Router::new()
            .route("/v1/chat/completions", post(mock_upstream))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ));
        let server = axum_test::TestServer::new(app).unwrap();

        let r = server
            .post("/v1/chat/completions")
            .add_query_param("cacheBreakpoint", "lastusermessage")
            .json(&body_unmarked())
            .await;
        r.assert_status(StatusCode::BAD_REQUEST);
        let v: serde_json::Value = r.json();
        assert_eq!(v["error"]["code"], "invalid_cache_breakpoint");
        assert_eq!(v["error"]["param"], "cacheBreakpoint");
        let msg = v["error"]["message"].as_str().unwrap();
        assert!(msg.contains("lastusermessage"), "names the rejected value: {msg}");
        assert!(msg.contains("lastUserMessage"), "names the supported value: {msg}");
    }

    #[dwctl_test_macros::test]
    async fn query_param_composes_with_explicit_markers(pool: PgPool) {
        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new("http://127.0.0.1:1"),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let app = Router::new()
            .route("/v1/chat/completions", post(mock_upstream_echoing))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ));
        let server = axum_test::TestServer::new(app).unwrap();

        let conflicting = serde_json::json!({
            "model": ALIAS,
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "q", "cache_control": {"type": "ephemeral", "ttl": "5m"}}
            ]}]
        });
        let r = server
            .post("/v1/chat/completions")
            .add_query_param("cacheBreakpoint", "lastUserMessage")
            .json(&conflicting)
            .await;
        r.assert_status(StatusCode::BAD_REQUEST);
        let v: serde_json::Value = r.json();
        assert_eq!(v["error"]["code"], "invalid_cache_control");
        assert!(v["error"]["message"].as_str().unwrap().contains("conflicts"));

        let body_wins = serde_json::json!({
            "model": ALIAS,
            "cache_control": {"type": "ephemeral", "ttl": "5m"},
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "q", "cache_control": {"type": "ephemeral", "ttl": "5m"}}
            ]}]
        });
        let r = server
            .post("/v1/chat/completions")
            .add_query_param("cacheBreakpoint", "lastUserMessage")
            .json(&body_wins)
            .await;
        r.assert_status_ok();

        let composing = serde_json::json!({
            "model": ALIAS,
            "messages": [
                {"role": "system", "content": [
                    {"type": "text", "text": "sys", "cache_control": {"type": "ephemeral", "ttl": "1h"}}
                ]},
                {"role": "user", "content": "q"}
            ]
        });
        let r = server
            .post("/v1/chat/completions")
            .add_query_param("cacheBreakpoint", "lastUserMessage")
            .json(&composing)
            .await;
        r.assert_status_ok();
    }

    #[dwctl_test_macros::test]
    async fn query_param_streaming_injects_terminal_frame(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;
        let id = create_test_model(&pool, "m", ALIAS, endpoint, user.id).await;
        sqlx::query!(
            r#"INSERT INTO model_cache_tariffs
                 (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, min_prefix_tokens)
               VALUES ($1, 1.25, 2.0, 2.5, 1024)"#,
            id
        )
        .execute(&pool)
        .await
        .unwrap();

        let tok = MockServer::start().await;
        mount_tokenizer_two_segments(&tok).await;

        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new(tok.uri()),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            all_tiers(),
            TelemetryPolicy::default(),
            false,
        );
        let app = Router::new()
            .route("/v1/chat/completions", post(mock_upstream_streaming))
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ));
        let server = axum_test::TestServer::new(app).unwrap();

        let mut body = body_unmarked();
        body["stream"] = serde_json::json!(true);
        let r = server
            .post("/v1/chat/completions")
            .add_query_param("cacheBreakpoint", "lastUserMessage")
            .add_header("authorization", format!("Bearer {}", key.secret))
            .json(&body)
            .await;
        r.assert_status_ok();
        let t = r.text();
        assert!(t.contains("\"cache_creation_input_tokens\":1510"), "creation injected: {t}");
        assert!(t.contains("data: [DONE]"), "DONE preserved: {t}");
    }
}
