
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::StreamExt;
use http_body_util::BodyExt;
use onwards::strict::stream_end::interrupted_event;
use serde_json::Value;
use tracing::{debug, warn};

use crate::auth::internal_credential::presented_key_hash;
use crate::config::ContinuationConfig;
use crate::crypto::ApiKeyHash;
use crate::db::models::api_keys::ApiKeyPurpose;
use crate::inference::rejections;
use crate::inference::store::ONWARDS_RESPONSE_ID_HEADER;
use crate::prompt_cache::sse::SseBufferedStream;

use super::accumulate::{self, StreamAccumulator};
use super::detect::{self, DeathEvent, Verdict};
use super::metrics;
use super::render::{RenderClient, RenderPrefix, RenderedPrefix};
use super::resume::{self, LegError, LegStream};
use super::rewrap::{self, DONE_FRAME};
use super::{ContinuationRoutes, InflightGuard, InflightLimiter, PurposeResolver, RouteInfo};

#[derive(Clone)]
pub struct ContinuationState {
    pub cfg: Arc<ContinuationConfig>,
    pub key_hash: ApiKeyHash,
    pub tokenizer: RenderClient,
    pub resume_target: Router,
    pub routes: Arc<ContinuationRoutes>,
    pub purposes: PurposeResolver,
    pub inflight: Arc<InflightLimiter>,
    pub body_limit: usize,
    pub sse_buffer_limit: usize,
}

impl ContinuationState {
    pub async fn build(
        cfg: &ContinuationConfig,
        cache_tokenizer_url: &str,
        pools: sqlx_pool_router::DynPools,
        resume_target: Router,
        body_limit: usize,
        sse_buffer_limit: usize,
    ) -> anyhow::Result<Self> {
        let key_hash = super::provision_global_key(&pools.write()).await?;
        let tokenizer_url = cfg.tokenizer_url.clone().unwrap_or_else(|| cache_tokenizer_url.to_string());
        let routes = Arc::new(ContinuationRoutes::new());
        if let Err(e) = routes.refresh(&pools.write()).await {
            crate::background_error!(
                crate::metrics::errors::component::CONTINUATION,
                "route_seed",
                Warning,
                error = %e,
                "Initial continuation route load failed; the poller will retry"
            );
        }
        Arc::clone(&routes).spawn_poller(pools.clone());

        Ok(Self {
            cfg: Arc::new(cfg.clone()),
            key_hash,
            tokenizer: RenderClient::new(tokenizer_url, Duration::from_secs(cfg.resume_deadline_secs)),
            resume_target,
            routes,
            purposes: PurposeResolver::new(pools),
            inflight: Arc::new(InflightLimiter::new(cfg.max_inflight_per_model)),
            body_limit,
            sse_buffer_limit,
        })
    }

    fn stall_timeout(&self) -> Duration {
        Duration::from_secs(self.cfg.stall_timeout_secs)
    }
}

#[derive(Debug, Clone)]
pub struct RequestContext {
    pub model: String,
    pub body: Value,
    pub path: String,
    pub response_id: Option<String>,
    pub route: RouteInfo,
    pub origin: &'static str,
    pub prices: Option<onwards::serving::BillingPrices>,
}

impl RequestContext {
    pub fn messages(&self) -> &Value {
        self.body.get("messages").unwrap_or(&Value::Null)
    }

    pub fn tools(&self) -> Option<&Value> {
        self.body.get("tools").filter(|v| !v.is_null())
    }

    pub fn chat_template_kwargs(&self) -> Option<&Value> {
        self.body.get("chat_template_kwargs").filter(|v| !v.is_null())
    }

    pub fn render_kwargs(&self) -> Option<Value> {
        self.route.merged_render_kwargs(self.chat_template_kwargs())
    }

    pub fn max_tokens(&self) -> Option<u32> {
        self.body
            .get("max_tokens")
            .or_else(|| self.body.get("max_completion_tokens"))
            .and_then(Value::as_u64)
            .map(|v| u32::try_from(v).unwrap_or(u32::MAX))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    Realtime,
    Batch,
    Playground,
}

impl Origin {
    fn from_purpose(purpose: Option<ApiKeyPurpose>) -> Self {
        match purpose {
            Some(ApiKeyPurpose::Batch) => Origin::Batch,
            Some(ApiKeyPurpose::Playground) => Origin::Playground,
            _ => Origin::Realtime,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Origin::Realtime => "realtime",
            Origin::Batch => "batch",
            Origin::Playground => "playground",
        }
    }

    fn is_enabled(self, cfg: &ContinuationConfig) -> bool {
        match self {
            Origin::Realtime => cfg.origins.realtime,
            Origin::Batch => cfg.origins.batch,
            Origin::Playground => cfg.origins.playground,
        }
    }
}

fn is_structured_output(body: &Value, model: &str) -> bool {
    const KEYS: [&str; 6] = [
        "response_format",
        "json_schema",
        "guided_json",
        "guided_regex",
        "guided_grammar",
        "guided_choice",
    ];
    KEYS.iter().any(|k| body.get(*k).is_some_and(|v| !v.is_null())) || model.ends_with("-dottxt")
}

fn is_chat_completions(req: &Request) -> bool {
    req.method() == Method::POST && req.uri().path().ends_with("/chat/completions")
}

fn is_sse(response: &Response) -> bool {
    response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|ct| ct.trim().eq_ignore_ascii_case("text/event-stream"))
}

fn presented_key(req: &Request) -> Option<ApiKeyHash> {
    let bearer = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
        .map(str::trim);
    presented_key_hash(req.extensions(), bearer)
}

pub async fn continuation_middleware(State(state): State<ContinuationState>, request: Request, next: Next) -> Response {
    if !state.cfg.enabled {
        return next.run(request).await;
    }

    if !is_chat_completions(&request) {
        return next.run(request).await;
    }

    let path = request.uri().path().to_string();
    let response_id = request
        .headers()
        .get(ONWARDS_RESPONSE_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let token = presented_key(&request);

    let (parts, body) = request.into_parts();
    let body_bytes = match axum::body::to_bytes(body, state.body_limit).await {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "Failed to read request body in continuation middleware");
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

    let forward = |parts, bytes: Bytes| Request::from_parts(parts, Body::from(bytes));
    let parsed = serde_json::from_slice::<Value>(&body_bytes).ok();

    let fusillade_stream = parts
        .headers
        .get("x-fusillade-stream")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let Some(body) = parsed.filter(|b| fusillade_stream || b.get("stream").and_then(Value::as_bool) == Some(true)) else {
        return next.run(forward(parts, body_bytes)).await;
    };
    let Some(model) = body.get("model").and_then(Value::as_str).map(str::to_string) else {
        return next.run(forward(parts, body_bytes)).await;
    };

    let Some(route) = state.routes.get(&model) else {
        metrics::record_outcome("ineligible", "no_route", "unknown");
        return next.run(forward(parts, body_bytes)).await;
    };
    if is_structured_output(&body, &model) {
        metrics::record_outcome("ineligible", "structured_output", "unknown");
        return next.run(forward(parts, body_bytes)).await;
    }
    if body.get("logprobs").and_then(Value::as_bool) == Some(true) {
        metrics::record_outcome("ineligible", "logprobs", "unknown");
        return next.run(forward(parts, body_bytes)).await;
    }
    if body.get("n").and_then(Value::as_u64).is_some_and(|n| n > 1) {
        metrics::record_outcome("ineligible", "multi_choice", "unknown");
        return next.run(forward(parts, body_bytes)).await;
    }
    if body_bytes.len() > state.cfg.max_buffer_bytes {
        metrics::record_outcome("ineligible", "cap_exceeded", "unknown");
        return next.run(forward(parts, body_bytes)).await;
    }

    let mut ctx = RequestContext {
        model,
        body,
        path,
        response_id,
        route,
        origin: "unknown",
        prices: None,
    };

    let response = next.run(forward(parts, body_bytes)).await;

    if !response.status().is_success() {
        metrics::record_outcome("disarmed", "non_2xx", "unknown");
        return response;
    }
    if !is_sse(&response) {
        metrics::record_outcome("disarmed", "not_streaming", "unknown");
        return response;
    }

    let origin = Origin::from_purpose(match token.as_ref() {
        Some(t) => state.purposes.resolve(t).await,
        None => None,
    });
    if !origin.is_enabled(&state.cfg) {
        metrics::record_outcome("ineligible", "origin_disabled", origin.label());
        return response;
    }
    ctx.origin = origin.label();
    ctx.prices = response.extensions().get::<onwards::serving::BillingPrices>().copied();
    metrics::record_eligible_stream(&ctx.model, ctx.origin);

    tee(response, state, ctx)
}

struct OutcomeGuard {
    recorded: bool,
    origin: &'static str,
}

impl OutcomeGuard {
    fn new(origin: &'static str) -> Self {
        Self { recorded: false, origin }
    }

    fn record(&mut self, outcome: &'static str, reason: &'static str) {
        if !self.recorded {
            self.recorded = true;
            metrics::record_outcome(outcome, reason, self.origin);
        }
    }

    fn defuse(&mut self) {
        self.recorded = true;
    }
}

impl Drop for OutcomeGuard {
    fn drop(&mut self) {
        if !self.recorded {
            metrics::record_outcome("disarmed", "client_disconnect", self.origin);
        }
    }
}

fn frame_payload(event: &[u8]) -> Option<&str> {
    event.split(|b| *b == b'\n').find_map(|line| {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let rest = line.strip_prefix(b"data:")?;
        std::str::from_utf8(rest).ok().map(str::trim)
    })
}

fn surfaced_death(first_death: &mut Option<Bytes>) -> Bytes {
    first_death.take().unwrap_or_else(interrupted_event)
}

fn tee(response: Response, state: ContinuationState, ctx: RequestContext) -> Response {
    let (parts, body) = response.into_parts();
    let leg_one = BodyExt::into_data_stream(body).map(|r| r.map_err(std::io::Error::other));
    let stall = state.stall_timeout();

    let stream = async_stream::stream! {
        let mut outcome = OutcomeGuard::new(ctx.origin);
        let mut acc: Box<dyn StreamAccumulator> = accumulate::for_model(&ctx.model, &state.cfg, &ctx.route);
        let mut current: LegStream = Box::pin(SseBufferedStream::with_limit(leg_one, state.sse_buffer_limit));
        let mut resuming = false;
        let mut leg_provider: Option<&'static str> = None;
        let mut leg_deadline: Option<tokio::time::Instant> = None;
        let mut saw_usage = false;
        let mut done_bytes: Option<Bytes> = None;
        let mut first_death: Option<Bytes> = None;
        let mut first_death_reason: Option<&'static str> = None;
        let mut refusal: Option<Bytes> = None;
        let mut attempts = 0u32;
        let mut last_render: Option<RenderedPrefix> = None;
        let mut death_at: Option<Instant> = None;
        let mut last_frame_at = Instant::now();
        let mut max_frame_gap = Duration::ZERO;
        let mut _slot: Option<InflightGuard> = None;

        'chain: loop {
            let verdict = loop {
                let finished = acc.saw_finish_reason() || saw_usage;
                let next_item = if let Some(deadline) = leg_deadline {
                    match tokio::time::timeout_at(deadline, current.next()).await {
                        Err(_) => break detect::classify(&DeathEvent::Stall { finished }),
                        Ok(item) => item,
                    }
                } else if resuming || acc.len_bytes() > 0 {
                    match tokio::time::timeout(stall, current.next()).await {
                        Err(_) => break detect::classify(&DeathEvent::Stall { finished }),
                        Ok(item) => item,
                    }
                } else {
                    current.next().await
                };
                let item = match next_item {
                    None => break detect::classify(&DeathEvent::Eof {
                        saw_finish_reason: acc.saw_finish_reason() || saw_usage,
                        saw_done: false,
                    }),
                    Some(Err(_)) => break detect::classify(&DeathEvent::TransportError { finished }),
                    Some(Ok(bytes)) => bytes,
                };

                max_frame_gap = max_frame_gap.max(last_frame_at.elapsed());
                last_frame_at = Instant::now();

                let payload = frame_payload(&item);
                if payload == Some("[DONE]") {
                    done_bytes = Some(item);
                    break detect::classify(&DeathEvent::Eof {
                        saw_finish_reason: acc.saw_finish_reason() || saw_usage,
                        saw_done: true,
                    });
                }

                let Some(value) = payload.and_then(|p| serde_json::from_str::<Value>(p).ok()) else {
                    if !resuming {
                        if payload.is_some() && acc.disarmed().is_none() {
                            let cause = crate::continuation::accumulate::AccumulateError::UnparseableFrame;
                            acc.disarm_externally(cause);
                            outcome.record("disarmed", cause.reason());
                        }
                        yield Ok::<Bytes, std::io::Error>(item);
                    }
                    continue;
                };

                match detect::classify(&DeathEvent::Frame(&value)) {
                    Verdict::Alive => {}
                    other => {
                        if first_death.is_none() {
                            first_death = Some(item.clone());
                        }
                        refusal = Some(item);
                        break other;
                    }
                }

                if !resuming {
                    if acc.disarmed().is_none() && let Err(e) = acc.ingest(&value) {
                        outcome.record("disarmed", e.reason());
                    }
                    saw_usage |= rewrap::usage_of(&value).is_some();
                    yield Ok(item);
                    continue;
                }

                if leg_provider.is_none() {
                    leg_deadline = None;
                    let provider = if value.get("id").and_then(Value::as_str).is_some_and(|id| id.starts_with("dyn-")) {
                        "dynamo"
                    } else {
                        "external"
                    };
                    leg_provider = Some(provider);
                    metrics::record_leg_served(
                        &ctx.model,
                        provider,
                        last_render.as_ref().map(|r| r.total as u64).unwrap_or(0),
                    );
                }
                let Some(env) = acc.envelope().cloned() else {
                    continue;
                };
                if let Some((leg_prompt, leg_completion)) = rewrap::usage_of(&value) {
                    let render = last_render.as_ref();
                    let seg = render.and_then(|r| r.continuation_tokens).unwrap_or(0) as u64;
                    let total = render.map(|r| r.total).unwrap_or(0) as u64;
                    let (merged, anomaly) = rewrap::merge_usage(seg, leg_prompt, leg_completion, total);
                    if let Some(a) = anomaly {
                        metrics::record_usage_anomaly(a.kind());
                    }
                    saw_usage = true;
                    yield Ok(rewrap::sse_frame(&rewrap::usage_frame(&env, merged)));
                    continue;
                }
                if let Some(chat) = rewrap::reframe_chunk(&value, &env) {
                    if let Some(died) = death_at.take() {
                        metrics::record_seam(&ctx.model, leg_provider.unwrap_or("external"), died.elapsed().as_secs_f64());
                    }
                    let _ = acc.ingest(&chat);
                    yield Ok(rewrap::sse_frame(&chat));
                }
            };

            match verdict {
                Verdict::Alive => unreachable!("Alive never breaks the frame loop"),
                Verdict::Complete | Verdict::LostTrailer => {
                    if !saw_usage
                        && let Some(env) = acc.envelope().cloned()
                        && let Some(text) = acc.continuation_text()
                        && let Some(usage) = render_only_usage(&state, &ctx, &text).await
                    {
                        metrics::record_usage_anomaly("no_usage_frame");
                        yield Ok(rewrap::sse_frame(&rewrap::usage_frame(&env, usage)));
                    }
                    yield Ok(done_bytes.unwrap_or_else(|| Bytes::from_static(DONE_FRAME)));
                    if attempts > 0 {
                        outcome.record("resumed", first_death_reason.unwrap_or("ok"));
                    } else {
                        outcome.defuse();
                    }
                    break 'chain;
                }
                Verdict::NoResume(reason) => {
                    if let Some(frame) = refusal.take().or_else(|| first_death.take()) {
                        yield Ok(frame);
                    }
                    outcome.record("failed", reason);
                    if !resuming {
                        while let Some(Ok(bytes)) = current.next().await {
                            yield Ok(bytes);
                        }
                    }
                    break 'chain;
                }
                Verdict::Resume(reason) => {
                    if first_death_reason.is_none() {
                        first_death_reason = Some(reason);
                    }
                    if !acc.plain_resume_ok() {
                        yield Ok(surfaced_death(&mut first_death));
                        outcome.record("failed", "needs_forward_parser");
                        break 'chain;
                    }
                    let Some(text) = acc.continuation_text() else {
                        yield Ok(surfaced_death(&mut first_death));
                        outcome.record("failed", "not_reconstructable");
                        break 'chain;
                    };
                    debug!(
                        model = %ctx.model,
                        reason,
                        attempts,
                        response_id = ?ctx.response_id,
                        "Mid-stream death detected; attempting resume"
                    );
                    if death_at.is_none() {
                        death_at = Some(Instant::now());
                    }
                    if _slot.is_none() {
                        match state.inflight.try_acquire(&ctx.model) {
                            Some(slot) => _slot = Some(slot),
                            None => {
                                yield Ok(surfaced_death(&mut first_death));
                                outcome.record("failed", "throttled");
                                break 'chain;
                            }
                        }
                    }

                    let mut leg: Option<super::resume::Leg> = None;
                    let mut last_failure = "attempts_exhausted";
                    while attempts < state.cfg.max_attempts {
                        attempts += 1;
                        match resume::attempt(&state, &ctx, &text, attempts).await {
                            Ok(l) => {
                                leg = Some(l);
                                break;
                            }
                            Err(LegError::MaxTokensReached(render)) => {
                                if let Some(env) = acc.envelope().cloned() {
                                    yield Ok(rewrap::sse_frame(&rewrap::length_stop_frame(&env)));
                                    if !saw_usage {
                                        let (usage, _) = rewrap::merge_usage(
                                            render.continuation_tokens.unwrap_or(0) as u64,
                                            0,
                                            0,
                                            render.total as u64,
                                        );
                                        yield Ok(rewrap::sse_frame(&rewrap::usage_frame(&env, usage)));
                                    }
                                }
                                yield Ok(Bytes::from_static(DONE_FRAME));
                                outcome.record("failed", "max_tokens_reached");
                                break 'chain;
                            }
                            Err(e) => {
                                warn!(model = %ctx.model, attempt = attempts, error = %e, "Resume leg failed");
                                last_failure = e.reason();
                            }
                        }
                    }

                    match leg {
                        Some(l) => {
                            last_render = Some(l.render);
                            current = l.stream;
                            resuming = true;
                            leg_provider = None;
                            leg_deadline = Some(l.deadline);
                            refusal = None;
                        }
                        None => {
                            yield Ok(surfaced_death(&mut first_death));
                            outcome.record("failed", last_failure);
                            break 'chain;
                        }
                    }
                }
            }
        }

        metrics::record_max_frame_gap(&ctx.model, max_frame_gap.as_secs_f64());
    };

    let mut response = Response::from_parts(parts, Body::from_stream(stream));
    response.headers_mut().remove(header::CONTENT_LENGTH);
    response
}

async fn render_only_usage(state: &ContinuationState, ctx: &RequestContext, text: &str) -> Option<rewrap::MergedUsage> {
    let render_kwargs = ctx.render_kwargs();
    let prefix = RenderPrefix {
        virtual_model: &ctx.model,
        messages: ctx.messages(),
        tools: ctx.tools(),
        chat_template_kwargs: render_kwargs.as_ref(),
        continuation_text: text,
    };
    let render = state.tokenizer.render(&prefix).await.ok()?;
    let seg = render.continuation_tokens? as u64;
    let (usage, _) = rewrap::merge_usage(seg, 0, 0, render.total as u64);
    Some(usage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn frame_payload_reads_the_data_line_through_crlf_and_multiline_events() {
        assert_eq!(frame_payload(b"data: {\"a\":1}\n\n"), Some("{\"a\":1}"));
        assert_eq!(frame_payload(b"data: [DONE]\r\n\r\n"), Some("[DONE]"));
        assert_eq!(frame_payload(b"event: message\ndata: {\"a\":1}\n\n"), Some("{\"a\":1}"));
        assert_eq!(frame_payload(b": keep-alive\n\n"), None, "comments carry no payload");
        assert_eq!(frame_payload(b"data:{\"tight\":1}\n\n"), Some("{\"tight\":1}"));
    }

    #[test]
    fn structured_output_is_detected_in_every_documented_form() {
        for key in [
            "response_format",
            "json_schema",
            "guided_json",
            "guided_regex",
            "guided_grammar",
            "guided_choice",
        ] {
            let body = json!({ key: {"type": "json_object"} });
            assert!(is_structured_output(&body, "m"), "{key} must disqualify the request");
        }
        assert!(
            is_structured_output(&json!({}), "deepseek-v4-flash-dottxt"),
            "-dottxt aliases are structured by construction"
        );
        assert!(
            !is_structured_output(&json!({"response_format": null}), "m"),
            "a null field is not a constraint"
        );
        assert!(!is_structured_output(&json!({"temperature": 0.7}), "deepseek-v4-flash"));
    }

    #[test]
    fn origin_derives_from_the_key_purpose_and_gates_on_config() {
        assert_eq!(Origin::from_purpose(Some(ApiKeyPurpose::Batch)), Origin::Batch);
        assert_eq!(Origin::from_purpose(Some(ApiKeyPurpose::Playground)), Origin::Playground);
        assert_eq!(Origin::from_purpose(Some(ApiKeyPurpose::Realtime)), Origin::Realtime);
        assert_eq!(Origin::from_purpose(None), Origin::Realtime);

        let cfg = ContinuationConfig::default();
        assert!(Origin::Realtime.is_enabled(&cfg));
        assert!(!Origin::Batch.is_enabled(&cfg));
        assert!(!Origin::Playground.is_enabled(&cfg));
    }

    #[test]
    fn request_context_reads_the_resume_inputs() {
        let ctx = RequestContext {
            model: "m".to_string(),
            body: json!({
                "model": "m", "stream": true,
                "messages": [{"role": "user", "content": "hi"}],
                "tools": [{"type": "function"}],
                "chat_template_kwargs": {"thinking": true},
                "max_tokens": 500
            }),
            path: "/chat/completions".to_string(),
            response_id: None,
            route: RouteInfo::default(),
            origin: "realtime",
            prices: None,
        };
        assert_eq!(ctx.messages()[0]["role"], "user");
        assert!(ctx.tools().is_some());
        assert_eq!(ctx.chat_template_kwargs().unwrap()["thinking"], true);
        assert_eq!(ctx.max_tokens(), Some(500));

        let ctx = RequestContext {
            model: "m".to_string(),
            body: json!({"max_completion_tokens": 42}),
            path: "/chat/completions".to_string(),
            response_id: None,
            route: RouteInfo::default(),
            origin: "realtime",
            prices: None,
        };
        assert_eq!(ctx.max_tokens(), Some(42));
        assert!(ctx.tools().is_none());
    }
}
