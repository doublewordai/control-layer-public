
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{Extensions, header};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use serde_json::{Value, json};
use sqlx::PgPool;

use super::tests::{Fixture, fixture_with};
use super::*;
use crate::inference::response_store::{ResponseContext, ResponseStore, StoreError};
use crate::inference::stream_usage::ClientUsage;
use crate::inference::translation::anthropic::AnthropicMessages;
use crate::inference::translation::middleware::translation_middleware;
use crate::inference::translation::responses::OpenResponses;
use crate::inference::translation::responses::hydrate::hydrate_previous_response;
use crate::inference::translation::{ProtocolTranslator, TranslationRegistry};
use crate::prompt_cache::{
    CacheBilling, CacheLayerState, Classifier, ModelConfigResolver, PostgresIndex, PrincipalResolver, UpstreamCachedTokens,
    cache_middleware,
};
use crate::test::utils::{create_test_endpoint, create_test_model};

const MODEL: &str = "acme/chat-model";
const OTHER_MODEL: &str = "acme/other-model";
const SYSTEM: &str = "You are a careful assistant for a fictional hardware store.";
const USER_1: &str = "Which of the listed drills is the lightest?";
const ASSISTANT_1: &str = "The compact model, at 1.1 kg.";
const USER_2: &str = "And the cheapest?";

fn policies() -> (TierPolicy, TelemetryPolicy) {
    (
        TierPolicy::from_config(&["5m".into(), "1h".into(), "24h".into()], "5m"),
        TelemetryPolicy::from_config(false, &[]),
    )
}

fn hashes(chat_body: &[u8]) -> Vec<Vec<u8>> {
    let (tier, telemetry) = policies();
    boundaries(chat_body, &tier, &telemetry).unwrap().hashes
}


#[derive(Clone, Copy)]
struct Reply {
    prompt_tokens: u64,
    engine_cached: u64,
    shape: Shape,
}

#[derive(Clone, Copy, PartialEq)]
enum Shape {
    Final,
    Continuous { interim_engine: u64 },
    OnLastContent,
    LateBody { millis: u64 },
    FinalWithoutTotal { interim_engine: u64 },
    FinalMissingTwo { complete_engine: u64 },
    NoPromptCount,
}

#[derive(Clone)]
struct Upstream(Arc<Mutex<Reply>>, Arc<Mutex<Option<Vec<String>>>>);

impl Upstream {
    fn set(&self, prompt_tokens: u64, engine_cached: u64) {
        self.set_shaped(prompt_tokens, engine_cached, Shape::Final);
    }

    fn set_script(&self, chunks: Vec<String>) {
        *self.1.lock().unwrap() = Some(chunks);
    }

    fn set_shaped(&self, prompt_tokens: u64, engine_cached: u64, shape: Shape) {
        *self.0.lock().unwrap() = Reply {
            prompt_tokens,
            engine_cached,
            shape,
        };
    }
}

async fn upstream(State(up): State<Upstream>, req: Request) -> Response {
    assert_eq!(req.uri().path(), "/v1/chat/completions", "translated before it gets here");
    let body: Value = serde_json::from_slice(&axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap()).unwrap();
    if let Some(chunks) = up.1.lock().unwrap().take() {
        let chunks = chunks.into_iter().map(|c| Ok::<_, std::io::Error>(Bytes::from(c)));
        return (
            [(header::CONTENT_TYPE, "text/event-stream")],
            Body::from_stream(futures::stream::iter(chunks)),
        )
            .into_response();
    }
    let Reply {
        prompt_tokens,
        engine_cached,
        shape,
    } = *up.0.lock().unwrap();
    let usage = |engine: u64| {
        json!({
            "prompt_tokens": prompt_tokens, "completion_tokens": 2, "total_tokens": prompt_tokens + 2,
            "prompt_tokens_details": {"cached_tokens": engine}
        })
    };
    if body["stream"] == json!(true) {
        let mut content = json!({"id": "c1", "object": "chat.completion.chunk", "created": 1, "model": MODEL,
                                 "choices": [{"index": 0, "delta": {"role": "assistant", "content": "ok"}, "finish_reason": null}]});
        let mut stop = json!({"id": "c1", "object": "chat.completion.chunk", "created": 1, "model": MODEL,
                              "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]});
        let usage_only = json!({"id": "c1", "object": "chat.completion.chunk", "created": 1, "model": MODEL, "choices": [], "usage": usage(engine_cached)});
        let frames = match shape {
            Shape::Final | Shape::LateBody { .. } => vec![content, stop, usage_only],
            Shape::Continuous { interim_engine } => {
                content["usage"] = usage(interim_engine);
                stop["usage"] = usage(interim_engine);
                vec![content, stop, usage_only]
            }
            Shape::OnLastContent => {
                stop["usage"] = usage(engine_cached);
                vec![content, stop]
            }
            Shape::FinalWithoutTotal { interim_engine } => {
                content["usage"] = usage(interim_engine);
                let mut last = usage_only;
                last["usage"].as_object_mut().unwrap().remove("total_tokens");
                vec![content, stop, last]
            }
            Shape::FinalMissingTwo { complete_engine } => {
                content["usage"] = usage(complete_engine);
                let mut last = usage_only;
                let u = last["usage"].as_object_mut().unwrap();
                u.remove("total_tokens");
                u.remove("completion_tokens");
                vec![content, stop, last]
            }
            Shape::NoPromptCount => {
                let mut last = usage_only;
                last["usage"].as_object_mut().unwrap().remove("prompt_tokens");
                vec![content, stop, last]
            }
        };
        let sse: String = frames.iter().map(|f| format!("data: {f}\n\n")).collect::<String>() + "data: [DONE]\n\n";
        ([(header::CONTENT_TYPE, "text/event-stream")], Body::from(sse)).into_response()
    } else {
        let completion = json!({
            "id": "c1", "object": "chat.completion", "created": 1, "model": MODEL,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": usage(engine_cached),
        });
        let mut completion = completion;
        if shape == Shape::NoPromptCount {
            completion["usage"].as_object_mut().unwrap().remove("prompt_tokens");
        }
        match shape {
            Shape::LateBody { millis } => {
                let bytes = Bytes::from(serde_json::to_vec(&completion).unwrap());
                let late = futures::stream::once(async move {
                    tokio::time::sleep(Duration::from_millis(millis)).await;
                    Ok::<_, std::io::Error>(bytes)
                });
                ([(header::CONTENT_TYPE, "application/json")], Body::from_stream(late)).into_response()
            }
            _ => axum::Json(completion).into_response(),
        }
    }
}

async fn client_usage(req: Request, next: Next) -> Response {
    let wanted = match req.headers().get("x-test-client-usage").and_then(|v| v.to_str().ok()) {
        Some("final") => Some(ClientUsage::Final),
        Some("omitted") => Some(ClientUsage::Omitted),
        _ => None,
    };
    let response = next.run(req).await;
    match wanted {
        Some(usage) => crate::inference::stream_usage::apply(response, usage, 1 << 20),
        None => response,
    }
}

type Observed = Arc<Mutex<Option<Extensions>>>;

async fn observe(State(slot): State<Observed>, req: Request, next: Next) -> Response {
    let response = next.run(req).await;
    *slot.lock().unwrap() = Some(response.extensions().clone());
    response
}

struct Stack {
    f: Fixture,
    server: axum_test::TestServer,
    upstream: Upstream,
    observed: Observed,
}

#[derive(Debug)]
struct Billed {
    text: String,
    billed: crate::prompt_cache::BilledCache,
    engine: Option<u64>,
}

impl Stack {
    async fn new(pool: PgPool, terms: Option<CacheAssurance>) -> Self {
        Self::with_orgs(pool, false, terms).await
    }

    async fn with_orgs(pool: PgPool, allowlisted: bool, terms: Option<CacheAssurance>) -> Self {
        let f = fixture_with(pool.clone(), allowlisted, 0, terms).await;
        let endpoint = create_test_endpoint(&pool, "ep", f.principal).await;
        for (name, alias) in [("m", MODEL), ("m2", OTHER_MODEL)] {
            let model_id = create_test_model(&pool, name, alias, endpoint, f.principal).await;
            sqlx::query!(
                r#"INSERT INTO model_cache_tariffs
                 (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, min_prefix_tokens)
               VALUES ($1, 1.25, 2.0, 2.5, 1024)"#,
                model_id
            )
            .execute(&pool)
            .await
            .unwrap();
        }

        let tok = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/v1/models"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(json!({
                "models": [{"alias": MODEL, "hf_repo": "o/m", "tokenizer_version": "sha256:t1"}]
            })))
            .mount(&tok)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/v1/tokenize"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(json!({
                "virtual_model": MODEL, "tokenizer_version": "sha256:t1",
                "segment_counts": [1500], "cumulative": [1500], "total": 1500
            })))
            .mount(&tok)
            .await;
        let tok_uri = tok.uri();
        std::mem::forget(tok);

        let (tier, telemetry) = policies();
        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            crate::prompt_cache::TokenizerClient::new(tok_uri),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            tier,
            telemetry,
            false,
        );
        let up = Upstream(
            Arc::new(Mutex::new(Reply {
                prompt_tokens: 0,
                engine_cached: 0,
                shape: Shape::Final,
            })),
            Arc::new(Mutex::new(None)),
        );
        let observed: Observed = Default::default();
        let translators: Vec<Arc<dyn ProtocolTranslator>> = vec![Arc::new(AnthropicMessages::new(true)), Arc::new(OpenResponses::new())];
        let app = Router::new()
            .route("/v1/chat/completions", post(upstream))
            .route("/v1/messages", post(upstream))
            .route("/v1/responses", post(upstream))
            .with_state(up.clone())
            .layer(from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)).with_reference_cache(f.cache.clone()),
                cache_middleware,
            ))
            .layer(from_fn_with_state(TranslationRegistry::new(translators), translation_middleware))
            .layer(from_fn_with_state(observed.clone(), observe))
            .layer(axum::middleware::from_fn(client_usage));
        Self {
            f,
            server: axum_test::TestServer::new(app).unwrap(),
            upstream: up,
            observed,
        }
    }

    async fn send(&self, path: &str, body: &Value, prompt_tokens: u64, engine_cached: u64) -> Billed {
        self.send_as(&self.f.secret, path, body, prompt_tokens, engine_cached).await
    }

    async fn send_as(&self, secret: &str, path: &str, body: &Value, prompt_tokens: u64, engine_cached: u64) -> Billed {
        self.upstream.set(prompt_tokens, engine_cached);
        self.send_raw(secret, path, body).await
    }

    async fn send_shaped(&self, path: &str, body: &Value, prompt_tokens: u64, engine_cached: u64, shape: Shape) -> Billed {
        self.upstream.set_shaped(prompt_tokens, engine_cached, shape);
        self.send_raw(&self.f.secret, path, body).await
    }

    async fn send_raw(&self, secret: &str, path: &str, body: &Value) -> Billed {
        self.send_with(secret, path, body, None).await
    }

    async fn send_with(&self, secret: &str, path: &str, body: &Value, client_usage: Option<ClientUsage>) -> Billed {
        let mut request = self.server.post(path).add_header("authorization", format!("Bearer {secret}"));
        match client_usage {
            Some(ClientUsage::Final) => request = request.add_header("x-test-client-usage", "final"),
            Some(ClientUsage::Omitted) => request = request.add_header("x-test-client-usage", "omitted"),
            Some(ClientUsage::Continuous) | None => {}
        }
        let r = request.json(body).await;
        r.assert_status_ok();
        let ext = self.observed.lock().unwrap().take().expect("the response passed the observer");
        Billed {
            text: r.text(),
            billed: ext.get::<CacheBilling>().and_then(CacheBilling::get).unwrap_or_default(),
            engine: ext.get::<UpstreamCachedTokens>().and_then(UpstreamCachedTokens::get),
        }
    }
}

fn chat(messages: &[(&str, &str)], stream: bool) -> Value {
    let messages: Vec<Value> = messages.iter().map(|(r, c)| json!({"role": r, "content": c})).collect();
    json!({"model": MODEL, "messages": messages, "stream": stream})
}

fn chat_usage(text: &str) -> Value {
    if let Ok(v) = serde_json::from_str::<Value>(text) {
        return v["usage"].clone();
    }
    text.lines()
        .rev()
        .filter_map(|l| l.strip_prefix("data: "))
        .filter_map(|d| serde_json::from_str::<Value>(d).ok())
        .filter_map(|v| v.get("usage").filter(|u| !u.is_null()).cloned())
        .next()
        .expect("a usage frame")
}

fn chat_bytes(v: &Value) -> Bytes {
    Bytes::from(serde_json::to_vec(v).unwrap())
}

async fn assured_chat_round_trip(pool: PgPool, stream: bool) {
    let s = Stack::new(pool, Some(CacheAssurance::GapShare(0.5))).await;
    let turn_1 = chat(&[("system", SYSTEM), ("user", USER_1)], stream);
    let r1 = s.send("/v1/chat/completions", &turn_1, 1_200, 0).await;
    assert_eq!(r1.billed.reference, Some(0), "a cold cache is a recorded miss");
    assert_eq!((r1.billed.stats.read, r1.billed.read_source), (0, None));
    s.f.wait_for(&chat_bytes(&turn_1), true).await;

    let turn_2 = chat(
        &[("system", SYSTEM), ("user", USER_1), ("assistant", ASSISTANT_1), ("user", USER_2)],
        stream,
    );
    let r2 = s.send("/v1/chat/completions", &turn_2, 1_500, 300).await;
    assert_eq!(r2.billed.reference, Some(1_200));
    assert_eq!(r2.billed.stats.read, 750);
    assert_eq!(r2.billed.read_source, Some(CacheReadSource::Reference));
    assert_eq!(r2.engine, Some(300), "the physical hit stays visible");
    let usage = chat_usage(&r2.text);
    assert_eq!(
        usage["prompt_tokens_details"]["cached_tokens"], 750,
        "the customer is told what was billed"
    );
    assert_eq!(usage["cache_read_input_tokens"], 750);
    assert_eq!(usage["cache_creation_input_tokens"], 0);
    assert_eq!(usage["prompt_tokens"], 1_500);
}

#[dwctl_test_macros::test]
async fn an_unmarked_request_is_billed_from_the_reference(pool: PgPool) {
    assured_chat_round_trip(pool, false).await;
}

#[dwctl_test_macros::test]
async fn an_unmarked_stream_is_billed_from_the_reference_at_its_usage_frame(pool: PgPool) {
    assured_chat_round_trip(pool, true).await;
}

#[dwctl_test_macros::test]
async fn without_terms_the_estimate_is_only_recorded(pool: PgPool) {
    let s = Stack::with_orgs(pool, true, None).await;
    let turn_1 = chat(&[("system", SYSTEM), ("user", USER_1)], false);
    s.send("/v1/chat/completions", &turn_1, 1_200, 0).await;
    s.f.wait_for(&chat_bytes(&turn_1), true).await;
    let turn_2 = chat(
        &[("system", SYSTEM), ("user", USER_1), ("assistant", ASSISTANT_1), ("user", USER_2)],
        false,
    );
    let r2 = s.send("/v1/chat/completions", &turn_2, 1_500, 300).await;
    assert_eq!(r2.billed.reference, Some(1_200));
    assert_eq!((r2.billed.stats.read, r2.billed.read_source), (300, Some(CacheReadSource::Engine)));
}

#[dwctl_test_macros::test]
async fn coverage_follows_the_organisation_and_alias_of_the_terms(pool: PgPool) {
    let s = Stack::new(pool.clone(), Some(CacheAssurance::GapShare(0.5))).await;
    let other_org = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
    let other_key = crate::test::utils::create_test_api_key_for_user(&pool, other_org.id).await;

    for stream in [false, true] {
        let mut other_alias = chat(&[("system", SYSTEM), ("user", USER_1)], stream);
        other_alias["model"] = json!(OTHER_MODEL);
        let r = s.send("/v1/chat/completions", &other_alias, 1_200, 300).await;
        assert_eq!(r.billed.reference, None, "the organisation's alias without terms, stream: {stream}");
        assert_eq!((r.billed.stats.read, r.billed.read_source), (300, Some(CacheReadSource::Engine)));

        let same_alias = chat(&[("system", SYSTEM), ("user", USER_2)], stream);
        let r = s.send_as(&other_key.secret, "/v1/chat/completions", &same_alias, 1_200, 300).await;
        assert_eq!(
            r.billed.reference, None,
            "another organisation on the covered alias, stream: {stream}"
        );
        assert_eq!((r.billed.stats.read, r.billed.read_source), (300, Some(CacheReadSource::Engine)));
    }

    let covered = chat(&[("system", SYSTEM), ("user", "A question of its own.")], false);
    let r = s.send("/v1/chat/completions", &covered, 1_200, 0).await;
    assert_eq!(r.billed.reference, Some(0));
    s.f.wait_for(&chat_bytes(&covered), true).await;
    let by_scope: Vec<(Uuid, String)> = sqlx::query_as("SELECT DISTINCT principal_id, virtual_model FROM reference_cache_entries")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(
        by_scope,
        vec![(s.f.principal, MODEL.to_string())],
        "only the covered pair has entries"
    );
}

fn last_count(text: &str, key: &str) -> Option<u64> {
    let needle = format!("\"{key}\":");
    let at = text.rfind(&needle)? + needle.len();
    text[at..].chars().take_while(char::is_ascii_digit).collect::<String>().parse().ok()
}

#[dwctl_test_macros::test]
async fn with_usage_on_every_frame_one_final_frame_bills(pool: PgPool) {
    let s = Stack::new(pool, Some(CacheAssurance::GapShare(0.5))).await;
    let messages = AnthropicMessages::new(true);
    let responses = OpenResponses::new();
    let cases: [(&str, Value, &str); 3] = [
        (
            "/v1/chat/completions",
            chat(&[("system", "Continuous, chat."), ("user", USER_1)], true),
            "cached_tokens",
        ),
        (
            "/v1/messages",
            json!({"model": MODEL, "max_tokens": 64, "stream": true, "system": "Continuous, messages.",
                   "messages": [{"role": "user", "content": USER_1}]}),
            "cache_read_input_tokens",
        ),
        (
            "/v1/responses",
            json!({"model": MODEL, "instructions": "Continuous, responses.", "input": USER_1, "stream": true}),
            "cached_tokens",
        ),
    ];
    for (path, turn_1, key) in cases {
        s.send(path, &turn_1, 1_200, 0).await;
        let translated = match path {
            "/v1/messages" => translate(&messages, path, &turn_1),
            "/v1/responses" => translate(&responses, path, &turn_1),
            _ => chat_bytes(&turn_1),
        };
        s.f.wait_for(&translated, true).await;
        let r = s
            .send_shaped(path, &turn_1, 1_200, 300, Shape::Continuous { interim_engine: 100 })
            .await;
        assert_eq!(r.billed.reference, Some(1_200), "{path}");
        assert_eq!(
            r.billed.stats.read, 750,
            "{path}: 300 + half of 900, from the final frame's engine count"
        );
        assert_eq!(r.engine, Some(300), "{path}: the last physical report");
        if path == "/v1/chat/completions" {
            let interim: Vec<u64> = r
                .text
                .lines()
                .filter_map(|l| l.strip_prefix("data: "))
                .filter_map(|d| serde_json::from_str::<Value>(d).ok())
                .filter(|c| c["choices"].as_array().is_some_and(|c| !c.is_empty()))
                .filter_map(|c| c["usage"]["prompt_tokens_details"]["cached_tokens"].as_u64())
                .collect();
            assert_eq!(interim, [0, 0], "interim frames show no cache count\n{}", r.text);
        }
        assert_eq!(
            last_count(&r.text, key),
            Some(750),
            "{path}: the customer keeps what bills\n{}",
            r.text
        );
    }
}

#[dwctl_test_macros::test]
async fn a_final_usage_frame_missing_a_count_still_agrees(pool: PgPool) {
    let s = Stack::new(pool, Some(CacheAssurance::GapShare(0.5))).await;
    let messages = AnthropicMessages::new(true);
    let responses = OpenResponses::new();
    let cases: [(&str, Value, &str); 3] = [
        (
            "/v1/chat/completions",
            chat(&[("system", "No total, chat."), ("user", USER_1)], true),
            "cached_tokens",
        ),
        (
            "/v1/messages",
            json!({"model": MODEL, "max_tokens": 64, "stream": true, "system": "No total, messages.",
                   "messages": [{"role": "user", "content": USER_1}]}),
            "cache_read_input_tokens",
        ),
        (
            "/v1/responses",
            json!({"model": MODEL, "instructions": "No total, responses.", "input": USER_1, "stream": true}),
            "cached_tokens",
        ),
    ];
    for (path, turn, key) in cases {
        s.send(path, &turn, 1_200, 0).await;
        let translated = match path {
            "/v1/messages" => translate(&messages, path, &turn),
            "/v1/responses" => translate(&responses, path, &turn),
            _ => chat_bytes(&turn),
        };
        s.f.wait_for(&translated, true).await;
        let r = s
            .send_shaped(path, &turn, 1_200, 700, Shape::FinalWithoutTotal { interim_engine: 300 })
            .await;
        assert_eq!(r.billed.stats.read, 950, "{path}");
        assert_eq!(
            last_count(&r.text, key),
            Some(950),
            "{path}: the customer keeps what bills\n{}",
            r.text
        );
    }
}

#[dwctl_test_macros::test]
async fn a_final_frame_missing_two_counts_is_merged(pool: PgPool) {
    let s = Stack::new(pool, Some(CacheAssurance::GapShare(0.5))).await;
    let messages = AnthropicMessages::new(true);
    let responses = OpenResponses::new();
    let cases: [(&str, Value, &str); 3] = [
        (
            "/v1/chat/completions",
            chat(&[("system", "Two missing, chat."), ("user", USER_1)], true),
            "cached_tokens",
        ),
        (
            "/v1/messages",
            json!({"model": MODEL, "max_tokens": 64, "stream": true, "system": "Two missing, messages.",
                   "messages": [{"role": "user", "content": USER_1}]}),
            "cache_read_input_tokens",
        ),
        (
            "/v1/responses",
            json!({"model": MODEL, "instructions": "Two missing, responses.", "input": USER_1, "stream": true}),
            "cached_tokens",
        ),
    ];
    for (path, turn, key) in cases {
        s.send(path, &turn, 1_200, 0).await;
        let translated = match path {
            "/v1/messages" => translate(&messages, path, &turn),
            "/v1/responses" => translate(&responses, path, &turn),
            _ => chat_bytes(&turn),
        };
        s.f.wait_for(&translated, true).await;
        let r = s
            .send_shaped(path, &turn, 1_200, 700, Shape::FinalMissingTwo { complete_engine: 300 })
            .await;
        assert_eq!(r.billed.stats.read, 950, "{path}: {:?}", r.billed);
        assert_eq!(
            last_count(&r.text, key),
            Some(950),
            "{path}: the customer keeps what bills\n{}",
            r.text
        );
    }
}

#[dwctl_test_macros::test]
async fn a_derived_prompt_count_reaches_the_completion_write(pool: PgPool) {
    let s = Stack::new(pool, Some(CacheAssurance::GapShare(0.5))).await;
    for stream in [false, true] {
        let turn = chat(
            &[
                ("system", if stream { "Derived prompt, streamed." } else { "Derived prompt." }),
                ("user", USER_1),
            ],
            stream,
        );
        let r = s.send_shaped("/v1/chat/completions", &turn, 1_200, 0, Shape::NoPromptCount).await;
        assert_eq!(r.billed.reference, Some(0), "stream: {stream}");
        s.f.wait_for(&chat_bytes(&turn), true).await;
        let exact: Option<i32> =
            sqlx::query_scalar("SELECT exact_tokens FROM reference_cache_entries WHERE principal_id = $1 AND prefix_hash = $2")
                .bind(s.f.principal)
                .bind(hashes(&chat_bytes(&turn)).pop().unwrap())
                .fetch_one(&s.f.pool)
                .await
                .unwrap();
        assert_eq!(exact, Some(1_200), "stream: {stream}");
    }
}

#[dwctl_test_macros::test]
async fn totals_on_a_content_frame_bill_at_the_final_frame(pool: PgPool) {
    let s = Stack::new(pool, Some(CacheAssurance::GapShare(0.5))).await;
    let turn = chat(&[("system", SYSTEM), ("user", USER_1)], true);
    s.send("/v1/chat/completions", &turn, 1_200, 0).await;
    s.f.wait_for(&chat_bytes(&turn), true).await;
    let r = s.send_shaped("/v1/chat/completions", &turn, 1_200, 300, Shape::OnLastContent).await;
    assert_eq!((r.billed.reference, r.billed.stats.read), (Some(1_200), 750), "300 + half of 900");
    assert_eq!(chat_usage(&r.text)["prompt_tokens_details"]["cached_tokens"], 750);
}

#[dwctl_test_macros::test]
async fn a_late_body_does_not_eat_the_join_deadline(pool: PgPool) {
    let s = Stack::new(pool, Some(CacheAssurance::GapShare(0.5))).await;
    let turn = chat(&[("system", SYSTEM), ("user", USER_1)], false);
    s.send("/v1/chat/completions", &turn, 1_200, 0).await;
    s.f.wait_for(&chat_bytes(&turn), true).await;

    let mut blocker = s.f.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE reference_cache_entries IN EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        blocker.rollback().await.unwrap();
    });
    let r = s
        .send_shaped("/v1/chat/completions", &turn, 1_200, 300, Shape::LateBody { millis: 400 })
        .await;
    release.await.unwrap();
    assert_eq!((r.billed.reference, r.billed.stats.read), (Some(1_200), 750));
}


struct Gen(u64);

impl Gen {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

const PROMPT: u64 = 1_200;

fn random_usage(g: &mut Gen) -> Value {
    let mut usage = serde_json::Map::new();
    let completion = 1 + g.below(20);
    if g.chance(10) {
        usage.insert("completion_tokens".into(), json!(completion));
        return Value::Object(usage);
    }
    let mut count = |g: &mut Gen, key: &str, value: u64, percent| {
        if g.chance(percent) {
            usage.insert(key.into(), json!(value));
        } else if g.chance(25) {
            usage.insert(key.into(), Value::Null);
        }
    };
    let prompt = if g.chance(15) { PROMPT / 2 } else { PROMPT };
    count(g, "prompt_tokens", prompt, 85);
    count(g, "completion_tokens", completion, 80);
    count(g, "total_tokens", prompt + completion, 80);
    match g.below(10) {
        0 => {
            usage.insert("prompt_tokens_details".into(), Value::Null);
        }
        1 => {}
        _ => {
            usage.insert("prompt_tokens_details".into(), json!({"cached_tokens": g.below(prompt + 1)}));
        }
    }
    Value::Object(usage)
}

struct Generated {
    chunks: Vec<String>,
    any_usage: bool,
    error: bool,
}

fn random_stream(g: &mut Gen, crlf: bool) -> Generated {
    let chunk = |choices: Value, usage: Option<Value>| {
        let mut frame = json!({"id": "c1", "object": "chat.completion.chunk", "created": 1, "model": MODEL, "choices": choices});
        if let Some(u) = usage {
            frame["usage"] = u;
        }
        format!("data: {frame}\n\n")
    };
    let mut frames = Vec::new();
    let mut any_usage = false;
    let mut maybe_usage = |g: &mut Gen, percent| {
        let usage = g.chance(percent).then(|| random_usage(g));
        any_usage |= usage.is_some();
        usage
    };
    if g.chance(50) {
        frames.push(chunk(
            json!([{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": null}]),
            None,
        ));
    }
    for _ in 0..1 + g.below(3) {
        let usage = maybe_usage(g, 30);
        frames.push(chunk(
            json!([{"index": 0, "delta": {"content": "ok "}, "finish_reason": null}]),
            usage,
        ));
    }
    let error = g.chance(15);
    if error {
        let mut frame = json!({"error": {"message": "upstream failed"}});
        if g.chance(50) {
            frame["choices"] = json!([]);
        }
        if let Some(u) = maybe_usage(g, 50) {
            frame["usage"] = u;
        }
        frames.push(format!("data: {frame}\n\n"));
    } else {
        let usage = maybe_usage(g, 30);
        frames.push(chunk(json!([{"index": 0, "delta": {}, "finish_reason": "stop"}]), usage));
        if g.chance(70) {
            let usage = maybe_usage(g, 100);
            let mut frame = chunk(json!([]), usage);
            if g.chance(30) {
                frame = frame.replace("\"choices\":[],", "");
            }
            frames.push(frame);
        }
    }
    let mut body = frames.concat();
    if crlf && g.chance(20) {
        body = body.replace('\n', "\r\n");
    }
    let done = if crlf && g.chance(10) {
        "data: [DONE]\r\n\r\n"
    } else {
        "data: [DONE]\n\n"
    };
    match g.below(10) {
        0 => {}
        1 => {
            let open = body.trim_end_matches(['\r', '\n']).len();
            let line_end = if body.ends_with("\r\n") { 2 } else { 1 };
            body.truncate(if g.chance(50) { open } else { open + line_end });
        }
        2 => {
            body.push_str(done);
            let choices = if g.chance(50) {
                json!([])
            } else {
                json!([{"index": 0, "delta": {}, "finish_reason": "stop"}])
            };
            body.push_str(&chunk(choices, Some(random_usage(g))));
        }
        _ => body.push_str(done),
    }

    let mut cuts: Vec<usize> = (0..g.below(6)).map(|_| g.below(body.len() as u64) as usize).collect();
    cuts.sort_unstable();
    cuts.dedup();
    let mut chunks = Vec::new();
    let mut start = 0;
    for cut in cuts.into_iter().chain([body.len()]) {
        if cut > start {
            chunks.push(body[start..cut].to_string());
            start = cut;
        }
    }
    Generated { chunks, any_usage, error }
}

fn last_chat_usage(text: &str) -> Option<Value> {
    text.lines()
        .rev()
        .filter_map(|l| l.strip_prefix("data:"))
        .filter_map(|d| serde_json::from_str::<Value>(d.trim()).ok())
        .find_map(|v| v.get("usage").filter(|u| u.is_object()).cloned())
}

#[dwctl_test_macros::test]
async fn the_customer_always_keeps_the_billed_count(pool: PgPool) {
    let s = Stack::new(pool, Some(CacheAssurance::GapShare(0.5))).await;
    let messages = AnthropicMessages::new(true);
    let responses = OpenResponses::new();
    let cases: [(&str, Value, &str); 3] = [
        (
            "/v1/chat/completions",
            chat(&[("system", "Invariant, chat."), ("user", USER_1)], true),
            "cached_tokens",
        ),
        (
            "/v1/messages",
            json!({"model": MODEL, "max_tokens": 64, "stream": true, "system": "Invariant, messages.",
                   "messages": [{"role": "user", "content": USER_1}]}),
            "cache_read_input_tokens",
        ),
        (
            "/v1/responses",
            json!({"model": MODEL, "instructions": "Invariant, responses.", "input": USER_1, "stream": true}),
            "cached_tokens",
        ),
    ];
    for (path, turn, _) in &cases {
        s.send(path, turn, PROMPT, 0).await;
        let translated = match *path {
            "/v1/messages" => translate(&messages, path, turn),
            "/v1/responses" => translate(&responses, path, turn),
            _ => chat_bytes(turn),
        };
        s.f.wait_for(&translated, true).await;
    }
    let mut checked: HashMap<(&str, Option<ClientUsage>), u32> = HashMap::new();
    for seed in 1..=300u64 {
        let runs = cases
            .iter()
            .map(|c| (c, None))
            .chain([(&cases[0], Some(ClientUsage::Final)), (&cases[0], Some(ClientUsage::Omitted))]);
        for ((path, turn, key), client_usage) in runs {
            let is_chat = *path == "/v1/chat/completions";
            let mut g = Gen::new(seed);
            let Generated { chunks, any_usage, error } = random_stream(&mut g, is_chat);
            s.upstream.set_script(chunks.clone());
            let r = s.send_with(&s.f.secret, path, turn, client_usage).await;
            let context = || {
                format!(
                    "seed {seed}, {path}, client usage: {client_usage:?}\nupstream: {:?}\ncustomer: {:?}",
                    chunks.concat(),
                    r.text
                )
            };
            assert!(r.billed.stats.read <= PROMPT, "{}", context());
            if is_chat {
                assert_eq!(
                    r.text.contains("upstream failed"),
                    error,
                    "the error reaches the customer; {}",
                    context()
                );
                if let Some(at) = r.text.find("upstream failed") {
                    assert!(last_chat_usage(&r.text[at..]).is_none(), "usage after the error; {}", context());
                }
                let usage = last_chat_usage(&r.text);
                if client_usage == Some(ClientUsage::Omitted) {
                    assert!(usage.is_none(), "a client that asked for no usage got some; {}", context());
                    continue;
                }
                let Some(usage) = usage else {
                    assert!(!any_usage, "a chat stream with usage showed none; {}", context());
                    continue;
                };
                let cached = usage["prompt_tokens_details"]["cached_tokens"].as_u64();
                assert_eq!(cached, Some(r.billed.stats.read), "the customer keeps what bills; {}", context());
                let read = r.billed.stats.read;
                assert!(
                    usage["prompt_tokens"].as_u64().map_or(read == 0, |p| read <= p),
                    "the bill fits the prompt the customer keeps; {}",
                    context()
                );
                *checked.entry((*path, client_usage)).or_default() += 1;
            } else if !error && let Some(visible) = last_count(&r.text, key) {
                assert_eq!(visible, r.billed.stats.read, "the customer keeps what bills; {}", context());
                *checked.entry((*path, client_usage)).or_default() += 1;
            }
        }
    }
    let runs = [
        ("/v1/chat/completions", None),
        ("/v1/messages", None),
        ("/v1/responses", None),
        ("/v1/chat/completions", Some(ClientUsage::Final)),
    ];
    for (path, client_usage) in runs {
        let shown = checked.get(&(path, client_usage)).copied().unwrap_or(0);
        assert!(
            shown > 150,
            "{path} (client usage: {client_usage:?}) showed a cached count in only {shown} of 300 cases"
        );
    }
}

#[dwctl_test_macros::test]
async fn a_marked_request_stays_on_the_module(pool: PgPool) {
    let s = Stack::new(pool, Some(CacheAssurance::GapShare(1.0))).await;
    let turn_1 = chat(&[("system", SYSTEM), ("user", USER_1)], false);
    s.send("/v1/chat/completions", &turn_1, 1_200, 0).await;
    s.f.wait_for(&chat_bytes(&turn_1), true).await;

    let mut marked = chat(
        &[("system", SYSTEM), ("user", USER_1), ("assistant", ASSISTANT_1), ("user", USER_2)],
        false,
    );
    marked["messages"][0]["content"] = json!([{"type": "text", "text": SYSTEM, "cache_control": {"type": "ephemeral"}}]);
    let r = s.send("/v1/chat/completions", &marked, 1_500, 300).await;
    assert_eq!(r.billed.reference, Some(1_200), "recorded all the same");
    assert_eq!(r.billed.stats.read, 0, "the module index is cold; the engine's 300 are ignored");
    assert_eq!(r.billed.stats.creation_5m, 1_500, "the module wrote the marked prefix");
    assert_ne!(r.billed.read_source, Some(CacheReadSource::Reference));
}

#[dwctl_test_macros::test]
async fn a_late_lookup_falls_back_to_the_engine(pool: PgPool) {
    let s = Stack::new(pool, Some(CacheAssurance::GapShare(0.5))).await;
    let mut blocker = s.f.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE reference_cache_entries IN EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    for stream in [false, true] {
        let turn = chat(&[("system", SYSTEM), ("user", if stream { USER_2 } else { USER_1 })], stream);
        let r = s.send("/v1/chat/completions", &turn, 1_200, 300).await;
        assert_eq!(r.billed.reference, None, "stream: {stream}");
        assert_eq!(r.billed.stats.read, 300);
        assert_eq!(r.billed.read_source, Some(CacheReadSource::Engine));
        assert_eq!(chat_usage(&r.text)["prompt_tokens_details"]["cached_tokens"], 300);
    }
    assert_eq!(s.f.rows().await, 0, "nothing written while the table is held");
    blocker.rollback().await.unwrap();
    s.f.wait_for(&chat_bytes(&chat(&[("system", SYSTEM), ("user", USER_1)], false)), true)
        .await;
    s.f.wait_for(&chat_bytes(&chat(&[("system", SYSTEM), ("user", USER_2)], true)), true)
        .await;
}

#[dwctl_test_macros::test]
async fn a_stream_writes_at_its_first_output(pool: PgPool) {
    use futures::StreamExt;
    use tower::ServiceExt;

    let s = Stack::new(pool.clone(), Some(CacheAssurance::GapShare(0.5))).await;
    let (chunks_tx, chunks_rx) = futures::channel::mpsc::unbounded::<Result<Bytes, std::io::Error>>();
    let chunks_rx = Arc::new(Mutex::new(Some(chunks_rx)));
    let (tier, telemetry) = policies();
    let classifier = Classifier::new(
        PrincipalResolver::new(pool.clone()),
        ModelConfigResolver::new(pool.clone()),
        crate::prompt_cache::TokenizerClient::new("http://127.0.0.1:1"),
        Arc::new(PostgresIndex::new(pool.clone(), 1)),
        tier,
        telemetry,
        false,
    );
    let app = Router::new()
        .route(
            "/v1/chat/completions",
            post(move || async move {
                let rx = chunks_rx.lock().unwrap().take().expect("one request");
                ([(header::CONTENT_TYPE, "text/event-stream")], Body::from_stream(rx)).into_response()
            }),
        )
        .layer(from_fn_with_state(
            CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)).with_reference_cache(s.f.cache.clone()),
            cache_middleware,
        ));
    let turn = chat(&[("system", SYSTEM), ("user", USER_1)], true);
    let request = axum::http::Request::post("/v1/chat/completions")
        .header("authorization", format!("Bearer {}", s.f.secret))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&turn).unwrap()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let mut forwarded = response.into_body().into_data_stream();
    let send = |c: &'static str| chunks_tx.unbounded_send(Ok(Bytes::from_static(c.as_bytes()))).unwrap();

    send(": keep-alive\n\n");
    forwarded.next().await.unwrap().unwrap();
    assert_eq!(s.f.rows().await, 0, "nothing before output starts");
    send("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\n");
    forwarded.next().await.unwrap().unwrap();
    s.f.wait_for(&chat_bytes(&turn), false).await;
    assert_eq!(s.f.rows().await, 2, "both boundaries, while the stream is still open");

    send("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1200,\"completion_tokens\":1,\"total_tokens\":1201}}\n\n");
    send("data: [DONE]\n\n");
    drop(chunks_tx);
    while forwarded.next().await.is_some() {}
    s.f.wait_for(&chat_bytes(&turn), true).await;
}

fn translate(translator: &dyn ProtocolTranslator, path: &str, body: &Value) -> Bytes {
    let (parts, ()) = axum::http::Request::post(path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(())
        .unwrap()
        .into_parts();
    translator
        .translate_request(&parts, Bytes::from(serde_json::to_vec(body).unwrap()))
        .unwrap_or_else(|e| panic!("translate {path}: {e:?}"))
        .body
}

#[dwctl_test_macros::test]
async fn messages_requests_write_and_match_entries(pool: PgPool) {
    let s = Stack::new(pool, Some(CacheAssurance::GapShare(0.5))).await;
    let messages = AnthropicMessages::new(true);
    for stream in [false, true] {
        let (system, user_1) = if stream {
            ("A second system prompt.", USER_2)
        } else {
            (SYSTEM, USER_1)
        };
        let turn_1 = json!({"model": MODEL, "max_tokens": 64, "stream": stream, "system": system,
                            "messages": [{"role": "user", "content": user_1}]});
        let r1 = s.send("/v1/messages", &turn_1, 1_200, 0).await;
        assert_eq!(r1.billed.reference, Some(0));
        s.f.wait_for(&translate(&messages, "/v1/messages", &turn_1), true).await;

        let turn_2 = json!({"model": MODEL, "max_tokens": 64, "stream": stream, "system": system, "messages": [
            {"role": "user", "content": user_1},
            {"role": "assistant", "content": [{"type": "text", "text": ASSISTANT_1}]},
            {"role": "user", "content": "Thanks."},
        ]});
        let r2 = s.send("/v1/messages", &turn_2, 1_500, 0).await;
        assert_eq!(r2.billed.reference, Some(1_200), "stream: {stream}");
        assert_eq!(
            (r2.billed.stats.read, r2.billed.read_source),
            (600, Some(CacheReadSource::Reference))
        );
        assert!(r2.text.contains("\"cache_read_input_tokens\":600"), "{}", r2.text);
    }
}

struct StaticStore(ResponseContext);

#[async_trait]
impl ResponseStore for StaticStore {
    async fn store(&self, _response: &Value) -> Result<String, StoreError> {
        unreachable!("hydration only reads")
    }

    async fn get_context(&self, _response_id: &str, _owner: &str) -> Result<Option<ResponseContext>, StoreError> {
        Ok(Some(self.0.clone()))
    }
}

fn chat_completion(message: Value) -> Bytes {
    Bytes::from(
        serde_json::to_vec(&json!({
            "id": "c1", "object": "chat.completion", "created": 1, "model": MODEL,
            "choices": [{"index": 0, "message": message, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12}
        }))
        .unwrap(),
    )
}

async fn hydrated(request: Value, previous_request: &Value, previous_reply: Value) -> Value {
    let request_bytes = Bytes::from(serde_json::to_vec(previous_request).unwrap());
    let response = OpenResponses::new()
        .translate_response(&request_bytes, Some("resp_previous"), chat_completion(previous_reply))
        .unwrap();
    let store = StaticStore(ResponseContext {
        input: previous_request["input"].clone(),
        response: serde_json::from_slice(&response).unwrap(),
    });
    let mut request = request;
    if hydrate_previous_response(&store, "owner", &mut request).await.is_err() {
        panic!("hydration failed");
    }
    request
}

#[dwctl_test_macros::test]
async fn responses_requests_write_and_match_entries(pool: PgPool) {
    let s = Stack::new(pool, Some(CacheAssurance::GapShare(0.5))).await;
    let responses = OpenResponses::new();
    for stream in [false, true] {
        let (system, user_1) = if stream {
            ("A second system prompt.", USER_2)
        } else {
            (SYSTEM, USER_1)
        };
        let turn_1 = json!({"model": MODEL, "instructions": system, "input": user_1, "stream": stream});
        let r1 = s.send("/v1/responses", &turn_1, 1_200, 0).await;
        assert_eq!(r1.billed.reference, Some(0));
        s.f.wait_for(&translate(&responses, "/v1/responses", &turn_1), true).await;

        let turn_2 = hydrated(
            json!({"model": MODEL, "instructions": system, "input": "Thanks.", "stream": stream, "previous_response_id": "resp_previous"}),
            &turn_1,
            json!({"role": "assistant", "content": ASSISTANT_1}),
        )
        .await;
        let r2 = s.send("/v1/responses", &turn_2, 1_500, 0).await;
        assert_eq!(r2.billed.reference, Some(1_200), "stream: {stream}");
        assert_eq!(
            (r2.billed.stats.read, r2.billed.read_source),
            (600, Some(CacheReadSource::Reference))
        );
        assert_eq!(last_count(&r2.text, "cached_tokens"), Some(600), "{}", r2.text);
    }
}


fn assert_prefix(prev: &[u8], next: &[u8], what: &str) {
    let (p, n) = (hashes(prev), hashes(next));
    assert!(!p.is_empty(), "{what}: no boundaries");
    assert!(n.len() > p.len(), "{what}: the next turn adds blocks");
    assert_eq!(&n[..p.len()], &p[..], "{what}: turn N's prefix is not a prefix of turn N+1");
}

#[test]
fn messages_turns_translate_to_a_stable_prefix() {
    let messages = AnthropicMessages::new(true);
    let tools = json!([{"name": "find_drill", "description": "Look a drill up.",
                        "input_schema": {"type": "object", "properties": {"q": {"type": "string"}}, "required": ["q"]}}]);
    let system = json!([{"type": "text", "text": SYSTEM}, {"type": "text", "text": "Prices are in euros."}]);
    let history = [
        json!({"role": "user", "content": USER_1}),
        json!({"role": "assistant", "content": [
            {"type": "text", "text": "Let me look."},
            {"type": "tool_use", "id": "toolu_1", "name": "find_drill", "input": {"q": "lightest"}}
        ]}),
        json!({"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "toolu_1", "content": "compact: 1.1 kg"}
        ]}),
        json!({"role": "assistant", "content": [{"type": "text", "text": ASSISTANT_1}]}),
        json!({"role": "user", "content": [{"type": "text", "text": USER_2}]}),
    ];
    let turn = |n: usize| {
        translate(
            &messages,
            "/v1/messages",
            &json!({"model": MODEL, "max_tokens": 64, "system": system, "tools": tools, "messages": history[..n]}),
        )
    };
    for (n, next) in [(1, 3), (3, 5)] {
        assert_prefix(&turn(n), &turn(next), &format!("messages {n} -> {next}"));
    }
}

#[tokio::test]
async fn responses_turns_with_hydration_translate_to_a_stable_prefix() {
    let responses = OpenResponses::new();
    let tools = json!([{"type": "function", "name": "find_drill", "description": "Look a drill up.",
                        "parameters": {"type": "object", "properties": {"q": {"type": "string"}}, "required": ["q"]}}]);
    let body = |input: Value, previous: Option<&str>| {
        let mut v = json!({"model": MODEL, "instructions": SYSTEM, "tools": tools, "input": input});
        if let Some(id) = previous {
            v["previous_response_id"] = json!(id);
        }
        v
    };

    let turn_1 = body(json!(USER_1), None);
    let reply_1 = json!({"role": "assistant", "content": null, "tool_calls": [
        {"id": "call_1", "type": "function", "function": {"name": "find_drill", "arguments": "{\"q\":\"lightest\"}"}}
    ]});
    let turn_2 = hydrated(
        body(
            json!([{"type": "function_call_output", "call_id": "call_1", "output": "compact: 1.1 kg"}]),
            Some("resp_1"),
        ),
        &turn_1,
        reply_1,
    )
    .await;
    let turn_3 = hydrated(
        body(json!(USER_2), Some("resp_2")),
        &turn_2,
        json!({"role": "assistant", "content": ASSISTANT_1}),
    )
    .await;

    let [t1, t2, t3] = [&turn_1, &turn_2, &turn_3].map(|t| translate(&responses, "/v1/responses", t));
    assert_prefix(&t1, &t2, "responses 1 -> 2 (tool call hydrated)");
    assert_prefix(&t2, &t3, "responses 2 -> 3 (text hydrated)");

    let full_3 = body(turn_3["input"].clone(), None);
    assert_eq!(
        hashes(&translate(&responses, "/v1/responses", &full_3)),
        hashes(&t3),
        "hydration renders exactly what a client resending the history would"
    );
}
