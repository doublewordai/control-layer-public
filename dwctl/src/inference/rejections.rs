
use axum::{
    http::{Extensions, HeaderMap, Request, header::AUTHORIZATION, request::Parts},
    response::Response,
};
use onwards::{auth::KeyHash, rejections::RejectionContext, strict::schema_mismatch::SchemaMismatch, target::Targets};

use crate::{auth::internal_credential::presented_key_hash, inference::translation::anthropic};

pub const DISPATCHED_HEADER: &str = "x-fusillade-batch-created-at";

#[derive(Clone, Default)]
pub struct Recorder {
    targets: Targets,
}

impl Recorder {
    pub fn new(targets: Targets) -> Self {
        Self { targets }
    }

    pub fn configured_alias<'m>(&self, model: &'m str) -> Option<&'m str> {
        self.targets.configured_alias(model)
    }

    pub fn class_route_of(&self, model: &str) -> Option<String> {
        self.targets.class_route_of(model)
    }
}

#[derive(Clone, Debug)]
pub struct RejectionCode {
    code: &'static str,
    recorded_on: Option<tracing::span::Id>,
}

impl RejectionCode {
    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn recorded_on(&self, span: &tracing::Span) -> bool {
        self.recorded_on.is_some() && self.recorded_on == span.id()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Origin<'a> {
    recorder: Option<&'a Recorder>,
    headers: &'a HeaderMap,
    extensions: Option<&'a Extensions>,
    path: &'a str,
}

impl<'a> Origin<'a> {
    pub(crate) fn new(recorder: Option<&'a Recorder>, headers: &'a HeaderMap, path: &'a str) -> Self {
        Self {
            recorder,
            headers,
            extensions: None,
            path,
        }
    }

    fn with_extensions(extensions: &'a Extensions, headers: &'a HeaderMap, path: &'a str) -> Self {
        Self {
            extensions: Some(extensions),
            ..Self::new(extensions.get(), headers, path)
        }
    }
}

impl<'a> From<&'a Parts> for Origin<'a> {
    fn from(parts: &'a Parts) -> Self {
        Self::with_extensions(&parts.extensions, &parts.headers, parts.uri.path())
    }
}

impl<'a, B> From<&'a Request<B>> for Origin<'a> {
    fn from(request: &'a Request<B>) -> Self {
        Self::with_extensions(request.extensions(), request.headers(), request.uri().path())
    }
}

pub(crate) fn refuse<'a>(
    origin: impl Into<Origin<'a>>,
    response: Response,
    code: &'static str,
    param: Option<&str>,
    model: Option<&str>,
) -> Response {
    Scope::of(origin).refuse(response, code, param, model)
}

pub(crate) fn refuse_mismatch<'a>(
    origin: impl Into<Origin<'a>>,
    response: Response,
    mismatch: Option<&SchemaMismatch>,
    model: Option<&str>,
) -> Response {
    let param = mismatch.and_then(|mismatch| mismatch.path.as_deref());
    let reason = mismatch.map(|mismatch| mismatch.reason);
    Scope::of(origin).refuse_with_reason(response, "schema_mismatch", param, reason, model)
}

pub(crate) fn record_caller<'a>(origin: impl Into<Origin<'a>>, model: Option<&str>) {
    let origin = origin.into();
    let span = origin
        .extensions
        .and_then(|extensions| extensions.get::<crate::telemetry::RequestSpan>())
        .map_or_else(tracing::Span::current, |root| root.0.clone());
    let Some((targets, mut context)) = Scope::of(origin).caller else {
        return;
    };
    if let Some(model) = model {
        context.set_model_if_configured(&targets, model);
    }
    if let Some(identity) = context.identity() {
        crate::telemetry::record_identity_attributes(&identity, &span);
    }
}

pub(crate) struct Scope {
    caller: Option<(Targets, RejectionContext)>,
}

impl Scope {
    pub(crate) fn of<'a>(origin: impl Into<Origin<'a>>) -> Self {
        let origin = origin.into();
        let caller = origin.recorder.map(|recorder| {
            let bearer = api_key(origin.path, origin.headers);
            let key = match origin.extensions {
                Some(extensions) => presented_key_hash(extensions, bearer),
                None => bearer.map(crate::crypto::hash_api_key),
            };
            let context = RejectionContext::for_key(
                &recorder.targets,
                key.map(KeyHash::from_bytes).as_ref(),
                origin.headers.contains_key(DISPATCHED_HEADER),
            );
            (recorder.targets.clone(), context)
        });
        Self { caller }
    }

    pub(crate) fn refuse(self, response: Response, code: &'static str, param: Option<&str>, model: Option<&str>) -> Response {
        self.refuse_with_reason(response, code, param, None, model)
    }

    fn refuse_with_reason(
        self,
        mut response: Response,
        code: &'static str,
        param: Option<&str>,
        reason: Option<&'static str>,
        model: Option<&str>,
    ) -> Response {
        let mut recorded_on = None;
        if let Some((targets, mut context)) = self.caller {
            if let Some(model) = model {
                context.set_model_if_configured(&targets, model);
            }
            context.record_with_reason(response.status(), code, param, reason);
            recorded_on = tracing::Span::current().id();
            if let Some(identity) = context.identity() {
                response.extensions_mut().insert(identity);
            }
        }
        response.extensions_mut().insert(RejectionCode { code, recorded_on });
        response
    }
}

pub(crate) fn api_key<'a>(path: &str, headers: &'a HeaderMap) -> Option<&'a str> {
    match headers.get(AUTHORIZATION) {
        Some(_) => bearer_token(headers),
        None if anthropic::accepts_x_api_key(path, headers) => headers.get("x-api-key")?.to_str().ok(),
        None => None,
    }
}

pub(crate) fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?.trim();
    let (scheme, token) = value.split_once(char::is_whitespace)?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then_some(token.trim())
        .filter(|token| !token.is_empty())
}

pub(crate) fn model_of(body: &[u8]) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct Model {
        model: Option<String>,
    }
    serde_json::from_slice::<Model>(body).ok()?.model
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::future::Future;

    use axum::Extension;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use onwards::target::Targets;

    use super::Recorder;

    pub fn recorder(alias: &str) -> Recorder {
        let targets = Targets::from_config(
            serde_json::from_value(serde_json::json!({
                "targets": {alias: {"url": "https://upstream.example.com/v1/"}},
                "auth": {"global_keys": [], "key_definitions": {
                    "k": {"key": "sk-test", "labels": {"account": "acct-1"}}
                }}
            }))
            .unwrap(),
        )
        .unwrap();
        Recorder::new(targets)
    }

    pub fn layer(alias: &str) -> Extension<Recorder> {
        Extension(recorder(alias))
    }

    pub fn rejections<F: Future>(future: F) -> (Vec<String>, F::Output) {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let output = metrics::with_local_recorder(&recorder, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(future)
        });
        let lines = handle
            .render()
            .lines()
            .filter(|line| line.starts_with("onwards_rejections_total{"))
            .map(str::to_string)
            .collect();
        (lines, output)
    }
}

#[cfg(test)]
mod tests {
    use super::{test_support::*, *};
    use axum::{
        Router,
        body::Body,
        http::{HeaderValue, StatusCode},
        response::IntoResponse,
        routing::post,
    };
    use tower::ServiceExt;

    async fn refused(request: Request<Body>) -> Response {
        let (parts, body) = request.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let response = StatusCode::BAD_REQUEST.into_response();
        refuse(&parts, response, "some_code", Some("some_param"), model_of(&bytes).as_deref())
    }

    fn app() -> Router {
        Router::new().route("/chat/completions", post(refused)).layer(layer("known-model"))
    }

    #[test]
    fn counts_configured_models_and_dispatched_traffic() {
        let (lines, (code, identities)) = rejections(async {
            let mut code = None;
            let mut identities = Vec::new();
            for (model, dispatched) in [("known-model", false), ("not-configured", true)] {
                let mut request = Request::post("/chat/completions").header("authorization", "Bearer sk-test");
                if dispatched {
                    request = request.header(DISPATCHED_HEADER, "2026-01-01T00:00:00Z");
                }
                let body = serde_json::json!({"model": model}).to_string();
                let response = app().oneshot(request.body(Body::from(body)).unwrap()).await.unwrap();
                code = response.extensions().get::<RejectionCode>().map(RejectionCode::code);
                identities.push(response.extensions().get::<onwards::RequestIdentity>().cloned());
            }
            (code, identities)
        });
        assert_eq!(code, Some("some_code"), "the response carries its code");
        let [configured, unconfigured] = identities.try_into().unwrap();
        let configured = configured.expect("the response names the caller");
        assert_eq!(configured.account_id.as_deref(), Some("acct-1"));
        assert_eq!(configured.model.as_deref(), Some("known-model"));
        assert_eq!(
            unconfigured.expect("the response names the caller").model,
            None,
            "unconfigured models are not labelled"
        );
        let realtime = lines.iter().find(|line| line.contains("traffic=\"realtime\"")).unwrap();
        assert!(realtime.contains("model=\"known-model\""), "{realtime}");
        assert!(
            realtime.contains("code=\"some_code\"") && realtime.contains("status=\"400\""),
            "{realtime}"
        );
        let dispatched = lines.iter().find(|line| line.contains("traffic=\"dispatched\"")).unwrap();
        assert!(
            dispatched.contains("model=\"\""),
            "unconfigured models stay unlabelled: {dispatched}"
        );
    }

    #[test]
    fn records_nothing_without_a_recorder() {
        let (lines, _) = rejections(async {
            let app = Router::new().route("/chat/completions", post(refused));
            app.oneshot(Request::post("/chat/completions").body(Body::empty()).unwrap())
                .await
                .unwrap();
        });
        assert!(lines.is_empty(), "{lines:?}");
    }

    #[test]
    fn names_no_caller_for_an_unknown_key() {
        let (_, identity) = rejections(async {
            let request = Request::post("/chat/completions")
                .header("authorization", "Bearer sk-unknown")
                .body(Body::from(r#"{"model": "known-model"}"#))
                .unwrap();
            let response = app().oneshot(request).await.unwrap();
            response.extensions().get::<onwards::RequestIdentity>().cloned()
        });
        assert_eq!(identity, None);
    }

    #[test]
    fn resolves_the_api_key_by_the_protocol_s_rules() {
        let headers = |pairs: &[(&'static str, &'static str)]| {
            let mut headers = HeaderMap::new();
            for (name, value) in pairs {
                headers.insert(*name, HeaderValue::from_static(value));
            }
            headers
        };
        let anthropic = headers(&[("x-api-key", "sk-x")]);
        assert_eq!(api_key("/ai/v1/messages", &anthropic), Some("sk-x"));
        assert_eq!(
            api_key("/ai/v1/chat/completions", &anthropic),
            None,
            "OpenAI routes ignore x-api-key"
        );
        let models = headers(&[("x-api-key", "sk-x"), ("anthropic-version", "2023-06-01")]);
        assert_eq!(api_key("/ai/v1/models", &models), Some("sk-x"));
        let both = headers(&[("authorization", "Bearer sk-a"), ("x-api-key", "sk-x")]);
        assert_eq!(api_key("/ai/v1/messages", &both), Some("sk-a"), "Authorization wins");
        let lowercase = headers(&[("authorization", "bearer sk-a")]);
        assert_eq!(
            api_key("/ai/v1/models", &lowercase),
            Some("sk-a"),
            "the scheme is case-insensitive, as /models accepts it"
        );
        let basic = headers(&[("authorization", "Basic abc"), ("x-api-key", "sk-x")]);
        assert_eq!(
            api_key("/ai/v1/messages", &basic),
            None,
            "a present Authorization is never replaced"
        );
    }

    #[test]
    fn reads_the_model_of_a_body() {
        assert_eq!(model_of(br#"{"model": "m", "x": 1}"#).as_deref(), Some("m"));
        assert_eq!(model_of(b"{not json"), None);
    }
}

#[cfg(test)]
mod class_route_tests {
    use onwards::target::Targets;

    use super::Recorder;

    #[test]
    fn only_targets_marked_by_the_config_writer_are_class_routes() {
        let targets = Targets::from_config(
            serde_json::from_value(serde_json::json!({
                "targets": {
                    "vendor:legacy": {"url": "https://upstream.example.com/v1/"},
                    "model": {"providers": [{"url": "https://upstream.example.com/v1/"}]},
                    "model:fast": {"providers": [{"url": "https://upstream.example.com/v1/"}], "class_route_of": "model"}
                },
                "auth": {"global_keys": [], "key_definitions": {}}
            }))
            .unwrap(),
        )
        .unwrap();
        let recorder = Recorder::new(targets);
        assert_eq!(recorder.class_route_of("model:fast").as_deref(), Some("model"));
        assert_eq!(recorder.class_route_of("vendor:legacy"), None);
        assert_eq!(recorder.class_route_of("model"), None);
        assert_eq!(recorder.class_route_of("unknown:fast"), None);
    }
}
