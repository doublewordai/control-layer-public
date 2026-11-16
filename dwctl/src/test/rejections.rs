
use crate::test::utils::create_test_config;
use sqlx::PgPool;

#[dwctl_test_macros::test]
async fn test_dwctl_layer_rejections_are_counted(pool: PgPool) {
    let mut config = create_test_config();
    config.onwards.strict_mode = true;
    config.enable_metrics = true;
    config.cache.enabled = false;
    config.limits.requests.max_body_size = 64;
    let app = crate::Application::new_with_pool(config, Some(pool), None)
        .await
        .expect("Failed to create application");
    let (server, _services) = app.into_test_server();

    let response = server.post("/ai/v1/chat/completions").bytes(vec![b'x'; 65].into()).await;
    response.assert_status(axum::http::StatusCode::PAYLOAD_TOO_LARGE);
    let response = server
        .post("/ai/v1/chat/completions")
        .add_header("Model-Override", "other")
        .json(&serde_json::json!({"model": "m"}))
        .await;
    response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    let response = server
        .post("/ai/v1/responses")
        .json(&serde_json::json!({"model": "m", "include": ["x"]}))
        .await;
    response.assert_status(axum::http::StatusCode::BAD_REQUEST);

    let metrics = server.get("/internal/metrics").await.text();
    let count = |code: &str, status: &str| {
        metrics
            .lines()
            .filter(|line| {
                line.starts_with("onwards_rejections_total{")
                    && line.contains(&format!("code=\"{code}\""))
                    && line.contains(&format!("status=\"{status}\""))
                    && line.contains("traffic=\"realtime\"")
            })
            .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
            .sum::<f64>()
    };
    assert_eq!(count("request_too_large", "413"), 1.0, "{metrics}");
    assert_eq!(count("unsupported_header", "400"), 1.0, "{metrics}");
    assert_eq!(count("invalid_parameter", "400"), 1.0, "{metrics}");
}

#[derive(Clone, Debug, Default)]
struct Exported(std::sync::Arc<std::sync::Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>);
impl opentelemetry_sdk::trace::SpanExporter for Exported {
    async fn export(&self, batch: Vec<opentelemetry_sdk::trace::SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.lock().unwrap().extend(batch);
        Ok(())
    }
}

#[dwctl_test_macros::test]
async fn test_dwctl_layer_rejections_are_traced(pool: PgPool) {
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider, SpanData};
    use tracing_subscriber::layer::SubscriberExt;

    let exporter = Exported::default();
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .with_simple_exporter(exporter.clone())
        .build();
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(provider.tracer("rejections-test"))),
    );

    let mut config = create_test_config();
    config.onwards.strict_mode = true;
    config.cache.enabled = false;
    config.limits.requests.max_body_size = 4096;
    let mock = wiremock::MockServer::start().await;
    let (server, key, _services) = super::responses::setup_ai_test_with_config(pool, &mock, config).await;

    let values = |span: &SpanData, key: &str| -> Vec<String> {
        span.attributes
            .iter()
            .filter(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.to_string())
            .collect()
    };
    let attr = |span: &SpanData, key: &str| values(span, key).into_iter().next();
    let refused = |path: &str| -> (SpanData, Vec<String>, bool, Vec<SpanData>) {
        let exported = std::mem::take(&mut *exporter.0.lock().unwrap());
        let root = exported
            .iter()
            .find(|span| attr(span, "url.path").as_deref() == Some(path))
            .expect("request span exported")
            .clone();
        let attributed = exported.iter().any(|span| attr(span, "onwards.account").is_some());
        let error_types = values(&root, "error.type");
        (root, error_types, attributed, exported)
    };
    let recorded = |spans: &[SpanData], key: &str| spans.iter().find_map(|span| attr(span, key));
    const PLANTED_NUMBER: &str = "9182736455";
    let leaks = |spans: &[SpanData]| -> Vec<String> {
        spans
            .iter()
            .flat_map(|span| {
                let events = span.events.iter().flat_map(|event| {
                    std::iter::once(event.name.to_string()).chain(event.attributes.iter().map(|kv| kv.value.to_string()))
                });
                span.attributes
                    .iter()
                    .map(|kv| kv.value.to_string())
                    .chain(events)
                    .collect::<Vec<_>>()
            })
            .filter(|value| value.contains("planted") || value.contains(PLANTED_NUMBER))
            .collect()
    };
    exporter.0.lock().unwrap().clear();

    let response = server
        .post("/ai/v1/messages")
        .add_header("x-api-key", &key)
        .add_header("anthropic-version", "2023-06-01")
        .bytes(vec![b'x'; 4097].into())
        .await;
    response.assert_status(axum::http::StatusCode::PAYLOAD_TOO_LARGE);
    let (root, error_types, _, _) = refused("/ai/v1/messages");
    assert_eq!(error_types, ["request_too_large"], "one error.type, the rejection code");
    assert_eq!(attr(&root, "http.response.status_code").as_deref(), Some("413"));
    assert!(attr(&root, "onwards.account").is_some(), "x-api-key attributes the account");
    assert!(attr(&root, "onwards.api_key_id").is_some());

    let response = server
        .post("/ai/v1/messages")
        .add_header("x-api-key", &key)
        .add_header("anthropic-version", "2023-06-01")
        .json(&serde_json::json!({
            "model": "gpt-4o", "max_tokens": "planted-value",
            "messages": [{"role": "user", "content": "hi"}],
            "metadata": {"planted-key": "planted-value"}
        }))
        .await;
    response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json();
    assert_eq!(body["type"], "error", "Anthropic's error shape, which has no param");
    let (_, error_types, attributed, spans) = refused("/ai/v1/messages");
    assert_eq!(error_types, ["schema_mismatch"], "the request span reports the rejection code");
    assert!(attributed, "x-api-key attributes the account");
    assert_eq!(recorded(&spans, "onwards.rejection.param").as_deref(), Some("max_tokens"));
    assert_eq!(recorded(&spans, "onwards.rejection.reason").as_deref(), Some("invalid_type"));
    assert_eq!(leaks(&spans), Vec::<String>::new());

    let response = server
        .post("/ai/v1/messages")
        .add_header("x-api-key", &key)
        .add_header("anthropic-version", "2023-06-01")
        .json(&serde_json::json!({
            "model": "gpt-4o", "max_tokens": 5,
            "messages": [{"role": "user", "content": "hi"}],
            "tool_choice": {"planted-key": {"$serde_json::private::RawValue": 9182736455u64}}
        }))
        .await;
    response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    let (_, error_types, _, spans) = refused("/ai/v1/messages");
    assert_eq!(error_types, ["invalid_json"]);
    assert!(
        spans.iter().any(|span| !span.events.is_empty()),
        "the refusal's log is exported as an event"
    );
    assert_eq!(leaks(&spans), Vec::<String>::new());

    let response = server
        .post("/ai/v1/messages")
        .add_header("x-api-key", &key)
        .add_header("anthropic-version", "2023-06-01")
        .json(&serde_json::json!({
            "model": "gpt-4o", "max_tokens": 5,
            "messages": [{"role": "planted-role", "content": "hi"}]
        }))
        .await;
    response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    let (_, error_types, _, spans) = refused("/ai/v1/messages");
    assert_eq!(error_types, ["invalid_parameter"]);
    assert_eq!(recorded(&spans, "onwards.rejection.param").as_deref(), Some("messages[].role"));
    assert_eq!(leaks(&spans), Vec::<String>::new());

    let response = server
        .post("/ai/v1/responses")
        .add_header("Authorization", &format!("Bearer {key}"))
        .json(&serde_json::json!({
            "model": "gpt-4o", "input": "hi",
            "text": {"format": {"type": "planted-value"}},
            "metadata": {"planted-key": "planted-value"}
        }))
        .await;
    response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json();
    assert_eq!(body["error"]["param"], "text.format.type");
    let (_, error_types, attributed, spans) = refused("/ai/v1/responses");
    assert_eq!(error_types, ["schema_mismatch"]);
    assert!(attributed, "the bearer key attributes the account");
    assert_eq!(recorded(&spans, "onwards.rejection.param").as_deref(), Some("text.format.type"));
    assert_eq!(recorded(&spans, "onwards.rejection.reason").as_deref(), Some("unknown_variant"));
    assert_eq!(leaks(&spans), Vec::<String>::new());
}
