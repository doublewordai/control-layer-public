
use axum::{extract::Request, middleware::Next, response::Response};
use chrono::{SecondsFormat, Utc};
use onwards::{ErrorOrigin, RequestIdentity, ServedBy};
use opentelemetry::trace::TraceContextExt;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
pub use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::trace::{SpanExporter, Tracer};
use serde_json::{Map, Value, json};
use std::backtrace::{Backtrace, BacktraceStatus};
use std::collections::HashMap;
use std::io::Write;
use std::panic::PanicHookInfo;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber, info};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::fmt::format::{Format, Json, JsonFields, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields, MakeWriter};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::{LookupSpan, Registry, SpanRef};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use crate::log_redaction::{RedactingMakeWriter, RedactingSpanExporter, redact_secrets};

pub const OTEL_TRACES_FILTER: &str = "OTEL_TRACES_FILTER";

pub const DEFAULT_OTEL_TRACES_FILTER: &str = "info";

use crate::config::LogFormat;
use crate::inference::rejections::RejectionCode;
use crate::inference::serving_name::{STANDARD_CLASS, split_served};

const MAX_SPAN_ACCOUNT_NAME_CHARS: usize = 128;

pub fn span_account_name(display_name: Option<&str>, username: &str) -> String {
    display_name
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or(username)
        .chars()
        .take(MAX_SPAN_ACCOUNT_NAME_CHARS)
        .collect()
}

#[derive(Clone, Debug)]
pub struct BatchCompletionWindow(String);

pub async fn carry_batch_completion_window(request: Request, next: Next) -> Response {
    let window = request
        .headers()
        .get("x-fusillade-batch-completion-window")
        .and_then(|value| value.to_str().ok())
        .map(|window| BatchCompletionWindow(window.to_string()));
    let mut response = next.run(request).await;
    if let Some(window) = window {
        response.extensions_mut().insert(window);
    }
    response
}

pub fn record_ai_response_attributes<B>(response: &axum::http::Response<B>, span: &tracing::Span, status: u16) {
    let extensions = response.extensions();
    if let Some(identity) = extensions.get::<RequestIdentity>() {
        record_identity_attributes(identity, span);
        let batch = identity.key_purpose.as_deref() == Some("batch");
        if batch && let Some(BatchCompletionWindow(window)) = extensions.get() {
            span.set_attribute("doubleword.batch.completion_window", window.clone());
        }
    }
    if status >= 400 {
        let (source, code) = match (extensions.get::<ErrorOrigin>(), extensions.get::<RejectionCode>()) {
            (Some(ErrorOrigin::Provider), _) => ("provider", None),
            (Some(ErrorOrigin::Gateway { code }), _) => ("gateway", Some(code.clone())),
            (None, Some(rejection)) => ("gateway", Some(rejection.code().to_string())),
            (None, None) if extensions.get::<ServedBy>().is_some() => ("provider", None),
            (None, None) => return,
        };
        span.set_attribute("doubleword.error.source", source);
        if let Some(code) = code.filter(|code| !code.is_empty()) {
            span.set_attribute("doubleword.rejection.code", code);
        }
    }
}

#[derive(Clone, Debug)]
pub struct RequestSpan(pub tracing::Span);

impl RequestSpan {
    pub fn of(extensions: Option<&axum::http::Extensions>) -> tracing::Span {
        extensions
            .and_then(|extensions| extensions.get::<Self>())
            .map_or_else(tracing::Span::current, |root| root.0.clone())
    }
}

pub fn record_request_shape(body: &serde_json::Value, span: &tracing::Span) {
    use crate::request_logging::schema_validity::{self, Budget, Validity};
    use crate::request_logging::{structured_output::response_format_label, tool_calls::tool_choice_mode};

    let mut budget = Budget::per_request();

    let format = [
        ("/response_format/type", "/response_format/json_schema/schema"),
        ("/text/format/type", "/text/format/schema"),
        ("/output_config/format/type", "/output_config/format/schema"),
    ]
    .into_iter()
    .find_map(|(format, schema)| Some((body.pointer(format)?.as_str()?, body.pointer(schema))));
    if let Some((format, schema)) = format {
        let format = response_format_label(format);
        span.set_attribute("doubleword.response_format", format);
        if format == "json_schema" {
            let validity = match schema.filter(|schema| !schema.is_null()) {
                Some(schema) => schema_validity::check(schema, &mut budget),
                None => Validity::Invalid("missing"),
            };
            match validity {
                Validity::Valid => span.set_attribute("doubleword.response_format.schema_valid", true),
                Validity::Invalid(reason) => {
                    span.set_attribute("doubleword.response_format.schema_valid", false);
                    span.set_attribute("doubleword.response_format.schema_error", reason);
                }
                Validity::Unchecked => span.set_attribute("doubleword.response_format.schema_error", "unchecked"),
            }
        }
    }
    if let Some(mode) = body.get("tool_choice").and_then(tool_choice_mode) {
        span.set_attribute("doubleword.tool_choice", mode);
    }
    if let Some(tools) = body.get("tools").and_then(serde_json::Value::as_array) {
        span.set_attribute("doubleword.tool_count", i64::try_from(tools.len()).unwrap_or(i64::MAX));
        let parameters = tools.iter().filter_map(|tool| {
            tool.pointer("/function/parameters")
                .or_else(|| tool.get("parameters"))
                .or_else(|| tool.get("input_schema"))
                .filter(|schema| !schema.is_null())
        });
        let (mut invalid, mut unchecked, mut first_error) = (0_i64, 0_i64, None);
        for schema in parameters {
            match schema_validity::check(schema, &mut budget) {
                Validity::Valid => {}
                Validity::Invalid(reason) => {
                    invalid += 1;
                    first_error.get_or_insert(reason);
                }
                Validity::Unchecked => unchecked += 1,
            }
        }
        span.set_attribute("doubleword.tool_schemas.invalid_count", invalid);
        if let Some(reason) = first_error {
            span.set_attribute("doubleword.tool_schemas.first_error", reason);
        }
        if unchecked > 0 {
            span.set_attribute("doubleword.tool_schemas.unchecked_count", unchecked);
        }
    }
    let max_tokens = ["max_completion_tokens", "max_tokens", "max_output_tokens"]
        .iter()
        .find_map(|key| body.get(*key).and_then(serde_json::Value::as_i64));
    if let Some(max_tokens) = max_tokens {
        span.set_attribute("gen_ai.request.max_tokens", max_tokens);
    }
    if let Some(stream) = body.get("stream").and_then(serde_json::Value::as_bool) {
        span.set_attribute("doubleword.stream", stream);
    }
}

#[derive(Clone, Debug)]
pub struct TraceParent(pub opentelemetry::trace::SpanContext);

pub async fn carry_trace_parent(request: Request, next: Next) -> Response {
    let parent = tracing::Span::current().context().span().span_context().clone();
    let mut response = next.run(request).await;
    if parent.is_valid() {
        response.extensions_mut().insert(TraceParent(parent));
    }
    response
}

pub fn record_identity_attributes(identity: &RequestIdentity, span: &tracing::Span) {
    let batch = identity.key_purpose.as_deref() == Some("batch");
    span.set_attribute("doubleword.traffic", if batch { "batch" } else { "realtime" });
    let (model, class) = identity
        .model
        .as_deref()
        .map(|name| {
            let (model, class) = split_served(name);
            (Some(model.to_owned()), Some(class.unwrap_or(STANDARD_CLASS).to_owned()))
        })
        .unwrap_or_default();
    let attrs = [
        ("doubleword.account_id", identity.account_id.clone()),
        ("doubleword.account_name", identity.account_name.clone()),
        ("doubleword.api_key_id", identity.api_key_id.map(|id| id.to_string())),
        ("doubleword.key_purpose", identity.key_purpose.clone()),
        ("gen_ai.request.model", model),
        ("doubleword.serving_class", class),
    ];
    for (key, value) in attrs {
        if let Some(value) = value {
            span.set_attribute(key, value);
        }
    }
}

pub fn record_service_caller(user: &crate::api::models::users::CurrentUser, span: &tracing::Span) {
    span.set_attribute("doubleword.account_id", user.id.to_string());
    if let Some(api_key_id) = user.api_key_id {
        span.set_attribute("doubleword.api_key_id", api_key_id.to_string());
    }
    span.set_attribute("doubleword.key_purpose", "platform");
    let roles: Vec<String> = user.roles.iter().map(|role| format!("{role:?}")).collect();
    span.set_attribute("doubleword.service_roles", roles.join(","));
}

pub fn init_telemetry(enable_otel_export: bool, log_format: LogFormat) -> anyhow::Result<Option<SdkTracerProvider>> {
    let env_filter = EnvFilter::new(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()));
    let traces_filter = std::env::var(OTEL_TRACES_FILTER).unwrap_or_else(|_| DEFAULT_OTEL_TRACES_FILTER.to_string());
    let (otel, provider, otlp_settings, otlp_error) = match enable_otel_export.then(create_otlp_tracer) {
        Some(Ok((tracer, provider, settings))) => (Some(otel_layer(tracer, &traces_filter)), Some(provider), Some(settings), None),
        Some(Err(error)) => (None, None, None, Some(error)),
        None => (None, None, None, None),
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer(log_format, std::io::stdout))
        .with(otel)
        .try_init()?;

    if log_format == LogFormat::Json {
        std::panic::set_hook(Box::new(write_panic_as_json));
    }

    if let Some(error) = otlp_error {
        return Err(error.context("Failed to set up OTLP export"));
    }
    match otlp_settings {
        Some(settings) => info!(
            service_name = settings.service_name,
            endpoint = settings.endpoint,
            custom_headers = settings.header_count,
            traces_filter,
            ?log_format,
            "Telemetry initialized with OTLP export enabled"
        ),
        None => info!(?log_format, "Telemetry initialized (OTLP export disabled)"),
    }

    Ok(provider)
}

pub(crate) fn fmt_layer<S, W>(format: LogFormat, writer: W) -> Box<dyn Layer<S> + Send + Sync>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    let layer = tracing_subscriber::fmt::layer().with_writer(RedactingMakeWriter(writer));
    match format {
        LogFormat::Compact => layer.compact().boxed(),
        LogFormat::Json => CorrelationLayer
            .and_then(
                layer
                    .json()
                    .event_format(JsonWithCorrelation(tracing_subscriber::fmt::format().json().flatten_event(true))),
            )
            .boxed(),
    }
}

const CORRELATION_FIELDS: [&str; 4] = ["x_request_id", "edge_request_id", "provider_request_id", "trace_id"];

#[derive(Default)]
struct CorrelationIds(Vec<(&'static str, String)>);

impl Visit for CorrelationIds {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.set(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.set(field, format!("{value:?}"));
    }
}

impl CorrelationIds {
    fn set(&mut self, field: &Field, value: String) {
        let Some(name) = CORRELATION_FIELDS.into_iter().find(|name| *name == field.name()) else {
            return;
        };
        match self.0.iter_mut().find(|(existing, _)| *existing == name) {
            Some((_, existing)) => *existing = value,
            None => self.0.push((name, value)),
        }
    }
}

struct CorrelationLayer;

impl<S> Layer<S> for CorrelationLayer
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut ids = CorrelationIds::default();
        attrs.record(&mut ids);
        if !ids.0.is_empty()
            && let Some(span) = ctx.span(id)
        {
            span.extensions_mut().insert(ids);
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut extensions = span.extensions_mut();
        match extensions.get_mut::<CorrelationIds>() {
            Some(ids) => values.record(ids),
            None => {
                let mut ids = CorrelationIds::default();
                values.record(&mut ids);
                if !ids.0.is_empty() {
                    extensions.insert(ids);
                }
            }
        }
    }
}

fn correlation_ids<'a, R: LookupSpan<'a> + 'a>(
    leaf_first: impl Iterator<Item = SpanRef<'a, R>>,
    skip: impl Fn(&str) -> bool,
) -> Vec<(&'static str, String)> {
    let mut found: Vec<(&'static str, String)> = Vec::new();
    for span in leaf_first {
        if let Some(ids) = span.extensions().get::<CorrelationIds>() {
            for (name, value) in &ids.0 {
                if !skip(name) && !found.iter().any(|(existing, _)| existing == name) {
                    found.push((name, value.clone()));
                }
            }
        }
    }
    found.sort_by_key(|(name, _)| CORRELATION_FIELDS.iter().position(|field| field == name));
    found
}

struct JsonWithCorrelation(Format<Json>);

impl<S, N> FormatEvent<S, N> for JsonWithCorrelation
where
    S: Subscriber + for<'span> LookupSpan<'span>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(&self, ctx: &FmtContext<'_, S, N>, mut writer: Writer<'_>, event: &Event<'_>) -> std::fmt::Result {
        let fields = event.metadata().fields();
        let ids = ctx
            .event_scope()
            .map(|scope| correlation_ids(scope, |name| fields.field(name).is_some()))
            .unwrap_or_default();
        if ids.is_empty() {
            return self.0.format_event(ctx, writer, event);
        }
        let mut line = String::new();
        self.0.format_event(ctx, Writer::new(&mut line), event)?;
        let Some(close) = line.trim_end().strip_suffix('}').map(str::len) else {
            return writer.write_str(&line);
        };
        let mut members = String::new();
        for (name, value) in ids {
            members.push(',');
            members.push_str(&Value::from(name).to_string());
            members.push(':');
            members.push_str(&Value::from(value).to_string());
        }
        line.insert_str(close, &members);
        writer.write_str(&line)
    }
}

fn write_panic_as_json(info: &PanicHookInfo<'_>) {
    let line = panic_line(info, &Backtrace::capture(), current_spans());
    let _ = writeln!(std::io::stderr().lock(), "{}", redact_secrets(&line.to_string()));
}

fn current_spans() -> Option<PanicSpans> {
    tracing::dispatcher::get_default(|dispatch| {
        let registry = dispatch.downcast_ref::<Registry>()?;
        let id = dispatch.current_span().id()?.clone();
        let span = registry.span(&id)?;
        Some(PanicSpans {
            span: span_json(&span),
            spans: span.scope().from_root().map(|span| span_json(&span)).collect(),
            correlation_ids: correlation_ids(span.scope(), |_| false),
        })
    })
}

struct PanicSpans {
    span: Value,
    spans: Vec<Value>,
    correlation_ids: Vec<(&'static str, String)>,
}

fn span_json(span: &SpanRef<'_, Registry>) -> Value {
    let mut object = span
        .extensions()
        .get::<FormattedFields<JsonFields>>()
        .and_then(|fields| serde_json::from_str::<Map<String, Value>>(fields).ok())
        .unwrap_or_default();
    object.insert("name".to_string(), span.name().into());
    Value::Object(object)
}

fn panic_line(info: &PanicHookInfo<'_>, backtrace: &Backtrace, spans: Option<PanicSpans>) -> Value {
    let payload = info
        .payload()
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
        .unwrap_or("Box<dyn Any>");
    let thread = std::thread::current();
    let thread = thread.name().unwrap_or("<unnamed>");
    let location = info.location().map(ToString::to_string);
    let message = match &location {
        Some(location) => format!("thread '{thread}' panicked at {location}: {payload}"),
        None => format!("thread '{thread}' panicked: {payload}"),
    };
    let mut line = json!({
        "timestamp": Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true),
        "level": "ERROR",
        "message": message,
        "thread": thread,
        "location": location,
        "target": "panic",
    });
    if let Some(spans) = spans {
        line["span"] = spans.span;
        line["spans"] = spans.spans.into();
        for (name, value) in spans.correlation_ids {
            line[name] = value.into();
        }
    }
    if backtrace.status() == BacktraceStatus::Captured {
        line["backtrace"] = backtrace.to_string().into();
    }
    line
}

fn otel_layer<S>(tracer: Tracer, directives: &str) -> impl Layer<S>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_location(false)
        .with_threads(false)
        .with_tracked_inactivity(false)
        .with_filter(EnvFilter::new(directives))
}

fn create_otlp_tracer() -> anyhow::Result<(Tracer, SdkTracerProvider, OtlpSettings)> {
    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "dwctl".to_string());

    let base = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_else(|_| "http://localhost:4318".to_string());
    let endpoint = if base.ends_with("/v1/traces") {
        base
    } else {
        format!("{}/v1/traces", base.trim_end_matches('/'))
    };

    let mut headers = HashMap::new();
    if let Ok(headers_str) = std::env::var("OTEL_EXPORTER_OTLP_HEADERS") {
        let decoded = headers_str.replace("%20", " ");
        for pair in decoded.split(',') {
            if let Some((key, value)) = pair.split_once('=') {
                let key = key.trim().to_string();
                let value = value.trim().to_string();
                headers.insert(key, value);
            }
        }
    }

    let headers_len = headers.len();

    let protocol = match std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL").as_deref().unwrap_or("http/protobuf") {
        "http/protobuf" => Protocol::HttpBinary,
        "http/json" => Protocol::HttpJson,
        _ => Protocol::HttpBinary,
    };

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(&endpoint)
        .with_protocol(protocol)
        .with_headers(headers)
        .build()?;

    let tracer_provider = tracer_provider(exporter, &service_name);
    let tracer = tracer_provider.tracer(service_name.clone());
    let settings = OtlpSettings {
        service_name,
        endpoint,
        header_count: headers_len,
    };

    Ok((tracer, tracer_provider, settings))
}

fn tracer_provider(exporter: impl SpanExporter + 'static, service_name: &str) -> SdkTracerProvider {
    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name(service_name.to_string())
        .build();
    SdkTracerProvider::builder()
        .with_batch_exporter(RedactingSpanExporter(exporter))
        .with_resource(resource)
        .build()
}

struct OtlpSettings {
    service_name: String,
    endpoint: String,
    header_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(body: serde_json::Value) -> impl Fn(&str) -> Option<String> {
        let (spans, _) = crate::request_logging::json_output::testing::recorded(|| {
            let span = tracing::info_span!("shape");
            record_request_shape(&body, &span);
        });
        move |key| crate::request_logging::json_output::testing::attribute(&spans, "shape", key)
    }

    #[test]
    fn a_request_shape_is_recorded_as_bounded_labels() {
        let chat = shape(serde_json::json!({
            "max_tokens": 100, "max_completion_tokens": 512, "stream": false,
            "response_format": {"type": "json_object"}, "tool_choice": "required", "tools": [{}, {}],
        }));
        assert_eq!(chat("doubleword.response_format").as_deref(), Some("json_object"));
        assert_eq!(chat("doubleword.tool_choice").as_deref(), Some("required"));
        assert_eq!(chat("doubleword.tool_count").as_deref(), Some("2"));
        assert_eq!(
            chat("gen_ai.request.max_tokens").as_deref(),
            Some("512"),
            "max_completion_tokens wins"
        );
        assert_eq!(chat("doubleword.stream").as_deref(), Some("false"));

        let responses = shape(serde_json::json!({"text": {"format": {"type": "json_schema"}}, "max_output_tokens": 64}));
        assert_eq!(responses("doubleword.response_format").as_deref(), Some("json_schema"));
        assert_eq!(responses("gen_ai.request.max_tokens").as_deref(), Some("64"));

        let plain = shape(serde_json::json!({"response_format": {"type": "private-grammar"}}));
        assert_eq!(plain("doubleword.response_format").as_deref(), Some("other"));
        assert_eq!(plain("doubleword.tool_choice"), None);
        assert_eq!(plain("doubleword.tool_count"), None);
    }

    #[test]
    fn schema_validity_is_recorded_as_bounded_labels() {
        let valid = serde_json::json!({"type": "object", "properties": {"private-property": {"type": "string"}}});
        let recursive = serde_json::json!({
            "$defs": {"private-node": {"type": "object", "properties": {"next": {"$ref": "#/$defs/private-node"}}}},
            "$ref": "#/$defs/private-node",
        });
        let meta_invalid = serde_json::json!({"type": "object", "required": "private-property"});
        let unresolved = serde_json::json!({"type": "object", "properties": {"private-property": {"$ref": "#/$defs/private-missing"}}});

        let schema_case = |schema: &serde_json::Value| {
            shape(serde_json::json!({"response_format": {"type": "json_schema", "json_schema": {"name": "private", "schema": schema}}}))
        };
        for (name, schema, error) in [
            ("valid", &valid, None),
            ("recursive", &recursive, None),
            ("meta-schema invalid", &meta_invalid, Some("meta_schema")),
            ("unresolved $ref", &unresolved, Some("unresolved_ref")),
        ] {
            let span = schema_case(schema);
            let valid = error.is_none().to_string();
            assert_eq!(
                span("doubleword.response_format.schema_valid").as_deref(),
                Some(valid.as_str()),
                "{name}"
            );
            assert_eq!(span("doubleword.response_format.schema_error").as_deref(), error, "{name}");
        }

        let responses = shape(serde_json::json!({"text": {"format": {"type": "json_schema", "name": "private", "schema": unresolved}}}));
        assert_eq!(
            responses("doubleword.response_format.schema_error").as_deref(),
            Some("unresolved_ref")
        );
        let anthropic = shape(serde_json::json!({"output_config": {"format": {"type": "json_schema", "schema": meta_invalid}}}));
        assert_eq!(anthropic("doubleword.response_format").as_deref(), Some("json_schema"));
        assert_eq!(anthropic("doubleword.response_format.schema_error").as_deref(), Some("meta_schema"));
        let missing = shape(serde_json::json!({"response_format": {"type": "json_schema", "json_schema": {"name": "private"}}}));
        assert_eq!(missing("doubleword.response_format.schema_valid").as_deref(), Some("false"));
        assert_eq!(missing("doubleword.response_format.schema_error").as_deref(), Some("missing"));

        let tools = shape(serde_json::json!({"tools": [
            {"type": "function", "function": {"name": "private-a", "parameters": valid}},
            {"type": "function", "function": {"name": "private-b", "parameters": unresolved}},
            {"type": "function", "name": "private-c", "parameters": meta_invalid},
            {"name": "private-d", "input_schema": recursive},
            {"type": "function", "function": {"name": "private-e"}},
            {"type": "web_search"},
        ]}));
        assert_eq!(tools("doubleword.tool_count").as_deref(), Some("6"));
        assert_eq!(tools("doubleword.tool_schemas.invalid_count").as_deref(), Some("2"));
        assert_eq!(tools("doubleword.tool_schemas.first_error").as_deref(), Some("unresolved_ref"));
        let all_valid = shape(serde_json::json!({"tools": [{"type": "function", "function": {"name": "private", "parameters": valid}}]}));
        assert_eq!(all_valid("doubleword.tool_schemas.invalid_count").as_deref(), Some("0"));
        assert_eq!(all_valid("doubleword.tool_schemas.first_error"), None);
        assert_eq!(all_valid("doubleword.tool_schemas.unchecked_count"), None);

        let costly = serde_json::json!({"type": "string", "description": "a".repeat(1 << 20)});
        let over_budget = shape(serde_json::json!({
            "response_format": {"type": "json_schema", "json_schema": {"name": "private", "schema": costly}},
            "tools": [{"type": "function", "function": {"name": "private", "parameters": costly}}, {"type": "function", "function": {"name": "private", "parameters": unresolved}}],
        }));
        assert_eq!(over_budget("doubleword.response_format.schema_valid"), None);
        assert_eq!(over_budget("doubleword.response_format.schema_error").as_deref(), Some("unchecked"));
        assert_eq!(over_budget("doubleword.tool_schemas.unchecked_count").as_deref(), Some("1"));
        assert_eq!(over_budget("doubleword.tool_schemas.invalid_count").as_deref(), Some("1"));
        assert_eq!(
            over_budget("doubleword.tool_schemas.first_error").as_deref(),
            Some("unresolved_ref")
        );

        let plain = shape(serde_json::json!({"response_format": {"type": "json_object"}}));
        for key in [
            "doubleword.response_format.schema_valid",
            "doubleword.response_format.schema_error",
            "doubleword.tool_schemas.invalid_count",
            "doubleword.tool_schemas.first_error",
            "doubleword.tool_schemas.unchecked_count",
        ] {
            assert_eq!(plain(key), None, "{key}");
        }
    }
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::trace::SpanData;
    use std::sync::{Arc, Mutex};
    use tracing::Instrument;

    #[derive(Clone, Debug, Default)]
    struct Exported(Arc<Mutex<Vec<SpanData>>>);

    impl SpanExporter for Exported {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            self.0.lock().unwrap().extend(batch);
            Ok(())
        }
    }

    #[tracing::instrument(level = "debug", err)]
    async fn repository_method() -> Result<(), &'static str> {
        Err("connection reset")
    }

    #[tokio::test]
    async fn exporter_filter_is_independent_of_the_log_filter() {
        let exported = Exported::default();
        let provider = SdkTracerProvider::builder().with_simple_exporter(exported.clone()).build();
        let subscriber = tracing_subscriber::registry()
            .with(EnvFilter::new("debug"))
            .with(otel_layer(provider.tracer("test"), "info"));

        let _guard = tracing::subscriber::set_default(subscriber);
        async {
            tracing::debug!(db.statement = "SELECT 1", "query");
            tracing::info!("finished");
            let _ = repository_method().await;
        }
        .instrument(tracing::info_span!("request"))
        .await;
        drop(_guard);
        provider.shutdown().unwrap();

        let spans = exported.0.lock().unwrap();
        let names: Vec<&str> = spans.iter().map(|span| span.name.as_ref()).collect();
        assert_eq!(names, ["request"]);

        let request = &spans[0];
        let events: Vec<(&str, Option<String>)> = request
            .events
            .iter()
            .map(|event| {
                let message = event
                    .attributes
                    .iter()
                    .find(|kv| kv.key.as_str() == "exception.message")
                    .map(|kv| kv.value.to_string());
                (event.name.as_ref(), message)
            })
            .collect();
        assert_eq!(
            events,
            [("finished", None), ("exception", Some("connection reset".to_string()))],
            "the repository method's error event must land on the request span: {:?}",
            request.events
        );

        let keys: Vec<&str> = request.attributes.iter().map(|kv| kv.key.as_str()).collect();
        for extra in [
            "code.file.path",
            "code.line.number",
            "code.module.name",
            "thread.id",
            "thread.name",
            "busy_ns",
            "idle_ns",
        ] {
            assert!(!keys.contains(&extra), "{extra} exported: {keys:?}");
        }
    }

    #[test]
    fn span_account_name_falls_back_and_is_bounded() {
        assert_eq!(span_account_name(Some(" Example Co "), "user"), "Example Co");
        assert_eq!(span_account_name(Some("   "), "user"), "user");
        assert_eq!(span_account_name(None, "user"), "user");
        let long = "é".repeat(MAX_SPAN_ACCOUNT_NAME_CHARS + 10);
        assert_eq!(span_account_name(Some(&long), "user").chars().count(), MAX_SPAN_ACCOUNT_NAME_CHARS);
    }

    #[test]
    fn otlp_tracer_builds_with_http_client() {
        let (_, provider, _) = create_otlp_tracer().expect(
            "OTLP tracer failed to build - likely a feature flag conflict \
             (reqwest-client vs reqwest-blocking-client)",
        );
        provider.shutdown().ok();
    }

    #[tokio::test]
    async fn otlp_reqwest_supports_https() {
        let client = reqwest::Client::new();
        let result = client.get("https://example.com").send().await;
        assert!(
            result.is_ok(),
            "reqwest cannot make HTTPS requests — TLS not configured: {}",
            result.unwrap_err()
        );
    }

    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Captured {
        fn lines(&self) -> Vec<String> {
            String::from_utf8(self.0.lock().unwrap().clone())
                .unwrap()
                .lines()
                .map(str::to_string)
                .collect()
        }
    }

    const KEY: &str = "sk-AbCdEfGhIjKlMnOpQrStUvWxYz0123456789_-AbCdE";

    #[derive(Debug)]
    #[allow(dead_code, reason = "read only through Debug")]
    struct LeakyInput {
        file_id: u32,
        api_key: String,
    }

    #[tracing::instrument(level = "debug")]
    fn create_record(input: LeakyInput) {
        tracing::warn!(authorization = %format!("Bearer {KEY}"), "upstream rejected the request");
        tracing::error!(error = %format!("invalid key {KEY}"), "create failed");
    }

    #[tokio::test]
    async fn credentials_are_redacted_from_logs_and_exported_spans() {
        let exported = Exported::default();
        let provider = tracer_provider(exported.clone(), "test");
        let json = Captured::default();
        let compact = Captured::default();
        let (json_writer, compact_writer) = (json.clone(), compact.clone());
        let subscriber = tracing_subscriber::registry()
            .with(EnvFilter::new("debug"))
            .with(fmt_layer(LogFormat::Json, move || json_writer.clone()))
            .with(fmt_layer(LogFormat::Compact, move || compact_writer.clone()))
            .with(otel_layer(provider.tracer("test"), "debug"));

        tracing::subscriber::with_default(subscriber, || {
            create_record(LeakyInput {
                file_id: 7,
                api_key: KEY.to_string(),
            });
        });
        provider.shutdown().unwrap();

        for (format, lines) in [("json", json.lines()), ("compact", compact.lines())] {
            assert_eq!(lines.len(), 2, "{format}: {lines:?}");
            for line in &lines {
                assert!(!line.contains(KEY), "{format} line leaked the key: {line}");
                assert!(line.contains("<redacted>"), "{format}: {line}");
            }
        }

        let spans = exported.0.lock().unwrap();
        assert_eq!(spans.len(), 1, "{spans:?}");
        let span = format!("{:?}", spans[0]);
        assert!(!span.contains(KEY), "exported span leaked the key: {span}");
        assert!(span.contains("file_id: 7"), "the rest of the input is still recorded: {span}");
    }

    #[test]
    fn json_events_carry_their_spans_request_ids_at_the_top_level() {
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::registry()
            .with(EnvFilter::new("info"))
            .with(fmt_layer(LogFormat::Json, move || writer.clone()));
        tracing::subscriber::with_default(subscriber, || {
            let request = tracing::info_span!("request", x_request_id = tracing::field::Empty, trace_id = "trace-1");
            request.record("x_request_id", tracing::field::display("req-outer"));
            request.in_scope(|| {
                tracing::info_span!("handler", edge_request_id = "edge-1").in_scope(|| {
                    tracing::info!("in request");
                    tracing::info_span!("retry", x_request_id = "req-inner").in_scope(|| tracing::info!("in retry"));
                    tracing::info!(x_request_id = "own", "sets its own");
                });
            });
            tracing::info!("outside");
        });

        let lines = captured.lines();
        let parsed: Vec<serde_json::Value> = lines.iter().map(|line| serde_json::from_str(line).unwrap()).collect();
        let [in_request, in_retry, own, outside] = parsed.as_slice() else {
            panic!("expected four lines: {lines:?}");
        };
        assert_eq!(in_request["x_request_id"], "req-outer");
        assert_eq!(in_request["edge_request_id"], "edge-1");
        assert_eq!(in_request["trace_id"], "trace-1");
        assert_eq!(in_request["spans"][0]["x_request_id"], "req-outer", "the span list is unchanged");
        assert_eq!(in_retry["x_request_id"], "req-inner");
        assert_eq!(own["x_request_id"], "own");
        assert_eq!(
            lines[2].matches(r#""x_request_id":"own""#).count(),
            1,
            "the event's own field is not repeated: {}",
            lines[2]
        );
        assert_eq!(own["edge_request_id"], "edge-1");
        assert!(outside.get("x_request_id").is_none(), "{outside}");
    }

    #[test]
    fn json_logging_child() {
        if std::env::var_os(JSON_LOGGING_CHILD).is_none() {
            return;
        }
        let provider = init_telemetry(true, LogFormat::Json).unwrap();
        assert!(provider.is_some());
        tracing::info_span!("request").in_scope(|| {
            tracing::error!(error = "first line\nsecond line", request_model = Some("model-a"), "tracing error");
        });
        log::error!("log crate error");
        let _ = std::thread::spawn(|| {
            tracing::info_span!("request", request_id = 7, x_request_id = "req-1").in_scope(|| {
                tracing::info_span!("worker").in_scope(|| panic!("worker panic"));
            });
        })
        .join();
    }

    const JSON_LOGGING_CHILD: &str = "DWCTL_JSON_LOGGING_TEST_CHILD";

    #[test]
    fn json_logging_writes_every_error_as_one_json_line() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "telemetry::tests::json_logging_child", "--nocapture"])
            .env(JSON_LOGGING_CHILD, "1")
            .env("RUST_LOG", "info")
            .env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")
            .output()
            .expect("start json logging child");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "json logging child failed:\n{stdout}\n{stderr}");

        let lines: Vec<serde_json::Value> = stdout
            .lines()
            .chain(stderr.lines())
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();
        let find = |message: &str| {
            lines
                .iter()
                .find(|line| line["message"].as_str().is_some_and(|m| m.contains(message)))
                .unwrap_or_else(|| panic!("no JSON line for {message:?}:\n{stdout}\n{stderr}"))
        };

        let tracing_error = find("tracing error");
        assert_eq!(tracing_error["level"], "ERROR");
        assert_eq!(tracing_error["error"], "first line\nsecond line");
        assert_eq!(tracing_error["request_model"], "model-a");
        assert_eq!(tracing_error["spans"][0]["name"], "request");

        assert_eq!(find("log crate error")["level"], "ERROR");

        let panic = find("worker panic");
        assert_eq!(panic["level"], "ERROR");
        assert_eq!(panic["target"], "panic");
        assert!(
            panic["message"].as_str().unwrap().contains("panicked at"),
            "panic message keeps the default wording: {panic}"
        );
        assert_eq!(panic["span"]["name"], "worker");
        assert_eq!(panic["spans"][0]["name"], "request");
        assert_eq!(panic["spans"][0]["request_id"], 7);
        assert_eq!(panic["spans"][1]["name"], "worker");
        assert_eq!(panic["x_request_id"], "req-1");

        let initialized = find("OTLP export enabled");
        assert_eq!(initialized["endpoint"], "http://127.0.0.1:9/v1/traces");
        assert!(
            !stdout.contains("[OTLP]") && !stderr.contains("[OTLP]"),
            "OTLP setup is logged as an event, not printed: {stderr}"
        );
    }
}
