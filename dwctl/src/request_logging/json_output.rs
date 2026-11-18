
use metrics::Label;
use opentelemetry::trace::{SpanContext, TraceContextExt};
use opentelemetry::{Array, StringValue, Value};
use serde::de::IgnoredAny;
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonOutcome {
    Valid,
    Missing,
    Empty,
    InvalidJson,
    NotObject,
}

impl JsonOutcome {
    pub const ALL: [Self; 5] = [Self::Valid, Self::Missing, Self::Empty, Self::InvalidJson, Self::NotObject];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::Missing => "missing",
            Self::Empty => "empty",
            Self::InvalidJson => "invalid_json",
            Self::NotObject => "not_object",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseErrorPosition {
    pub line: usize,
    pub column: usize,
    pub category: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonCheck {
    pub outcome: JsonOutcome,
    pub length: usize,
    pub error: Option<ParseErrorPosition>,
}

impl JsonCheck {
    pub fn missing() -> Self {
        Self {
            outcome: JsonOutcome::Missing,
            length: 0,
            error: None,
        }
    }

    fn of(outcome: JsonOutcome, length: usize) -> Self {
        Self {
            outcome,
            length,
            error: None,
        }
    }
}

pub fn check_json_object(text: &str) -> JsonCheck {
    let length = text.len();
    if text.trim().is_empty() {
        return JsonCheck::of(JsonOutcome::Empty, length);
    }
    match serde_json::from_str::<IgnoredAny>(text) {
        Ok(_) if text.trim_start().starts_with('{') => JsonCheck::of(JsonOutcome::Valid, length),
        Ok(_) => JsonCheck::of(JsonOutcome::NotObject, length),
        Err(error) => JsonCheck {
            outcome: JsonOutcome::InvalidJson,
            length,
            error: Some(ParseErrorPosition {
                line: error.line(),
                column: error.column(),
                category: match error.classify() {
                    serde_json::error::Category::Syntax => "syntax",
                    serde_json::error::Category::Eof => "eof",
                    serde_json::error::Category::Data => "data",
                    serde_json::error::Category::Io => "io",
                },
            }),
        },
    }
}

pub fn failures(checks: &[JsonCheck]) -> usize {
    checks.iter().filter(|check| check.outcome != JsonOutcome::Valid).count()
}

pub fn finish_reason_label(reason: &str) -> &'static str {
    match reason {
        "stop" => "stop",
        "length" => "length",
        "tool_calls" => "tool_calls",
        "content_filter" => "content_filter",
        "function_call" => "function_call",
        "error" => "error",
        _ => "other",
    }
}

pub(crate) fn count(name: &'static str, labels: &[(&'static str, String)], outcome: JsonOutcome) {
    for candidate in JsonOutcome::ALL {
        let labels: Vec<Label> = labels
            .iter()
            .map(|(key, value)| Label::new(*key, value.clone()))
            .chain([Label::new("outcome", candidate.as_str())])
            .collect();
        metrics::counter!(name, labels).increment(u64::from(candidate == outcome));
    }
}

#[derive(Debug, Default)]
pub struct OutputContext<'a> {
    pub model: Option<&'a str>,
    pub served_by: Option<&'a str>,
    pub max_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
    pub parent: Option<&'a SpanContext>,
}

impl OutputContext<'_> {
    pub fn model(&self) -> String {
        self.model.unwrap_or_default().to_string()
    }

    pub(crate) fn span(
        &self,
        name: &'static str,
        prefix: &str,
        lengths: &str,
        checks: &[JsonCheck],
        stream: bool,
        finish_reasons: &[&'static str],
    ) -> tracing::Span {
        let span = tracing::info_span!("dwctl.output_check", otel.name = name, otel.status_code = tracing::field::Empty);
        if let Some(parent) = self.parent {
            let _ = span.set_parent(opentelemetry::Context::new().with_remote_span_context(parent.clone()));
        }
        span.set_attribute("gen_ai.request.model", self.model());
        if let Some(max_tokens) = self.max_tokens {
            span.set_attribute("gen_ai.request.max_tokens", max_tokens);
        }
        if let Some(tokens) = self.output_tokens {
            span.set_attribute("gen_ai.usage.output_tokens", tokens);
        }
        if let Some(tokens) = self.reasoning_tokens {
            span.set_attribute("gen_ai.usage.output_tokens.reasoning", tokens);
        }
        span.set_attribute(
            "gen_ai.response.finish_reasons",
            Value::Array(Array::String(finish_reasons.iter().map(|&r| StringValue::from(r)).collect())),
        );
        span.set_attribute(format!("{prefix}.count"), checks.len() as i64);
        span.set_attribute(format!("{prefix}.failed"), failures(checks) as i64);
        span.set_attribute(
            format!("{prefix}.outcomes"),
            Value::Array(Array::String(
                checks.iter().map(|c| StringValue::from(c.outcome.as_str())).collect(),
            )),
        );
        span.set_attribute(
            format!("{prefix}.{lengths}"),
            Value::Array(Array::I64(
                checks.iter().map(|c| i64::try_from(c.length).unwrap_or(i64::MAX)).collect(),
            )),
        );
        if let Some(error) = first_error(checks) {
            span.set_attribute(format!("{prefix}.first_error.line"), error.line as i64);
            span.set_attribute(format!("{prefix}.first_error.column"), error.column as i64);
            span.set_attribute(format!("{prefix}.first_error.category"), error.category);
        }
        span.set_attribute("doubleword.stream", stream);
        if let Some(served_by) = self.served_by {
            span.set_attribute("doubleword.served_by", served_by.to_string());
        }
        if failures(checks) > 0 {
            span.record("otel.status_code", "ERROR");
        }
        span
    }
}

pub fn first_error(checks: &[JsonCheck]) -> Option<ParseErrorPosition> {
    checks.iter().find_map(|check| check.error)
}

pub(crate) fn stream_label(stream: bool) -> &'static str {
    if stream { "true" } else { "false" }
}

#[cfg(test)]
pub(crate) mod testing {

    use opentelemetry::trace::{SpanContext, TraceFlags, TracerProvider as _};
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Clone, Debug, Default)]
    struct Exported(Arc<Mutex<Vec<SpanData>>>);

    impl SpanExporter for Exported {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            self.0.lock().unwrap().extend(batch);
            Ok(())
        }
    }

    pub fn recorded(record: impl FnOnce()) -> (Vec<SpanData>, String) {
        let exporter = Exported::default();
        let provider = SdkTracerProvider::builder().with_simple_exporter(exporter.clone()).build();
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(provider.tracer("output-checks-test")));
        let prometheus = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = prometheus.handle();
        metrics::with_local_recorder(&prometheus, || tracing::subscriber::with_default(subscriber, record));
        provider.force_flush().unwrap();
        let spans = exporter.0.lock().unwrap().clone();
        (spans, handle.render())
    }

    pub fn parent(flags: TraceFlags) -> SpanContext {
        SpanContext::new(
            opentelemetry::trace::TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
            opentelemetry::trace::SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            flags,
            false,
            Default::default(),
        )
    }

    pub fn attribute(spans: &[SpanData], name: &str, key: &str) -> Option<String> {
        spans
            .iter()
            .find(|span| span.name == name)
            .expect("span exported")
            .attributes
            .iter()
            .find(|a| a.key.as_str() == key)
            .map(|a| a.value.to_string())
    }

    pub fn counter(rendered: &str, name: &str, labels: &[&str]) -> Option<u64> {
        rendered
            .lines()
            .filter(|line| line.starts_with(name) && !line.starts_with('#'))
            .find(|line| labels.iter().all(|label| line.contains(label)))
            .and_then(|line| line.rsplit(' ').next()?.parse().ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_is_classified() {
        assert_eq!(check_json_object(r#"{"city": "Paris"}"#).outcome, JsonOutcome::Valid);
        assert_eq!(check_json_object(" {}\n").outcome, JsonOutcome::Valid);
        assert_eq!(check_json_object("").outcome, JsonOutcome::Empty);
        assert_eq!(check_json_object(r#""Paris""#).outcome, JsonOutcome::NotObject);
        assert_eq!(check_json_object("[1]").outcome, JsonOutcome::NotObject);
        assert_eq!(check_json_object("null").outcome, JsonOutcome::NotObject);

        let truncated = check_json_object(r#"{"city": "Par"#);
        assert_eq!(truncated.outcome, JsonOutcome::InvalidJson);
        assert_eq!(truncated.length, 13);
        assert_eq!(truncated.error.map(|error| error.category), Some("eof"));

        let trailing = check_json_object(r#"{"a": 1}{"a": 2}"#);
        assert_eq!(trailing.outcome, JsonOutcome::InvalidJson);
        assert_eq!(
            trailing.error,
            Some(ParseErrorPosition {
                line: 1,
                column: 9,
                category: "syntax"
            })
        );
    }

    #[test]
    fn a_finish_reason_is_bounded() {
        assert_eq!(finish_reason_label("length"), "length");
        assert_eq!(finish_reason_label("private-value-7f3a"), "other");
    }

    #[test]
    fn counting_registers_every_outcome_at_zero() {
        let (_, rendered) = testing::recorded(|| count("test_total", &[("model", "m".to_string())], JsonOutcome::Valid));
        assert_eq!(testing::counter(&rendered, "test_total", &[r#"outcome="valid""#]), Some(1));
        for outcome in ["missing", "empty", "invalid_json", "not_object"] {
            let label = format!(r#"outcome="{outcome}""#);
            assert_eq!(
                testing::counter(&rendered, "test_total", &[&label, r#"model="m""#]),
                Some(0),
                "{rendered}"
            );
        }
    }
}
