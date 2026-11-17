
use std::collections::BTreeMap;

use opentelemetry::trace::{SpanContext, TraceContextExt};
use opentelemetry::{Array, StringValue, Value};
use serde::de::IgnoredAny;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::request_logging::models::{AiResponse, ChatCompletionChunk};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgumentsOutcome {
    Valid,
    Empty,
    InvalidJson,
    NotObject,
}

impl ArgumentsOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "valid",
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
pub struct ToolCallCheck {
    pub outcome: ArgumentsOutcome,
    pub length: usize,
    pub error: Option<ParseErrorPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallReport {
    pub calls: Vec<ToolCallCheck>,
    pub stream: bool,
    pub finish_reasons: Vec<&'static str>,
}

impl ToolCallReport {
    pub fn failures(&self) -> usize {
        self.calls.iter().filter(|call| call.outcome != ArgumentsOutcome::Valid).count()
    }
}

pub fn check_arguments(arguments: &str) -> ToolCallCheck {
    let length = arguments.len();
    if arguments.trim().is_empty() {
        return ToolCallCheck {
            outcome: ArgumentsOutcome::Empty,
            length,
            error: None,
        };
    }
    match serde_json::from_str::<IgnoredAny>(arguments) {
        Ok(_) if arguments.trim_start().starts_with('{') => ToolCallCheck {
            outcome: ArgumentsOutcome::Valid,
            length,
            error: None,
        },
        Ok(_) => ToolCallCheck {
            outcome: ArgumentsOutcome::NotObject,
            length,
            error: None,
        },
        Err(error) => ToolCallCheck {
            outcome: ArgumentsOutcome::InvalidJson,
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

pub fn inspect(response: &AiResponse) -> Option<ToolCallReport> {
    let report = match response {
        AiResponse::ChatCompletions(completion) => {
            let mut calls = Vec::new();
            let mut finish_reasons = Vec::new();
            for choice in &completion.choices {
                let Some(tool_calls) = choice.message.tool_calls.as_ref().filter(|calls| !calls.is_empty()) else {
                    continue;
                };
                calls.extend(tool_calls.iter().map(|call| check_arguments(&call.function.arguments)));
                finish_reasons.extend(choice.finish_reason.as_deref().map(finish_reason_label));
            }
            ToolCallReport {
                calls,
                stream: false,
                finish_reasons,
            }
        }
        AiResponse::ChatCompletionsStream(chunks) => {
            let mut arguments: BTreeMap<(u32, u32), String> = BTreeMap::new();
            let mut finish_reasons: BTreeMap<u32, &'static str> = BTreeMap::new();
            for chunk in chunks {
                let ChatCompletionChunk::Chunk(chunk) = chunk else {
                    continue;
                };
                for choice in &chunk.choices {
                    for call in choice.delta.tool_calls.iter().flatten() {
                        let fragment = call.function.as_ref().and_then(|function| function.arguments.as_deref());
                        arguments
                            .entry((choice.index, call.index))
                            .or_default()
                            .push_str(fragment.unwrap_or_default());
                    }
                    if let Some(reason) = &choice.finish_reason {
                        finish_reasons.insert(choice.index, finish_reason_label(reason));
                    }
                }
            }
            let choices_with_calls: Vec<u32> = arguments.keys().map(|(choice, _)| *choice).collect();
            ToolCallReport {
                calls: arguments.values().map(|arguments| check_arguments(arguments)).collect(),
                stream: true,
                finish_reasons: finish_reasons
                    .into_iter()
                    .filter(|(choice, _)| choices_with_calls.contains(choice))
                    .map(|(_, reason)| reason)
                    .collect(),
            }
        }
        _ => return None,
    };
    (!report.calls.is_empty()).then_some(report)
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

pub fn tool_choice_mode(tool_choice: &serde_json::Value) -> Option<&'static str> {
    match tool_choice {
        serde_json::Value::Null => None,
        serde_json::Value::String(mode) => Some(named_tool_choice_mode(mode)),
        serde_json::Value::Object(choice) => Some(match choice.get("type").and_then(serde_json::Value::as_str) {
            Some("function" | "tool") => "function",
            Some("auto") => "auto",
            Some("any") => "required",
            Some("none") => "none",
            _ => "other",
        }),
        _ => Some("other"),
    }
}

pub fn named_tool_choice_mode(mode: &str) -> &'static str {
    match mode {
        "auto" => "auto",
        "none" => "none",
        "required" => "required",
        _ => "other",
    }
}

#[derive(Debug, Default)]
pub struct ToolCallContext<'a> {
    pub model: Option<&'a str>,
    pub tool_choice: Option<&'a str>,
    pub served_by: Option<&'a str>,
    pub parent: Option<&'a SpanContext>,
}

pub fn record(report: &ToolCallReport, context: &ToolCallContext<'_>) {
    let model = context.model.unwrap_or_default().to_string();
    let tool_choice = context.tool_choice.unwrap_or("unset");
    let stream = if report.stream { "true" } else { "false" };
    for call in &report.calls {
        metrics::counter!(
            "dwctl_tool_call_arguments_total",
            "model" => model.clone(),
            "outcome" => call.outcome.as_str(),
            "tool_choice" => tool_choice.to_string(),
            "stream" => stream,
        )
        .increment(1);
    }

    let span = tracing::info_span!("dwctl.tool_call_arguments", otel.name = "dwctl.tool_call_arguments");
    if let Some(parent) = context.parent {
        let _ = span.set_parent(opentelemetry::Context::new().with_remote_span_context(parent.clone()));
    }
    let failed = report.failures();
    let first_error = report.calls.iter().find_map(|call| call.error);
    span.set_attribute("gen_ai.request.model", model.clone());
    span.set_attribute(
        "gen_ai.response.finish_reasons",
        Value::Array(Array::String(
            report.finish_reasons.iter().map(|&reason| StringValue::from(reason)).collect(),
        )),
    );
    span.set_attribute("doubleword.tool_calls.count", report.calls.len() as i64);
    span.set_attribute("doubleword.tool_calls.failed", failed as i64);
    span.set_attribute(
        "doubleword.tool_calls.outcomes",
        Value::Array(Array::String(
            report.calls.iter().map(|call| StringValue::from(call.outcome.as_str())).collect(),
        )),
    );
    span.set_attribute(
        "doubleword.tool_calls.argument_lengths",
        Value::Array(Array::I64(
            report
                .calls
                .iter()
                .map(|call| i64::try_from(call.length).unwrap_or(i64::MAX))
                .collect(),
        )),
    );
    if let Some(error) = first_error {
        span.set_attribute("doubleword.tool_calls.first_error.line", error.line as i64);
        span.set_attribute("doubleword.tool_calls.first_error.column", error.column as i64);
        span.set_attribute("doubleword.tool_calls.first_error.category", error.category);
    }
    span.set_attribute("doubleword.tool_choice", tool_choice.to_string());
    span.set_attribute("doubleword.stream", report.stream);
    if let Some(served_by) = context.served_by {
        span.set_attribute("doubleword.served_by", served_by.to_string());
    }
    if failed > 0 {
        span.set_attribute("otel.status_code", "ERROR");
        let _entered = span.enter();
        tracing::warn!(
            model = %model,
            tool_calls = report.calls.len(),
            failed,
            tool_choice,
            stream = report.stream,
            served_by = context.served_by,
            first_error_line = first_error.map(|error| error.line),
            first_error_column = first_error.map(|error| error.column),
            first_error_category = first_error.map(|error| error.category),
            "Tool call arguments are not a valid JSON object"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn chat(tool_calls: serde_json::Value) -> AiResponse {
        serde_json::from_value(json!({
            "id": "chatcmpl-1", "object": "chat.completion", "created": 1, "model": "model-a",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": null, "tool_calls": tool_calls},
                "finish_reason": "tool_calls"
            }]
        }))
        .unwrap()
    }

    fn call(arguments: &str) -> serde_json::Value {
        json!({"id": "call_1", "type": "function", "function": {"name": "lookup", "arguments": arguments}})
    }

    fn stream(deltas: &[serde_json::Value], finish_reason: &str) -> AiResponse {
        let mut chunks: Vec<ChatCompletionChunk> = deltas
            .iter()
            .map(|delta| {
                serde_json::from_value(json!({
                    "id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 1, "model": "model-a",
                    "choices": [{"index": 0, "delta": delta}]
                }))
                .unwrap()
            })
            .collect();
        chunks.push(
            serde_json::from_value(json!({
                "id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 1, "model": "model-a",
                "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}]
            }))
            .unwrap(),
        );
        AiResponse::ChatCompletionsStream(chunks)
    }

    fn fragment(index: u32, arguments: &str) -> serde_json::Value {
        json!({"tool_calls": [{"index": index, "function": {"arguments": arguments}}]})
    }

    #[test]
    fn arguments_are_classified() {
        assert_eq!(check_arguments(r#"{"city": "Paris"}"#).outcome, ArgumentsOutcome::Valid);
        assert_eq!(check_arguments(" {}\n").outcome, ArgumentsOutcome::Valid);
        assert_eq!(check_arguments("").outcome, ArgumentsOutcome::Empty);
        assert_eq!(check_arguments(r#""Paris""#).outcome, ArgumentsOutcome::NotObject);
        assert_eq!(check_arguments("[1]").outcome, ArgumentsOutcome::NotObject);
        assert_eq!(check_arguments("null").outcome, ArgumentsOutcome::NotObject);

        let truncated = check_arguments(r#"{"city": "Par"#);
        assert_eq!(truncated.outcome, ArgumentsOutcome::InvalidJson);
        assert_eq!(truncated.length, 13);
        assert_eq!(truncated.error.map(|error| error.category), Some("eof"));

        let trailing = check_arguments(r#"{"a": 1}{"a": 2}"#);
        assert_eq!(trailing.outcome, ArgumentsOutcome::InvalidJson);
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
    fn non_streaming_tool_calls_are_checked_in_order() {
        let report = inspect(&chat(json!([call(r#"{"q": 1}"#), call(r#"{"q": "#)]))).unwrap();
        assert!(!report.stream);
        assert_eq!(
            report.calls.iter().map(|call| call.outcome).collect::<Vec<_>>(),
            [ArgumentsOutcome::Valid, ArgumentsOutcome::InvalidJson]
        );
        assert_eq!(report.failures(), 1);
        assert_eq!(report.finish_reasons, ["tool_calls"]);
    }

    #[test]
    fn a_stream_is_checked_after_joining_its_fragments() {
        let response = stream(
            &[
                json!({"tool_calls": [{"index": 0, "id": "call_1", "type": "function", "function": {"name": "lookup", "arguments": ""}}]}),
                fragment(0, r#"{"city""#),
                fragment(0, r#": "Paris"}"#),
                json!({"tool_calls": [{"index": 1, "id": "call_2", "type": "function", "function": {"name": "lookup", "arguments": "{\"city\": "}}]}),
            ],
            "tool_calls",
        );
        let report = inspect(&response).unwrap();
        assert!(report.stream);
        assert_eq!(
            report.calls.iter().map(|call| call.outcome).collect::<Vec<_>>(),
            [ArgumentsOutcome::Valid, ArgumentsOutcome::InvalidJson]
        );
        assert_eq!(report.calls[0].length, r#"{"city": "Paris"}"#.len());
        assert_eq!(report.finish_reasons, ["tool_calls"]);
    }

    #[test]
    fn responses_without_tool_calls_report_nothing() {
        let answer: AiResponse = serde_json::from_value(json!({
            "id": "chatcmpl-1", "object": "chat.completion", "created": 1, "model": "model-a",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "Hi"}, "finish_reason": "stop"}]
        }))
        .unwrap();
        assert_eq!(inspect(&answer), None);
        assert_eq!(inspect(&chat(json!([]))), None);
        assert_eq!(inspect(&stream(&[json!({"content": "Hi"})], "stop")), None);
        assert_eq!(inspect(&AiResponse::Other(serde_json::Value::Null)), None);
    }

    #[test]
    fn tool_choice_modes_are_bounded() {
        assert_eq!(tool_choice_mode(&json!("auto")), Some("auto"));
        assert_eq!(tool_choice_mode(&json!("required")), Some("required"));
        assert_eq!(tool_choice_mode(&json!("none")), Some("none"));
        assert_eq!(tool_choice_mode(&json!("anything-a-client-sends")), Some("other"));
        assert_eq!(
            tool_choice_mode(&json!({"type": "function", "function": {"name": "lookup"}})),
            Some("function")
        );
        assert_eq!(tool_choice_mode(&json!({"type": "any"})), Some("required"));
        assert_eq!(tool_choice_mode(&json!({"type": "tool", "name": "lookup"})), Some("function"));
        assert_eq!(tool_choice_mode(&serde_json::Value::Null), None);
    }

    #[test]
    fn finish_reasons_are_bounded() {
        let mut response = chat(json!([call("{}")]));
        if let AiResponse::ChatCompletions(completion) = &mut response {
            completion.choices[0].finish_reason = Some("private-value-7f3a".to_string());
        }
        assert_eq!(inspect(&response).unwrap().finish_reasons, ["other"]);
        assert_eq!(finish_reason_label("length"), "length");
    }

    fn record_under(report: &ToolCallReport, parent: &SpanContext) -> (Vec<opentelemetry_sdk::trace::SpanData>, String) {
        use opentelemetry::trace::TracerProvider as _;
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

        let exporter = Exported::default();
        let provider = SdkTracerProvider::builder().with_simple_exporter(exporter.clone()).build();
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(provider.tracer("tool-calls-test")));
        let prometheus = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = prometheus.handle();
        metrics::with_local_recorder(&prometheus, || {
            tracing::subscriber::with_default(subscriber, || {
                record(
                    report,
                    &ToolCallContext {
                        model: Some("model-a"),
                        tool_choice: Some("required"),
                        served_by: Some("upstream.example.com"),
                        parent: Some(parent),
                    },
                )
            })
        });
        provider.force_flush().unwrap();
        let spans = exporter.0.lock().unwrap().clone();
        (spans, handle.render())
    }

    fn parent(flags: opentelemetry::trace::TraceFlags) -> SpanContext {
        SpanContext::new(
            opentelemetry::trace::TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
            opentelemetry::trace::SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            flags,
            false,
            Default::default(),
        )
    }

    #[test]
    fn record_exports_metadata_but_no_content() {
        const SECRET: &str = "private-value-7f3a";
        let response = chat(json!([
            call(&format!(r#"{{"note": "{SECRET}"}}"#)),
            call(&format!(r#"{{"note": "{SECRET}"#)),
        ]));
        let report = inspect(&response).unwrap();
        let (spans, rendered) = record_under(&report, &parent(opentelemetry::trace::TraceFlags::SAMPLED));

        for outcome in ["valid", "invalid_json"] {
            assert!(
                rendered.lines().any(|line| line.starts_with("dwctl_tool_call_arguments_total")
                    && line.contains(&format!(r#"outcome="{outcome}""#))
                    && line.contains(r#"model="model-a""#)
                    && line.contains(r#"tool_choice="required""#)
                    && line.contains(r#"stream="false""#)
                    && line.ends_with(" 1")),
                "{rendered}"
            );
        }

        let span = spans
            .iter()
            .find(|span| span.name == "dwctl.tool_call_arguments")
            .expect("span exported");
        let attr = |key: &str| span.attributes.iter().find(|a| a.key.as_str() == key).map(|a| a.value.to_string());
        assert_eq!(span.span_context.trace_id().to_string(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(attr("doubleword.tool_calls.count").as_deref(), Some("2"));
        assert_eq!(attr("doubleword.tool_calls.failed").as_deref(), Some("1"));
        assert_eq!(attr("doubleword.tool_calls.first_error.category").as_deref(), Some("eof"));
        assert_eq!(attr("doubleword.tool_choice").as_deref(), Some("required"));
        assert_eq!(attr("doubleword.served_by").as_deref(), Some("upstream.example.com"));
        assert!(attr("doubleword.tool_calls.outcomes").unwrap().contains("invalid_json"));

        let recorded = format!("{spans:?}{rendered}");
        assert!(!recorded.contains(SECRET), "{recorded}");
        assert!(!recorded.contains("lookup"), "tool names are not recorded: {recorded}");
    }

    #[test]
    fn record_keeps_the_requests_sampling_decision() {
        let report = inspect(&chat(json!([call("{}")]))).unwrap();
        let (spans, rendered) = record_under(&report, &parent(opentelemetry::trace::TraceFlags::default()));
        assert!(spans.is_empty(), "{spans:?}");
        assert!(rendered.contains("dwctl_tool_call_arguments_total"));
    }
}
