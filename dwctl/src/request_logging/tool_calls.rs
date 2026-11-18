
use std::collections::{BTreeMap, BTreeSet};

use crate::request_logging::json_output::{self, JsonCheck, OutputContext, check_json_object, finish_reason_label};
use crate::request_logging::models::{AiResponse, ChatCompletionChunk};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallReport {
    pub calls: Vec<JsonCheck>,
    pub stream: bool,
    pub finish_reasons: Vec<&'static str>,
}

pub fn inspect(response: &AiResponse, tool_choice: Option<&str>) -> Option<ToolCallReport> {
    let forced = matches!(tool_choice, Some("required" | "function"));
    let report = match response {
        AiResponse::ChatCompletions(completion) => {
            let mut calls = Vec::new();
            let mut finish_reasons = Vec::new();
            for choice in &completion.choices {
                let tool_calls = choice.message.tool_calls.as_deref().unwrap_or_default();
                let owed = forced || choice.finish_reason.as_deref() == Some("tool_calls");
                if tool_calls.is_empty() && !owed {
                    continue;
                }
                calls.extend(tool_calls.iter().map(|call| check_json_object(&call.function.arguments)));
                if tool_calls.is_empty() {
                    calls.push(JsonCheck::missing());
                }
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
            let mut seen: BTreeSet<u32> = BTreeSet::new();
            for chunk in chunks {
                let ChatCompletionChunk::Chunk(chunk) = chunk else {
                    continue;
                };
                for choice in &chunk.choices {
                    seen.insert(choice.index);
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
            let owed = |choice: &u32| forced || finish_reasons.get(choice) == Some(&"tool_calls");
            let choices: BTreeSet<u32> = arguments
                .keys()
                .map(|(choice, _)| *choice)
                .chain(seen.iter().copied().filter(owed))
                .collect();
            let mut calls = Vec::new();
            for &choice in &choices {
                let before = calls.len();
                calls.extend(
                    arguments
                        .range((choice, 0)..=(choice, u32::MAX))
                        .map(|(_, arguments)| check_json_object(arguments)),
                );
                if calls.len() == before {
                    calls.push(JsonCheck::missing());
                }
            }
            ToolCallReport {
                calls,
                stream: true,
                finish_reasons: choices.iter().filter_map(|choice| finish_reasons.get(choice).copied()).collect(),
            }
        }
        _ => return None,
    };
    (!report.calls.is_empty()).then_some(report)
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

pub fn record(report: &ToolCallReport, context: &OutputContext<'_>, tool_choice: Option<&str>) {
    let model = context.model();
    let tool_choice = tool_choice.unwrap_or("unset");
    let stream = json_output::stream_label(report.stream);
    let labels = [
        ("model", model.clone()),
        ("tool_choice", tool_choice.to_string()),
        ("stream", stream.to_string()),
    ];
    for call in &report.calls {
        json_output::count("dwctl_tool_call_arguments_total", &labels, call.outcome);
    }

    let span = context.span(
        "dwctl.tool_call_arguments",
        "doubleword.tool_calls",
        "argument_lengths",
        &report.calls,
        report.stream,
        &report.finish_reasons,
    );
    span.set_attribute("doubleword.tool_choice", tool_choice.to_string());
    let failed = json_output::failures(&report.calls);
    if failed > 0 {
        let first_error = json_output::first_error(&report.calls);
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
    use crate::request_logging::json_output::JsonOutcome;
    use crate::request_logging::json_output::testing::{attribute, counter, parent, recorded};
    use opentelemetry::trace::TraceFlags;
    use serde_json::json;

    fn chat_with(tool_calls: serde_json::Value, finish_reason: &str) -> AiResponse {
        serde_json::from_value(json!({
            "id": "chatcmpl-1", "object": "chat.completion", "created": 1, "model": "model-a",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": null, "tool_calls": tool_calls},
                "finish_reason": finish_reason
            }]
        }))
        .unwrap()
    }

    fn chat(tool_calls: serde_json::Value) -> AiResponse {
        chat_with(tool_calls, "tool_calls")
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

    fn outcomes(report: &ToolCallReport) -> Vec<JsonOutcome> {
        report.calls.iter().map(|call| call.outcome).collect()
    }

    #[test]
    fn non_streaming_tool_calls_are_checked_in_order() {
        let report = inspect(&chat(json!([call(r#"{"q": 1}"#), call(r#"{"q": "#)])), None).unwrap();
        assert!(!report.stream);
        assert_eq!(outcomes(&report), [JsonOutcome::Valid, JsonOutcome::InvalidJson]);
        assert_eq!(json_output::failures(&report.calls), 1);
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
        let report = inspect(&response, None).unwrap();
        assert!(report.stream);
        assert_eq!(outcomes(&report), [JsonOutcome::Valid, JsonOutcome::InvalidJson]);
        assert_eq!(report.calls[0].length, r#"{"city": "Paris"}"#.len());
        assert_eq!(report.finish_reasons, ["tool_calls"]);
    }

    #[test]
    fn finishing_with_tool_calls_but_none_is_a_missing_call() {
        let report = inspect(&chat(serde_json::Value::Null), None).unwrap();
        assert_eq!(outcomes(&report), [JsonOutcome::Missing]);
        assert_eq!(report.finish_reasons, ["tool_calls"]);

        let report = inspect(&stream(&[json!({"content": "<tool_call>lookup"})], "tool_calls"), None).unwrap();
        assert!(report.stream);
        assert_eq!(outcomes(&report), [JsonOutcome::Missing]);
        assert_eq!(report.calls[0].length, 0);
    }

    #[test]
    fn a_forced_tool_choice_answered_without_a_call_is_missing() {
        let answer: AiResponse = serde_json::from_value(json!({
            "id": "chatcmpl-1", "object": "chat.completion", "created": 1, "model": "model-a",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "Hi"}, "finish_reason": "stop"}]
        }))
        .unwrap();
        for forced in ["required", "function"] {
            let report = inspect(&answer, Some(forced)).unwrap();
            assert_eq!(outcomes(&report), [JsonOutcome::Missing], "{forced}");
            assert_eq!(report.finish_reasons, ["stop"]);
            let streamed = inspect(&stream(&[json!({"content": "Hi"})], "stop"), Some(forced)).unwrap();
            assert_eq!(outcomes(&streamed), [JsonOutcome::Missing], "{forced}");
        }
        assert_eq!(inspect(&answer, Some("auto")), None);
        assert_eq!(
            outcomes(&inspect(&chat_with(json!([call("{}")]), "stop"), Some("required")).unwrap()),
            [JsonOutcome::Valid]
        );
    }

    #[test]
    fn responses_without_tool_calls_report_nothing() {
        let answer: AiResponse = serde_json::from_value(json!({
            "id": "chatcmpl-1", "object": "chat.completion", "created": 1, "model": "model-a",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "Hi"}, "finish_reason": "stop"}]
        }))
        .unwrap();
        assert_eq!(inspect(&answer, None), None);
        assert_eq!(inspect(&chat_with(json!([]), "stop"), None), None);
        assert_eq!(inspect(&stream(&[json!({"content": "Hi"})], "stop"), None), None);
        assert_eq!(inspect(&AiResponse::Other(serde_json::Value::Null), None), None);
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
        let response = chat_with(json!([call("{}")]), "private-value-7f3a");
        assert_eq!(inspect(&response, None).unwrap().finish_reasons, ["other"]);
    }

    fn record_under(report: &ToolCallReport, flags: TraceFlags) -> (Vec<opentelemetry_sdk::trace::SpanData>, String) {
        let parent = parent(flags);
        recorded(|| {
            record(
                report,
                &OutputContext {
                    model: Some("model-a"),
                    served_by: Some("upstream.example.com"),
                    max_tokens: Some(200),
                    output_tokens: Some(200),
                    reasoning_tokens: Some(150),
                    parent: Some(&parent),
                },
                Some("required"),
            )
        })
    }

    #[test]
    fn record_exports_metadata_but_no_content() {
        const SECRET: &str = "private-value-7f3a";
        let response = chat(json!([
            call(&format!(r#"{{"note": "{SECRET}"}}"#)),
            call(&format!(r#"{{"note": "{SECRET}"#)),
        ]));
        let report = inspect(&response, None).unwrap();
        let (spans, rendered) = record_under(&report, TraceFlags::SAMPLED);

        let labels = [r#"model="model-a""#, r#"tool_choice="required""#, r#"stream="false""#];
        let count = |outcome: &str| {
            let outcome = format!(r#"outcome="{outcome}""#);
            counter(
                &rendered,
                "dwctl_tool_call_arguments_total",
                &[&labels[..], &[outcome.as_str()]].concat(),
            )
        };
        assert_eq!(count("valid"), Some(1), "{rendered}");
        assert_eq!(count("invalid_json"), Some(1), "{rendered}");
        assert_eq!(count("missing"), Some(0), "{rendered}");

        const NAME: &str = "dwctl.tool_call_arguments";
        let attr = |key: &str| attribute(&spans, NAME, key);
        let span = spans.iter().find(|span| span.name == NAME).unwrap();
        assert_eq!(span.span_context.trace_id().to_string(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(attr("doubleword.tool_calls.count").as_deref(), Some("2"));
        assert_eq!(attr("doubleword.tool_calls.failed").as_deref(), Some("1"));
        assert_eq!(attr("doubleword.tool_calls.first_error.category").as_deref(), Some("eof"));
        assert!(attr("doubleword.tool_calls.argument_lengths").is_some());
        assert_eq!(attr("doubleword.tool_choice").as_deref(), Some("required"));
        assert_eq!(attr("doubleword.served_by").as_deref(), Some("upstream.example.com"));
        assert_eq!(attr("gen_ai.request.max_tokens").as_deref(), Some("200"));
        assert_eq!(attr("gen_ai.usage.output_tokens.reasoning").as_deref(), Some("150"));
        assert!(attr("doubleword.tool_calls.outcomes").unwrap().contains("invalid_json"));

        let recorded = format!("{spans:?}{rendered}");
        assert!(!recorded.contains(SECRET), "{recorded}");
        assert!(!recorded.contains("lookup"), "tool names are not recorded: {recorded}");
    }

    #[test]
    fn record_keeps_the_requests_sampling_decision() {
        let report = inspect(&chat(json!([call("{}")])), None).unwrap();
        let (spans, rendered) = record_under(&report, TraceFlags::default());
        assert!(spans.is_empty(), "{spans:?}");
        assert!(rendered.contains("dwctl_tool_call_arguments_total"));
    }
}
