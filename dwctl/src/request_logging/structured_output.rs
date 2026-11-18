
use std::collections::BTreeMap;

use onwards::strict::schemas::chat_completions::{ContentPart, MessageContent};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::request_logging::json_output::{self, JsonCheck, OutputContext, check_json_object, finish_reason_label};
use crate::request_logging::models::{AiResponse, ChatCompletionChunk};

pub fn response_format_label(format_type: &str) -> &'static str {
    match format_type {
        "json_object" => "json_object",
        "json_schema" => "json_schema",
        "text" => "text",
        _ => "other",
    }
}

pub fn expects_json(format: &str) -> bool {
    matches!(format, "json_object" | "json_schema")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredOutputReport {
    pub choices: Vec<JsonCheck>,
    pub finish_reasons: Vec<&'static str>,
    pub stream: bool,
}

fn finish_label(reason: Option<&str>) -> &'static str {
    reason.map_or("unset", finish_reason_label)
}

pub fn inspect(response: &AiResponse) -> Option<StructuredOutputReport> {
    let mut report = StructuredOutputReport {
        choices: Vec::new(),
        finish_reasons: Vec::new(),
        stream: false,
    };
    match response {
        AiResponse::ChatCompletions(completion) => {
            for choice in &completion.choices {
                let message = &choice.message;
                if message.tool_calls.as_ref().is_some_and(|calls| !calls.is_empty())
                    || choice.finish_reason.as_deref() == Some("tool_calls")
                {
                    continue;
                }
                report.choices.push(match &message.content {
                    None => JsonCheck::missing(),
                    Some(MessageContent::Text(text)) => check_json_object(text),
                    Some(MessageContent::Parts(parts)) => check_json_object(
                        &parts
                            .iter()
                            .filter_map(|part| match part {
                                ContentPart::Text { text } => Some(text.as_str()),
                                _ => None,
                            })
                            .collect::<String>(),
                    ),
                });
                report.finish_reasons.push(finish_label(choice.finish_reason.as_deref()));
            }
        }
        AiResponse::ChatCompletionsStream(chunks) => {
            report.stream = true;
            #[derive(Default)]
            struct Choice {
                content: Option<String>,
                tool_calls: bool,
                finish_reason: Option<&'static str>,
            }
            let mut choices: BTreeMap<u32, Choice> = BTreeMap::new();
            for chunk in chunks {
                let ChatCompletionChunk::Chunk(chunk) = chunk else {
                    continue;
                };
                for delta in &chunk.choices {
                    let choice = choices.entry(delta.index).or_default();
                    if let Some(content) = &delta.delta.content {
                        choice.content.get_or_insert_default().push_str(content);
                    }
                    choice.tool_calls |= delta.delta.tool_calls.as_ref().is_some_and(|calls| !calls.is_empty());
                    if let Some(reason) = &delta.finish_reason {
                        choice.finish_reason = Some(finish_reason_label(reason));
                    }
                }
            }
            for choice in choices
                .into_values()
                .filter(|choice| !choice.tool_calls && choice.finish_reason != Some("tool_calls"))
            {
                report
                    .choices
                    .push(choice.content.as_deref().map_or_else(JsonCheck::missing, check_json_object));
                report.finish_reasons.push(choice.finish_reason.unwrap_or("unset"));
            }
        }
        _ => return None,
    }
    (!report.choices.is_empty()).then_some(report)
}

pub fn record(report: &StructuredOutputReport, context: &OutputContext<'_>, format: &str) {
    let model = context.model();
    let stream = json_output::stream_label(report.stream);
    for (check, finish_reason) in report.choices.iter().zip(&report.finish_reasons) {
        json_output::count(
            "dwctl_structured_output_total",
            &[
                ("model", model.clone()),
                ("response_format", format.to_string()),
                ("finish_reason", finish_reason.to_string()),
                ("stream", stream.to_string()),
            ],
            check.outcome,
        );
    }

    let span = context.span(
        "dwctl.structured_output",
        "doubleword.structured_output",
        "content_lengths",
        &report.choices,
        report.stream,
        &report.finish_reasons,
    );
    span.set_attribute("doubleword.response_format", format.to_string());
    let failed = json_output::failures(&report.choices);
    if failed > 0 {
        let first_error = json_output::first_error(&report.choices);
        let _entered = span.enter();
        tracing::warn!(
            model = %model,
            response_format = format,
            choices = report.choices.len(),
            failed,
            outcome = report.choices.iter().find(|c| c.outcome != json_output::JsonOutcome::Valid).map(|c| c.outcome.as_str()),
            finish_reasons = ?report.finish_reasons,
            stream = report.stream,
            served_by = context.served_by,
            max_tokens = context.max_tokens,
            output_tokens = context.output_tokens,
            reasoning_tokens = context.reasoning_tokens,
            first_error_line = first_error.map(|error| error.line),
            first_error_column = first_error.map(|error| error.column),
            first_error_category = first_error.map(|error| error.category),
            "Structured output is not a valid JSON object"
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

    fn chat(message: serde_json::Value, finish_reason: &str) -> AiResponse {
        serde_json::from_value(json!({
            "id": "chatcmpl-1", "object": "chat.completion", "created": 1, "model": "model-a",
            "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}]
        }))
        .unwrap()
    }

    fn stream(deltas: &[serde_json::Value], finish_reason: &str) -> AiResponse {
        let chunk = |choice: serde_json::Value| {
            serde_json::from_value(json!({
                "id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 1, "model": "model-a",
                "choices": [choice]
            }))
            .unwrap()
        };
        let mut chunks: Vec<ChatCompletionChunk> = deltas.iter().map(|delta| chunk(json!({"index": 0, "delta": delta}))).collect();
        chunks.push(chunk(json!({"index": 0, "delta": {}, "finish_reason": finish_reason})));
        AiResponse::ChatCompletionsStream(chunks)
    }

    fn outcomes(report: &StructuredOutputReport) -> Vec<JsonOutcome> {
        report.choices.iter().map(|check| check.outcome).collect()
    }

    #[test]
    fn formats_are_bounded() {
        assert_eq!(response_format_label("json_schema"), "json_schema");
        assert_eq!(response_format_label("json_object"), "json_object");
        assert_eq!(response_format_label("text"), "text");
        assert_eq!(response_format_label("grammar"), "other");
        assert!(expects_json("json_schema"));
        assert!(!expects_json("text"));
    }

    #[test]
    fn content_is_checked() {
        let report = inspect(&chat(json!({"role": "assistant", "content": r#"{"a": 1}"#}), "stop")).unwrap();
        assert_eq!(outcomes(&report), [JsonOutcome::Valid]);
        assert_eq!(report.finish_reasons, ["stop"]);

        let fenced = inspect(&chat(json!({"role": "assistant", "content": "```json\n{}\n```"}), "stop")).unwrap();
        assert_eq!(outcomes(&fenced), [JsonOutcome::InvalidJson]);
        assert_eq!(fenced.choices[0].error.map(|error| (error.line, error.column)), Some((1, 1)));

        let parts = json!({"role": "assistant", "content": [{"type": "text", "text": "{\"a\""}, {"type": "text", "text": ": 1}"}]});
        assert_eq!(outcomes(&inspect(&chat(parts, "stop")).unwrap()), [JsonOutcome::Valid]);
    }

    #[test]
    fn a_choice_without_content_is_missing() {
        let report = inspect(&chat(
            json!({"role": "assistant", "content": null, "reasoning_content": "thinking"}),
            "length",
        ))
        .unwrap();
        assert_eq!(outcomes(&report), [JsonOutcome::Missing]);
        assert_eq!(report.finish_reasons, ["length"]);

        let streamed = inspect(&stream(&[json!({"reasoning_content": "thinking"})], "length")).unwrap();
        assert!(streamed.stream);
        assert_eq!(outcomes(&streamed), [JsonOutcome::Missing]);
    }

    #[test]
    fn a_stream_is_checked_after_joining_its_content() {
        let report = inspect(&stream(&[json!({"content": "{\"a\": "}), json!({"content": "\"b\"}"})], "stop")).unwrap();
        assert_eq!(outcomes(&report), [JsonOutcome::Valid]);
        assert_eq!(report.choices[0].length, r#"{"a": "b"}"#.len());

        let cut = inspect(&stream(&[json!({"content": "{\"a\": "})], "length")).unwrap();
        assert_eq!(outcomes(&cut), [JsonOutcome::InvalidJson]);
        assert_eq!(cut.finish_reasons, ["length"]);
    }

    #[test]
    fn choices_with_tool_calls_are_left_to_the_tool_call_check() {
        let call = json!({"id": "call_1", "type": "function", "function": {"name": "lookup", "arguments": "{}"}});
        assert_eq!(
            inspect(&chat(
                json!({"role": "assistant", "content": null, "tool_calls": [call]}),
                "tool_calls"
            )),
            None
        );
        let streamed = stream(
            &[json!({"tool_calls": [{"index": 0, "function": {"arguments": "{}"}}]})],
            "tool_calls",
        );
        assert_eq!(inspect(&streamed), None);
        assert_eq!(inspect(&chat(json!({"role": "assistant", "content": null}), "tool_calls")), None);
        assert_eq!(inspect(&stream(&[json!({"content": "x"})], "tool_calls")), None);
        assert_eq!(inspect(&AiResponse::Other(serde_json::Value::Null)), None);
    }

    #[test]
    fn record_exports_metadata_but_no_content() {
        const SECRET: &str = "private-value-7f3a";
        let report = inspect(&chat(
            json!({"role": "assistant", "content": format!(r#"{{"note": "{SECRET}"#)}),
            "length",
        ))
        .unwrap();
        let parent = parent(TraceFlags::SAMPLED);
        let (spans, rendered) = recorded(|| {
            record(
                &report,
                &OutputContext {
                    model: Some("model-a"),
                    served_by: Some("upstream.example.com"),
                    max_tokens: Some(200),
                    output_tokens: Some(200),
                    reasoning_tokens: Some(180),
                    parent: Some(&parent),
                },
                "json_schema",
            )
        });

        let labels = [
            r#"model="model-a""#,
            r#"response_format="json_schema""#,
            r#"finish_reason="length""#,
            r#"stream="false""#,
        ];
        let count = |outcome: &str| {
            let outcome = format!(r#"outcome="{outcome}""#);
            counter(
                &rendered,
                "dwctl_structured_output_total",
                &[&labels[..], &[outcome.as_str()]].concat(),
            )
        };
        assert_eq!(count("invalid_json"), Some(1), "{rendered}");
        assert_eq!(count("valid"), Some(0), "{rendered}");

        let attr = |key: &str| attribute(&spans, "dwctl.structured_output", key);
        assert_eq!(attr("doubleword.response_format").as_deref(), Some("json_schema"));
        assert_eq!(attr("doubleword.structured_output.failed").as_deref(), Some("1"));
        assert_eq!(attr("doubleword.structured_output.first_error.category").as_deref(), Some("eof"));
        assert!(attr("doubleword.structured_output.content_lengths").is_some());
        assert_eq!(attr("gen_ai.usage.output_tokens.reasoning").as_deref(), Some("180"));
        assert!(attr("gen_ai.response.finish_reasons").unwrap().contains("length"));

        let span = spans.iter().find(|span| span.name == "dwctl.structured_output").unwrap();
        assert!(
            matches!(span.status, opentelemetry::trace::Status::Error { .. }),
            "a failed check is an error span: {:?}",
            span.status
        );

        let recorded = format!("{spans:?}{rendered}");
        assert!(!recorded.contains(SECRET), "{recorded}");
    }
}
