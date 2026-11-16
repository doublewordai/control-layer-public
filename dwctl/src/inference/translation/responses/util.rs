
#![allow(dead_code)]

use serde_json::{Map, Value};

use onwards::strict::schemas::chat_completions::Usage;

use super::types::{InputTokensDetails, OutputTokensDetails, ResponseUsage};

pub(super) fn incomplete_reason(finish_reason: Option<&str>) -> Option<&'static str> {
    match finish_reason {
        Some("length") => Some("max_output_tokens"),
        Some("content_filter") => Some("content_filter"),
        _ => None,
    }
}

pub(crate) fn ensure_field(object: &mut Map<String, Value>, key: &str, default: impl FnOnce() -> Value) {
    if !object.contains_key(key) {
        object.insert(key.to_string(), default());
    }
}

pub(crate) fn merge_reasoning_text(
    reasoning: Option<&String>,
    reasoning_content: Option<&String>,
    reasoning_details: Option<&Vec<Value>>,
) -> String {
    let mut parts: Vec<&str> = Vec::new();

    if let Some(rc) = reasoning_content
        && !rc.is_empty()
    {
        parts.push(rc);
    }
    if let Some(r) = reasoning
        && !r.is_empty()
        && !parts.contains(&r.as_str())
    {
        parts.push(r);
    }
    if let Some(details) = reasoning_details {
        for detail in details {
            if let Some(text) = detail.get("text").and_then(|v| v.as_str())
                && !text.is_empty()
                && !parts.contains(&text)
            {
                parts.push(text);
            }
        }
    }

    parts.join("\n")
}

pub(crate) fn chat_usage_to_response_usage(u: &Usage) -> ResponseUsage {
    fn extract(details: Option<&Value>, key: &str) -> u32 {
        details
            .and_then(|v| v.get(key))
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
            .min(u32::MAX as u64) as u32
    }

    let cached_tokens = extract(u.prompt_tokens_details.as_ref(), "cached_tokens");
    let reasoning_tokens = extract(u.completion_tokens_details.as_ref(), "reasoning_tokens");

    ResponseUsage {
        input_tokens: u.prompt_tokens,
        output_tokens: u.completion_tokens,
        total_tokens: u.total_tokens,
        input_tokens_details: InputTokensDetails {
            cached_tokens,
            cache_write_tokens: 0,
        },
        output_tokens_details: OutputTokensDetails { reasoning_tokens },
    }
}

pub(super) fn cache_write_tokens(usage: Option<&Value>) -> u32 {
    usage
        .and_then(|u| u.get("cache_creation_input_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0)
        .min(u32::MAX as u64) as u32
}
