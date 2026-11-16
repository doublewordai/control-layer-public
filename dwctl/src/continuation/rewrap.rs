
use bytes::Bytes;
use serde_json::{Value, json};

pub const DONE_FRAME: &[u8] = b"data: [DONE]\n\n";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub id: String,
    pub model: String,
    pub created: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergedUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
}

impl MergedUsage {
    fn to_value(self) -> Value {
        json!({
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageAnomaly {
    PromptUnderflow,
}

impl UsageAnomaly {
    pub fn kind(self) -> &'static str {
        match self {
            UsageAnomaly::PromptUnderflow => "prompt_underflow",
        }
    }
}

pub fn merge_usage(seg: u64, reported_prompt: u64, leg_completion: u64, render_total: u64) -> (MergedUsage, Option<UsageAnomaly>) {
    let (prompt_tokens, anomaly) = if reported_prompt >= seg {
        (reported_prompt - seg, None)
    } else {
        (render_total.saturating_sub(seg), Some(UsageAnomaly::PromptUnderflow))
    };
    let completion_tokens = seg.saturating_add(leg_completion);
    (
        MergedUsage {
            prompt_tokens,
            completion_tokens,
            total_tokens: prompt_tokens.saturating_add(completion_tokens),
        },
        anomaly,
    )
}

pub fn usage_of(chunk: &Value) -> Option<(u64, u64)> {
    let usage = chunk.get("usage")?.as_object()?;
    let prompt = usage.get("prompt_tokens").and_then(Value::as_u64).unwrap_or(0);
    let completion = usage.get("completion_tokens").and_then(Value::as_u64).unwrap_or(0);
    Some((prompt, completion))
}

pub fn reframe_chunk(chunk: &Value, env: &Envelope) -> Option<Value> {
    let choice = chunk.get("choices")?.as_array()?.first()?;
    let text = choice.get("text").and_then(Value::as_str).unwrap_or("");
    let finish_reason = choice.get("finish_reason").cloned().unwrap_or(Value::Null);
    if text.is_empty() && finish_reason.is_null() {
        return None;
    }
    let mut delta = serde_json::Map::new();
    if !text.is_empty() {
        delta.insert("content".to_string(), Value::String(text.to_string()));
    }
    Some(json!({
        "id": env.id,
        "object": "chat.completion.chunk",
        "created": env.created,
        "model": env.model,
        "choices": [{
            "index": 0,
            "delta": Value::Object(delta),
            "finish_reason": finish_reason,
        }],
    }))
}

pub fn length_stop_frame(env: &Envelope) -> Value {
    json!({
        "id": env.id,
        "object": "chat.completion.chunk",
        "created": env.created,
        "model": env.model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": "length"}],
    })
}

pub fn usage_frame(env: &Envelope, usage: MergedUsage) -> Value {
    json!({
        "id": env.id,
        "object": "chat.completion.chunk",
        "created": env.created,
        "model": env.model,
        "choices": [],
        "usage": usage.to_value(),
    })
}

pub fn sse_frame(value: &Value) -> Bytes {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(b"data: ");
    match serde_json::to_vec(value) {
        Ok(mut v) => out.append(&mut v),
        Err(_) => out.extend_from_slice(b"{}"),
    }
    out.extend_from_slice(b"\n\n");
    Bytes::from(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env() -> Envelope {
        Envelope {
            id: "chatcmpl-abc".to_string(),
            model: "dsv4-flash".to_string(),
            created: 1_700_000_000,
        }
    }


    #[test]
    fn merge_usage_subtracts_the_partial_generation_from_the_leg_prompt() {
        let (usage, anomaly) = merge_usage(40, 1040, 60, 1042);
        assert_eq!(anomaly, None);
        assert_eq!(usage.prompt_tokens, 1000, "the customer pays for their original prompt once");
        assert_eq!(usage.completion_tokens, 100, "everything the client received across both legs");
        assert_eq!(usage.total_tokens, 1100);
    }

    #[test]
    fn merge_usage_with_no_generation_before_the_leg_is_the_leg_itself() {
        let (usage, anomaly) = merge_usage(0, 1000, 60, 1000);
        assert_eq!(anomaly, None);
        assert_eq!(
            usage,
            MergedUsage {
                prompt_tokens: 1000,
                completion_tokens: 60,
                total_tokens: 1060
            }
        );
    }

    #[test]
    fn merge_usage_falls_back_to_render_when_the_provider_undercounts() {
        let (usage, anomaly) = merge_usage(40, 30, 60, 1042);
        assert_eq!(anomaly, Some(UsageAnomaly::PromptUnderflow));
        assert_eq!(usage.prompt_tokens, 1002, "render-derived prompt (total 1042 - seg 40)");
        assert_eq!(usage.completion_tokens, 100, "the generation total is unaffected by the anomaly");
        assert_eq!(usage.total_tokens, 1102);
    }

    #[test]
    fn merge_usage_never_underflows_even_when_the_render_is_smaller_than_the_segment() {
        let (usage, anomaly) = merge_usage(40, 0, 0, 10);
        assert_eq!(anomaly, Some(UsageAnomaly::PromptUnderflow));
        assert_eq!(usage.prompt_tokens, 0);
        assert_eq!(usage.completion_tokens, 40);
    }

    #[test]
    fn usage_of_reads_counts_and_tolerates_null_usage() {
        assert_eq!(
            usage_of(&json!({"usage": {"prompt_tokens": 7, "completion_tokens": 3}})),
            Some((7, 3))
        );
        assert_eq!(usage_of(&json!({"usage": null})), None);
        assert_eq!(usage_of(&json!({"choices": []})), None);
        assert_eq!(usage_of(&json!({"usage": {"prompt_tokens": 7}})), Some((7, 0)));
    }


    #[test]
    fn reframe_maps_text_onto_the_original_envelope() {
        let chunk = json!({
            "id": "cmpl-leg2", "object": "text_completion", "created": 1_800_000_000,
            "model": "continuation-composite",
            "choices": [{"text": " world", "index": 0, "logprobs": null, "finish_reason": null}]
        });
        let out = reframe_chunk(&chunk, &env()).unwrap();
        assert_eq!(out["id"], "chatcmpl-abc", "the client's stream id, not the leg's");
        assert_eq!(out["object"], "chat.completion.chunk");
        assert_eq!(out["created"], 1_700_000_000);
        assert_eq!(out["model"], "dsv4-flash");
        assert_eq!(out["choices"][0]["delta"]["content"], " world");
        assert!(
            out["choices"][0]["delta"].get("role").is_none(),
            "leg 1 already opened the message; a second role delta would start a new one"
        );
        assert_eq!(out["choices"][0]["finish_reason"], Value::Null);
    }

    #[test]
    fn reframe_passes_through_finish_reason_on_the_last_content_chunk() {
        let chunk = json!({
            "choices": [{"text": "!", "index": 0, "finish_reason": "stop"}]
        });
        let out = reframe_chunk(&chunk, &env()).unwrap();
        assert_eq!(out["choices"][0]["finish_reason"], "stop");
        assert_eq!(out["choices"][0]["delta"]["content"], "!");
    }

    #[test]
    fn reframe_emits_a_bare_finish_reason_chunk_with_no_content_key() {
        let chunk = json!({"choices": [{"text": "", "index": 0, "finish_reason": "length"}]});
        let out = reframe_chunk(&chunk, &env()).unwrap();
        assert_eq!(out["choices"][0]["finish_reason"], "length");
        assert!(out["choices"][0]["delta"].as_object().unwrap().is_empty());
    }

    #[test]
    fn reframe_skips_frames_with_nothing_for_the_client() {
        assert!(reframe_chunk(&json!({"choices": [], "usage": {"prompt_tokens": 1}}), &env()).is_none());
        assert!(reframe_chunk(&json!({"choices": [{"text": "", "finish_reason": null}]}), &env()).is_none());
        assert!(reframe_chunk(&json!({"error": {"message": "boom"}}), &env()).is_none());
    }


    #[test]
    fn usage_frame_is_a_choiceless_chat_chunk() {
        let frame = usage_frame(
            &env(),
            MergedUsage {
                prompt_tokens: 1000,
                completion_tokens: 100,
                total_tokens: 1100,
            },
        );
        assert_eq!(frame["object"], "chat.completion.chunk");
        assert_eq!(frame["choices"].as_array().unwrap().len(), 0);
        assert_eq!(frame["usage"]["prompt_tokens"], 1000);
        assert_eq!(frame["usage"]["completion_tokens"], 100);
        assert_eq!(frame["usage"]["total_tokens"], 1100);
        assert!(
            frame["usage"].get("prompt_tokens_details").is_none(),
            "our frame stays minimal — the cache layer above injects its own cache_* fields"
        );
    }

    #[test]
    fn sse_frame_wraps_json_in_a_data_event() {
        let bytes = sse_frame(&json!({"a": 1}));
        assert_eq!(&bytes[..], b"data: {\"a\":1}\n\n");
    }
}
