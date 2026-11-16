
use axum::body::Bytes;
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize)]
struct LenientChatCompletion {
    #[serde(default = "default_id")]
    id: String,
    #[serde(default = "default_object")]
    object: String,
    #[serde(default = "default_created")]
    created: u32,
    model: String,
    choices: Vec<LenientChoice>,
    #[serde(default)]
    usage: Option<LenientUsage>,
    #[serde(default)]
    system_fingerprint: Option<String>,
    #[serde(default)]
    service_tier: Option<String>,
    #[serde(flatten, skip_serializing)]
    _extra: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct LenientChoice {
    #[serde(default)]
    index: u64,
    message: serde_json::Value,
    #[serde(default)]
    finish_reason: Option<String>,
    #[serde(default)]
    logprobs: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stop_reason: Option<serde_json::Value>,
    #[serde(flatten, skip_serializing)]
    _extra: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct LenientUsage {
    #[serde(default)]
    prompt_tokens: u64,
    #[serde(default)]
    completion_tokens: u64,
    #[serde(default)]
    total_tokens: u64,
    #[serde(default)]
    prompt_tokens_details: Option<PromptTokensDetails>,
    #[serde(default)]
    completion_tokens_details: Option<CompletionTokensDetails>,
    #[serde(flatten, skip_serializing)]
    _extra: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct PromptTokensDetails {
    #[serde(default)]
    cached_tokens: u64,
    #[serde(default)]
    audio_tokens: u64,
    #[serde(flatten, skip_serializing)]
    _extra: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CompletionTokensDetails {
    #[serde(default)]
    reasoning_tokens: u64,
    #[serde(default)]
    audio_tokens: u64,
    #[serde(default)]
    accepted_prediction_tokens: u64,
    #[serde(default)]
    rejected_prediction_tokens: u64,
    #[serde(flatten, skip_serializing)]
    _extra: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct LenientStreamChunk {
    #[serde(default = "default_id")]
    id: String,
    #[serde(default = "default_stream_object")]
    object: String,
    #[serde(default = "default_created")]
    created: u32,
    model: String,
    choices: Vec<LenientStreamChoice>,
    #[serde(default)]
    usage: Option<LenientUsage>,
    #[serde(default)]
    system_fingerprint: Option<String>,
    #[serde(default)]
    service_tier: Option<String>,
    #[serde(flatten, skip_serializing)]
    _extra: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct LenientStreamChoice {
    #[serde(default)]
    index: u64,
    #[serde(default)]
    delta: Option<serde_json::Value>,
    #[serde(default)]
    finish_reason: Option<String>,
    #[serde(default)]
    logprobs: Option<serde_json::Value>,
    #[serde(flatten, skip_serializing)]
    _extra: HashMap<String, serde_json::Value>,
}

fn default_id() -> String {
    "chatcmpl-unknown".to_string()
}

fn default_object() -> String {
    "chat.completion".to_string()
}

fn default_stream_object() -> String {
    "chat.completion.chunk".to_string()
}

fn default_created() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32
}

pub struct ResponseSanitizer {
    pub original_model: Option<String>,
}

impl ResponseSanitizer {
    pub fn sanitize(
        &self,
        path: &str,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Option<Bytes>, String> {
        if !path.contains("/chat/completions") {
            return Ok(None);
        }

        let is_streaming = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.contains("text/event-stream"))
            .unwrap_or(false);

        if is_streaming {
            self.sanitize_streaming(body)
        } else {
            self.sanitize_non_streaming(body)
        }
    }

    fn sanitize_non_streaming(&self, body: &[u8]) -> Result<Option<Bytes>, String> {
        let mut completion: LenientChatCompletion = serde_json::from_slice(body)
            .map_err(|e| format!("Failed to parse response as JSON: {}", e))?;

        if let Some(ref original) = self.original_model {
            completion.model = original.clone();
        }

        let sanitized_bytes = serde_json::to_vec(&completion)
            .map_err(|e| format!("Failed to serialize sanitized response: {}", e))?;

        Ok(Some(Bytes::from(sanitized_bytes)))
    }

    pub fn sanitize_streaming(&self, body: &[u8]) -> Result<Option<Bytes>, String> {
        let body_str = std::str::from_utf8(body)
            .map_err(|e| format!("Invalid UTF-8 in streaming response: {}", e))?;

        let mut sanitized_lines = Vec::new();

        for line in body_str.lines() {
            if let Some(data_part) = line.strip_prefix("data: ") {

                if data_part.trim() == "[DONE]" {
                    sanitized_lines.push(line.to_string());
                } else {
                    let value: serde_json::Value = serde_json::from_str(data_part)
                        .map_err(|e| format!("Failed to parse stream chunk: {}", e))?;

                    if let Some(error_event) = extract_embedded_error_envelope(&value) {
                        tracing::warn!(
                            data_len = data_part.len(),
                            "Provider returned error envelope in SSE stream (non-strict: forwarding verbatim)"
                        );
                        sanitized_lines.push(error_event);
                    } else {
                        let mut chunk: LenientStreamChunk = serde_json::from_value(value)
                            .map_err(|e| format!("Failed to parse stream chunk: {}", e))?;

                        if let Some(ref original) = self.original_model {
                            chunk.model = original.clone();
                        }

                        let sanitized_json = serde_json::to_string(&chunk)
                            .map_err(|e| format!("Failed to serialize stream chunk: {}", e))?;

                        sanitized_lines.push(format!("data: {}", sanitized_json));
                    }
                }
            } else if line.is_empty() {
                sanitized_lines.push(String::new());
            }
        }

        let mut sanitized_body = sanitized_lines.join("\n");

        let input_trailing = body_str.chars().rev().take_while(|&c| c == '\n').count();
        let output_trailing = sanitized_body
            .chars()
            .rev()
            .take_while(|&c| c == '\n')
            .count();

        for _ in output_trailing..input_trailing {
            sanitized_body.push('\n');
        }

        Ok(Some(Bytes::from(sanitized_body)))
    }
}

fn extract_embedded_error_envelope(value: &serde_json::Value) -> Option<String> {
    let error_obj = value
        .get("error")
        .filter(|v| v.is_object() && (v.get("message").is_some() || v.get("code").is_some()))?;
    let envelope = serde_json::json!({ "error": error_obj });
    Some(format!("data: {envelope}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};

    #[test]
    fn test_non_streaming_sanitization() {
        let sanitizer = ResponseSanitizer {
            original_model: Some("gpt-4".to_string()),
        };

        let response_json = r#"{
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1677652288,
            "model": "gpt-4-turbo",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello!"
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 9,
                "completion_tokens": 2,
                "total_tokens": 11
            }
        }"#;

        let headers = HeaderMap::new();
        let result = sanitizer
            .sanitize("/v1/chat/completions", &headers, response_json.as_bytes())
            .unwrap()
            .unwrap();

        let sanitized: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(sanitized["model"], "gpt-4");
    }

    #[test]
    fn test_matched_stop_reason_is_preserved() {
        let sanitizer = ResponseSanitizer {
            original_model: Some("m".to_string()),
        };
        let response_json = r#"{
            "id": "c", "object": "chat.completion", "created": 1, "model": "m",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": "one two" },
                "finish_reason": "stop",
                "stop_reason": "three"
            }]
        }"#;
        let result = sanitizer
            .sanitize(
                "/v1/chat/completions",
                &HeaderMap::new(),
                response_json.as_bytes(),
            )
            .unwrap()
            .unwrap();
        let sanitized: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(sanitized["choices"][0]["stop_reason"], "three");

        let plain = r#"{
            "id": "c", "object": "chat.completion", "created": 1, "model": "m",
            "choices": [{ "index": 0, "message": { "role": "assistant", "content": "hi" }, "finish_reason": "stop" }]
        }"#;
        let result = sanitizer
            .sanitize("/v1/chat/completions", &HeaderMap::new(), plain.as_bytes())
            .unwrap()
            .unwrap();
        let sanitized: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!(sanitized["choices"][0].get("stop_reason").is_none());
    }

    #[test]
    fn test_streaming_sanitization() {
        let sanitizer = ResponseSanitizer {
            original_model: Some("gpt-4".to_string()),
        };

        let sse_response = "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"created\":1677652288,\"model\":\"gpt-4-turbo\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n";

        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            HeaderValue::from_static("text/event-stream"),
        );

        let result = sanitizer
            .sanitize("/v1/chat/completions", &headers, sse_response.as_bytes())
            .unwrap()
            .unwrap();

        let result_str = std::str::from_utf8(&result).unwrap();
        assert!(result_str.contains("\"model\":\"gpt-4\""));
        assert!(result_str.contains("data: [DONE]"));

        assert!(
            result_str.ends_with("\n\n"),
            "SSE response should end with \\n\\n, got: {:?}",
            &result_str[result_str.len().saturating_sub(10)..]
        );

        assert!(
            result_str.contains("}\n\ndata:"),
            "SSE messages should be separated by \\n\\n"
        );

        let trailing_count = result_str.chars().rev().take_while(|&c| c == '\n').count();
        assert_eq!(
            trailing_count, 2,
            "Should have exactly 2 trailing newlines, got {}",
            trailing_count
        );
    }

    #[test]
    fn test_streaming_multiple_chunks() {
        let sanitizer = ResponseSanitizer {
            original_model: Some("gpt-4".to_string()),
        };

        let sse_response = "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"created\":1677652288,\"model\":\"gpt-4-turbo\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\ndata: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"created\":1677652289,\"model\":\"gpt-4-turbo\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" World\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n";

        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            HeaderValue::from_static("text/event-stream"),
        );

        let result = sanitizer
            .sanitize("/v1/chat/completions", &headers, sse_response.as_bytes())
            .unwrap()
            .unwrap();

        let result_str = std::str::from_utf8(&result).unwrap();

        let chunk_count = result_str.matches("\"model\":\"gpt-4\"").count();
        assert_eq!(chunk_count, 2, "Both chunks should have model rewritten");

        assert!(result_str.ends_with("\n\n"));

        let message_count = result_str.matches("data:").count();
        assert_eq!(message_count, 3);
    }

    #[test]
    fn test_path_filtering() {
        let sanitizer = ResponseSanitizer {
            original_model: None,
        };

        let headers = HeaderMap::new();
        let result = sanitizer.sanitize("/v1/models", &headers, b"test").unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_no_model_rewrite() {
        let sanitizer = ResponseSanitizer {
            original_model: None,
        };

        let response_json = r#"{
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1677652288,
            "model": "gpt-4-turbo",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello!"
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 9,
                "completion_tokens": 2,
                "total_tokens": 11
            }
        }"#;

        let headers = HeaderMap::new();
        let result = sanitizer
            .sanitize("/v1/chat/completions", &headers, response_json.as_bytes())
            .unwrap()
            .unwrap();

        let sanitized: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(sanitized["model"], "gpt-4-turbo");
    }

    #[test]
    fn test_openrouter_response_sanitization() {
        let sanitizer = ResponseSanitizer {
            original_model: Some("fred".to_string()),
        };

        let openrouter_response = r#"{
            "id":"gen-1768414327-n9nnQrvnhHU8HF0oRVKq",
            "provider":"DeepInfra",
            "model":"google/gemma-3-12b-it",
            "object":"chat.completion",
            "created":1768414327,
            "choices":[{
                "logprobs":null,
                "finish_reason":"stop",
                "native_finish_reason":"stop",
                "index":0,
                "message":{
                    "role":"assistant",
                    "content":"Hello! I'm doing well, thank you for asking! As an AI, I don't experience feelings like humans do, but I'm operating smoothly and ready to assist you. 😊 \n\nHow are *you* doing today?",
                    "refusal":null,
                    "reasoning":null
                }
            }],
            "usage":{
                "prompt_tokens":15,
                "completion_tokens":51,
                "total_tokens":66,
                "cost":0.00000723,
                "is_byok":false,
                "prompt_tokens_details":{
                    "cached_tokens":0,
                    "audio_tokens":0,
                    "video_tokens":0
                },
                "cost_details":{
                    "upstream_inference_cost":null,
                    "upstream_inference_prompt_cost":6e-7,
                    "upstream_inference_completions_cost":0.00000663
                },
                "completion_tokens_details":{
                    "reasoning_tokens":0,
                    "image_tokens":0
                }
            }
        }"#;

        let headers = HeaderMap::new();
        let result = sanitizer
            .sanitize(
                "/v1/chat/completions",
                &headers,
                openrouter_response.as_bytes(),
            )
            .unwrap()
            .unwrap();

        let sanitized: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(sanitized["model"], "fred");

        assert_eq!(sanitized["id"], "gen-1768414327-n9nnQrvnhHU8HF0oRVKq");
        assert_eq!(sanitized["object"], "chat.completion");
        assert_eq!(sanitized["created"], 1768414327);

        let choices = sanitized["choices"].as_array().unwrap();
        assert_eq!(choices.len(), 1);
        assert_eq!(choices[0]["index"], 0);
        assert_eq!(choices[0]["finish_reason"], "stop");
        assert_eq!(choices[0]["message"]["role"], "assistant");
        assert!(
            choices[0]["message"]["content"]
                .as_str()
                .unwrap()
                .starts_with("Hello!")
        );

        assert!(sanitized.get("provider").is_none());
        assert!(choices[0].get("native_finish_reason").is_none());

        assert!(choices[0].get("logprobs").is_some());
        assert_eq!(choices[0]["logprobs"], serde_json::Value::Null);

        assert!(sanitized.get("usage").is_some());
        let usage = &sanitized["usage"];
        assert_eq!(usage["prompt_tokens"], 15);
        assert_eq!(usage["completion_tokens"], 51);
        assert_eq!(usage["total_tokens"], 66);

        assert!(usage.get("cost").is_none());
        assert!(usage.get("is_byok").is_none());
        assert!(usage.get("cost_details").is_none());

        assert!(usage.get("prompt_tokens_details").is_some());
        assert!(usage.get("completion_tokens_details").is_some());

        let prompt_details = &usage["prompt_tokens_details"];
        assert_eq!(prompt_details["cached_tokens"], 0);
        assert_eq!(prompt_details["audio_tokens"], 0);
        assert!(prompt_details.get("video_tokens").is_none());

        let completion_details = &usage["completion_tokens_details"];
        assert_eq!(completion_details["reasoning_tokens"], 0);
        assert!(completion_details.get("image_tokens").is_none());
    }

    #[test]
    fn test_streaming_sanitizer_surfaces_embedded_error_verbatim() {
        let sanitizer = ResponseSanitizer {
            original_model: Some("gpt-4".to_string()),
        };
        let body = b"data: {\"id\":\"gen-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"openai/gpt-oss-20b\",\"choices\":[],\"error\":{\"message\":\"Provider rate limited\",\"code\":429}}\n\n";
        let result = sanitizer.sanitize_streaming(body).unwrap().unwrap();
        let out = std::str::from_utf8(&result).unwrap();

        assert!(
            out.contains("data: {\"error\""),
            "expected stand-alone error event, got: {out}"
        );
        assert!(
            out.contains("\"code\":429"),
            "expected code 429 preserved, got: {out}"
        );
        assert!(
            out.contains("Provider rate limited"),
            "expected verbatim provider message, got: {out}"
        );
    }

    #[test]
    fn test_streaming_sanitizer_ignores_empty_error_object() {
        let sanitizer = ResponseSanitizer {
            original_model: None,
        };
        let body =
            b"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"m\",\"choices\":[],\"error\":{}}\n\n";
        let result = sanitizer.sanitize_streaming(body).unwrap().unwrap();
        let out = std::str::from_utf8(&result).unwrap();
        assert!(
            !out.contains("data: {\"error\""),
            "empty error object must not surface as an error event, got: {out}"
        );
    }

    #[test]
    fn test_streaming_sanitizer_does_not_mask_account_class_code() {
        let sanitizer = ResponseSanitizer {
            original_model: None,
        };
        let body = b"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"m\",\"choices\":[],\"error\":{\"message\":\"insufficient credits\",\"code\":402}}\n\n";
        let result = sanitizer.sanitize_streaming(body).unwrap().unwrap();
        let out = std::str::from_utf8(&result).unwrap();

        assert!(
            out.contains("data: {\"error\""),
            "expected stand-alone error event, got: {out}"
        );
        assert!(
            out.contains("\"code\":402"),
            "non-strict must preserve the original 402, got: {out}"
        );
        assert!(
            !out.contains("\"code\":502"),
            "non-strict must not mask the code to 502, got: {out}"
        );
        assert!(out.contains("insufficient credits"));
    }
}
