
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::utils::ensure_field;

pub(crate) fn generated_chat_completion_id() -> String {
    format!("chatcmpl-{}", Uuid::new_v4())
}

fn normalize_null_like_tool_call_arguments(message: &mut Value) {
    let Some(tool_calls) = message.get_mut("tool_calls").and_then(Value::as_array_mut) else {
        return;
    };

    for tool_call in tool_calls {
        let Some(function) = tool_call.get_mut("function").and_then(Value::as_object_mut) else {
            continue;
        };

        let null_like = match function.get("arguments") {
            None | Some(Value::Null) => true,
            Some(Value::String(arguments)) => {
                let arguments = arguments.trim();
                arguments.is_empty() || arguments == "null"
            }
            Some(_) => false,
        };

        if null_like {
            function.insert("arguments".to_string(), Value::String("{}".to_string()));
        }
    }
}

fn normalize_chat_message_value(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };

    ensure_field(object, "role", || Value::String("assistant".to_string()));
    ensure_field(object, "content", || Value::Null);

    normalize_null_like_tool_call_arguments(value);
}

fn normalize_chat_choice_value(value: &mut Value, fallback_index: usize) {
    let Some(object) = value.as_object_mut() else {
        return;
    };

    ensure_field(object, "index", || Value::from(fallback_index));
    ensure_field(object, "finish_reason", || Value::Null);
    ensure_field(object, "logprobs", || Value::Null);

    if let Some(message) = object.get_mut("message") {
        normalize_chat_message_value(message);
    }
}

fn normalize_chat_chunk_choice_value(value: &mut Value, fallback_index: usize) {
    let Some(object) = value.as_object_mut() else {
        return;
    };

    ensure_field(object, "index", || Value::from(fallback_index));
    ensure_field(object, "finish_reason", || Value::Null);
    ensure_field(object, "logprobs", || Value::Null);

    if !object.contains_key("delta") {
        object.insert("delta".to_string(), serde_json::json!({}));
    }
}

pub fn normalize_chat_completion_response_value(value: &mut Value, fallback_model: &str) {
    let Some(object) = value.as_object_mut() else {
        return;
    };

    if !object.contains_key("choices") {
        return;
    }

    ensure_field(object, "id", || {
        Value::String(generated_chat_completion_id())
    });
    ensure_field(object, "object", || {
        Value::String("chat.completion".to_string())
    });
    ensure_field(object, "created", || Value::from(0));
    ensure_field(object, "model", || {
        Value::String(fallback_model.to_string())
    });
    ensure_field(object, "usage", || Value::Null);
    ensure_field(object, "system_fingerprint", || Value::Null);
    ensure_field(object, "service_tier", || Value::Null);

    if let Some(choices) = object.get_mut("choices").and_then(Value::as_array_mut) {
        for (index, choice) in choices.iter_mut().enumerate() {
            normalize_chat_choice_value(choice, index);
        }
    }
}

pub fn normalize_chat_completion_chunk_value(
    value: &mut Value,
    fallback_model: &str,
    fallback_id: &str,
) {
    let Some(object) = value.as_object_mut() else {
        return;
    };

    if !object.contains_key("choices") {
        return;
    }

    ensure_field(object, "id", || Value::String(fallback_id.to_string()));
    ensure_field(object, "object", || {
        Value::String("chat.completion.chunk".to_string())
    });
    ensure_field(object, "created", || Value::from(0));
    ensure_field(object, "model", || {
        Value::String(fallback_model.to_string())
    });
    ensure_field(object, "usage", || Value::Null);
    ensure_field(object, "system_fingerprint", || Value::Null);
    ensure_field(object, "service_tier", || Value::Null);

    if let Some(choices) = object.get_mut("choices").and_then(Value::as_array_mut) {
        for (index, choice) in choices.iter_mut().enumerate() {
            normalize_chat_chunk_choice_value(choice, index);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,

    pub messages: Vec<ChatMessage>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<StreamOptions>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<StopSequence>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_completion_tokens: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub logit_bias: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_logprobs: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Tool>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ResponseFormat>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,

    #[serde(flatten)]
    pub extra: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<MessageContent>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_details: Option<Vec<serde_json::Value>>,

    #[serde(flatten)]
    pub extra: Option<serde_json::Value>,
}

fn collapse_reasoning_fields(
    reasoning: &mut Option<String>,
    reasoning_content: &mut Option<String>,
) {
    let taken = reasoning.take();

    if reasoning_content.is_none() {
        *reasoning_content = taken;
    }
}

impl ChatMessage {
    pub fn canonicalise_reasoning(&mut self) {
        collapse_reasoning_fields(&mut self.reasoning, &mut self.reasoning_content);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    Text(String),
    Parts(Vec<ContentPart>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ContentPart {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "image_url")]
    ImageUrl { image_url: ImageUrl },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageUrl {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_usage: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StopSequence {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tool {
    #[serde(rename = "type")]
    pub tool_type: String,
    pub function: FunctionDefinition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionDefinition {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ToolChoice {
    Mode(String),
    Specific {
        #[serde(rename = "type")]
        tool_type: String,
        function: ToolChoiceFunction,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolChoiceFunction {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub call_type: String,
    pub function: FunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseFormat {
    #[serde(rename = "type")]
    pub format_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub json_schema: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionResponse {
    pub id: String,
    pub object: String,
    pub created: u64,
    pub model: String,
    pub choices: Vec<Choice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Choice {
    pub index: u32,
    pub message: ChatMessage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens_details: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens_details: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionChunk {
    pub id: String,
    pub object: String,
    pub created: u64,
    pub model: String,
    pub choices: Vec<ChunkChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkChoice {
    pub index: u32,
    pub delta: ChunkDelta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ChunkToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_details: Option<Vec<serde_json::Value>>,
}

impl ChunkDelta {
    pub fn canonicalise_reasoning(&mut self) {
        collapse_reasoning_fields(&mut self.reasoning, &mut self.reasoning_content);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkToolCall {
    pub index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "type")]
    pub call_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<ChunkFunctionCall>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkFunctionCall {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_simple_request() {
        let json = r#"{
            "model": "gpt-4",
            "messages": [
                {"role": "user", "content": "Hello"}
            ]
        }"#;

        let request: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.model, "gpt-4");
        assert_eq!(request.messages.len(), 1);
    }

    #[test]
    fn test_deserialize_multimodal_content() {
        let json = r#"{
            "model": "gpt-4-vision",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "What's in this image?"},
                        {"type": "image_url", "image_url": {"url": "https://example.com/image.jpg"}}
                    ]
                }
            ]
        }"#;

        let request: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.messages.len(), 1);
        if let Some(MessageContent::Parts(parts)) = &request.messages[0].content {
            assert_eq!(parts.len(), 2);
        } else {
            panic!("Expected parts content");
        }
    }

    #[test]
    fn test_deserialize_with_tools() {
        let json = r#"{
            "model": "gpt-4",
            "messages": [{"role": "user", "content": "What's the weather?"}],
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "get_weather",
                        "description": "Get the weather",
                        "parameters": {"type": "object", "properties": {}}
                    }
                }
            ]
        }"#;

        let request: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert!(request.tools.is_some());
        assert_eq!(request.tools.unwrap().len(), 1);
    }

    #[test]
    fn test_serialize_response() {
        let response = ChatCompletionResponse {
            id: "chatcmpl-123".to_string(),
            object: "chat.completion".to_string(),
            created: 1234567890,
            model: "gpt-4".to_string(),
            choices: vec![Choice {
                index: 0,
                message: ChatMessage {
                    role: "assistant".to_string(),
                    content: Some(MessageContent::Text("Hello!".to_string())),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                    reasoning: None,
                    reasoning_content: None,
                    reasoning_details: None,
                    extra: None,
                },
                finish_reason: Some("stop".to_string()),
                logprobs: None,
            }],
            usage: Some(Usage {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
                prompt_tokens_details: None,
                completion_tokens_details: None,
            }),
            system_fingerprint: None,
            service_tier: None,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("chatcmpl-123"));
    }

    #[test]
    fn test_nonstreaming_response_with_reasoning_roundtrips() {
        let json = r#"{
            "id": "chatcmpl-abc",
            "object": "chat.completion",
            "created": 1700000000,
            "model": "deepseek-r1",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "The answer is 4.",
                    "reasoning": "Let me think step by step...",
                    "reasoning_details": [{"type": "text", "text": "step 1"}]
                },
                "finish_reason": "stop"
            }]
        }"#;

        let response: ChatCompletionResponse = serde_json::from_str(json).unwrap();
        let msg = &response.choices[0].message;
        assert_eq!(
            msg.reasoning.as_deref(),
            Some("Let me think step by step...")
        );
        assert!(msg.reasoning_details.is_some());
        assert_eq!(msg.reasoning_details.as_ref().unwrap().len(), 1);

        let serialized = serde_json::to_string(&response).unwrap();
        let round_tripped: ChatCompletionResponse = serde_json::from_str(&serialized).unwrap();
        assert_eq!(round_tripped.choices[0].message.reasoning, msg.reasoning);
    }

    #[test]
    fn test_streaming_chunk_with_reasoning_roundtrips() {
        let json = r#"{
            "id": "chatcmpl-abc",
            "object": "chat.completion.chunk",
            "created": 1700000000,
            "model": "deepseek-r1",
            "choices": [{
                "index": 0,
                "delta": {
                    "reasoning": "thinking...",
                    "reasoning_details": [{"type": "text", "text": "step"}]
                },
                "finish_reason": null
            }]
        }"#;

        let chunk: ChatCompletionChunk = serde_json::from_str(json).unwrap();
        let delta = &chunk.choices[0].delta;
        assert_eq!(delta.reasoning.as_deref(), Some("thinking..."));
        assert!(delta.reasoning_details.is_some());

        let serialized = serde_json::to_string(&chunk).unwrap();
        let round_tripped: ChatCompletionChunk = serde_json::from_str(&serialized).unwrap();
        assert_eq!(round_tripped.choices[0].delta.reasoning, delta.reasoning);
    }

    #[test]
    fn test_streaming_chunk_with_reasoning_content_vllm_roundtrips() {
        let json = r#"{
            "id": "chatcmpl-abc",
            "object": "chat.completion.chunk",
            "created": 1700000000,
            "model": "deepseek-r1",
            "choices": [{
                "index": 0,
                "delta": {
                    "reasoning_content": "let me reason about this..."
                },
                "finish_reason": null
            }]
        }"#;

        let chunk: ChatCompletionChunk = serde_json::from_str(json).unwrap();
        let delta = &chunk.choices[0].delta;
        assert_eq!(
            delta.reasoning_content.as_deref(),
            Some("let me reason about this...")
        );
        assert!(delta.reasoning.is_none());

        let serialized = serde_json::to_string(&chunk).unwrap();
        assert!(serialized.contains("reasoning_content"));
        assert!(!serialized.contains("\"reasoning\""));
    }

    #[test]
    fn canonicalise_moves_openrouter_reasoning_onto_reasoning_content() {
        let mut msg = ChatMessage {
            role: "assistant".to_string(),
            content: None,
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning: Some("thinking...".to_string()),
            reasoning_content: None,
            reasoning_details: None,
            extra: None,
        };

        msg.canonicalise_reasoning();

        assert_eq!(msg.reasoning_content.as_deref(), Some("thinking..."));
        assert!(
            msg.reasoning.is_none(),
            "the OpenRouter spelling should not survive canonicalisation"
        );
    }

    #[test]
    fn canonicalise_leaves_vllm_reasoning_content_untouched() {
        let mut msg = ChatMessage {
            role: "assistant".to_string(),
            content: None,
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning: None,
            reasoning_content: Some("let me reason...".to_string()),
            reasoning_details: None,
            extra: None,
        };

        msg.canonicalise_reasoning();

        assert_eq!(msg.reasoning_content.as_deref(), Some("let me reason..."));
        assert!(msg.reasoning.is_none());
    }

    #[test]
    fn canonicalise_prefers_reasoning_content_when_both_present() {
        let mut msg = ChatMessage {
            role: "assistant".to_string(),
            content: None,
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning: Some("openrouter copy".to_string()),
            reasoning_content: Some("vllm copy".to_string()),
            reasoning_details: None,
            extra: None,
        };

        msg.canonicalise_reasoning();

        assert_eq!(msg.reasoning_content.as_deref(), Some("vllm copy"));
        assert!(msg.reasoning.is_none());
    }

    #[test]
    fn canonicalise_preserves_reasoning_details() {
        let mut msg = ChatMessage {
            role: "assistant".to_string(),
            content: None,
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning: Some("thinking...".to_string()),
            reasoning_content: None,
            reasoning_details: Some(vec![serde_json::json!({"type": "text", "text": "step 1"})]),
            extra: None,
        };

        msg.canonicalise_reasoning();

        assert_eq!(msg.reasoning_content.as_deref(), Some("thinking..."));
        assert!(
            msg.reasoning_details.is_some(),
            "structured details carry data the flat field cannot represent"
        );
    }

    #[test]
    fn canonicalise_on_delta_matches_message_behaviour() {
        let mut delta = ChunkDelta {
            role: None,
            content: None,
            tool_calls: None,
            reasoning: Some("streaming fragment".to_string()),
            reasoning_content: None,
            reasoning_details: None,
        };

        delta.canonicalise_reasoning();

        assert_eq!(
            delta.reasoning_content.as_deref(),
            Some("streaming fragment")
        );
        assert!(delta.reasoning.is_none());

        let serialized = serde_json::to_string(&delta).unwrap();
        assert!(serialized.contains("reasoning_content"));
        assert!(!serialized.contains("\"reasoning\""));
    }

    #[test]
    fn canonicalise_is_a_noop_without_reasoning() {
        let mut delta = ChunkDelta {
            role: None,
            content: Some("hello".to_string()),
            tool_calls: None,
            reasoning: None,
            reasoning_content: None,
            reasoning_details: None,
        };

        delta.canonicalise_reasoning();

        assert!(delta.reasoning.is_none());
        assert!(delta.reasoning_content.is_none());
        assert_eq!(delta.content.as_deref(), Some("hello"));
    }

    #[test]
    fn test_chunk_without_reasoning_no_regression() {
        let json = r#"{
            "id": "chatcmpl-abc",
            "object": "chat.completion.chunk",
            "created": 1700000000,
            "model": "gpt-4",
            "choices": [{
                "index": 0,
                "delta": {
                    "content": "Hello!"
                },
                "finish_reason": null
            }]
        }"#;

        let chunk: ChatCompletionChunk = serde_json::from_str(json).unwrap();
        let delta = &chunk.choices[0].delta;
        assert_eq!(delta.content.as_deref(), Some("Hello!"));
        assert!(delta.reasoning.is_none());
        assert!(delta.reasoning_content.is_none());
        assert!(delta.reasoning_details.is_none());

        let serialized = serde_json::to_string(&chunk).unwrap();
        assert!(!serialized.contains("reasoning"));
    }
}
