
use std::collections::HashMap;

use onwards::strict::schemas::chat_completions::{ChatCompletionChunk as ChatChunk, ChatCompletionRequest, ChatCompletionResponse};
use onwards::strict::schemas::completions::{CompletionChunk as CompletionStreamChunk, CompletionRequest, CompletionResponse};
use onwards::strict::schemas::embeddings::{EmbeddingsRequest, EmbeddingsResponse};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::inference::translation::anthropic::model::MessagesResponse;
use crate::inference::translation::responses::types::{ResponsesResponse, ResponsesStreamingEvent};

#[derive(Debug, Clone, PartialEq, Error)]
pub enum SseParseError {
    #[error("Input does not contain valid SSE format or contains no data")]
    InvalidFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum AiRequest {
    ChatCompletions(ChatCompletionRequest),
    Embeddings(EmbeddingsRequest),
    Completions(CompletionRequest),
    Other(Value),
}

#[derive(Debug, Clone)]
pub struct ResponsesRequest {
    pub model: Option<String>,
    pub stream: Option<bool>,
}

#[derive(Debug, Clone, Default)]
pub struct MessagesRequest {
    pub model: Option<String>,
    pub stream: Option<bool>,
    pub max_tokens: Option<i64>,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub tool_count: i32,
    pub message_count: Option<i32>,
}

impl MessagesRequest {
    pub fn from_value(value: &Value) -> Self {
        let array_len = |key: &str| {
            value
                .get(key)
                .and_then(Value::as_array)
                .map(|a| i32::try_from(a.len()).unwrap_or(i32::MAX))
        };
        Self {
            model: value.get("model").and_then(Value::as_str).map(String::from),
            stream: value.get("stream").and_then(Value::as_bool),
            max_tokens: value.get("max_tokens").and_then(Value::as_i64),
            temperature: value.get("temperature").and_then(Value::as_f64).map(|t| t as f32),
            top_p: value.get("top_p").and_then(Value::as_f64).map(|t| t as f32),
            tool_count: array_len("tools").unwrap_or(0),
            message_count: array_len("messages"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedAIRequest {
    pub headers: HashMap<String, String>,
    pub request: AiRequest,
    #[serde(skip)]
    pub responses_request: Option<ResponsesRequest>,
    #[serde(skip)]
    pub messages_request: Option<MessagesRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamErrorChunk {
    pub error: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChatCompletionChunk {
    Chunk(Box<ChatChunk>),
    Error(StreamErrorChunk),
    #[serde(rename = "[DONE]")]
    Done,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CompletionChunk {
    Chunk(CompletionStreamChunk),
    Error(StreamErrorChunk),
    #[serde(rename = "[DONE]")]
    Done,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum AiResponse {
    ChatCompletions(ChatCompletionResponse),
    ChatCompletionsStream(Vec<ChatCompletionChunk>),
    Completions(CompletionResponse),
    CompletionsStream(Vec<CompletionChunk>),
    Embeddings(EmbeddingsResponse),
    Responses(ResponsesResponse),
    ResponsesStream(Vec<ResponsesStreamingEvent>),
    Anthropic(MessagesResponse),
    AnthropicStream(Vec<Value>),
    Other(Value),
}

