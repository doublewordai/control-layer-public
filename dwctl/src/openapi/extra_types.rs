
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;


#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "role": "user",
    "content": "What is a doubleword?"
}))]
pub struct ChatMessage {
    #[schema(example = "user")]
    pub role: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "What is a doubleword?")]
    pub content: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "id": "call_abc123",
    "type": "function",
    "function": {
        "name": "get_weather",
        "arguments": "{\"location\": \"San Francisco\"}"
    }
}))]
pub struct ToolCall {
    #[schema(example = "call_abc123")]
    pub id: String,

    #[serde(rename = "type")]
    #[schema(example = "function")]
    pub call_type: String,

    pub function: FunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "get_weather",
    "arguments": "{\"location\": \"San Francisco\"}"
}))]
pub struct FunctionCall {
    #[schema(example = "get_weather")]
    pub name: String,

    #[schema(example = "{\"location\": \"San Francisco\"}")]
    pub arguments: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "model": "Qwen/Qwen3-30B-A3B-FP8",
    "messages": [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What is a doubleword?"}
    ],
    "temperature": 0.7,
    "max_tokens": 256
}))]
pub struct ChatCompletionRequest {
    #[schema(example = "Qwen/Qwen3-30B-A3B-FP8")]
    pub model: String,

    pub messages: Vec<ChatMessage>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 0.7)]
    pub temperature: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 1.0)]
    pub top_p: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 1)]
    pub n: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = false)]
    pub stream: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 256)]
    pub max_tokens: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 0.0)]
    pub presence_penalty: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 0.0)]
    pub frequency_penalty: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Tool>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Get the current weather in a location",
        "parameters": {
            "type": "object",
            "properties": {
                "location": {"type": "string", "description": "City name"}
            },
            "required": ["location"]
        }
    }
}))]
pub struct Tool {
    #[serde(rename = "type")]
    #[schema(example = "function")]
    pub tool_type: String,

    pub function: FunctionDefinition,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "get_weather",
    "description": "Get the current weather in a location",
    "parameters": {
        "type": "object",
        "properties": {
            "location": {"type": "string", "description": "City name"}
        },
        "required": ["location"]
    }
}))]
pub struct FunctionDefinition {
    #[schema(example = "get_weather")]
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "Get the current weather in a location")]
    pub description: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "id": "chatcmpl-abc123",
    "object": "chat.completion",
    "created": 1703187200,
    "model": "Qwen/Qwen3-30B-A3B-FP8",
    "choices": [{
        "index": 0,
        "message": {
            "role": "assistant",
            "content": "A doubleword is a data unit that is twice the size of a standard word in computer architecture, typically 32 or 64 bits depending on the system."
        },
        "finish_reason": "stop"
    }],
    "usage": {
        "prompt_tokens": 24,
        "completion_tokens": 36,
        "total_tokens": 60
    }
}))]
pub struct ChatCompletionResponse {
    #[schema(example = "chatcmpl-abc123")]
    pub id: String,

    #[schema(example = "chat.completion")]
    pub object: String,

    #[schema(example = 1703187200)]
    pub created: i64,

    #[schema(example = "Qwen/Qwen3-30B-A3B-FP8")]
    pub model: String,

    pub choices: Vec<ChatChoice>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "index": 0,
    "message": {
        "role": "assistant",
        "content": "A doubleword is a data unit that is twice the size of a standard word in computer architecture."
    },
    "finish_reason": "stop"
}))]
pub struct ChatChoice {
    #[schema(example = 0)]
    pub index: i32,

    pub message: ChatMessage,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "stop")]
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "prompt_tokens": 24,
    "completion_tokens": 36,
    "total_tokens": 60
}))]
pub struct Usage {
    #[schema(example = 24)]
    pub prompt_tokens: i32,

    #[schema(example = 36)]
    pub completion_tokens: i32,

    #[schema(example = 60)]
    pub total_tokens: i32,
}


#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "model": "Qwen/Qwen3-Embedding-8B",
    "input": "What is a doubleword?"
}))]
pub struct EmbeddingRequest {
    #[schema(example = "Qwen/Qwen3-Embedding-8B")]
    pub model: String,

    pub input: EmbeddingInput,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "float")]
    pub encoding_format: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
#[schema(example = "What is a doubleword?")]
pub enum EmbeddingInput {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "object": "list",
    "data": [{
        "object": "embedding",
        "index": 0,
        "embedding": [0.0023, -0.0134, 0.0256]
    }],
    "model": "Qwen/Qwen3-Embedding-8B",
    "usage": {
        "prompt_tokens": 6,
        "total_tokens": 6
    }
}))]
pub struct EmbeddingResponse {
    #[schema(example = "list")]
    pub object: String,

    pub data: Vec<EmbeddingData>,

    #[schema(example = "Qwen/Qwen3-Embedding-8B")]
    pub model: String,

    pub usage: EmbeddingUsage,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "object": "embedding",
    "index": 0,
    "embedding": [0.0023, -0.0134, 0.0256]
}))]
pub struct EmbeddingData {
    #[schema(example = "embedding")]
    pub object: String,

    #[schema(example = 0)]
    pub index: i32,

    pub embedding: Vec<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "prompt_tokens": 6,
    "total_tokens": 6
}))]
pub struct EmbeddingUsage {
    #[schema(example = 6)]
    pub prompt_tokens: i32,

    #[schema(example = 6)]
    pub total_tokens: i32,
}


#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "object": "list",
    "data": [{
        "id": "Qwen/Qwen3-30B-A3B-FP8",
        "object": "model",
        "created": 1703187200,
        "owned_by": "qwen"
    }]
}))]
pub struct ModelsListResponse {
    #[schema(example = "list")]
    pub object: String,

    pub data: Vec<ModelObject>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "id": "Qwen/Qwen3-30B-A3B-FP8",
    "object": "model",
    "created": 1703187200,
    "owned_by": "qwen"
}))]
pub struct ModelObject {
    #[schema(example = "Qwen/Qwen3-30B-A3B-FP8")]
    pub id: String,

    #[schema(example = "model")]
    pub object: String,

    #[schema(example = 1703187200)]
    pub created: i64,

    #[schema(example = "qwen")]
    pub owned_by: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub supported_reasoning_efforts: Option<crate::reasoning::SupportedReasoningEfforts>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tariffs: Option<Vec<crate::api::models::tariffs::TariffResponse>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_pricing: Option<crate::api::models::cache_pricing::CachePricingResponse>,
}


#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "error": {
        "message": "Invalid API key provided",
        "type": "authentication_error",
        "code": "invalid_api_key"
    }
}))]
pub struct OpenAIErrorResponse {
    pub error: OpenAIError,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "message": "Invalid API key provided",
    "type": "authentication_error",
    "code": "invalid_api_key"
}))]
pub struct OpenAIError {
    #[schema(example = "Invalid API key provided")]
    pub message: String,

    #[serde(rename = "type")]
    #[schema(example = "authentication_error")]
    pub error_type: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub param: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "invalid_api_key")]
    pub code: Option<String>,
}


#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
#[schema(example = "What is a doubleword?")]
pub enum ResponseInput {
    Single(String),
    Messages(Vec<ChatMessage>),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "type": "message",
    "role": "assistant",
    "content": "A doubleword is a data unit that is twice the size of a standard word in computer architecture."
}))]
pub struct ResponseItem {
    #[serde(rename = "type")]
    #[schema(example = "message")]
    pub item_type: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "assistant")]
    pub role: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "A doubleword is a data unit that is twice the size of a standard word.")]
    pub content: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "model": "deepseek-ai/DeepSeek-V4-Pro",
    "input": "What is a doubleword?",
    "temperature": 0.7,
    "max_output_tokens": 256
}))]
pub struct ResponseRequest {
    #[schema(example = "deepseek-ai/DeepSeek-V4-Pro")]
    pub model: String,

    pub input: ResponseInput,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "You are a helpful assistant.")]
    pub instructions: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 0.7)]
    pub temperature: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 1.0)]
    pub top_p: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 256)]
    pub max_output_tokens: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub modalities: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "medium")]
    pub reasoning_effort: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Tool>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = true)]
    pub parallel_tool_calls: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = false)]
    pub stream: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_response_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub include: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = false)]
    pub store: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 0.0)]
    pub presence_penalty: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 0.0)]
    pub frequency_penalty: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "id": "resp-abc123",
    "object": "response",
    "created_at": 1703187200,
    "completed_at": 1703187205,
    "model": "deepseek-ai/DeepSeek-V4-Pro",
    "status": "completed",
    "output": [{
        "type": "message",
        "role": "assistant",
        "content": "A doubleword is a data unit that is twice the size of a standard word in computer architecture."
    }],
    "usage": {
        "prompt_tokens": 10,
        "completion_tokens": 25,
        "total_tokens": 35
    },
    "temperature": 0.7,
    "top_p": 1.0
}))]
pub struct ResponseObject {
    #[schema(example = "resp-abc123")]
    pub id: String,

    #[schema(example = "response")]
    pub object: String,

    #[schema(example = 1703187200)]
    pub created_at: i64,

    #[schema(example = 1703187205)]
    pub completed_at: i64,

    #[schema(example = "deepseek-ai/DeepSeek-V4-Pro")]
    pub model: String,

    #[schema(example = "completed")]
    pub status: String,

    pub output: Vec<ResponseItem>,

    pub usage: Usage,

    #[schema(example = 0.7)]
    pub temperature: f32,

    #[schema(example = 1.0)]
    pub top_p: f32,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}
