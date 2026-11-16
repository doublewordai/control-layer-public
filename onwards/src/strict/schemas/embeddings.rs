
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingsRequest {
    pub model: String,

    pub input: EmbeddingInput,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding_format: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EmbeddingInput {
    Single(String),
    Multiple(Vec<String>),
    Tokens(Vec<Vec<u32>>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingsResponse {
    pub object: String,
    pub data: Vec<EmbeddingData>,
    pub model: String,
    pub usage: EmbeddingsUsage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingData {
    pub object: String,
    pub embedding: Embedding,
    pub index: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Embedding {
    Float(Vec<f32>),
    Base64(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingsUsage {
    pub prompt_tokens: u32,
    pub total_tokens: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_simple_request() {
        let json = r#"{
            "model": "text-embedding-3-small",
            "input": "Hello, world!"
        }"#;

        let request: EmbeddingsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.model, "text-embedding-3-small");
        assert!(matches!(request.input, EmbeddingInput::Single(_)));
    }

    #[test]
    fn test_deserialize_multiple_inputs() {
        let json = r#"{
            "model": "text-embedding-3-small",
            "input": ["Hello", "World"]
        }"#;

        let request: EmbeddingsRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(request.input, EmbeddingInput::Multiple(_)));
    }

    #[test]
    fn test_deserialize_with_options() {
        let json = r#"{
            "model": "text-embedding-3-small",
            "input": "Hello",
            "encoding_format": "float",
            "dimensions": 256
        }"#;

        let request: EmbeddingsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.encoding_format, Some("float".to_string()));
        assert_eq!(request.dimensions, Some(256));
    }

    #[test]
    fn test_serialize_response() {
        let response = EmbeddingsResponse {
            object: "list".to_string(),
            data: vec![EmbeddingData {
                object: "embedding".to_string(),
                embedding: Embedding::Float(vec![0.1, 0.2, 0.3]),
                index: 0,
            }],
            model: "text-embedding-3-small".to_string(),
            usage: EmbeddingsUsage {
                prompt_tokens: 5,
                total_tokens: 5,
            },
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("text-embedding-3-small"));
    }
}
