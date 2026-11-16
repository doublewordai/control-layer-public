
use thiserror::Error;

use crate::types::RequestId;

pub type Result<T> = std::result::Result<T, FusilladeError>;

#[derive(Error, Debug)]
pub enum FusilladeError {
    #[error("Request not found: {0}")]
    RequestNotFound(RequestId),

    #[error("Request {id} is in state '{current_state}', expected one of: {expected}")]
    RequestStateConflict {
        id: RequestId,
        current_state: String,
        expected: &'static str,
    },

    #[error("Request cancelled: {0}")]
    RequestCancelled(RequestId),

    #[error("Daemon is shutting down")]
    Shutdown,

    #[error("Invalid state transition: request {0} is in state '{1}', expected '{2}'")]
    InvalidState(RequestId, String, String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("HTTP request failed: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("First chunk timeout: {0}")]
    FirstChunkTimeout(String),

    #[error("Tokens timeout: {0}")]
    TokensTimeout(String),

    #[error("Body timeout: {0}")]
    BodyTimeout(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub mod error_serialization {
    use anyhow::Error;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SerializedError {
        pub message: String,
        pub sources: Vec<String>,
    }

    pub fn serialize_error(error: &Error) -> String {
        let serialized = SerializedError {
            message: error.to_string(),
            sources: error.chain().skip(1).map(|e| e.to_string()).collect(),
        };
        serde_json::to_string(&serialized).unwrap_or_else(|_| {
            format!(
                r#"{{"message":"{}","sources":[]}}"#,
                error.to_string().replace('"', "\\\"")
            )
        })
    }

    pub fn deserialize_error(json: &str) -> Error {
        match serde_json::from_str::<SerializedError>(json) {
            Ok(serialized) => {
                let mut error_msg = serialized.message;
                if !serialized.sources.is_empty() {
                    error_msg.push_str("\nCaused by:\n");
                    for (i, source) in serialized.sources.iter().enumerate() {
                        error_msg.push_str(&format!("  {}: {}\n", i + 1, source));
                    }
                }
                anyhow::anyhow!(error_msg)
            }
            Err(_) => {
                anyhow::anyhow!("Deserialization failed: {}", json)
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_serialize_deserialize_simple_error() {
            let error = anyhow::anyhow!("Test error");
            let serialized = serialize_error(&error);
            let deserialized = deserialize_error(&serialized);
            assert_eq!(error.to_string(), deserialized.to_string());
        }

        #[test]
        fn test_serialize_deserialize_with_context() {
            let error = anyhow::anyhow!("Root cause")
                .context("Middle context")
                .context("Top context");
            let serialized = serialize_error(&error);
            let deserialized = deserialize_error(&serialized);
            assert!(deserialized.to_string().contains("Top context"));
            assert!(deserialized.to_string().contains("Middle context"));
            assert!(deserialized.to_string().contains("Root cause"));
        }
    }
}
