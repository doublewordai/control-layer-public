
use std::fmt;

use async_trait::async_trait;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct ResponseContext {
    pub input: Value,
    pub response: Value,
}

#[derive(Debug, Clone)]
pub enum StoreError {
    NotFound(String),
    StorageError(String),
    SerializationError(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::NotFound(id) => write!(f, "Response not found: {id}"),
            StoreError::StorageError(msg) => write!(f, "Storage error: {msg}"),
            StoreError::SerializationError(msg) => write!(f, "Serialization error: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

#[async_trait]
pub trait ResponseStore: Send + Sync {
    async fn store(&self, response: &Value) -> Result<String, StoreError>;

    async fn get_context(&self, response_id: &str, owner: &str) -> Result<Option<ResponseContext>, StoreError>;
}

#[derive(Debug, Clone, Default)]
pub struct NoOpResponseStore;

#[async_trait]
impl ResponseStore for NoOpResponseStore {
    async fn store(&self, _response: &Value) -> Result<String, StoreError> {
        Ok("noop".to_string())
    }
    async fn get_context(&self, _response_id: &str, _owner: &str) -> Result<Option<ResponseContext>, StoreError> {
        Ok(None)
    }
}
