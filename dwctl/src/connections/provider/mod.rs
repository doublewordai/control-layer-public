
pub mod s3;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalFile {
    pub key: String,
    pub size_bytes: Option<i64>,
    pub last_modified: Option<DateTime<Utc>>,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionTestResult {
    pub ok: bool,
    pub message: Option<String>,
    pub scope: Option<serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("access denied: {0}")]
    AccessDenied(String),

    #[error("provider error: {0}")]
    Internal(String),

    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, ProviderError>> + Send>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileListPage {
    pub files: Vec<ExternalFile>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ListFilesOptions {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub search: Option<String>,
}

#[async_trait]
pub trait SourceProvider: Send + Sync {
    fn provider_type(&self) -> &str;

    async fn list_files(&self) -> Result<Vec<ExternalFile>, ProviderError>;

    async fn list_files_paged(&self, options: ListFilesOptions) -> Result<FileListPage, ProviderError>;

    async fn stream_file(&self, file_key: &str) -> Result<ByteStream, ProviderError>;

    async fn test_connection(&self) -> Result<ConnectionTestResult, ProviderError>;
}

pub fn create_provider(provider_type: &str, config: serde_json::Value) -> Result<Box<dyn SourceProvider>, ProviderError> {
    match provider_type {
        "s3" => {
            let s3_config: s3::S3Config = serde_json::from_value(config).map_err(|e| ProviderError::InvalidConfig(e.to_string()))?;
            Ok(Box::new(s3::S3Provider::new(s3_config)))
        }
        other => Err(ProviderError::InvalidConfig(format!("unsupported provider: {other}"))),
    }
}
