
pub mod anthropic;
pub mod middleware;
pub mod responses;

use axum::http::{HeaderMap, StatusCode, Uri, request::Parts};
use bytes::Bytes;
use onwards::reasoning::ReasoningError;
use onwards::strict::schema_mismatch::{SchemaMismatch, locate_declared};
use serde::de::DeserializeOwned;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum TranslationError {
    #[error("{0}")]
    InvalidJson(String),
    #[error("{message}")]
    SchemaMismatch { mismatch: Option<SchemaMismatch>, message: String },
    #[error("{message}")]
    InvalidParameter { param: &'static str, message: String },
    #[error("{message}")]
    UnsupportedParameter { param: &'static str, message: String },
    #[error("{0}")]
    Reasoning(ReasoningError),
    #[error("{0}")]
    Internal(String),
}

pub(crate) fn unsupported_server_tool(tool_type: &str) -> TranslationError {
    TranslationError::UnsupportedParameter {
        param: "tools",
        message: format!("server-side tool '{tool_type}' is not supported; remove it from 'tools' or use a client-executed tool"),
    }
}

pub(crate) fn parse_request<T: DeserializeOwned>(body: &[u8], declared: &[&str], protocol: &str) -> Result<T, TranslationError> {
    serde_json::from_slice(body).map_err(|e| {
        let message = format!("invalid {protocol} request: {e}");
        if e.is_data() {
            TranslationError::SchemaMismatch {
                mismatch: locate_declared::<T>(body, declared),
                message,
            }
        } else {
            TranslationError::InvalidJson(message)
        }
    })
}

pub struct TranslatedRequest {
    pub uri: Uri,
    pub headers: HeaderMap,
    pub body: Bytes,
}

pub trait ProtocolTranslator: Send + Sync {
    fn name(&self) -> &'static str;

    fn detect(&self, path: &str, headers: &HeaderMap) -> bool;

    fn translates_request_body(&self) -> bool {
        true
    }

    fn translate_request(&self, parts: &Parts, body: Bytes) -> Result<TranslatedRequest, TranslationError>;

    fn translate_response(&self, request: &Bytes, response_id: Option<&str>, body: Bytes) -> Result<Bytes, TranslationError>;

    fn translate_error(&self, status: StatusCode, body: Bytes) -> (StatusCode, Bytes);

    fn error_from_message(&self, status: StatusCode, message: &str) -> (StatusCode, Bytes);

    fn error_for_param(&self, status: StatusCode, message: &str, _param: Option<&str>) -> (StatusCode, Bytes) {
        self.error_from_message(status, message)
    }

    fn error_for_code(&self, status: StatusCode, message: &str, param: Option<&str>, _code: &str) -> (StatusCode, Bytes) {
        self.error_for_param(status, message, param)
    }

    fn stream_reframer(&self, request: &Bytes, response_id: Option<&str>) -> Box<dyn StreamReframer>;
}

pub trait StreamReframer: Send {
    fn push(&mut self, chunk: &serde_json::Value) -> Vec<u8>;

    fn error(&mut self, message: &str) -> Vec<u8>;

    fn finish(&mut self) -> Vec<u8>;
}

#[derive(Clone)]
pub struct TranslationRegistry {
    translators: Vec<Arc<dyn ProtocolTranslator>>,
    max_body_size: usize,
    sse_buffer_limit: usize,
}

impl TranslationRegistry {
    pub fn new(translators: Vec<Arc<dyn ProtocolTranslator>>) -> Self {
        Self {
            translators,
            max_body_size: usize::MAX,
            sse_buffer_limit: onwards::sse::DEFAULT_SSE_BUFFER_LIMIT,
        }
    }

    pub fn with_max_body_size(mut self, max_body_size: usize) -> Self {
        self.max_body_size = max_body_size;
        self
    }

    pub fn max_body_size(&self) -> usize {
        self.max_body_size
    }

    pub fn with_sse_buffer_limit(mut self, sse_buffer_limit: usize) -> Self {
        self.sse_buffer_limit = sse_buffer_limit;
        self
    }

    pub fn sse_buffer_limit(&self) -> usize {
        self.sse_buffer_limit
    }

    pub fn detect(&self, path: &str, headers: &HeaderMap) -> Option<Arc<dyn ProtocolTranslator>> {
        self.translators.iter().find(|t| t.detect(path, headers)).cloned()
    }
}
