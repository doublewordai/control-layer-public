
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FileId(pub Uuid);

impl From<Uuid> for FileId {
    fn from(uuid: Uuid) -> Self {
        FileId(uuid)
    }
}

impl std::ops::Deref for FileId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for FileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0.to_string()[..8])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct BatchId(pub Uuid);

impl From<Uuid> for BatchId {
    fn from(uuid: Uuid) -> Self {
        BatchId(uuid)
    }
}

impl std::ops::Deref for BatchId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for BatchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct TemplateId(pub Uuid);

impl From<Uuid> for TemplateId {
    fn from(uuid: Uuid) -> Self {
        TemplateId(uuid)
    }
}

impl std::ops::Deref for TemplateId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for TemplateId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0.to_string()[..8])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    Batch,
    BatchOutput,
    BatchError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFileType {
    Output,
    Error,
}

impl fmt::Display for Purpose {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Purpose::Batch => write!(f, "batch"),
            Purpose::BatchOutput => write!(f, "batch_output"),
            Purpose::BatchError => write!(f, "batch_error"),
        }
    }
}

impl FromStr for Purpose {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "batch" => Ok(Purpose::Batch),
            "batch_output" => Ok(Purpose::BatchOutput),
            "batch_error" => Ok(Purpose::BatchError),
            _ => Err(format!("Invalid purpose: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileStatus {
    Processed,
    Error,
    Deleted,
    Expired,
}

impl fmt::Display for FileStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileStatus::Processed => write!(f, "processed"),
            FileStatus::Error => write!(f, "error"),
            FileStatus::Deleted => write!(f, "deleted"),
            FileStatus::Expired => write!(f, "expired"),
        }
    }
}

impl FromStr for FileStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "processed" => Ok(FileStatus::Processed),
            "error" => Ok(FileStatus::Error),
            "deleted" => Ok(FileStatus::Deleted),
            "expired" => Ok(FileStatus::Expired),
            _ => Err(format!("Invalid file status: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct File {
    pub id: FileId,
    pub name: String,
    pub description: Option<String>,
    pub size_bytes: i64,
    pub status: FileStatus,
    pub error_message: Option<String>,
    pub purpose: Option<Purpose>,
    pub expires_at: Option<DateTime<Utc>>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub uploaded_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub size_finalized: bool,
    pub api_key_id: Option<Uuid>,
    pub source_connection_id: Option<Uuid>,
    pub source_external_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct RequestTemplateInput {
    pub custom_id: Option<String>,
    pub endpoint: String,
    pub method: String,
    pub path: String,
    pub body: String,
    pub model: String,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct BatchOutputItem {
    pub id: String,
    pub custom_id: Option<String>,
    pub response: BatchResponseDetails,
    pub error: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct BatchErrorItem {
    pub id: String,
    pub custom_id: Option<String>,
    pub response: Option<serde_json::Value>,
    pub error: BatchErrorDetails,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct BatchResponseDetails {
    pub status_code: i16,
    pub request_id: Option<String>,
    pub body: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct BatchErrorDetails {
    pub code: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum FileContentItem {
    Template(RequestTemplateInput),
    Output(BatchOutputItem),
    Error(BatchErrorItem),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchResultStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct BatchResultItem {
    pub id: String,
    pub custom_id: Option<String>,
    pub model: String,
    pub input_body: serde_json::Value,
    pub response_body: Option<serde_json::Value>,
    pub error: Option<String>,
    pub status: BatchResultStatus,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct FileMetadata {
    pub filename: Option<String>,
    pub description: Option<String>,
    pub purpose: Option<String>,
    pub expires_after_anchor: Option<String>,
    pub expires_after_seconds: Option<i64>,
    pub size_bytes: Option<i64>,
    pub uploaded_by: Option<String>,
    pub api_key_id: Option<Uuid>,
    pub source_connection_id: Option<Uuid>,
    pub source_external_key: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct FileFilter {
    pub uploaded_by: Option<String>,
    pub status: Option<String>,
    pub purpose: Option<String>,
    pub search: Option<String>,
    pub after: Option<FileId>,
    pub limit: Option<usize>,
    pub api_key_ids: Option<Vec<Uuid>>,
    pub ascending: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ListBatchesFilter {
    pub created_by: Option<String>,
    pub search: Option<String>,
    pub after: Option<BatchId>,
    pub limit: Option<i64>,
    pub api_key_ids: Option<Vec<Uuid>>,
    pub status: Option<String>,
    pub created_after: Option<DateTime<Utc>>,
    pub created_before: Option<DateTime<Utc>>,
    pub active_first: bool,
    pub completion_windows: Option<Vec<String>>,
    pub service_tiers: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub enum FileStreamItem {
    Metadata(FileMetadata),
    Template(RequestTemplateInput),
    Abort,
    #[deprecated(note = "Use FileStreamItem::Abort and retain the producer error locally instead")]
    Error(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileStreamResult {
    Success(FileId),
    Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchInput {
    pub file_id: FileId,
    pub endpoint: String,
    pub completion_window: String,
    pub metadata: Option<serde_json::Value>,
    pub created_by: Option<String>,
    pub api_key_id: Option<Uuid>,
    pub total_requests: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackgroundBatchInput {
    pub file_id: FileId,
    pub endpoint: String,
    pub metadata: Option<serde_json::Value>,
    pub created_by: Option<String>,
    pub api_key_id: Option<Uuid>,
    pub total_requests: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Batch {
    pub id: BatchId,
    pub file_id: Option<FileId>,
    pub created_at: DateTime<Utc>,
    pub metadata: Option<serde_json::Value>,
    pub service_tier: Option<String>,
    pub completion_window: Option<String>,
    pub endpoint: String,
    pub output_file_id: Option<FileId>,
    pub error_file_id: Option<FileId>,
    pub created_by: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub cancelling_at: Option<DateTime<Utc>>,
    pub errors: Option<serde_json::Value>,

    pub total_requests: i64,
    pub pending_requests: i64,
    pub in_progress_requests: i64,
    pub completed_requests: i64,
    pub failed_requests: i64,
    pub canceled_requests: i64,
    pub requests_started_at: Option<DateTime<Utc>>,

    pub finalizing_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,

    pub deleted_at: Option<DateTime<Utc>>,

    pub notification_sent_at: Option<DateTime<Utc>>,

    pub api_key_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct BatchNotification {
    pub batch: Batch,
    pub model: String,
    pub input_file_name: Option<String>,
    pub input_file_description: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BatchStatus {
    pub batch_id: BatchId,
    pub file_id: Option<FileId>,
    pub file_name: Option<String>,
    pub total_requests: i64,
    pub pending_requests: i64,
    pub in_progress_requests: i64,
    pub completed_requests: i64,
    pub failed_requests: i64,
    pub canceled_requests: i64,
    pub started_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl BatchStatus {
    pub fn is_finished(&self) -> bool {
        self.completed_requests + self.failed_requests + self.canceled_requests
            == self.total_requests
    }

    pub fn openai_status(&self) -> &'static str {
        if self.failed_at.is_some() {
            return "failed";
        }

        if self.started_at.is_none() {
            return "validating";
        }

        let terminal_count =
            self.completed_requests + self.failed_requests + self.canceled_requests;

        if terminal_count == 0 {
            "in_progress"
        } else if terminal_count == self.total_requests {
            if self.canceled_requests == self.total_requests {
                "cancelled"
            } else if self.completed_requests == 0 {
                "failed"
            } else {
                "completed"
            }
        } else if terminal_count as f64 / self.total_requests as f64 >= 0.95 {
            "finalizing"
        } else {
            "in_progress"
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelTemplateStats {
    pub model: String,
    pub request_count: i64,
    pub total_body_bytes: i64,
}

#[cfg(test)]
mod background_tests {
    use super::*;

    #[test]
    fn background_batch_input_does_not_require_a_completion_window() {
        let file_id = FileId(Uuid::new_v4());
        let input = BackgroundBatchInput {
            file_id,
            endpoint: "/v1/responses".to_string(),
            metadata: None,
            created_by: Some("user-a".to_string()),
            api_key_id: None,
            total_requests: Some(2),
        };

        assert_eq!(input.file_id, file_id);
        assert_eq!(input.total_requests, Some(2));
    }

    #[test]
    fn background_batch_serializes_without_a_deadline() {
        let batch = Batch {
            id: BatchId(Uuid::new_v4()),
            file_id: Some(FileId(Uuid::new_v4())),
            created_at: Utc::now(),
            metadata: None,
            service_tier: Some("background".to_string()),
            completion_window: None,
            endpoint: "/v1/responses".to_string(),
            output_file_id: None,
            error_file_id: None,
            created_by: "user-a".to_string(),
            expires_at: None,
            cancelling_at: None,
            errors: None,
            total_requests: 0,
            pending_requests: 0,
            in_progress_requests: 0,
            completed_requests: 0,
            failed_requests: 0,
            canceled_requests: 0,
            requests_started_at: None,
            finalizing_at: None,
            completed_at: None,
            failed_at: None,
            cancelled_at: None,
            deleted_at: None,
            notification_sent_at: None,
            api_key_id: None,
        };

        let json = serde_json::to_value(batch).expect("batch should serialize");
        assert_eq!(json["service_tier"], "background");
        assert!(json["completion_window"].is_null());
        assert!(json["expires_at"].is_null());
    }
}
