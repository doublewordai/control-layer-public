
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc};
use tokio::task::AbortHandle;
use uuid::Uuid;

use crate::batch::{BatchId, TemplateId};
use crate::error::Result;
use crate::http::HttpResponse;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "sqlx-postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx-postgres",
    sqlx(type_name = "text", rename_all = "lowercase")
)]
pub enum RequestStateFilter {
    Pending,
    Claimed,
    Processing,
    Completed,
    Failed,
    Canceled,
}

pub trait RequestState: Send + Sync {}

#[derive(Debug, Clone, Serialize)]
pub struct Request<T: RequestState> {
    pub state: T,
    pub data: RequestData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RequestData {
    pub id: RequestId,

    pub batch_id: Option<BatchId>,

    pub template_id: TemplateId,

    pub custom_id: Option<String>,

    pub endpoint: String,

    pub method: String,

    pub path: String,

    pub body: String,

    pub model: String,

    pub api_key: String,

    #[serde(default)]
    pub created_by: String,

    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub batch_metadata: std::collections::HashMap<String, String>,
}


#[derive(Debug, Clone, Serialize)]
pub struct Pending {
    pub retry_attempt: u32,

    pub not_before: Option<DateTime<Utc>>,

    pub batch_expires_at: DateTime<Utc>,
}

impl RequestState for Pending {}

#[derive(Debug, Clone, Serialize)]
pub struct LeakStamp {
    pub window_class: String,
    pub window_secs: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Claimed {
    pub daemon_id: DaemonId,
    pub claimed_at: DateTime<Utc>,
    pub retry_attempt: u32,
    pub batch_expires_at: DateTime<Utc>,
    pub leak: Option<LeakStamp>,
}

impl RequestState for Claimed {}

#[derive(Debug, Clone, Serialize)]
pub struct Processing {
    pub daemon_id: DaemonId,
    pub claimed_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub retry_attempt: u32,
    pub batch_expires_at: DateTime<Utc>,
    #[serde(skip)]
    pub result_rx: Arc<Mutex<mpsc::Receiver<Result<HttpResponse>>>>,
    #[serde(skip)]
    pub abort_handle: AbortHandle,
}

impl RequestState for Processing {}

#[derive(Debug, Clone, Serialize)]
pub struct Completed {
    pub response_status: u16,
    pub response_body: String,
    pub claimed_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub routed_model: String,
}

impl RequestState for Completed {}

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "details")]
pub enum FailureReason {
    RetriableHttpStatus { status: u16, body: String },

    NonRetriableHttpStatus { status: u16, body: String },

    NetworkError { error: String },

    Timeout { error: String },

    TaskTerminated,

    RequestBuilderError { error: String },

    BatchTerminated,
}

impl FailureReason {
    pub fn is_retriable(&self) -> bool {
        match self {
            FailureReason::RetriableHttpStatus { .. } => true,
            FailureReason::NonRetriableHttpStatus { .. } => false,
            FailureReason::NetworkError { .. } => true,
            FailureReason::Timeout { .. } => true,
            FailureReason::TaskTerminated => true,
            FailureReason::RequestBuilderError { .. } => false,
            FailureReason::BatchTerminated => false,
        }
    }

    pub fn metric_label(&self) -> &'static str {
        match self {
            FailureReason::RetriableHttpStatus { .. } => "retriable_http_status",
            FailureReason::NonRetriableHttpStatus { .. } => "non_retriable_http_status",
            FailureReason::NetworkError { .. } => "network_error",
            FailureReason::Timeout { .. } => "timeout",
            FailureReason::TaskTerminated => "task_terminated",
            FailureReason::RequestBuilderError { .. } => "builder_error",
            FailureReason::BatchTerminated => "batch_terminated",
        }
    }

    pub fn status_code_label(&self) -> String {
        match self {
            FailureReason::RetriableHttpStatus { status, .. }
            | FailureReason::NonRetriableHttpStatus { status, .. } => status.to_string(),
            _ => String::new(),
        }
    }

    pub fn to_error_message(&self) -> String {
        match self {
            FailureReason::RetriableHttpStatus { status, body } => {
                format!(
                    "HTTP request returned retriable status code: {} ({}-byte body omitted)",
                    status,
                    body.len()
                )
            }
            FailureReason::NonRetriableHttpStatus { status, body } => {
                format!(
                    "HTTP request returned error status code: {} ({}-byte body omitted)",
                    status,
                    body.len()
                )
            }
            FailureReason::NetworkError { error } => {
                format!("Network error: {}", error)
            }
            FailureReason::Timeout { error } => {
                format!("Request timed out: {}", error)
            }
            FailureReason::TaskTerminated => "HTTP task terminated unexpectedly".to_string(),
            FailureReason::BatchTerminated => {
                "Request was not processed because its batch reached a terminal state".to_string()
            }
            FailureReason::RequestBuilderError { error } => {
                format!("Failed to build HTTP request: {}", error)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Failed {
    pub reason: FailureReason,
    pub failed_at: DateTime<Utc>,
    pub retry_attempt: u32,
    pub batch_expires_at: DateTime<Utc>,
    pub routed_model: String,
}

impl RequestState for Failed {}

#[derive(Debug, Clone, Serialize)]
pub struct Canceled {
    pub canceled_at: DateTime<Utc>,
}

impl RequestState for Canceled {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CascadeTargetState {
    Canceled,
    Failed,
}

impl CascadeTargetState {
    pub fn as_str(&self) -> &'static str {
        match self {
            CascadeTargetState::Canceled => "canceled",
            CascadeTargetState::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct RequestId(pub Uuid);

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0.to_string()[..8])
    }
}

impl From<Uuid> for RequestId {
    fn from(uuid: Uuid) -> Self {
        RequestId(uuid)
    }
}

impl std::ops::Deref for RequestId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct DaemonId(pub Uuid);

impl std::fmt::Display for DaemonId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0.to_string()[..8])
    }
}

impl From<Uuid> for DaemonId {
    fn from(uuid: Uuid) -> Self {
        DaemonId(uuid)
    }
}

impl std::ops::Deref for DaemonId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


#[derive(Debug)]
pub enum RequestCompletionResult {
    Completed(Request<Completed>),
    Failed(Request<Failed>),
    Canceled(Request<Canceled>),
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "state", content = "request")]
pub enum AnyRequest {
    Pending(Request<Pending>),
    Claimed(Request<Claimed>),
    Processing(Request<Processing>),
    Completed(Request<Completed>),
    Failed(Request<Failed>),
    Canceled(Request<Canceled>),
}

impl AnyRequest {
    pub fn id(&self) -> RequestId {
        match self {
            AnyRequest::Pending(r) => r.data.id,
            AnyRequest::Claimed(r) => r.data.id,
            AnyRequest::Processing(r) => r.data.id,
            AnyRequest::Completed(r) => r.data.id,
            AnyRequest::Failed(r) => r.data.id,
            AnyRequest::Canceled(r) => r.data.id,
        }
    }

    pub fn variant(&self) -> &'static str {
        match self {
            AnyRequest::Pending(_) => "Pending",
            AnyRequest::Claimed(_) => "Claimed",
            AnyRequest::Processing(_) => "Processing",
            AnyRequest::Completed(_) => "Completed",
            AnyRequest::Failed(_) => "Failed",
            AnyRequest::Canceled(_) => "Canceled",
        }
    }

    pub fn data(&self) -> &RequestData {
        match self {
            AnyRequest::Pending(r) => &r.data,
            AnyRequest::Claimed(r) => &r.data,
            AnyRequest::Processing(r) => &r.data,
            AnyRequest::Completed(r) => &r.data,
            AnyRequest::Failed(r) => &r.data,
            AnyRequest::Canceled(r) => &r.data,
        }
    }

    pub fn is_pending(&self) -> bool {
        matches!(self, AnyRequest::Pending(_))
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            AnyRequest::Completed(_) | AnyRequest::Failed(_) | AnyRequest::Canceled(_)
        )
    }

    pub fn as_pending(&self) -> Option<&Request<Pending>> {
        match self {
            AnyRequest::Pending(r) => Some(r),
            _ => None,
        }
    }

    pub fn into_pending(self) -> Option<Request<Pending>> {
        match self {
            AnyRequest::Pending(r) => Some(r),
            _ => None,
        }
    }
}


impl From<Request<Pending>> for AnyRequest {
    fn from(r: Request<Pending>) -> Self {
        AnyRequest::Pending(r)
    }
}

impl From<Request<Claimed>> for AnyRequest {
    fn from(r: Request<Claimed>) -> Self {
        AnyRequest::Claimed(r)
    }
}

impl From<Request<Processing>> for AnyRequest {
    fn from(r: Request<Processing>) -> Self {
        AnyRequest::Processing(r)
    }
}

impl From<Request<Completed>> for AnyRequest {
    fn from(r: Request<Completed>) -> Self {
        AnyRequest::Completed(r)
    }
}

impl From<Request<Failed>> for AnyRequest {
    fn from(r: Request<Failed>) -> Self {
        AnyRequest::Failed(r)
    }
}

impl From<Request<Canceled>> for AnyRequest {
    fn from(r: Request<Canceled>) -> Self {
        AnyRequest::Canceled(r)
    }
}

#[cfg(test)]
mod zdr_logging_tests {
    use super::*;

    #[test]
    fn to_error_message_omits_provider_body() {
        const SENTINEL: &str = "ZDR-SENTINEL-PROVIDER-BODY-7c1e";

        for reason in [
            FailureReason::RetriableHttpStatus {
                status: 503,
                body: format!("{{\"completion\":\"{SENTINEL}\"}}"),
            },
            FailureReason::NonRetriableHttpStatus {
                status: 400,
                body: format!("{{\"error\":\"{SENTINEL}\"}}"),
            },
        ] {
            let msg = reason.to_error_message();
            assert!(
                !msg.contains(SENTINEL),
                "provider response body leaked into error message: {msg}"
            );
            assert!(
                msg.contains(&reason.status_code_label()),
                "expected status code in message, got: {msg}"
            );
            assert!(!msg.is_empty());
        }
    }
}
