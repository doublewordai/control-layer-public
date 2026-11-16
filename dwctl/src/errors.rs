
use crate::db::errors::DbError;
use crate::types::{Operation, Permission};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;

const POOL_EXHAUSTED_RETRY_AFTER_SECS: &str = "30";

pub const INVALID_API_KEY_MESSAGE: &str = "Invalid API key. API keys are region-bound: if you expected this key to work, check that your base URL matches the region the key was created in. See https://docs.doubleword.ai/inference-api/regional-endpoints";

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("Not authenticated")]
    Unauthenticated { message: Option<String> },

    #[error("Insufficient permissions to {action:?} {resource}")]
    InsufficientPermissions {
        required: Permission,
        action: Operation,
        resource: String,
    },

    #[error("{message}")]
    BadRequest { message: String },

    #[error("{message}")]
    UnprocessableEntity { message: String },

    #[error("{resource} with ID {id} not found")]
    NotFound { resource: String, id: String },

    #[error("{message}")]
    Gone { message: String },

    #[error("Failed to {operation}")]
    Internal { operation: String },

    #[error(transparent)]
    Database(#[from] DbError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),

    #[error("Conflict: {message}")]
    Conflict {
        message: String,
        conflicts: Option<Vec<AliasConflict>>,
    },

    #[error("Payload too large: {message}")]
    PayloadTooLarge { message: String },

    #[error("Unsupported media type: {message}")]
    UnsupportedMediaType { message: String },

    #[error("Insufficient credits: {message}")]
    InsufficientCredits { current_balance: Decimal, message: String },

    #[error("Spending cap exceeded: {message}")]
    SpendCapExceeded { message: String },

    #[error("Model access denied: {message}")]
    ModelAccessDenied { model_name: String, message: String },

    #[error("Modality access denied: {message}")]
    ModalityAccessDenied {
        model_name: String,
        purpose: String,
        message: String,
    },

    #[error("{modality} is disabled for this organization")]
    ModalityDisabled { modality: crate::modalities::Modality },

    #[error("Too many requests: {message}")]
    TooManyRequests { message: String },

    #[error("Service temporarily unavailable: {message}")]
    ServiceUnavailable { message: String },

    #[error("Not implemented on this server: {message}")]
    NotImplemented { message: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AliasConflict {
    pub model_name: String,
    pub attempted_alias: String,
}

impl Error {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Error::Unauthenticated { .. } => StatusCode::UNAUTHORIZED,
            Error::InsufficientPermissions { .. } => StatusCode::FORBIDDEN,
            Error::BadRequest { .. } => StatusCode::BAD_REQUEST,
            Error::UnprocessableEntity { .. } => StatusCode::UNPROCESSABLE_ENTITY,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::Gone { .. } => StatusCode::GONE,
            Error::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Database(db_err) => match db_err {
                DbError::NotFound => StatusCode::NOT_FOUND,
                DbError::UniqueViolation { .. } => StatusCode::CONFLICT,
                DbError::ForeignKeyViolation { .. } => StatusCode::BAD_REQUEST,
                DbError::CheckViolation { .. } => StatusCode::BAD_REQUEST,
                DbError::ProtectedEntity { .. } => StatusCode::FORBIDDEN,
                DbError::InvalidModelField { .. } => StatusCode::BAD_REQUEST,
                DbError::PoolExhausted => StatusCode::SERVICE_UNAVAILABLE,
                DbError::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Error::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Conflict { .. } => StatusCode::CONFLICT,
            Error::PayloadTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Error::UnsupportedMediaType { .. } => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Error::InsufficientCredits { .. } => StatusCode::PAYMENT_REQUIRED,
            Error::SpendCapExceeded { .. } => StatusCode::PAYMENT_REQUIRED,
            Error::ModelAccessDenied { .. } => StatusCode::FORBIDDEN,
            Error::ModalityAccessDenied { .. } => StatusCode::FORBIDDEN,
            Error::ModalityDisabled { .. } => StatusCode::FORBIDDEN,
            Error::TooManyRequests { .. } => StatusCode::TOO_MANY_REQUESTS,
            Error::ServiceUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Error::NotImplemented { .. } => StatusCode::NOT_IMPLEMENTED,
        }
    }

    pub fn user_message(&self) -> String {
        match self {
            Error::Unauthenticated { message } => message.clone().unwrap_or_else(|| "Authentication required".to_string()),
            Error::InsufficientPermissions { action, resource, .. } => {
                format!("Insufficient permissions to {action} {resource}")
            }
            Error::BadRequest { message } => message.clone(),
            Error::UnprocessableEntity { message } => message.clone(),
            Error::PayloadTooLarge { message } => message.clone(),
            Error::UnsupportedMediaType { message } => message.clone(),
            Error::NotFound { resource, id } => {
                format!("{resource} with ID {id} not found")
            }
            Error::Gone { message } => message.clone(),
            Error::Internal { .. } => "Internal server error".to_string(),
            Error::Database(db_err) => match db_err {
                DbError::NotFound => "Resource not found".to_string(),
                DbError::UniqueViolation { constraint, table, .. } => {
                    match (table.as_deref(), constraint.as_deref()) {
                        (Some("users"), Some(c)) if c.contains("email") => "An account with this email address already exists".to_string(),
                        (Some("users"), Some(c)) if c.contains("username") => "This username is already taken".to_string(),
                        (Some("deployed_models"), Some("deployed_models_alias_unique")) => {
                            "The specified alias is already in use. Please choose a different alias.".to_string()
                        }
                        (Some("user_organizations"), Some(c)) if c.contains("invite_email") => {
                            "A pending invite already exists for this email address".to_string()
                        }
                        _ => "Resource already exists".to_string(),
                    }
                }
                DbError::ForeignKeyViolation { .. } => "Invalid reference to related resource".to_string(),
                DbError::CheckViolation { .. } => "Invalid data provided".to_string(),
                DbError::ProtectedEntity {
                    operation,
                    entity_type,
                    reason,
                    ..
                } => {
                    format!("Cannot {operation:?} {entity_type}: {reason}")
                }
                DbError::InvalidModelField { field } => format!("Field '{field}' must not be empty or whitespace"),
                DbError::PoolExhausted => "Service temporarily overloaded, please retry".to_string(),
                DbError::Other(_) => "Database error occurred".to_string(),
            },
            Error::Other(_) => "Internal server error".to_string(),
            Error::Conflict { message, conflicts } => {
                if let Some(conflicts) = conflicts {
                    let aliases: Vec<String> = conflicts.iter().map(|c| c.attempted_alias.to_string()).collect();
                    format!("{message}: {}", aliases.join(", "))
                } else {
                    message.clone()
                }
            }
            Error::InsufficientCredits { message, .. } => message.clone(),
            Error::SpendCapExceeded { message } => message.clone(),
            Error::ModelAccessDenied { message, .. } => message.clone(),
            Error::ModalityAccessDenied { message, .. } => message.clone(),
            Error::ModalityDisabled { modality } => modality.disabled_message(),
            Error::TooManyRequests { message } => message.clone(),
            Error::ServiceUnavailable { message } => message.clone(),
            Error::NotImplemented { message } => message.clone(),
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match &self {
            Error::Database(DbError::Other(_)) | Error::Internal { .. } | Error::Other(_) => {
                tracing::error!("Internal service error: {:#}", self);
            }
            Error::Database(DbError::PoolExhausted) => {
                tracing::warn!("Database connection pool exhausted - service overloaded");
            }
            Error::Database(_) => {
                tracing::warn!("Database constraint error: {}", self);
            }
            Error::Unauthenticated { .. } | Error::InsufficientPermissions { .. } => {
                tracing::info!("Authorization error: {}", self);
            }
            Error::BadRequest { .. }
            | Error::UnprocessableEntity { .. }
            | Error::NotFound { .. }
            | Error::Gone { .. }
            | Error::PayloadTooLarge { .. }
            | Error::UnsupportedMediaType { .. } => {
                tracing::debug!("Client error: {}", self);
            }
            Error::Conflict { .. } => {
                tracing::warn!("Conflict error: {}", self);
            }
            Error::InsufficientCredits { .. } => {
                tracing::info!("Insufficient credits error: {}", self);
            }
            Error::SpendCapExceeded { .. } => {
                tracing::info!("Spending cap exceeded: {}", self);
            }
            Error::ModelAccessDenied { .. } => {
                tracing::info!("Model access denied error: {}", self);
            }
            Error::ModalityAccessDenied { .. } => {
                tracing::info!("Modality access denied error: {}", self);
            }
            Error::ModalityDisabled { .. } => {
                tracing::info!("Modality disabled for organization: {}", self);
            }
            Error::TooManyRequests { .. } => {
                tracing::info!("Rate limit exceeded: {}", self);
            }
            Error::ServiceUnavailable { .. } => {
                tracing::warn!("Service temporarily unavailable: {}", self);
            }
            Error::NotImplemented { .. } => {
                tracing::warn!("Not implemented on this server: {}", self);
            }
        }

        let status = self.status_code();

        match &self {
            Error::Conflict { message, conflicts } => {
                use serde_json::json;
                let body = if let Some(conflicts) = conflicts {
                    json!({
                        "message": message,
                        "conflicts": conflicts
                    })
                } else {
                    json!({ "message": message })
                };

                (status, axum::response::Json(body)).into_response()
            }
            Error::Database(DbError::PoolExhausted) => {
                use axum::http::header::RETRY_AFTER;
                use serde_json::json;
                let body = json!({
                    "error": "service_unavailable",
                    "message": self.user_message(),
                    "retry_after_seconds": 30
                });
                (status, [(RETRY_AFTER, POOL_EXHAUSTED_RETRY_AFTER_SECS)], axum::response::Json(body)).into_response()
            }
            Error::Database(DbError::UniqueViolation { constraint, table, .. }) => {
                use serde_json::json;

                let (message, resource) = match (table.as_deref(), constraint.as_deref()) {
                    (Some("users"), Some(c)) if c.contains("email") => {
                        ("An account with this email address already exists".to_string(), "user")
                    }
                    (Some("users"), Some(c)) if c.contains("username") => ("This username is already taken".to_string(), "user"),
                    (Some("deployed_models"), Some("deployed_models_alias_unique")) => (
                        "The specified alias is already in use. Please choose a different alias.".to_string(),
                        "deployment",
                    ),
                    (Some("inference_endpoints"), Some(c)) if c.contains("name") => {
                        ("An endpoint with this name already exists".to_string(), "endpoint")
                    }
                    (Some("inference_endpoints"), Some(c)) if c.contains("url") => {
                        ("An endpoint with this URL already exists".to_string(), "endpoint")
                    }
                    _ => ("Resource already exists".to_string(), "unknown"),
                };

                let body = json!({
                    "message": message,
                    "resource": resource
                });

                (status, axum::response::Json(body)).into_response()
            }
            Error::TooManyRequests { message } => {
                use axum::http::header::RETRY_AFTER;
                use serde_json::json;

                let retry_after_secs = "60";

                let body = json!({
                    "error": "too_many_requests",
                    "message": message,
                    "retry_after_seconds": 30
                });

                (status, [(RETRY_AFTER, retry_after_secs)], axum::response::Json(body)).into_response()
            }
            Error::ServiceUnavailable { message } => {
                use axum::http::header::RETRY_AFTER;
                use serde_json::json;
                let body = json!({
                    "error": "service_unavailable",
                    "message": message,
                    "retry_after_seconds": 30
                });
                (status, [(RETRY_AFTER, "30")], axum::response::Json(body)).into_response()
            }
            Error::ModalityDisabled { .. } => {
                use serde_json::json;
                let body = json!({
                    "error": {
                        "message": self.user_message(),
                        "type": "invalid_request_error",
                        "code": "modality_disabled",
                        "param": null
                    }
                });
                (status, axum::response::Json(body)).into_response()
            }
            _ => {
                let user_message = self.user_message();
                (status, user_message).into_response()
            }
        }
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Error::Internal { operation: msg }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
