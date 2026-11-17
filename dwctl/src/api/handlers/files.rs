
use sqlx_pool_router::PoolProvider;

use crate::api::models::files::{
    FileContentQuery, FileCostEstimate, FileCostEstimateQuery, FileDeleteResponse, FileListResponse, FileResponse, ListFilesQuery,
    ListObject, ObjectType, Purpose,
};
use crate::api::models::users::CurrentUser;
use crate::auth::permissions::{RequiresPermission, can_read_all_resources, operation, resource};
use crate::inference::serving_name::has_class_suffix;

use crate::AppState;
use crate::db::{
    handlers::api_keys::ApiKeys,
    handlers::connections::Connections,
    handlers::deployments::{DeploymentFilter, Deployments},
    handlers::repository::Repository,
    handlers::tariffs::Tariffs,
    handlers::users::Users,
    models::api_keys::ApiKeyPurpose,
    models::deployments::{ModelStatus, ModelType},
    models::users::UserDBResponse,
};
use crate::errors::{Error, Result};
use crate::image_normalizer::{ImageInput, ImageNormalizer, Mode as ImageNormalizerMode, walker as image_walker};
use crate::reasoning::ModelReasoningPolicy;
use crate::sync::key_policy::AccountTolerations;
use crate::types::Resource;
use crate::upload_compression::{self, ChunkError, Detected};
use axum::{
    Json,
    body::Body,
    extract::{FromRequest, Multipart, Path, Query, State},
    http::StatusCode,
};
use bytes::Bytes;
use chrono::Utc;
use fusillade::Storage;
use futures::StreamExt;
use futures::stream::Stream;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;
use crate::limits::{MULTIPART_OVERHEAD, UploadLimiter, UploadPermit};
use axum::extract::rejection::LengthLimitError;

fn is_file_owner(current_user: &CurrentUser, uploaded_by: Option<&str>) -> bool {
    let user_id = current_user.id.to_string();
    if uploaded_by == Some(user_id.as_str()) {
        return true;
    }
    if let Some(org_id) = current_user.active_organization {
        let org_id_str = org_id.to_string();
        if uploaded_by == Some(org_id_str.as_str()) {
            return true;
        }
    }
    false
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpenAIBatchRequest {
    custom_id: String,
    method: String,
    url: String,
    body: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AllowedHttpMethod {
    Post,
}

impl std::str::FromStr for AllowedHttpMethod {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_uppercase().as_str() {
            "POST" => Ok(Self::Post),
            _ => Err(Error::BadRequest {
                message: format!("Unsupported HTTP method '{}'. Only POST is currently supported.", s),
            }),
        }
    }
}

fn validate_url_path(url: &str, allowed_url_paths: &[String]) -> Result<()> {
    if !allowed_url_paths.iter().any(|path| path == url) {
        return Err(Error::BadRequest {
            message: format!(
                "Unsupported URL path '{}'. Allowed paths are: {}",
                url,
                allowed_url_paths.join(", ")
            ),
        });
    }
    Ok(())
}

const MAX_CUSTOM_ID_LENGTH: usize = 64;

fn validate_custom_id(custom_id: &str) -> Result<()> {
    if custom_id.len() > MAX_CUSTOM_ID_LENGTH {
        return Err(Error::BadRequest {
            message: format!(
                "custom_id exceeds maximum length of {} characters (got {})",
                MAX_CUSTOM_ID_LENGTH,
                custom_id.len()
            ),
        });
    }

    if axum::http::HeaderValue::from_str(custom_id).is_err() {
        return Err(Error::BadRequest {
            message: "custom_id contains invalid characters: must be valid ASCII without control characters".to_string(),
        });
    }

    Ok(())
}

fn validate_endpoint_model_type(url: &str, model: &str, model_type: &ModelType) -> Result<()> {
    let expected = match url {
        "/v1/chat/completions" | "/v1/completions" | "/v1/responses" | "/v1/messages" => ModelType::Chat,
        "/v1/embeddings" => ModelType::Embeddings,
        _ => return Ok(()),
    };

    if *model_type != expected {
        return Err(Error::BadRequest {
            message: format!(
                "Model '{}' is a {} model but endpoint '{}' requires a {} model",
                model,
                format!("{:?}", model_type).to_uppercase(),
                url,
                format!("{:?}", expected).to_uppercase(),
            ),
        });
    }

    Ok(())
}

impl OpenAIBatchRequest {
    fn to_internal(&self, ctx: &FileRequestContext) -> Result<fusillade::RequestTemplateInput> {
        validate_custom_id(&self.custom_id)?;

        let _validated_method = self.method.parse::<AllowedHttpMethod>()?;

        validate_url_path(&self.url, &ctx.allowed_url_paths)?;

        let model = self
            .body
            .get("model")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::BadRequest {
                message: "Missing 'model' field in request body".to_string(),
            })?
            .to_string();

        if has_class_suffix(&model) {
            let message = match ctx.class_routes.get(&model) {
                Some(standard) => format!("Model '{model}' is available for realtime requests only; use '{standard}' in batch files."),
                None => format!("Model '{model}' has not been configured or is not available to user."),
            };
            return Err(Error::BadRequest { message });
        }

        let accessible_model = ctx
            .accessible_models
            .get(&model)
            .ok_or_else(|| match ctx.retired_models.get(&model) {
                Some(message) => Error::Gone { message: message.clone() },
                None => Error::ModelAccessDenied {
                    model_name: model.clone(),
                    message: format!("Model '{}' has not been configured or is not available to user.", model),
                },
            })?;

        if let Some(model_type) = &accessible_model.model_type {
            validate_endpoint_model_type(&self.url, &model, model_type)?;
        }

        accessible_model
            .reasoning_policy
            .validate_request(&self.url, &self.body)
            .map_err(|error| match error.status_code() {
                422 => Error::UnprocessableEntity {
                    message: error.message().to_string(),
                },
                _ => Error::BadRequest {
                    message: error.message().to_string(),
                },
            })?;

        let mut sanitized_body = self.body.clone();
        sanitized_body["model"] = model.clone().into();
        crate::inference::middleware::strip_scheduling_priority(&mut sanitized_body);

        if let Some(tolerations) = ctx.tolerations.for_model(&model) {
            crate::inference::middleware::set_routing_tolerations(&mut sanitized_body, &tolerations);
        }

        let body = serde_json::to_string(&sanitized_body).map_err(|e| Error::BadRequest {
            message: format!("Invalid JSON body: {}", e),
        })?;

        Ok(fusillade::RequestTemplateInput {
            custom_id: Some(self.custom_id.clone()),
            endpoint: ctx.endpoint.clone(),
            method: self.method.clone(),
            path: self.url.clone(),
            body,
            model,
        })
    }

    fn from_internal(internal: &fusillade::RequestTemplateInput) -> Result<Self> {
        let body: serde_json::Value = serde_json::from_str(&internal.body).map_err(|e| Error::Internal {
            operation: format!("Failed to parse stored body as JSON: {}", e),
        })?;

        Ok(OpenAIBatchRequest {
            custom_id: internal
                .custom_id
                .clone()
                .unwrap_or_else(|| format!("req-{}", uuid::Uuid::new_v4())),
            method: internal.method.clone(),
            url: internal.path.clone(),
            body,
        })
    }
}

struct FileStreamConfig {
    max_file_size: u64,
    max_requests_per_file: usize,
    max_request_body_size: u64,
    buffer_size: usize,
    expiry_seconds: std::ops::RangeInclusive<i64>,
    normalizer: Option<Arc<dyn ImageNormalizer>>,
    normalizer_mode: ImageNormalizerMode,
    access_pool: Option<sqlx::PgPool>,
    access_attribution: Option<crate::api::handlers::images::ImageAttribution>,
}

impl std::fmt::Debug for FileStreamConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileStreamConfig")
            .field("max_file_size", &self.max_file_size)
            .field("max_requests_per_file", &self.max_requests_per_file)
            .field("max_request_body_size", &self.max_request_body_size)
            .field("buffer_size", &self.buffer_size)
            .field("expiry_seconds", &self.expiry_seconds)
            .field("normalizer_enabled", &self.normalizer.is_some())
            .field("normalizer_mode", &self.normalizer_mode)
            .field("image_access_enabled", &self.access_pool.is_some())
            .field("access_attribution", &self.access_attribution)
            .finish()
    }
}

#[derive(Debug)]
enum BatchNormalizeError {
    BadInput(String),
    Unfetchable(String),
    FetchFailed(String),
    Transient(String),
    StoreFailed(String),
    AccessUnavailable,
}

async fn normalize_template_body_in_place(
    normalizer: Option<&Arc<dyn ImageNormalizer>>,
    mode: ImageNormalizerMode,
    template: &mut fusillade::RequestTemplateInput,
    access_pool: Option<&sqlx::PgPool>,
    access_attribution: Option<crate::api::handlers::images::ImageAttribution>,
) -> std::result::Result<(), BatchNormalizeError> {
    let Some(normalizer) = normalizer else {
        return Ok(());
    };
    let mut body_value: serde_json::Value = match serde_json::from_str(&template.body) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };
    let err_cell: std::sync::Mutex<Option<BatchNormalizeError>> = std::sync::Mutex::new(None);
    let walker_result = image_walker::substitute_with(&mut body_value, mode, |url| {
        let normalizer = Arc::clone(normalizer);
        let access_pool = access_pool.cloned();
        let err_cell = &err_cell;
        let is_data_uri = url.starts_with("data:");
        async move {
            let input = if is_data_uri {
                ImageInput::DataUri(url)
            } else {
                ImageInput::HttpUrl(url)
            };
            match normalizer.ingest(input).await {
                Ok(ingested) => {
                    if let (Some(pool), Some(attribution)) = (access_pool, access_attribution) {
                        if let Err(e) = crate::api::handlers::images::try_record_image_access(
                            &pool,
                            attribution,
                            ingested.token,
                            &ingested.mime,
                            ingested.bytes_len,
                        )
                        .await
                        {
                            tracing::warn!(error = %e, "image_access bookkeeping failed on file upload");
                            if let Ok(mut g) = err_cell.lock()
                                && g.is_none()
                            {
                                *g = Some(BatchNormalizeError::AccessUnavailable);
                            }
                            return Err(());
                        }
                    }
                    Ok::<String, ()>(ingested.token.to_dw_img_uri())
                }
                Err(e) => {
                    let mapped = match e {
                        crate::image_normalizer::NormalizeError::BadInput(m) => BatchNormalizeError::BadInput(m),
                        crate::image_normalizer::NormalizeError::Unfetchable(m) => BatchNormalizeError::Unfetchable(m),
                        crate::image_normalizer::NormalizeError::FetchFailed(m) => BatchNormalizeError::FetchFailed(m),
                        crate::image_normalizer::NormalizeError::Transient(m) => BatchNormalizeError::Transient(m),
                        crate::image_normalizer::NormalizeError::StoreFailed(m) => BatchNormalizeError::StoreFailed(m),
                        crate::image_normalizer::NormalizeError::NotFound => {
                            BatchNormalizeError::StoreFailed("image token not found in store".to_string())
                        }
                        crate::image_normalizer::NormalizeError::Forbidden => {
                            BatchNormalizeError::BadInput("image token is not accessible to this caller".to_string())
                        }
                        crate::image_normalizer::NormalizeError::AccessUnavailable => BatchNormalizeError::AccessUnavailable,
                    };
                    if let Ok(mut g) = err_cell.lock()
                        && g.is_none()
                    {
                        *g = Some(mapped);
                    }
                    Err(())
                }
            }
        }
    })
    .await;
    if walker_result.is_err() {
        let e = err_cell
            .into_inner()
            .ok()
            .flatten()
            .unwrap_or_else(|| BatchNormalizeError::StoreFailed("unknown normalisation error".into()));
        return Err(e);
    }
    let count = walker_result.expect("walker_result is Ok by control flow above");
    if count > 0 {
        template.body = serde_json::to_string(&body_value)
            .map_err(|e| BatchNormalizeError::StoreFailed(format!("re-serialise after image normalisation: {e}")))?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
enum FileUploadError {
    StreamInterrupted { message: String },
    FileTooLarge { max: u64 },
    TooManyRequests { count: usize, max: usize },
    InvalidJson { line: u64, error: String },
    InvalidUtf8 { line: u64, byte_offset: i64, error: String },
    InvalidCompression { format: &'static str, message: String },
    UnsupportedCompression { format: &'static str },
    NoFile,
    EmptyFile,
    ModelAccessDenied { model: String, line: u64 },
    ModelRetired { line: u64, message: String },
    ValidationError { line: u64, message: String },
    UnprocessableValidationError { line: u64, message: String },
    ImageTransient { line: u64, message: String },
    ImageStoreFailed { line: u64, message: String },
    ImageAccessUnavailable { line: u64 },
    ImageUnfetchable { line: u64, message: String },
    ImageFetchFailed { line: u64, message: String },
    InvalidPurpose { purpose: String },
    InvalidExpiry { value: String, min: i64, max: i64 },
}

fn map_batch_normalize_error(e: BatchNormalizeError, line: u64) -> FileUploadError {
    match e {
        BatchNormalizeError::BadInput(message) => FileUploadError::ValidationError { line, message },
        BatchNormalizeError::Unfetchable(message) => FileUploadError::ImageUnfetchable { line, message },
        BatchNormalizeError::FetchFailed(message) => FileUploadError::ImageFetchFailed { line, message },
        BatchNormalizeError::Transient(message) => FileUploadError::ImageTransient { line, message },
        BatchNormalizeError::StoreFailed(message) => FileUploadError::ImageStoreFailed { line, message },
        BatchNormalizeError::AccessUnavailable => FileUploadError::ImageAccessUnavailable { line },
    }
}

const UPLOAD_REJECTIONS: &str = "dwctl_file_upload_rejections_total";

const UPLOAD_REJECTION_CODES: &[&str] = &[
    "not_multipart",
    "upload_capacity",
    "batch_disabled",
    "invalid_multipart",
    "stream_interrupted",
    "file_too_large",
    "too_many_lines",
    "invalid_json",
    "invalid_utf8",
    "invalid_compression",
    "unsupported_compression",
    "no_file",
    "empty_file",
    "model_access_denied",
    "model_retired",
    "validation_error",
    "unprocessable_line",
    "image_transient",
    "image_store_failed",
    "image_access_unavailable",
    "image_unfetchable",
    "image_fetch_failed",
    "invalid_purpose",
    "invalid_expiry",
];

pub(crate) fn initialize_upload_rejection_metrics() {
    metrics::describe_counter!(UPLOAD_REJECTIONS, "File uploads refused, by the reason they were refused");
    for &code in UPLOAD_REJECTION_CODES {
        metrics::counter!(UPLOAD_REJECTIONS, "code" => code).increment(0);
    }
}

fn refuse_upload(code: &'static str, error: Error) -> Error {
    metrics::counter!(UPLOAD_REJECTIONS, "code" => code).increment(1);
    error
}

async fn acquire_upload_slot(limiter: Option<&UploadLimiter>) -> Result<Option<UploadPermit>> {
    match limiter {
        Some(limiter) => limiter
            .acquire()
            .await
            .map(Some)
            .map_err(|error| refuse_upload("upload_capacity", error)),
        None => Ok(None),
    }
}

impl FileUploadError {
    fn code(&self) -> &'static str {
        match self {
            FileUploadError::StreamInterrupted { .. } => "stream_interrupted",
            FileUploadError::FileTooLarge { .. } => "file_too_large",
            FileUploadError::TooManyRequests { .. } => "too_many_lines",
            FileUploadError::InvalidJson { .. } => "invalid_json",
            FileUploadError::InvalidUtf8 { .. } => "invalid_utf8",
            FileUploadError::InvalidCompression { .. } => "invalid_compression",
            FileUploadError::UnsupportedCompression { .. } => "unsupported_compression",
            FileUploadError::NoFile => "no_file",
            FileUploadError::EmptyFile => "empty_file",
            FileUploadError::ModelAccessDenied { .. } => "model_access_denied",
            FileUploadError::ModelRetired { .. } => "model_retired",
            FileUploadError::ValidationError { .. } => "validation_error",
            FileUploadError::UnprocessableValidationError { .. } => "unprocessable_line",
            FileUploadError::ImageTransient { .. } => "image_transient",
            FileUploadError::ImageStoreFailed { .. } => "image_store_failed",
            FileUploadError::ImageAccessUnavailable { .. } => "image_access_unavailable",
            FileUploadError::ImageUnfetchable { .. } => "image_unfetchable",
            FileUploadError::ImageFetchFailed { .. } => "image_fetch_failed",
            FileUploadError::InvalidPurpose { .. } => "invalid_purpose",
            FileUploadError::InvalidExpiry { .. } => "invalid_expiry",
        }
    }

    fn into_refusal(self) -> Error {
        let code = self.code();
        refuse_upload(code, self.into_http_error())
    }

    fn into_http_error(self) -> Error {
        match self {
            FileUploadError::StreamInterrupted { message } => {
                Error::BadRequest {
                    message: format!("Failed to read multipart upload: {message}"),
                }
            }
            FileUploadError::FileTooLarge { max } => {
                if max == 0 {
                    Error::Internal {
                        operation: "upload file: unexpected size limit error with unlimited file size configured".to_string(),
                    }
                } else {
                    Error::PayloadTooLarge {
                        message: format!("File exceeds the maximum allowed size of {} bytes", max),
                    }
                }
            }
            FileUploadError::TooManyRequests { count, max } => Error::BadRequest {
                message: format!("File contains {} requests, which exceeds the maximum of {}", count, max),
            },
            FileUploadError::InvalidJson { line, error } => Error::BadRequest {
                message: format!("Invalid JSON on line {}: {}", line, error),
            },
            FileUploadError::InvalidUtf8 { line, byte_offset, error } => Error::BadRequest {
                message: format!(
                    "File contains invalid UTF-8 on/near line {} at byte offset {}: {}",
                    line, byte_offset, error
                ),
            },
            FileUploadError::InvalidCompression { format, message } => Error::BadRequest {
                message: format!("File is not valid {format} data: {message}"),
            },
            FileUploadError::UnsupportedCompression { format } => Error::UnsupportedMediaType {
                message: format!(
                    "File is compressed with {format}, which is not supported. Upload it uncompressed, or compressed with gzip or zstd."
                ),
            },
            FileUploadError::NoFile => Error::BadRequest {
                message: "No file field found in multipart upload".to_string(),
            },
            FileUploadError::EmptyFile => Error::BadRequest {
                message: "File contains no valid request templates".to_string(),
            },
            FileUploadError::ModelAccessDenied { model, line } => Error::ModelAccessDenied {
                model_name: model.clone(),
                message: format!(
                    "Line {}: Model '{}' has not been configured or is not available to user",
                    line, model
                ),
            },
            FileUploadError::ModelRetired { line, message } => Error::Gone {
                message: format!("Line {}: {}", line, message),
            },
            FileUploadError::ValidationError { line, message } => Error::BadRequest {
                message: format!("Line {}: {}", line, message),
            },
            FileUploadError::UnprocessableValidationError { line, message } => Error::UnprocessableEntity {
                message: format!("Line {}: {}", line, message),
            },
            FileUploadError::ImageTransient { line, message } => Error::ServiceUnavailable {
                message: format!("Line {}: image fetch failed after retries, please retry: {}", line, message),
            },
            FileUploadError::ImageStoreFailed { line, message } => Error::ServiceUnavailable {
                message: format!("Line {}: image content store temporarily unavailable: {}", line, message),
            },
            FileUploadError::ImageAccessUnavailable { line } => Error::ServiceUnavailable {
                message: format!("Line {}: image access store temporarily unavailable, please retry", line),
            },
            FileUploadError::ImageUnfetchable { line, message } => Error::BadRequest {
                message: format!(
                    "Line {}: referenced image could not be retrieved ({}); ensure the URL is publicly accessible and does not require authentication",
                    line, message
                ),
            },
            FileUploadError::ImageFetchFailed { line, message } => Error::ServiceUnavailable {
                message: format!("Line {}: referenced image could not be fetched (upstream error): {}", line, message),
            },
            FileUploadError::InvalidPurpose { purpose } => Error::BadRequest {
                message: format!("Invalid purpose '{}'. Only 'batch' is supported.", purpose),
            },
            FileUploadError::InvalidExpiry { value, min, max } => Error::BadRequest {
                message: format!(
                    "Invalid expires_after[seconds] '{}'. Must be an integer between {} and {}.",
                    value, min, max
                ),
            },
        }
    }
}

fn is_multipart_form(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("multipart/form-data"))
}

fn is_length_limit_error(err: &(dyn std::error::Error + 'static)) -> bool {
    if err.downcast_ref::<LengthLimitError>().is_some() {
        return true;
    }

    if let Some(multer_err) = err.downcast_ref::<multer::Error>()
        && is_multer_length_limit(multer_err)
    {
        return true;
    }

    let err_string = err.to_string().to_lowercase();
    if err_string.contains("length limit exceeded") {
        return true;
    }

    if let Some(source) = std::error::Error::source(err) {
        return is_length_limit_error(source);
    }

    false
}

fn is_multer_length_limit(err: &multer::Error) -> bool {
    match err {
        multer::Error::StreamSizeExceeded { .. } | multer::Error::FieldSizeExceeded { .. } => true,
        multer::Error::StreamReadFailed(boxed) => {
            is_length_limit_error(boxed.as_ref())
        }
        _ => false,
    }
}

type FileStreamResult = (
    Pin<Box<dyn Stream<Item = fusillade::FileStreamItem> + Send>>,
    Arc<Mutex<Option<FileUploadError>>>,
);

fn resolve_upload_stream_result(
    result: fusillade::FileStreamResult,
    error_slot: &Arc<Mutex<Option<FileUploadError>>>,
) -> Result<fusillade::FileId> {
    match result {
        fusillade::FileStreamResult::Success(file_id) => Ok(file_id),
        fusillade::FileStreamResult::Aborted => {
            let upload_err = match error_slot.lock() {
                Ok(mut guard) => guard.take(),
                Err(poisoned) => poisoned.into_inner().take(),
            };
            if let Some(upload_err) = upload_err {
                tracing::warn!("File upload aborted with error: {:?}", upload_err);
                return Err(upload_err.into_refusal());
            }

            Err(Error::Internal {
                operation: "create file: fusillade returned Aborted without an upload error".to_string(),
            })
        }
    }
}

struct FileRequestContext {
    endpoint: String,
    accessible_models: HashMap<String, AccessibleBatchModel>,
    retired_models: HashMap<String, String>,
    allowed_url_paths: Vec<String>,
    class_routes: HashMap<String, String>,
    tolerations: AccountTolerations,
}

struct AccessibleBatchModel {
    model_type: Option<ModelType>,
    reasoning_policy: ModelReasoningPolicy,
}

fn map_request_validation_error(error: &Error, line: u64) -> FileUploadError {
    match error {
        Error::ModelAccessDenied { model_name, .. } => FileUploadError::ModelAccessDenied {
            model: model_name.clone(),
            line,
        },
        Error::Gone { message } => FileUploadError::ModelRetired {
            line,
            message: message.clone(),
        },
        Error::UnprocessableEntity { message } => FileUploadError::UnprocessableValidationError {
            line,
            message: message.clone(),
        },
        _ => FileUploadError::ValidationError {
            line,
            message: error.to_string(),
        },
    }
}

#[tracing::instrument(skip(multipart, req_ctx), fields(config.max_file_size, config.max_requests_per_file, uploaded_by = ?uploaded_by, endpoint = %req_ctx.endpoint, config.buffer_size))]
fn create_file_stream(
    mut multipart: Multipart,
    config: FileStreamConfig,
    uploaded_by: Option<String>,
    req_ctx: FileRequestContext,
    api_key_id: Option<uuid::Uuid>,
) -> FileStreamResult {
    let normalizer = config.normalizer.clone();
    let normalizer_mode = config.normalizer_mode;
    let expiry_seconds = config.expiry_seconds.clone();
    let access_pool = config.access_pool.clone();
    let access_attribution = config.access_attribution;
    let (tx, rx) = mpsc::channel(config.buffer_size);
    let error_slot: Arc<Mutex<Option<FileUploadError>>> = Arc::new(Mutex::new(None));
    let error_slot_clone = Arc::clone(&error_slot);

    tokio::spawn(async move {
        let mut total_size = 0u64;
        let mut line_count = 0u64;
        let mut incomplete_line = String::with_capacity(1024);
        let mut incomplete_utf8_bytes = Vec::with_capacity(4);
        let mut metadata = fusillade::FileMetadata {
            uploaded_by,
            api_key_id,
            ..Default::default()
        };
        let mut file_processed = false;

        macro_rules! abort {
            ($error:expr) => {{
                match error_slot_clone.lock() {
                    Ok(mut guard) => *guard = Some($error),
                    Err(poisoned) => *poisoned.into_inner() = Some($error),
                }
                let _ = tx.send(fusillade::FileStreamItem::Abort).await;
                return;
            }};
        }

        loop {
            let field = match multipart.next_field().await {
                Ok(Some(field)) => field,
                Ok(None) => break,
                Err(e) => {
                    if is_length_limit_error(&e) {
                        abort!(FileUploadError::FileTooLarge { max: config.max_file_size });
                    } else {
                        abort!(FileUploadError::StreamInterrupted {
                            message: format!("Multipart parsing failed: {}", e),
                        });
                    }
                }
            };

            let field_name = field.name().unwrap_or("");

            match field_name {
                "purpose" => {
                    if let Ok(value) = field.text().await {
                        match value.parse::<fusillade::batch::Purpose>() {
                            Ok(fusillade::batch::Purpose::Batch) => metadata.purpose = Some(value),
                            _ => abort!(FileUploadError::InvalidPurpose { purpose: value }),
                        }
                    }
                }
                "expires_after[anchor]" => {
                    if let Ok(value) = field.text().await {
                        metadata.expires_after_anchor = Some(value);
                    }
                }
                "expires_after[seconds]" => {
                    if let Ok(value) = field.text().await {
                        match value.trim().parse::<i64>() {
                            Ok(seconds) if expiry_seconds.contains(&seconds) => {
                                metadata.expires_after_seconds = Some(seconds);
                            }
                            _ => abort!(FileUploadError::InvalidExpiry {
                                value,
                                min: *expiry_seconds.start(),
                                max: *expiry_seconds.end(),
                            }),
                        }
                    }
                }
                "file" => {
                    let filename = field.file_name().map(|s| s.to_string());

                    let (format, mut chunks) = upload_compression::open(Box::pin(field)).await;
                    if let Detected::Unsupported(format) = format {
                        abort!(FileUploadError::UnsupportedCompression { format });
                    }
                    metadata.filename = filename.map(|name| format.decoded_file_name(&name));

                    if tx.send(fusillade::FileStreamItem::Metadata(metadata.clone())).await.is_err() {
                        return;
                    }

                    loop {
                        match chunks.next().await {
                            Some(Ok(chunk)) => {
                                let chunk_size = chunk.len() as u64;
                                total_size += chunk_size;

                                tracing::debug!(
                                    "Processing chunk: {} bytes, total: {} bytes, lines so far: {}",
                                    chunk_size,
                                    total_size,
                                    line_count
                                );

                                if config.max_file_size > 0 && total_size > config.max_file_size {
                                    abort!(FileUploadError::FileTooLarge { max: config.max_file_size });
                                }

                                let combined_bytes = if incomplete_utf8_bytes.is_empty() {
                                    chunk.to_vec()
                                } else {
                                    let mut combined = incomplete_utf8_bytes.clone();
                                    combined.extend_from_slice(&chunk);
                                    combined
                                };

                                let (chunk_str, remaining_bytes) = match std::str::from_utf8(&combined_bytes) {
                                    Ok(s) => {
                                        incomplete_utf8_bytes.clear();
                                        (s.to_string(), Vec::new())
                                    }
                                    Err(e) => {
                                        let valid_up_to = e.valid_up_to();

                                        if e.error_len().is_some() {
                                            let byte_offset = (total_size - chunk_size) as i64 + valid_up_to as i64;
                                            tracing::error!(
                                                "UTF-8 parsing error on/near line {}, byte offset {}",
                                                line_count + 1,
                                                byte_offset
                                            );

                                            abort!(FileUploadError::InvalidUtf8 {
                                                line: line_count + 1,
                                                byte_offset,
                                                error: e.to_string(),
                                            });
                                        }

                                        let valid_str = std::str::from_utf8(&combined_bytes[..valid_up_to])
                                            .expect("valid_up_to should point to valid UTF-8");
                                        let remaining = combined_bytes[valid_up_to..].to_vec();

                                        tracing::debug!("Incomplete UTF-8 sequence at chunk boundary, buffering {} bytes", remaining.len());

                                        (valid_str.to_string(), remaining)
                                    }
                                };

                                incomplete_utf8_bytes = remaining_bytes;

                                let text_to_process = if incomplete_line.is_empty() {
                                    chunk_str.to_string()
                                } else {
                                    format!("{}{}", incomplete_line, chunk_str)
                                };

                                let mut lines = text_to_process.lines().peekable();
                                let ends_with_newline = chunk_str.ends_with('\n');

                                while let Some(line) = lines.next() {
                                    let is_last_line = lines.peek().is_none();

                                    if is_last_line && !ends_with_newline {
                                        incomplete_line = line.to_string();
                                        break;
                                    }

                                    let trimmed = line.trim();
                                    if trimmed.is_empty() {
                                        continue;
                                    }

                                    if config.max_requests_per_file > 0 && line_count >= config.max_requests_per_file as u64 {
                                        abort!(FileUploadError::TooManyRequests {
                                            count: (line_count + 1).try_into().unwrap_or(usize::MAX),
                                            max: config.max_requests_per_file,
                                        });
                                    }

                                    match serde_json::from_str::<OpenAIBatchRequest>(trimmed) {
                                        Ok(openai_req) => {
                                            match openai_req.to_internal(&req_ctx) {
                                                Ok(mut template) => {
                                                    if let Err(e) = normalize_template_body_in_place(
                                                        normalizer.as_ref(),
                                                        normalizer_mode,
                                                        &mut template,
                                                        access_pool.as_ref(),
                                                        access_attribution,
                                                    )
                                                    .await
                                                    {
                                                        abort!(map_batch_normalize_error(e, line_count + 1));
                                                    }

                                                    if config.max_request_body_size > 0
                                                        && template.body.len() as u64 > config.max_request_body_size
                                                    {
                                                        abort!(FileUploadError::ValidationError {
                                                            line: line_count + 1,
                                                            message: format!(
                                                                "Request body is {} bytes, which exceeds the maximum allowed size of {} bytes",
                                                                template.body.len(),
                                                                config.max_request_body_size
                                                            ),
                                                        });
                                                    }

                                                    line_count += 1;
                                                    incomplete_line.clear();
                                                    if tx.send(fusillade::FileStreamItem::Template(template)).await.is_err() {
                                                        return;
                                                    }
                                                }
                                                Err(e) => {
                                                    let upload_err = map_request_validation_error(&e, line_count + 1);
                                                    abort!(upload_err);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            abort!(FileUploadError::InvalidJson {
                                                line: line_count + 1,
                                                error: e.to_string(),
                                            });
                                        }
                                    }
                                }
                            }
                            None => {
                                break;
                            }
                            Some(Err(ChunkError::Decode(e))) => {
                                abort!(FileUploadError::InvalidCompression {
                                    format: format.name(),
                                    message: e.to_string(),
                                });
                            }
                            Some(Err(ChunkError::Upload(e))) => {
                                tracing::warn!(
                                    error_display = %e,
                                    error_debug = ?e,
                                    "File upload stream error"
                                );

                                if is_length_limit_error(&e) {
                                    abort!(FileUploadError::FileTooLarge { max: config.max_file_size });
                                } else {
                                    abort!(FileUploadError::StreamInterrupted { message: e.to_string() });
                                }
                            }
                        }
                    }

                    if !incomplete_line.is_empty() {
                        let trimmed = incomplete_line.trim();
                        if !trimmed.is_empty() {
                            if config.max_requests_per_file > 0 && line_count >= config.max_requests_per_file as u64 {
                                abort!(FileUploadError::TooManyRequests {
                                    count: (line_count + 1).try_into().unwrap_or(usize::MAX),
                                    max: config.max_requests_per_file,
                                });
                            }

                            match serde_json::from_str::<OpenAIBatchRequest>(trimmed) {
                                Ok(openai_req) => {
                                    match openai_req.to_internal(&req_ctx) {
                                        Ok(mut template) => {
                                            if let Err(e) = normalize_template_body_in_place(
                                                normalizer.as_ref(),
                                                normalizer_mode,
                                                &mut template,
                                                access_pool.as_ref(),
                                                access_attribution,
                                            )
                                            .await
                                            {
                                                abort!(map_batch_normalize_error(e, line_count + 1));
                                            }

                                            if config.max_request_body_size > 0 && template.body.len() as u64 > config.max_request_body_size
                                            {
                                                abort!(FileUploadError::ValidationError {
                                                    line: line_count + 1,
                                                    message: format!(
                                                        "Request body is {} bytes, which exceeds the maximum allowed size of {} bytes",
                                                        template.body.len(),
                                                        config.max_request_body_size
                                                    ),
                                                });
                                            }

                                            line_count += 1;
                                            if tx.send(fusillade::FileStreamItem::Template(template)).await.is_err() {
                                                return;
                                            }
                                        }
                                        Err(e) => {
                                            let upload_err = map_request_validation_error(&e, line_count + 1);
                                            abort!(upload_err);
                                        }
                                    }
                                }
                                Err(e) => {
                                    abort!(FileUploadError::InvalidJson {
                                        line: line_count + 1,
                                        error: e.to_string(),
                                    });
                                }
                            }
                        }
                    }

                    if line_count == 0 {
                        abort!(FileUploadError::EmptyFile);
                    }

                    metadata.size_bytes = match i64::try_from(total_size) {
                        Ok(size) => Some(size),
                        Err(_) => {
                            abort!(FileUploadError::FileTooLarge { max: config.max_file_size });
                        }
                    };
                    file_processed = true;

                }
                _ => {
                }
            }
        }

        if !file_processed {
            abort!(FileUploadError::NoFile);
        }

        let _ = tx.send(fusillade::FileStreamItem::Metadata(metadata.clone())).await;
    });

    (Box::pin(ReceiverStream::new(rx)), error_slot)
}

#[utoipa::path(
    post,
    path = "/files",
    tag = "files",
    summary = "Upload file",
    description = "Upload a JSONL file for batch processing.

Each line must be a valid JSON object containing `custom_id`, `method`, `url`, and `body` fields. The `model` field in the body must reference a model your API key has access to.",
    request_body(
        content_type = "multipart/form-data",
        description = "Multipart form with `file` (the JSONL file, optionally gzip- or zstd-compressed) and `purpose` (must be `batch`)."
    ),
    responses(
        (status = 201, description = "File uploaded and validated successfully.", body = FileResponse),
        (status = 400, description = "Invalid file format, malformed JSON, missing required fields, a referenced image URL that was rejected or whose origin refused access (e.g. 403/404), etc."),
        (status = 403, description = "Model referenced in the file is not configured or not accessible to your account."),
        (status = 413, description = "File exceeds the maximum allowed size."),
        (status = 415, description = "The request is not `multipart/form-data`, or the file is compressed in a format other than gzip or zstd."),
        (status = 422, description = "A line is well-formed but its reasoning settings cannot be processed (e.g. a reasoning effort that needs `max_completion_tokens` when the line does not set it)."),
        (status = 429, description = "Too many concurrent uploads. Retry after a short delay."),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists."),
        (status = 503, description = "A referenced image could not be fetched due to a transient upstream/store error, or the service is briefly overloaded. Retry after a short delay.")
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id))]
pub async fn upload_file<P: PoolProvider>(
    State(state): State<AppState<P>>,
    current_user: RequiresPermission<resource::Files, operation::CreateOwn>,
    request: axum::http::Request<axum::body::Body>,
) -> Result<(StatusCode, Json<FileResponse>)> {
    if !is_multipart_form(request.headers()) {
        let error = Error::UnsupportedMediaType {
            message: "File uploads must be sent as multipart/form-data, with the file in a `file` field".to_string(),
        };
        return Err(refuse_upload("not_multipart", error));
    }

    let _permit = acquire_upload_slot(state.limiters.file_uploads.as_deref()).await?;

    let config = state.current_config();
    let max_file_size = config.limits.files.max_file_size;

    if max_file_size > 0
        && let Some(content_length) = request.headers().get(axum::http::header::CONTENT_LENGTH)
        && let Ok(length_str) = content_length.to_str()
        && let Ok(length) = length_str.parse::<u64>()
        && length > max_file_size.saturating_add(MULTIPART_OVERHEAD)
    {
        let error = Error::PayloadTooLarge {
            message: format!("File exceeds the maximum allowed size of {} bytes", max_file_size),
        };
        return Err(refuse_upload("file_too_large", error));
    }

    let multipart = Multipart::from_request(request, &state).await.map_err(|e| {
        let error = Error::BadRequest {
            message: format!("Invalid multipart request: {}", e),
        };
        refuse_upload("invalid_multipart", error)
    })?;

    let target_user_id = current_user.active_organization.unwrap_or(current_user.id);
    let uploaded_by = Some(target_user_id.to_string());

    {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Internal {
            operation: format!("get db connection for modality check: {}", e),
        })?;
        crate::api::handlers::modalities::ensure_batch_enabled(&mut conn, target_user_id)
            .await
            .map_err(|error| match error {
                Error::ModalityDisabled { .. } => refuse_upload("batch_disabled", error),
                error => error,
            })?;
    }

    let normalizer = if config.image_normalizer.enabled {
        Some(state.image_normalizer.clone())
    } else {
        None
    };
    let normalizer_mode = ImageNormalizerMode::All;
    let stream_config = FileStreamConfig {
        max_file_size: config.limits.files.max_file_size,
        max_requests_per_file: config.limits.files.max_requests_per_file,
        max_request_body_size: config.limits.requests.max_body_size,
        buffer_size: config.batches.files.upload_buffer_size,
        expiry_seconds: config.batches.files.min_expiry_seconds..=config.batches.files.max_expiry_seconds,
        normalizer,
        normalizer_mode,
        access_pool: Some(state.db.write().into_inner()),
        access_attribution: Some(crate::api::handlers::images::ImageAttribution {
            user_id: current_user.active_organization.unwrap_or(current_user.id),
            organization_id: current_user.active_organization,
        }),
    };

    let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut api_keys_repo = ApiKeys::new(&mut conn);
    let api_key_id = api_keys_repo
        .resolve_batch_execution_key(target_user_id, current_user.id, current_user.api_key_id)
        .await
        .map_err(Error::Database)?;

    let key_hash = api_keys_repo.get_secret_hash(api_key_id).await.map_err(Error::Database)?;
    let tolerations = match key_hash.and_then(|hash| state.key_policy_cache.account_tolerations_if_known(&hash)) {
        Some(resolved) => resolved,
        None => {
            let policy = crate::sync::key_policy::load_policy(&mut conn, target_user_id)
                .await
                .map_err(|e| Error::Database(e.into()))?;
            state.key_policy_cache.account_tolerations(policy)
        }
    };

    let endpoint = format!("http://{}:{}/ai", config.host, config.port);

    let mut deployments_repo = Deployments::new(&mut conn);
    let filter = DeploymentFilter::new(0, i64::MAX)
        .with_accessible_to(target_user_id)
        .with_statuses(vec![ModelStatus::Active])
        .with_deleted(false);
    let accessible_deployments = deployments_repo.list(&filter).await.map_err(Error::Database)?;
    let aliases = accessible_deployments
        .iter()
        .map(|deployment| deployment.alias.clone())
        .collect::<Vec<_>>();
    let mut reasoning_policies = deployments_repo.get_reasoning_policies(&aliases).await.map_err(Error::Database)?;
    let accessible_models: HashMap<String, AccessibleBatchModel> = accessible_deployments
        .into_iter()
        .map(|deployment| {
            let reasoning_policy = reasoning_policies.remove(&deployment.alias).unwrap_or_default();
            (
                deployment.alias,
                AccessibleBatchModel {
                    model_type: deployment.model_type,
                    reasoning_policy,
                },
            )
        })
        .collect();
    let retired_models = crate::error_enrichment::retirement_messages(&mut conn)
        .await
        .map_err(Error::Database)?;

    drop(conn);

    let (file_stream, error_slot) = create_file_stream(
        multipart,
        stream_config,
        uploaded_by,
        FileRequestContext {
            endpoint,
            accessible_models,
            retired_models,
            allowed_url_paths: config.batches.allowed_url_paths.clone(),
            tolerations,
            class_routes: state
                .onwards_targets
                .as_ref()
                .map(|targets| {
                    targets
                        .targets
                        .iter()
                        .filter_map(|entry| {
                            let model = entry.value().default_pool().class_route_of()?.to_owned();
                            Some((entry.key().clone(), model))
                        })
                        .collect()
                })
                .unwrap_or_default(),
        },
        Some(api_key_id),
    );

    let created_file_result = state.request_manager.create_file_stream(file_stream).await.map_err(|e| {
        let upload_err = match error_slot.lock() {
            Ok(mut guard) => guard.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(upload_err) = upload_err {
            tracing::warn!("File upload aborted with error: {:?}", upload_err);
            return upload_err.into_refusal();
        }

        tracing::warn!("Fusillade error during file upload: {:?}", e);
        match e {
            fusillade::FusilladeError::ValidationError(msg) => Error::BadRequest { message: msg },
            _ => Error::Internal {
                operation: format!("create file: {}", e),
            },
        }
    })?;

    let created_file_id = resolve_upload_stream_result(created_file_result, &error_slot)?;

    tracing::debug!("File {} uploaded successfully", created_file_id);

    let file = state
        .request_manager
        .get_file_from_primary_pool(created_file_id)
        .await
        .map_err(|e| Error::Internal {
            operation: format!("retrieve created file: {}", e),
        })?;

    let api_purpose = match file.purpose {
        Some(fusillade::batch::Purpose::Batch) => Purpose::Batch,
        Some(fusillade::batch::Purpose::BatchOutput) => Purpose::BatchOutput,
        Some(fusillade::batch::Purpose::BatchError) => Purpose::BatchError,
        None => Purpose::Batch,
    };

    Ok((
        StatusCode::CREATED,
        Json(FileResponse {
            id: file.id.0.to_string(),
            object_type: ObjectType::File,
            bytes: file.size_bytes,
            created_at: file.created_at.timestamp(),
            filename: file.name,
            purpose: api_purpose,
            expires_at: file.expires_at.map(|dt| dt.timestamp()),
            created_by_email: None,
            context_name: None,
            context_type: None,
            source: file.source_connection_id.map(|_| "sync".to_string()),
            source_name: None,
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/files",
    tag = "files",
    summary = "List files",
    description = "Returns a paginated list of your uploaded files.

Use cursor-based pagination: pass `last_id` from the response as the `after` parameter to fetch the next page.",
    responses(
        (status = 200, description = "List of files. Check `has_more` to determine if additional pages exist.", body = FileListResponse),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    ),
    params(
        ListFilesQuery
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id))]
pub async fn list_files<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Query(query): Query<ListFilesQuery>,
    current_user: RequiresPermission<resource::Files, operation::ReadOwn>,
) -> Result<Json<FileListResponse>> {
    let can_read_all_files = can_read_all_resources(&current_user, Resource::Files);

    if query.order != "asc" && query.order != "desc" {
        return Err(Error::BadRequest {
            message: "Order must be 'asc' or 'desc'".to_string(),
        });
    }

    let limit = query.pagination.limit();

    let after = query
        .pagination
        .after
        .as_ref()
        .and_then(|id_str| uuid::Uuid::parse_str(id_str).ok().map(fusillade::FileId::from));

    let api_key_ids_filter = if let Some(member_id) = query.member_id {
        let mut read_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let key_ids = match current_user.active_organization {
            Some(org_id) => ApiKeys::new(&mut read_conn)
                .find_hidden_key_ids(org_id, ApiKeyPurpose::Batch, member_id)
                .await
                .map_err(Error::Database)?,
            None if can_read_all_files => ApiKeys::new(&mut read_conn)
                .find_all_hidden_key_ids_by_creator(ApiKeyPurpose::Batch, member_id)
                .await
                .map_err(Error::Database)?,
            None => {
                return Err(Error::BadRequest {
                    message: "member_id filter is only available in organization context or for platform managers".to_string(),
                });
            }
        };
        if key_ids.is_empty() {
            return Ok(Json(FileListResponse {
                object_type: ListObject::List,
                data: vec![],
                first_id: None,
                last_id: None,
                has_more: false,
            }));
        }
        Some(key_ids)
    } else if let Some(org_id) = current_user.active_organization.filter(|_| query.own) {
        let mut read_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let key_ids = ApiKeys::new(&mut read_conn)
            .find_hidden_key_ids(org_id, ApiKeyPurpose::Batch, current_user.id)
            .await
            .map_err(Error::Database)?;
        if key_ids.is_empty() {
            return Ok(Json(FileListResponse {
                object_type: ListObject::List,
                data: vec![],
                first_id: None,
                last_id: None,
                has_more: false,
            }));
        }
        Some(key_ids)
    } else {
        None
    };

    let filter = fusillade::FileFilter {
        uploaded_by: if let Some(org_id) = current_user.active_organization {
            Some(org_id.to_string())
        } else if !can_read_all_files || query.own {
            Some(current_user.id.to_string())
        } else {
            None
        },
        status: None,
        purpose: query.purpose.clone(),
        search: query.search.clone(),
        after,
        limit: Some((limit + 1) as usize),
        api_key_ids: api_key_ids_filter,
        ascending: query.order == "asc",
    };

    let mut files = state.request_manager.list_files(filter).await.map_err(|e| Error::Internal {
        operation: format!("list files: {}", e),
    })?;

    let has_more = files.len() > limit as usize;
    if has_more {
        files.truncate(limit as usize);
    }

    let first_id = files.first().map(|f| f.id.0.to_string());
    let last_id = files.last().map(|f| f.id.0.to_string());

    let mut read_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;

    let api_key_ids: Vec<uuid::Uuid> = files
        .iter()
        .filter_map(|f| f.api_key_id)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let api_key_creator_map: std::collections::HashMap<uuid::Uuid, uuid::Uuid> = if !api_key_ids.is_empty() {
        ApiKeys::new(&mut read_conn)
            .get_creators_by_key_ids(api_key_ids)
            .await
            .map_err(Error::Database)?
    } else {
        std::collections::HashMap::new()
    };

    let mut all_user_ids: std::collections::HashSet<uuid::Uuid> = std::collections::HashSet::new();
    for f in &files {
        if let Some(owner_id) = f.uploaded_by.as_ref().and_then(|id| uuid::Uuid::parse_str(id).ok()) {
            all_user_ids.insert(owner_id);
        }
        if let Some(api_key_id) = f.api_key_id
            && let Some(&creator_id) = api_key_creator_map.get(&api_key_id)
        {
            all_user_ids.insert(creator_id);
        }
    }

    let user_map: std::collections::HashMap<uuid::Uuid, UserDBResponse> = if !all_user_ids.is_empty() {
        Users::new(&mut read_conn)
            .get_bulk(all_user_ids.into_iter().collect())
            .await
            .map_err(|e| Error::Internal {
                operation: format!("fetch users: {}", e),
            })?
    } else {
        std::collections::HashMap::new()
    };

    let source_conn_ids: Vec<uuid::Uuid> = files
        .iter()
        .filter_map(|f| f.source_connection_id)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let connection_info: std::collections::HashMap<uuid::Uuid, (String, uuid::Uuid)> = if !source_conn_ids.is_empty() {
        Connections::new(&mut read_conn)
            .get_names_by_ids(&source_conn_ids)
            .await
            .map_err(Error::Database)?
    } else {
        std::collections::HashMap::new()
    };

    let data: Vec<FileResponse> = files
        .iter()
        .map(|f| {
            let api_purpose = match f.purpose {
                Some(fusillade::batch::Purpose::Batch) => Purpose::Batch,
                Some(fusillade::batch::Purpose::BatchOutput) => Purpose::BatchOutput,
                Some(fusillade::batch::Purpose::BatchError) => Purpose::BatchError,
                None => Purpose::Batch,
            };

            let individual_creator_id = f.api_key_id.and_then(|key_id| api_key_creator_map.get(&key_id).copied());
            let created_by_email = individual_creator_id.and_then(|uid| user_map.get(&uid)).map(|u| u.email.clone());

            let owner_id = f.uploaded_by.as_ref().and_then(|id| uuid::Uuid::parse_str(id).ok());
            let owner = owner_id.and_then(|id| user_map.get(&id));
            let (context_name, context_type) = match owner {
                Some(u) if u.user_type == "organization" => {
                    let name = u.display_name.clone().unwrap_or_else(|| u.email.clone());
                    (Some(name), Some("organization".to_string()))
                }
                Some(_) => (Some("Personal".to_string()), Some("personal".to_string())),
                None => (None, None),
            };

            FileResponse {
                id: f.id.0.to_string(),
                object_type: ObjectType::File,
                bytes: f.size_bytes,
                created_at: f.created_at.timestamp(),
                filename: f.name.clone(),
                purpose: api_purpose,
                expires_at: f.expires_at.map(|dt| dt.timestamp()),
                created_by_email,
                context_name,
                context_type,
                source: f.source_connection_id.map(|_| "sync".to_string()),
                source_name: f
                    .source_connection_id
                    .and_then(|id| connection_info.get(&id).map(|(name, _)| name.clone())),
            }
        })
        .collect();

    Ok(Json(FileListResponse {
        object_type: ListObject::List,
        data,
        first_id,
        last_id,
        has_more,
    }))
}

#[utoipa::path(
    get,
    path = "/files/{file_id}",
    tag = "files",
    summary = "Retrieve file",
    description = "Returns metadata about a specific file, including its size, creation time, and purpose.",
    responses(
        (status = 200, description = "File metadata.", body = FileResponse),
        (status = 404, description = "File not found or you don't have access to it."),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    ),
    params(
        ("file_id" = String, Path, description = "The file ID returned when the file was uploaded.")
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id, file_id = %file_id_str))]
pub async fn get_file<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(file_id_str): Path<String>,
    current_user: RequiresPermission<resource::Files, operation::ReadOwn>,
) -> Result<Json<FileResponse>> {
    let can_read_all_files = can_read_all_resources(&current_user, Resource::Files);

    let file_id = Uuid::parse_str(&file_id_str).map_err(|_| Error::BadRequest {
        message: "Invalid file ID format".to_string(),
    })?;

    let file = state
        .request_manager
        .get_file(fusillade::FileId(file_id))
        .await
        .map_err(|_e| Error::NotFound {
            resource: "File".to_string(),
            id: file_id_str.clone(),
        })?;

    if !can_read_all_files && !is_file_owner(&current_user, file.uploaded_by.as_deref()) {
        return Err(Error::NotFound {
            resource: "File".to_string(),
            id: file_id_str,
        });
    }

    let api_purpose = match file.purpose {
        Some(fusillade::batch::Purpose::Batch) => Purpose::Batch,
        Some(fusillade::batch::Purpose::BatchOutput) => Purpose::BatchOutput,
        Some(fusillade::batch::Purpose::BatchError) => Purpose::BatchError,
        None => Purpose::Batch,
    };

    let mut read_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;

    let created_by_email = if let Some(api_key_id) = file.api_key_id {
        let creator_map = ApiKeys::new(&mut read_conn)
            .get_creators_by_key_ids(vec![api_key_id])
            .await
            .map_err(Error::Database)?;
        if let Some(&creator_id) = creator_map.get(&api_key_id) {
            Users::new(&mut read_conn)
                .get_bulk(vec![creator_id])
                .await
                .map_err(|e| Error::Internal {
                    operation: format!("fetch creator user: {}", e),
                })?
                .get(&creator_id)
                .map(|u| u.email.clone())
        } else {
            None
        }
    } else {
        None
    };

    let (context_name, context_type) = if let Some(owner_id) = file.uploaded_by.as_ref().and_then(|id| Uuid::parse_str(id).ok()) {
        let user_map = Users::new(&mut read_conn)
            .get_bulk(vec![owner_id])
            .await
            .map_err(|e| Error::Internal {
                operation: format!("fetch owner user: {}", e),
            })?;
        match user_map.get(&owner_id) {
            Some(u) if u.user_type == "organization" => {
                let name = u.display_name.clone().unwrap_or_else(|| u.email.clone());
                (Some(name), Some("organization".to_string()))
            }
            Some(_) => (Some("Personal".to_string()), Some("personal".to_string())),
            None => (None, None),
        }
    } else {
        (None, None)
    };

    Ok(Json(FileResponse {
        id: file.id.0.to_string(),
        object_type: ObjectType::File,
        bytes: file.size_bytes,
        created_at: file.created_at.timestamp(),
        filename: file.name,
        purpose: api_purpose,
        expires_at: file.expires_at.map(|dt| dt.timestamp()),
        created_by_email,
        context_name,
        context_type,
        source: file.source_connection_id.map(|_| "sync".to_string()),
        source_name: if let Some(conn_id) = file.source_connection_id {
            match Connections::new(&mut read_conn).get_by_id(conn_id).await {
                Ok(Some(conn)) => Some(conn.name),
                Ok(None) => None,
                Err(e) => {
                    tracing::warn!(error = %e, connection_id = %conn_id, "Failed to look up connection name for file");
                    None
                }
            }
        } else {
            None
        },
    }))
}

#[utoipa::path(
    get,
    path = "/files/{file_id}/content",
    tag = "files",
    summary = "Retrieve file content",
    description = "Download the content of a file as JSONL.

For input files, returns the original request templates. For output files, returns the completed responses. Supports pagination via `limit` and `offset` query parameters.",
    responses(
        (status = 200, description = "File content as newline-delimited JSON. Check the `X-Incomplete` header to determine if more content exists.", content_type = "application/x-ndjson"),
        (status = 404, description = "File not found or you don't have access to it."),
        (status = 410, description = "The file has passed its `expires_at`; its content is no longer available."),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    ),
    params(
        ("file_id" = String, Path, description = "The file ID returned when the file was uploaded."),
        FileContentQuery
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id, file_id = %file_id_str))]
pub async fn get_file_content<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(file_id_str): Path<String>,
    Query(query): Query<FileContentQuery>,
    current_user: RequiresPermission<resource::Files, operation::ReadOwn>,
) -> Result<axum::response::Response> {
    let can_read_all_files = can_read_all_resources(&current_user, Resource::Files);

    let file_id = Uuid::parse_str(&file_id_str).map_err(|_| Error::BadRequest {
        message: "Invalid file ID format".to_string(),
    })?;

    let file = state
        .request_manager
        .get_file(fusillade::FileId(file_id))
        .await
        .map_err(|_e| Error::NotFound {
            resource: "File".to_string(),
            id: file_id_str.clone(),
        })?;

    if !can_read_all_files && !is_file_owner(&current_user, file.uploaded_by.as_deref()) {
        return Err(Error::NotFound {
            resource: "File".to_string(),
            id: file_id_str,
        });
    }

    if file.status == fusillade::batch::FileStatus::Expired || file.expires_at.is_some_and(|expires_at| expires_at <= Utc::now()) {
        return Err(Error::Gone {
            message: format!("File {file_id_str} has expired; its content is no longer available"),
        });
    }

    let (file_may_receive_more_data, file_content_count) = match file.purpose {
        Some(fusillade::batch::Purpose::Batch) => (false, None),
        Some(fusillade::batch::Purpose::BatchOutput) => {
            let batch = state
                .request_manager
                .get_batch_by_output_file_id(fusillade::FileId(file_id), fusillade::batch::OutputFileType::Output)
                .await
                .map_err(|e| Error::Internal {
                    operation: format!("get batch by output file: {}", e),
                })?;
            if let Some(batch) = batch {
                let status = state
                    .request_manager
                    .get_batch_status(batch.id)
                    .await
                    .map_err(|e| Error::Internal {
                        operation: format!("get batch status: {}", e),
                    })?;
                let still_processing = !status.is_finished();
                (still_processing, Some(status.completed_requests as usize))
            } else {
                (false, None)
            }
        }
        Some(fusillade::batch::Purpose::BatchError) => {
            let batch = state
                .request_manager
                .get_batch_by_output_file_id(fusillade::FileId(file_id), fusillade::batch::OutputFileType::Error)
                .await
                .map_err(|e| Error::Internal {
                    operation: format!("get batch by error file: {}", e),
                })?;
            if let Some(batch) = batch {
                let status = state
                    .request_manager
                    .get_batch_status(batch.id)
                    .await
                    .map_err(|e| Error::Internal {
                        operation: format!("get batch status: {}", e),
                    })?;
                let still_processing = !status.is_finished();
                (still_processing, Some(status.failed_requests as usize))
            } else {
                (false, None)
            }
        }
        None => (false, None),
    };

    let offset = query.pagination.skip() as usize;
    let search = query.search.clone();
    let requested_limit = query.pagination.limit.map(|_| query.pagination.limit() as usize);

    fn serialize_content_item(content_item: fusillade::FileContentItem) -> fusillade::Result<String> {
        match content_item {
            fusillade::FileContentItem::Template(template) => {
                OpenAIBatchRequest::from_internal(&template)
                    .map_err(|e| fusillade::FusilladeError::Other(anyhow::anyhow!("Failed to transform to OpenAI format: {:?}", e)))
                    .and_then(|openai_req| {
                        serde_json::to_string(&openai_req)
                            .map(|json| format!("{}\n", json))
                            .map_err(|e| fusillade::FusilladeError::Other(anyhow::anyhow!("JSON serialization failed: {}", e)))
                    })
            }
            fusillade::FileContentItem::Output(output) => serde_json::to_string(&output)
                .map(|json| format!("{}\n", json))
                .map_err(|e| fusillade::FusilladeError::Other(anyhow::anyhow!("JSON serialization failed: {}", e))),
            fusillade::FileContentItem::Error(error) => serde_json::to_string(&error)
                .map(|json| format!("{}\n", json))
                .map_err(|e| fusillade::FusilladeError::Other(anyhow::anyhow!("JSON serialization failed: {}", e))),
        }
    }

    if let Some(limit) = requested_limit {
        let content_stream = state
            .request_manager
            .get_file_content_stream(fusillade::FileId(file_id), offset, search);

        let mut buffer: Vec<_> = content_stream.take(limit + 1).collect().await;
        let has_more_pages = buffer.len() > limit;
        buffer.truncate(limit);

        let line_count = buffer.len();
        let last_line = offset + line_count;
        let has_more = has_more_pages || file_may_receive_more_data;

        let mut jsonl_lines = Vec::new();
        for content_result in buffer {
            let json_line = content_result.and_then(serialize_content_item).map_err(|e| Error::Internal {
                operation: format!("serialize content: {}", e),
            })?;
            jsonl_lines.push(json_line);
        }

        let jsonl_content = jsonl_lines.join("");

        let mut response = axum::response::Response::new(Body::from(jsonl_content));
        response
            .headers_mut()
            .insert("content-type", "application/x-ndjson".parse().unwrap());
        response.headers_mut().insert("X-Incomplete", has_more.to_string().parse().unwrap());
        response.headers_mut().insert("X-Last-Line", last_line.to_string().parse().unwrap());
        *response.status_mut() = StatusCode::OK;

        Ok(response)
    } else {
        let expected_count = if search.is_none() {
            file_content_count.map(|c| c.saturating_sub(offset))
        } else {
            None
        };

        let content_stream = state
            .request_manager
            .get_file_content_stream(fusillade::FileId(file_id), offset, search);

        let content_stream: Pin<Box<dyn Stream<Item = fusillade::Result<fusillade::FileContentItem>> + Send>> =
            if let Some(count) = expected_count {
                Box::pin(content_stream.take(count))
            } else {
                Box::pin(content_stream)
            };

        let body_stream = content_stream.map(|result| {
            result
                .and_then(|item| serialize_content_item(item).map(Bytes::from))
                .map_err(|e| std::io::Error::other(e.to_string()))
        });

        let body = Body::from_stream(body_stream);
        let mut response = axum::response::Response::new(body);
        response
            .headers_mut()
            .insert("content-type", "application/x-ndjson".parse().unwrap());
        response
            .headers_mut()
            .insert("X-Incomplete", file_may_receive_more_data.to_string().parse().unwrap());
        if let Some(count) = expected_count {
            let last_line = offset + count;
            response.headers_mut().insert("X-Last-Line", last_line.to_string().parse().unwrap());
        }
        *response.status_mut() = StatusCode::OK;

        Ok(response)
    }
}

#[utoipa::path(
    delete,
    path = "/files/{file_id}",
    tag = "files",
    summary = "Delete file",
    description = "Permanently delete a file.

Deleting a file also deletes any batches that were created from it. This action cannot be undone.",
    responses(
        (status = 200, description = "File deleted successfully.", body = FileDeleteResponse),
        (status = 404, description = "File not found or you don't have access to it."),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    ),
    params(
        ("file_id" = String, Path, description = "The file ID returned when the file was uploaded.")
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id, file_id = %file_id_str))]
pub async fn delete_file<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(file_id_str): Path<String>,
    current_user: RequiresPermission<resource::Files, operation::DeleteOwn>,
) -> Result<Json<FileDeleteResponse>> {
    let can_delete_all_files = can_read_all_resources(&current_user, Resource::Files);

    let file_id = Uuid::parse_str(&file_id_str).map_err(|_| Error::BadRequest {
        message: "Invalid file ID format".to_string(),
    })?;

    let file = state
        .request_manager
        .get_file(fusillade::FileId(file_id))
        .await
        .map_err(|_e| Error::NotFound {
            resource: "File".to_string(),
            id: file_id_str.clone(),
        })?;

    if !can_delete_all_files && !is_file_owner(&current_user, file.uploaded_by.as_deref()) {
        return Err(Error::NotFound {
            resource: "File".to_string(),
            id: file_id_str.clone(),
        });
    }

    state
        .request_manager
        .delete_file(fusillade::FileId(file_id))
        .await
        .map_err(|e| match e {
            fusillade::FusilladeError::FileNotFound(_) => Error::NotFound {
                resource: "File".to_string(),
                id: file_id_str.clone(),
            },
            other => Error::Internal {
                operation: format!("delete file: {}", other),
            },
        })?;

    Ok(Json(FileDeleteResponse {
        id: file_id.to_string(),
        object_type: ObjectType::File,
        deleted: true,
    }))
}

#[utoipa::path(
    get,
    path = "/files/{file_id}/cost-estimate",
    tag = "files",
    summary = "Get file cost estimate",
    description = "Estimate the cost of processing a batch file before creating a batch.

Returns a breakdown by model including estimated input/output tokens and cost. Useful for validating costs before committing to a batch run.",
    responses(
        (status = 200, description = "Cost estimate with per-model breakdown.", body = FileCostEstimate),
        (status = 404, description = "File not found or you don't have access to it."),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    ),
    params(
        ("file_id" = String, Path, description = "The ID of the file to estimate cost for"),
        FileCostEstimateQuery
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id, file_id = %file_id_str))]
pub async fn get_file_cost_estimate<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(file_id_str): Path<String>,
    Query(query): Query<FileCostEstimateQuery>,
    current_user: RequiresPermission<resource::Files, operation::ReadOwn>,
) -> Result<Json<crate::api::models::files::FileCostEstimate>> {
    let can_read_all_files = can_read_all_resources(&current_user, Resource::Files);

    let file_id = Uuid::parse_str(&file_id_str).map_err(|_| Error::BadRequest {
        message: "Invalid file ID format".to_string(),
    })?;

    let file = state
        .request_manager
        .get_file(fusillade::FileId(file_id))
        .await
        .map_err(|_e| Error::NotFound {
            resource: "File".to_string(),
            id: file_id_str.clone(),
        })?;

    if !can_read_all_files && !is_file_owner(&current_user, file.uploaded_by.as_deref()) {
        return Err(Error::NotFound {
            resource: "File".to_string(),
            id: file_id_str,
        });
    }

    let template_stats = state
        .request_manager
        .get_file_template_stats(fusillade::FileId(file_id))
        .await
        .map_err(|e| Error::Internal {
            operation: format!("get file template stats: {}", e),
        })?;

    let mut model_stats: HashMap<String, (i64, i64)> = HashMap::new();

    for stat in &template_stats {
        let estimated_input_tokens = stat.total_body_bytes / 4;
        model_stats.insert(stat.model.clone(), (stat.request_count, estimated_input_tokens));
    }

    let models_in_file: Vec<String> = template_stats.iter().map(|s| s.model.clone()).collect();

    let mut conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut deployments_repo = Deployments::new(&mut conn);

    let filter = DeploymentFilter::new(0, 1000)
        .with_statuses(vec![ModelStatus::Active])
        .with_deleted(false);
    let all_deployments = deployments_repo.list(&filter).await.map_err(Error::Database)?;

    let mut model_info: HashMap<
        String,
        (
            crate::db::models::deployments::DeploymentDBResponse,
            Option<i64>,
            Option<crate::db::models::deployments::ModelType>,
        ),
    > = HashMap::new();

    for deployment in all_deployments {
        if !models_in_file.contains(&deployment.alias) {
            model_info.insert(deployment.alias.clone(), (deployment.clone(), None, deployment.model_type.clone()));
            continue;
        }

        let avg_output_tokens: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT AVG(completion_tokens)::BIGINT
            FROM (
                SELECT completion_tokens
                FROM http_analytics
                WHERE model = $1
                  AND completion_tokens IS NOT NULL
                  AND status_code = 200
                ORDER BY timestamp DESC
                LIMIT 100
            ) recent_responses
            "#,
        )
        .bind(&deployment.alias)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| Error::Database(e.into()))?
        .flatten();

        model_info.insert(
            deployment.alias.clone(),
            (deployment.clone(), avg_output_tokens, deployment.model_type.clone()),
        );
    }

    let mut total_cost = Decimal::ZERO;
    let mut model_breakdowns = Vec::new();

    let mut tariffs_repo = Tariffs::new(&mut conn);
    let current_time = Utc::now();

    let completion_window = query.completion_window.as_deref().unwrap_or("24h");
    let pricing_account = file.uploaded_by.as_deref().and_then(|owner| Uuid::parse_str(owner).ok());

    for (model_alias, (request_count, input_tokens)) in model_stats {
        let (deployment_opt, avg_output_tokens, model_type) = model_info
            .get(&model_alias)
            .map(|(d, avg, mt)| (Some(d.clone()), *avg, mt.clone()))
            .unwrap_or((None, None, None));

        let estimated_output_tokens = if matches!(model_type, Some(crate::db::models::deployments::ModelType::Embeddings)) {
            request_count
        } else if let Some(avg) = avg_output_tokens {
            avg * request_count
        } else {
            ((input_tokens as f64) * 1.1) as i64
        };

        let cost = if let Some(deployment) = deployment_opt {
            let pricing_result = tariffs_repo
                .get_effective_pricing_at_timestamp(
                    deployment.id,
                    pricing_account,
                    "batch",
                    Some(completion_window),
                    Some("standard"),
                    current_time,
                )
                .await
                .map_err(Error::Database)?;

            if let Some((input_price, output_price)) = pricing_result {
                let input_cost = Decimal::from(input_tokens) * input_price;
                let output_cost = Decimal::from(estimated_output_tokens) * output_price;
                input_cost + output_cost
            } else {
                Decimal::ZERO
            }
        } else {
            Decimal::ZERO
        };

        total_cost += cost;

        model_breakdowns.push(crate::api::models::files::ModelCostBreakdown {
            model: model_alias,
            request_count,
            estimated_input_tokens: input_tokens,
            estimated_output_tokens,
            estimated_cost: cost.to_string(),
        });
    }

    let total_requests: i64 = model_breakdowns.iter().map(|m| m.request_count).sum();
    let total_input_tokens: i64 = model_breakdowns.iter().map(|m| m.estimated_input_tokens).sum();
    let total_output_tokens: i64 = model_breakdowns.iter().map(|m| m.estimated_output_tokens).sum();

    Ok(Json(crate::api::models::files::FileCostEstimate {
        file_id: file_id_str,
        total_requests,
        total_estimated_input_tokens: total_input_tokens,
        total_estimated_output_tokens: total_output_tokens,
        total_estimated_cost: total_cost.to_string(),
        models: model_breakdowns,
    }))
}

#[cfg(test)]
mod tests {
    use crate::api::models::files::FileResponse;
    use crate::api::models::users::Role;
    use crate::db::models::api_keys::ApiKeyPurpose;
    use crate::test::utils::*;
    use chrono::Utc;
    use sqlx::PgPool;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    async fn upload_batch_jsonl(
        app: &axum_test::TestServer,
        user: &crate::api::models::users::UserResponse,
        jsonl: &str,
    ) -> axum_test::TestResponse {
        app.post("/ai/v1/files")
            .add_header(&add_auth_headers(user)[0].0, &add_auth_headers(user)[0].1)
            .add_header(&add_auth_headers(user)[1].0, &add_auth_headers(user)[1].1)
            .multipart(axum_test::multipart::MultipartForm::new().add_text("purpose", "batch").add_part(
                "file",
                axum_test::multipart::Part::bytes(jsonl.as_bytes().to_vec()).file_name("reasoning-batch.jsonl"),
            ))
            .await
    }

    async fn batch_uploader(pool: &PgPool) -> crate::api::models::users::UserResponse {
        let user = create_test_user_with_roles(pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(pool).await;
        add_user_to_group(pool, user.id, group.id).await;
        let deployment = create_test_deployment(pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(pool, deployment.id, group.id, user.id).await;
        user
    }

    const ONE_LINE: &str = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn upload_without_expires_after_gets_the_default_lifetime(pool: PgPool) {
        let default_seconds = 2 * 24 * 60 * 60;
        let mut config = create_test_config();
        config.batches.files.default_expiry_seconds = default_seconds;
        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = batch_uploader(&pool).await;

        let before = Utc::now().timestamp();
        let upload = upload_batch_jsonl(&app, &user, ONE_LINE).await;
        upload.assert_status(axum::http::StatusCode::CREATED);
        let file: FileResponse = upload.json();

        let expires_at = file.expires_at.expect("uploads always carry an expiry");
        assert!(
            (before + default_seconds..=before + default_seconds + 5).contains(&expires_at),
            "expires_at {expires_at} is not {default_seconds}s after upload at {before}"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn upload_rejects_expires_after_outside_the_configured_range(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = batch_uploader(&pool).await;
        let limits = crate::config::FilesConfig::default();
        let before = upload_rejections("invalid_expiry");

        for value in [
            (limits.max_expiry_seconds + 1).to_string(),
            (limits.min_expiry_seconds - 1).to_string(),
            "-1".to_string(),
            "soon".to_string(),
        ] {
            let response = app
                .post("/ai/v1/files")
                .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
                .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
                .multipart(
                    axum_test::multipart::MultipartForm::new()
                        .add_text("purpose", "batch")
                        .add_text("expires_after[seconds]", value.clone())
                        .add_part(
                            "file",
                            axum_test::multipart::Part::bytes(ONE_LINE.as_bytes().to_vec()).file_name("batch.jsonl"),
                        ),
                )
                .await;
            response.assert_status(axum::http::StatusCode::BAD_REQUEST);
            let body = response.text();
            assert!(body.contains("expires_after[seconds]"), "value {value}: {body}");
            assert!(body.contains(&limits.max_expiry_seconds.to_string()), "value {value}: {body}");
        }
        assert_eq!(upload_rejections("invalid_expiry"), before + 4);

        let files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM fusillade.files")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(files, 0, "a refused upload leaves no file behind");

        let response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_text("expires_after[seconds]", limits.max_expiry_seconds.to_string())
                    .add_part(
                        "file",
                        axum_test::multipart::Part::bytes(ONE_LINE.as_bytes().to_vec()).file_name("batch.jsonl"),
                    ),
            )
            .await;
        response.assert_status(axum::http::StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn downloading_an_expired_file_is_gone(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = batch_uploader(&pool).await;
        let upload = upload_batch_jsonl(&app, &user, ONE_LINE).await;
        upload.assert_status(axum::http::StatusCode::CREATED);
        let file: FileResponse = upload.json();
        let file_id = uuid::Uuid::parse_str(&file.id).unwrap();

        let content = app
            .get(&format!("/ai/v1/files/{}/content", file.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        content.assert_status_ok();

        sqlx::query("UPDATE fusillade.files SET expires_at = NOW() - INTERVAL '1 second' WHERE id = $1")
            .bind(file_id)
            .execute(&pool)
            .await
            .unwrap();
        let content = app
            .get(&format!("/ai/v1/files/{}/content", file.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        content.assert_status(axum::http::StatusCode::GONE);

        let metadata = app
            .get(&format!("/ai/v1/files/{}", file.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        metadata.assert_status_ok();
        let status: String = sqlx::query_scalar("SELECT status FROM fusillade.files WHERE id = $1")
            .bind(file_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "expired");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn validates_reasoning_controls_during_upload(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let deployment = create_test_deployment(&pool, user.id, "reasoning-model", "reasoning-model").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        sqlx::query(
            r#"
            UPDATE deployed_models
            SET reasoning_translation_overrides = $1
            WHERE id = $2
            "#,
        )
        .bind(serde_json::json!({
            "chat_completions": {
                "mode": "override",
                "translation": {
                    "unsupported_efforts": ["none", "minimal", "low", "medium", "xhigh", "max"],
                    "writes": [
                        {"target_path": "/reasoning_effort", "values": {"high": "high"}},
                        {"target_path": "/thinking_token_budget", "values": {"high": 8192}}
                    ]
                }
            },
            "responses": {"mode": "disabled"}
        }))
        .bind(deployment.id)
        .execute(&pool)
        .await
        .unwrap();

        let omitted = upload_batch_jsonl(
            &app,
            &user,
            r#"{"custom_id":"omitted","method":"POST","url":"/v1/chat/completions","body":{"model":"reasoning-model","messages":[]}}"#,
        )
        .await;
        omitted.assert_status(axum::http::StatusCode::CREATED);

        let provider_native = upload_batch_jsonl(
            &app,
            &user,
            r#"{"custom_id":"native","method":"POST","url":"/v1/chat/completions","body":{"model":"reasoning-model","messages":[],"thinking":{"type":"enabled"}}}"#,
        )
        .await;
        provider_native.assert_status(axum::http::StatusCode::BAD_REQUEST);
        assert!(provider_native.text().contains("use 'reasoning_effort'"));

        let unsupported = upload_batch_jsonl(
            &app,
            &user,
            r#"{"custom_id":"unsupported","method":"POST","url":"/v1/chat/completions","body":{"model":"reasoning-model","messages":[],"reasoning_effort":"medium"}}"#,
        )
        .await;
        unsupported.assert_status(axum::http::StatusCode::BAD_REQUEST);
        assert!(unsupported.text().contains("not supported by this provider"));

        let missing_budget_limit = upload_batch_jsonl(
            &app,
            &user,
            r#"{"custom_id":"budget","method":"POST","url":"/v1/chat/completions","body":{"model":"reasoning-model","messages":[],"reasoning_effort":"high"}}"#,
        )
        .await;
        missing_budget_limit.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
        assert!(missing_budget_limit.text().contains("max_completion_tokens is not set"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_and_download_file_content(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello 1"}]}}
{"custom_id":"request-2","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello 2"}]}}
{"custom_id":"request-3","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello 3"}]}}
"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
        let file: FileResponse = upload_response.json();
        let file_id = file.id;

        let download_response = app
            .get(&format!("/ai/v1/files/{}/content", file_id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        download_response.assert_status(axum::http::StatusCode::OK);
        download_response.assert_header("content-type", "application/x-ndjson");

        let downloaded_content = download_response.text();

        let original_lines: Vec<&str> = jsonl_content.trim().lines().collect();
        let downloaded_lines: Vec<&str> = downloaded_content.trim().lines().collect();

        assert_eq!(original_lines.len(), downloaded_lines.len(), "Number of lines should match");

        for (i, (orig, down)) in original_lines.iter().zip(downloaded_lines.iter()).enumerate() {
            let orig_json: serde_json::Value = serde_json::from_str(orig).unwrap_or_else(|_| panic!("Failed to parse original line {}", i));
            let down_json: serde_json::Value =
                serde_json::from_str(down).unwrap_or_else(|_| panic!("Failed to parse downloaded line {}", i));
            assert_eq!(orig_json, down_json, "Line {} should match (orig: {}, down: {})", i, orig, down);
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_compressed_file_stores_decoded_content(pool: PgPool) {
        use std::io::Write;

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello 1"}]}}
{"custom_id":"request-2","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello 2"}]}}
"#;
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gzip.write_all(jsonl_content.as_bytes()).unwrap();
        let gzip = gzip.finish().unwrap();
        let zstd = zstd::encode_all(jsonl_content.as_bytes(), 3).unwrap();

        for (compressed, name) in [(gzip, "test-batch.jsonl.gz"), (zstd, "test-batch.jsonl.zst")] {
            let upload_response = app
                .post("/ai/v1/files")
                .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
                .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
                .multipart(
                    axum_test::multipart::MultipartForm::new()
                        .add_text("purpose", "batch")
                        .add_part("file", axum_test::multipart::Part::bytes(compressed).file_name(name)),
                )
                .await;

            upload_response.assert_status(axum::http::StatusCode::CREATED);
            let file: FileResponse = upload_response.json();
            assert_eq!(file.filename, "test-batch.jsonl", "{name}");
            assert_eq!(file.bytes, jsonl_content.len() as i64, "{name}: size is the decoded size");

            let downloaded = app
                .get(&format!("/ai/v1/files/{}/content", file.id))
                .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
                .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
                .await
                .text();
            let ids: Vec<String> = downloaded
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()["custom_id"].to_string())
                .collect();
            assert_eq!(ids, ["\"request-1\"", "\"request-2\""], "{name}");
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_broken_or_unsupported_compression_names_the_problem(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;

        let truncated_gzip = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\xab\x56".to_vec();
        let xz = b"\xfd7zXZ\x00\x00\x04\xe6\xd6\xb4\x46".to_vec();
        for (content, status, expected, code) in [
            (
                truncated_gzip,
                axum::http::StatusCode::BAD_REQUEST,
                "not valid gzip data",
                "invalid_compression",
            ),
            (
                xz,
                axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "compressed with xz",
                "unsupported_compression",
            ),
        ] {
            let before = crate::test::utils::upload_rejections(code);
            let upload_response = app
                .post("/ai/v1/files")
                .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
                .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
                .multipart(
                    axum_test::multipart::MultipartForm::new()
                        .add_text("purpose", "batch")
                        .add_part("file", axum_test::multipart::Part::bytes(content).file_name("batch.jsonl")),
                )
                .await;

            upload_response.assert_status(status);
            let body = upload_response.text();
            assert!(body.contains(expected), "{body}");
            assert_eq!(crate::test::utils::upload_rejections(code), before + 1, "{code} is counted");
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_that_is_not_a_multipart_form_returns_415(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let line = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4"}}"#;
        let upload_rejections = crate::test::utils::upload_rejections;
        let (not_multipart, invalid_multipart) = (upload_rejections("not_multipart"), upload_rejections("invalid_multipart"));

        for content_type in [Some("application/jsonl"), Some("application/x-www-form-urlencoded"), None] {
            let mut request = app
                .post("/ai/v1/files")
                .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
                .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1);
            if let Some(content_type) = content_type {
                request = request.content_type(content_type);
            }
            let upload_response = request.bytes(line.as_bytes().to_vec().into()).await;

            upload_response.assert_status(axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE);
            assert!(upload_response.text().contains("multipart/form-data"), "{content_type:?}");
        }

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .content_type("multipart/form-data")
            .bytes(line.as_bytes().to_vec().into())
            .await;
        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);

        assert_eq!(upload_rejections("not_multipart"), not_multipart + 3);
        assert_eq!(upload_rejections("invalid_multipart"), invalid_multipart + 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_missing_model_field(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"messages":[{"role":"user","content":"Hello"}]}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let error_body = upload_response.text();
        assert!(error_body.contains("model"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_invalid_purpose_returns_400(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;

        let jsonl = r#"{"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}}"#;

        for bad_purpose in ["batches", "", "batch_output", "batch_error"] {
            let file_part = axum_test::multipart::Part::bytes(jsonl.as_bytes()).file_name("test-batch.jsonl");
            let resp = app
                .post("/ai/v1/files")
                .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
                .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
                .multipart(
                    axum_test::multipart::MultipartForm::new()
                        .add_text("purpose", bad_purpose)
                        .add_part("file", file_part),
                )
                .await;

            resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
            let body = resp.text();
            assert!(
                body.contains("Only 'batch' is supported"),
                "purpose={bad_purpose:?}: unexpected body: {body}"
            );
        }

        let list = app
            .get("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        list.assert_status(axum::http::StatusCode::OK);
        let listed: serde_json::Value = list.json();
        assert_eq!(
            listed["data"].as_array().map(|a| a.len()),
            Some(0),
            "expected no files, got: {listed}"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_model_access_denied(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "allowed-model", "allowed-model").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"unauthorized-model","messages":[{"role":"user","content":"Hello"}]}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::FORBIDDEN);
        let error_body = upload_response.text();
        assert!(error_body.contains("Model"));
        assert!(error_body.contains("has not been configured or is not available to user"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_missing_custom_id(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;

        let jsonl_content =
            r#"{"method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let error_body = upload_response.text();
        assert!(error_body.contains("custom_id"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_custom_id_with_control_characters(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = "{\"custom_id\":\"request-1\\r\\nX-Injected: malicious\",\"method\":\"POST\",\"url\":\"/v1/chat/completions\",\"body\":{\"model\":\"gpt-4\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello\"}]}}";

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let error_body = upload_response.text();
        assert!(error_body.contains("invalid characters"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_custom_id_too_long(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let long_id = "a".repeat(65);
        let jsonl_content = format!(
            r#"{{"custom_id":"{}","method":"POST","url":"/v1/chat/completions","body":{{"model":"gpt-4","messages":[{{"role":"user","content":"Hello"}}]}}}}"#,
            long_id
        );

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes().to_vec()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let error_body = upload_response.text();
        assert!(error_body.contains("exceeds maximum length"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_invalid_json_body(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":"not a json object"}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let error_body = upload_response.text();
        assert!(error_body.contains("model"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_malformed_jsonl(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;

        let jsonl_content = "this is not json at all\n{also not json}";

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_empty_file(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;

        let jsonl_content = "";

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_with_metadata_after_file_field(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_part("file", file_part)
                    .add_text("purpose", "batch")
                    .add_text("expires_after[anchor]", "processing_complete")
                    .add_text("expires_after[seconds]", "86400"),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
        let file: FileResponse = upload_response.json();

        let get_response = app
            .get(&format!("/ai/v1/files/{}", file.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        get_response.assert_status(axum::http::StatusCode::OK);
        let retrieved_file: FileResponse = get_response.json();
        assert_eq!(retrieved_file.purpose, crate::api::models::files::Purpose::Batch);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_file_cost_estimate(pool: PgPool) {
        use rust_decimal::Decimal;
        use std::str::FromStr;

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment1 = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment1.id, group.id, user.id).await;

        let deployment2 = create_test_deployment(&pool, user.id, "gpt-3.5-model", "gpt-3.5").await;
        add_deployment_to_group(&pool, deployment2.id, group.id, user.id).await;

        use crate::db::handlers::Tariffs;
        use crate::db::models::tariffs::TariffCreateDBRequest;

        let mut conn = pool.acquire().await.unwrap();
        let mut tariffs_repo = Tariffs::new(&mut conn);

        tariffs_repo
            .create(&TariffCreateDBRequest {
                deployed_model_id: deployment1.id,
                name: "batch".to_string(),
                input_price_per_token: Decimal::from_str("0.00003").unwrap(),
                output_price_per_token: Decimal::from_str("0.00006").unwrap(),
                api_key_purpose: Some(ApiKeyPurpose::Batch),
                completion_window: Some("24h".to_string()),
                valid_from: Some(Utc::now() - chrono::Duration::minutes(1)),
                user_id: None,
            })
            .await
            .unwrap();

        tariffs_repo
            .create(&TariffCreateDBRequest {
                deployed_model_id: deployment2.id,
                name: "batch".to_string(),
                input_price_per_token: Decimal::from_str("0.000001").unwrap(),
                output_price_per_token: Decimal::from_str("0.000002").unwrap(),
                api_key_purpose: Some(ApiKeyPurpose::Batch),
                completion_window: Some("24h".to_string()),
                valid_from: Some(Utc::now() - chrono::Duration::minutes(1)),
                user_id: None,
            })
            .await
            .unwrap();

        drop(conn);

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello 1"}]}}
{"custom_id":"request-2","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-3.5","messages":[{"role":"user","content":"Hello 2"}]}}
{"custom_id":"request-3","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello 3"}]}}
"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
        let file: FileResponse = upload_response.json();
        let file_id = file.id;

        let estimate_response = app
            .get(&format!("/ai/v1/files/{}/cost-estimate", file_id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        estimate_response.assert_status(axum::http::StatusCode::OK);
        let estimate: crate::api::models::files::FileCostEstimate = estimate_response.json();

        assert_eq!(estimate.file_id, file_id);
        assert_eq!(estimate.total_requests, 3);
        assert_eq!(estimate.models.len(), 2);

        let gpt4_breakdown = estimate
            .models
            .iter()
            .find(|m| m.model == "gpt-4")
            .expect("Should have gpt-4 breakdown");
        let gpt35_breakdown = estimate
            .models
            .iter()
            .find(|m| m.model == "gpt-3.5")
            .expect("Should have gpt-3.5 breakdown");

        assert_eq!(gpt4_breakdown.request_count, 2);
        assert_eq!(gpt35_breakdown.request_count, 1);

        assert!(gpt4_breakdown.estimated_input_tokens > 0);
        assert!(gpt4_breakdown.estimated_output_tokens > 0);
        assert!(gpt35_breakdown.estimated_input_tokens > 0);
        assert!(gpt35_breakdown.estimated_output_tokens > 0);

        let gpt4_cost = Decimal::from_str(&gpt4_breakdown.estimated_cost).unwrap();
        let gpt35_cost = Decimal::from_str(&gpt35_breakdown.estimated_cost).unwrap();
        assert!(gpt4_cost > Decimal::ZERO, "GPT-4 cost should be greater than zero");
        assert!(gpt35_cost > Decimal::ZERO, "GPT-3.5 cost should be greater than zero");

        let total_cost = Decimal::from_str(&estimate.total_estimated_cost).unwrap();
        assert_eq!(total_cost, gpt4_cost + gpt35_cost);

        let total_input: i64 = estimate.models.iter().map(|m| m.estimated_input_tokens).sum();
        let total_output: i64 = estimate.models.iter().map(|m| m.estimated_output_tokens).sum();
        assert_eq!(estimate.total_estimated_input_tokens, total_input);
        assert_eq!(estimate.total_estimated_output_tokens, total_output);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_file_cost_estimate_with_different_slas(pool: PgPool) {
        use rust_decimal::Decimal;
        use std::str::FromStr;

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        use crate::db::handlers::Tariffs;
        use crate::db::models::tariffs::TariffCreateDBRequest;

        let mut conn = pool.acquire().await.unwrap();
        let mut tariffs_repo = Tariffs::new(&mut conn);

        tariffs_repo
            .create(&TariffCreateDBRequest {
                deployed_model_id: deployment.id,
                name: "batch-24h".to_string(),
                input_price_per_token: Decimal::from_str("0.00003").unwrap(),
                output_price_per_token: Decimal::from_str("0.00006").unwrap(),
                api_key_purpose: Some(ApiKeyPurpose::Batch),
                completion_window: Some("24h".to_string()),
                valid_from: Some(Utc::now() - chrono::Duration::minutes(1)),
                user_id: None,
            })
            .await
            .unwrap();

        tariffs_repo
            .create(&TariffCreateDBRequest {
                deployed_model_id: deployment.id,
                name: "batch-1h".to_string(),
                input_price_per_token: Decimal::from_str("0.00006").unwrap(),
                output_price_per_token: Decimal::from_str("0.00012").unwrap(),
                api_key_purpose: Some(ApiKeyPurpose::Batch),
                completion_window: Some("1h".to_string()),
                valid_from: Some(Utc::now() - chrono::Duration::minutes(1)),
                user_id: None,
            })
            .await
            .unwrap();

        drop(conn);

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
        let file: FileResponse = upload_response.json();
        let file_id = file.id;

        let estimate_24h_response = app
            .get(&format!("/ai/v1/files/{}/cost-estimate", file_id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        estimate_24h_response.assert_status(axum::http::StatusCode::OK);
        let estimate_24h: crate::api::models::files::FileCostEstimate = estimate_24h_response.json();

        let estimate_1h_response = app
            .get(&format!("/ai/v1/files/{}/cost-estimate?completion_window=1h", file_id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        estimate_1h_response.assert_status(axum::http::StatusCode::OK);
        let estimate_1h: crate::api::models::files::FileCostEstimate = estimate_1h_response.json();

        assert_eq!(estimate_24h.total_estimated_input_tokens, estimate_1h.total_estimated_input_tokens);
        assert_eq!(
            estimate_24h.total_estimated_output_tokens,
            estimate_1h.total_estimated_output_tokens
        );

        let cost_24h = Decimal::from_str(&estimate_24h.total_estimated_cost).unwrap();
        let cost_1h = Decimal::from_str(&estimate_1h.total_estimated_cost).unwrap();

        assert!(cost_1h > cost_24h, "1h priority should cost more than 24h priority");
        assert!(cost_24h > Decimal::ZERO, "24h priority cost should be greater than zero");

        let ratio = cost_1h / cost_24h;
        assert!(
            ratio > Decimal::from_str("1.9").unwrap() && ratio < Decimal::from_str("2.1").unwrap(),
            "1h priority should be approximately 2x the cost of 24h priority, got ratio: {}",
            ratio
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_invalid_http_method(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"GET","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let error_body = upload_response.text();
        assert!(error_body.contains("Unsupported HTTP method"));
        assert!(error_body.contains("GET"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_invalid_url_path(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/api/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let error_body = upload_response.text();
        assert!(error_body.contains("Unsupported URL path"));
        assert!(error_body.contains("/api/completions"));
        assert!(error_body.contains("/v1/chat/completions"));
        assert!(error_body.contains("/v1/embeddings"));
        assert!(error_body.contains("/v1/responses"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_accepts_responses_url_path(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/responses","body":{"model":"gpt-4","input":"Hello"}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_accepts_messages_url_path(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/messages","body":{"model":"gpt-4","max_tokens":16,"messages":[{"role":"user","content":"Hello"}]}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_strips_priority_field(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "qwen-model", "Qwen/Qwen3-VL-30B-A3B-Instruct-FP8").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id": "priority-hijack", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "Qwen/Qwen3-VL-30B-A3B-Instruct-FP8", "messages": [{"role": "user", "content": "urgent"}], "priority": -999999, "nvext": {"router": {"ttft_target": 1, "itl_target": 2, "keep": true}, "agent_hints": {"priority": 999, "keep": true}, "keep": true}}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-priority.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
        let file: FileResponse = upload_response.json();
        let file_id = file.id;

        let download_response = app
            .get(&format!("/ai/v1/files/{}/content", file_id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        download_response.assert_status(axum::http::StatusCode::OK);
        let downloaded_content = download_response.text();

        let downloaded_json: serde_json::Value =
            serde_json::from_str(downloaded_content.trim()).expect("Downloaded content should be valid JSON");

        let body = downloaded_json.get("body").expect("Should have body field");
        assert!(body.get("priority").is_none(), "Priority field should be stripped from body");

        assert!(body["nvext"]["router"].get("ttft_target").is_none());
        assert!(body["nvext"]["router"].get("itl_target").is_none());
        assert!(body["nvext"]["agent_hints"].get("priority").is_none());
        assert_eq!(body["nvext"]["router"]["keep"], true);
        assert_eq!(body["nvext"]["agent_hints"]["keep"], true);
        assert_eq!(body["nvext"]["keep"], true);

        assert_eq!(
            body.get("model").and_then(|v| v.as_str()).unwrap(),
            "Qwen/Qwen3-VL-30B-A3B-Instruct-FP8"
        );
        assert!(body.get("messages").is_some(), "Messages field should be preserved");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_x_incomplete_false_for_batch_input_file(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hi"}]}}
{"custom_id":"req-2","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}
{"custom_id":"req-3","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hey"}]}}
"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");
        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
        let file: FileResponse = upload_response.json();

        let response = app
            .get(&format!("/ai/v1/files/{}/content", file.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::OK);

        let incomplete_header = response.headers().get("x-incomplete");
        assert_eq!(
            incomplete_header.and_then(|h| h.to_str().ok()),
            Some("false"),
            "Batch input file should have X-Incomplete: false"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_x_incomplete_true_with_pagination(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Msg 1"}]}}
{"custom_id":"req-2","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Msg 2"}]}}
{"custom_id":"req-3","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Msg 3"}]}}
{"custom_id":"req-4","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Msg 4"}]}}
{"custom_id":"req-5","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Msg 5"}]}}
"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");
        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
        let file: FileResponse = upload_response.json();

        let response = app
            .get(&format!("/ai/v1/files/{}/content?limit=2", file.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::OK);

        let incomplete_header = response.headers().get("x-incomplete");
        assert_eq!(
            incomplete_header.and_then(|h| h.to_str().ok()),
            Some("true"),
            "Should have X-Incomplete: true when paginated"
        );

        let response = app
            .get(&format!("/ai/v1/files/{}/content", file.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::OK);

        let incomplete_header = response.headers().get("x-incomplete");
        assert_eq!(
            incomplete_header.and_then(|h| h.to_str().ok()),
            Some("false"),
            "Should have X-Incomplete: false when all data fetched"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_x_incomplete_for_batch_output_file_running(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Test"}]}}
"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");
        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
        let file: FileResponse = upload_response.json();

        let batch_response = app
            .post("/ai/v1/batches")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .json(&serde_json::json!({
                "input_file_id": file.id,
                "endpoint": "/v1/chat/completions",
                "completion_window": "24h"
            }))
            .await;

        batch_response.assert_status(axum::http::StatusCode::CREATED);
        let batch: serde_json::Value = batch_response.json();
        let output_file_id = batch["output_file_id"].as_str().expect("Should have output_file_id");

        let response = app
            .get(&format!("/ai/v1/files/{}/content", output_file_id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::OK);

        let incomplete_header = response.headers().get("x-incomplete");
        assert_eq!(
            incomplete_header.and_then(|h| h.to_str().ok()),
            Some("true"),
            "Output file should be incomplete while batch has pending requests"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_x_incomplete_false_for_batch_output_file_complete(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Test"}]}}
"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");
        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
        let file: FileResponse = upload_response.json();

        let batch_response = app
            .post("/ai/v1/batches")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .json(&serde_json::json!({
                "input_file_id": file.id,
                "endpoint": "/v1/chat/completions",
                "completion_window": "24h"
            }))
            .await;

        batch_response.assert_status(axum::http::StatusCode::CREATED);
        let batch: serde_json::Value = batch_response.json();
        let batch_id = batch["id"].as_str().expect("Should have id");
        let output_file_id = batch["output_file_id"].as_str().expect("Should have output_file_id");

        let batch_uuid = batch_id.strip_prefix("batch_").unwrap_or(batch_id);
        let batch_uuid = Uuid::parse_str(batch_uuid).expect("Valid batch UUID");

        for attempt in 0..200 {
            let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM fusillade.requests WHERE batch_id = $1")
                .bind(batch_uuid)
                .fetch_one(&pool)
                .await
                .expect("Failed to count requests");
            if count > 0 {
                break;
            }
            assert!(
                attempt < 199,
                "Timed out waiting for requests to be populated for batch {batch_uuid}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        sqlx::query(
            r#"
            UPDATE fusillade.requests
            SET state = 'completed', response_status = 200, response_body = '{"choices":[]}', completed_at = NOW()
            WHERE batch_id = $1
            "#,
        )
        .bind(batch_uuid)
        .execute(&pool)
        .await
        .expect("Failed to complete requests");

        let response = app
            .get(&format!("/ai/v1/files/{}/content", output_file_id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::OK);

        let incomplete_header = response.headers().get("x-incomplete");
        assert_eq!(
            incomplete_header.and_then(|h| h.to_str().ok()),
            Some("false"),
            "Output file should be complete when batch has no pending/in-progress requests"
        );
    }

    #[tokio::test]
    async fn test_upload_rate_limiting_rejects_when_queue_full() {
        use crate::config::FileLimitsConfig;
        use crate::limits::UploadLimiter;
        use std::sync::Arc;

        let config = FileLimitsConfig {
            max_concurrent_uploads: 1,
            max_waiting_uploads: 1,
            max_upload_wait_secs: 0,
            ..Default::default()
        };
        let limiter = Arc::new(UploadLimiter::new(&config).unwrap());

        let _permit1 = limiter.acquire().await.unwrap();

        let limiter_clone = limiter.clone();
        let handle = tokio::spawn(async move { limiter_clone.acquire().await });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let result = limiter.acquire().await;
        assert!(result.is_err(), "Third request should be rejected when queue is full");

        if let Err(crate::errors::Error::TooManyRequests { message }) = result {
            assert!(message.contains("Too many file uploads"));
        } else {
            panic!("Expected TooManyRequests error");
        }

        drop(_permit1);
        let _ = handle.await;
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_with_rate_limiter_configured(pool: PgPool) {
        let mut config = create_test_config();
        config.limits.files.max_concurrent_uploads = 10;
        config.limits.files.max_waiting_uploads = 20;
        config.limits.files.max_upload_wait_secs = 60;

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_rejects_file_exceeding_max_requests(pool: PgPool) {
        let mut config = create_test_config();
        config.limits.files.max_requests_per_file = 2;

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}
{"custom_id":"request-2","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}
{"custom_id":"request-3","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let body = upload_response.text();
        assert!(
            body.contains("exceeds the maximum of"),
            "Expected error about request limit, got: {}",
            body
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_allows_file_at_max_requests(pool: PgPool) {
        let mut config = create_test_config();
        config.limits.files.max_requests_per_file = 2;

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}
{"custom_id":"request-2","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_file_content_streaming(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let num_requests = 30;
        let jsonl_lines: Vec<String> = (0..num_requests)
            .map(|i| {
                format!(
                    r#"{{"custom_id":"req-{}","method":"POST","url":"/v1/chat/completions","body":{{"model":"gpt-4","messages":[{{"role":"user","content":"Test {}"}}]}}}}"#,
                    i, i
                )
            })
            .collect();
        let jsonl_content = jsonl_lines.join("\n") + "\n";

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes().to_vec()).file_name("test.jsonl");
        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
        let file: FileResponse = upload_response.json();

        let batch_response = app
            .post("/ai/v1/batches")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .json(&serde_json::json!({
                "input_file_id": file.id,
                "endpoint": "/v1/chat/completions",
                "completion_window": "24h"
            }))
            .await;

        batch_response.assert_status(axum::http::StatusCode::CREATED);
        let batch: serde_json::Value = batch_response.json();
        let batch_id_str = batch["id"].as_str().expect("Should have id");
        let output_file_id = batch["output_file_id"].as_str().expect("Should have output_file_id");

        let batch_uuid_str = batch_id_str.strip_prefix("batch_").unwrap_or(batch_id_str);
        let batch_uuid = Uuid::parse_str(batch_uuid_str).expect("Valid batch UUID");

        loop {
            let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM fusillade.requests WHERE batch_id = $1")
                .bind(batch_uuid)
                .fetch_one(&pool)
                .await
                .expect("Failed to count requests");
            if count > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        sqlx::query(
            r#"
            UPDATE fusillade.requests
            SET state = 'completed', response_status = 200,
                response_body = '{"choices":[{"message":{"content":"ok"}}]}',
                completed_at = NOW()
            WHERE batch_id = $1
            "#,
        )
        .bind(batch_uuid)
        .execute(&pool)
        .await
        .expect("Failed to complete requests");

        let auth = add_auth_headers(&user);

        let response = app
            .get(&format!("/ai/v1/files/{}/content", output_file_id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::OK);
        response.assert_header("content-type", "application/x-ndjson");
        response.assert_header("X-Incomplete", "false");
        assert!(
            response.headers().get("content-length").is_none(),
            "Unlimited download should be streamed without content-length"
        );

        let body = response.text();
        let lines: Vec<&str> = body.trim().lines().collect();
        assert_eq!(lines.len(), num_requests, "Should return all {} results", num_requests);

        for line in &lines {
            let item: serde_json::Value = serde_json::from_str(line).expect("Each line should be valid JSON");
            assert!(item.get("custom_id").is_some(), "Each result should have custom_id");
        }

        let page_size = 10;
        let response = app
            .get(&format!("/ai/v1/files/{}/content?limit={}", output_file_id, page_size))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::OK);
        response.assert_header("X-Incomplete", "true");
        response.assert_header("X-Last-Line", &page_size.to_string());

        let body = response.text();
        let lines: Vec<&str> = body.trim().lines().collect();
        assert_eq!(lines.len(), page_size, "Should return exactly {} results", page_size);

        let response = app
            .get(&format!(
                "/ai/v1/files/{}/content?limit={}&skip={}",
                output_file_id,
                page_size,
                num_requests - page_size
            ))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;

        response.assert_status(axum::http::StatusCode::OK);
        response.assert_header("X-Incomplete", "false");
        response.assert_header("X-Last-Line", &num_requests.to_string());
    }

    #[test]
    fn test_file_upload_error_into_http_error_stream_interrupted() {
        let err = super::FileUploadError::StreamInterrupted {
            message: "Multipart parsing failed: connection reset".to_string(),
        };
        let http_err = err.into_http_error();
        assert_eq!(http_err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        match http_err {
            crate::errors::Error::BadRequest { message } => {
                assert!(message.contains("connection reset"));
            }
            other => panic!("Expected BadRequest error, got {other:?}"),
        }
    }

    #[test]
    fn test_file_upload_error_into_http_error_file_too_large() {
        let err = super::FileUploadError::FileTooLarge { max: 100_000_000 };
        let http_err = err.into_http_error();
        match http_err {
            crate::errors::Error::PayloadTooLarge { message } => {
                assert!(message.contains("100000000"));
                assert!(!message.contains("200000000"));
            }
            _ => panic!("Expected PayloadTooLarge error"),
        }
    }

    #[test]
    fn test_file_upload_error_into_http_error_too_many_requests() {
        let err = super::FileUploadError::TooManyRequests { count: 1001, max: 1000 };
        let http_err = err.into_http_error();
        match http_err {
            crate::errors::Error::BadRequest { message } => {
                assert!(message.contains("1001"));
                assert!(message.contains("1000"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_file_upload_error_into_http_error_invalid_json() {
        let err = super::FileUploadError::InvalidJson {
            line: 42,
            error: "expected comma".to_string(),
        };
        let http_err = err.into_http_error();
        match http_err {
            crate::errors::Error::BadRequest { message } => {
                assert!(message.contains("line 42"));
                assert!(message.contains("expected comma"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_file_upload_error_into_http_error_invalid_utf8() {
        let err = super::FileUploadError::InvalidUtf8 {
            line: 5,
            byte_offset: 128,
            error: "invalid byte sequence".to_string(),
        };
        let http_err = err.into_http_error();
        match http_err {
            crate::errors::Error::BadRequest { message } => {
                assert!(message.contains("line 5"));
                assert!(message.contains("byte offset 128"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_file_upload_error_into_http_error_no_file() {
        let err = super::FileUploadError::NoFile;
        let http_err = err.into_http_error();
        match http_err {
            crate::errors::Error::BadRequest { message } => {
                assert!(message.contains("No file field"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_file_upload_error_into_http_error_invalid_purpose() {
        let err = super::FileUploadError::InvalidPurpose {
            purpose: "batches".to_string(),
        };
        let http_err = err.into_http_error();
        match http_err {
            crate::errors::Error::BadRequest { message } => {
                assert!(message.contains("batches"));
                assert!(message.contains("Only 'batch' is supported"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_file_upload_error_into_http_error_empty_file() {
        let err = super::FileUploadError::EmptyFile;
        let http_err = err.into_http_error();
        match http_err {
            crate::errors::Error::BadRequest { message } => {
                assert!(message.contains("no valid request templates"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_file_upload_error_into_http_error_model_access_denied() {
        let error = super::FileUploadError::ModelAccessDenied {
            model: "gpt-5".to_string(),
            line: 42,
        };
        let http_error = error.into_http_error();
        match http_error {
            crate::errors::Error::ModelAccessDenied { model_name, message } => {
                assert_eq!(model_name, "gpt-5");
                assert!(message.contains("42"));
                assert!(message.contains("gpt-5"));
            }
            _ => panic!("Expected ModelAccessDenied error, got {:?}", http_error),
        }
    }

    #[test]
    fn test_file_upload_error_into_http_error_model_retired() {
        let error = super::FileUploadError::ModelRetired {
            line: 3,
            message: "The model `retired-model` was shut down.".to_string(),
        };
        match error.into_http_error() {
            crate::errors::Error::Gone { message } => {
                assert_eq!(message, "Line 3: The model `retired-model` was shut down.");
            }
            other => panic!("Expected Gone error, got {:?}", other),
        }
    }

    #[test]
    fn test_file_upload_error_into_http_error_validation_error() {
        let err = super::FileUploadError::ValidationError {
            line: 3,
            message: "custom_id too long".to_string(),
        };
        let http_err = err.into_http_error();
        match http_err {
            crate::errors::Error::BadRequest { message } => {
                assert!(message.contains("Line 3"));
                assert!(message.contains("custom_id too long"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_batch_fetch_failed_maps_to_service_unavailable_not_validation() {
        let mapped = super::map_batch_normalize_error(super::BatchNormalizeError::FetchFailed("send: connection reset".to_string()), 5);
        assert!(
            matches!(mapped, super::FileUploadError::ImageFetchFailed { .. }),
            "FetchFailed must map to ImageFetchFailed, got {mapped:?}"
        );
        match mapped.into_http_error() {
            crate::errors::Error::ServiceUnavailable { message } => {
                assert!(message.contains("Line 5"));
                assert!(message.contains("connection reset"));
            }
            _ => panic!("Expected ServiceUnavailable error"),
        }
    }

    #[test]
    fn test_batch_unfetchable_maps_to_bad_request_not_server_error() {
        let mapped = super::map_batch_normalize_error(super::BatchNormalizeError::Unfetchable("origin 403 Forbidden".to_string()), 5);
        assert!(
            matches!(mapped, super::FileUploadError::ImageUnfetchable { .. }),
            "Unfetchable must map to ImageUnfetchable, got {mapped:?}"
        );
        match mapped.into_http_error() {
            crate::errors::Error::BadRequest { message } => {
                assert!(message.contains("Line 5"));
                assert!(message.contains("403 Forbidden"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[test]
    fn test_resolve_upload_stream_result_aborted_maps_error_slot_to_http_error() {
        let error_slot = Arc::new(Mutex::new(Some(super::FileUploadError::InvalidJson {
            line: 7,
            error: "expected value".to_string(),
        })));

        let err = super::resolve_upload_stream_result(fusillade::FileStreamResult::Aborted, &error_slot)
            .expect_err("aborted stream should map to an HTTP error");

        match err {
            crate::errors::Error::BadRequest { message } => {
                assert!(message.contains("line 7"));
                assert!(message.contains("expected value"));
            }
            other => panic!("Expected BadRequest error, got {other:?}"),
        }

        let slot = error_slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(slot.is_none(), "error slot should be consumed");
    }

    #[test]
    fn test_resolve_upload_stream_result_aborted_without_error_slot_is_internal() {
        let error_slot = Arc::new(Mutex::new(None));

        let err = super::resolve_upload_stream_result(fusillade::FileStreamResult::Aborted, &error_slot)
            .expect_err("aborted stream without a control-layer error should be internal");

        match err {
            crate::errors::Error::Internal { operation } => {
                assert!(operation.contains("fusillade returned Aborted without an upload error"));
            }
            other => panic!("Expected Internal error, got {other:?}"),
        }
    }

    fn upload_rejection_lines(f: impl FnOnce()) -> Vec<String> {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, f);
        handle
            .render()
            .lines()
            .filter(|line| line.starts_with("dwctl_file_upload_rejections_total{"))
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn test_every_upload_refusal_is_counted_by_its_code() {
        use super::FileUploadError::*;
        let s = || "x".to_string();
        let refusals = [
            StreamInterrupted { message: s() },
            FileTooLarge { max: 1 },
            TooManyRequests { count: 2, max: 1 },
            InvalidJson { line: 1, error: s() },
            InvalidUtf8 {
                line: 1,
                byte_offset: 0,
                error: s(),
            },
            InvalidCompression {
                format: "gzip",
                message: s(),
            },
            UnsupportedCompression { format: "xz" },
            NoFile,
            EmptyFile,
            ModelAccessDenied { model: s(), line: 1 },
            ModelRetired { line: 1, message: s() },
            ValidationError { line: 1, message: s() },
            UnprocessableValidationError { line: 1, message: s() },
            ImageTransient { line: 1, message: s() },
            ImageStoreFailed { line: 1, message: s() },
            ImageAccessUnavailable { line: 1 },
            ImageUnfetchable { line: 1, message: s() },
            ImageFetchFailed { line: 1, message: s() },
            InvalidPurpose { purpose: s() },
        ];
        for refusal in refusals {
            let code = refusal.code();
            assert!(super::UPLOAD_REJECTION_CODES.contains(&code), "{code} is registered at startup");
            let lines = upload_rejection_lines(|| {
                refusal.into_refusal();
            });
            assert_eq!(
                lines,
                [format!(r#"dwctl_file_upload_rejections_total{{code="{code}"}} 1"#)],
                "{code}"
            );
        }
    }

    #[test]
    fn test_upload_capacity_refusals_are_counted() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let config = crate::config::FileLimitsConfig {
                        max_concurrent_uploads: 1,
                        max_upload_wait_secs: 0,
                        ..Default::default()
                    };
                    let limiter = crate::limits::UploadLimiter::new(&config).unwrap();
                    assert!(super::acquire_upload_slot(None).await.unwrap().is_none(), "unlimited");
                    let _held = super::acquire_upload_slot(Some(&limiter)).await.unwrap();
                    let Err(refused) = super::acquire_upload_slot(Some(&limiter)).await else {
                        panic!("the only slot is held");
                    };
                    assert_eq!(refused.status_code(), axum::http::StatusCode::TOO_MANY_REQUESTS);
                });
        });
        let rendered = handle.render();
        assert!(
            rendered
                .lines()
                .any(|line| line == r#"dwctl_file_upload_rejections_total{code="upload_capacity"} 1"#),
            "{rendered}"
        );
    }

    #[test]
    fn test_upload_rejection_codes_start_at_zero() {
        let lines = upload_rejection_lines(super::initialize_upload_rejection_metrics);
        assert_eq!(lines.len(), super::UPLOAD_REJECTION_CODES.len(), "{lines:?}");
        for code in super::UPLOAD_REJECTION_CODES {
            let line = format!(r#"dwctl_file_upload_rejections_total{{code="{code}"}} 0"#);
            assert!(lines.contains(&line), "{line} in {lines:?}");
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_content_length_early_rejection(pool: PgPool) {
        let mut config = create_test_config();
        config.limits.files.max_file_size = 1000;

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;

        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;

        let large_content = "x".repeat(15 * 1024);
        let file_part = axum_test::multipart::Part::bytes(large_content.into_bytes()).file_name("test.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_part("file", file_part)
                    .add_text("purpose", "batch"),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::PAYLOAD_TOO_LARGE);
        let body = upload_response.text();
        assert!(body.contains("exceeds the maximum allowed size"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_streaming_size_limit_returns_413(pool: PgPool) {
        let mut config = create_test_config();
        config.limits.files.max_file_size = 5000;

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;

        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let mut lines = Vec::new();
        for i in 0..50 {
            lines.push(format!(
                r#"{{"custom_id":"req-{}","method":"POST","url":"/v1/chat/completions","body":{{"model":"gpt-4","messages":[{{"role":"user","content":"Hello world number {}"}}]}}}}"#,
                i, i
            ));
        }
        let large_content = lines.join("\n");
        let file_part = axum_test::multipart::Part::bytes(large_content.into_bytes()).file_name("test.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_part("file", file_part)
                    .add_text("purpose", "batch"),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_invalid_utf8(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;

        let mut content = b"{\"custom_id\":\"req-1\",\"method\":\"POST\",\"url\":\"/v1/chat/completions\",\"body\":{\"model\":\"gpt-4\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello ".to_vec();
        content.extend_from_slice(&[0xFF, 0xFE]);
        content.extend_from_slice(b"\"}]}}");

        let file_part = axum_test::multipart::Part::bytes(content).file_name("test.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let body = upload_response.text();
        assert!(
            body.contains("UTF-8") || body.contains("utf-8") || body.contains("encoding"),
            "Expected error about UTF-8, got: {}",
            body
        );
    }

    #[test]
    fn test_multer_error_variants_exist() {
        let _stream_size = multer::Error::StreamSizeExceeded { limit: 0 };
        let _field_size = multer::Error::FieldSizeExceeded {
            limit: 0,
            field_name: None,
        };
        let _stream_read = multer::Error::StreamReadFailed(Box::new(std::io::Error::other("test")));
    }

    #[test]
    fn test_axum_length_limit_error_exists() {
        use super::LengthLimitError;
        fn assert_error<T: std::error::Error + 'static>() {}
        assert_error::<LengthLimitError>();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_rejects_request_exceeding_max_body_size(pool: PgPool) {
        let mut config = create_test_config();
        config.limits.requests.max_body_size = 100;

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let large_content = "x".repeat(200);
        let jsonl_content = format!(
            r#"{{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{{"model":"gpt-4","messages":[{{"role":"user","content":"{}"}}]}}}}"#,
            large_content
        );

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.into_bytes()).file_name("test.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let body = upload_response.text();
        assert!(
            body.contains("exceeds the maximum allowed size"),
            "Expected error about request body size, got: {}",
            body
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_allows_request_within_max_body_size(pool: PgPool) {
        let mut config = create_test_config();
        config.limits.requests.max_body_size = 10 * 1024;

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_rejects_embeddings_model_on_chat_endpoint(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment_with_model_type(
            &pool,
            user.id,
            "text-embedding-3-small",
            "my-embed",
            crate::db::models::deployments::ModelType::Embeddings,
        )
        .await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"my-embed","messages":[{"role":"user","content":"Hello"}]}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let body = upload_response.text();
        assert!(body.contains("EMBEDDINGS"), "Expected EMBEDDINGS in error, got: {}", body);
        assert!(body.contains("/v1/chat/completions"), "Expected endpoint in error, got: {}", body);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_rejects_chat_model_on_embeddings_endpoint(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment_with_model_type(
            &pool,
            user.id,
            "gpt-4-model",
            "my-chat",
            crate::db::models::deployments::ModelType::Chat,
        )
        .await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content =
            r#"{"custom_id":"req-1","method":"POST","url":"/v1/embeddings","body":{"model":"my-chat","input":"Hello world"}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let body = upload_response.text();
        assert!(body.contains("CHAT"), "Expected CHAT in error, got: {}", body);
        assert!(body.contains("/v1/embeddings"), "Expected endpoint in error, got: {}", body);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_allows_matching_endpoint_and_model_type(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let chat_deployment = create_test_deployment_with_model_type(
            &pool,
            user.id,
            "gpt-4-model",
            "my-chat",
            crate::db::models::deployments::ModelType::Chat,
        )
        .await;
        add_deployment_to_group(&pool, chat_deployment.id, group.id, user.id).await;

        let embed_deployment = create_test_deployment_with_model_type(
            &pool,
            user.id,
            "text-embedding-3-small",
            "my-embed",
            crate::db::models::deployments::ModelType::Embeddings,
        )
        .await;
        add_deployment_to_group(&pool, embed_deployment.id, group.id, user.id).await;

        let jsonl_content = concat!(
            r#"{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"my-chat","messages":[{"role":"user","content":"Hello"}]}}"#,
            "\n",
            r#"{"custom_id":"req-2","method":"POST","url":"/v1/embeddings","body":{"model":"my-embed","input":"Hello world"}}"#,
            "\n",
        );

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_upload_skips_validation_for_untyped_models(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "mystery-model", "mystery").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content =
            r#"{"custom_id":"req-1","method":"POST","url":"/v1/embeddings","body":{"model":"mystery","input":"Hello world"}}"#;

        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");

        let upload_response = app
            .post("/ai/v1/files")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", file_part),
            )
            .await;

        upload_response.assert_status(axum::http::StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_files_member_id_rejected_outside_org_context(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let auth = add_auth_headers(&user);

        let resp = app
            .get(&format!("/ai/v1/files?member_id={}", Uuid::new_v4()))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let body = resp.text();
        assert!(
            body.contains("organization context"),
            "Expected error about org context, got: {}",
            body
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_files_member_id_no_key_returns_empty(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let org = create_test_org(&pool, user.id).await;
        let auth = add_auth_headers(&user);
        let org_cookie = format!("dw_active_org={}", org.id);

        let resp = app
            .get(&format!("/ai/v1/files?member_id={}", Uuid::new_v4()))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        resp.assert_status_ok();
        let body: serde_json::Value = resp.json();
        assert_eq!(body["data"].as_array().unwrap().len(), 0);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_files_enrichment_in_personal_context(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;
        let auth = add_auth_headers(&user);

        let jsonl_content = r#"{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("personal-test.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        upload_resp.assert_status(axum::http::StatusCode::CREATED);

        let list_resp = app
            .get("/ai/v1/files")
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        list_resp.assert_status_ok();
        let body: serde_json::Value = list_resp.json();
        let files = body["data"].as_array().unwrap();
        assert!(!files.is_empty(), "Expected at least one personal file");
        for file in files {
            assert!(
                file.get("context_name").is_some() && !file["context_name"].is_null(),
                "context_name should be present even in personal context"
            );
            assert_eq!(
                file["context_type"].as_str(),
                Some("personal"),
                "personal file should have context_type=personal"
            );
        }
    }
}
