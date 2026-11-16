
use crate::crypto::hash_api_key;
use crate::inference::response_store::StoreError;
use axum::{
    Json,
    extract::{Path, State},
    http::HeaderMap,
};
use fusillade::Storage;
use serde::{Deserialize, Serialize};
use sqlx_pool_router::PoolProvider;
use utoipa::ToSchema;

use crate::AppState;
use crate::errors::{Error, Result};
use crate::inference::store::ResponseLookup;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "id": "resp_abc123",
    "object": "response",
    "deleted": true
}))]
pub struct ResponseDeleted {
    #[schema(example = "resp_abc123")]
    pub id: String,
    #[serde(rename = "object")]
    #[schema(example = "response")]
    pub object_type: ResponseDeletedObjectType,
    #[schema(example = true)]
    pub deleted: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ResponseDeletedObjectType {
    Response,
}

#[tracing::instrument(skip_all)]
pub async fn get_response<P: PoolProvider>(
    State(state): State<AppState<P>>,
    headers: HeaderMap,
    Path(response_id): Path<String>,
) -> Result<Json<serde_json::Value>> {
    let api_key = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or_else(|| Error::Unauthenticated { message: None })?;

    let (api_key_id, owner_id): (uuid::Uuid, String) =
        sqlx::query_as("SELECT id, user_id::text FROM public.api_keys WHERE secret_hash = $1 AND is_deleted = false LIMIT 1")
            .bind(&hash_api_key(api_key)[..])
            .fetch_optional(state.db.write())
            .await
            .map_err(|e| Error::Database(e.into()))?
            .ok_or_else(|| Error::Unauthenticated { message: None })?;
    state.api_key_usage.record(api_key_id);

    let uuid_str = response_id.strip_prefix("resp_").unwrap_or(&response_id);
    let response_uuid = uuid::Uuid::parse_str(uuid_str).map_err(|_| Error::NotFound {
        resource: "response".to_string(),
        id: response_id.clone(),
    })?;

    let auth_request_id = fusillade::RequestId(response_uuid);

    let detail = state
        .request_manager
        .get_request_detail(auth_request_id)
        .await
        .map_err(|e| match e {
            fusillade::FusilladeError::RequestNotFound(_) => Error::NotFound {
                resource: "response".to_string(),
                id: response_id.clone(),
            },
            _ => Error::Database(crate::db::errors::DbError::Other(anyhow::anyhow!("{e}"))),
        })?;

    let owner = detail.created_by.as_str();
    if owner != owner_id {
        return Err(Error::NotFound {
            resource: "response".to_string(),
            id: response_id,
        });
    }

    match state.response_store.get_response(&response_id).await.map_err(|e| match e {
        StoreError::NotFound(_) => Error::NotFound {
            resource: "response".to_string(),
            id: response_id.clone(),
        },
        _ => Error::Database(crate::db::errors::DbError::Other(anyhow::anyhow!("{e}"))),
    })? {
        ResponseLookup::Found(resp) => Ok(Json(resp)),
        ResponseLookup::NotFound => Err(Error::NotFound {
            resource: "response".to_string(),
            id: response_id,
        }),
        ResponseLookup::Gone => Err(Error::Gone {
            message: "zdr_request_unavailable: this zero-data-retention response is no longer available".to_string(),
        }),
    }
}

#[utoipa::path(
    delete,
    path = "/responses/{response_id}",
    tag = "responses-api",
    summary = "Delete response",
    description = "Hard-deletes a response and every fusillade row that backs it (\
including the dedicated request_templates row carrying the prompt body). \
Provided for right-to-erasure compliance.

Token / cost analytics in `http_analytics` and billing transactions in \
`credits_transactions` are preserved — they are denormalized off the fusillade \
schema and survive the erasure.",
    params(
        ("response_id" = String, Path, description = "The response ID returned when the response was created (with or without the `resp_` prefix).")
    ),
    responses(
        (status = 200, description = "Response deleted.", body = ResponseDeleted),
        (status = 401, description = "Invalid or missing API key. Ensure your `Authorization` header is set to `Bearer YOUR_API_KEY`."),
        (status = 404, description = "Response not found or you don't have access to it."),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    ),
    security(("BearerAuth" = []))
)]
#[tracing::instrument(skip_all)]
pub async fn delete_response<P: PoolProvider>(
    State(state): State<AppState<P>>,
    headers: HeaderMap,
    Path(response_id): Path<String>,
) -> Result<Json<ResponseDeleted>> {
    let api_key = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or_else(|| Error::Unauthenticated { message: None })?;

    let (api_key_id, owner_id): (uuid::Uuid, String) =
        sqlx::query_as("SELECT id, user_id::text FROM public.api_keys WHERE secret_hash = $1 AND is_deleted = false LIMIT 1")
            .bind(&hash_api_key(api_key)[..])
            .fetch_optional(state.db.write())
            .await
            .map_err(|e| Error::Database(e.into()))?
            .ok_or_else(|| Error::Unauthenticated { message: None })?;
    state.api_key_usage.record(api_key_id);

    let uuid_str = response_id.strip_prefix("resp_").unwrap_or(&response_id);
    let response_uuid = uuid::Uuid::parse_str(uuid_str).map_err(|_| Error::NotFound {
        resource: "response".to_string(),
        id: response_id.clone(),
    })?;

    state
        .request_manager
        .delete_owned_response_group(response_uuid, &owner_id)
        .await
        .map_err(|error| match error {
            fusillade::FusilladeError::RequestNotFound(_) => Error::NotFound {
                resource: "response".to_string(),
                id: response_id.clone(),
            },
            _ => erasure_error(&error),
        })?;

    Ok(Json(ResponseDeleted {
        id: response_id,
        object_type: ResponseDeletedObjectType::Response,
        deleted: true,
    }))
}

fn erasure_error(error: &fusillade::FusilladeError) -> Error {
    use fusillade::RetainedResponseMaintenanceError as Maintenance;
    match Maintenance::from_fusillade_error(error) {
        Some(Maintenance::RetirementPending) => Error::Conflict {
            message: "Response erasure is temporarily unavailable while retention maintenance runs; retry shortly".to_string(),
            conflicts: None,
        },
        Some(class) => Error::Database(crate::db::errors::DbError::Other(anyhow::anyhow!(
            "Response erasure failed ({class:?})"
        ))),
        None => Error::Database(crate::db::errors::DbError::Other(anyhow::anyhow!(
            "Response erasure failed (storage)"
        ))),
    }
}
