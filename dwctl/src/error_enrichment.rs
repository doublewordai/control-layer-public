
use std::{collections::HashMap, fmt};

use crate::{
    auth::internal_credential::presented_key_hash,
    crypto::ApiKeyHash,
    db::errors::DbError,
    db::handlers::{Credits, api_keys::ApiKeys},
    errors::Error,
    inference::rejections::{Recorder, RejectionCode},
    inference::serving_name::ServingName,
    telemetry::span_account_name,
    types::UserId,
};
use axum::{
    body::Body,
    extract::State,
    http::{Request, Response, StatusCode},
    middleware::Next,
    response::IntoResponse,
};
use onwards::{ErrorOrigin, RequestIdentity};
use rust_decimal::Decimal;
use serde::Deserialize;
use sqlx::{PgConnection, PgPool};
use tracing::{debug, instrument};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct ChatRequest {
    model: String,
}

#[instrument(name = "dwctl.error_enrichment", skip_all, fields(http.request.method = %request.method(), url.path = %request.uri().path(), url.query = request.uri().query().unwrap_or("")))]
pub async fn error_enrichment_middleware(
    State(pools): State<sqlx_pool_router::DynPools>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let bearer = request
        .headers()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|auth| auth.strip_prefix("Bearer ").or_else(|| auth.strip_prefix("bearer ")))
        .map(str::trim);
    let api_key = presented_key_hash(request.extensions(), bearer);

    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => {
            let reconstructed = Request::from_parts(parts, Body::empty());
            return next.run(reconstructed).await;
        }
    };

    let model_name = serde_json::from_slice::<ChatRequest>(&bytes).ok().map(|req| req.model);
    let lookup_model = model_name.as_deref().map(|model| ServingName::of(&parts.extensions, model).model);
    let span_model = model_name
        .as_deref()
        .and_then(|model| parts.extensions.get::<Recorder>()?.configured_alias(model))
        .map(str::to_string);

    let reconstructed = Request::from_parts(parts, Body::from(bytes));

    let response = next.run(reconstructed).await;

    if response.status() == StatusCode::FORBIDDEN
        && response.extensions().get::<RejectionCode>().is_none()
        && let Some(key) = api_key
    {
        debug!("Intercepted 403 response on AI proxy path, attempting enrichment");

        let key_info = get_api_key_user_and_purpose(pools.write().into_inner(), &key).await.ok().flatten();

        let identity = key_info.as_ref().map(|owner| owner.identity(span_model.clone()));
        let tag = |mut response: Response<Body>, code: &str| {
            if let Some(identity) = identity.clone() {
                response.extensions_mut().insert(identity);
            }
            response.extensions_mut().insert(ErrorOrigin::Gateway { code: code.to_string() });
            response
        };


        if let Some(ApiKeyOwner { purpose, .. }) = key_info.as_ref()
            && !crate::db::models::api_keys::is_inference_purpose(purpose)
        {
            let body = serde_json::json!({
                "error": {
                    "message": format!("API keys with purpose '{purpose}' cannot be used for inference requests."),
                    "type": "invalid_request_error"
                }
            });
            return tag(
                Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap_or_else(|_| StatusCode::FORBIDDEN.into_response()),
                "non_inference_key",
            );
        }

        if let Some(model) = &model_name
            && let Some(lookup) = &lookup_model
            && let Some(ApiKeyOwner { user_id, .. }) = key_info.as_ref()
            && let Ok(has_access) = check_user_has_model_access(pools.write().into_inner(), *user_id, lookup).await
            && !has_access
        {
            if let Ok(Some(message)) = retirement_message(pools.write().into_inner(), lookup).await {
                return tag(retired_model_response(&message), "model_retired");
            }
            return tag(
                Error::ModelAccessDenied {
                    model_name: model.clone(),
                    message: format!(
                        "You do not have access to '{}'. Please contact your administrator to request access.",
                        model
                    ),
                }
                .into_response(),
                "model_access_denied",
            );
        }

        if let Some(model) = &model_name
            && let Some(lookup) = &lookup_model
            && let Ok(Some(purpose)) = check_modality_blocked(pools.write().into_inner(), &key, lookup).await
        {
            return tag(
                Error::ModalityAccessDenied {
                    model_name: model.clone(),
                    purpose: purpose.clone(),
                    message: modality_blocked_message(&purpose, model),
                }
                .into_response(),
                "modality_blocked",
            );
        }

        if let Ok(Some(balance)) = get_balance_of_api_key(pools.write().into_inner(), &key).await
            && balance <= Decimal::ZERO
        {
            return tag(
                Error::InsufficientCredits {
                    current_balance: balance,
                    message: "Account balance too low. Please add credits to continue.".to_string(),
                }
                .into_response(),
                "insufficient_credits",
            );
        }

        if let Ok(Some(cap)) = get_spend_cap_state(pools.write().into_inner(), &key).await
            && cap.window_spend >= cap.limit
        {
            if cap.window_current {
                let resets = match cap.resets_at {
                    Some(at) => format!("; resets {}", at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
                    None => String::new(),
                };
                let body = serde_json::json!({
                    "error": {
                        "message": format!(
                            "API key has reached its spending cap of ${} (spent ${} this period{resets}). Raise or remove the cap to resume.",
                            cap.limit.round_dp(2),
                            cap.window_spend.round_dp(2),
                        ),
                        "type": "insufficient_quota",
                        "code": "spend_cap_exceeded",
                        "param": null
                    }
                });
                return tag(
                    Response::builder()
                        .status(StatusCode::PAYMENT_REQUIRED)
                        .header("content-type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap_or_else(|_| StatusCode::PAYMENT_REQUIRED.into_response()),
                    "spend_cap_exceeded",
                );
            }

            maybe_fire_boundary_resync(&pools.write()).await;

            let body = serde_json::json!({
                "error": {
                    "message": "Spending cap window has reset; the key is being reinstated. Retry shortly.",
                    "type": "rate_limit_error",
                    "code": "spend_cap_reset_pending",
                    "param": null
                }
            });
            return tag(
                Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .header("content-type", "application/json")
                    .header("retry-after", "5")
                    .body(Body::from(body.to_string()))
                    .unwrap_or_else(|_| StatusCode::TOO_MANY_REQUESTS.into_response()),
                "spend_cap_reset_pending",
            );
        }
    }

    response
}

static LAST_BOUNDARY_RESYNC_EPOCH_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
const BOUNDARY_RESYNC_MIN_INTERVAL_SECS: u64 = 60;

#[cfg(test)]
pub(crate) fn reset_boundary_resync_throttle_for_tests() {
    LAST_BOUNDARY_RESYNC_EPOCH_SECS.store(0, std::sync::atomic::Ordering::Relaxed);
}

async fn maybe_fire_boundary_resync(pool: &PgPool) {
    use std::sync::atomic::Ordering;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let last = LAST_BOUNDARY_RESYNC_EPOCH_SECS.load(Ordering::Relaxed);
    if now.saturating_sub(last) < BOUNDARY_RESYNC_MIN_INTERVAL_SECS {
        return;
    }
    if LAST_BOUNDARY_RESYNC_EPOCH_SECS
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }

    let epoch_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    let payload = format!("api_key_spend_cap_boundary:{epoch_micros}");
    if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)")
        .bind(crate::config::ONWARDS_CONFIG_CHANGED_CHANNEL)
        .bind(&payload)
        .execute(pool)
        .await
    {
        debug!(error = %e, "boundary resync notify failed (best-effort; fallback sync will readmit)");
    }
}

struct SpendCapState {
    limit: Decimal,
    window_spend: Decimal,
    window_current: bool,
    resets_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[instrument(skip_all, name = "dwctl.get_spend_cap_state")]
async fn get_spend_cap_state(pool: PgPool, api_key: &ApiKeyHash) -> Result<Option<SpendCapState>, DbError> {
    let row = sqlx::query!(
        r#"
        SELECT root.spend_limit AS "spend_limit!",
               COALESCE(ck.window_spend, 0) AS "window_spend!",
               api_key_cap_window_current(ck.window_started_at, root.spend_limit_interval) AS "window_current!",
               api_key_cap_window_resets_at(root.spend_limit_interval) AS resets_at
        FROM api_keys ak
        JOIN api_keys root ON root.id = COALESCE(ak.parent_api_key_id, ak.id)
        LEFT JOIN api_key_spend_checkpoints ck ON ck.api_key_id = root.id
        WHERE ak.secret_hash = $1 AND ak.is_deleted = false AND root.spend_limit IS NOT NULL
        "#,
        &api_key[..]
    )
    .fetch_optional(&pool)
    .await?;

    Ok(row.map(|r| SpendCapState {
        limit: r.spend_limit,
        window_spend: r.window_spend,
        window_current: r.window_current,
        resets_at: r.resets_at,
    }))
}

fn modality_label(purpose: &str) -> String {
    match purpose {
        "batch" => "Batch".to_string(),
        "realtime" => "Real-time".to_string(),
        "playground" => "Playground".to_string(),
        "platform" => "Platform".to_string(),
        other => {
            let mut chars = other.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().chain(chars).collect(),
                None => "This".to_string(),
            }
        }
    }
}

const FLEX_DISPATCH_PURPOSE: &str = "batch";

#[instrument(skip(pool, api_key), name = "dwctl.check_modality_blocked")]
pub async fn check_modality_blocked(pool: PgPool, api_key: &ApiKeyHash, model_alias: &str) -> Result<Option<String>, DbError> {
    let mut conn = pool.acquire().await?;

    let purpose = sqlx::query_scalar!(
        r#"
        SELECT mtr.api_key_purpose
        FROM model_traffic_rules mtr
        JOIN deployed_models dm ON dm.id = mtr.deployed_model_id
        JOIN api_keys ak ON ak.purpose = mtr.api_key_purpose
        WHERE dm.alias = $1
          AND dm.deleted = false
          AND ak.secret_hash = $2
          AND ak.is_deleted = false
          AND mtr.action = 'deny'
        LIMIT 1
        "#,
        model_alias,
        &api_key[..],
    )
    .fetch_optional(&mut *conn)
    .await?;

    Ok(purpose)
}

#[instrument(skip(pool), name = "dwctl.check_modality_blocked_for_purpose")]
pub async fn check_modality_blocked_for_purpose(pool: PgPool, model_alias: &str, purpose: &str) -> Result<Option<String>, DbError> {
    let mut conn = pool.acquire().await?;

    let found = sqlx::query_scalar!(
        r#"
        SELECT mtr.api_key_purpose
        FROM model_traffic_rules mtr
        JOIN deployed_models dm ON dm.id = mtr.deployed_model_id
        WHERE dm.alias = $1
          AND dm.deleted = false
          AND mtr.api_key_purpose = $2
          AND mtr.action = 'deny'
        LIMIT 1
        "#,
        model_alias,
        purpose,
    )
    .fetch_optional(&mut *conn)
    .await?;

    Ok(found)
}

#[instrument(skip_all, name = "dwctl.get_user_id_of_api_key")]
pub async fn get_user_id_of_api_key(pool: PgPool, api_key: &ApiKeyHash) -> Result<UserId, DbError> {
    let mut conn = pool.acquire().await?;
    let mut api_keys_repo = ApiKeys::new(&mut conn);
    api_keys_repo
        .get_user_id_by_hash(api_key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("API key not found or associated user doesn't exist").into())
}

#[instrument(skip_all, name = "dwctl.get_api_key_user_and_purpose")]
async fn get_api_key_user_and_purpose(pool: PgPool, api_key: &ApiKeyHash) -> Result<Option<ApiKeyOwner>, DbError> {
    let mut conn = pool.acquire().await?;
    let row = sqlx::query!(
        r#"
        SELECT k.id, k.user_id, k.purpose, u.display_name, u.username
        FROM api_keys k
        JOIN users u ON u.id = k.user_id
        WHERE k.secret_hash = $1 AND k.is_deleted = false
        "#,
        &api_key[..]
    )
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|r| ApiKeyOwner {
        api_key_id: r.id,
        user_id: r.user_id,
        purpose: r.purpose,
        account_name: span_account_name(r.display_name.as_deref(), &r.username),
    }))
}

struct ApiKeyOwner {
    api_key_id: Uuid,
    user_id: UserId,
    purpose: String,
    account_name: String,
}

impl ApiKeyOwner {
    fn identity(&self, model: Option<String>) -> RequestIdentity {
        RequestIdentity {
            account_id: Some(self.user_id.to_string()),
            account_name: Some(self.account_name.clone()),
            api_key_id: Some(self.api_key_id),
            key_purpose: Some(self.purpose.clone()),
            model,
        }
    }
}

#[instrument(skip_all)]
async fn get_balance_of_api_key(pool: PgPool, api_key: &ApiKeyHash) -> Result<Option<Decimal>, DbError> {
    let user_id = get_user_id_of_api_key(pool.clone(), api_key).await?;
    let mut conn = pool.acquire().await?;
    Credits::new(&mut conn).get_balance_for_admission(user_id).await
}

#[instrument(skip_all, name = "dwctl.check_user_has_model_access")]
pub async fn check_user_has_model_access(pool: PgPool, user_id: UserId, model_alias: &str) -> Result<bool, DbError> {
    let mut conn = pool.acquire().await?;

    let result = sqlx::query_scalar!(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM deployed_models d
            JOIN deployment_groups dg ON dg.deployment_id = d.id
            WHERE d.alias = $1
              AND d.deleted = false
              AND dg.group_id IN (
                  SELECT ug.group_id FROM user_groups ug WHERE ug.user_id = $2
                  UNION
                  SELECT '00000000-0000-0000-0000-000000000000'::uuid
                  WHERE $2 != '00000000-0000-0000-0000-000000000000'
              )
        ) as "has_access!"
        "#,
        model_alias,
        user_id
    )
    .fetch_one(&mut *conn)
    .await?;

    Ok(result)
}

pub async fn validate_api_key_model_access(pool: PgPool, api_key: &ApiKeyHash, model: &str) -> Result<(), ModelAccessRefusal> {
    let ApiKeyOwner { user_id, purpose, .. } = get_api_key_user_and_purpose(pool.clone(), api_key)
        .await
        .map_err(|_| ModelAccessRefusal::Denied("Invalid API key".to_string()))?
        .ok_or_else(|| ModelAccessRefusal::Denied("Invalid API key".to_string()))?;

    if user_id != uuid::Uuid::nil() && !crate::db::models::api_keys::is_inference_purpose(&purpose) {
        return Err(ModelAccessRefusal::Denied(format!(
            "API keys with purpose '{purpose}' cannot be used for inference requests."
        )));
    }

    let has_access = check_user_has_model_access(pool.clone(), user_id, model)
        .await
        .map_err(|e| ModelAccessRefusal::Denied(format!("Failed to check model access: {e}")))?;

    if !has_access {
        if let Ok(Some(message)) = retirement_message(pool.clone(), model).await {
            return Err(ModelAccessRefusal::Retired(message));
        }
        return Err(ModelAccessRefusal::Denied(format!(
            "You do not have access to '{}'. Please contact your administrator to request access.",
            model
        )));
    }

    if let Some(purpose) = check_modality_blocked_for_purpose(pool, model, FLEX_DISPATCH_PURPOSE)
        .await
        .map_err(|e| ModelAccessRefusal::Denied(format!("Failed to check modality routing rules: {e}")))?
    {
        return Err(ModelAccessRefusal::Denied(modality_blocked_message(&purpose, model)));
    }

    Ok(())
}

fn modality_blocked_message(purpose: &str, model: &str) -> String {
    format!(
        "{} access to '{}' is blocked by a routing rule. Please contact your administrator to request access.",
        modality_label(purpose),
        model
    )
}

#[derive(Debug)]
pub enum ModelAccessRefusal {
    Retired(String),
    Denied(String),
}

impl fmt::Display for ModelAccessRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ModelAccessRefusal::Retired(message) | ModelAccessRefusal::Denied(message) => f.write_str(message),
        }
    }
}

pub fn retired_model_response(message: &str) -> Response<Body> {
    let body = serde_json::json!({
        "error": {
            "message": message,
            "type": "invalid_request_error",
            "param": null,
            "code": "model_not_found"
        }
    });
    Response::builder()
        .status(StatusCode::GONE)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap_or_else(|_| StatusCode::GONE.into_response())
}

pub async fn retirement_messages(conn: &mut PgConnection) -> Result<HashMap<String, String>, DbError> {
    let messages = sqlx::query_as::<_, (String, String)>(
        "SELECT alias, metadata->>'retirement_message' FROM deployed_models WHERE NOT deleted AND metadata->>'retirement_message' IS NOT NULL",
    )
    .fetch_all(conn)
    .await?;
    Ok(messages.into_iter().collect())
}

#[instrument(skip_all, name = "dwctl.retirement_message")]
async fn retirement_message(pool: PgPool, model_alias: &str) -> Result<Option<String>, DbError> {
    let mut conn = pool.acquire().await?;
    let message = sqlx::query_scalar::<_, Option<String>>(
        "SELECT metadata->>'retirement_message' FROM deployed_models WHERE alias = $1 AND deleted = false LIMIT 1",
    )
    .bind(model_alias)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(message.flatten())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::hash_api_key;
    use crate::db::{
        handlers::{Credits, Repository as _, api_keys::ApiKeys},
        models::{
            api_keys::{ApiKeyCreateDBRequest, ApiKeyPurpose},
            credits::{CreditTransactionCreateDBRequest, CreditTransactionType},
        },
    };
    use crate::{api::models::users::Role, test::utils::create_test_user};
    use rust_decimal::Decimal;

    #[test]
    fn test_modality_label_known_and_unknown_purposes() {
        assert_eq!(modality_label("batch"), "Batch");
        assert_eq!(modality_label("realtime"), "Real-time");
        assert_eq!(modality_label("playground"), "Playground");
        assert_eq!(modality_label("platform"), "Platform");

        assert_eq!(modality_label("evaluation"), "Evaluation");

        assert_eq!(modality_label(""), "This");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_error_enrichment_middleware_reports_retired_model(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let mut conn = pool.acquire().await.unwrap();
        let api_key = ApiKeys::new(&mut conn)
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "Test Key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Realtime,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();
        drop(conn);

        let endpoint_id = crate::test::utils::create_test_endpoint(&pool, "test-endpoint", user.id).await;
        let deployment_id = crate::test::utils::create_test_model(&pool, "retired-model-name", "retired-model", endpoint_id, user.id).await;
        sqlx::query("UPDATE deployed_models SET metadata = $1 WHERE id = $2")
            .bind(serde_json::json!({"retirement_message": "The model `retired-model` was shut down. Use `current-model` instead."}))
            .bind(deployment_id)
            .execute(&pool)
            .await
            .unwrap();

        let router = axum::Router::new()
            .route(
                "/ai/v1/chat/completions",
                axum::routing::post(|| async { StatusCode::FORBIDDEN.into_response() }),
            )
            .layer(axum::middleware::from_fn_with_state(
                sqlx_pool_router::DynPools::new(pool.clone()),
                error_enrichment_middleware,
            ));
        let server = axum_test::TestServer::new(router).expect("Failed to create test server");

        let retired = server
            .post("/ai/v1/chat/completions")
            .add_header("authorization", &format!("Bearer {}", api_key.secret))
            .json(&serde_json::json!({"model": "retired-model", "messages": []}))
            .await;
        retired.assert_status(StatusCode::GONE);
        let body: serde_json::Value = retired.json();
        assert_eq!(body["error"]["code"], "model_not_found");
        assert_eq!(
            body["error"]["message"],
            "The model `retired-model` was shut down. Use `current-model` instead."
        );

        let unknown = server
            .post("/ai/v1/chat/completions")
            .add_header("authorization", &format!("Bearer {}", api_key.secret))
            .json(&serde_json::json!({"model": "test-model", "messages": []}))
            .await;
        unknown.assert_status(StatusCode::FORBIDDEN);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_error_enrichment_middleware_enriches_403_with_balance(pool: PgPool) {
        use crate::test::utils::{add_deployment_to_group, add_user_to_group, create_test_group};

        let user = create_test_user(&pool, Role::StandardUser).await;

        let mut api_key_conn = pool.acquire().await.unwrap();
        let mut api_keys_repo = ApiKeys::new(&mut api_key_conn);
        let api_key = api_keys_repo
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "Test Key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Realtime,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();

        let mut credits_conn = pool.acquire().await.unwrap();
        let mut credits_repo = Credits::new(&mut credits_conn);
        credits_repo
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::AdminGrant,
                amount: Decimal::new(5000, 2),
                source_id: uuid::Uuid::new_v4().to_string(),
                description: Some("Initial credits".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();

        let router = axum::Router::new()
            .route(
                "/ai/v1/chat/completions",
                axum::routing::post(|| async {
                    axum::response::Response::builder()
                        .status(StatusCode::FORBIDDEN)
                        .body(axum::body::Body::from("Forbidden"))
                        .unwrap()
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                sqlx_pool_router::DynPools::new(pool.clone()),
                crate::error_enrichment::error_enrichment_middleware,
            ));

        let server = axum_test::TestServer::new(router).expect("Failed to create test server");

        let response = server
            .post("/ai/v1/chat/completions")
            .add_header("authorization", &format!("Bearer {}", api_key.secret))
            .json(&serde_json::json!({
                "model": "test-model",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code().as_u16(), 403);
        let body = response.text();
        assert!(body.contains("do not have access to 'test-model'"));

        let mut credits_conn = pool.acquire().await.unwrap();
        let mut credits_repo = Credits::new(&mut credits_conn);
        credits_repo
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::Usage,
                amount: Decimal::new(10000, 2),
                source_id: uuid::Uuid::new_v4().to_string(),
                description: Some("Usage".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();

        let endpoint_id = crate::test::utils::create_test_endpoint(&pool, "test-endpoint", user.id).await;
        let deployment_id =
            crate::test::utils::create_test_model(&pool, "authorized-model-name", "authorized-model", endpoint_id, user.id).await;

        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        add_deployment_to_group(&pool, deployment_id, group.id, user.id).await;

        let response = server
            .post("/ai/v1/chat/completions")
            .add_header("authorization", &format!("Bearer {}", api_key.secret))
            .json(&serde_json::json!({
                "model": "authorized-model",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code().as_u16(), 402);
        let body = response.text();
        println!("Enriched response body: {}", body);
        assert!(body.contains("balance too low"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_error_enrichment_spend_cap_arms(pool: PgPool) {
        use crate::db::handlers::api_keys::ApiKeys as ApiKeysRepo;
        use crate::test::utils::{add_deployment_to_group, add_user_to_group, create_test_group};

        let user = create_test_user(&pool, Role::StandardUser).await;
        let endpoint_id = crate::test::utils::create_test_endpoint(&pool, "cap-endpoint", user.id).await;
        let deployment_id = crate::test::utils::create_test_model(&pool, "cap-model-name", "cap-model", endpoint_id, user.id).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        add_deployment_to_group(&pool, deployment_id, group.id, user.id).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut api_keys_repo = ApiKeysRepo::new(&mut conn);
        let api_key = api_keys_repo
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "Capped Key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Realtime,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();
        drop(conn);

        let mut credits_conn = pool.acquire().await.unwrap();
        let mut credits_repo = Credits::new(&mut credits_conn);
        credits_repo
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::AdminGrant,
                amount: Decimal::new(5000, 2),
                source_id: uuid::Uuid::new_v4().to_string(),
                description: Some("Initial credits".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();
        drop(credits_conn);

        sqlx::query("UPDATE api_keys SET spend_limit = 10 WHERE id = $1")
            .bind(api_key.id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO api_key_spend_checkpoints (api_key_id, total_spend, window_spend) VALUES ($1, 10.5, 10.5)")
            .bind(api_key.id)
            .execute(&pool)
            .await
            .unwrap();

        let router = axum::Router::new()
            .route(
                "/ai/v1/chat/completions",
                axum::routing::post(|| async {
                    axum::response::Response::builder()
                        .status(StatusCode::FORBIDDEN)
                        .body(axum::body::Body::from("Forbidden"))
                        .unwrap()
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                sqlx_pool_router::DynPools::new(pool.clone()),
                crate::error_enrichment::error_enrichment_middleware,
            ));
        let server = axum_test::TestServer::new(router).expect("Failed to create test server");
        let request = || {
            server
                .post("/ai/v1/chat/completions")
                .add_header("authorization", &format!("Bearer {}", api_key.secret))
                .json(&serde_json::json!({
                    "model": "cap-model",
                    "messages": [{"role": "user", "content": "Hello"}]
                }))
        };

        let response = request().await;
        assert_eq!(response.status_code().as_u16(), 402);
        let body = response.text();
        assert!(body.contains("spend_cap_exceeded"), "expected cap code, got: {body}");
        assert!(body.contains("insufficient_quota"), "expected OpenAI quota type, got: {body}");
        assert!(body.contains("spending cap of $10"), "expected cap amount, got: {body}");

        let mut credits_conn = pool.acquire().await.unwrap();
        let mut credits_repo = Credits::new(&mut credits_conn);
        credits_repo
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::Usage,
                amount: Decimal::new(10000, 2),
                source_id: uuid::Uuid::new_v4().to_string(),
                description: Some("Usage".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();
        drop(credits_conn);
        let response = request().await;
        assert_eq!(response.status_code().as_u16(), 402);
        let body = response.text();
        assert!(body.contains("balance too low"), "balance must supersede the cap, got: {body}");
        assert!(!body.contains("spend_cap_exceeded"), "cap error must not surface, got: {body}");

        sqlx::query("INSERT INTO user_feature_flags (user_id, feature_flag, enabled) VALUES ($1, 'ALLOW_NEGATIVE_BALANCE', true) ON CONFLICT (user_id, feature_flag) DO UPDATE SET enabled = EXCLUDED.enabled")
            .bind(user.id)
            .execute(&pool)
            .await
            .unwrap();
        let response = request().await;
        response.assert_status(StatusCode::PAYMENT_REQUIRED);
        assert!(response.text().contains("spend_cap_exceeded"));
        assert!(!response.text().contains("balance too low"));
        sqlx::query("INSERT INTO user_feature_flags (user_id, feature_flag, enabled) VALUES ($1, 'ALLOW_NEGATIVE_BALANCE', false) ON CONFLICT (user_id, feature_flag) DO UPDATE SET enabled = EXCLUDED.enabled")
            .bind(user.id)
            .execute(&pool)
            .await
            .unwrap();

        let mut credits_conn = pool.acquire().await.unwrap();
        let mut credits_repo = Credits::new(&mut credits_conn);
        credits_repo
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::AdminGrant,
                amount: Decimal::new(20000, 2),
                source_id: uuid::Uuid::new_v4().to_string(),
                description: Some("Top-up".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();
        drop(credits_conn);
        sqlx::query("UPDATE api_keys SET spend_limit_interval = 'daily' WHERE id = $1")
            .bind(api_key.id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE api_key_spend_checkpoints SET window_started_at = now() - interval '2 days' WHERE api_key_id = $1")
            .bind(api_key.id)
            .execute(&pool)
            .await
            .unwrap();

        crate::error_enrichment::reset_boundary_resync_throttle_for_tests();
        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await.unwrap();
        listener.listen(crate::config::ONWARDS_CONFIG_CHANGED_CHANNEL).await.unwrap();
        while tokio::time::timeout(std::time::Duration::from_millis(10), listener.try_recv())
            .await
            .is_ok()
        {}

        let response = request().await;
        assert_eq!(
            response.status_code().as_u16(),
            429,
            "rolled window returns the retriable reset-pending response"
        );
        assert_eq!(
            response.headers().get("retry-after").and_then(|v| v.to_str().ok()),
            Some("5"),
            "reset-pending must carry the short notify-driven Retry-After"
        );
        let body = response.text();
        assert!(body.contains("spend_cap_reset_pending"), "expected reset-pending code, got: {body}");
        assert!(
            !body.contains("spend_cap_exceeded"),
            "must not claim an exceeded cap after the window rolled, got: {body}"
        );

        let notification = tokio::time::timeout(std::time::Duration::from_secs(2), listener.recv())
            .await
            .expect("timeout waiting for boundary resync notification")
            .expect("failed to receive notification");
        assert!(
            notification.payload().starts_with("api_key_spend_cap_boundary:"),
            "expected boundary resync payload, got: {}",
            notification.payload()
        );

        let response = request().await;
        assert_eq!(response.status_code().as_u16(), 429);
        while let Ok(Ok(n)) = tokio::time::timeout(std::time::Duration::from_millis(500), listener.try_recv()).await {
            if let Some(n) = n {
                assert!(
                    !n.payload().starts_with("api_key_spend_cap_boundary:"),
                    "second request within the throttle window must not re-notify"
                );
            } else {
                break;
            }
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_error_enrichment_middleware_passes_through_legitimate_403(pool: PgPool) {
        use crate::test::utils::{add_deployment_to_group, add_user_to_group, create_test_group};

        let user = create_test_user(&pool, Role::StandardUser).await;

        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let mut api_key_conn = pool.acquire().await.unwrap();
        let mut api_keys_repo = ApiKeys::new(&mut api_key_conn);
        let api_key = api_keys_repo
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "Test Key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Realtime,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();

        let mut credits_conn = pool.acquire().await.unwrap();
        let mut credits_repo = Credits::new(&mut credits_conn);
        credits_repo
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::AdminGrant,
                amount: Decimal::new(5000, 2),
                source_id: uuid::Uuid::new_v4().to_string(),
                description: Some("Initial credits".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();

        let endpoint_id = crate::test::utils::create_test_endpoint(&pool, "test-endpoint", user.id).await;
        let deployment_id =
            crate::test::utils::create_test_model(&pool, "authorized-model-name", "authorized-model", endpoint_id, user.id).await;

        add_deployment_to_group(&pool, deployment_id, group.id, user.id).await;

        let router = axum::Router::new()
            .route(
                "/ai/v1/chat/completions",
                axum::routing::post(|| async {
                    axum::response::Response::builder()
                        .status(StatusCode::FORBIDDEN)
                        .body(axum::body::Body::from("Rate limit exceeded"))
                        .unwrap()
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                sqlx_pool_router::DynPools::new(pool.clone()),
                crate::error_enrichment::error_enrichment_middleware,
            ));

        let server = axum_test::TestServer::new(router).expect("Failed to create test server");

        let response = server
            .post("/ai/v1/chat/completions")
            .add_header("authorization", &format!("Bearer {}", api_key.secret))
            .json(&serde_json::json!({
                "model": "authorized-model",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code().as_u16(), 403);
        let body = response.text();
        assert!(body.contains("Rate limit exceeded"));
        assert!(!body.contains("balance"));
        assert!(!body.contains("access"));
    }

    #[dwctl_test_macros::test]
    async fn test_error_enrichment_middleware_keeps_dwctl_layer_refusals(pool: PgPool) {
        use crate::inference::rejections::{self, RejectionCode};
        use tower::ServiceExt;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let mut conn = pool.acquire().await.unwrap();
        let api_key = ApiKeys::new(&mut conn)
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "Test Key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Realtime,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();

        let router = axum::Router::new()
            .route(
                "/ai/v1/chat/completions",
                axum::routing::post(|request: Request<Body>| async move {
                    let response = (StatusCode::FORBIDDEN, "image token forbidden").into_response();
                    rejections::refuse(&request, response, "image_token_forbidden", None, None)
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                sqlx_pool_router::DynPools::new(pool.clone()),
                error_enrichment_middleware,
            ));
        let request = Request::post("/ai/v1/chat/completions")
            .header("authorization", format!("Bearer {}", api_key.secret))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"model": "some-model", "messages": []}"#))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let code = response.extensions().get::<RejectionCode>().map(RejectionCode::code);
        assert_eq!(code, Some("image_token_forbidden"));
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"image token forbidden");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_error_enrichment_identity_labels_only_configured_models(pool: PgPool) {
        use crate::inference::rejections::test_support::recorder;
        use tower::ServiceExt;

        let user = create_test_user(&pool, Role::StandardUser).await;
        sqlx::query("UPDATE users SET display_name = '   ' WHERE id = $1")
            .bind(user.id)
            .execute(&pool)
            .await
            .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let api_key = ApiKeys::new(&mut conn)
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "Platform Key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Platform,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();

        let router = axum::Router::new()
            .route(
                "/ai/v1/chat/completions",
                axum::routing::post(|| async { StatusCode::FORBIDDEN.into_response() }),
            )
            .layer(axum::middleware::from_fn_with_state(
                sqlx_pool_router::DynPools::new(pool.clone()),
                error_enrichment_middleware,
            ))
            .layer(axum::Extension(recorder("gpt-4o")));
        let identity = |model: &'static str| {
            let router = router.clone();
            let secret = api_key.secret.to_string();
            async move {
                let request = Request::post("/ai/v1/chat/completions")
                    .header("authorization", format!("Bearer {secret}"))
                    .header("content-type", "application/json")
                    .body(Body::from(format!(r#"{{"model": "{model}", "messages": []}}"#)))
                    .unwrap();
                let response = router.oneshot(request).await.unwrap();
                assert_eq!(response.status(), StatusCode::FORBIDDEN);
                response.extensions().get::<RequestIdentity>().cloned().unwrap()
            }
        };

        let known = identity("gpt-4o").await;
        assert_eq!(known.model.as_deref(), Some("gpt-4o"));
        assert_eq!(known.account_name.as_deref(), Some(user.username.as_str()));
        assert_eq!(known.key_purpose.as_deref(), Some("platform"));

        assert_eq!(identity("anything-a-client-sends").await.model, None);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_error_enrichment_middleware_ignores_non_ai_paths(pool: PgPool) {
        let router = axum::Router::new()
            .route(
                "/admin/api/v1/users",
                axum::routing::get(|| async {
                    axum::response::Response::builder()
                        .status(StatusCode::FORBIDDEN)
                        .body(axum::body::Body::from("Admin Forbidden"))
                        .unwrap()
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                sqlx_pool_router::DynPools::new(pool.clone()),
                crate::error_enrichment::error_enrichment_middleware,
            ));

        let server = axum_test::TestServer::new(router).expect("Failed to create test server");
        let response = server.get("/admin/api/v1/users").await;

        assert_eq!(response.status_code().as_u16(), 403);
        assert_eq!(response.text(), "Admin Forbidden");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_error_enrichment_middleware_ignores_non_403_errors(pool: PgPool) {
        let router = axum::Router::new()
            .route(
                "/ai/v1/chat/completions",
                axum::routing::post(|| async {
                    axum::response::Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .header("authorization", "Bearer dummy-key")
                        .body(axum::body::Body::from("Not Found"))
                        .unwrap()
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                sqlx_pool_router::DynPools::new(pool.clone()),
                crate::error_enrichment::error_enrichment_middleware,
            ));

        let server = axum_test::TestServer::new(router).expect("Failed to create test server");
        let response = server
            .post("/ai/v1/chat/completions")
            .add_header("authorization", "Bearer test-key")
            .json(&serde_json::json!({"model": "test"}))
            .await;

        assert_eq!(response.status_code().as_u16(), 404);
        assert_eq!(response.text(), "Not Found");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_error_enrichment_middleware_enriches_403_with_modality_block(pool: PgPool) {
        use crate::test::utils::{add_deployment_to_group, add_user_to_group, create_test_group};

        let user = create_test_user(&pool, Role::StandardUser).await;

        let mut api_key_conn = pool.acquire().await.unwrap();
        let mut api_keys_repo = ApiKeys::new(&mut api_key_conn);
        let api_key = api_keys_repo
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "Batch Key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Batch,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();

        let mut credits_conn = pool.acquire().await.unwrap();
        let mut credits_repo = Credits::new(&mut credits_conn);
        credits_repo
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::AdminGrant,
                amount: Decimal::new(5000, 2),
                source_id: uuid::Uuid::new_v4().to_string(),
                description: Some("Initial credits".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();

        let endpoint_id = crate::test::utils::create_test_endpoint(&pool, "test-endpoint", user.id).await;
        let deployment_id = crate::test::utils::create_test_model(&pool, "blocked-model-name", "blocked-model", endpoint_id, user.id).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        add_deployment_to_group(&pool, deployment_id, group.id, user.id).await;

        sqlx::query!(
            "INSERT INTO model_traffic_rules (deployed_model_id, api_key_purpose, action) VALUES ($1, 'batch', 'deny')",
            deployment_id
        )
        .execute(&pool)
        .await
        .unwrap();

        let router = axum::Router::new()
            .route(
                "/ai/v1/chat/completions",
                axum::routing::post(|| async {
                    axum::response::Response::builder()
                        .status(StatusCode::FORBIDDEN)
                        .body(axum::body::Body::from("Forbidden"))
                        .unwrap()
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                sqlx_pool_router::DynPools::new(pool.clone()),
                crate::error_enrichment::error_enrichment_middleware,
            ));

        let server = axum_test::TestServer::new(router).expect("Failed to create test server");

        let response = server
            .post("/ai/v1/chat/completions")
            .add_header("authorization", &format!("Bearer {}", api_key.secret))
            .json(&serde_json::json!({
                "model": "blocked-model",
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .await;

        assert_eq!(response.status_code().as_u16(), 403);
        let body = response.text();
        assert!(
            body.contains("Batch") && body.contains("blocked-model") && body.contains("administrator"),
            "expected modality-blocked message, got: {body}"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_validate_api_key_model_access_rejects_modality_blocked(pool: PgPool) {
        use crate::test::utils::{add_deployment_to_group, add_user_to_group, create_test_group};

        let user = create_test_user(&pool, Role::StandardUser).await;

        let mut api_key_conn = pool.acquire().await.unwrap();
        let mut api_keys_repo = ApiKeys::new(&mut api_key_conn);
        let api_key = api_keys_repo
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "Batch Key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Batch,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();

        let endpoint_id = crate::test::utils::create_test_endpoint(&pool, "test-endpoint", user.id).await;
        let deployment_id = crate::test::utils::create_test_model(&pool, "blocked-model-name", "blocked-model", endpoint_id, user.id).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        add_deployment_to_group(&pool, deployment_id, group.id, user.id).await;

        sqlx::query!(
            "INSERT INTO model_traffic_rules (deployed_model_id, api_key_purpose, action) VALUES ($1, 'batch', 'deny')",
            deployment_id
        )
        .execute(&pool)
        .await
        .unwrap();

        let result = validate_api_key_model_access(pool.clone(), &hash_api_key(&api_key.secret), "blocked-model").await;
        let err = result.expect_err("expected modality-blocked rejection");
        assert!(
            err.to_string().contains("Batch") && err.to_string().contains("blocked-model") && err.to_string().contains("administrator"),
            "expected modality-blocked message, got: {err}"
        );

        let other_deployment_id = crate::test::utils::create_test_model(&pool, "open-model-name", "open-model", endpoint_id, user.id).await;
        add_deployment_to_group(&pool, other_deployment_id, group.id, user.id).await;
        validate_api_key_model_access(pool.clone(), &hash_api_key(&api_key.secret), "open-model")
            .await
            .expect("expected access to be granted on a model without a deny rule");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_validate_flex_allowed_despite_realtime_deny_rule(pool: PgPool) {
        use crate::test::utils::{add_deployment_to_group, add_user_to_group, create_test_group};

        let user = create_test_user(&pool, Role::StandardUser).await;

        let mut api_key_conn = pool.acquire().await.unwrap();
        let mut api_keys_repo = ApiKeys::new(&mut api_key_conn);
        let api_key = api_keys_repo
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "Realtime Key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Realtime,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();
        drop(api_key_conn);

        let endpoint_id = crate::test::utils::create_test_endpoint(&pool, "test-endpoint", user.id).await;
        let deployment_id = crate::test::utils::create_test_model(&pool, "async-only-name", "async-only", endpoint_id, user.id).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        add_deployment_to_group(&pool, deployment_id, group.id, user.id).await;

        sqlx::query!(
            "INSERT INTO model_traffic_rules (deployed_model_id, api_key_purpose, action) VALUES ($1, 'realtime', 'deny')",
            deployment_id
        )
        .execute(&pool)
        .await
        .unwrap();

        validate_api_key_model_access(pool.clone(), &hash_api_key(&api_key.secret), "async-only")
            .await
            .expect("Flex request must be allowed despite a realtime-only deny rule");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_validate_flex_blocked_by_batch_deny_rule_with_realtime_key(pool: PgPool) {
        use crate::test::utils::{add_deployment_to_group, add_user_to_group, create_test_group};

        let user = create_test_user(&pool, Role::StandardUser).await;

        let mut api_key_conn = pool.acquire().await.unwrap();
        let mut api_keys_repo = ApiKeys::new(&mut api_key_conn);
        let api_key = api_keys_repo
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "Realtime Key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Realtime,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();
        drop(api_key_conn);

        let endpoint_id = crate::test::utils::create_test_endpoint(&pool, "test-endpoint", user.id).await;
        let deployment_id = crate::test::utils::create_test_model(&pool, "no-batch-name", "no-batch", endpoint_id, user.id).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        add_deployment_to_group(&pool, deployment_id, group.id, user.id).await;

        sqlx::query!(
            "INSERT INTO model_traffic_rules (deployed_model_id, api_key_purpose, action) VALUES ($1, 'batch', 'deny')",
            deployment_id
        )
        .execute(&pool)
        .await
        .unwrap();

        let err = validate_api_key_model_access(pool.clone(), &hash_api_key(&api_key.secret), "no-batch")
            .await
            .expect_err("Flex request must be blocked by a batch deny rule (Flex runs as batch)");
        assert!(
            err.to_string().contains("Batch") && err.to_string().contains("no-batch") && err.to_string().contains("administrator"),
            "expected batch modality-blocked message, got: {err}"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_validate_api_key_model_access_rejects_platform_purpose(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;

        let mut api_key_conn = pool.acquire().await.unwrap();
        let mut api_keys_repo = ApiKeys::new(&mut api_key_conn);
        let api_key = api_keys_repo
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "Platform Key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Platform,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();

        let result = validate_api_key_model_access(pool.clone(), &hash_api_key(&api_key.secret), "any-model").await;
        let err = result.expect_err("expected platform-purpose rejection");
        assert!(
            err.to_string().contains("platform") && err.to_string().contains("inference"),
            "expected non-inference purpose message, got: {err}"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_validate_api_key_model_access_exempts_system_key_from_purpose_gate(pool: PgPool) {
        let mut api_key_conn = pool.acquire().await.unwrap();
        let mut api_keys_repo = ApiKeys::new(&mut api_key_conn);
        let api_key = api_keys_repo
            .create(&ApiKeyCreateDBRequest {
                user_id: uuid::Uuid::nil(),
                name: "System Key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Platform,
                created_by: uuid::Uuid::nil(),
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();

        if let Err(msg) = validate_api_key_model_access(pool.clone(), &hash_api_key(&api_key.secret), "any-model").await {
            assert!(
                !msg.to_string().contains("cannot be used for inference requests"),
                "system key must be exempt from the purpose gate, got: {msg}"
            );
        }
    }

    #[dwctl_test_macros::test]
    async fn test_validate_api_key_model_access_reports_retired_model(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let mut conn = pool.acquire().await.unwrap();
        let api_key = ApiKeys::new(&mut conn)
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "Test Key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Realtime,
                created_by: user.id,
                spend_limit: None,
                spend_limit_interval: None,
            })
            .await
            .unwrap();
        drop(conn);

        let endpoint_id = crate::test::utils::create_test_endpoint(&pool, "test-endpoint", user.id).await;
        let deployment_id = crate::test::utils::create_test_model(&pool, "retired-model-name", "retired-model", endpoint_id, user.id).await;
        sqlx::query("UPDATE deployed_models SET metadata = $1 WHERE id = $2")
            .bind(serde_json::json!({"retirement_message": "The model `retired-model` was shut down."}))
            .bind(deployment_id)
            .execute(&pool)
            .await
            .unwrap();

        let refusal = validate_api_key_model_access(pool.clone(), &hash_api_key(&api_key.secret), "retired-model")
            .await
            .expect_err("a retired model must be refused");
        assert!(matches!(refusal, ModelAccessRefusal::Retired(message) if message == "The model `retired-model` was shut down."));

        let mut conn = pool.acquire().await.unwrap();
        assert_eq!(
            retirement_messages(&mut conn)
                .await
                .unwrap()
                .get("retired-model")
                .map(String::as_str),
            Some("The model `retired-model` was shut down.")
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_error_enrichment_middleware_without_auth_header(pool: PgPool) {
        let router = axum::Router::new()
            .route(
                "/ai/v1/chat/completions",
                axum::routing::post(|| async {
                    axum::response::Response::builder()
                        .status(StatusCode::FORBIDDEN)
                        .body(axum::body::Body::from("No Auth"))
                        .unwrap()
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                sqlx_pool_router::DynPools::new(pool.clone()),
                crate::error_enrichment::error_enrichment_middleware,
            ));

        let server = axum_test::TestServer::new(router).expect("Failed to create test server");
        let response = server
            .post("/ai/v1/chat/completions")
            .json(&serde_json::json!({"model": "test"}))
            .await;

        assert_eq!(response.status_code().as_u16(), 403);
        assert_eq!(response.text(), "No Auth");
    }
}
