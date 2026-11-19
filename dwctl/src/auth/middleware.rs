
use crate::{
    api::models::users::CurrentUser,
    db::{handlers::api_keys::ApiKeys, models::api_keys::ApiKeyPurpose},
    errors::Error,
};
use anyhow::Context;
use axum::{
    body::Body,
    extract::{FromRequestParts, Request, State},
    http::{HeaderValue, Method, Uri},
    middleware::Next,
    response::Response,
};
use tracing::{debug, trace};

const PLAYGROUND_CREDENTIAL_TTL: std::time::Duration = std::time::Duration::from_secs(60);

pub(crate) async fn admin_ai_proxy<P: sqlx_pool_router::PoolProvider + Clone>(
    state: crate::AppState<P>,
    mut request: Request,
) -> Result<Request, Error> {
    let uri = request.uri().clone();
    let path = uri.path();

    if !path.starts_with("/admin/api/v1/ai/") || request.method() == Method::OPTIONS {
        return Ok(request);
    }
    debug!("Intercepted admin AI proxy request: {}", path);

    let (mut parts, body) = request.into_parts();
    let current_user = CurrentUser::from_request_parts(&mut parts, &state).await?;

    request = Request::from_parts(parts, body);

    let max_bytes = state.limiters.inference_body_size;
    let body = std::mem::take(request.body_mut());
    let body_bytes = crate::inference::body_limit::read_inference_body(request.headers(), body, max_bytes).await?;

    let model_name = onwards::extract_model_from_request(request.headers(), &body_bytes).ok_or(Error::BadRequest {
        message: "Could not extract model from request".to_string(),
    })?;

    debug!("Model name extracted from request: {}", model_name);

    let target_user_id = current_user.active_organization.unwrap_or(current_user.id);
    let mut api_key_conn = state.db.write().acquire().await.unwrap();
    let mut api_keys_repo = ApiKeys::new(&mut api_key_conn);
    let playground_key_id = api_keys_repo
        .get_or_create_hidden_key(target_user_id, ApiKeyPurpose::Playground, current_user.id)
        .await
        .with_context(|| {
            format!(
                "Failed to get or create hidden playground API key for target user {} (current user {})",
                target_user_id, current_user.id
            )
        })?;

    debug!("User has access to model: {}", model_name);
    let new_path = path.replace("/admin/api/v1/ai", "/ai");

    let query_string = uri.query().map(|q| format!("?{q}")).unwrap_or_default();
    let mut parts = uri.into_parts();
    parts.path_and_query = Some(
        format!("{new_path}{query_string}")
            .parse()
            .with_context(|| format!("Failed to parse rewritten path: {new_path}{query_string}"))?,
    );

    let new_uri = Uri::from_parts(parts).with_context(|| "Failed to construct URI from parts")?;

    *request.uri_mut() = new_uri;

    let credential = state.internal_credentials.mint(playground_key_id, PLAYGROUND_CREDENTIAL_TTL);
    let headers = request.headers_mut();
    headers.insert(
        "authorization",
        HeaderValue::from_str(&format!("Bearer {credential}")).with_context(|| "Failed to create authorization header value")?,
    );

    *request.body_mut() = Body::from(body_bytes);

    trace!("Rewritten request URI: {}", request.uri());
    Ok(request)
}

pub async fn admin_ai_proxy_middleware<P: sqlx_pool_router::PoolProvider + Clone>(
    State(state): State<crate::AppState<P>>,
    request: Request,
    next: Next,
) -> Result<Response, Error> {
    let path = request.uri().path().to_owned();
    let request = match admin_ai_proxy(state, request).await {
        Ok(request) => request,
        Err(Error::PayloadTooLarge { message }) => return Ok(crate::inference::body_limit::oversized_body(message, &path)),
        Err(error) => return Err(error),
    };
    Ok(next.run(request).await)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    use crate::{
        api::models::{
            groups::GroupCreate,
            users::{CurrentUser, Role},
        },
        auth::{middleware::admin_ai_proxy, session},
        db::{
            handlers::{Deployments, Groups, InferenceEndpoints, Repository as _},
            models::{
                deployments::DeploymentCreateDBRequest, groups::GroupCreateDBRequest, inference_endpoints::InferenceEndpointCreateDBRequest,
            },
        },
        test::utils::{create_test_config, create_test_user},
    };

    #[dwctl_test_macros::test]
    async fn test_playground_body_limit_precedes_model_parsing(pool: PgPool) {
        let mut config = create_test_config();
        config.limits.requests.max_body_size = 4;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config.clone()).await;
        config.limits.requests.max_body_size = 100;
        state.config.store(config);
        for length in [None, Some("5")] {
            let mut request = axum::http::Request::post("/admin/api/v1/ai/v1/chat/completions")
                .header("x-doubleword-user", user.external_user_id.as_ref().unwrap_or(&user.username))
                .header("x-doubleword-email", &user.email);
            if let Some(length) = length {
                request = request.header("content-length", length);
            }
            let error = admin_ai_proxy(state.clone(), request.body("xxxxx".into()).unwrap())
                .await
                .unwrap_err();
            assert!(matches!(error, crate::errors::Error::PayloadTooLarge { .. }));
        }

        let router = axum::Router::new()
            .fallback(|| async { "unexpected success" })
            .layer(axum::middleware::from_fn_with_state(state, super::admin_ai_proxy_middleware));
        let server = axum_test::TestServer::new(router).unwrap();
        for endpoint in ["chat/completions", "messages"] {
            let response = server
                .post(&format!("/admin/api/v1/ai/v1/{endpoint}"))
                .add_header("x-doubleword-user", user.external_user_id.as_ref().unwrap_or(&user.username))
                .add_header("x-doubleword-email", &user.email)
                .bytes("xxxxx".into())
                .await;
            response.assert_status(axum::http::StatusCode::PAYLOAD_TOO_LARGE);
            let body: serde_json::Value = response.json();
            assert_eq!(body["error"]["type"], "request_too_large");
            if endpoint == "messages" {
                assert_eq!(body["type"], "error");
            } else {
                assert_eq!(body["error"]["code"], "request_too_large");
            }
        }
    }

    #[dwctl_test_macros::test]
    async fn test_user_no_access_auth_error(pool: PgPool) {
        let config = create_test_config();
        let mut inference_conn = pool.acquire().await.unwrap();
        let user = create_test_user(&pool, Role::StandardUser).await;
        let mut endpoints = InferenceEndpoints::new(&mut inference_conn);
        let endpoint = endpoints
            .create(&InferenceEndpointCreateDBRequest {
                name: "Test Endpoint".to_string(),
                description: Some("Test endpoint".to_string()),
                url: "http://localhost:8000".parse().unwrap(),
                api_key: None,
                model_filter: None,
                auth_header_name: None,
                auth_header_prefix: None,
                reasoning_translation: None,
                accepts_scheduling_priority: false,
                created_by: user.id,
                kind: Default::default(),
            })
            .await
            .expect("Failed to create test inference endpoint");

        let mut deployment_conn = pool.acquire().await.unwrap();
        let mut deployments = Deployments::new(&mut deployment_conn);
        let model = deployments
            .create(
                &DeploymentCreateDBRequest::builder()
                    .created_by(user.id)
                    .model_name("test_model".to_string())
                    .alias("gpt-4".to_string())
                    .maybe_description(Some("Test deployment".to_string()))
                    .hosted_on(endpoint.id)
                    .build(),
            )
            .await
            .expect("Failed to create test deployment");

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;
        let external_user_id = user.external_user_id.as_ref().unwrap_or(&user.username);
        let request = axum::http::Request::builder()
            .uri("/admin/api/v1/ai/v1/chat/completions")
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", &user.email)
            .body(
                json!({
                    "model": model.alias
                })
                .to_string()
                .into(),
            )
            .unwrap();
        let request = admin_ai_proxy(state, request).await.unwrap();
        assert_eq!(request.uri().path(), "/ai/v1/chat/completions");
        assert!(request.headers().get("authorization").is_some());
    }

    #[dwctl_test_macros::test]
    async fn test_user_access_no_auth_error(pool: PgPool) {
        let config = create_test_config();
        let user = create_test_user(&pool, Role::StandardUser).await;
        let mut inference_conn = pool.acquire().await.unwrap();
        let mut endpoints = InferenceEndpoints::new(&mut inference_conn);
        let endpoint = endpoints
            .create(&InferenceEndpointCreateDBRequest {
                name: "Test Endpoint".to_string(),
                description: Some("Test endpoint".to_string()),
                url: "http://localhost:8000".parse().unwrap(),
                api_key: None,
                model_filter: None,
                auth_header_name: None,
                auth_header_prefix: None,
                reasoning_translation: None,
                accepts_scheduling_priority: false,
                created_by: user.id,
                kind: Default::default(),
            })
            .await
            .expect("Failed to create test inference endpoint");

        let mut deployment_conn = pool.acquire().await.unwrap();
        let mut deployments = Deployments::new(&mut deployment_conn);
        let model = deployments
            .create(
                &DeploymentCreateDBRequest::builder()
                    .created_by(user.id)
                    .model_name("test_model".to_string())
                    .alias("gpt-4".to_string())
                    .maybe_description(Some("Test deployment".to_string()))
                    .hosted_on(endpoint.id)
                    .build(),
            )
            .await
            .expect("Failed to create test deployment");

        let mut group_con = pool.acquire().await.unwrap();
        let mut groups = Groups::new(&mut group_con);
        let group = groups
            .create(&GroupCreateDBRequest::new(
                Uuid::nil(),
                GroupCreate {
                    name: "a group".to_string(),
                    description: Some("A test group".to_string()),
                },
            ))
            .await
            .expect("Failed to create test group");

        groups
            .add_user_to_group(user.id, group.id)
            .await
            .expect("Failed to add user to group");
        groups
            .add_deployment_to_group(model.id, group.id, Uuid::nil())
            .await
            .expect("Failed to add deployment to group");

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;
        let external_user_id = user.external_user_id.as_ref().unwrap_or(&user.username);
        let request = axum::http::Request::builder()
            .uri("/admin/api/v1/ai/v1/chat/completions")
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", &user.email)
            .body(
                json!({
                    "model": model.alias
                })
                .to_string()
                .into(),
            )
            .unwrap();
        let request = admin_ai_proxy(state, request).await.unwrap();
        assert_eq!(request.uri().path(), "/ai/v1/chat/completions");
        assert!(request.headers().get("authorization").is_some());
    }

    #[dwctl_test_macros::test]
    async fn test_header_must_be_supplied(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;
        let request = axum::http::Request::builder()
            .uri("/admin/api/v1/ai/v1/chat/completions")
            .body(
                json!({
                    "model": "irrelevant"
                })
                .to_string()
                .into(),
            )
            .unwrap();
        let err = admin_ai_proxy(state, request).await.unwrap_err();
        assert_eq!(err.status_code().as_u16(), 401);
    }

    #[dwctl_test_macros::test]
    async fn test_preflight_passes_through(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;
        let request = axum::http::Request::builder()
            .method("OPTIONS")
            .uri("/admin/api/v1/ai/v1/chat/completions")
            .header("x-doubleword-user", "preflight@example.org")
            .header("x-doubleword-email", "preflight@example.org")
            .body(axum::body::Body::empty())
            .unwrap();
        let request = admin_ai_proxy(state, request).await.unwrap();
        assert_eq!(request.uri().path(), "/admin/api/v1/ai/v1/chat/completions");
        assert!(request.headers().get("authorization").is_none());
    }

    #[dwctl_test_macros::test]
    async fn test_unknown_user_no_access(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;
        let request = axum::http::Request::builder()
            .uri("/admin/api/v1/ai/v1/chat/completions")
            .header("x-doubleword-user", "test@example.org")
            .header("x-doubleword-email", "test@example.org")
            .body(
                json!({
                    "model": "irrelevant"
                })
                .to_string()
                .into(),
            )
            .unwrap();
        let request = admin_ai_proxy(state, request).await.unwrap();
        assert_eq!(request.uri().path(), "/ai/v1/chat/completions");
        assert!(request.headers().get("authorization").is_some());
    }

    #[dwctl_test_macros::test]
    async fn test_unknown_model_not_found(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let external_user_id = user.external_user_id.as_ref().unwrap_or(&user.username);
        let request = axum::http::Request::builder()
            .uri("/admin/api/v1/ai/v1/chat/completions")
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", &user.email)
            .body(
                json!({
                    "model": "nonexistent"
                })
                .to_string()
                .into(),
            )
            .unwrap();
        let request = admin_ai_proxy(state, request).await.unwrap();
        assert_eq!(request.uri().path(), "/ai/v1/chat/completions");
        assert!(request.headers().get("authorization").is_some());
    }

    #[dwctl_test_macros::test]
    async fn test_ignored_paths(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let request = axum::http::Request::builder()
            .uri("/nonsense/admin/api/v1/ai/v1/chat/completions")
            .header("x-doubleword-user", user.email.clone())
            .header("x-doubleword-email", user.email.clone())
            .body(
                json!({
                    "model": "nonexistent"
                })
                .to_string()
                .into(),
            )
            .unwrap();
        let err = admin_ai_proxy(state, request).await.unwrap();
        assert_eq!(err.uri().path(), "/nonsense/admin/api/v1/ai/v1/chat/completions");
    }

    #[dwctl_test_macros::test]
    async fn test_user_access_through_everyone_group(pool: PgPool) {
        let config = create_test_config();
        let user = create_test_user(&pool, Role::StandardUser).await;
        let mut inference_conn = pool.acquire().await.unwrap();
        let mut endpoints = InferenceEndpoints::new(&mut inference_conn);
        let endpoint = endpoints
            .create(&InferenceEndpointCreateDBRequest {
                name: "Test Endpoint".to_string(),
                description: Some("Test endpoint".to_string()),
                url: "http://localhost:8000".parse().unwrap(),
                api_key: None,
                model_filter: None,
                auth_header_name: None,
                auth_header_prefix: None,
                reasoning_translation: None,
                accepts_scheduling_priority: false,
                created_by: user.id,
                kind: Default::default(),
            })
            .await
            .expect("Failed to create test inference endpoint");

        let mut deployment_conn = pool.acquire().await.unwrap();
        let mut deployments = Deployments::new(&mut deployment_conn);
        let model = deployments
            .create(
                &DeploymentCreateDBRequest::builder()
                    .created_by(user.id)
                    .model_name("test_model".to_string())
                    .alias("gpt-4-everyone".to_string())
                    .maybe_description(Some("Test deployment for Everyone group".to_string()))
                    .hosted_on(endpoint.id)
                    .build(),
            )
            .await
            .expect("Failed to create test deployment");

        let mut group_conn = pool.acquire().await.unwrap();
        let mut groups = Groups::new(&mut group_conn);

        let everyone_group_id = uuid::Uuid::nil();
        groups
            .add_deployment_to_group(model.id, everyone_group_id, user.id)
            .await
            .expect("Failed to add deployment to Everyone group");

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;
        let external_user_id = user.external_user_id.as_ref().unwrap_or(&user.username);
        let request = axum::http::Request::builder()
            .uri("/admin/api/v1/ai/v1/chat/completions")
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", &user.email)
            .body(
                json!({
                    "model": model.alias
                })
                .to_string()
                .into(),
            )
            .unwrap();

        let request = admin_ai_proxy(state, request).await.unwrap();
        assert_eq!(request.uri().path(), "/ai/v1/chat/completions");
        assert!(request.headers().get("authorization").is_some());
    }

    #[dwctl_test_macros::test]
    async fn test_jwt_session_authentication(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let mut inference_conn = pool.acquire().await.unwrap();
        let mut endpoints = InferenceEndpoints::new(&mut inference_conn);
        let endpoint = endpoints
            .create(&InferenceEndpointCreateDBRequest {
                name: "Test Endpoint".to_string(),
                description: Some("Test endpoint".to_string()),
                url: "http://localhost:8000".parse().unwrap(),
                api_key: None,
                model_filter: None,
                auth_header_name: None,
                auth_header_prefix: None,
                reasoning_translation: None,
                accepts_scheduling_priority: false,
                created_by: user.id,
                kind: Default::default(),
            })
            .await
            .expect("Failed to create test inference endpoint");

        let mut deployment_conn = pool.acquire().await.unwrap();
        let mut deployments = Deployments::new(&mut deployment_conn);
        let model = deployments
            .create(
                &DeploymentCreateDBRequest::builder()
                    .created_by(user.id)
                    .model_name("test_model".to_string())
                    .alias("gpt-4-jwt".to_string())
                    .maybe_description(Some("Test deployment for JWT auth".to_string()))
                    .hosted_on(endpoint.id)
                    .build(),
            )
            .await
            .expect("Failed to create test deployment");

        let mut group_conn = pool.acquire().await.unwrap();
        let mut groups = Groups::new(&mut group_conn);
        let group = groups
            .create(&GroupCreateDBRequest::new(
                Uuid::nil(),
                GroupCreate {
                    name: "jwt group".to_string(),
                    description: Some("A test group for JWT".to_string()),
                },
            ))
            .await
            .expect("Failed to create test group");

        groups
            .add_user_to_group(user.id, group.id)
            .await
            .expect("Failed to add user to group");
        groups
            .add_deployment_to_group(model.id, group.id, Uuid::nil())
            .await
            .expect("Failed to add deployment to group");

        let current_user = CurrentUser {
            auth_method: crate::api::models::users::AuthMethod::TrustedSso,
            id: user.id,
            username: user.username,
            email: user.email,
            is_admin: user.is_admin,
            roles: user.roles,
            display_name: user.display_name,
            avatar_url: user.avatar_url,
            payment_provider_id: None,
            organizations: vec![],
            active_organization: None,
            api_key_id: None,
        };
        let jwt_token = session::create_session_token(&current_user, &config).unwrap();

        let mut state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config.clone()).await;
        state = crate::AppState {
            db: state.db,
            config: state.config,
            outlet_db: None,
            metrics_recorder: None,
            batch_sla_metrics: state.batch_sla_metrics,
            is_leader: false,
            request_manager: state.request_manager,
            task_runner: state.task_runner,
            limiters: state.limiters,
            connections_encryption_key: None,
            response_store: state.response_store,
            image_normalizer: state.image_normalizer.clone(),
            keystore: state.keystore.clone(),
            key_policy_cache: state.key_policy_cache.clone(),
            onwards_targets: None,
            internal_credentials: state.internal_credentials.clone(),
            api_key_usage: Default::default(),
            openrouter_refresh: Default::default(),
        };

        let request = axum::http::Request::builder()
            .uri("/admin/api/v1/ai/v1/chat/completions")
            .header("cookie", format!("{}={}", config.auth.native.session.cookie_name, jwt_token))
            .body(
                json!({
                    "model": model.alias
                })
                .to_string()
                .into(),
            )
            .unwrap();

        let request = admin_ai_proxy(state, request).await.unwrap();
        assert_eq!(request.uri().path(), "/ai/v1/chat/completions");
        assert!(request.headers().get("authorization").is_some());
    }

    #[dwctl_test_macros::test]
    async fn test_auth_method_priority_jwt_over_header(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        let jwt_user = create_test_user(&pool, Role::StandardUser).await;
        let header_user = create_test_user(&pool, Role::StandardUser).await;

        let mut inference_conn = pool.acquire().await.unwrap();
        let mut endpoints = InferenceEndpoints::new(&mut inference_conn);
        let endpoint = endpoints
            .create(&InferenceEndpointCreateDBRequest {
                name: "Test Endpoint".to_string(),
                description: Some("Test endpoint".to_string()),
                url: "http://localhost:8000".parse().unwrap(),
                api_key: None,
                model_filter: None,
                auth_header_name: None,
                auth_header_prefix: None,
                reasoning_translation: None,
                accepts_scheduling_priority: false,
                created_by: jwt_user.id,
                kind: Default::default(),
            })
            .await
            .expect("Failed to create test inference endpoint");

        let mut deployment_conn = pool.acquire().await.unwrap();
        let mut deployments = Deployments::new(&mut deployment_conn);
        let model = deployments
            .create(
                &DeploymentCreateDBRequest::builder()
                    .created_by(jwt_user.id)
                    .model_name("test_model".to_string())
                    .alias("gpt-4-priority".to_string())
                    .maybe_description(Some("Test deployment for auth priority".to_string()))
                    .hosted_on(endpoint.id)
                    .build(),
            )
            .await
            .expect("Failed to create test deployment");

        let mut group_conn = pool.acquire().await.unwrap();
        let mut groups = Groups::new(&mut group_conn);
        let group = groups
            .create(&GroupCreateDBRequest::new(
                Uuid::nil(),
                GroupCreate {
                    name: "priority group".to_string(),
                    description: Some("A test group for auth priority".to_string()),
                },
            ))
            .await
            .expect("Failed to create test group");

        groups
            .add_user_to_group(jwt_user.id, group.id)
            .await
            .expect("Failed to add JWT user to group");
        groups
            .add_deployment_to_group(model.id, group.id, Uuid::nil())
            .await
            .expect("Failed to add deployment to group");

        let current_user = CurrentUser {
            auth_method: crate::api::models::users::AuthMethod::TrustedSso,
            id: jwt_user.id,
            username: jwt_user.username,
            email: jwt_user.email.clone(),
            is_admin: jwt_user.is_admin,
            roles: jwt_user.roles,
            display_name: jwt_user.display_name,
            avatar_url: jwt_user.avatar_url,
            payment_provider_id: None,
            organizations: vec![],
            active_organization: None,
            api_key_id: None,
        };
        let jwt_token = session::create_session_token(&current_user, &config).unwrap();

        let mut state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config.clone()).await;
        state = crate::AppState {
            db: state.db,
            config: state.config,
            outlet_db: None,
            metrics_recorder: None,
            batch_sla_metrics: state.batch_sla_metrics,
            is_leader: false,
            request_manager: state.request_manager,
            task_runner: state.task_runner,
            limiters: state.limiters,
            connections_encryption_key: None,
            response_store: state.response_store,
            image_normalizer: state.image_normalizer.clone(),
            keystore: state.keystore.clone(),
            key_policy_cache: state.key_policy_cache.clone(),
            onwards_targets: None,
            internal_credentials: state.internal_credentials.clone(),
            api_key_usage: Default::default(),
            openrouter_refresh: Default::default(),
        };

        let header_external_user_id = header_user.external_user_id.as_ref().unwrap_or(&header_user.username);
        let request = axum::http::Request::builder()
            .uri("/admin/api/v1/ai/v1/chat/completions")
            .header("cookie", format!("{}={}", config.auth.native.session.cookie_name, jwt_token))
            .header("x-doubleword-user", header_external_user_id)
            .header("x-doubleword-email", &header_user.email)
            .body(
                json!({
                    "model": model.alias
                })
                .to_string()
                .into(),
            )
            .unwrap();

        let request = admin_ai_proxy(state, request).await.unwrap();
        assert_eq!(request.uri().path(), "/ai/v1/chat/completions");
        assert!(request.headers().get("authorization").is_some());
    }

    #[dwctl_test_macros::test]
    async fn test_disabled_auth_methods(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = false;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let mut inference_conn = pool.acquire().await.unwrap();
        let mut endpoints = InferenceEndpoints::new(&mut inference_conn);
        let endpoint = endpoints
            .create(&InferenceEndpointCreateDBRequest {
                name: "Test Endpoint".to_string(),
                description: Some("Test endpoint".to_string()),
                url: "http://localhost:8000".parse().unwrap(),
                api_key: None,
                model_filter: None,
                auth_header_name: None,
                auth_header_prefix: None,
                reasoning_translation: None,
                accepts_scheduling_priority: false,
                created_by: user.id,
                kind: Default::default(),
            })
            .await
            .expect("Failed to create test inference endpoint");

        let mut deployment_conn = pool.acquire().await.unwrap();
        let mut deployments = Deployments::new(&mut deployment_conn);
        let model = deployments
            .create(
                &DeploymentCreateDBRequest::builder()
                    .created_by(user.id)
                    .model_name("test_model".to_string())
                    .alias("gpt-4-disabled".to_string())
                    .maybe_description(Some("Test deployment for disabled auth".to_string()))
                    .hosted_on(endpoint.id)
                    .build(),
            )
            .await
            .expect("Failed to create test deployment");

        let mut group_conn = pool.acquire().await.unwrap();
        let mut groups = Groups::new(&mut group_conn);
        let group = groups
            .create(&GroupCreateDBRequest::new(
                Uuid::nil(),
                GroupCreate {
                    name: "disabled auth group".to_string(),
                    description: Some("A test group for disabled auth".to_string()),
                },
            ))
            .await
            .expect("Failed to create test group");

        groups
            .add_user_to_group(user.id, group.id)
            .await
            .expect("Failed to add user to group");
        groups
            .add_deployment_to_group(model.id, group.id, Uuid::nil())
            .await
            .expect("Failed to add deployment to group");

        let current_user = CurrentUser {
            auth_method: crate::api::models::users::AuthMethod::TrustedSso,
            id: user.id,
            username: user.username.clone(),
            email: user.email.clone(),
            is_admin: user.is_admin,
            roles: user.roles.clone(),
            display_name: user.display_name,
            avatar_url: user.avatar_url,
            payment_provider_id: None,
            organizations: vec![],
            active_organization: None,
            api_key_id: None,
        };
        let jwt_token = session::create_session_token(&current_user, &config).unwrap();

        let mut state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config.clone()).await;
        state = crate::AppState {
            db: state.db,
            config: state.config,
            outlet_db: None,
            metrics_recorder: None,
            batch_sla_metrics: state.batch_sla_metrics,
            is_leader: false,
            request_manager: state.request_manager,
            task_runner: state.task_runner,
            limiters: state.limiters,
            connections_encryption_key: None,
            response_store: state.response_store,
            image_normalizer: state.image_normalizer.clone(),
            keystore: state.keystore.clone(),
            key_policy_cache: state.key_policy_cache.clone(),
            onwards_targets: None,
            internal_credentials: state.internal_credentials.clone(),
            api_key_usage: Default::default(),
            openrouter_refresh: Default::default(),
        };

        let external_user_id = user.external_user_id.as_ref().unwrap_or(&user.username);
        let request = axum::http::Request::builder()
            .uri("/admin/api/v1/ai/v1/chat/completions")
            .header("cookie", format!("{}={}", config.auth.native.session.cookie_name, jwt_token))
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", &user.email)
            .body(
                json!({
                    "model": model.alias
                })
                .to_string()
                .into(),
            )
            .unwrap();

        let request = admin_ai_proxy(state, request).await.unwrap();
        assert_eq!(request.uri().path(), "/ai/v1/chat/completions");
        assert!(request.headers().get("authorization").is_some());
    }

    #[dwctl_test_macros::test]
    async fn test_auto_user_creation_via_current_user(pool: PgPool) {
        let config = create_test_config();

        let mut tx = pool.begin().await.unwrap();

        let mut endpoints = InferenceEndpoints::new(&mut tx);
        let endpoint = endpoints
            .create(&InferenceEndpointCreateDBRequest {
                name: "Test Endpoint".to_string(),
                description: Some("Test endpoint".to_string()),
                url: "http://localhost:8000".parse().unwrap(),
                api_key: None,
                model_filter: None,
                auth_header_name: None,
                auth_header_prefix: None,
                reasoning_translation: None,
                accepts_scheduling_priority: false,
                created_by: Uuid::nil(),
                kind: Default::default(),
            })
            .await
            .expect("Failed to create test inference endpoint");

        let mut deployments = Deployments::new(&mut tx);
        let model = deployments
            .create(
                &DeploymentCreateDBRequest::builder()
                    .created_by(Uuid::nil())
                    .model_name("test_model".to_string())
                    .alias("gpt-4-auto-create".to_string())
                    .maybe_description(Some("Test deployment for auto user creation".to_string()))
                    .hosted_on(endpoint.id)
                    .build(),
            )
            .await
            .expect("Failed to create test deployment");

        let mut groups = Groups::new(&mut tx);
        groups
            .add_deployment_to_group(model.id, Uuid::nil(), Uuid::nil())
            .await
            .expect("Failed to add deployment to Everyone group");

        tx.commit().await.unwrap();

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let new_user_email = "auto-created@example.com";

        let mut user_conn = pool.acquire().await.unwrap();
        let mut users_repo = crate::db::handlers::Users::new(&mut user_conn);
        let existing_user = users_repo.get_user_by_email(new_user_email).await.unwrap();
        assert!(existing_user.is_none());

        let request = axum::http::Request::builder()
            .uri("/admin/api/v1/ai/v1/chat/completions")
            .header("x-doubleword-user", new_user_email)
            .header("x-doubleword-email", new_user_email)
            .body(
                json!({
                    "model": model.alias
                })
                .to_string()
                .into(),
            )
            .unwrap();

        let request = admin_ai_proxy(state, request).await.unwrap();
        assert_eq!(request.uri().path(), "/ai/v1/chat/completions");
        assert!(request.headers().get("authorization").is_some());

        let created_user = users_repo.get_user_by_email(new_user_email).await.unwrap();
        assert!(created_user.is_some());
        let db_user = created_user.unwrap();
        assert_eq!(db_user.email, new_user_email);
        assert_eq!(db_user.auth_source, "proxy-header");
    }

    #[dwctl_test_macros::test]
    async fn test_invalid_jwt_fallback_to_header(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let mut tx = pool.begin().await.unwrap();

        let mut endpoints = InferenceEndpoints::new(&mut tx);
        let endpoint = endpoints
            .create(&InferenceEndpointCreateDBRequest {
                name: "Test Endpoint".to_string(),
                description: Some("Test endpoint".to_string()),
                url: "http://localhost:8000".parse().unwrap(),
                api_key: None,
                model_filter: None,
                auth_header_name: None,
                auth_header_prefix: None,
                reasoning_translation: None,
                accepts_scheduling_priority: false,
                created_by: user.id,
                kind: Default::default(),
            })
            .await
            .expect("Failed to create test inference endpoint");

        let mut deployments = Deployments::new(&mut tx);
        let model = deployments
            .create(
                &DeploymentCreateDBRequest::builder()
                    .created_by(user.id)
                    .model_name("test_model".to_string())
                    .alias("gpt-4-fallback".to_string())
                    .maybe_description(Some("Test deployment for auth fallback".to_string()))
                    .hosted_on(endpoint.id)
                    .build(),
            )
            .await
            .expect("Failed to create test deployment");

        let mut groups = Groups::new(&mut tx);
        let group = groups
            .create(&GroupCreateDBRequest::new(
                Uuid::nil(),
                GroupCreate {
                    name: "fallback group".to_string(),
                    description: Some("A test group for auth fallback".to_string()),
                },
            ))
            .await
            .expect("Failed to create test group");

        groups
            .add_user_to_group(user.id, group.id)
            .await
            .expect("Failed to add user to group");
        groups
            .add_deployment_to_group(model.id, group.id, Uuid::nil())
            .await
            .expect("Failed to add deployment to group");

        tx.commit().await.unwrap();

        let mut state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config.clone()).await;
        state = crate::AppState {
            db: state.db,
            config: state.config,
            outlet_db: None,
            metrics_recorder: None,
            batch_sla_metrics: state.batch_sla_metrics,
            is_leader: false,
            request_manager: state.request_manager,
            task_runner: state.task_runner,
            limiters: state.limiters,
            connections_encryption_key: None,
            response_store: state.response_store,
            image_normalizer: state.image_normalizer.clone(),
            keystore: state.keystore.clone(),
            key_policy_cache: state.key_policy_cache.clone(),
            onwards_targets: None,
            internal_credentials: state.internal_credentials.clone(),
            api_key_usage: Default::default(),
            openrouter_refresh: Default::default(),
        };

        let external_user_id = user.external_user_id.as_ref().unwrap_or(&user.username);
        let request = axum::http::Request::builder()
            .uri("/admin/api/v1/ai/v1/chat/completions")
            .header("cookie", format!("{}=invalid-jwt-token", config.auth.native.session.cookie_name))
            .header("x-doubleword-user", external_user_id)
            .header("x-doubleword-email", &user.email)
            .body(
                json!({
                    "model": model.alias
                })
                .to_string()
                .into(),
            )
            .unwrap();

        let request = admin_ai_proxy(state, request).await.unwrap();
        assert_eq!(request.uri().path(), "/ai/v1/chat/completions");
        assert!(request.headers().get("authorization").is_some());
    }

    #[dwctl_test_macros::test]
    async fn test_invalid_api_key_with_valid_jwt(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.enabled = true;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let mut inference_conn = pool.acquire().await.unwrap();
        let mut endpoints = InferenceEndpoints::new(&mut inference_conn);
        let endpoint = endpoints
            .create(&InferenceEndpointCreateDBRequest {
                name: "Test Endpoint".to_string(),
                description: Some("Test endpoint".to_string()),
                url: "http://localhost:8000".parse().unwrap(),
                api_key: None,
                model_filter: None,
                auth_header_name: None,
                auth_header_prefix: None,
                reasoning_translation: None,
                accepts_scheduling_priority: false,
                created_by: user.id,
                kind: Default::default(),
            })
            .await
            .expect("Failed to create test inference endpoint");

        let mut deployment_conn = pool.acquire().await.unwrap();
        let mut deployments = Deployments::new(&mut deployment_conn);
        let model = deployments
            .create(
                &DeploymentCreateDBRequest::builder()
                    .created_by(user.id)
                    .model_name("test_model".to_string())
                    .alias("gpt-4-playground".to_string())
                    .maybe_description(Some("Test deployment for playground scenario".to_string()))
                    .hosted_on(endpoint.id)
                    .build(),
            )
            .await
            .expect("Failed to create test deployment");

        let mut group_conn = pool.acquire().await.unwrap();
        let mut groups = Groups::new(&mut group_conn);
        let group = groups
            .create(&GroupCreateDBRequest::new(
                Uuid::nil(),
                GroupCreate {
                    name: "playground group".to_string(),
                    description: Some("A test group for playground".to_string()),
                },
            ))
            .await
            .expect("Failed to create test group");

        groups
            .add_user_to_group(user.id, group.id)
            .await
            .expect("Failed to add user to group");
        groups
            .add_deployment_to_group(model.id, group.id, Uuid::nil())
            .await
            .expect("Failed to add deployment to group");

        let current_user = CurrentUser {
            auth_method: crate::api::models::users::AuthMethod::TrustedSso,
            id: user.id,
            username: user.username,
            email: user.email.clone(),
            is_admin: user.is_admin,
            roles: user.roles,
            display_name: user.display_name,
            avatar_url: user.avatar_url,
            payment_provider_id: None,
            organizations: vec![],
            active_organization: None,
            api_key_id: None,
        };
        let jwt_token = session::create_session_token(&current_user, &config).unwrap();

        let mut state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config.clone()).await;
        state = crate::AppState {
            db: state.db,
            config: state.config,
            outlet_db: None,
            metrics_recorder: None,
            batch_sla_metrics: state.batch_sla_metrics,
            is_leader: false,
            request_manager: state.request_manager,
            task_runner: state.task_runner,
            limiters: state.limiters,
            connections_encryption_key: None,
            response_store: state.response_store,
            image_normalizer: state.image_normalizer.clone(),
            keystore: state.keystore.clone(),
            key_policy_cache: state.key_policy_cache.clone(),
            onwards_targets: None,
            internal_credentials: state.internal_credentials.clone(),
            api_key_usage: Default::default(),
            openrouter_refresh: Default::default(),
        };

        let request = axum::http::Request::builder()
            .uri("/admin/api/v1/ai/v1/chat/completions")
            .header("cookie", format!("{}={}", config.auth.native.session.cookie_name, jwt_token))
            .header("authorization", "Bearer invalid-api-key-placeholder")
            .body(
                json!({
                    "model": model.alias
                })
                .to_string()
                .into(),
            )
            .unwrap();

        assert!(matches!(
            admin_ai_proxy(state, request).await,
            Err(crate::errors::Error::Unauthenticated { .. })
        ));
    }
}
