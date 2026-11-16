
use axum::{Json, extract::State, response::IntoResponse};
use serde::Serialize;
use utoipa::ToSchema;

use rust_decimal::prelude::ToPrimitive;

use crate::{AppState, api::models::users::CurrentUser};

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct BatchConfigResponse {
    pub enabled: bool,
    pub allowed_completion_windows: Vec<String>,
    pub allowed_url_paths: Vec<String>,
    pub async_requests: AsyncRequestsConfigResponse,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AsyncRequestsConfigResponse {
    pub enabled: bool,
    pub completion_window: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct OnwardsConfigResponse {
    pub strict_mode: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TolerationPresetResponse {
    pub id: String,
    pub label: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ConfigResponse {
    pub region: Option<String>,
    pub organization: Option<String>,
    pub payment_enabled: bool,
    pub first_payment_match_up_to: f64,
    pub docs_jsonl_url: Option<String>,
    pub docs_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batches: Option<BatchConfigResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ai_api_base_url: Option<String>,
    pub onwards: OnwardsConfigResponse,
    pub scheduling_toleration_presets: Vec<TolerationPresetResponse>,
}

#[utoipa::path(
    get,
    path = "/config",
    tag = "config",
    summary = "Get instance configuration",
    description = "Returns the current instance configuration including region, organization, \
        payment status, and batch processing capabilities. Use this endpoint to discover \
        what features are available on this Control Layer deployment.",
    responses(
        (status = 200, description = "Instance configuration", body = ConfigResponse),
        (status = 401, description = "Authentication required"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_config(State(state): State<AppState>, _user: CurrentUser) -> impl IntoResponse {
    let config = state.current_config();
    let metadata = &config.metadata;

    let batches_config = if config.batches.enabled {
        Some(BatchConfigResponse {
            enabled: config.batches.enabled,
            allowed_completion_windows: config.batches.allowed_completion_windows.clone(),
            allowed_url_paths: config.batches.allowed_url_paths.clone(),
            async_requests: AsyncRequestsConfigResponse {
                enabled: config.batches.async_requests.enabled,
                completion_window: config.batches.async_requests.completion_window.clone(),
            },
        })
    } else {
        None
    };

    let match_ceiling = config.credits.first_payment_match_up_to.to_f64().unwrap_or_else(|| {
        tracing::warn!(
            configured = %config.credits.first_payment_match_up_to,
            "credits.first_payment_match_up_to is not representable as f64; reporting the promotion as disabled"
        );
        0.0
    });

    let response = ConfigResponse {
        region: metadata.region.clone(),
        organization: metadata.organization.clone(),
        payment_enabled: config.payment.is_some(),
        first_payment_match_up_to: match_ceiling,
        docs_url: metadata.docs_url.clone(),
        docs_jsonl_url: metadata.docs_jsonl_url.clone(),
        batches: batches_config,
        ai_api_base_url: metadata.ai_api_base_url.clone(),
        onwards: OnwardsConfigResponse {
            strict_mode: config.onwards.strict_mode,
        },
        scheduling_toleration_presets: config
            .scheduling
            .toleration_presets
            .iter()
            .map(|preset| TolerationPresetResponse {
                id: preset.id.clone(),
                label: preset.label.clone(),
                description: preset.description.clone(),
            })
            .collect(),
    };

    Json(response)
}

#[cfg(test)]
mod tests {
    use crate::Application;
    use crate::api::models::users::Role;
    use crate::test::utils::{add_auth_headers, create_test_app, create_test_app_with_config, create_test_config, create_test_user};
    use axum::http::StatusCode;
    use serde_json::Value;
    use sqlx::PgPool;
    use tempfile::tempdir;

    fn write_test_config(path: &std::path::Path, organization: &str, docs_url: &str) {
        let config = format!(
            r#"host: "127.0.0.1"
port: 0
dashboard_url: "http://localhost:3001"
database:
  type: external
  url: "postgres://ignored"
admin_email: "admin@test.com"
secret_key: "test-secret-key-for-testing-only"
model_sources: []
metadata:
  organization: "{organization}"
  docs_url: "{docs_url}"
auth:
  native:
    enabled: false
  proxy_header:
    enabled: true
batches:
  enabled: true
  allowed_completion_windows:
    - "24h"
  allowed_url_paths:
    - "/v1/chat/completions"
background_services:
  onwards_sync:
    enabled: false
  probe_scheduler:
    enabled: false
  batch_daemon:
    enabled: never
  leader_election:
    enabled: false
onwards:
  strict_mode: false
"#
        );

        std::fs::write(path, config).expect("failed to write test config");
    }

    #[dwctl_test_macros::test]
    async fn test_get_config_returns_metadata(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let headers = add_auth_headers(&user);
        let response = app
            .get("/admin/api/v1/config")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;

        response.assert_status(StatusCode::OK);
        let json: Value = response.json();

        assert!(json.get("region").is_some());
        assert!(json.get("organization").is_some());
    }

    #[dwctl_test_macros::test]
    async fn test_get_config_exposes_the_first_payment_match_ceiling(pool: PgPool) {
        let mut config = create_test_config();
        config.credits.first_payment_match_up_to = rust_decimal::Decimal::new(50, 0);
        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let headers = add_auth_headers(&user);
        let response = app
            .get("/admin/api/v1/config")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;

        response.assert_status(StatusCode::OK);
        let json: Value = response.json();

        assert_eq!(json.get("first_payment_match_up_to").and_then(|v| v.as_f64()), Some(50.0));
    }

    #[dwctl_test_macros::test]
    async fn test_get_config_reports_a_disabled_promotion_as_zero(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let headers = add_auth_headers(&user);
        let response = app
            .get("/admin/api/v1/config")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;

        response.assert_status(StatusCode::OK);
        let json: Value = response.json();

        assert_eq!(json.get("first_payment_match_up_to").and_then(|v| v.as_f64()), Some(0.0));
    }

    #[dwctl_test_macros::test]
    async fn test_get_config_requires_authentication(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;

        let response = app.get("/admin/api/v1/config").await;

        response.assert_status(StatusCode::UNAUTHORIZED);
    }

    #[dwctl_test_macros::test]
    async fn test_get_config_includes_batch_slas(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let headers = add_auth_headers(&user);
        let response = app
            .get("/admin/api/v1/config")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;

        response.assert_status(StatusCode::OK);
        let json: Value = response.json();

        let batches = json.get("batches").expect("batches field should exist");
        assert_eq!(batches.get("enabled").and_then(|v| v.as_bool()), Some(true));

        let slas = batches
            .get("allowed_completion_windows")
            .and_then(|v| v.as_array())
            .expect("allowed_completion_windows should be an array");

        assert!(slas.iter().any(|v| v.as_str() == Some("24h")));

        let url_paths = batches
            .get("allowed_url_paths")
            .and_then(|v| v.as_array())
            .expect("allowed_url_paths should be an array");

        assert!(url_paths.iter().any(|v| v.as_str() == Some("/v1/chat/completions")));
    }

    #[dwctl_test_macros::test]
    async fn test_get_config_reflects_live_config_file_changes(pool: PgPool) {
        let tempdir = tempdir().expect("failed to create tempdir");
        let config_path = tempdir.path().join("config.yaml");
        write_test_config(&config_path, "Initial Org", "https://docs.example.com/initial");

        let config = crate::config::Config::load_from_path(&config_path).expect("failed to load initial test config");
        let app = Application::new_with_pool_and_config_path(config, Some(config_path.clone()), Some(pool.clone()), None)
            .await
            .expect("failed to create application");
        let (server, bg_services) = app.into_test_server();

        let user = create_test_user(&pool, Role::StandardUser).await;
        let headers = add_auth_headers(&user);

        let initial_response = server
            .get("/admin/api/v1/config")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;
        initial_response.assert_status(StatusCode::OK);
        let initial_json: Value = initial_response.json();
        assert_eq!(initial_json.get("organization").and_then(|v| v.as_str()), Some("Initial Org"));
        assert_eq!(
            initial_json.get("docs_url").and_then(|v| v.as_str()),
            Some("https://docs.example.com/initial")
        );

        write_test_config(&config_path, "Updated Org", "https://docs.example.com/updated");

        let mut final_json = None;
        for _ in 0..50 {
            let response = server
                .get("/admin/api/v1/config")
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .await;
            response.assert_status(StatusCode::OK);
            let json: Value = response.json();

            if json.get("organization").and_then(|v| v.as_str()) == Some("Updated Org")
                && json.get("docs_url").and_then(|v| v.as_str()) == Some("https://docs.example.com/updated")
            {
                final_json = Some(json);
                break;
            }

            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let final_json = final_json.expect("config endpoint never reflected updated config file");
        assert_eq!(final_json.get("organization").and_then(|v| v.as_str()), Some("Updated Org"));
        assert_eq!(
            final_json.get("docs_url").and_then(|v| v.as_str()),
            Some("https://docs.example.com/updated")
        );

        bg_services.shutdown().await;
    }
}
