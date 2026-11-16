
use crate::api::models::users::Role;
use crate::config::{DaemonConfig, DaemonEnabled};
use crate::db::handlers::api_keys::ApiKeys;
use crate::db::models::api_keys::ApiKeyPurpose;
use crate::test::utils::{
    add_auth_headers, add_deployment_to_group, add_user_to_group, create_test_admin_user, create_test_app_with_config, create_test_config,
    create_test_endpoint, create_test_model, create_test_user, create_test_user_with_roles,
};
use sqlx::PgPool;

const SENTINEL_PROMPT: &str = "ZDR-SENTINEL-PROMPT-7e1d4a";
const SENTINEL_COMPLETION: &str = "ZDR-SENTINEL-COMPLETION-9b2c5f";

const MODEL_ALIAS: &str = "zdr-sentinel";

struct SentinelFixture {
    server: axum_test::TestServer,
    _bg_services: crate::BackgroundServices,
    _mock_server: wiremock::MockServer,
    realtime_key: String,
    user_id: uuid::Uuid,
}

async fn setup_sentinel_fixture(pool: &PgPool) -> SentinelFixture {
    let mock_server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/v1/chat/completions"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "chatcmpl-zdr-sentinel",
            "object": "chat.completion",
            "created": 1_677_652_288,
            "model": "upstream-model",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": SENTINEL_COMPLETION },
                "finish_reason": "stop"
            }],
            "usage": { "prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8 }
        })))
        .mount(&mock_server)
        .await;

    let mut config = create_test_config();
    config.background_services.onwards_sync.enabled = true;

    let app = crate::Application::new_with_pool(config, Some(pool.clone()), None)
        .await
        .expect("Failed to create application");
    let (server, bg_services) = app.into_test_server();

    let admin_user = create_test_admin_user(pool, Role::PlatformManager).await;
    let admin_headers = add_auth_headers(&admin_user);
    let user = create_test_user(pool, Role::StandardUser).await;
    let user_headers = add_auth_headers(&user);

    let group: crate::api::models::groups::GroupResponse = server
        .post("/admin/api/v1/groups")
        .add_header(&admin_headers[0].0, &admin_headers[0].1)
        .add_header(&admin_headers[1].0, &admin_headers[1].1)
        .json(&serde_json::json!({ "name": "zdr-sentinel-group", "description": "ZDR sentinel test" }))
        .await
        .json();

    server
        .post(&format!("/admin/api/v1/groups/{}/users/{}", group.id, user.id))
        .add_header(&admin_headers[0].0, &admin_headers[0].1)
        .add_header(&admin_headers[1].0, &admin_headers[1].1)
        .await;

    server
        .post("/admin/api/v1/transactions")
        .add_header(&admin_headers[0].0, &admin_headers[0].1)
        .add_header(&admin_headers[1].0, &admin_headers[1].1)
        .json(&serde_json::json!({
            "user_id": user.id,
            "transaction_type": "admin_grant",
            "amount": 1000,
            "source_id": admin_user.id,
            "description": "Credits for ZDR sentinel test"
        }))
        .await;

    let endpoint: crate::api::models::inference_endpoints::InferenceEndpointResponse = server
        .post("/admin/api/v1/endpoints")
        .add_header(&admin_headers[0].0, &admin_headers[0].1)
        .add_header(&admin_headers[1].0, &admin_headers[1].1)
        .json(&serde_json::json!({ "name": "ZDR Sentinel Endpoint", "url": format!("{}/v1", mock_server.uri()) }))
        .await
        .json();

    let model: crate::api::models::deployments::DeployedModelResponse = server
        .post("/admin/api/v1/models")
        .add_header(&admin_headers[0].0, &admin_headers[0].1)
        .add_header(&admin_headers[1].0, &admin_headers[1].1)
        .json(&serde_json::json!({
            "type": "standard",
            "model_name": "zdr-sentinel-model",
            "alias": MODEL_ALIAS,
            "hosted_on": endpoint.id,
            "tariffs": [{
                "name": "default",
                "input_price_per_token": "0.001",
                "output_price_per_token": "0.003",
                "api_key_purpose": "realtime"
            }]
        }))
        .await
        .json();

    server
        .post(&format!("/admin/api/v1/groups/{}/models/{}", group.id, model.id))
        .add_header(&admin_headers[0].0, &admin_headers[0].1)
        .add_header(&admin_headers[1].0, &admin_headers[1].1)
        .await;

    let realtime_key: crate::api::models::api_keys::ApiKeyResponse = server
        .post(&format!("/admin/api/v1/users/{}/api-keys", user.id))
        .add_header(&user_headers[0].0, &user_headers[0].1)
        .add_header(&user_headers[1].0, &user_headers[1].1)
        .json(&serde_json::json!({ "name": "ZDR Realtime Key", "purpose": "realtime" }))
        .await
        .json();

    bg_services.sync_onwards_config(pool).await.expect("Failed to sync onwards config");

    SentinelFixture {
        server,
        _bg_services: bg_services,
        _mock_server: mock_server,
        realtime_key: realtime_key.key,
        user_id: user.id,
    }
}

async fn send_sentinel_request(fixture: &SentinelFixture) {
    let body = serde_json::json!({
        "model": MODEL_ALIAS,
        "messages": [{ "role": "user", "content": SENTINEL_PROMPT }]
    });
    for attempt in 0..100 {
        let resp = fixture
            .server
            .post("/ai/v1/chat/completions")
            .add_header("authorization", format!("Bearer {}", fixture.realtime_key))
            .json(&body)
            .await;
        let status = resp.status_code().as_u16();
        if status == 200 {
            return;
        }
        assert!(
            matches!(status, 403 | 404),
            "realtime request should succeed; got {status}, body: {}",
            resp.text()
        );
        assert!(attempt < 99, "model never became routable after polling, last status: {status}");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn poll_latest_row_json(pool: &PgPool, table: &str, where_sql: &str, order_col: &str) -> Option<String> {
    let query = format!("SELECT to_jsonb(t)::text FROM {table} t WHERE {where_sql} ORDER BY t.{order_col} DESC LIMIT 1");
    for _ in 0..200 {
        let row: Option<(String,)> = sqlx::query_as(&query).fetch_optional(pool).await.expect("query durable store");
        if let Some((json,)) = row {
            return Some(json);
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    None
}

async fn all_rows_json(pool: &PgPool, table: &str) -> String {
    let query = format!("SELECT coalesce(string_agg(to_jsonb(t)::text, ' '), '') FROM {table} t");
    let row: (String,) = sqlx::query_as(&query).fetch_one(pool).await.expect("aggregate durable store rows");
    row.0
}

#[dwctl_test_macros::test]
async fn zdr_sentinel_realtime_request_does_not_persist_to_analytics(pool: PgPool) {
    let fixture = setup_sentinel_fixture(&pool).await;
    send_sentinel_request(&fixture).await;

    let success_row = poll_latest_row_json(&pool, "http_analytics", "status_code = 200", "timestamp")
        .await
        .expect("an http_analytics row should be written for the successful request");

    let all = all_rows_json(&pool, "http_analytics").await;
    assert!(!all.contains(SENTINEL_PROMPT), "prompt sentinel leaked into http_analytics: {all}");
    assert!(
        !all.contains(SENTINEL_COMPLETION),
        "completion sentinel leaked into http_analytics: {all}"
    );

    let v: serde_json::Value = serde_json::from_str(&success_row).expect("row is valid json");
    assert_eq!(v["status_code"].as_i64(), Some(200), "status should be recorded");
    assert_eq!(v["total_tokens"].as_i64(), Some(8), "token usage should be recorded");
    assert!(v["model"].is_string(), "model should be recorded: {success_row}");
}

#[ignore = "ZDR request-logging capture gate not yet implemented"]
#[dwctl_test_macros::test]
async fn zdr_sentinel_realtime_request_not_in_request_logs(pool: PgPool) {
    let fixture = setup_sentinel_fixture(&pool).await;
    send_sentinel_request(&fixture).await;

    let row = poll_latest_row_json(&pool, "outlet.http_requests", "true", "correlation_id")
        .await
        .expect("an http_requests row should be written when request logging is enabled");

    assert!(!row.contains(SENTINEL_PROMPT), "prompt sentinel persisted to http_requests: {row}");
    assert!(
        !row.contains(SENTINEL_COMPLETION),
        "completion sentinel persisted to http_requests: {row}"
    );
    let _ = fixture.user_id;
}


const ASYNC_PROMPT_SENTINEL: &str = "pikachu-async-prompt-3f9a17";
const ASYNC_ERROR_SENTINEL: &str = "pikachu-fainted-error-body-8c2d04";

#[derive(Clone)]
struct CaptureWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
    type Writer = CaptureWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[dwctl_test_macros::test]
async fn zdr_sentinel_async_batch_failure_does_not_log_payload(pool: PgPool) {
    let log_buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .with_writer(CaptureWriter(log_buf.clone()))
        .finish();
    let _log_guard = tracing::subscriber::set_default(subscriber);

    let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
    add_user_to_group(&pool, user.id, uuid::Uuid::nil()).await;
    let batch_key_id = {
        let mut conn = pool.acquire().await.expect("acquire");
        ApiKeys::new(&mut conn)
            .get_or_create_hidden_key(user.id, ApiKeyPurpose::Batch, user.id)
            .await
            .expect("batch api key")
    };

    let mock_server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/v1/chat/completions"))
        .respond_with(wiremock::ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": { "message": ASYNC_ERROR_SENTINEL, "type": "invalid_request_error" }
        })))
        .mount(&mock_server)
        .await;

    let endpoint_id = create_test_endpoint(&pool, "zdr-async-endpoint", user.id).await;
    sqlx::query("UPDATE inference_endpoints SET url = $1 WHERE id = $2")
        .bind(mock_server.uri())
        .bind(endpoint_id)
        .execute(&pool)
        .await
        .expect("update endpoint url");
    let deployment_id = create_test_model(&pool, "pikachu-async-model", "pikachu-async", endpoint_id, user.id).await;
    add_deployment_to_group(&pool, deployment_id, uuid::Uuid::nil(), user.id).await;

    let mut config = create_test_config();
    config.host = "127.0.0.1".to_string();
    config.port = mock_server.address().port();
    config.background_services.batch_daemon = DaemonConfig {
        enabled: DaemonEnabled::Always,
        claim_interval_ms: 100,
        max_retries: Some(0),
        ..Default::default()
    };
    config.background_services.onwards_sync.enabled = true;
    config.background_services.probe_scheduler.enabled = false;
    config.background_services.leader_election.enabled = false;
    let (_server, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;

    let file_id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO fusillade.files (id, name, purpose, size_bytes, status, uploaded_by, created_at)
         VALUES ($1, 'zdr.jsonl', 'batch', 100, 'processed', $2, NOW())",
    )
    .bind(file_id)
    .bind(user.id.to_string())
    .execute(&pool)
    .await
    .expect("insert file");

    let template_id = uuid::Uuid::new_v4();
    let request_body = serde_json::json!({
        "model": "pikachu-async",
        "messages": [{ "role": "user", "content": ASYNC_PROMPT_SENTINEL }]
    })
    .to_string();
    crate::test::utils::insert_fusillade_template(
        &pool,
        template_id,
        Some(file_id),
        "pikachu-async",
        &mock_server.uri(),
        "/v1/chat/completions",
        &request_body,
        Some("req-1"),
    )
    .await;

    let batch_id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO fusillade.batches (id, created_by, file_id, endpoint, completion_window, expires_at, created_at, api_key_id)
         VALUES ($1, $2, $3, '/v1/chat/completions', '24h', $4, NOW(), $5)",
    )
    .bind(batch_id)
    .bind(user.id.to_string())
    .bind(file_id)
    .bind(chrono::Utc::now() + chrono::Duration::hours(24))
    .bind(batch_key_id)
    .execute(&pool)
    .await
    .expect("insert batch");

    let request_id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, created_at)
         VALUES ($1, $2, $3, 'pikachu-async', 'pending', NOW())",
    )
    .bind(request_id)
    .bind(batch_id)
    .bind(template_id)
    .execute(&pool)
    .await
    .expect("insert request");

    let mut failed = false;
    for _ in 0..150 {
        let state: Option<String> = sqlx::query_scalar("SELECT state::text FROM fusillade.requests WHERE id = $1")
            .bind(request_id)
            .fetch_optional(&pool)
            .await
            .expect("query request state");
        if state.as_deref() == Some("failed") {
            failed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(failed, "daemon never marked the request failed");

    const PROBE: &str = "zdr-capture-probe-marker-do-not-remove";
    tokio::spawn(async { tracing::warn!("{PROBE}") }).await.unwrap();

    let logs = String::from_utf8_lossy(&log_buf.lock().unwrap()).into_owned();
    assert!(
        logs.contains(PROBE),
        "log capture is not observing spawned-task events; the sentinel assertions below would be vacuous"
    );

    assert!(!logs.contains(ASYNC_PROMPT_SENTINEL), "prompt content leaked into async/flex logs");
    assert!(
        !logs.contains(ASYNC_ERROR_SENTINEL),
        "provider error body leaked into async/flex logs (fusillade daemon terminal-failure log)"
    );
}
