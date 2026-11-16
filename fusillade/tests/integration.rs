use fusillade::batch::{BackgroundBatchInput, BatchInput, RequestTemplateInput};
use fusillade::daemon::{
    DaemonConfig, ModelEscalationConfig, RetentionMaintenanceConfig, default_should_retry,
};
use fusillade::http::{HttpResponse, MockHttpClient};
use fusillade::manager::{ModelFilter, ModelFilterState, Storage};
use fusillade::request::{
    CreateBackgroundInput, CreateFlexInput, Failed, ListRequestsFilter, Request, ServiceTierFilter,
};
use fusillade::{Daemon, DaemonMode, PostgresDaemon};
use fusillade_arsenal::{PostgresRequestManager as PostgresStore, TestDbPools};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

type TestStore = PostgresStore<TestDbPools>;
type TestDaemon = PostgresDaemon<TestDbPools, MockHttpClient>;

async fn postgres_store(pool: sqlx::PgPool, config: &DaemonConfig) -> Arc<TestStore> {
    Arc::new(PostgresStore::new(
        TestDbPools::new(pool).await.unwrap(),
        config.into(),
    ))
}

fn postgres_daemon(
    store: Arc<TestStore>,
    http_client: Arc<MockHttpClient>,
    config: DaemonConfig,
) -> Arc<TestDaemon> {
    Arc::new(PostgresDaemon::new(store, http_client, config))
}

async fn mark_models_live_for_test(manager: &PostgresStore<TestDbPools>, models: &[&str]) {
    let filters: Vec<ModelFilter> = models
        .iter()
        .map(|model| ModelFilter {
            model: (*model).to_string(),
            state: ModelFilterState::Live,
            expected_ready_at: None,
        })
        .collect();

    manager.append_model_filter_events(&filters).await.unwrap();
}

async fn seed_leak_work(store: &TestStore) {
    for model in ["large", "slow"] {
        store
            .append_model_filter_events(&[ModelFilter {
                model: model.into(),
                state: ModelFilterState::Absent,
                expected_ready_at: None,
            }])
            .await
            .unwrap();
        let file = store
            .create_file(
                "tiny".into(),
                Some("alice".into()),
                (0..3)
                    .map(|n| RequestTemplateInput {
                        custom_id: Some(n.to_string()),
                        endpoint: "https://api.example.com".into(),
                        method: "POST".into(),
                        path: "/test".into(),
                        body: "{}".into(),
                        model: model.into(),
                    })
                    .collect(),
            )
            .await
            .unwrap();
        store
            .create_batch(BatchInput {
                file_id: file,
                endpoint: "/v1/chat/completions".into(),
                completion_window: "24h".into(),
                metadata: None,
                created_by: Some("alice".into()),
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();
    }
    for _ in 0..1 {
        store
            .create_flex(CreateFlexInput {
                request_id: uuid::Uuid::new_v4(),
                body: "{}".into(),
                model: "large".into(),
                endpoint: "https://api.example.com".into(),
                method: "POST".into(),
                path: "/test".into(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: "alice".into(),
                metadata: None,
            })
            .await
            .unwrap();
    }
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn heartbeat_lease_loss_fails_daemon_without_cancelling_caller(pool: sqlx::PgPool) {
    let config = DaemonConfig {
        mode: DaemonMode::RequestOnly,
        heartbeat_interval_ms: 100,
        stale_daemon_threshold_ms: 500,
        claim_interval_ms: 1_000,
        status_log_interval_ms: None,
        throughput_log_interval_ms: None,
        purge_interval_ms: 0,
        ..Default::default()
    };
    let store = postgres_store(pool.clone(), &config).await;
    let caller_shutdown = CancellationToken::new();
    let daemon = Arc::new(Daemon::new(
        store,
        Arc::new(MockHttpClient::new()),
        config,
        caller_shutdown.clone(),
    ));
    let daemon_handle = tokio::spawn(daemon.run_with_mode(DaemonMode::RequestOnly));

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let running: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM daemons WHERE status = 'running')",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("daemon did not register as running");

    let mut lock = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE daemons IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();

    let error = tokio::time::timeout(Duration::from_secs(3), daemon_handle)
        .await
        .expect("daemon did not self-fence after its heartbeat lease expired")
        .expect("daemon task panicked")
        .expect_err("heartbeat lease loss must fail the daemon run loop");
    assert!(error.to_string().contains("heartbeat lease expired"));
    assert!(
        !caller_shutdown.is_cancelled(),
        "self-fencing must not cancel the caller's process-wide token"
    );

    lock.rollback().await.unwrap();
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn batch_fixed_leak_interval_eventually_refills(pool: sqlx::PgPool) {
    let config = DaemonConfig {
        claim_interval_ms: 10,
        model_concurrency_limits: Arc::new(dashmap::DashMap::from_iter([
            ("large".to_string(), 10),
            ("slow".to_string(), 10),
        ])),
        ..Default::default()
    };
    let store = postgres_store(pool, &config).await;
    seed_leak_work(&store).await;
    let client = Arc::new(MockHttpClient::new());
    for _ in 0..7 {
        client.add_response(
            "POST /test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: "{}".into(),
            }),
        );
    }

    let shutdown = CancellationToken::new();
    let daemon = Arc::new(
        PostgresDaemon::new(store, client.clone(), config).with_leak_config(
            fusillade::daemon::LeakConfig {
                model_leak_interval_seconds: std::collections::HashMap::from([(
                    "large".into(),
                    std::num::NonZeroU32::new(1).unwrap(),
                )]),
                ..Default::default()
            },
        ),
    );
    let handle = daemon.clone().run(shutdown.clone()).unwrap();
    let progressed = tokio::time::timeout(Duration::from_secs(30), async {
        while client.call_count() < 5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();
    progressed.expect("batch workloads failed to refill and dispatch");
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn batch_leak_disabled_preserves_async_and_live_claims(pool: sqlx::PgPool) {
    let config = DaemonConfig {
        claim_interval_ms: 10,
        model_concurrency_limits: Arc::new(dashmap::DashMap::from_iter([
            ("large".to_string(), 10),
            ("slow".to_string(), 10),
        ])),
        ..Default::default()
    };
    let store = postgres_store(pool.clone(), &config).await;
    for (iteration, explicit_disable) in [true, false].into_iter().enumerate() {
        seed_leak_work(&store).await;
        mark_models_live_for_test(&store, &["slow"]).await;
        let client = Arc::new(MockHttpClient::new());
        for _ in 0..10 {
            client.add_response(
                "POST /test",
                Ok(HttpResponse {
                    x_request_id: None,
                    status: 200,
                    body: "{}".into(),
                }),
            );
        }
        let mut daemon = PostgresDaemon::new(store.clone(), client.clone(), config.clone());
        if explicit_disable {
            daemon = daemon.with_leak_config(fusillade::daemon::LeakConfig {
                leak_enabled: false,
                ..Default::default()
            });
        }
        let shutdown = CancellationToken::new();
        let handle = Arc::new(daemon).run(shutdown.clone()).unwrap();
        let progressed = tokio::time::timeout(Duration::from_secs(30), async {
            while client.call_count() < 4 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        shutdown.cancel();
        handle.await.unwrap().unwrap();
        progressed.expect("live batch and async claims must still dispatch");
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM requests WHERE model = 'large' AND batch_id IS NOT NULL AND state = 'pending'"
        ).fetch_one(&pool).await.unwrap();
        assert_eq!(
            pending,
            3 * (iteration as i64 + 1),
            "disabled batch leaking must not consume an initial token"
        );
    }
}

fn retry_backoff_ms(config: &DaemonConfig, retry_attempt: u32) -> i64 {
    config
        .backoff_ms
        .saturating_mul(config.backoff_factor.saturating_pow(retry_attempt))
        .min(config.max_backoff_ms) as i64
}

fn assert_next_retry_would_cross_effective_deadline(
    failed: &Request<Failed>,
    config: &DaemonConfig,
) {
    let batch_expires_at = failed
        .state
        .batch_expires_at
        .expect("this assertion is only valid for SLA requests");
    let effective_deadline = match config.stop_before_deadline_ms {
        Some(stop_before_deadline_ms) => {
            batch_expires_at - chrono::Duration::milliseconds(stop_before_deadline_ms)
        }
        None => batch_expires_at,
    };
    let next_retry_at = failed.state.failed_at
        + chrono::Duration::milliseconds(retry_backoff_ms(config, failed.state.retry_attempt));

    assert!(
        next_retry_at >= effective_deadline,
        "expected terminal failure because the next retry at {next_retry_at} would cross effective deadline {effective_deadline}; failed_at={}, retry_attempt={}",
        failed.state.failed_at,
        failed.state.retry_attempt
    );
}

async fn wait_for_mock_calls(client: &MockHttpClient, expected: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if client.call_count() >= expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {expected} HTTP calls; observed {}",
            client.call_count()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn call_priority(call: &fusillade::http::MockCall) -> i64 {
    serde_json::from_str::<serde_json::Value>(&call.body).unwrap()["nvext"]["agent_hints"]
        ["priority"]
        .as_i64()
        .expect("daemon must inject an integer priority")
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn postgres_convenience_builder_installs_retained_response_fence(pool: sqlx::PgPool) {
    let retention = RetentionMaintenanceConfig::new(fusillade::RetentionPolicy {
        batchless_seconds_by_service_tier: std::collections::HashMap::from([(
            "flex".to_string(),
            86_400,
        )]),
        max_late_writer_seconds: Some(7_200),
        ..Default::default()
    });
    let daemon = PostgresDaemon::try_from_pools_with_retention(
        TestDbPools::new(pool).await.unwrap(),
        DaemonConfig::default(),
        retention,
    )
    .expect("valid retention configuration must construct");
    assert_eq!(
        daemon.storage().retained_response_fence_seconds(),
        Some(7_200)
    );
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn postgres_run_rejects_invalid_structure_and_fence_before_spawning(pool: sqlx::PgPool) {
    let config = DaemonConfig {
        batch_archive_sweep_enabled: true,
        batch_archive_sweep_interval_ms: 0,
        ..Default::default()
    };
    let store = postgres_store(pool.clone(), &config).await;
    let error = postgres_daemon(store, Arc::new(MockHttpClient::new()), config)
        .run(CancellationToken::new())
        .expect_err("invalid structural config must fail synchronously");
    assert!(error.to_string().contains("sweep"));

    let retention = RetentionMaintenanceConfig::new(fusillade::RetentionPolicy {
        batchless_seconds_by_service_tier: std::collections::HashMap::from([(
            "flex".to_string(),
            86_400,
        )]),
        max_late_writer_seconds: Some(7_200),
        ..Default::default()
    })
    .with_batchless_archive_sweep_enabled(true);
    let config = DaemonConfig::default();
    let store = postgres_store(pool, &config).await;
    let error = Arc::new(
        PostgresDaemon::new(store, Arc::new(MockHttpClient::new()), config)
            .with_retention_maintenance(retention),
    )
    .run(CancellationToken::new())
    .expect_err("a mismatched store fence must fail synchronously");
    assert!(error.to_string().contains("late-writer fence"));
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn default_claims_start_while_weekly_partition_ddl_is_blocked(pool: sqlx::PgPool) {
    let mut blocker = pool.begin().await.expect("blocker transaction must start");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('ensure_archive_partitions')::bigint)")
        .execute(&mut *blocker)
        .await
        .expect("weekly partition lock must be held");

    let http_client = Arc::new(MockHttpClient::new());
    http_client.add_response(
        "POST /v1/ddl-independent",
        Ok(HttpResponse {
            x_request_id: None,
            status: 200,
            body: r#"{"result":"claimed"}"#.to_string(),
        }),
    );
    let limits = Arc::new(dashmap::DashMap::new());
    limits.insert("ddl-independent-model".to_string(), 1);
    let config = DaemonConfig {
        claim_interval_ms: 10,
        claim_batch_size: 1,
        model_concurrency_limits: limits,
        status_log_interval_ms: None,
        throughput_log_interval_ms: None,
        heartbeat_interval_ms: 10_000,
        cancellation_poll_interval_ms: 100,
        claim_query_timeout_ms: 5_000,
        ..Default::default()
    };
    let manager = postgres_store(pool, &config).await;
    manager
        .create_flex(CreateFlexInput {
            request_id: uuid::Uuid::new_v4(),
            body: r#"{"prompt":"claim while DDL waits"}"#.to_string(),
            model: "ddl-independent-model".to_string(),
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/v1/ddl-independent".to_string(),
            api_key_id: uuid::Uuid::new_v4(),
            created_by: "ddl-independent-owner".to_string(),
            metadata: None,
        })
        .await
        .expect("request must seed");
    mark_models_live_for_test(manager.as_ref(), &["ddl-independent-model"]).await;

    let shutdown = CancellationToken::new();
    let handle = postgres_daemon(manager, http_client.clone(), config)
        .run(shutdown.clone())
        .expect("default daemon structure must start");
    tokio::time::timeout(Duration::from_secs(2), wait_for_mock_calls(&http_client, 1))
        .await
        .expect("claim loop must not await the blocked weekly DDL");

    blocker.rollback().await.expect("blocker must release");
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("daemon must stop")
        .expect("daemon task must not panic")
        .expect("daemon must stop cleanly");
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn background_tier_end_to_end(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());
    let triggers: Vec<_> = (0..5)
        .map(|index| {
            http_client.add_response_with_trigger(
                "POST /v1/background-test",
                Ok(HttpResponse {
                    x_request_id: None,
                    status: 200,
                    body: format!(r#"{{"result":{index}}}"#),
                }),
            )
        })
        .collect();
    let mut triggers = triggers.into_iter();

    let model_limits = Arc::new(dashmap::DashMap::new());
    model_limits.insert("spare-model".to_string(), 4);
    let config = DaemonConfig {
        mode: fusillade::DaemonMode::Both,
        claim_batch_size: 10,
        batch_claim_size: 10,
        batch_claim_batch_size: 10,
        claim_interval_ms: 10,
        batch_claim_interval_ms: 10,
        model_concurrency_limits: model_limits,
        background_concurrency_limit: 2,
        inject_deadline_priority: true,
        status_log_interval_ms: None,
        throughput_log_interval_ms: None,
        heartbeat_interval_ms: 10_000,
        cancellation_poll_interval_ms: 100,
        purge_interval_ms: 0,
        ..Default::default()
    };
    let manager = postgres_store(pool.clone(), &config).await;

    for sequence in 0..2 {
        let request_id = uuid::Uuid::new_v4();
        manager
            .create_flex(CreateFlexInput {
                request_id,
                body: format!(r#"{{"kind":"sla","sequence":{sequence}}}"#),
                model: "spare-model".to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/background-test".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: format!("sla-owner-{sequence}"),
                metadata: None,
            })
            .await
            .unwrap();
    }

    let file_id = manager
        .create_file(
            "background-e2e".to_string(),
            None,
            vec![RequestTemplateInput {
                custom_id: Some("background-batch".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/background-test".to_string(),
                body: r#"{"kind":"background-batch","nvext":{"agent_hints":{"priority":999}}}"#
                    .to_string(),
                model: "spare-model".to_string(),
            }],
        )
        .await
        .unwrap();
    let background_batch = manager
        .create_background_batch(BackgroundBatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            metadata: None,
            created_by: Some("background-batch-owner".to_string()),
            api_key_id: None,
            total_requests: Some(1),
        })
        .await
        .unwrap();
    let background_request_id = uuid::Uuid::new_v4();
    manager
        .create_background(CreateBackgroundInput {
            request_id: background_request_id,
            body: r#"{"kind":"background-batchless","nvext":{"agent_hints":{"priority":999}}}"#
                .to_string(),
            model: "spare-model".to_string(),
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/v1/background-test".to_string(),
            api_key_id: uuid::Uuid::new_v4(),
            created_by: "background-request-owner".to_string(),
            metadata: None,
        })
        .await
        .unwrap();
    mark_models_live_for_test(manager.as_ref(), &["spare-model"]).await;

    let shutdown = CancellationToken::new();
    let daemon_handle = postgres_daemon(manager.clone(), http_client.clone(), config)
        .run(shutdown.clone())
        .unwrap();

    wait_for_mock_calls(&http_client, 2).await;
    let stability_deadline = tokio::time::Instant::now() + Duration::from_millis(150);
    while tokio::time::Instant::now() < stability_deadline {
        assert_eq!(
            http_client.call_count(),
            2,
            "background must not enter the SLA-reserved portion of model capacity"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for call in http_client.get_calls() {
        assert!(call.body.contains(r#""kind":"sla""#));
        assert!(call_priority(&call) > i32::MIN as i64);
    }

    triggers.next().unwrap().send(()).unwrap();
    wait_for_mock_calls(&http_client, 4).await;
    let calls = http_client.get_calls();
    assert!(calls[2].body.contains("background"));
    assert!(calls[3].body.contains("background"));
    assert_eq!(call_priority(&calls[2]), i32::MIN as i64);
    assert_eq!(call_priority(&calls[3]), i32::MIN as i64);
    assert!(
        calls[2].body.contains(r#""kind":"background-batch""#)
            || calls[3].body.contains(r#""kind":"background-batch""#)
    );
    assert!(
        calls[2].body.contains(r#""kind":"background-batchless""#)
            || calls[3].body.contains(r#""kind":"background-batchless""#)
    );
    assert_eq!(
        http_client.in_flight_count(),
        3,
        "each background modality may use foreground headroom without reserving it"
    );

    let late_sla_id = uuid::Uuid::new_v4();
    manager
        .create_flex(CreateFlexInput {
            request_id: late_sla_id,
            body: r#"{"kind":"late-sla"}"#.to_string(),
            model: "spare-model".to_string(),
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/v1/background-test".to_string(),
            api_key_id: uuid::Uuid::new_v4(),
            created_by: "late-sla-owner".to_string(),
            metadata: None,
        })
        .await
        .unwrap();
    wait_for_mock_calls(&http_client, 5).await;
    let calls = http_client.get_calls();
    assert!(calls[4].body.contains("late-sla"));
    assert!(call_priority(&calls[4]) > call_priority(&calls[2]));

    for _ in 0..3 {
        triggers.next().unwrap().send(()).unwrap();
    }
    triggers.next().unwrap().send(()).unwrap();

    let completion_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let batch = manager.get_batch(background_batch.id).await.unwrap();
        let background = manager
            .get_request_detail(fusillade::RequestId(background_request_id))
            .await
            .unwrap();
        let late_sla = manager
            .get_request_detail(fusillade::RequestId(late_sla_id))
            .await
            .unwrap();
        if batch.completed_at.is_some()
            && background.status == "completed"
            && late_sla.status == "completed"
        {
            assert!(batch.output_file_id.is_some());
            assert!(batch.error_file_id.is_some());
            break;
        }
        assert!(
            tokio::time::Instant::now() < completion_deadline,
            "background work did not finalize before timeout"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let active: i64 = sqlx::query_scalar!(
        r#"SELECT COUNT(*) AS "count!" FROM requests WHERE state IN ('claimed', 'processing')"#
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(active, 0);
    assert_eq!(http_client.in_flight_count(), 0);

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), daemon_handle)
        .await
        .expect("daemon should stop")
        .expect("daemon task should not panic")
        .expect("daemon should stop cleanly");
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
#[test_log::test]
async fn test_daemon_claims_and_completes_request(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());
    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            x_request_id: None,
            status: 200,
            body: r#"{"result":"success"}"#.to_string(),
        }),
    );

    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("test-model".to_string(), 10);

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        model_concurrency_limits,

        max_retries: Some(3),
        stop_before_deadline_ms: None,
        backoff_ms: 100,
        backoff_factor: 2,
        max_backoff_ms: 1000,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100,
        ..Default::default()
    };

    let manager = postgres_store(pool.clone(), &config).await;

    let file_id = manager
        .create_file(
            "test-file".to_string(),
            Some("Test file".to_string()),
            vec![fusillade::RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "test-model".to_string(),
            }],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: None,
            api_key_id: None,
            total_requests: None,
        })
        .await
        .expect("Failed to create batch");
    mark_models_live_for_test(manager.as_ref(), &["test-model"]).await;

    let requests = manager
        .get_batch_requests(batch.id)
        .await
        .expect("Failed to get batch requests");
    assert_eq!(requests.len(), 1);
    let request_id = requests[0].id();

    let shutdown_token = CancellationToken::new();
    postgres_daemon(manager.clone(), http_client.clone(), config)
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut completed = false;

    while start.elapsed() < timeout {
        let results = manager
            .get_requests(vec![request_id])
            .await
            .expect("Failed to get request");

        if let Some(Ok(any_request)) = results.first()
            && any_request.is_terminal()
        {
            if let fusillade::AnyRequest::Completed(req) = any_request {
                assert_eq!(req.state.response_status, 200);
                assert_eq!(req.state.response_body, r#"{"result":"success"}"#);
                completed = true;
                break;
            } else {
                panic!(
                    "Request reached terminal state but was not completed: {:?}",
                    any_request
                );
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_token.cancel();

    assert!(
        completed,
        "Request did not complete within timeout. Check daemon processing."
    );

    assert_eq!(http_client.call_count(), 1);
    let calls = http_client.get_calls();
    assert_eq!(calls[0].method, "POST");
    assert_eq!(calls[0].path, "/v1/test");
    assert_eq!(calls[0].api_key, "");
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn test_daemon_respects_per_model_concurrency_limits(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());

    let trigger1 = http_client.add_response_with_trigger(
        "POST /v1/test",
        Ok(HttpResponse {
            x_request_id: None,
            status: 200,
            body: r#"{"result":"1"}"#.to_string(),
        }),
    );
    let trigger2 = http_client.add_response_with_trigger(
        "POST /v1/test",
        Ok(HttpResponse {
            x_request_id: None,
            status: 200,
            body: r#"{"result":"2"}"#.to_string(),
        }),
    );
    let trigger3 = http_client.add_response_with_trigger(
        "POST /v1/test",
        Ok(HttpResponse {
            x_request_id: None,
            status: 200,
            body: r#"{"result":"3"}"#.to_string(),
        }),
    );
    let trigger4 = http_client.add_response_with_trigger(
        "POST /v1/test",
        Ok(HttpResponse {
            x_request_id: None,
            status: 200,
            body: r#"{"result":"4"}"#.to_string(),
        }),
    );
    let trigger5 = http_client.add_response_with_trigger(
        "POST /v1/test",
        Ok(HttpResponse {
            x_request_id: None,
            status: 200,
            body: r#"{"result":"5"}"#.to_string(),
        }),
    );

    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("gpt-4".to_string(), 2);

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        model_concurrency_limits,

        max_retries: Some(3),
        stop_before_deadline_ms: None,
        backoff_ms: 100,
        backoff_factor: 2,
        max_backoff_ms: 1000,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100,
        ..Default::default()
    };

    let manager = postgres_store(pool.clone(), &config).await;

    let file_id = manager
        .create_file(
            "test-file".to_string(),
            Some("Test concurrency limits".to_string()),
            vec![
                fusillade::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test1"}"#.to_string(),
                    model: "gpt-4".to_string(),
                },
                fusillade::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test2"}"#.to_string(),
                    model: "gpt-4".to_string(),
                },
                fusillade::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test3"}"#.to_string(),
                    model: "gpt-4".to_string(),
                },
                fusillade::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test4"}"#.to_string(),
                    model: "gpt-4".to_string(),
                },
                fusillade::RequestTemplateInput {
                    custom_id: None,
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test5"}"#.to_string(),
                    model: "gpt-4".to_string(),
                },
            ],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: None,
            api_key_id: None,
            total_requests: None,
        })
        .await
        .expect("Failed to create batch");
    mark_models_live_for_test(manager.as_ref(), &["gpt-4"]).await;

    let shutdown_token = CancellationToken::new();
    postgres_daemon(manager.clone(), http_client.clone(), config)
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(2);
    let mut reached_limit = false;

    while start.elapsed() < timeout {
        let in_flight = http_client.in_flight_count();
        if in_flight == 2 {
            reached_limit = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        reached_limit,
        "Expected exactly 2 requests in-flight, got {}",
        http_client.in_flight_count()
    );

    let start = tokio::time::Instant::now();
    let stable_duration = Duration::from_millis(100);
    while start.elapsed() < stable_duration {
        let in_flight = http_client.in_flight_count();
        assert!(
            in_flight <= 2,
            "Concurrency limit violated: {} requests in-flight (expected max 2)",
            in_flight
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        http_client.in_flight_count(),
        2,
        "Expected exactly 2 requests in-flight after stability check"
    );

    trigger1.send(()).unwrap();

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(2);
    let mut third_started = false;

    while start.elapsed() < timeout {
        if http_client.call_count() >= 3 {
            third_started = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        third_started,
        "Third request should have started after first completed"
    );

    assert_eq!(
        http_client.in_flight_count(),
        2,
        "Should maintain concurrency limit of 2"
    );

    trigger2.send(()).unwrap();
    trigger3.send(()).unwrap();
    trigger4.send(()).unwrap();
    trigger5.send(()).unwrap();

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut all_completed = false;

    while start.elapsed() < timeout {
        let status = manager
            .get_batch_status(batch.id)
            .await
            .expect("Failed to get batch status");

        if status.completed_requests == 5 {
            all_completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_token.cancel();

    assert!(all_completed, "All 5 requests should have completed");

    assert_eq!(http_client.call_count(), 5);
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn test_daemon_cuts_claim_capacity_after_escalated_model_529(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());
    let first_wave = [
        http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 529,
                body: r#"{"error":"model overloaded"}"#.to_string(),
            }),
        ),
        http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: r#"{"result":"success"}"#.to_string(),
            }),
        ),
        http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: r#"{"result":"success"}"#.to_string(),
            }),
        ),
        http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: r#"{"result":"success"}"#.to_string(),
            }),
        ),
    ];
    let remaining_triggers: Vec<_> = (0..5)
        .map(|_| {
            http_client.add_response_with_trigger(
                "POST /v1/test",
                Ok(HttpResponse {
                    x_request_id: None,
                    status: 200,
                    body: r#"{"result":"success"}"#.to_string(),
                }),
            )
        })
        .collect();

    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("source-model".to_string(), 4);
    model_concurrency_limits.insert("overloaded-model".to_string(), 4);
    let model_escalations = Arc::new(dashmap::DashMap::new());
    model_escalations.insert(
        "source-model".to_string(),
        ModelEscalationConfig {
            escalation_model: "overloaded-model".to_string(),
            escalation_threshold_seconds: 7_200,
        },
    );
    let config = DaemonConfig {
        claim_batch_size: 20,
        claim_interval_ms: 10,
        model_concurrency_limits,
        model_escalations,
        adaptive_concurrency: true,
        memory_gate_high_fraction: 0.75,
        max_retries: Some(3),
        stop_before_deadline_ms: None,
        backoff_ms: 10,
        backoff_factor: 1,
        max_backoff_ms: 10,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10_000,
        cancellation_poll_interval_ms: 100,
        ..Default::default()
    };
    let manager = postgres_store(pool, &config).await;
    let templates = (0..8)
        .map(|index| fusillade::RequestTemplateInput {
            custom_id: Some(format!("request-{index}")),
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/v1/test".to_string(),
            body: format!(r#"{{"prompt":"test-{index}"}}"#),
            model: "source-model".to_string(),
        })
        .collect();
    let file_id = manager
        .create_file("adaptive-concurrency".to_string(), None, templates)
        .await
        .unwrap();
    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/test".to_string(),
            completion_window: "1h".to_string(),
            metadata: None,
            created_by: None,
            api_key_id: None,
            total_requests: None,
        })
        .await
        .unwrap();
    mark_models_live_for_test(manager.as_ref(), &["source-model"]).await;

    let shutdown_token = CancellationToken::new();
    let daemon_handle = postgres_daemon(manager.clone(), http_client.clone(), config)
        .run(shutdown_token.clone())
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        while http_client.in_flight_count() != 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the configured initial concurrency should saturate");

    let [
        overload_trigger,
        success_trigger_1,
        success_trigger_2,
        success_trigger_3,
    ] = first_wave;
    overload_trigger.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while http_client.in_flight_count() != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the 529 response should finish before the successful requests");

    for trigger in [success_trigger_1, success_trigger_2, success_trigger_3] {
        trigger.send(()).unwrap();
    }

    tokio::time::timeout(Duration::from_secs(2), async {
        while http_client.in_flight_count() != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the 529 should cut source-model's limit from four to three");

    for trigger in remaining_triggers {
        trigger.send(()).unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = manager.get_batch_status(batch.id).await.unwrap();
            if status.completed_requests == 8 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all requests should complete after the throttled waves drain");
    shutdown_token.cancel();
    tokio::time::timeout(Duration::from_secs(5), daemon_handle)
        .await
        .expect("daemon should stop")
        .expect("daemon task should not panic")
        .expect("daemon should stop cleanly");
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn adaptive_concurrency_grows_past_the_configured_limit(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());
    let triggers: Vec<_> = (0..12)
        .map(|_| {
            http_client.add_response_with_trigger(
                "POST /v1/grow",
                Ok(HttpResponse {
                    x_request_id: None,
                    status: 200,
                    body: r#"{"result":"success"}"#.to_string(),
                }),
            )
        })
        .collect();

    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("growing-model".to_string(), 2);
    let config = DaemonConfig {
        claim_batch_size: 20,
        claim_interval_ms: 10,
        model_concurrency_limits,
        adaptive_concurrency: true,
        memory_gate_high_fraction: 0.75,
        max_retries: Some(3),
        stop_before_deadline_ms: None,
        status_log_interval_ms: None,
        throughput_log_interval_ms: None,
        heartbeat_interval_ms: 10_000,
        cancellation_poll_interval_ms: 100,
        purge_interval_ms: 0,
        ..Default::default()
    };
    let manager = postgres_store(pool, &config).await;
    for index in 0..12 {
        manager
            .create_flex(CreateFlexInput {
                request_id: uuid::Uuid::new_v4(),
                body: format!(r#"{{"kind":"grow-{index}"}}"#),
                model: "growing-model".to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/grow".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: format!("owner-{index}"),
                metadata: None,
            })
            .await
            .unwrap();
    }
    mark_models_live_for_test(manager.as_ref(), &["growing-model"]).await;

    let shutdown_token = CancellationToken::new();
    let daemon_handle = postgres_daemon(manager.clone(), http_client.clone(), config)
        .run(shutdown_token.clone())
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while http_client.in_flight_count() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the configured limit should bound the first wave");

    let mut triggers = triggers.into_iter();
    triggers.next().unwrap().send(()).unwrap();
    triggers.next().unwrap().send(()).unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while http_client.in_flight_count() <= 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a clean, capacity-bound model should be allowed past its configured limit");

    shutdown_token.cancel();
    for trigger in triggers {
        let _ = trigger.send(());
    }
    tokio::time::timeout(Duration::from_secs(5), daemon_handle)
        .await
        .expect("daemon should stop")
        .expect("daemon task should not panic")
        .expect("daemon should stop cleanly");
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn background_529_does_not_throttle_foreground_claims(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());
    let overload_trigger = http_client.add_response_with_trigger(
        "POST /v1/adaptive-shared",
        Ok(HttpResponse {
            x_request_id: None,
            status: 529,
            body: r#"{"error":"model overloaded"}"#.to_string(),
        }),
    );
    let success_triggers: Vec<_> = (0..4)
        .map(|_| {
            http_client.add_response_with_trigger(
                "POST /v1/adaptive-shared",
                Ok(HttpResponse {
                    x_request_id: None,
                    status: 200,
                    body: r#"{"result":"success"}"#.to_string(),
                }),
            )
        })
        .collect();

    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("shared-model".to_string(), 4);
    let config = DaemonConfig {
        mode: fusillade::DaemonMode::Both,
        claim_batch_size: 10,
        batch_claim_size: 10,
        batch_claim_batch_size: 10,
        claim_interval_ms: 10,
        batch_claim_interval_ms: 10,
        model_concurrency_limits,
        background_concurrency_limit: 4,
        inject_deadline_priority: true,
        adaptive_concurrency: true,
        memory_gate_high_fraction: 0.75,
        max_retries: Some(3),
        stop_before_deadline_ms: None,
        backoff_ms: 60_000,
        backoff_factor: 1,
        max_backoff_ms: 60_000,
        status_log_interval_ms: None,
        throughput_log_interval_ms: None,
        heartbeat_interval_ms: 10_000,
        cancellation_poll_interval_ms: 100,
        purge_interval_ms: 0,
        ..Default::default()
    };
    let manager = postgres_store(pool, &config).await;
    let background_request_id = uuid::Uuid::new_v4();
    manager
        .create_background(CreateBackgroundInput {
            request_id: background_request_id,
            body: r#"{"kind":"background-overload"}"#.to_string(),
            model: "shared-model".to_string(),
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/v1/adaptive-shared".to_string(),
            api_key_id: uuid::Uuid::new_v4(),
            created_by: "background-owner".to_string(),
            metadata: None,
        })
        .await
        .unwrap();
    mark_models_live_for_test(manager.as_ref(), &["shared-model"]).await;

    let shutdown_token = CancellationToken::new();
    let daemon_handle = postgres_daemon(manager.clone(), http_client.clone(), config)
        .run(shutdown_token.clone())
        .unwrap();

    wait_for_mock_calls(&http_client, 1).await;
    overload_trigger.send(()).unwrap();
    let overload_recorded_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let background = manager
            .get_request_detail(fusillade::RequestId(background_request_id))
            .await
            .unwrap();
        if background.status == "pending" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < overload_recorded_deadline,
            "background 529 was not rescheduled before timeout"
        );
        tokio::task::yield_now().await;
    }

    let foreground_ids: Vec<_> = (0..4).map(|_| uuid::Uuid::new_v4()).collect();
    for (index, request_id) in foreground_ids.iter().copied().enumerate() {
        manager
            .create_flex(CreateFlexInput {
                request_id,
                body: format!(r#"{{"kind":"foreground-{index}"}}"#),
                model: "shared-model".to_string(),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/adaptive-shared".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: format!("foreground-owner-{index}"),
                metadata: None,
            })
            .await
            .unwrap();
    }

    wait_for_mock_calls(&http_client, 5).await;
    assert_eq!(http_client.in_flight_count(), 4);
    let stability_deadline = tokio::time::Instant::now() + Duration::from_millis(150);
    while tokio::time::Instant::now() < stability_deadline {
        assert_eq!(
            http_client.call_count(),
            5,
            "a background 529 must not reduce foreground capacity"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut success_triggers = success_triggers.into_iter();
    success_triggers.next().unwrap().send(()).unwrap();
    success_triggers.next().unwrap().send(()).unwrap();
    success_triggers.next().unwrap().send(()).unwrap();
    success_triggers.next().unwrap().send(()).unwrap();

    let completion_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut all_completed = true;
        for request_id in &foreground_ids {
            let request = manager
                .get_request_detail(fusillade::RequestId(*request_id))
                .await
                .unwrap();
            all_completed &= request.status == "completed";
        }
        if all_completed {
            break;
        }
        assert!(
            tokio::time::Instant::now() < completion_deadline,
            "foreground requests did not complete before timeout"
        );
        tokio::task::yield_now().await;
    }

    shutdown_token.cancel();
    tokio::time::timeout(Duration::from_secs(5), daemon_handle)
        .await
        .expect("daemon should stop")
        .expect("daemon task should not panic")
        .expect("daemon should stop cleanly");
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn test_daemon_retries_configured_and_default_statuses(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());

    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            x_request_id: None,
            status: 499,
            body: r#"{"error":"arbitrary upstream cancellation"}"#.to_string(),
        }),
    );

    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            x_request_id: None,
            status: 503,
            body: r#"{"error":"service unavailable"}"#.to_string(),
        }),
    );

    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            x_request_id: None,
            status: 200,
            body: r#"{"result":"success after retries"}"#.to_string(),
        }),
    );

    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("test-model".to_string(), 10);

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        model_concurrency_limits,

        max_retries: Some(5),
        stop_before_deadline_ms: None,
        backoff_ms: 10,
        backoff_factor: 2,
        max_backoff_ms: 100,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100,
        ..Default::default()
    };

    let manager = postgres_store(pool.clone(), &config).await;

    let file_id = manager
        .create_file(
            "test-file".to_string(),
            Some("Test retry logic".to_string()),
            vec![fusillade::RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "test-model".to_string(),
            }],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: None,
            api_key_id: None,
            total_requests: None,
        })
        .await
        .expect("Failed to create batch");
    mark_models_live_for_test(manager.as_ref(), &["test-model"]).await;

    let requests = manager
        .get_batch_requests(batch.id)
        .await
        .expect("Failed to get batch requests");
    assert_eq!(requests.len(), 1);
    let request_id = requests[0].id();

    let shutdown_token = CancellationToken::new();
    postgres_daemon(manager.clone(), http_client.clone(), config)
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut completed = false;

    while start.elapsed() < timeout {
        let results = manager
            .get_requests(vec![request_id])
            .await
            .expect("Failed to get request");

        if let Some(Ok(any_request)) = results.first()
            && let fusillade::AnyRequest::Completed(req) = any_request
        {
            assert_eq!(req.state.response_status, 200);
            assert_eq!(
                req.state.response_body,
                r#"{"result":"success after retries"}"#
            );
            completed = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_token.cancel();

    assert!(completed, "Request should have completed after retries");

    assert_eq!(
        http_client.call_count(),
        3,
        "Expected 3 HTTP calls (2 failed attempts + 1 success)"
    );
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn test_batch_capacity_529_does_not_spend_a_retry_attempt(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());

    for _ in 0..3 {
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 529,
                body: r#"{"error":{"message":"at capacity","type":"overloaded_error","code":"batch_capacity_exceeded"}}"#
                    .to_string(),
            }),
        );
    }
    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            x_request_id: None,
            status: 200,
            body: r#"{"result":"success after capacity refusals"}"#.to_string(),
        }),
    );

    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("test-model".to_string(), 10);

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        model_concurrency_limits,
        max_retries: Some(0),
        stop_before_deadline_ms: None,
        backoff_ms: 5,
        backoff_factor: 1,
        max_backoff_ms: 5,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100,
        ..Default::default()
    };

    let manager = postgres_store(pool.clone(), &config).await;

    let file_id = manager
        .create_file(
            "test-file".to_string(),
            Some("batch capacity".to_string()),
            vec![fusillade::RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "test-model".to_string(),
            }],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: None,
            api_key_id: None,
            total_requests: None,
        })
        .await
        .expect("Failed to create batch");
    mark_models_live_for_test(manager.as_ref(), &["test-model"]).await;

    let requests = manager
        .get_batch_requests(batch.id)
        .await
        .expect("Failed to get batch requests");
    let request_id = requests[0].id();

    let shutdown_token = CancellationToken::new();
    postgres_daemon(manager.clone(), http_client.clone(), config)
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut completed = false;
    while start.elapsed() < timeout {
        let results = manager
            .get_requests(vec![request_id])
            .await
            .expect("get requests");
        if let Some(Ok(any_request)) = results.first() {
            match any_request {
                fusillade::AnyRequest::Completed(req) => {
                    assert_eq!(req.state.response_status, 200);
                    assert_eq!(
                        req.state.response_body,
                        r#"{"result":"success after capacity refusals"}"#
                    );
                    completed = true;
                    break;
                }
                fusillade::AnyRequest::Failed(_) => {
                    panic!("capacity refusal failed terminally instead of rescheduling");
                }
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    shutdown_token.cancel();
    assert!(
        completed,
        "Request should have completed after capacity opened"
    );
    assert_eq!(
        http_client.call_count(),
        4,
        "three refusals plus the success are four dispatch attempts"
    );
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn test_batch_capacity_529_reschedules_even_when_should_retry_rejects_529(
    pool: sqlx::PgPool,
) {
    let http_client = Arc::new(MockHttpClient::new());

    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            x_request_id: None,
            status: 529,
            body: r#"{"error":{"message":"at capacity","type":"overloaded_error","code":"batch_capacity_exceeded"}}"#
                .to_string(),
        }),
    );
    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            x_request_id: None,
            status: 200,
            body: r#"{"result":"success after capacity refusal"}"#.to_string(),
        }),
    );

    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("test-model".to_string(), 10);

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        model_concurrency_limits,
        max_retries: Some(0),
        stop_before_deadline_ms: None,
        backoff_ms: 5,
        backoff_factor: 1,
        max_backoff_ms: 5,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(|_| false),
        additional_retryable_statuses: vec![],
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100,
        ..Default::default()
    };

    let manager = postgres_store(pool.clone(), &config).await;

    let file_id = manager
        .create_file(
            "test-file".to_string(),
            Some("batch capacity predicate".to_string()),
            vec![fusillade::RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "test-model".to_string(),
            }],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: None,
            api_key_id: None,
            total_requests: None,
        })
        .await
        .expect("Failed to create batch");
    mark_models_live_for_test(manager.as_ref(), &["test-model"]).await;

    let requests = manager
        .get_batch_requests(batch.id)
        .await
        .expect("Failed to get batch requests");
    let request_id = requests[0].id();

    let shutdown_token = CancellationToken::new();
    postgres_daemon(manager.clone(), http_client.clone(), config)
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut completed = false;
    while start.elapsed() < timeout {
        let results = manager
            .get_requests(vec![request_id])
            .await
            .expect("get requests");
        if let Some(Ok(any_request)) = results.first() {
            match any_request {
                fusillade::AnyRequest::Completed(req) => {
                    assert_eq!(req.state.response_status, 200);
                    assert_eq!(
                        req.state.response_body,
                        r#"{"result":"success after capacity refusal"}"#
                    );
                    completed = true;
                    break;
                }
                fusillade::AnyRequest::Failed(_) => {
                    panic!("admission refusal failed terminally despite the retry predicate");
                }
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    shutdown_token.cancel();
    assert!(
        completed,
        "Request should have completed after capacity opened"
    );
    assert_eq!(http_client.call_count(), 2, "one refusal plus the success");
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn test_daemon_dynamically_updates_concurrency_limits(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());

    let mut triggers = vec![];
    for i in 1..=10 {
        let trigger = http_client.add_response_with_trigger(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: format!(r#"{{"result":"{}"}}"#, i),
            }),
        );
        triggers.push(trigger);
    }

    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("gpt-4".to_string(), 2);

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        model_concurrency_limits: model_concurrency_limits.clone(),
        max_retries: Some(3),
        stop_before_deadline_ms: None,
        backoff_ms: 100,
        backoff_factor: 2,
        max_backoff_ms: 1000,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100,
        ..Default::default()
    };

    let manager = postgres_store(pool.clone(), &config).await;

    let templates: Vec<_> = (1..=10)
        .map(|i| fusillade::RequestTemplateInput {
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/v1/test".to_string(),
            body: format!(r#"{{"prompt":"test{}"}}"#, i),
            model: "gpt-4".to_string(),
        })
        .collect();

    let file_id = manager
        .create_file(
            "test-file".to_string(),
            Some("Test dynamic limits".to_string()),
            templates,
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: None,
            api_key_id: None,
            total_requests: None,
        })
        .await
        .expect("Failed to create batch");
    mark_models_live_for_test(manager.as_ref(), &["gpt-4"]).await;

    let shutdown_token = CancellationToken::new();
    postgres_daemon(manager.clone(), http_client.clone(), config)
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(2);
    let mut reached_initial_limit = false;

    while start.elapsed() < timeout {
        let in_flight = http_client.in_flight_count();
        if in_flight == 2 {
            reached_initial_limit = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        reached_initial_limit,
        "Expected exactly 2 requests in-flight with initial limit"
    );

    model_concurrency_limits.insert("gpt-4".to_string(), 5);

    triggers.remove(0).send(()).unwrap();

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(2);
    let mut reached_new_limit = false;

    while start.elapsed() < timeout {
        let in_flight = http_client.in_flight_count();
        if in_flight >= 4 {
            reached_new_limit = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        reached_new_limit,
        "Expected more requests in-flight after limit increase, got {}",
        http_client.in_flight_count()
    );

    model_concurrency_limits.insert("gpt-4".to_string(), 3);

    for trigger in triggers {
        trigger.send(()).unwrap();
    }

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut all_completed = false;

    while start.elapsed() < timeout {
        let status = manager
            .get_batch_status(batch.id)
            .await
            .expect("Failed to get batch status");

        if status.completed_requests == 10 {
            all_completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_token.cancel();

    assert!(all_completed, "All 10 requests should have completed");
    assert_eq!(http_client.call_count(), 10);
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn test_deadline_aware_retry_stops_before_deadline(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());

    for _ in 0..20 {
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 500,
                body: r#"{"error":"server error"}"#.to_string(),
            }),
        );
    }

    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("test-model".to_string(), 10);

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        model_concurrency_limits,

        max_retries: Some(10_000),
        stop_before_deadline_ms: Some(500),
        backoff_ms: 50,
        backoff_factor: 2,
        max_backoff_ms: 200,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100,
        ..Default::default()
    };

    let manager = postgres_store(pool.clone(), &config).await;

    let file_id = manager
        .create_file(
            "test-file".to_string(),
            Some("Test deadline cutoff".to_string()),
            vec![fusillade::RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "test-model".to_string(),
            }],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "2s".to_string(),
            metadata: None,
            created_by: None,
            api_key_id: None,
            total_requests: None,
        })
        .await
        .expect("Failed to create batch");
    mark_models_live_for_test(manager.as_ref(), &["test-model"]).await;

    let requests = manager
        .get_batch_requests(batch.id)
        .await
        .expect("Failed to get batch requests");
    let request_id = requests[0].id();

    let shutdown_token = CancellationToken::new();
    postgres_daemon(manager.clone(), http_client.clone(), config.clone())
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut results = None;

    while start.elapsed() < timeout {
        let res = manager
            .get_requests(vec![request_id])
            .await
            .expect("Failed to get request");

        if let Some(Ok(req)) = res.first()
            && req.is_terminal()
        {
            results = Some(res);
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_token.cancel();

    let results = results.expect("Request should have reached terminal state within timeout");

    if let Some(Ok(fusillade::AnyRequest::Failed(failed))) = results.first() {
        let retry_count = failed.state.retry_attempt;
        let call_count = http_client.call_count();

        assert_next_retry_would_cross_effective_deadline(failed, &config);
        assert!(
            retry_count > 0,
            "Expected at least one retry attempt before the buffered deadline, got {}",
            retry_count
        );

        assert_eq!(
            call_count,
            (retry_count + 1) as usize,
            "Expected call count to match retry attempts + 1 initial attempt, got {} calls for {} retry attempts",
            call_count,
            retry_count
        );

        assert!(
            !failed.state.reason.to_error_message().is_empty(),
            "Expected failed request to have failure reason"
        );
    } else {
        panic!(
            "Expected request to be in Failed state, got {:?}",
            results.first()
        );
    }
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn test_retry_stops_at_deadline_when_no_limits_set(pool: sqlx::PgPool) {
    let http_client = Arc::new(MockHttpClient::new());

    for _ in 0..20 {
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 500,
                body: r#"{"error":"server error"}"#.to_string(),
            }),
        );
    }

    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("test-model".to_string(), 10);

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        model_concurrency_limits,

        max_retries: None,
        stop_before_deadline_ms: None,
        backoff_ms: 50,
        backoff_factor: 2,
        max_backoff_ms: 200,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100,
        ..Default::default()
    };

    let manager = postgres_store(pool.clone(), &config).await;

    let file_id = manager
        .create_file(
            "test-file".to_string(),
            Some("Test no limits retry".to_string()),
            vec![fusillade::RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "test-model".to_string(),
            }],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "2s".to_string(),
            metadata: None,
            created_by: None,
            api_key_id: None,
            total_requests: None,
        })
        .await
        .expect("Failed to create batch");
    mark_models_live_for_test(manager.as_ref(), &["test-model"]).await;

    let requests = manager
        .get_batch_requests(batch.id)
        .await
        .expect("Failed to get batch requests");
    let request_id = requests[0].id();

    let shutdown_token = CancellationToken::new();
    postgres_daemon(manager.clone(), http_client.clone(), config.clone())
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut results = None;

    while start.elapsed() < timeout {
        let res = manager
            .get_requests(vec![request_id])
            .await
            .expect("Failed to get request");

        if let Some(Ok(req)) = res.first()
            && req.is_terminal()
        {
            results = Some(res);
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_token.cancel();

    let results = results.expect("Request should have reached terminal state within timeout");

    if let Some(Ok(fusillade::AnyRequest::Failed(failed))) = results.first() {
        let retry_count = failed.state.retry_attempt;
        let call_count = http_client.call_count();

        assert_next_retry_would_cross_effective_deadline(failed, &config);
        assert!(
            retry_count > 0,
            "Expected at least one retry attempt before the deadline, got {}",
            retry_count
        );

        assert_eq!(
            call_count,
            (retry_count + 1) as usize,
            "Expected call count to match retry attempts + 1 initial attempt, got {} calls for {} retry attempts",
            call_count,
            retry_count
        );

        assert!(
            !failed.state.reason.to_error_message().is_empty(),
            "Expected failed request to have failure reason"
        );
    } else {
        panic!(
            "Expected request to be in Failed state, got {:?}",
            results.first()
        );
    }
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn test_route_at_claim_time_escalation(pool: sqlx::PgPool) {

    let http_client = Arc::new(MockHttpClient::new());

    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            x_request_id: None,
            status: 200,
            body: r#"{"result":"escalated response"}"#.to_string(),
        }),
    );

    let model_escalations = Arc::new(dashmap::DashMap::new());
    model_escalations.insert(
        "gpt-4".to_string(),
        ModelEscalationConfig {
            escalation_model: "gpt-4-turbo".to_string(),
            escalation_threshold_seconds: 7200,
        },
    );

    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("gpt-4".to_string(), 10);
    model_concurrency_limits.insert("gpt-4-turbo".to_string(), 10);

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        model_concurrency_limits,

        model_escalations,
        max_retries: Some(3),
        stop_before_deadline_ms: None,
        backoff_ms: 100,
        backoff_factor: 2,
        max_backoff_ms: 1000,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100,
        ..Default::default()
    };

    let manager = postgres_store(pool.clone(), &config).await;

    let file_id = manager
        .create_file(
            "test-escalation".to_string(),
            Some("Test route-at-claim-time escalation".to_string()),
            vec![fusillade::RequestTemplateInput {
                custom_id: Some("escalation-test".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "gpt-4".to_string(),
            }],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "1h".to_string(),
            metadata: None,
            created_by: None,
            api_key_id: None,
            total_requests: None,
        })
        .await
        .expect("Failed to create batch");
    mark_models_live_for_test(manager.as_ref(), &["gpt-4"]).await;

    let requests = manager
        .get_batch_requests(batch.id)
        .await
        .expect("Failed to get batch requests");
    assert_eq!(requests.len(), 1);
    let request_id = requests[0].id();

    let shutdown_token = CancellationToken::new();
    postgres_daemon(manager.clone(), http_client.clone(), config)
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut completed = false;

    while start.elapsed() < timeout {
        let results = manager
            .get_requests(vec![request_id])
            .await
            .expect("Failed to get request");

        if let Some(Ok(any_request)) = results.first()
            && any_request.is_terminal()
        {
            if let fusillade::AnyRequest::Completed(req) = any_request {
                assert_eq!(req.state.response_status, 200);
                assert_eq!(
                    req.state.response_body,
                    r#"{"result":"escalated response"}"#
                );
                completed = true;
                break;
            } else {
                panic!(
                    "Request reached terminal state but was not completed: {:?}",
                    any_request
                );
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_token.cancel();

    assert!(completed, "Request did not complete within timeout");

    assert_eq!(http_client.call_count(), 1);
    let calls = http_client.get_calls();
    assert_eq!(calls[0].api_key, "", "no stored key reaches the dispatch");
}

#[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
async fn test_route_at_claim_time_no_escalation_when_enough_time(pool: sqlx::PgPool) {

    let http_client = Arc::new(MockHttpClient::new());

    http_client.add_response(
        "POST /v1/test",
        Ok(HttpResponse {
            x_request_id: None,
            status: 200,
            body: r#"{"result":"normal response"}"#.to_string(),
        }),
    );

    let model_escalations = Arc::new(dashmap::DashMap::new());
    model_escalations.insert(
        "gpt-4".to_string(),
        ModelEscalationConfig {
            escalation_model: "gpt-4-turbo".to_string(),
            escalation_threshold_seconds: 60,
        },
    );

    let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
    model_concurrency_limits.insert("gpt-4".to_string(), 10);
    model_concurrency_limits.insert("gpt-4-turbo".to_string(), 10);

    let config = DaemonConfig {
        claim_batch_size: 10,
        claim_interval_ms: 10,
        model_concurrency_limits,

        model_escalations,
        max_retries: Some(3),
        stop_before_deadline_ms: None,
        backoff_ms: 100,
        backoff_factor: 2,
        max_backoff_ms: 1000,
        status_log_interval_ms: None,
        heartbeat_interval_ms: 10000,
        should_retry: Arc::new(default_should_retry),
        claim_timeout_ms: 60000,
        processing_timeout_ms: 600000,
        cancellation_poll_interval_ms: 100,
        ..Default::default()
    };

    let manager = postgres_store(pool.clone(), &config).await;

    let file_id = manager
        .create_file(
            "test-no-escalation".to_string(),
            Some("Test no escalation when enough time".to_string()),
            vec![fusillade::RequestTemplateInput {
                custom_id: Some("no-escalation-test".to_string()),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "gpt-4".to_string(),
            }],
        )
        .await
        .expect("Failed to create file");

    let batch = manager
        .create_batch(fusillade::batch::BatchInput {
            file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            created_by: None,
            api_key_id: None,
            total_requests: None,
        })
        .await
        .expect("Failed to create batch");
    mark_models_live_for_test(manager.as_ref(), &["gpt-4"]).await;

    let requests = manager
        .get_batch_requests(batch.id)
        .await
        .expect("Failed to get batch requests");
    let request_id = requests[0].id();

    let shutdown_token = CancellationToken::new();
    postgres_daemon(manager.clone(), http_client.clone(), config)
        .run(shutdown_token.clone())
        .expect("Failed to start daemon");

    let start = tokio::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut completed = false;

    while start.elapsed() < timeout {
        let results = manager
            .get_requests(vec![request_id])
            .await
            .expect("Failed to get request");

        if let Some(Ok(any_request)) = results.first()
            && any_request.is_terminal()
        {
            completed = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_token.cancel();

    assert!(completed, "Request did not complete within timeout");

    assert_eq!(http_client.call_count(), 1);
    let calls = http_client.get_calls();
    assert_eq!(calls[0].api_key, "", "no stored key reaches the dispatch");
}

mod batch_results_stream {
    use super::*;
    use futures::StreamExt;

    async fn collect_batch_results(
        manager: &PostgresStore<TestDbPools>,
        batch_id: fusillade::batch::BatchId,
    ) -> Vec<fusillade::batch::BatchResultItem> {
        let stream = manager.get_batch_results_stream(batch_id, 0, None, None);
        stream
            .filter_map(|r| async { r.ok() })
            .collect::<Vec<_>>()
            .await
    }

    #[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
    async fn test_batch_results_basic(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: r#"{"result":"success1"}"#.to_string(),
            }),
        );
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: r#"{"result":"success2"}"#.to_string(),
            }),
        );

        let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
        model_concurrency_limits.insert("gpt-4".to_string(), 10);

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            model_concurrency_limits,
            ..Default::default()
        };

        let manager = postgres_store(pool.clone(), &config).await;

        let file_id = manager
            .create_file(
                "test-results".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("req-1".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test1"}"#.to_string(),
                        model: "gpt-4".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("req-2".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test2"}"#.to_string(),
                        model: "gpt-4".to_string(),
                    },
                ],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .expect("Failed to create batch");
        mark_models_live_for_test(manager.as_ref(), &["gpt-4"]).await;

        let shutdown_token = CancellationToken::new();
        postgres_daemon(manager.clone(), http_client.clone(), config)
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");
            if requests.iter().all(|r| r.is_terminal()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_token.cancel();

        let results = collect_batch_results(&manager, batch.id).await;

        assert_eq!(results.len(), 2, "Should have 2 results");

        let custom_ids: Vec<_> = results
            .iter()
            .filter_map(|r| r.custom_id.as_ref())
            .collect();
        assert!(custom_ids.contains(&&"req-1".to_string()));
        assert!(custom_ids.contains(&&"req-2".to_string()));

        for result in &results {
            assert_eq!(
                result.status,
                fusillade::batch::BatchResultStatus::Completed
            );
            assert!(result.response_body.is_some());
        }
    }

    #[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
    async fn test_batch_results_streamable_after_input_file_deleted(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: r#"{"result":"success"}"#.to_string(),
            }),
        );

        let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
        model_concurrency_limits.insert("gpt-4".to_string(), 10);

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            model_concurrency_limits,
            ..Default::default()
        };

        let manager = postgres_store(pool.clone(), &config).await;

        let file_id = manager
            .create_file(
                "to-delete".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("test".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test"}"#.to_string(),
                    model: "gpt-4".to_string(),
                }],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .expect("Failed to create batch");
        mark_models_live_for_test(manager.as_ref(), &["gpt-4"]).await;

        let shutdown_token = CancellationToken::new();
        postgres_daemon(manager.clone(), http_client.clone(), config)
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");
            if requests.iter().all(|r| r.is_terminal()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_token.cancel();

        let stream = manager.get_batch_results_stream(batch.id, 0, None, None);
        let results: Vec<_> = stream.collect().await;
        assert_eq!(
            results.len(),
            1,
            "Should have 1 result before file deletion"
        );
        let result = results[0]
            .as_ref()
            .expect("Result should be Ok before file deletion");
        assert_eq!(
            result.status,
            fusillade::batch::BatchResultStatus::Completed
        );
        assert!(result.response_body.is_some());

        sqlx::query!(
            "UPDATE batches SET file_id = NULL WHERE id = $1",
            *batch.id as uuid::Uuid
        )
        .execute(&pool)
        .await
        .expect("Failed to clear file_id");

        let stream = manager.get_batch_results_stream(batch.id, 0, None, None);
        let results: Vec<_> = stream.collect().await;
        assert_eq!(results.len(), 1, "Should have 1 result after file deletion");
        let result = results[0]
            .as_ref()
            .expect("Result should be Ok after input file deletion");
        assert_eq!(
            result.status,
            fusillade::batch::BatchResultStatus::Completed
        );
        assert!(result.response_body.is_some());
        assert_eq!(result.custom_id.as_deref(), Some("test"));
    }

    #[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
    async fn test_generated_files_unavailable_after_batch_deleted(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: r#"{"result":"success"}"#.to_string(),
            }),
        );

        let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
        model_concurrency_limits.insert("gpt-4".to_string(), 10);

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            model_concurrency_limits,
            ..Default::default()
        };

        let manager = postgres_store(pool.clone(), &config).await;

        let file_id = manager
            .create_file(
                "output-after-delete".to_string(),
                None,
                vec![RequestTemplateInput {
                    custom_id: Some("test".to_string()),
                    endpoint: "https://api.example.com".to_string(),
                    method: "POST".to_string(),
                    path: "/v1/test".to_string(),
                    body: r#"{"prompt":"test"}"#.to_string(),
                    model: "gpt-4".to_string(),
                }],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .expect("Failed to create batch");
        mark_models_live_for_test(manager.as_ref(), &["gpt-4"]).await;

        let shutdown_token = CancellationToken::new();
        postgres_daemon(manager.clone(), http_client.clone(), config)
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let b = manager.get_batch(batch.id).await.expect("get_batch failed");
            if b.completed_requests == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        shutdown_token.cancel();

        let batch = manager.get_batch(batch.id).await.expect("get_batch failed");
        let output_file_id = batch
            .output_file_id
            .expect("Batch should have output_file_id");
        let error_file_id = batch
            .error_file_id
            .expect("Batch should have error_file_id");

        manager
            .get_file(output_file_id)
            .await
            .expect("Output file should exist before deletion");
        manager
            .get_file(error_file_id)
            .await
            .expect("Error file should exist before deletion");
        let results_before = manager
            .get_file_content(output_file_id)
            .await
            .expect("Should be able to get output file content before deletion");
        assert!(
            !results_before.is_empty(),
            "Output file should have content before deletion"
        );

        manager
            .get_file_content(error_file_id)
            .await
            .expect("Should be able to read error file content before deletion");

        manager
            .delete_batch(batch.id)
            .await
            .expect("delete_batch failed");

        let batch_result = manager.get_batch(batch.id).await;
        assert!(
            batch_result.is_err(),
            "Batch should not be found after deletion"
        );

        assert!(manager.get_file(output_file_id).await.is_err());
        assert!(manager.get_file(error_file_id).await.is_err());
        assert!(manager.get_file_content(output_file_id).await.is_err());
        assert!(manager.get_file_content(error_file_id).await.is_err());
    }

    #[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
    async fn test_batch_results_pagination(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        for i in 0..5 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    x_request_id: None,
                    status: 200,
                    body: format!(r#"{{"result":"success{}"}}"#, i),
                }),
            );
        }

        let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
        model_concurrency_limits.insert("gpt-4".to_string(), 10);

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            model_concurrency_limits,
            ..Default::default()
        };

        let manager = postgres_store(pool.clone(), &config).await;

        let templates: Vec<_> = (0..5)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("req-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: format!(r#"{{"prompt":"test{}"}}"#, i),
                model: "gpt-4".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file("pagination-test".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .expect("Failed to create batch");
        mark_models_live_for_test(manager.as_ref(), &["gpt-4"]).await;

        let shutdown_token = CancellationToken::new();
        postgres_daemon(manager.clone(), http_client.clone(), config)
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");
            if requests.iter().all(|r| r.is_terminal()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_token.cancel();

        let all_results = collect_batch_results(&manager, batch.id).await;
        assert_eq!(all_results.len(), 5, "Should have 5 results");

        let stream = manager.get_batch_results_stream(batch.id, 2, None, None);
        let offset_results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(
            offset_results.len(),
            3,
            "Should have 3 results with offset 2"
        );

        let offset_ids: Vec<_> = offset_results
            .iter()
            .filter_map(|r| r.custom_id.as_ref())
            .collect();
        assert!(offset_ids.contains(&&"req-2".to_string()));
        assert!(offset_ids.contains(&&"req-3".to_string()));
        assert!(offset_ids.contains(&&"req-4".to_string()));
    }

    #[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
    async fn test_batch_results_status_filter(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: r#"{"result":"success1"}"#.to_string(),
            }),
        );
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 200,
                body: r#"{"result":"success2"}"#.to_string(),
            }),
        );
        http_client.add_response(
            "POST /v1/test",
            Ok(HttpResponse {
                x_request_id: None,
                status: 500,
                body: r#"{"error":"server error"}"#.to_string(),
            }),
        );

        let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
        model_concurrency_limits.insert("gpt-4".to_string(), 10);

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            model_concurrency_limits,
            max_retries: Some(0),
            ..Default::default()
        };

        let manager = postgres_store(pool.clone(), &config).await;

        let templates: Vec<_> = (0..3)
            .map(|i| RequestTemplateInput {
                custom_id: Some(format!("req-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: format!(r#"{{"prompt":"test{}"}}"#, i),
                model: "gpt-4".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file("filter-test".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .expect("Failed to create batch");
        mark_models_live_for_test(manager.as_ref(), &["gpt-4"]).await;

        let shutdown_token = CancellationToken::new();
        postgres_daemon(manager.clone(), http_client.clone(), config)
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");
            if requests.iter().all(|r| r.is_terminal()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_token.cancel();

        let stream =
            manager.get_batch_results_stream(batch.id, 0, None, Some("completed".to_string()));
        let completed_results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(
            completed_results.len(),
            2,
            "Should have 2 completed results"
        );
        for r in &completed_results {
            assert_eq!(r.status, fusillade::batch::BatchResultStatus::Completed);
        }

        let stream =
            manager.get_batch_results_stream(batch.id, 0, None, Some("failed".to_string()));
        let failed_results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(failed_results.len(), 1, "Should have 1 failed result");
        assert_eq!(
            failed_results[0].status,
            fusillade::batch::BatchResultStatus::Failed
        );
    }

    #[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
    async fn test_batch_results_search_filter(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());

        for _ in 0..3 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    x_request_id: None,
                    status: 200,
                    body: r#"{"result":"success"}"#.to_string(),
                }),
            );
        }

        let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
        model_concurrency_limits.insert("gpt-4".to_string(), 10);

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            model_concurrency_limits,
            ..Default::default()
        };

        let manager = postgres_store(pool.clone(), &config).await;

        let file_id = manager
            .create_file(
                "search-test".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: Some("Alpha-Request".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test"}"#.to_string(),
                        model: "gpt-4".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("Beta-Request".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test"}"#.to_string(),
                        model: "gpt-4".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: Some("Gamma-Item".to_string()),
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: r#"{"prompt":"test"}"#.to_string(),
                        model: "gpt-4".to_string(),
                    },
                ],
            )
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .expect("Failed to create batch");
        mark_models_live_for_test(manager.as_ref(), &["gpt-4"]).await;

        let shutdown_token = CancellationToken::new();
        postgres_daemon(manager.clone(), http_client.clone(), config)
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let requests = manager
                .get_batch_requests(batch.id)
                .await
                .expect("Failed to get requests");
            if requests.iter().all(|r| r.is_terminal()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown_token.cancel();

        let stream =
            manager.get_batch_results_stream(batch.id, 0, Some("request".to_string()), None);
        let search_results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(
            search_results.len(),
            2,
            "Should find 2 results containing 'request'"
        );

        let custom_ids: Vec<_> = search_results
            .iter()
            .filter_map(|r| r.custom_id.as_ref())
            .collect();
        assert!(custom_ids.contains(&&"Alpha-Request".to_string()));
        assert!(custom_ids.contains(&&"Beta-Request".to_string()));

        let stream = manager.get_batch_results_stream(batch.id, 0, Some("ALPHA".to_string()), None);
        let alpha_results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(alpha_results.len(), 1, "Should find 1 result for 'ALPHA'");
        assert_eq!(
            alpha_results[0].custom_id,
            Some("Alpha-Request".to_string())
        );
    }

    #[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
    #[test_log::test]
    async fn test_retry_failed_requests_for_batch_retries_all(pool: sqlx::PgPool) {

        let http_client = Arc::new(MockHttpClient::new());

        for _ in 0..2 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    x_request_id: None,
                    status: 429,
                    body: "rate limited".to_string(),
                }),
            );
        }
        for _ in 0..2 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    x_request_id: None,
                    status: 400,
                    body: "bad request".to_string(),
                }),
            );
        }

        for _ in 0..4 {
            http_client.add_response(
                "POST /v1/test",
                Ok(HttpResponse {
                    x_request_id: None,
                    status: 200,
                    body: r#"{"result":"success"}"#.to_string(),
                }),
            );
        }

        let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
        model_concurrency_limits.insert("test-model".to_string(), 10);

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            model_concurrency_limits,

            max_retries: Some(0),
            stop_before_deadline_ms: None,
            backoff_ms: 100,
            backoff_factor: 2,
            max_backoff_ms: 1000,
            status_log_interval_ms: None,
            heartbeat_interval_ms: 10000,
            should_retry: Arc::new(default_should_retry),
            claim_timeout_ms: 60000,
            processing_timeout_ms: 600000,
            cancellation_poll_interval_ms: 100,
            ..Default::default()
        };

        let manager = postgres_store(pool.clone(), &config).await;

        let templates = (0..4)
            .map(|i| fusillade::RequestTemplateInput {
                custom_id: Some(format!("req-{}", i)),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: r#"{"test":"data"}"#.to_string(),
                model: "test-model".to_string(),
            })
            .collect();

        let file_id = manager
            .create_file("test-file".to_string(), None, templates)
            .await
            .expect("Failed to create file");

        let batch = manager
            .create_batch(fusillade::batch::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .expect("Failed to create batch");
        mark_models_live_for_test(manager.as_ref(), &["test-model"]).await;

        let shutdown_token = CancellationToken::new();
        postgres_daemon(manager.clone(), http_client.clone(), config)
            .run(shutdown_token.clone())
            .expect("Failed to start daemon");

        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let status = manager
                .get_batch_status(batch.id)
                .await
                .expect("Failed to get status");
            if status.failed_requests == 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let status_before = manager
            .get_batch_status(batch.id)
            .await
            .expect("Failed to get status");
        assert_eq!(status_before.failed_requests, 4);

        let retried_count = manager
            .retry_failed_requests_for_batch(batch.id)
            .await
            .expect("Failed to retry batch");
        assert_eq!(retried_count, 4, "Should retry all 4 failed requests");

        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let status = manager
                .get_batch_status(batch.id)
                .await
                .expect("Failed to get status");
            if status.completed_requests == 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        shutdown_token.cancel();

        let status_after = manager
            .get_batch_status(batch.id)
            .await
            .expect("Failed to get status");
        assert_eq!(
            status_after.completed_requests, 4,
            "All 4 requests should complete after retry"
        );
        assert_eq!(status_after.failed_requests, 0, "No failed requests remain");
    }
}

mod queue_counts {
    use super::*;
    use fusillade::request::DaemonId;
    use uuid::Uuid;

    #[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
    async fn test_pending_queue_counts_by_model_and_completion_window(pool: sqlx::PgPool) {
        let http_client = Arc::new(MockHttpClient::new());
        let manager = Arc::new(PostgresStore::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            http_client,
        ));

        let file_24h = manager
            .create_file(
                "file-24h".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: "{}".to_string(),
                        model: "gpt-4".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: "{}".to_string(),
                        model: "gpt-4".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: "{}".to_string(),
                        model: "gpt-3.5".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let batch_24h = manager
            .create_batch(BatchInput {
                file_id: file_24h,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let file_1h = manager
            .create_file(
                "file-1h".to_string(),
                None,
                vec![
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: "{}".to_string(),
                        model: "gpt-4".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: "{}".to_string(),
                        model: "gpt-3.5".to_string(),
                    },
                    RequestTemplateInput {
                        custom_id: None,
                        endpoint: "https://api.example.com".to_string(),
                        method: "POST".to_string(),
                        path: "/v1/test".to_string(),
                        body: "{}".to_string(),
                        model: "gpt-3.5".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        let _batch_1h = manager
            .create_batch(BatchInput {
                file_id: file_1h,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();

        let reqs_24h = manager.get_batch_requests(batch_24h.id).await.unwrap();
        let to_claim = reqs_24h
            .iter()
            .find(|r| r.data().model == "gpt-4")
            .expect("Expected a gpt-4 request")
            .id();
        let daemon_id = DaemonId(Uuid::new_v4());
        sqlx::query!(
            "UPDATE requests SET state = 'claimed', daemon_id = $2, claimed_at = NOW() WHERE id = $1",
            *to_claim as Uuid,
            *daemon_id as Uuid,
        )
        .execute(&pool)
        .await
        .unwrap();

        let counts = manager
            .get_pending_request_counts_by_model_and_window(
                &[
                    ("1h".to_string(), None, 3600),
                    ("24h".to_string(), None, 24 * 3600),
                ],
                &["pending".to_string()],
                &[],
                &ServiceTierFilter::Any,
                false,
            )
            .await
            .unwrap();

        let mut expected: std::collections::HashMap<
            String,
            std::collections::HashMap<String, i64>,
        > = std::collections::HashMap::new();
        expected
            .entry("gpt-3.5".to_string())
            .or_default()
            .insert("1h".to_string(), 2);
        expected
            .entry("gpt-3.5".to_string())
            .or_default()
            .insert("24h".to_string(), 3);
        expected
            .entry("gpt-4".to_string())
            .or_default()
            .insert("1h".to_string(), 1);
        expected
            .entry("gpt-4".to_string())
            .or_default()
            .insert("24h".to_string(), 2);

        assert_eq!(counts, expected);
    }
}

mod service_tier {
    use super::*;

    #[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
    #[test_log::test]
    async fn test_populate_batch_sets_service_tier(pool: sqlx::PgPool) {
        let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
        model_concurrency_limits.insert("test-model".to_string(), 10);

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            model_concurrency_limits,
            ..Default::default()
        };

        let manager = postgres_store(pool.clone(), &config).await;

        let template = RequestTemplateInput {
            custom_id: None,
            endpoint: "https://api.example.com".to_string(),
            method: "POST".to_string(),
            path: "/v1/test".to_string(),
            body: r#"{"prompt":"test"}"#.to_string(),
            model: "test-model".to_string(),
        };

        let file_id_1h = manager
            .create_file("file-1h".to_string(), None, vec![template.clone()])
            .await
            .unwrap();
        let batch_1h = manager
            .create_batch(BatchInput {
                file_id: file_id_1h,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "1h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();
        let requests_1h = manager.get_batch_requests(batch_1h.id).await.unwrap();
        assert_eq!(requests_1h.len(), 1);
        let tier_1h: Option<String> =
            sqlx::query_scalar("SELECT service_tier FROM requests WHERE batch_id = $1")
                .bind(*batch_1h.id as uuid::Uuid)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(tier_1h, Some("flex".to_string()));

        let file_id_24h = manager
            .create_file("file-24h".to_string(), None, vec![template.clone()])
            .await
            .unwrap();
        let batch_24h = manager
            .create_batch(BatchInput {
                file_id: file_id_24h,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();
        let requests_24h = manager.get_batch_requests(batch_24h.id).await.unwrap();
        assert_eq!(requests_24h.len(), 1);
        let tier_24h: Option<String> =
            sqlx::query_scalar("SELECT service_tier FROM requests WHERE batch_id = $1")
                .bind(*batch_24h.id as uuid::Uuid)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(tier_24h, None);
    }

    #[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
    #[test_log::test]
    async fn test_list_requests_filters_by_service_tier(pool: sqlx::PgPool) {
        let model_concurrency_limits = Arc::new(dashmap::DashMap::new());
        model_concurrency_limits.insert("test-model".to_string(), 10);

        let config = DaemonConfig {
            claim_batch_size: 10,
            claim_interval_ms: 10,
            model_concurrency_limits,
            ..Default::default()
        };

        let manager = postgres_store(pool.clone(), &config).await;

        manager
            .create_flex(fusillade::CreateFlexInput {
                request_id: uuid::Uuid::new_v4(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "test-model".to_string(),
                endpoint: "http://localhost".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: "tier-user".to_string(),
                metadata: None,
            })
            .await
            .unwrap();
        manager
            .create_realtime(fusillade::CreateRealtimeInput {
                request_id: uuid::Uuid::new_v4(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "test-model".to_string(),
                endpoint: "http://localhost".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "tier-user".to_string(),
            })
            .await
            .unwrap();

        let flex_result = manager
            .list_requests(ListRequestsFilter {
                service_tiers: Some(vec!["flex".to_string()]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(flex_result.data.len(), 1);
        assert_eq!(flex_result.data[0].service_tier, Some("flex".to_string()));

        let all_result = manager
            .list_requests(ListRequestsFilter::default())
            .await
            .unwrap();
        assert_eq!(all_result.data.len(), 2);
    }
}

mod unverified_volume_counts {
    use super::*;

    async fn seed_batch(
        manager: &PostgresStore<TestDbPools>,
        creditor: &str,
        completion_window: &str,
        n: usize,
    ) {
        let templates: Vec<RequestTemplateInput> = (0..n)
            .map(|_| RequestTemplateInput {
                custom_id: None,
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/test".to_string(),
                body: "{}".to_string(),
                model: "test-model".to_string(),
            })
            .collect();
        let file_id = manager
            .create_file(
                format!("f-{creditor}-{completion_window}-{n}"),
                None,
                templates,
            )
            .await
            .unwrap();
        manager
            .create_batch(BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: completion_window.to_string(),
                metadata: None,
                created_by: Some(creditor.to_string()),
                api_key_id: None,
                total_requests: None,
            })
            .await
            .unwrap();
    }

    async fn seed_flex(manager: &PostgresStore<TestDbPools>, creditor: &str) {
        manager
            .create_flex(fusillade::CreateFlexInput {
                request_id: uuid::Uuid::new_v4(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "test-model".to_string(),
                endpoint: "http://localhost".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                api_key_id: uuid::Uuid::new_v4(),
                created_by: creditor.to_string(),
                metadata: None,
            })
            .await
            .unwrap();
    }

    #[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
    #[test_log::test]
    async fn test_sum_owner_batch_requests_in_window(pool: sqlx::PgPool) {
        let manager = PostgresStore::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        seed_batch(&manager, "user-a", "24h", 3).await;
        seed_batch(&manager, "user-a", "1h", 2).await;
        seed_batch(&manager, "user-b", "24h", 5).await;

        let past = chrono::Utc::now() - chrono::Duration::hours(48);
        let future = chrono::Utc::now() + chrono::Duration::hours(1);

        assert_eq!(
            manager
                .sum_owner_batch_requests_in_window("user-a", "24h", past, true)
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            manager
                .sum_owner_batch_requests_in_window("user-a", "1h", past, true)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            manager
                .sum_owner_batch_requests_in_window("user-b", "24h", past, true)
                .await
                .unwrap(),
            5,
            "other creditors must not bleed into the count"
        );

        assert_eq!(
            manager
                .sum_owner_batch_requests_in_window("user-a", "24h", future, true)
                .await
                .unwrap(),
            0,
            "created_at < cutoff must be excluded"
        );

        assert_eq!(
            manager
                .sum_owner_batch_requests_in_window("nobody", "24h", past, true)
                .await
                .unwrap(),
            0
        );
    }

    #[sqlx::test(migrator = "fusillade_arsenal::MIGRATOR")]
    #[test_log::test]
    async fn test_count_owner_flex_requests_since(pool: sqlx::PgPool) {
        let manager = PostgresStore::with_client(
            TestDbPools::new(pool.clone()).await.unwrap(),
            Arc::new(MockHttpClient::new()),
        );

        seed_flex(&manager, "user-a").await;
        seed_flex(&manager, "user-a").await;
        manager
            .create_realtime(fusillade::CreateRealtimeInput {
                request_id: uuid::Uuid::new_v4(),
                body: r#"{"prompt":"test"}"#.to_string(),
                model: "test-model".to_string(),
                endpoint: "http://localhost".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                created_by: "user-a".to_string(),
            })
            .await
            .unwrap();
        seed_batch(&manager, "user-a", "1h", 4).await;
        seed_flex(&manager, "user-b").await;

        let past = chrono::Utc::now() - chrono::Duration::hours(48);
        let future = chrono::Utc::now() + chrono::Duration::hours(1);

        assert_eq!(
            manager
                .count_owner_flex_requests_since("user-a", past, true)
                .await
                .unwrap(),
            2,
            "counts only batchless flex rows: excludes realtime, batched 1h, and other creditors"
        );
        assert_eq!(
            manager
                .count_owner_flex_requests_since("user-b", past, true)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            manager
                .count_owner_flex_requests_since("user-a", future, true)
                .await
                .unwrap(),
            0,
            "created_at < cutoff must be excluded"
        );

        for (limit, expected) in [(5, 2), (2, 2), (1, 1), (0, 0), (-3, 0)] {
            assert_eq!(
                manager
                    .count_owner_flex_requests_since_capped("user-a", past, true, limit)
                    .await
                    .unwrap(),
                expected,
                "capped count with limit {limit}"
            );
        }
    }
}
