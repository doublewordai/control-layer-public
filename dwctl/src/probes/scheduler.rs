
use crate::auth::internal_credential::InternalCredentials;
use crate::metrics::errors::component::PROBE_SCHEDULER;
use crate::probes::db::ProbeManager;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone)]
pub struct ProbeScheduler {
    pool: sqlx_pool_router::DynPools,
    listener_pool: sqlx_pool_router::DynPools,
    config: crate::config::Config,
    credentials: InternalCredentials,
    schedulers: Arc<RwLock<HashMap<Uuid, JoinHandle<()>>>>,
}

impl ProbeScheduler {
    pub fn new(pool: impl sqlx_pool_router::PoolProvider, config: crate::config::Config, credentials: InternalCredentials) -> Self {
        let pool = sqlx_pool_router::DynPools::new(pool);
        Self {
            listener_pool: pool.clone(),
            pool,
            config,
            credentials,
            schedulers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn with_listener_pool(mut self, listener_pool: impl sqlx_pool_router::PoolProvider) -> Self {
        self.listener_pool = sqlx_pool_router::DynPools::new(listener_pool);
        self
    }

    pub async fn initialize(&self, shutdown_token: CancellationToken) -> Result<(), anyhow::Error> {
        if shutdown_token.is_cancelled() {
            tracing::info!("Shutdown signal received, skipping scheduler initialization");
            return Ok(());
        }

        let probes = ProbeManager::list_active_probes(&self.pool.write()).await?;

        tracing::info!("Initializing schedulers for {} active probes", probes.len());

        for probe in probes {
            if shutdown_token.is_cancelled() {
                tracing::info!("Shutdown signal received during initialization, stopping");
                break;
            }
            self.start_scheduler(probe.id, shutdown_token.clone()).await?;
        }

        Ok(())
    }

    async fn start_scheduler(&self, probe_id: Uuid, shutdown_token: CancellationToken) -> Result<(), anyhow::Error> {
        {
            let schedulers = self.schedulers.read().await;
            if schedulers.contains_key(&probe_id) {
                return Ok(());
            }
        }

        let pool = self.pool.clone();
        let config = self.config.clone();
        let credentials = self.credentials.clone();

        let handle = tokio::spawn(async move {
            let _should_delay = match ProbeManager::get_recent_results(&pool.write(), probe_id, 1).await {
                Ok(results) => {
                    if let Some(last_result) = results.first() {
                        let probe = match ProbeManager::get_probe(&pool.write(), probe_id).await {
                            Ok(p) => p,
                            Err(e) => {
                                crate::background_error!(
                                    PROBE_SCHEDULER,
                                    "probe_fetch",
                                    Warning,
                                    "Error fetching probe {}: {}",
                                    probe_id,
                                    e
                                );
                                return;
                            }
                        };

                        let now = chrono::Utc::now();
                        let time_since_last = now - last_result.executed_at;
                        let interval = chrono::Duration::seconds(probe.interval_seconds as i64);

                        if time_since_last < interval {
                            let wait_duration = interval - time_since_last;
                            let wait_secs = wait_duration.num_seconds().max(0) as u64;

                            tracing::info!(
                                "Probe {} last executed {}s ago, waiting {}s until next execution",
                                probe.name,
                                time_since_last.num_seconds(),
                                wait_secs
                            );

                            tokio::time::sleep(tokio::time::Duration::from_secs(wait_secs)).await;
                            false
                        } else {
                            tracing::info!(
                                "Probe {} last executed {}s ago (>{}s interval), executing immediately",
                                probe.name,
                                time_since_last.num_seconds(),
                                probe.interval_seconds
                            );
                            true
                        }
                    } else {
                        true
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Error checking last execution for probe {}: {}, will execute immediately",
                        probe_id,
                        e
                    );
                    true
                }
            };

            loop {
                if shutdown_token.is_cancelled() {
                    tracing::info!("Shutdown signal received, stopping scheduler for probe {}", probe_id);
                    break;
                }

                let probe = match ProbeManager::get_probe(&pool.write(), probe_id).await {
                    Ok(p) => p,
                    Err(e) => {
                        crate::background_error!(PROBE_SCHEDULER, "probe_fetch", Warning, "Error fetching probe {}: {}", probe_id, e);
                        break;
                    }
                };

                if !probe.active {
                    tracing::info!("Probe {} is not active, stopping scheduler", probe.name);
                    break;
                }

                match ProbeManager::execute_probe(&pool.write(), probe_id, &config, &credentials).await {
                    Ok(result) => {
                        if result.success {
                            tracing::debug!(
                                "Probe {} executed successfully in {}ms",
                                probe.name,
                                result.response_time_ms.unwrap_or(0)
                            );
                        } else {
                            tracing::warn!("Probe {} execution failed: {:?}", probe.name, result.error_message);
                        }
                    }
                    Err(e) => {
                        crate::background_error!(
                            PROBE_SCHEDULER,
                            "probe_execute",
                            Warning,
                            "Error executing probe {}: {}",
                            probe.name,
                            e
                        );
                    }
                }

                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(probe.interval_seconds as u64)) => {}
                    _ = shutdown_token.cancelled() => {
                        tracing::info!("Shutdown signal received during sleep, stopping scheduler for probe {}", probe_id);
                        break;
                    }
                }
            }

            tracing::info!("Scheduler for probe {} has stopped", probe_id);
        });

        let mut schedulers = self.schedulers.write().await;
        schedulers.insert(probe_id, handle);

        tracing::info!("Started scheduler for probe {}", probe_id);

        Ok(())
    }

    async fn stop_scheduler(&self, probe_id: Uuid) -> Result<(), anyhow::Error> {
        let mut schedulers = self.schedulers.write().await;

        if let Some(handle) = schedulers.remove(&probe_id) {
            handle.abort();
            tracing::info!("Stopped scheduler for probe {}", probe_id);
        }

        Ok(())
    }

    pub async fn stop_all(&self) -> Result<(), anyhow::Error> {
        let mut schedulers = self.schedulers.write().await;
        let count = schedulers.len();

        for (probe_id, handle) in schedulers.drain() {
            handle.abort();
            tracing::debug!("Stopped scheduler for probe {}", probe_id);
        }

        if count > 0 {
            tracing::info!("Stopped {} probe schedulers", count);
        }

        Ok(())
    }

    pub async fn sync_with_database(&self, shutdown_token: CancellationToken) -> Result<(), anyhow::Error> {
        let active_probes = ProbeManager::list_active_probes(&self.pool.write()).await?;
        let active_probe_ids: std::collections::HashSet<Uuid> = active_probes.iter().map(|p| p.id).collect();

        let schedulers = self.schedulers.read().await;
        let running_probe_ids: std::collections::HashSet<Uuid> = schedulers.keys().copied().collect();
        drop(schedulers);

        for probe_id in active_probe_ids.difference(&running_probe_ids) {
            tracing::info!("Starting scheduler for newly activated probe {}", probe_id);
            if let Err(e) = self.start_scheduler(*probe_id, shutdown_token.clone()).await {
                crate::background_error!(
                    PROBE_SCHEDULER,
                    "scheduler_start",
                    Warning,
                    "Failed to start scheduler for probe {}: {}",
                    probe_id,
                    e
                );
            }
        }

        for probe_id in running_probe_ids.difference(&active_probe_ids) {
            tracing::info!("Stopping scheduler for deactivated probe {}", probe_id);
            if let Err(e) = self.stop_scheduler(*probe_id).await {
                crate::background_error!(
                    PROBE_SCHEDULER,
                    "scheduler_stop",
                    Warning,
                    "Failed to stop scheduler for probe {}: {}",
                    probe_id,
                    e
                );
            }
        }

        Ok(())
    }

    async fn handle_probe_change(&self, probe_id: Uuid, active: bool, shutdown_token: CancellationToken) -> Result<(), anyhow::Error> {
        if active {
            if !self.is_scheduler_running(probe_id).await {
                tracing::info!("Probe {} activated, starting scheduler", probe_id);
                self.start_scheduler(probe_id, shutdown_token).await?;
            }
        } else {
            if self.is_scheduler_running(probe_id).await {
                tracing::info!("Probe {} deactivated, stopping scheduler", probe_id);
                self.stop_scheduler(probe_id).await?;
            }
        }
        Ok(())
    }

    async fn is_scheduler_running(&self, probe_id: Uuid) -> bool {
        let schedulers = self.schedulers.read().await;
        schedulers.contains_key(&probe_id)
    }

    async fn run_daemon_polling(self, shutdown_token: CancellationToken, sync_interval_seconds: u64) {
        tracing::info!(
            "Starting probe scheduler daemon in polling mode (sync every {}s)",
            sync_interval_seconds
        );

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(sync_interval_seconds));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.sync_with_database(shutdown_token.clone()).await {
                        crate::background_error!(PROBE_SCHEDULER, "db_sync", Warning, "Error syncing probe schedulers with database: {}", e);
                    }
                }
                _ = shutdown_token.cancelled() => {
                    tracing::info!("Shutdown signal received, stopping probe scheduler daemon");
                    break;
                }
            }
        }
    }

    pub async fn run_daemon(self, shutdown_token: CancellationToken, use_listen_notify: bool, fallback_sync_interval_seconds: u64) {
        if !use_listen_notify {
            return self.run_daemon_polling(shutdown_token, fallback_sync_interval_seconds).await;
        }
        tracing::info!(
            "Starting probe scheduler daemon with LISTEN/NOTIFY (fallback sync every {}s)",
            fallback_sync_interval_seconds
        );

        loop {
            if shutdown_token.is_cancelled() {
                tracing::info!("Shutdown signal received, stopping probe scheduler daemon");
                break;
            }

            let mut listener = match sqlx::postgres::PgListener::connect_with(&self.listener_pool.write()).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("Failed to create LISTEN connection: {}", e);
                    tokio::select! {
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {}
                        _ = shutdown_token.cancelled() => {
                            tracing::info!("Shutdown signal received during reconnect delay");
                            break;
                        }
                    }
                    continue;
                }
            };

            if let Err(e) = listener.listen("probe_changes").await {
                tracing::error!("Failed to LISTEN on probe_changes: {}", e);
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {}
                    _ = shutdown_token.cancelled() => {
                        tracing::info!("Shutdown signal received during reconnect delay");
                        break;
                    }
                }
                continue;
            }

            tracing::info!("LISTEN connection established for probe changes");

            let mut fallback_interval = tokio::time::interval(tokio::time::Duration::from_secs(fallback_sync_interval_seconds));
            fallback_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        tracing::info!("Shutdown signal received, stopping probe scheduler daemon");
                        return;
                    }

                    notification = listener.recv() => {
                        match notification {
                            Ok(notif) => {
                                match serde_json::from_str::<serde_json::Value>(notif.payload()) {
                                    Ok(payload) => {
                                        if let (Some(probe_id), Some(active)) = (
                                            payload.get("probe_id").and_then(|v| v.as_str()).and_then(|s| Uuid::parse_str(s).ok()),
                                            payload.get("active").and_then(|v| v.as_bool())
                                        ) {
                                            tracing::debug!("Received probe change notification: probe_id={}, active={}", probe_id, active);
                                            if let Err(e) = self.handle_probe_change(probe_id, active, shutdown_token.clone()).await {
                                                crate::background_error!(PROBE_SCHEDULER, "probe_change", Warning, "Failed to handle probe change for {}: {}", probe_id, e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        crate::background_error!(PROBE_SCHEDULER, "payload_parse", Warning, "Failed to parse notification payload: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("Error receiving notification: {}", e);
                                break;
                            }
                        }
                    }

                    _ = fallback_interval.tick() => {
                        tracing::debug!("Running fallback sync");
                        if let Err(e) = self.sync_with_database(shutdown_token.clone()).await {
                            crate::background_error!(PROBE_SCHEDULER, "db_sync", Warning, "Error during fallback sync: {}", e);
                        }
                    }
                }
            }

            tracing::warn!("LISTEN connection lost, reconnecting in 5s...");
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {}
                _ = shutdown_token.cancelled() => {
                    tracing::info!("Shutdown signal received during reconnect delay");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::models::probes::CreateProbe;
    use crate::probes::db::ProbeManager;

    async fn setup_test_deployment(pool: &sqlx::PgPool) -> Uuid {
        let unique_id = Uuid::new_v4();
        let endpoint_name = format!("test-endpoint-{}", unique_id);
        let model_name = format!("test-model-{}", unique_id);

        let endpoint_id = sqlx::query_scalar!(
            "INSERT INTO inference_endpoints (name, url, created_by) VALUES ($1, $2, $3) RETURNING id",
            endpoint_name,
            "http://localhost:8080",
            Uuid::nil()
        )
        .fetch_one(pool)
        .await
        .unwrap();

        sqlx::query_scalar!(
            "INSERT INTO deployed_models (model_name, alias, type, hosted_on, created_by) VALUES ($1, $2, $3, $4, $5) RETURNING id",
            model_name.clone(),
            model_name,
            "chat" as _,
            endpoint_id,
            Uuid::nil()
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    fn create_test_config() -> crate::config::Config {
        crate::test::utils::create_test_config()
    }

    #[dwctl_test_macros::test]
    async fn test_scheduler_initialize(pool: sqlx::PgPool) {
        let deployment_id1 = setup_test_deployment(&pool).await;
        let deployment_id2 = setup_test_deployment(&pool).await;

        let _probe1 = ProbeManager::create_probe(
            &pool,
            CreateProbe {
                name: "Probe 1".to_string(),
                deployment_id: deployment_id1,
                interval_seconds: 60,
                http_method: "POST".to_string(),
                request_path: None,
                request_body: None,
            },
        )
        .await
        .unwrap();

        let _probe2 = ProbeManager::create_probe(
            &pool,
            CreateProbe {
                name: "Probe 2".to_string(),
                deployment_id: deployment_id2,
                interval_seconds: 120,
                http_method: "POST".to_string(),
                request_path: None,
                request_body: None,
            },
        )
        .await
        .unwrap();

        let config = create_test_config();
        let scheduler = ProbeScheduler::new(pool, config, InternalCredentials::generate());

        scheduler.initialize(CancellationToken::new()).await.unwrap();

        let schedulers = scheduler.schedulers.read().await;
        assert_eq!(schedulers.len(), 2);
    }

    #[dwctl_test_macros::test]
    async fn test_sync_starts_new_schedulers(pool: sqlx::PgPool) {
        let deployment_id = setup_test_deployment(&pool).await;

        let config = create_test_config();
        let scheduler = ProbeScheduler::new(pool.clone(), config, InternalCredentials::generate());

        scheduler.initialize(CancellationToken::new()).await.unwrap();
        let initial_count = scheduler.schedulers.read().await.len();
        assert_eq!(initial_count, 0);

        let _probe = ProbeManager::create_probe(
            &pool,
            CreateProbe {
                name: "New Probe".to_string(),
                deployment_id,
                interval_seconds: 60,
                http_method: "POST".to_string(),
                request_path: None,
                request_body: None,
            },
        )
        .await
        .unwrap();

        scheduler.sync_with_database(CancellationToken::new()).await.unwrap();

        let new_count = scheduler.schedulers.read().await.len();
        assert_eq!(new_count, 1);
    }

    #[dwctl_test_macros::test]
    async fn test_sync_stops_deactivated_schedulers(pool: sqlx::PgPool) {
        let deployment_id = setup_test_deployment(&pool).await;

        let probe = ProbeManager::create_probe(
            &pool,
            CreateProbe {
                name: "Test Probe".to_string(),
                deployment_id,
                interval_seconds: 60,
                http_method: "POST".to_string(),
                request_path: None,
                request_body: None,
            },
        )
        .await
        .unwrap();

        let config = create_test_config();
        let scheduler = ProbeScheduler::new(pool.clone(), config, InternalCredentials::generate());

        scheduler.initialize(CancellationToken::new()).await.unwrap();
        assert_eq!(scheduler.schedulers.read().await.len(), 1);

        ProbeManager::deactivate_probe(&pool, probe.id).await.unwrap();

        scheduler.sync_with_database(CancellationToken::new()).await.unwrap();
        assert_eq!(scheduler.schedulers.read().await.len(), 0);
    }

    #[dwctl_test_macros::test]
    async fn test_stop_all_schedulers(pool: sqlx::PgPool) {
        for i in 0..3 {
            let deployment_id = setup_test_deployment(&pool).await;
            ProbeManager::create_probe(
                &pool,
                CreateProbe {
                    name: format!("Probe {}", i),
                    deployment_id,
                    interval_seconds: 60,
                    http_method: "POST".to_string(),
                    request_path: None,
                    request_body: None,
                },
            )
            .await
            .unwrap();
        }

        let config = create_test_config();
        let scheduler = ProbeScheduler::new(pool, config, InternalCredentials::generate());

        scheduler.initialize(CancellationToken::new()).await.unwrap();
        assert_eq!(scheduler.schedulers.read().await.len(), 3);

        scheduler.stop_all().await.unwrap();
        assert_eq!(scheduler.schedulers.read().await.len(), 0);
    }

    #[dwctl_test_macros::test]
    async fn test_scheduler_ignores_inactive_probes(pool: sqlx::PgPool) {
        let deployment_id = setup_test_deployment(&pool).await;

        let probe = ProbeManager::create_probe(
            &pool,
            CreateProbe {
                name: "Inactive Probe".to_string(),
                deployment_id,
                interval_seconds: 60,
                http_method: "POST".to_string(),
                request_path: None,
                request_body: None,
            },
        )
        .await
        .unwrap();

        ProbeManager::deactivate_probe(&pool, probe.id).await.unwrap();

        let config = create_test_config();
        let scheduler = ProbeScheduler::new(pool, config, InternalCredentials::generate());

        scheduler.initialize(CancellationToken::new()).await.unwrap();

        assert_eq!(scheduler.schedulers.read().await.len(), 0);
    }
}
