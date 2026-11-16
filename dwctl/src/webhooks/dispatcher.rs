
use crate::metrics::errors::component::WEBHOOK_DISPATCH;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use metrics::counter;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::WebhookConfig;
use crate::db::handlers::Webhooks;
use crate::webhooks::signing;


#[derive(Debug)]
struct WebhookSendRequest {
    url: String,
    headers: Vec<(String, String)>,
    body: String,
    delivery_id: Uuid,
    webhook_id: Uuid,
    attempt_count: i32,
}

#[derive(Debug)]
enum SendOutcome {
    Success { status_code: u16 },
    Failure { status_code: Option<u16>, error: String },
}

#[derive(Debug)]
struct WebhookSendResult {
    delivery_id: Uuid,
    webhook_id: Uuid,
    attempt_count: i32,
    outcome: SendOutcome,
}


pub struct WebhookDispatcher {
    pool: sqlx_pool_router::DynPools,
    send_tx: mpsc::Sender<WebhookSendRequest>,
    result_rx: mpsc::Receiver<WebhookSendResult>,
    retry_schedule: Vec<i64>,
    claim_batch_size: i64,
    circuit_breaker_threshold: i32,
}

impl WebhookDispatcher {
    pub fn spawn(pool: impl sqlx_pool_router::PoolProvider, config: &WebhookConfig, shutdown: CancellationToken) -> Self {
        let pool = sqlx_pool_router::DynPools::new(pool);
        let (send_tx, send_rx) = mpsc::channel::<WebhookSendRequest>(config.channel_capacity);
        let (result_tx, result_rx) = mpsc::channel(config.channel_capacity);

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .expect("Failed to create webhook HTTP client");

        tokio::spawn(run_sender(send_rx, result_tx, http_client, config.max_concurrent_sends, shutdown));

        Self {
            pool,
            send_tx,
            result_rx,
            retry_schedule: config.retry_schedule_secs.clone(),
            claim_batch_size: config.claim_batch_size,
            circuit_breaker_threshold: config.circuit_breaker_threshold,
        }
    }

    pub async fn tick(&mut self) {
        tracing::debug!("Webhook dispatcher tick");
        self.claim_and_send().await;
        self.drain_results().await;
    }

    async fn claim_and_send(&self) {
        let mut conn = match self.pool.write().acquire().await {
            Ok(c) => c,
            Err(e) => {
                crate::background_error!(WEBHOOK_DISPATCH, "db_acquire", Warning, error = %e, "Failed to acquire connection for retry claims");
                return;
            }
        };

        let deliveries = {
            let mut repo = Webhooks::new(&mut conn);
            match repo.claim_retriable_deliveries(self.claim_batch_size).await {
                Ok(d) => d,
                Err(e) => {
                    crate::background_error!(WEBHOOK_DISPATCH, "claim", Warning, error = %e, "Failed to claim retriable deliveries");
                    return;
                }
            }
        };

        if deliveries.is_empty() {
            tracing::debug!("No deliveries to claim");
            return;
        }

        counter!("dwctl_webhook_deliveries_claimed_total").increment(deliveries.len() as u64);
        tracing::debug!(count = deliveries.len(), "Claimed deliveries for sending");

        for delivery in deliveries {
            let (Some(url), Some(secret), Some(enabled)) = (&delivery.webhook_url, &delivery.webhook_secret, delivery.webhook_enabled)
            else {
                tracing::warn!(
                    delivery_id = %delivery.id,
                    webhook_id = %delivery.webhook_id,
                    "Webhook not found for delivery, marking exhausted"
                );
                let mut repo = Webhooks::new(&mut conn);
                let _ = repo.mark_exhausted(delivery.id).await;
                continue;
            };

            if !enabled {
                tracing::debug!(
                    delivery_id = %delivery.id,
                    webhook_id = %delivery.webhook_id,
                    "Webhook disabled, marking delivery exhausted"
                );
                let mut repo = Webhooks::new(&mut conn);
                let _ = repo.mark_exhausted(delivery.id).await;
                continue;
            }

            let payload_str = match serde_json::to_string(&delivery.payload) {
                Ok(s) => s,
                Err(e) => {
                    crate::background_error!(WEBHOOK_DISPATCH, "payload_serialize", Warning, error = %e, delivery_id = %delivery.id, "Failed to serialize delivery payload, marking exhausted");
                    let mut repo = Webhooks::new(&mut conn);
                    let _ = repo.mark_exhausted(delivery.id).await;
                    continue;
                }
            };

            let timestamp = Utc::now().timestamp();
            let msg_id = delivery.event_id.to_string();
            let signature = match signing::sign_payload(&msg_id, timestamp, &payload_str, secret) {
                Some(s) => s,
                None => {
                    crate::background_error!(WEBHOOK_DISPATCH, "sign", Warning, delivery_id = %delivery.id, "Failed to sign webhook payload");
                    continue;
                }
            };

            let send_request = WebhookSendRequest {
                url: url.clone(),
                headers: vec![
                    ("Content-Type".to_string(), "application/json".to_string()),
                    ("webhook-id".to_string(), msg_id),
                    ("webhook-timestamp".to_string(), timestamp.to_string()),
                    ("webhook-signature".to_string(), signature),
                    ("webhook-version".to_string(), "1".to_string()),
                ],
                body: payload_str,
                delivery_id: delivery.id,
                webhook_id: delivery.webhook_id,
                attempt_count: delivery.attempt_count,
            };

            if let Err(e) = self.send_tx.try_send(send_request) {
                tracing::warn!(
                    delivery_id = %delivery.id,
                    "Failed to push to sender channel (will retry after claim timeout): {}",
                    e
                );
            }
        }
    }

    async fn drain_results(&mut self) {
        let mut conn = match self.pool.write().acquire().await {
            Ok(c) => c,
            Err(e) => {
                crate::background_error!(WEBHOOK_DISPATCH, "db_acquire", Warning, error = %e, "Failed to acquire connection for result drain");
                return;
            }
        };

        let mut drained = 0u32;
        while let Ok(result) = self.result_rx.try_recv() {
            drained += 1;
            let mut repo = Webhooks::new(&mut conn);

            match result.outcome {
                SendOutcome::Success { status_code } => {
                    counter!("dwctl_webhook_deliveries_total", "outcome" => "success").increment(1);
                    if let Err(e) = repo.mark_delivered(result.delivery_id, status_code as i32).await {
                        crate::background_error!(WEBHOOK_DISPATCH, "mark_delivered", Warning, error = %e, delivery_id = %result.delivery_id, "Failed to mark delivery as delivered");
                    }
                    if let Err(e) = repo.reset_failures(result.webhook_id).await {
                        crate::background_error!(WEBHOOK_DISPATCH, "reset_failures", Warning, error = %e, webhook_id = %result.webhook_id, "Failed to reset webhook failures");
                    }
                    tracing::debug!(
                        webhook_id = %result.webhook_id,
                        delivery_id = %result.delivery_id,
                        status = status_code,
                        "Webhook delivered successfully"
                    );
                }
                SendOutcome::Failure { status_code, ref error } => {
                    counter!("dwctl_webhook_deliveries_total", "outcome" => "failure").increment(1);
                    if let Err(e) = repo
                        .mark_failed(
                            result.delivery_id,
                            status_code.map(|c| c as i32),
                            error,
                            result.attempt_count,
                            &self.retry_schedule,
                        )
                        .await
                    {
                        crate::background_error!(WEBHOOK_DISPATCH, "mark_failed", Warning, error = %e, delivery_id = %result.delivery_id, "Failed to mark delivery as failed");
                    }
                    match repo.increment_failures(result.webhook_id, self.circuit_breaker_threshold).await {
                        Ok(None) => {
                            tracing::debug!(
                                webhook_id = %result.webhook_id,
                                delivery_id = %result.delivery_id,
                                "Webhook deleted while delivery was in-flight, skipping failure tracking"
                            );
                        }
                        Err(e) => {
                            crate::background_error!(WEBHOOK_DISPATCH, "increment_failures", Warning, error = %e, webhook_id = %result.webhook_id, "Failed to increment webhook failures");
                        }
                        Ok(Some(_)) => {}
                    }
                    tracing::warn!(
                        webhook_id = %result.webhook_id,
                        delivery_id = %result.delivery_id,
                        status_code = ?status_code,
                        error = %error,
                        "Webhook delivery failed"
                    );
                }
            }
        }

        if drained > 0 {
            tracing::debug!(count = drained, "Drained webhook send results");
        } else {
            tracing::debug!("No send results to drain");
        }
    }
}


async fn run_sender(
    mut rx: mpsc::Receiver<WebhookSendRequest>,
    result_tx: mpsc::Sender<WebhookSendResult>,
    http_client: reqwest::Client,
    max_concurrent_sends: usize,
    shutdown: CancellationToken,
) {
    let semaphore = Arc::new(Semaphore::new(max_concurrent_sends));

    loop {
        let request = tokio::select! {
            req = rx.recv() => {
                match req {
                    Some(r) => r,
                    None => {
                        tracing::debug!("Webhook sender channel closed, shutting down");
                        break;
                    }
                }
            }
            _ = shutdown.cancelled() => {
                tracing::debug!("Webhook sender received shutdown signal");
                break;
            }
        };

        let permit = match semaphore.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                tracing::warn!("Webhook sender semaphore closed");
                break;
            }
        };

        let client = http_client.clone();
        let tx = result_tx.clone();

        tokio::spawn(async move {
            let _permit = permit;

            tracing::debug!(
                delivery_id = %request.delivery_id,
                url = %request.url,
                attempt = request.attempt_count,
                "Sending webhook HTTP request"
            );

            let mut req_builder = client.post(&request.url);
            for (name, value) in &request.headers {
                req_builder = req_builder.header(name, value);
            }
            req_builder = req_builder.body(request.body);

            let outcome = match req_builder.send().await {
                Ok(response) => {
                    let status_code = response.status().as_u16();
                    if response.status().is_success() {
                        SendOutcome::Success { status_code }
                    } else {
                        SendOutcome::Failure {
                            status_code: Some(status_code),
                            error: format!("HTTP {}", status_code),
                        }
                    }
                }
                Err(e) => SendOutcome::Failure {
                    status_code: None,
                    error: e.to_string(),
                },
            };

            let result = WebhookSendResult {
                delivery_id: request.delivery_id,
                webhook_id: request.webhook_id,
                attempt_count: request.attempt_count,
                outcome,
            };

            if let Err(e) = tx.send(result).await {
                crate::background_error!(WEBHOOK_DISPATCH, "result_send", Warning, delivery_id = %request.delivery_id, "Failed to send webhook result back: {}", e);
            }
        });
    }

    tracing::debug!("Webhook sender task exited");
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn start_sender() -> (
        mpsc::Sender<WebhookSendRequest>,
        mpsc::Receiver<WebhookSendResult>,
        CancellationToken,
    ) {
        let (send_tx, send_rx) = mpsc::channel(10);
        let (result_tx, result_rx) = mpsc::channel(10);
        let http_client = reqwest::Client::new();
        let shutdown = CancellationToken::new();

        let sender_shutdown = shutdown.clone();
        tokio::spawn(async move {
            run_sender(send_rx, result_tx, http_client, 20, sender_shutdown).await;
        });

        (send_tx, result_rx, shutdown)
    }

    fn make_request(url: &str, delivery_id: Uuid, webhook_id: Uuid, attempt: i32) -> WebhookSendRequest {
        WebhookSendRequest {
            url: url.to_string(),
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: r#"{"test": true}"#.to_string(),
            delivery_id,
            webhook_id,
            attempt_count: attempt,
        }
    }

    #[tokio::test]
    async fn test_successful_delivery() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        let (send_tx, mut result_rx, shutdown) = start_sender().await;
        let delivery_id = Uuid::new_v4();
        let webhook_id = Uuid::new_v4();

        send_tx
            .send(make_request(&mock_server.uri(), delivery_id, webhook_id, 0))
            .await
            .unwrap();

        let result = result_rx.recv().await.unwrap();
        assert_eq!(result.delivery_id, delivery_id);
        assert_eq!(result.webhook_id, webhook_id);
        assert_eq!(result.attempt_count, 0);
        assert!(matches!(result.outcome, SendOutcome::Success { status_code: 200 }));

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_http_error_delivery() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&mock_server)
            .await;

        let (send_tx, mut result_rx, shutdown) = start_sender().await;
        let delivery_id = Uuid::new_v4();

        send_tx
            .send(make_request(&mock_server.uri(), delivery_id, Uuid::new_v4(), 2))
            .await
            .unwrap();

        let result = result_rx.recv().await.unwrap();
        assert_eq!(result.delivery_id, delivery_id);
        assert_eq!(result.attempt_count, 2);
        assert!(matches!(
            result.outcome,
            SendOutcome::Failure {
                status_code: Some(500),
                ..
            }
        ));

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_network_error_delivery() {
        let (send_tx, mut result_rx, shutdown) = start_sender().await;
        let delivery_id = Uuid::new_v4();

        send_tx
            .send(make_request("http://127.0.0.1:1", delivery_id, Uuid::new_v4(), 0))
            .await
            .unwrap();

        let result = result_rx.recv().await.unwrap();
        assert_eq!(result.delivery_id, delivery_id);
        assert!(matches!(result.outcome, SendOutcome::Failure { status_code: None, .. }));

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_metadata_passed_through() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let (send_tx, mut result_rx, shutdown) = start_sender().await;
        let delivery_id = Uuid::new_v4();
        let webhook_id = Uuid::new_v4();

        send_tx
            .send(make_request(&mock_server.uri(), delivery_id, webhook_id, 5))
            .await
            .unwrap();

        let result = result_rx.recv().await.unwrap();
        assert_eq!(result.delivery_id, delivery_id);
        assert_eq!(result.webhook_id, webhook_id);
        assert_eq!(result.attempt_count, 5);

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_sender_exits_on_channel_close() {
        let (send_tx, send_rx) = mpsc::channel(10);
        let (result_tx, _result_rx) = mpsc::channel(10);
        let http_client = reqwest::Client::new();
        let shutdown = CancellationToken::new();

        let handle = tokio::spawn(async move {
            run_sender(send_rx, result_tx, http_client, 20, shutdown).await;
        });

        drop(send_tx);

        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("sender should exit when channel closes")
            .expect("sender should not panic");
    }
}
