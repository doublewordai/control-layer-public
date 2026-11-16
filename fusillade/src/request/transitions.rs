
use std::sync::Arc;

use metrics::counter;
use serde_json;
use tokio::sync::Mutex;
use tracing::Instrument;

use crate::{
    FusilladeError,
    error::Result,
    http::{HttpClient, HttpResponse},
    manager::Storage,
};

use super::types::{
    Canceled, Claimed, Completed, DaemonId, Failed, FailureReason, Pending, Processing, Request,
    RequestCompletionResult,
};

#[derive(Debug, Clone, Copy)]
pub enum CancellationReason {
    User,
    Shutdown,
}

impl Request<Pending> {
    pub async fn claim<S: Storage + ?Sized>(
        self,
        daemon_id: DaemonId,
        storage: &S,
    ) -> Result<Request<Claimed>> {
        let request = Request {
            data: self.data,
            state: Claimed {
                daemon_id,
                claimed_at: chrono::Utc::now(),
                retry_attempt: self.state.retry_attempt,
                batch_expires_at: self.state.batch_expires_at,
                leak: None,
            },
        };
        storage.persist(&request).await?;
        Ok(request)
    }

    pub async fn cancel<S: Storage + ?Sized>(self, storage: &S) -> Result<Request<Canceled>> {
        let request = Request {
            data: self.data,
            state: Canceled {
                canceled_at: chrono::Utc::now(),
            },
        };
        storage.persist(&request).await?;
        Ok(request)
    }
}

impl Request<Claimed> {
    pub async fn unclaim<S: Storage + ?Sized>(self, storage: &S) -> Result<Request<Pending>> {
        let request = Request {
            data: self.data,
            state: Pending {
                retry_attempt: self.state.retry_attempt,
                not_before: None,
                batch_expires_at: self.state.batch_expires_at,
            },
        };
        storage.persist(&request).await?;
        Ok(request)
    }

    pub async fn cancel<S: Storage + ?Sized>(self, storage: &S) -> Result<Request<Canceled>> {
        let request = Request {
            data: self.data,
            state: Canceled {
                canceled_at: chrono::Utc::now(),
            },
        };
        storage.persist(&request).await?;
        Ok(request)
    }

    pub async fn process<H: HttpClient + 'static, S: Storage>(
        self,
        http_client: H,
        storage: &S,
    ) -> Result<Request<Processing>> {
        let request_data = self.data.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let current_span = tracing::Span::current();
        let task_handle = tokio::spawn(
            async move {
                let result = http_client
                    .execute(&request_data, &request_data.api_key)
                    .await;
                let _ = tx.send(result).await;
            }
            .instrument(current_span),
        );

        let processing_state = Processing {
            daemon_id: self.state.daemon_id,
            claimed_at: self.state.claimed_at,
            started_at: chrono::Utc::now(),
            retry_attempt: self.state.retry_attempt,
            batch_expires_at: self.state.batch_expires_at,
            result_rx: Arc::new(Mutex::new(rx)),
            abort_handle: task_handle.abort_handle(),
        };

        let request = Request {
            data: self.data,
            state: processing_state,
        };

        if let Err(e) = storage.persist(&request).await {
            request.state.abort_handle.abort();
            return Err(e);
        }

        Ok(request)
    }
}

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: Option<u32>,
    pub stop_before_deadline_ms: Option<i64>,
    pub backoff_ms: u64,
    pub backoff_factor: u64,
    pub max_backoff_ms: u64,
}

impl From<&crate::daemon::DaemonConfig> for RetryConfig {
    fn from(config: &crate::daemon::DaemonConfig) -> Self {
        RetryConfig {
            max_retries: config.max_retries,
            stop_before_deadline_ms: config.stop_before_deadline_ms,
            backoff_ms: config.backoff_ms,
            backoff_factor: config.backoff_factor,
            max_backoff_ms: config.max_backoff_ms,
        }
    }
}

impl Request<Failed> {
    pub fn can_retry(
        self,
        retry_attempt: u32,
        config: RetryConfig,
    ) -> std::result::Result<Request<Pending>, Box<Self>> {
        let backoff_duration = {
            let exponential = config
                .backoff_ms
                .saturating_mul(config.backoff_factor.saturating_pow(retry_attempt));
            exponential.min(config.max_backoff_ms)
        };

        let now = chrono::Utc::now();
        let not_before = now + chrono::Duration::milliseconds(backoff_duration as i64);

        if let Some(max_retries) = config.max_retries
            && retry_attempt >= max_retries
        {
            counter!(
                "fusillade_retry_denied_total",
                "model" => self.data.model.clone(),
                "reason" => "max_retries"
            )
            .increment(1);
            return Err(Box::new(self));
        }

        let effective_deadline = if let Some(stop_before_deadline_ms) =
            config.stop_before_deadline_ms
        {
            self.state.batch_expires_at - chrono::Duration::milliseconds(stop_before_deadline_ms)
        } else {
            self.state.batch_expires_at
        };

        if not_before >= effective_deadline {
            counter!(
                "fusillade_retry_denied_total",
                "model" => self.data.model.clone(),
                "reason" => "deadline"
            )
            .increment(1);
            return Err(Box::new(self));
        }


        let request = Request {
            data: self.data,
            state: Pending {
                retry_attempt: retry_attempt + 1,
                not_before: Some(not_before),
                batch_expires_at: self.state.batch_expires_at,
            },
        };

        Ok(request)
    }
}

impl Request<Processing> {
    pub async fn complete<S, F, Fut>(
        self,
        storage: &S,
        should_retry: F,
        cancellation: Fut,
    ) -> Result<RequestCompletionResult>
    where
        S: Storage + ?Sized,
        F: Fn(&HttpResponse) -> bool,
        Fut: std::future::Future<Output = CancellationReason>,
    {
        enum Outcome {
            Result(Option<std::result::Result<HttpResponse, FusilladeError>>),
            Canceled(CancellationReason),
        }

        let outcome = {
            let mut rx = self.state.result_rx.lock().await;

            tokio::select! {
                result = rx.recv() => Outcome::Result(result),
                reason = cancellation => Outcome::Canceled(reason),
            }
        };

        let result = match outcome {
            Outcome::Canceled(CancellationReason::User) => {
                self.state.abort_handle.abort();
                let canceled = Request {
                    data: self.data,
                    state: Canceled {
                        canceled_at: chrono::Utc::now(),
                    },
                };
                return Ok(RequestCompletionResult::Canceled(canceled));
            }
            Outcome::Canceled(CancellationReason::Shutdown) => {
                self.state.abort_handle.abort();
                return Err(FusilladeError::Shutdown);
            }
            Outcome::Result(result) => result,
        };

        match result {
            Some(Ok(http_response)) => {
                let is_error = http_response.status >= 400;

                if should_retry(&http_response) {
                    let error_code = serde_json::from_str::<serde_json::Value>(&http_response.body)
                        .ok()
                        .and_then(|v| v.get("error")?.get("code")?.as_str().map(String::from))
                        .unwrap_or_default();

                    counter!(
                        "fusillade_http_status_retriable_total",
                        "model" => self.data.model.clone(),
                        "status" => http_response.status.to_string(),
                        "code" => error_code,
                    )
                    .increment(1);

                    let failed_state = Failed {
                        reason: FailureReason::RetriableHttpStatus {
                            status: http_response.status,
                            body: http_response.body.clone(),
                        },
                        failed_at: chrono::Utc::now(),
                        retry_attempt: self.state.retry_attempt,
                        batch_expires_at: self.state.batch_expires_at,
                        routed_model: self.data.model.clone(),
                    };
                    let request = Request {
                        data: self.data,
                        state: failed_state,
                    };
                    Ok(RequestCompletionResult::Failed(request))
                } else if is_error {
                    let failed_state = Failed {
                        reason: FailureReason::NonRetriableHttpStatus {
                            status: http_response.status,
                            body: http_response.body.clone(),
                        },
                        failed_at: chrono::Utc::now(),
                        retry_attempt: self.state.retry_attempt,
                        batch_expires_at: self.state.batch_expires_at,
                        routed_model: self.data.model.clone(),
                    };
                    let request = Request {
                        data: self.data,
                        state: failed_state,
                    };
                    storage.persist(&request).await?;
                    Ok(RequestCompletionResult::Failed(request))
                } else {
                    let completed_state = Completed {
                        response_status: http_response.status,
                        response_body: http_response.body,
                        claimed_at: self.state.claimed_at,
                        started_at: self.state.started_at,
                        completed_at: chrono::Utc::now(),
                        routed_model: self.data.model.clone(),
                    };
                    let request = Request {
                        data: self.data,
                        state: completed_state,
                    };
                    storage.persist(&request).await?;
                    Ok(RequestCompletionResult::Completed(request))
                }
            }
            Some(Err(e)) => {
                let reason = match &e {
                    FusilladeError::HttpClient(reqwest_err) if reqwest_err.is_builder() => {
                        FailureReason::RequestBuilderError {
                            error: reqwest_err.to_string(),
                        }
                    }
                    FusilladeError::HttpClient(reqwest_err) if reqwest_err.is_timeout() => {
                        FailureReason::Timeout {
                            error: reqwest_err.to_string(),
                        }
                    }
                    FusilladeError::FirstChunkTimeout(msg) => {
                        FailureReason::Timeout { error: msg.clone() }
                    }
                    FusilladeError::TokensTimeout(msg) => {
                        FailureReason::Timeout { error: msg.clone() }
                    }
                    FusilladeError::BodyTimeout(msg) => {
                        FailureReason::Timeout { error: msg.clone() }
                    }
                    _ => FailureReason::NetworkError {
                        error: crate::error::error_serialization::serialize_error(&e.into()),
                    },
                };

                let failed_state = Failed {
                    reason,
                    failed_at: chrono::Utc::now(),
                    retry_attempt: self.state.retry_attempt,
                    batch_expires_at: self.state.batch_expires_at,
                    routed_model: self.data.model.clone(),
                };
                let request = Request {
                    data: self.data,
                    state: failed_state,
                };
                Ok(RequestCompletionResult::Failed(request))
            }
            None => {
                let failed_state = Failed {
                    reason: FailureReason::TaskTerminated,
                    failed_at: chrono::Utc::now(),
                    retry_attempt: self.state.retry_attempt,
                    batch_expires_at: self.state.batch_expires_at,
                    routed_model: self.data.model.clone(),
                };
                let request = Request {
                    data: self.data,
                    state: failed_state,
                };
                storage.persist(&request).await?;
                Ok(RequestCompletionResult::Failed(request))
            }
        }
    }

    pub async fn cancel<S: Storage + ?Sized>(self, storage: &S) -> Result<Request<Canceled>> {
        self.state.abort_handle.abort();

        let request = Request {
            data: self.data,
            state: Canceled {
                canceled_at: chrono::Utc::now(),
            },
        };
        storage.persist(&request).await?;
        Ok(request)
    }
}
