
use std::sync::Arc;

use tokio::sync::{Mutex, oneshot};
use tracing::Instrument;

use crate::{FusilladeError, error::Result, manager::Storage};

use super::types::{
    Canceled, Claimed, Completed, DaemonId, Failed, FailureReason, HttpResponse, Pending,
    Processing, Request, RequestCompletionResult, is_batch_capacity_exceeded_response,
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
                claimed_at: None,
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
                claimed_at: Some(self.state.claimed_at),
            },
        };
        storage.persist(&request).await?;
        Ok(request)
    }

    pub async fn process<S, Fut>(
        self,
        storage: &S,
        response_fut: Fut,
    ) -> Result<Request<Processing>>
    where
        S: Storage,
        Fut: std::future::Future<Output = Result<HttpResponse>> + Send + 'static,
    {
        let (dispatch_tx, dispatch_rx) = oneshot::channel();
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let current_span = tracing::Span::current();
        let task_handle = tokio::spawn(
            async move {
                if dispatch_rx.await.is_err() {
                    return;
                }
                let result = response_fut.await;
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

        let mut request = Request {
            data: self.data,
            state: processing_state,
        };

        if let Err(e) = storage.persist(&request).await {
            request.state.abort_handle.abort();
            return Err(e);
        }

        request.state.started_at = chrono::Utc::now();

        if dispatch_tx.send(()).is_err() {
            request.state.abort_handle.abort();
            return Err(FusilladeError::Other(anyhow::anyhow!(
                "HTTP dispatch task terminated before request processing began"
            )));
        }

        Ok(request)
    }
}

pub const DEFAULT_MAX_CAPACITY_RESCHEDULE_AGE_MS: i64 = 3_600_000;

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: Option<u32>,
    pub stop_before_deadline_ms: Option<i64>,
    pub backoff_ms: u64,
    pub backoff_factor: u64,
    pub max_backoff_ms: u64,
    pub max_capacity_reschedule_age_ms: Option<i64>,
}

impl Request<Failed> {
    pub fn can_retry(
        self,
        retry_attempt: u32,
        config: RetryConfig,
    ) -> std::result::Result<Request<Pending>, Box<Self>> {
        self.reschedule_retry(retry_attempt, config, true)
    }

    pub fn can_retry_without_spending_attempt(
        self,
        retry_attempt: u32,
        config: RetryConfig,
    ) -> std::result::Result<Request<Pending>, Box<Self>> {
        self.reschedule_retry(retry_attempt, config, false)
    }

    fn reschedule_retry(
        self,
        retry_attempt: u32,
        config: RetryConfig,
        spend_attempt: bool,
    ) -> std::result::Result<Request<Pending>, Box<Self>> {
        let backoff_duration = {
            let exponential = config
                .backoff_ms
                .saturating_mul(config.backoff_factor.saturating_pow(retry_attempt));
            exponential.min(config.max_backoff_ms)
        };

        let now = chrono::Utc::now();
        let not_before = now + chrono::Duration::milliseconds(backoff_duration as i64);

        if spend_attempt
            && let Some(max_retries) = config.max_retries
            && retry_attempt >= max_retries
        {
            return Err(Box::new(self));
        }

        if !spend_attempt
            && self.state.batch_expires_at.is_none()
            && let Some(max_age_ms) = config.max_capacity_reschedule_age_ms
            && let Some(created_at) = self
                .data
                .batch_metadata
                .get("created_at")
                .and_then(|value| value.parse::<chrono::DateTime<chrono::Utc>>().ok())
            && now.signed_duration_since(created_at) >= chrono::Duration::milliseconds(max_age_ms)
        {
            return Err(Box::new(self));
        }

        if let Some(batch_expires_at) = self.state.batch_expires_at {
            let effective_deadline =
                if let Some(stop_before_deadline_ms) = config.stop_before_deadline_ms {
                    batch_expires_at - chrono::Duration::milliseconds(stop_before_deadline_ms)
                } else {
                    batch_expires_at
                };

            if not_before >= effective_deadline {
                return Err(Box::new(self));
            }
        }


        let request = Request {
            data: self.data,
            state: Pending {
                retry_attempt: if spend_attempt {
                    retry_attempt + 1
                } else {
                    retry_attempt
                },
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
                        claimed_at: Some(self.state.claimed_at),
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

                let batch_capacity_refusal =
                    is_batch_capacity_exceeded_response(http_response.status, &http_response.body);

                if should_retry(&http_response) || batch_capacity_refusal {
                    let failed_state = Failed {
                        reason: FailureReason::RetriableHttpStatus {
                            status: http_response.status,
                            body: http_response.body.clone(),
                        },
                        failed_at: chrono::Utc::now(),
                        claimed_at: Some(self.state.claimed_at),
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
                        claimed_at: Some(self.state.claimed_at),
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
                    FusilladeError::HttpRequestBuilder(error) => {
                        FailureReason::RequestBuilderError {
                            error: error.clone(),
                        }
                    }
                    FusilladeError::HttpClientTimeout(error) => FailureReason::Timeout {
                        error: error.clone(),
                    },
                    FusilladeError::UploadStallTimeout(msg) => {
                        FailureReason::Timeout { error: msg.clone() }
                    }
                    _ => FailureReason::NetworkError {
                        error: crate::error::error_serialization::serialize_error(&e.into()),
                    },
                };

                let failed_state = Failed {
                    reason,
                    failed_at: chrono::Utc::now(),
                    claimed_at: Some(self.state.claimed_at),
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
                    claimed_at: Some(self.state.claimed_at),
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
                claimed_at: Some(self.state.claimed_at),
            },
        };
        storage.persist(&request).await?;
        Ok(request)
    }
}

#[cfg(test)]
mod background_tests {
    use super::*;
    use crate::batch::{BatchId, TemplateId};
    use crate::request::{RequestData, RequestId};
    use uuid::Uuid;

    fn failed_request(deadline: Option<chrono::DateTime<chrono::Utc>>) -> Request<Failed> {
        Request {
            data: RequestData {
                id: RequestId(Uuid::new_v4()),
                batch_id: Some(BatchId(Uuid::new_v4())),
                template_id: TemplateId(Uuid::new_v4()),
                custom_id: None,
                endpoint: "http://localhost:3001".to_string(),
                method: "POST".to_string(),
                path: "/v1/responses".to_string(),
                body: "{}".to_string(),
                model: "model-a".to_string(),
                api_key: "test-key".to_string(),
                created_by: "user-a".to_string(),
                batch_metadata: Default::default(),
            },
            state: Failed {
                reason: FailureReason::TaskTerminated,
                failed_at: chrono::Utc::now(),
                claimed_at: None,
                retry_attempt: 0,
                batch_expires_at: deadline,
                routed_model: "model-a".to_string(),
            },
        }
    }

    fn retry_config() -> RetryConfig {
        RetryConfig {
            max_retries: Some(2),
            stop_before_deadline_ms: Some(1_000),
            backoff_ms: 1,
            backoff_factor: 2,
            max_backoff_ms: 10,
            max_capacity_reschedule_age_ms: Some(3_600_000),
        }
    }

    #[test]
    fn background_retry_ignores_deadline_cutoff() {
        let pending = failed_request(None)
            .can_retry(0, retry_config())
            .expect("background request should retry without a deadline");

        assert_eq!(pending.state.batch_expires_at, None);
        assert_eq!(pending.state.retry_attempt, 1);
    }

    #[test]
    fn sla_retry_still_honors_deadline_cutoff() {
        let expired = chrono::Utc::now() - chrono::Duration::seconds(1);
        assert!(
            failed_request(Some(expired))
                .can_retry(0, retry_config())
                .is_err()
        );
    }

    #[test]
    fn background_retry_still_honors_retry_count() {
        assert!(failed_request(None).can_retry(2, retry_config()).is_err());
    }

    fn failed_request_with_reason(
        reason: FailureReason,
        retry_attempt: u32,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Request<Failed> {
        let mut request = failed_request(deadline);
        request.state.reason = reason;
        request.state.retry_attempt = retry_attempt;
        request
    }

    fn batch_capacity_body() -> String {
        format!(
            r#"{{"error":{{"type":"overloaded_error","code":"{}"}}}}"#,
            crate::request::types::BATCH_CAPACITY_EXCEEDED_CODE
        )
    }

    #[test]
    fn batch_capacity_529_reschedules_without_spending_an_attempt() {
        let request = failed_request_with_reason(
            FailureReason::RetriableHttpStatus {
                status: 529,
                body: batch_capacity_body(),
            },
            2,
            Some(chrono::Utc::now() + chrono::Duration::hours(1)),
        );
        assert!(request.state.reason.is_batch_capacity_exceeded());

        let pending = request
            .can_retry_without_spending_attempt(2, retry_config())
            .expect("admission control is rescheduled");
        assert_eq!(
            pending.state.retry_attempt, 2,
            "the refusal must not consume a retry attempt"
        );
        assert!(pending.state.not_before.is_some(), "normal backoff applies");
    }

    #[test]
    fn ordinary_529_still_spends_an_attempt() {
        let request = failed_request_with_reason(
            FailureReason::RetriableHttpStatus {
                status: 529,
                body: r#"{"error":{"type":"overloaded_error"}}"#.to_string(),
            },
            1,
            Some(chrono::Utc::now() + chrono::Duration::hours(1)),
        );
        assert!(!request.state.reason.is_batch_capacity_exceeded());

        let pending = request.can_retry(1, retry_config()).expect("rescheduled");
        assert_eq!(pending.state.retry_attempt, 2);
    }

    #[test]
    fn batch_capacity_529_still_honours_the_deadline_cutoff() {
        let expired = chrono::Utc::now() - chrono::Duration::seconds(1);
        let request = failed_request_with_reason(
            FailureReason::RetriableHttpStatus {
                status: 529,
                body: batch_capacity_body(),
            },
            2,
            Some(expired),
        );
        assert!(
            request
                .can_retry_without_spending_attempt(2, retry_config())
                .is_err(),
            "the request can no longer finish in its window"
        );
    }

    fn with_created_at(
        mut request: Request<Failed>,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> Request<Failed> {
        request
            .data
            .batch_metadata
            .insert("created_at".to_string(), created_at.to_rfc3339());
        request
    }

    #[test]
    fn batch_capacity_529_without_deadline_stops_at_the_age_bound() {
        let request = with_created_at(
            failed_request_with_reason(
                FailureReason::RetriableHttpStatus {
                    status: 529,
                    body: batch_capacity_body(),
                },
                0,
                None,
            ),
            chrono::Utc::now() - chrono::Duration::milliseconds(3_600_001),
        );

        assert!(
            request
                .can_retry_without_spending_attempt(0, retry_config())
                .is_err(),
            "a deadline-less request must stop once it is older than the bound"
        );
    }

    #[test]
    fn batch_capacity_529_without_deadline_reschedules_within_the_age_bound() {
        let request = with_created_at(
            failed_request_with_reason(
                FailureReason::RetriableHttpStatus {
                    status: 529,
                    body: batch_capacity_body(),
                },
                0,
                None,
            ),
            chrono::Utc::now() - chrono::Duration::seconds(1),
        );

        assert!(
            request
                .can_retry_without_spending_attempt(0, retry_config())
                .is_ok(),
            "a young deadline-less request is still rescheduled"
        );
    }

    #[test]
    fn batch_capacity_529_with_deadline_ignores_the_age_bound() {
        let request = with_created_at(
            failed_request_with_reason(
                FailureReason::RetriableHttpStatus {
                    status: 529,
                    body: batch_capacity_body(),
                },
                0,
                Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            ),
            chrono::Utc::now() - chrono::Duration::hours(2),
        );

        assert!(
            request
                .can_retry_without_spending_attempt(0, retry_config())
                .is_ok(),
            "the batch deadline, not the age bound, governs requests with one"
        );
    }

    #[test]
    fn batch_capacity_recognition_requires_a_529_with_the_code() {
        for reason in [
            FailureReason::RetriableHttpStatus {
                status: 429,
                body: batch_capacity_body(),
            },
            FailureReason::NonRetriableHttpStatus {
                status: 529,
                body: "not json".to_string(),
            },
            FailureReason::Timeout {
                error: "timed out".to_string(),
            },
        ] {
            assert!(!reason.is_batch_capacity_exceeded(), "{reason:?}");
        }

        assert!(
            FailureReason::NonRetriableHttpStatus {
                status: 529,
                body: batch_capacity_body(),
            }
            .is_batch_capacity_exceeded()
        );
    }
}
