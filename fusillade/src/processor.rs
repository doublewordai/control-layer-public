
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::Instrument;

use crate::error::Result;
use crate::http::{HttpClient, HttpResponse};
use crate::manager::Storage;
use crate::request::{Claimed, Request, RequestCompletionResult, transitions::CancellationReason};

pub type CancellationFuture = Pin<Box<dyn std::future::Future<Output = CancellationReason> + Send>>;

pub type ShouldRetry = Arc<dyn Fn(&HttpResponse) -> bool + Send + Sync>;

#[async_trait]
pub trait RequestProcessor<S, H>: Send + Sync
where
    S: Storage + Sync,
    H: HttpClient + 'static,
{
    async fn process(
        &self,
        request: Request<Claimed>,
        http: H,
        storage: &S,
        should_retry: ShouldRetry,
        cancellation: CancellationFuture,
        ctx: ProcessContext,
    ) -> Result<RequestCompletionResult>;
}

#[derive(Debug)]
pub struct ProcessContext {
    span: tracing::Span,
}

impl ProcessContext {
    pub(crate) fn new(span: tracing::Span) -> Self {
        Self { span }
    }

    pub fn record_x_request_id(&self, id: &str) {
        self.span
            .record("x_request_id", tracing::field::display(id));
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultRequestProcessor;

#[async_trait]
impl<S, H> RequestProcessor<S, H> for DefaultRequestProcessor
where
    S: Storage + Sync,
    H: HttpClient + 'static,
{
    async fn process(
        &self,
        request: Request<Claimed>,
        http: H,
        storage: &S,
        should_retry: ShouldRetry,
        cancellation: CancellationFuture,
        ctx: ProcessContext,
    ) -> Result<RequestCompletionResult> {
        let request_id = request.data.id;
        let daemon_id = request.state.daemon_id;
        let retry_attempt = request.state.retry_attempt;
        let processing = async {
            tracing::debug!("Sending batch request to inference endpoint");
            let request_data = request.data.clone();
            let api_key = request_data.api_key.clone();
            let response_fut = async move {
                let result = http.execute(&request_data, &api_key).await;
                if let Ok(response) = &result
                    && let Some(id) = &response.x_request_id
                {
                    ctx.record_x_request_id(id);
                }
                result
            };
            request.process(storage, response_fut).await
        }
        .instrument(tracing::info_span!(
            "fusillade.state.claimed",
            otel.name = "fusillade.state.claimed",
            request_id = %request_id,
            daemon_id = %daemon_id,
            retry_attempt,
        ))
        .await?;

        let retry_attempt_at_completion = processing.state.retry_attempt;

        async {
            processing
                .complete(storage, |response| (should_retry)(response), cancellation)
                .await
        }
        .instrument(tracing::info_span!(
            "fusillade.state.processing",
            otel.name = "fusillade.state.processing",
            request_id = %request_id,
            retry_attempt = retry_attempt_at_completion,
        ))
        .await
    }
}
