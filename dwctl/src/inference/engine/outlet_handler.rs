
use chrono::{DateTime, Utc};
use outlet::{RequestData, RequestHandler, ResponseData};
use tokio::sync::mpsc::error::TrySendError;
use uuid::Uuid;

use super::writer::{RawCompletedRequest, RequestsWriterSender};
use crate::auth::internal_credential::{InternalAuthState, carried_key_hash};
use crate::crypto::ApiKeyHash;
use crate::inference::store::{ONWARDS_RESPONSE_ID_HEADER, lookup_created_by};

#[derive(Clone)]
pub struct FusilladeOutletHandler {
    sender: RequestsWriterSender,
    dwctl_pool: sqlx_pool_router::DynPools,
    internal_auth: InternalAuthState,
}

impl FusilladeOutletHandler {
    pub fn new(sender: RequestsWriterSender, dwctl_pool: impl sqlx_pool_router::PoolProvider, internal_auth: InternalAuthState) -> Self {
        Self {
            sender,
            dwctl_pool: sqlx_pool_router::DynPools::new(dwctl_pool),
            internal_auth,
        }
    }

    fn extract_response_id(request: &RequestData) -> Option<String> {
        Self::header_str(request, ONWARDS_RESPONSE_ID_HEADER).map(String::from)
    }

    fn extract_request_id(request: &RequestData) -> Option<Uuid> {
        Self::header_str(request, "x-fusillade-request-id").and_then(|s| Uuid::parse_str(s).ok())
    }

    fn extract_api_key(request: &RequestData) -> Option<String> {
        Self::header_str(request, "authorization")
            .and_then(|s| s.strip_prefix("Bearer "))
            .map(String::from)
    }

    fn header_str<'a>(request: &'a RequestData, name: &str) -> Option<&'a str> {
        request
            .headers
            .get(name)
            .and_then(|values| values.first())
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
    }

    fn extract_complete_response_ctx(request: &RequestData) -> Option<CompleteResponseCtx> {
        let response_id = Self::extract_response_id(request)?;

        let request_id = match Self::extract_request_id(request) {
            Some(id) => id,
            None => {
                tracing::warn!(response_id = %response_id, "Missing x-fusillade-request-id header — skipping enqueue");
                return None;
            }
        };

        let model = Self::header_str(request, "x-onwards-model").unwrap_or("unknown").to_string();
        let endpoint = Self::header_str(request, "x-onwards-endpoint").unwrap_or("").to_string();
        let api_key = Self::extract_api_key(request);

        if endpoint.is_empty() {
            tracing::warn!(
                response_id = %response_id,
                uri = %request.uri,
                "Missing x-onwards-endpoint header — complete-response synthesize will fail if create-response hasn't run"
            );
        }

        Some(CompleteResponseCtx {
            response_id,
            request_id,
            model,
            endpoint,
            api_key,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
struct CompleteResponseCtx {
    response_id: String,
    request_id: Uuid,
    model: String,
    endpoint: String,
    api_key: Option<String>,
}

impl FusilladeOutletHandler {
    async fn resolve_attribution(&self, ctx: &CompleteResponseCtx, carried: Option<ApiKeyHash>) -> Option<String> {
        let api_key = match ctx.api_key.as_deref() {
            Some(key) if !key.is_empty() => key.to_string(),
            _ => {
                tracing::debug!(
                    response_id = %ctx.response_id,
                    "Skipping response writer send - no api_key on request"
                );
                metrics::counter!("dwctl_requests_writer_dropped_total", "reason" => "missing_api_key").increment(1);
                return None;
            }
        };
        let key_hash = self.internal_auth.captured_key_hash(Some(&api_key), carried);
        let created_by = lookup_created_by(&self.dwctl_pool.write(), key_hash.as_ref()).await;
        match created_by {
            Some(uid) if !uid.is_empty() => Some(uid),
            _ => {
                tracing::debug!(
                    response_id = %ctx.response_id,
                    "Skipping response writer send - api_key did not resolve to a user"
                );
                metrics::counter!("dwctl_requests_writer_dropped_total", "reason" => "unknown_api_key").increment(1);
                None
            }
        }
    }
}

fn queue_for_writer(sender: &RequestsWriterSender, record: RawCompletedRequest, response_id: &str, kind: &'static str) {
    match sender.try_send(record) {
        Ok(()) => {
            metrics::counter!("dwctl_requests_writer_sends_total", "result" => "ok").increment(1);
        }
        Err(TrySendError::Full(_)) => {
            metrics::counter!("dwctl_requests_writer_dropped_total", "reason" => "channel_full").increment(1);
            tracing::debug!(response_id = %response_id, kind, "Dropping record: requests writer channel is full");
        }
        Err(TrySendError::Closed(_)) => {
            metrics::counter!("dwctl_requests_writer_sends_total", "result" => "err").increment(1);
            tracing::warn!(response_id = %response_id, kind, "Failed to send record to requests writer (channel closed)");
        }
    }
}

impl RequestHandler for FusilladeOutletHandler {
    async fn handle_request(&self, _data: RequestData) {}

    fn handle_response(&self, request_data: RequestData, response_data: ResponseData) -> impl std::future::Future<Output = ()> + Send {
        let sender = self.sender.clone();
        let handler = self.clone();

        async move {
            let Some(ctx) = Self::extract_complete_response_ctx(&request_data) else {
                return;
            };
            let carried = carried_key_hash(&response_data.extensions);
            let Some(created_by) = handler.resolve_attribution(&ctx, carried).await else {
                return;
            };

            let status_code = response_data.status.as_u16();
            let response_body = response_data
                .body
                .as_ref()
                .and_then(|b| std::str::from_utf8(b).ok())
                .unwrap_or("")
                .to_string();
            let request_body = request_data
                .body
                .as_ref()
                .and_then(|b| std::str::from_utf8(b).ok())
                .unwrap_or("")
                .to_string();

            let (response_body, request_body) = if request_is_zdr(&request_data) {
                (String::new(), String::new())
            } else {
                (response_body, request_body)
            };

            let started_at: DateTime<Utc> = request_data.timestamp.into();
            let completed_at = started_at + response_duration(&response_data);

            queue_for_writer(
                &sender,
                RawCompletedRequest {
                    request_id: ctx.request_id,
                    status_code,
                    response_body,
                    request_body,
                    model: ctx.model,
                    endpoint: ctx.endpoint,
                    created_by,
                    started_at,
                    completed_at,
                },
                &ctx.response_id,
                "completed-response",
            );
        }
    }

    fn handle_abandoned(&self, request_data: RequestData) -> impl std::future::Future<Output = ()> + Send {
        let sender = self.sender.clone();
        let handler = self.clone();

        async move {
            let Some(ctx) = Self::extract_complete_response_ctx(&request_data) else {
                return;
            };
            let Some(created_by) = handler.resolve_attribution(&ctx, None).await else {
                return;
            };

            const STATUS_CLIENT_CLOSED: u16 = 499;
            let abandoned_body = serde_json::json!({
                "error": {
                    "type": "client_disconnected",
                    "message": "client cancelled request before upstream response",
                    "code": STATUS_CLIENT_CLOSED,
                }
            })
            .to_string();

            let started_at: DateTime<Utc> = request_data.timestamp.into();

            queue_for_writer(
                &sender,
                RawCompletedRequest {
                    request_id: ctx.request_id,
                    status_code: STATUS_CLIENT_CLOSED,
                    response_body: abandoned_body,
                    request_body: String::new(),
                    model: ctx.model,
                    endpoint: ctx.endpoint,
                    created_by,
                    started_at,
                    completed_at: started_at,
                },
                &ctx.response_id,
                "abandoned-response",
            );
        }
    }
}

fn response_duration(response: &ResponseData) -> chrono::Duration {
    chrono::Duration::from_std(response.duration).unwrap_or_else(|_| chrono::Duration::zero())
}

fn request_is_zdr(request: &RequestData) -> bool {
    request
        .headers
        .get(crate::inference::zdr::ZDR_MARKER_HEADER)
        .and_then(|values| values.first())
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        == Some("1")
}

#[derive(Clone)]
pub struct ZdrBodyScrubber<H> {
    inner: H,
}

impl<H> ZdrBodyScrubber<H> {
    pub fn new(inner: H) -> Self {
        Self { inner }
    }
}

impl<H: RequestHandler> RequestHandler for ZdrBodyScrubber<H> {
    fn handle_request(&self, mut data: RequestData) -> impl std::future::Future<Output = ()> + Send {
        if request_is_zdr(&data) {
            data.body = None;
        }
        self.inner.handle_request(data)
    }

    fn handle_response(
        &self,
        mut request_data: RequestData,
        mut response_data: ResponseData,
    ) -> impl std::future::Future<Output = ()> + Send {
        if request_is_zdr(&request_data) {
            request_data.body = None;
            response_data.body = None;
        }
        self.inner.handle_response(request_data, response_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::time::SystemTime;

    fn make_request_data(headers: HashMap<String, Vec<Bytes>>) -> RequestData {
        RequestData {
            correlation_id: 1,
            timestamp: SystemTime::now(),
            method: axum::http::Method::POST,
            uri: "/v1/responses".parse().unwrap(),
            headers,
            body: None,
            trace_id: None,
            span_id: None,
        }
    }

    fn writer_record() -> RawCompletedRequest {
        let started_at = chrono::Utc::now();
        RawCompletedRequest {
            request_id: Uuid::new_v4(),
            status_code: 200,
            response_body: "x".repeat(64 * 1024),
            request_body: "y".repeat(64 * 1024),
            model: "test-model".to_string(),
            endpoint: "/v1/responses".to_string(),
            created_by: Uuid::new_v4().to_string(),
            started_at,
            completed_at: started_at,
        }
    }

    #[test]
    fn test_queue_for_writer_drops_instead_of_waiting_when_the_writer_is_behind() {
        let (sender, _receiver) = RequestsWriterSender::full_for_test(writer_record());
        assert_eq!(sender.queued_records(), 1);

        queue_for_writer(&sender, writer_record(), "resp_test", "completed-response");

        assert_eq!(sender.queued_records(), 1, "the dropped record must not be counted as queued");
    }

    #[test]
    fn test_extract_response_id_present() {
        let mut headers = HashMap::new();
        headers.insert(
            ONWARDS_RESPONSE_ID_HEADER.to_string(),
            vec![Bytes::from("resp_12345678-1234-1234-1234-123456789abc")],
        );
        let request = make_request_data(headers);
        let id = FusilladeOutletHandler::extract_response_id(&request);
        assert_eq!(id, Some("resp_12345678-1234-1234-1234-123456789abc".to_string()));
    }

    #[test]
    fn test_extract_response_id_absent() {
        let request = make_request_data(HashMap::new());
        let id = FusilladeOutletHandler::extract_response_id(&request);
        assert!(id.is_none());
    }

    #[test]
    fn test_extract_response_id_present_with_fusillade_request_header() {
        let mut headers = HashMap::new();
        headers.insert("x-fusillade-request-id".to_string(), vec![Bytes::from("some-id")]);
        headers.insert(
            ONWARDS_RESPONSE_ID_HEADER.to_string(),
            vec![Bytes::from("resp_12345678-1234-1234-1234-123456789abc")],
        );
        let request = make_request_data(headers);
        let id = FusilladeOutletHandler::extract_response_id(&request);
        assert_eq!(id, Some("resp_12345678-1234-1234-1234-123456789abc".to_string()));
    }

    #[test]
    fn test_extract_request_id_present() {
        let mut headers = HashMap::new();
        let uuid_str = "12345678-1234-1234-1234-123456789abc";
        headers.insert("x-fusillade-request-id".to_string(), vec![Bytes::from(uuid_str)]);
        let request = make_request_data(headers);
        let id = FusilladeOutletHandler::extract_request_id(&request);
        assert_eq!(id, Some(Uuid::parse_str(uuid_str).unwrap()));
    }

    #[test]
    fn test_extract_request_id_absent() {
        let request = make_request_data(HashMap::new());
        assert!(FusilladeOutletHandler::extract_request_id(&request).is_none());
    }

    #[test]
    fn test_extract_request_id_invalid_uuid() {
        let mut headers = HashMap::new();
        headers.insert("x-fusillade-request-id".to_string(), vec![Bytes::from("not-a-uuid")]);
        let request = make_request_data(headers);
        assert!(FusilladeOutletHandler::extract_request_id(&request).is_none());
    }

    #[test]
    fn test_extract_api_key_strips_bearer_prefix() {
        let mut headers = HashMap::new();
        headers.insert("authorization".to_string(), vec![Bytes::from("Bearer sk-test-123")]);
        let request = make_request_data(headers);
        assert_eq!(FusilladeOutletHandler::extract_api_key(&request), Some("sk-test-123".to_string()));
    }

    #[test]
    fn test_extract_api_key_without_bearer_prefix_is_none() {
        let mut headers = HashMap::new();
        headers.insert("authorization".to_string(), vec![Bytes::from("sk-test-123")]);
        let request = make_request_data(headers);
        assert!(FusilladeOutletHandler::extract_api_key(&request).is_none());
    }

    #[test]
    fn test_extract_api_key_absent() {
        let request = make_request_data(HashMap::new());
        assert!(FusilladeOutletHandler::extract_api_key(&request).is_none());
    }

    fn full_headers() -> HashMap<String, Vec<Bytes>> {
        let mut headers = HashMap::new();
        headers.insert(
            ONWARDS_RESPONSE_ID_HEADER.to_string(),
            vec![Bytes::from("resp_12345678-1234-1234-1234-123456789abc")],
        );
        headers.insert(
            "x-fusillade-request-id".to_string(),
            vec![Bytes::from("12345678-1234-1234-1234-123456789abc")],
        );
        headers.insert("x-onwards-model".to_string(), vec![Bytes::from("Qwen/Qwen3.5-9B")]);
        headers.insert("x-onwards-endpoint".to_string(), vec![Bytes::from("http://127.0.0.1:3001/ai")]);
        headers.insert("authorization".to_string(), vec![Bytes::from("Bearer sk-test")]);
        headers
    }

    #[test]
    fn test_extract_complete_response_ctx_all_headers_present() {
        let request = make_request_data(full_headers());
        let ctx = FusilladeOutletHandler::extract_complete_response_ctx(&request).expect("should extract");
        assert_eq!(ctx.response_id, "resp_12345678-1234-1234-1234-123456789abc");
        assert_eq!(ctx.request_id, Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap());
        assert_eq!(ctx.model, "Qwen/Qwen3.5-9B");
        assert_eq!(ctx.endpoint, "http://127.0.0.1:3001/ai");
        assert_eq!(ctx.api_key, Some("sk-test".to_string()));
    }

    #[test]
    fn test_extract_complete_response_ctx_missing_response_id_returns_none() {
        let mut headers = full_headers();
        headers.remove(ONWARDS_RESPONSE_ID_HEADER);
        let request = make_request_data(headers);
        assert!(FusilladeOutletHandler::extract_complete_response_ctx(&request).is_none());
    }

    #[test]
    fn test_extract_complete_response_ctx_missing_request_id_returns_none() {
        let mut headers = full_headers();
        headers.remove("x-fusillade-request-id");
        let request = make_request_data(headers);
        assert!(FusilladeOutletHandler::extract_complete_response_ctx(&request).is_none());
    }

    #[test]
    fn test_extract_complete_response_ctx_invalid_request_id_returns_none() {
        let mut headers = full_headers();
        headers.insert("x-fusillade-request-id".to_string(), vec![Bytes::from("not-a-uuid")]);
        let request = make_request_data(headers);
        assert!(FusilladeOutletHandler::extract_complete_response_ctx(&request).is_none());
    }

    #[test]
    fn test_extract_complete_response_ctx_missing_endpoint_warns_but_returns_some() {
        let mut headers = full_headers();
        headers.remove("x-onwards-endpoint");
        let request = make_request_data(headers);
        let ctx = FusilladeOutletHandler::extract_complete_response_ctx(&request).expect("should still extract");
        assert_eq!(ctx.endpoint, "");
    }

    #[test]
    fn test_extract_complete_response_ctx_missing_model_defaults_to_unknown() {
        let mut headers = full_headers();
        headers.remove("x-onwards-model");
        let request = make_request_data(headers);
        let ctx = FusilladeOutletHandler::extract_complete_response_ctx(&request).expect("should extract");
        assert_eq!(ctx.model, "unknown");
    }

    #[test]
    fn test_extract_complete_response_ctx_missing_api_key_is_none() {
        let mut headers = full_headers();
        headers.remove("authorization");
        let request = make_request_data(headers);
        let ctx = FusilladeOutletHandler::extract_complete_response_ctx(&request).expect("should extract");
        assert_eq!(ctx.api_key, None);
    }

    #[derive(Clone, Default)]
    struct BodyRecorder {
        req: std::sync::Arc<std::sync::Mutex<Option<Option<Bytes>>>>,
        resp: std::sync::Arc<std::sync::Mutex<Option<Option<Bytes>>>>,
    }

    impl RequestHandler for BodyRecorder {
        fn handle_request(&self, data: RequestData) -> impl std::future::Future<Output = ()> + Send {
            *self.req.lock().unwrap() = Some(data.body.clone());
            async {}
        }

        fn handle_response(&self, request_data: RequestData, response_data: ResponseData) -> impl std::future::Future<Output = ()> + Send {
            *self.req.lock().unwrap() = Some(request_data.body.clone());
            *self.resp.lock().unwrap() = Some(response_data.body.clone());
            async {}
        }
    }

    fn body_request(zdr: bool) -> RequestData {
        let mut headers = HashMap::new();
        if zdr {
            headers.insert(crate::inference::zdr::ZDR_MARKER_HEADER.to_string(), vec![Bytes::from_static(b"1")]);
        }
        let mut data = make_request_data(headers);
        data.body = Some(Bytes::from_static(br#"{"secret":"prompt"}"#));
        data
    }

    fn body_response() -> ResponseData {
        ResponseData {
            extensions: Default::default(),
            correlation_id: 1,
            timestamp: SystemTime::now(),
            status: axum::http::StatusCode::OK,
            headers: HashMap::new(),
            body: Some(Bytes::from_static(br#"{"secret":"reply"}"#)),
            duration_to_first_byte: std::time::Duration::from_millis(1),
            duration: std::time::Duration::from_millis(2),
        }
    }

    #[tokio::test]
    async fn zdr_scrubber_blanks_marked_request_body() {
        let rec = BodyRecorder::default();
        ZdrBodyScrubber::new(rec.clone()).handle_request(body_request(true)).await;
        assert_eq!(*rec.req.lock().unwrap(), Some(None), "ZDR request body must be blanked");
    }

    #[tokio::test]
    async fn zdr_scrubber_blanks_marked_response_bodies() {
        let rec = BodyRecorder::default();
        ZdrBodyScrubber::new(rec.clone())
            .handle_response(body_request(true), body_response())
            .await;
        assert_eq!(
            *rec.req.lock().unwrap(),
            Some(None),
            "ZDR request body must be blanked on response log"
        );
        assert_eq!(*rec.resp.lock().unwrap(), Some(None), "ZDR response body must be blanked");
    }

    #[tokio::test]
    async fn zdr_scrubber_passes_through_unmarked() {
        let rec = BodyRecorder::default();
        ZdrBodyScrubber::new(rec.clone())
            .handle_response(body_request(false), body_response())
            .await;
        assert!(
            rec.req.lock().unwrap().clone().unwrap().is_some(),
            "non-ZDR request body must pass through"
        );
        assert!(
            rec.resp.lock().unwrap().clone().unwrap().is_some(),
            "non-ZDR response body must pass through"
        );
    }
}
