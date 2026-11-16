
use std::{net::IpAddr, time::Duration};

use async_trait::async_trait;
use fusillade::API_KEY_ID_METADATA_KEY;
use fusillade::request::{Claimed, Failed, FailureReason, Request, RequestCompletionResult, RequestData};
use fusillade::{CancellationFuture, DefaultRequestProcessor, HttpClient, ProcessContext, RequestProcessor, ShouldRetry, Storage};

use crate::auth::internal_credential::InternalCredentials;

#[derive(Clone)]
pub struct DispatchAuth {
    credentials: InternalCredentials,
    ttl: Duration,
    bind_host: String,
    port: u16,
}

impl DispatchAuth {
    pub fn new(credentials: InternalCredentials, ttl: Duration, bind_host: &str, port: u16) -> Self {
        Self {
            credentials,
            ttl,
            bind_host: unbracket(bind_host).to_string(),
            port,
        }
    }

    fn reaches_this_process(&self, endpoint: &str) -> bool {
        let Ok(url) = url::Url::parse(endpoint) else {
            return false;
        };
        let Some(host) = url.host_str().map(unbracket) else {
            return false;
        };
        let local = match host.parse::<IpAddr>() {
            Ok(ip) => ip.is_loopback() || ip.is_unspecified() || host == self.bind_host,
            Err(_) => host.eq_ignore_ascii_case("localhost") || host == self.bind_host,
        };
        url.scheme() == "http" && local && url.port_or_known_default() == Some(self.port)
    }
}

fn unbracket(host: &str) -> &str {
    host.trim_start_matches('[').trim_end_matches(']')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthRefusal {
    NoKeyId,
    NotConfigured,
    ForeignEndpoint,
}

impl AuthRefusal {
    fn reason(self) -> &'static str {
        match self {
            Self::NoKeyId => "no_key_id",
            Self::NotConfigured => "not_configured",
            Self::ForeignEndpoint => "foreign_endpoint",
        }
    }
}

pub struct DispatchProcessor {
    auth: Option<DispatchAuth>,
    keystore: Option<crate::keystore::Keystore>,
    streamable_endpoints: Vec<String>,
    default: DefaultRequestProcessor,
}

impl Default for DispatchProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl DispatchProcessor {
    pub fn new() -> Self {
        Self {
            auth: None,
            keystore: None,
            streamable_endpoints: Vec::new(),
            default: DefaultRequestProcessor,
        }
    }

    pub fn with_streamable_endpoints(mut self, endpoints: Vec<String>) -> Self {
        self.streamable_endpoints = endpoints;
        self
    }

    fn should_mark(&self, path: &str) -> bool {
        self.streamable_endpoints.iter().any(|e| e == path)
    }

    pub fn with_auth(mut self, auth: DispatchAuth) -> Self {
        self.auth = Some(auth);
        self
    }

    fn authenticate(&self, data: &mut RequestData) -> Result<(), AuthRefusal> {
        data.api_key.clear();
        let api_key_id = data
            .batch_metadata
            .remove(API_KEY_ID_METADATA_KEY)
            .and_then(|id| id.parse::<uuid::Uuid>().ok())
            .ok_or(AuthRefusal::NoKeyId)?;
        let auth = self.auth.as_ref().ok_or(AuthRefusal::NotConfigured)?;
        if !auth.reaches_this_process(&data.endpoint) {
            return Err(AuthRefusal::ForeignEndpoint);
        }
        data.api_key = auth.credentials.mint(api_key_id, auth.ttl);
        Ok(())
    }

    pub fn with_keystore(mut self, keystore: Option<crate::keystore::Keystore>) -> Self {
        self.keystore = keystore;
        self
    }

    fn failed(request: Request<Claimed>, reason: FailureReason) -> Request<Failed> {
        let claimed_at = request.state.claimed_at;
        Request {
            state: Failed {
                reason,
                failed_at: chrono::Utc::now(),
                claimed_at: Some(claimed_at),
                retry_attempt: request.state.retry_attempt,
                batch_expires_at: request.state.batch_expires_at,
                routed_model: request.data.model.clone(),
            },
            data: request.data,
        }
    }

    fn zdr_unprocessable() -> FailureReason {
        FailureReason::RequestBuilderError {
            error: "Zero-data-retention request could not be processed".to_string(),
        }
    }
}

#[async_trait]
impl<S, H> RequestProcessor<S, H> for DispatchProcessor
where
    S: Storage + Send + Sync + 'static,
    H: HttpClient + Clone + Send + Sync + 'static,
{
    async fn process(
        &self,
        mut request: Request<Claimed>,
        http: H,
        storage: &S,
        should_retry: ShouldRetry,
        cancellation: CancellationFuture,
        ctx: ProcessContext,
    ) -> fusillade::Result<RequestCompletionResult> {
        if let Err(refusal) = self.authenticate(&mut request.data) {
            crate::background_error!(
                crate::metrics::errors::component::DISPATCH_AUTH,
                refusal.reason(),
                Error,
                request_id = %request.data.id.0,
                "Request cannot be authenticated for dispatch; failing request"
            );
            let failed = Self::failed(
                request,
                FailureReason::RequestBuilderError {
                    error: "Request could not be authenticated".to_string(),
                },
            );
            storage.persist(&failed).await?;
            return Ok(RequestCompletionResult::Failed(failed));
        }

        if crate::inference::zdr::is_zdr_body(&request.data.body) {
            let Some(keystore) = self.keystore.as_ref() else {
                crate::background_error!(
                    crate::metrics::errors::component::ZDR_DISPATCH,
                    "keystore_missing",
                    Error,
                    request_id = %request.data.id.0,
                    "ZDR request claimed but keystore is not configured; failing request"
                );
                let failed = Self::failed(request, Self::zdr_unprocessable());
                storage.persist(&failed).await?;
                return Ok(RequestCompletionResult::Failed(failed));
            };
            let key_id = crate::inference::zdr::key_id(&request.data.id.0, crate::inference::zdr::KeyKind::Request);
            match keystore.get(&key_id).await {
                Ok(Some(key)) => {
                    match crate::inference::zdr::decrypt_body(&key, &request.data.body) {
                        Ok(plaintext) => request.data.body = plaintext,
                        Err(e) => {
                            crate::background_error!(
                                crate::metrics::errors::component::ZDR_DISPATCH,
                                "decrypt_failed",
                                Error,
                                request_id = %request.data.id.0,
                                error = %e,
                                "ZDR request body could not be decrypted; failing request"
                            );
                            let failed = Self::failed(request, Self::zdr_unprocessable());
                            storage.persist(&failed).await?;
                            return Ok(RequestCompletionResult::Failed(failed));
                        }
                    }
                    request
                        .data
                        .batch_metadata
                        .insert(crate::inference::zdr::ZDR_MARKER_KEY.to_string(), "1".to_string());
                }
                Ok(None) => {
                    crate::background_error!(
                        crate::metrics::errors::component::ZDR_DISPATCH,
                        "key_expired",
                        Error,
                        request_id = %request.data.id.0,
                        "ZDR request key expired or was deleted before dispatch; cannot decrypt"
                    );
                    let failed = Self::failed(request, Self::zdr_unprocessable());
                    storage.persist(&failed).await?;
                    return Ok(RequestCompletionResult::Failed(failed));
                }
                Err(e) => {
                    crate::background_error!(
                        crate::metrics::errors::component::ZDR_DISPATCH,
                        "keystore_unreachable",
                        Warning,
                        request_id = %request.data.id.0,
                        error = %e,
                        "ZDR keystore unreachable during dispatch; scheduling retry"
                    );
                    let failed = Self::failed(
                        request,
                        FailureReason::NetworkError {
                            error: "Zero-data-retention request could not be processed".to_string(),
                        },
                    );
                    return Ok(RequestCompletionResult::Failed(failed));
                }
            }
        }

        if self.should_mark(&request.data.path) {
            request
                .data
                .batch_metadata
                .insert(crate::inference::outbound_request::STREAM_MARKER_KEY.to_string(), "1".to_string());
        }

        self.default.process(request, http, storage, should_retry, cancellation, ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn processor() -> DispatchProcessor {
        DispatchProcessor::new().with_streamable_endpoints(vec!["/v1/chat/completions".to_string()])
    }

    #[test]
    fn a_streamable_path_is_marked() {
        assert!(processor().should_mark("/v1/chat/completions"));
    }

    #[test]
    fn a_path_that_is_not_streamable_is_left_unmarked() {
        assert!(!processor().should_mark("/v1/embeddings"));
    }

    #[test]
    fn no_configuration_marks_nothing() {
        assert!(!DispatchProcessor::new().should_mark("/v1/chat/completions"));
    }

    const PORT: u16 = 3001;

    fn authenticating(credentials: &InternalCredentials) -> DispatchProcessor {
        DispatchProcessor::new().with_auth(DispatchAuth::new(credentials.clone(), Duration::from_secs(60), "0.0.0.0", PORT))
    }

    fn request(endpoint: &str, api_key_id: Option<uuid::Uuid>) -> RequestData {
        RequestData {
            id: fusillade::RequestId(uuid::Uuid::new_v4()),
            batch_id: None,
            template_id: fusillade::TemplateId(uuid::Uuid::new_v4()),
            custom_id: None,
            endpoint: endpoint.to_string(),
            method: "POST".to_string(),
            path: "/v1/chat/completions".to_string(),
            body: "{}".to_string(),
            model: "model".to_string(),
            api_key: String::new(),
            created_by: String::new(),
            batch_metadata: api_key_id
                .map(|id| (API_KEY_ID_METADATA_KEY.to_string(), id.to_string()))
                .into_iter()
                .collect(),
        }
    }

    #[test]
    fn a_key_id_gets_a_credential() {
        let credentials = InternalCredentials::generate();
        let key = uuid::Uuid::new_v4();
        let mut data = request("http://0.0.0.0:3001/ai", Some(key));
        authenticating(&credentials).authenticate(&mut data).unwrap();
        assert_eq!(credentials.verify(&data.api_key), Ok(key));
        assert!(InternalCredentials::generate().verify(&data.api_key).is_err());
        assert!(data.batch_metadata.is_empty(), "the key id is not forwarded");
    }

    #[test]
    fn a_request_without_a_key_id_is_refused() {
        let mut data = request("http://127.0.0.1:3001/ai", None);
        data.api_key = "sk-legacy".to_string();
        assert_eq!(
            authenticating(&InternalCredentials::generate()).authenticate(&mut data),
            Err(AuthRefusal::NoKeyId)
        );
        assert_eq!(data.api_key, "");
    }

    #[test]
    fn a_credential_is_only_minted_for_this_process() {
        let credentials = InternalCredentials::generate();
        for endpoint in [
            "http://10.0.0.5:3001/ai",
            "http://api.example.com:3001/ai",
            "https://127.0.0.1:3001/ai",
            "http://127.0.0.1:3002/ai",
            "not a url",
        ] {
            let mut data = request(endpoint, Some(uuid::Uuid::new_v4()));
            assert_eq!(
                authenticating(&credentials).authenticate(&mut data),
                Err(AuthRefusal::ForeignEndpoint),
                "{endpoint}"
            );
            assert_eq!(data.api_key, "", "{endpoint}");
        }

        let mut data = request("http://127.0.0.1:3001/ai", Some(uuid::Uuid::new_v4()));
        assert_eq!(DispatchProcessor::new().authenticate(&mut data), Err(AuthRefusal::NotConfigured));
        assert_eq!(data.api_key, "");
    }

    #[test]
    fn this_process_is_recognised_by_every_local_address() {
        let auth = DispatchAuth::new(InternalCredentials::generate(), Duration::from_secs(60), "10.1.2.3", PORT);
        for endpoint in [
            "http://127.0.0.1:3001/ai",
            "http://[::1]:3001/ai",
            "http://0.0.0.0:3001/ai",
            "http://localhost:3001/ai",
            "http://10.1.2.3:3001/ai",
        ] {
            assert!(auth.reaches_this_process(endpoint), "{endpoint}");
        }
        assert!(!auth.reaches_this_process("http://10.1.2.4:3001/ai"));
    }
}
