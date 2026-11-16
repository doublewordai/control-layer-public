
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use axum::http::{Method, StatusCode, Uri};
use bytes::Bytes;
use outlet::{RequestData, ResponseData};
use outlet_postgres::SerializationError;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum RecomputeError {
    #[error("stored response could not be parsed: {0}")]
    Parse(SerializationError),
    #[error("request has no stored response body")]
    NoResponseBody,
    #[error("stored endpoint {0:?} is not a valid request path")]
    BadEndpoint(String),
    #[error("stored status code {0} is not a valid HTTP status")]
    BadStatus(u16),
    #[error("payload is zero-data-retention encrypted; tokens cannot be verified")]
    ZeroDataRetention,
    #[error("response carries no usage object; tokens cannot be re-read from the body (needs the tokenizer path)")]
    NoUsage,
}

const ZDR_ENVELOPE_PREFIX: &[u8] = b"dwzdr1:";

pub fn is_zdr_envelope(body: Option<&[u8]>) -> bool {
    body.is_some_and(|b| b.starts_with(ZDR_ENVELOPE_PREFIX))
}

#[derive(Debug, Clone)]
pub struct StoredExchange {
    pub endpoint: String,
    pub request_body: Option<Vec<u8>>,
    pub response_body: Option<Vec<u8>>,
    pub status_code: u16,
    pub streamed: bool,
}

impl StoredExchange {
    pub fn streamed_from_body(request_body: Option<&[u8]>) -> bool {
        request_body
            .and_then(|b| serde_json::from_slice::<Value>(b).ok())
            .and_then(|v| v.get("stream").and_then(Value::as_bool))
            .unwrap_or(false)
    }

    pub fn to_outlet_pair(&self) -> Result<(RequestData, ResponseData), RecomputeError> {
        let Some(response_body) = &self.response_body else {
            return Err(RecomputeError::NoResponseBody);
        };
        if is_zdr_envelope(Some(response_body)) || is_zdr_envelope(self.request_body.as_deref()) {
            return Err(RecomputeError::ZeroDataRetention);
        }

        let uri: Uri = self
            .endpoint
            .parse()
            .map_err(|_| RecomputeError::BadEndpoint(self.endpoint.clone()))?;

        let status = StatusCode::from_u16(self.status_code).map_err(|_| RecomputeError::BadStatus(self.status_code))?;

        let headers: HashMap<String, Vec<Bytes>> = HashMap::new();

        let request_data = RequestData {
            correlation_id: 0,
            timestamp: SystemTime::UNIX_EPOCH,
            method: Method::POST,
            uri,
            headers,
            body: self.request_body.clone().map(Bytes::from),
            trace_id: None,
            span_id: None,
        };

        let response_data = ResponseData {
            extensions: Default::default(),
            correlation_id: 0,
            timestamp: SystemTime::UNIX_EPOCH,
            status,
            headers: HashMap::new(),
            body: Some(Bytes::from(response_body.clone())),
            duration: Duration::ZERO,
            duration_to_first_byte: Duration::ZERO,
        };

        Ok((request_data, response_data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recompute::cache_fields::CreationTier;
    use crate::recompute::{TokenSource, recompute_from_stored_response};

    fn exchange(endpoint: &str, request: &str, response: &str) -> StoredExchange {
        StoredExchange {
            endpoint: endpoint.to_string(),
            request_body: Some(request.as_bytes().to_vec()),
            response_body: Some(response.as_bytes().to_vec()),
            status_code: 200,
            streamed: false,
        }
    }

    #[test]
    fn healthy_chat_completion_recomputes_to_the_same_numbers() {
        let e = exchange(
            "/v1/chat/completions",
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#,
            r#"{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":20000,"completion_tokens":8,"total_tokens":20008}}"#,
        );
        let (req, resp) = e.to_outlet_pair().unwrap();
        let got = recompute_from_stored_response(&req, &resp, CreationTier::FiveMinute).unwrap();

        assert_eq!(got.counts.prompt, 20_000);
        assert_eq!(got.counts.completion, 8);
        assert_eq!(got.prompt_source, TokenSource::Reported);
        assert!(got.is_exact());
    }

    #[test]
    fn anthropic_cached_request_recovers_the_total_and_preserves_the_split() {
        let e = exchange(
            "/v1/messages",
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":16}"#,
            r#"{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":5000,"output_tokens":8,"cache_read_input_tokens":12000,"cache_creation_input_tokens":3000,"cache_creation":{"ephemeral_1h_input_tokens":3000}}}"#,
        );
        let (req, resp) = e.to_outlet_pair().unwrap();
        let got = recompute_from_stored_response(&req, &resp, CreationTier::FiveMinute).unwrap();

        assert_eq!(got.counts.prompt, 20_000, "cached tokens are folded back into the total");
        assert_eq!(got.counts.cache_read, 12_000, "the split is carried through untouched");
        assert_eq!(got.counts.cache_creation_1h, 3_000);
        assert!(got.is_exact());
    }

    #[test]
    fn blocking_body_is_recognised_regardless_of_endpoint() {
        let anthropic_body = r#"{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":5000,"output_tokens":8}}"#;
        let request = r#"{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":16}"#;

        for endpoint in ["/v1/messages", "/v1/chat/completions"] {
            let (req, resp) = exchange(endpoint, request, anthropic_body).to_outlet_pair().unwrap();
            let got = recompute_from_stored_response(&req, &resp, CreationTier::FiveMinute).unwrap();
            assert_eq!(got.counts.prompt, 5_000, "blocking shape recovered via {endpoint}");
        }
    }

    #[test]
    fn streaming_dispatch_depends_on_the_endpoint() {
        let sse = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"m\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":5000,\"output_tokens\":0,\"cache_read_input_tokens\":12000,\"cache_creation_input_tokens\":3000}}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"input_tokens\":5000,\"output_tokens\":8,\"cache_read_input_tokens\":12000,\"cache_creation_input_tokens\":3000}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        let request = r#"{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"stream":true}"#;

        let mut right = exchange("/v1/messages", request, sse);
        right.streamed = true;
        let (req, resp) = right.to_outlet_pair().unwrap();
        let right = recompute_from_stored_response(&req, &resp, CreationTier::FiveMinute).unwrap();
        assert_eq!(right.counts.prompt, 20_000, "5000 input + 12000 read + 3000 creation");
        assert_eq!(right.counts.completion, 8);

        let mut wrong = exchange("/v1/chat/completions", request, sse);
        wrong.streamed = true;
        let (req, resp) = wrong.to_outlet_pair().unwrap();
        let wrong = recompute_from_stored_response(&req, &resp, CreationTier::FiveMinute).unwrap();
        assert_eq!(
            wrong.counts.prompt, 20_000,
            "the chat-completion SSE parser finds no usage in an Anthropic event stream, \
             but the raw-usage fallback recovers the terminal frame's counts"
        );
        assert_eq!(wrong.counts.completion, 8);
    }

    #[test]
    fn a_request_with_no_response_is_refused_not_zeroed() {
        let e = StoredExchange {
            endpoint: "/v1/chat/completions".to_string(),
            request_body: Some(b"{}".to_vec()),
            response_body: None,
            status_code: 0,
            streamed: false,
        };
        assert!(matches!(e.to_outlet_pair(), Err(RecomputeError::NoResponseBody)));
    }

    #[test]
    fn an_unparseable_endpoint_is_refused_not_guessed() {
        let e = StoredExchange {
            endpoint: "not a uri".to_string(),
            request_body: None,
            response_body: Some(b"{}".to_vec()),
            status_code: 200,
            streamed: false,
        };
        assert!(matches!(e.to_outlet_pair(), Err(RecomputeError::BadEndpoint(_))));
    }

    #[test]
    fn real_incident_row_recomputes_to_the_measured_truth() {
        let e = exchange(
            "/messages?beta=true",
            r#"{"model":"deepseek-ai/DeepSeek-V4-Flash","messages":[{"role":"user","content":"hi"}],"max_tokens":1024}"#,
            r#"{"id":"msg_1","type":"message","role":"assistant","model":"deepseek-ai/DeepSeek-V4-Flash","content":[],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":565,"output_tokens":658,"cache_read_input_tokens":17631,"cache_creation_input_tokens":538}}"#,
        );
        let (req, resp) = e.to_outlet_pair().unwrap();
        let got = recompute_from_stored_response(&req, &resp, CreationTier::FiveMinute).unwrap();

        assert_eq!(got.counts.prompt, 18_734, "565 + 17631 + 538");
        assert_eq!(got.counts.completion, 658);
        assert_eq!(got.counts.cache_read, 17_631);
        assert_eq!(got.counts.cache_creation_5m, 538, "flat creation must not read as zero");
        assert!(got.cache_tier_inferred, "the body gave no tier - say so in the report");

        assert_eq!(
            got.counts.prompt - got.counts.cache_read - got.counts.cache_creation_5m,
            565,
            "uncached input is the provider's input_tokens"
        );
    }

    #[test]
    fn dynamo_body_without_role_recovers_counts_from_raw_usage() {
        let e = exchange(
            "/chat/completions",
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#,
            r#"{"choices":[{"finish_reason":"stop","index":0,"message":{"content":"done","reasoning_content":"thinking"}}],"created":1786737102,"id":"dyn-1","model":"m","object":"chat.completion","usage":{"cache_creation":{"ephemeral_1h_input_tokens":0,"ephemeral_24h_input_tokens":0,"ephemeral_5m_input_tokens":340},"cache_creation_input_tokens":340,"cache_read_input_tokens":30974,"completion_tokens":74,"completion_tokens_details":{"reasoning_tokens":21},"prompt_tokens":32558,"prompt_tokens_details":{"cached_tokens":30974},"total_tokens":32632}}"#,
        );
        let (req, resp) = e.to_outlet_pair().unwrap();
        let got = recompute_from_stored_response(&req, &resp, CreationTier::FiveMinute).unwrap();

        assert_eq!(got.counts.prompt, 32_558, "recovered from the raw usage object");
        assert_eq!(got.counts.completion, 74);
        assert_eq!(got.reasoning, 21);
        assert_eq!(got.total, 32_632);
        assert_eq!(got.counts.cache_read, 30_974);
        assert_eq!(got.counts.cache_creation_5m, 340, "nested split read as before");
        assert!(!got.cache_tier_inferred, "the body stated the tier");
        assert_eq!(got.prompt_source, TokenSource::Reported, "still the provider's own numbers");
    }

    #[test]
    fn null_usage_is_refused_not_reported_as_a_healthy_zero() {
        let e = exchange(
            "/chat/completions",
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#,
            r#"{"choices":[{"finish_reason":"tool_calls","index":0,"message":{"role":"assistant","content":null}}],"created":1,"id":"c","model":"m","object":"chat.completion","usage":null}"#,
        );
        let (req, resp) = e.to_outlet_pair().unwrap();
        let got = recompute_from_stored_response(&req, &resp, CreationTier::FiveMinute);
        assert!(
            matches!(got, Err(RecomputeError::NoUsage)),
            "a usage-less body is absence of evidence, not a zero measurement: {got:?}"
        );
    }

    #[test]
    fn an_explicit_zero_usage_object_is_a_measurement_not_a_refusal() {
        let e = exchange(
            "/chat/completions",
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#,
            r#"{"choices":[{"finish_reason":"stop","index":0,"message":{"role":"assistant","content":""}}],"created":1,"id":"c","model":"m","object":"chat.completion","usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}}"#,
        );
        let (req, resp) = e.to_outlet_pair().unwrap();
        let got = recompute_from_stored_response(&req, &resp, CreationTier::FiveMinute).unwrap();
        assert_eq!(got.counts.prompt, 0);
        assert_eq!(got.counts.completion, 0);
    }

    #[test]
    fn reasoning_and_total_are_recomputed_not_dropped() {
        let e = exchange(
            "/v1/chat/completions",
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#,
            r#"{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}],"usage":{"prompt_tokens":100,"completion_tokens":80,"total_tokens":180,"completion_tokens_details":{"reasoning_tokens":60}}}"#,
        );
        let (req, resp) = e.to_outlet_pair().unwrap();
        let got = recompute_from_stored_response(&req, &resp, CreationTier::FiveMinute).unwrap();

        assert_eq!(got.counts.prompt, 100);
        assert_eq!(got.counts.completion, 80);
        assert_eq!(got.reasoning, 60, "reasoning is a subset of completion, not an addition");
        assert_eq!(got.total, 180);
    }

    #[test]
    fn anthropic_total_is_derived_and_reasoning_is_zero() {
        let e = exchange(
            "/v1/messages",
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":16}"#,
            r#"{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":565,"output_tokens":658,"cache_read_input_tokens":17631,"cache_creation_input_tokens":538}}"#,
        );
        let (req, resp) = e.to_outlet_pair().unwrap();
        let got = recompute_from_stored_response(&req, &resp, CreationTier::FiveMinute).unwrap();

        assert_eq!(got.counts.prompt, 18_734);
        assert_eq!(got.counts.completion, 658);
        assert_eq!(got.reasoning, 0, "Anthropic folds thinking into output_tokens");
        assert_eq!(got.total, 18_734 + 658, "no total reported - derived from the two");
    }

    #[test]
    fn an_unparseable_status_is_refused_not_coerced() {
        let body = r#"{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"m","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}"#;
        for bogus in [0u16, 42, 1_000] {
            let e = StoredExchange {
                endpoint: "/v1/chat/completions".to_string(),
                request_body: None,
                response_body: Some(body.as_bytes().to_vec()),
                status_code: bogus,
                streamed: false,
            };
            assert!(
                matches!(e.to_outlet_pair(), Err(RecomputeError::BadStatus(s)) if s == bogus),
                "status {bogus} must be refused, not replayed"
            );
        }
    }

    #[test]
    fn a_valid_status_is_preserved_exactly() {
        let body = r#"{"error":{"message":"nope"}}"#;
        for real in [200u16, 400, 429, 502] {
            let e = StoredExchange {
                endpoint: "/v1/chat/completions".to_string(),
                request_body: None,
                response_body: Some(body.as_bytes().to_vec()),
                status_code: real,
                streamed: false,
            };
            let (_, resp) = e.to_outlet_pair().unwrap();
            assert_eq!(resp.status.as_u16(), real);
        }
    }

    #[test]
    fn stream_flag_is_read_from_the_request_body() {
        assert!(StoredExchange::streamed_from_body(Some(br#"{"stream":true}"#)));
        assert!(!StoredExchange::streamed_from_body(Some(br#"{"stream":false}"#)));
        assert!(!StoredExchange::streamed_from_body(Some(b"{}")));
        assert!(!StoredExchange::streamed_from_body(None), "absent body ⇒ not streaming");
        assert!(!StoredExchange::streamed_from_body(Some(b"not json")));
    }

    #[test]
    fn streamed_response_usage_is_read_from_the_terminal_frame() {
        let mut e = exchange(
            "/v1/chat/completions",
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}"#,
            "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}],\"usage\":null}\n\ndata: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":5,\"total_tokens\":16}}\n\ndata: [DONE]\n\n",
        );
        e.streamed = true;

        let (req, resp) = e.to_outlet_pair().unwrap();
        let got = recompute_from_stored_response(&req, &resp, CreationTier::FiveMinute).unwrap();
        assert_eq!(got.counts.prompt, 11);
        assert_eq!(got.counts.completion, 5);
    }
}
