
use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::metrics;

#[derive(Clone)]
pub struct RenderClient {
    http: Client,
    base_url: String,
}

#[derive(Debug)]
pub struct RenderPrefix<'a> {
    pub virtual_model: &'a str,
    pub messages: &'a Value,
    pub tools: Option<&'a Value>,
    pub chat_template_kwargs: Option<&'a Value>,
    pub continuation_text: &'a str,
}

#[derive(Debug, Serialize)]
struct WireRequest<'a> {
    virtual_model: &'a str,
    messages: &'a Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chat_template_kwargs: Option<&'a Value>,
    add_generation_prompt: bool,
    continuation_text: &'a str,
    return_token_ids: bool,
    return_rendered: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RenderedPrefix {
    pub token_ids: Vec<u32>,
    pub total: u32,
    #[serde(default)]
    pub continuation_tokens: Option<u32>,
}

#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    #[error("model {0:?} is not mapped in tokenizer-svc")]
    Unmapped(String),
    #[error("tokenizer-svc cannot render for {0:?}: {1}")]
    Unsupported(String, String),
    #[error("tokenizer-svc response has no continuation_tokens (service too old)")]
    MissingSegmentCount,
    #[error("tokenizer-svc request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("tokenizer-svc returned {status}")]
    Status { status: u16, body: String },
}

impl RenderError {
    pub fn outcome(&self) -> &'static str {
        match self {
            RenderError::Unmapped(_) => "unmapped",
            RenderError::Unsupported(..) => "unsupported",
            RenderError::MissingSegmentCount => "missing_segment_count",
            RenderError::Status { .. } => "http_error",
            RenderError::Http(_) => "transport_error",
        }
    }
}

impl RenderClient {
    pub fn new(base_url: impl Into<String>, timeout: Duration) -> Self {
        let http = Client::builder()
            .timeout(timeout)
            .build()
            .expect("reqwest client builds with default TLS");
        Self::with_client(http, base_url)
    }

    pub fn with_client(http: Client, base_url: impl Into<String>) -> Self {
        Self {
            http,
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    pub async fn render(&self, prefix: &RenderPrefix<'_>) -> Result<RenderedPrefix, RenderError> {
        let start = std::time::Instant::now();
        let result = self.render_inner(prefix).await;
        metrics::record_render_duration(start.elapsed().as_secs_f64());
        metrics::record_render(match &result {
            Ok(_) => "ok",
            Err(e) => e.outcome(),
        });
        result
    }

    async fn render_inner(&self, prefix: &RenderPrefix<'_>) -> Result<RenderedPrefix, RenderError> {
        let resp = self
            .http
            .post(format!("{}/v1/render", self.base_url))
            .json(&WireRequest {
                virtual_model: prefix.virtual_model,
                messages: prefix.messages,
                tools: prefix.tools,
                chat_template_kwargs: prefix.chat_template_kwargs,
                add_generation_prompt: true,
                continuation_text: prefix.continuation_text,
                return_token_ids: true,
                return_rendered: false,
            })
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            if status.as_u16() == 422 && body.contains("UNMAPPED_MODEL") {
                return Err(RenderError::Unmapped(prefix.virtual_model.to_string()));
            }
            if (status.as_u16() == 422 && body.contains("NO_CHAT_TEMPLATE"))
                || (status.as_u16() == 400 && body.contains("TEMPLATE_RENDER_FAILED"))
            {
                return Err(RenderError::Unsupported(prefix.virtual_model.to_string(), body));
            }
            return Err(RenderError::Status {
                status: status.as_u16(),
                body,
            });
        }

        let rendered: RenderedPrefix = resp.json().await?;
        if rendered.continuation_tokens.is_none() {
            return Err(RenderError::MissingSegmentCount);
        }
        Ok(rendered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    const TIMEOUT: Duration = Duration::from_secs(5);

    fn messages() -> Value {
        json!([{"role": "user", "content": "hi"}])
    }

    fn prefix<'a>(messages: &'a Value, tools: Option<&'a Value>) -> RenderPrefix<'a> {
        RenderPrefix {
            virtual_model: "dsv4-flash",
            messages,
            tools,
            chat_template_kwargs: None,
            continuation_text: "Hello, wor",
        }
    }

    #[tokio::test]
    async fn sends_the_one_hop_resume_shape_and_parses_token_ids() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/render"))
            .respond_with(|req: &Request| {
                let body: Value = serde_json::from_slice(&req.body).unwrap();
                assert_eq!(body["virtual_model"], "dsv4-flash");
                assert_eq!(body["add_generation_prompt"], true);
                assert_eq!(body["continuation_text"], "Hello, wor");
                assert_eq!(body["return_token_ids"], true);
                assert_eq!(body["return_rendered"], false, "we never read the rendered string");
                assert_eq!(body["messages"][0]["role"], "user");
                assert!(body.get("tools").is_none(), "absent fields are omitted, not null");
                assert!(body.get("chat_template_kwargs").is_none());
                ResponseTemplate::new(200).set_body_json(json!({
                    "virtual_model": "dsv4-flash",
                    "tokenizer_version": "sha256:abc",
                    "template_version": "sha256:def",
                    "token_ids": [1, 2, 3, 4, 5],
                    "total": 5,
                    "continuation_tokens": 2
                }))
            })
            .mount(&server)
            .await;

        let messages = messages();
        let out = RenderClient::new(server.uri(), TIMEOUT)
            .render(&prefix(&messages, None))
            .await
            .unwrap();
        assert_eq!(out.token_ids, vec![1, 2, 3, 4, 5]);
        assert_eq!(out.total, 5);
        assert_eq!(out.continuation_tokens, Some(2));
    }

    #[tokio::test]
    async fn forwards_tools_and_template_kwargs_when_present() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/render"))
            .respond_with(|req: &Request| {
                let body: Value = serde_json::from_slice(&req.body).unwrap();
                assert_eq!(body["tools"][0]["function"]["name"], "get_weather");
                assert_eq!(body["chat_template_kwargs"]["thinking"], true);
                ResponseTemplate::new(200).set_body_json(json!({
                    "token_ids": [7], "total": 1, "continuation_tokens": 0
                }))
            })
            .mount(&server)
            .await;

        let messages = messages();
        let tools = json!([{"type": "function", "function": {"name": "get_weather"}}]);
        let kwargs = json!({"thinking": true});
        let mut p = prefix(&messages, Some(&tools));
        p.chat_template_kwargs = Some(&kwargs);
        RenderClient::new(server.uri(), TIMEOUT).render(&p).await.unwrap();
    }

    #[tokio::test]
    async fn a_response_without_continuation_tokens_is_an_error_not_a_zero() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/render"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "token_ids": [1, 2, 3], "total": 3
            })))
            .mount(&server)
            .await;

        let messages = messages();
        let err = RenderClient::new(server.uri(), TIMEOUT)
            .render(&prefix(&messages, None))
            .await
            .unwrap_err();
        assert!(matches!(err, RenderError::MissingSegmentCount));
        assert_eq!(err.outcome(), "missing_segment_count");
    }

    #[tokio::test]
    async fn typed_errors_for_unmapped_and_unrenderable_models() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/render"))
            .respond_with(ResponseTemplate::new(422).set_body_json(json!({"code": "UNMAPPED_MODEL"})))
            .mount(&server)
            .await;
        let messages = messages();
        let err = RenderClient::new(server.uri(), TIMEOUT)
            .render(&prefix(&messages, None))
            .await
            .unwrap_err();
        assert!(matches!(err, RenderError::Unmapped(m) if m == "dsv4-flash"));

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/render"))
            .respond_with(ResponseTemplate::new(422).set_body_json(json!({"code": "NO_CHAT_TEMPLATE"})))
            .mount(&server)
            .await;
        let err = RenderClient::new(server.uri(), TIMEOUT)
            .render(&prefix(&messages, None))
            .await
            .unwrap_err();
        assert_eq!(err.outcome(), "unsupported");
    }

    #[tokio::test]
    async fn server_errors_surface_as_status() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/render"))
            .respond_with(ResponseTemplate::new(503).set_body_string("overloaded"))
            .mount(&server)
            .await;
        let messages = messages();
        let err = RenderClient::new(server.uri(), TIMEOUT)
            .render(&prefix(&messages, None))
            .await
            .unwrap_err();
        assert!(matches!(err, RenderError::Status { status: 503, .. }));
        assert_eq!(err.outcome(), "http_error");
    }

    #[tokio::test]
    async fn a_hung_service_times_out_inside_the_attempt_budget() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/render"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
            .mount(&server)
            .await;
        let messages = messages();
        let err = RenderClient::new(server.uri(), Duration::from_millis(50))
            .render(&prefix(&messages, None))
            .await
            .unwrap_err();
        assert_eq!(err.outcome(), "transport_error");
    }
}
