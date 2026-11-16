
use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::metrics as cache_metrics;

#[derive(Clone)]
pub struct TokenizerClient {
    http: Client,
    base_url: String,
}

#[derive(Debug, Serialize)]
struct TokenizeRequest<'a> {
    virtual_model: &'a str,
    segments: &'a [String],
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenizeResponse {
    pub virtual_model: String,
    pub tokenizer_version: String,
    pub segment_counts: Vec<u32>,
    pub cumulative: Vec<u32>,
    pub total: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelInfo {
    pub alias: String,
    pub hf_repo: String,
    pub tokenizer_version: String,
    #[serde(default)]
    pub template_version: Option<String>,
}

#[derive(Debug, Serialize)]
struct RenderRequest<'a> {
    virtual_model: &'a str,
    messages: &'a serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<&'a serde_json::Value>,
    add_generation_prompt: bool,
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    prefixes: &'a [WirePrefix],
    return_rendered: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum WirePrefix {
    Tools { tools: usize },
    Message { message: usize },
    Block { message: usize, block: usize },
    ToolCall { message: usize, tool_call: usize },
}

#[derive(Debug, Clone, Deserialize)]
pub struct RenderResponse {
    pub virtual_model: String,
    pub tokenizer_version: String,
    pub template_version: String,
    pub total: u32,
    #[serde(default)]
    pub prefix_counts: Vec<Option<u32>>,
}

#[derive(Debug, Deserialize)]
struct ModelsResponse {
    models: Vec<ModelInfo>,
}

#[derive(Debug, thiserror::Error)]
pub enum TokenizerError {
    #[error("model {0:?} is not mapped in tokenizer-svc")]
    Unmapped(String),
    #[error("tokenizer-svc cannot render for {0:?}: {1}")]
    RenderUnsupported(String, String),
    #[error("tokenizer-svc request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("tokenizer-svc returned {status}: {body}")]
    Status { status: u16, body: String },
}

pub type TokenizerResult<T> = std::result::Result<T, TokenizerError>;

impl TokenizerClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(5))
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

    pub async fn tokenize(&self, virtual_model: &str, segments: &[String]) -> TokenizerResult<TokenizeResponse> {
        let start = std::time::Instant::now();
        let size = cache_metrics::tokenize_size_bucket(segments.iter().map(String::len).sum());
        let result = self.tokenize_inner(virtual_model, segments).await;
        let model_label = match &result {
            Ok(_) => virtual_model,
            Err(TokenizerError::Unmapped(_)) => "unmapped",
            Err(_) => "error",
        };
        cache_metrics::record_tokenizer_duration(model_label, size, start.elapsed().as_secs_f64());
        cache_metrics::record_tokenizer_request(match &result {
            Ok(_) => "ok",
            Err(TokenizerError::Unmapped(_)) => "unmapped_422",
            Err(TokenizerError::Status { .. }) => "http_error",
            Err(_) => "transport_error",
        });
        result
    }

    pub async fn render(
        &self,
        virtual_model: &str,
        messages: &serde_json::Value,
        tools: Option<&serde_json::Value>,
        add_generation_prompt: bool,
        prefixes: &[WirePrefix],
    ) -> TokenizerResult<RenderResponse> {
        let start = std::time::Instant::now();
        let size = cache_metrics::tokenize_size_bucket(messages.to_string().len() + tools.map(|t| t.to_string().len()).unwrap_or(0));
        let result = self
            .render_inner(virtual_model, messages, tools, add_generation_prompt, prefixes)
            .await;
        let model_label = match &result {
            Ok(_) => virtual_model,
            Err(TokenizerError::Unmapped(_)) => "unmapped",
            Err(TokenizerError::RenderUnsupported(..)) => "render_unsupported",
            Err(_) => "error",
        };
        cache_metrics::record_tokenizer_duration(model_label, size, start.elapsed().as_secs_f64());
        cache_metrics::record_tokenizer_request(match &result {
            Ok(_) => "render_ok",
            Err(TokenizerError::Unmapped(_)) => "unmapped_422",
            Err(TokenizerError::RenderUnsupported(..)) => "render_unsupported",
            Err(TokenizerError::Status { .. }) => "http_error",
            Err(_) => "transport_error",
        });
        result
    }

    async fn render_inner(
        &self,
        virtual_model: &str,
        messages: &serde_json::Value,
        tools: Option<&serde_json::Value>,
        add_generation_prompt: bool,
        prefixes: &[WirePrefix],
    ) -> TokenizerResult<RenderResponse> {
        let resp = self
            .http
            .post(format!("{}/v1/render", self.base_url))
            .json(&RenderRequest {
                virtual_model,
                messages,
                tools,
                add_generation_prompt,
                prefixes,
                return_rendered: false,
            })
            .send()
            .await?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp.json().await?);
        }
        let body = resp.text().await.unwrap_or_default();
        if status.as_u16() == 422 && body.contains("UNMAPPED_MODEL") {
            return Err(TokenizerError::Unmapped(virtual_model.to_string()));
        }
        if (status.as_u16() == 422 && body.contains("NO_CHAT_TEMPLATE"))
            || (status.as_u16() == 400 && body.contains("TEMPLATE_RENDER_FAILED"))
        {
            return Err(TokenizerError::RenderUnsupported(virtual_model.to_string(), body));
        }
        Err(TokenizerError::Status {
            status: status.as_u16(),
            body,
        })
    }

    async fn tokenize_inner(&self, virtual_model: &str, segments: &[String]) -> TokenizerResult<TokenizeResponse> {
        let resp = self
            .http
            .post(format!("{}/v1/tokenize", self.base_url))
            .json(&TokenizeRequest { virtual_model, segments })
            .send()
            .await?;
        parse_tokenize(resp, virtual_model).await
    }

    pub async fn models(&self) -> TokenizerResult<Vec<ModelInfo>> {
        let resp = self
            .http
            .get(format!("{}/v1/models", self.base_url))
            .send()
            .await?
            .error_for_status()?;
        Ok(resp.json::<ModelsResponse>().await?.models)
    }

    pub async fn healthz(&self) -> TokenizerResult<bool> {
        let resp = self.http.get(format!("{}/healthz", self.base_url)).send().await?;
        Ok(resp.status().is_success())
    }
}

async fn parse_tokenize(resp: reqwest::Response, virtual_model: &str) -> TokenizerResult<TokenizeResponse> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp.json().await?);
    }
    let body = resp.text().await.unwrap_or_default();
    if status.as_u16() == 422 && body.contains("UNMAPPED_MODEL") {
        return Err(TokenizerError::Unmapped(virtual_model.to_string()));
    }
    Err(TokenizerError::Status {
        status: status.as_u16(),
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn tokenize_parses_counts() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/tokenize"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "virtual_model": "test-model",
                "tokenizer_version": "sha256:abc",
                "segment_counts": [128, 16],
                "cumulative": [128, 144],
                "total": 144
            })))
            .mount(&server)
            .await;

        let client = TokenizerClient::new(server.uri());
        let r = client
            .tokenize("test-model", &["sys".to_string(), "user".to_string()])
            .await
            .unwrap();
        assert_eq!(r.total, 144);
        assert_eq!(r.segment_counts, vec![128, 16]);
        assert_eq!(r.cumulative, vec![128, 144]);
        assert_eq!(r.tokenizer_version, "sha256:abc");
    }

    #[tokio::test]
    async fn unmapped_model_maps_to_typed_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/tokenize"))
            .respond_with(ResponseTemplate::new(422).set_body_json(serde_json::json!({ "code": "UNMAPPED_MODEL" })))
            .mount(&server)
            .await;

        let client = TokenizerClient::new(server.uri());
        let err = client.tokenize("mystery-model", &["hi".to_string()]).await.unwrap_err();
        match err {
            TokenizerError::Unmapped(m) => assert_eq!(m, "mystery-model"),
            other => panic!("expected Unmapped, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn other_errors_are_status() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/tokenize"))
            .respond_with(ResponseTemplate::new(503).set_body_string("overloaded"))
            .mount(&server)
            .await;

        let client = TokenizerClient::new(server.uri());
        let err = client.tokenize("test-model", &["hi".to_string()]).await.unwrap_err();
        assert!(matches!(err, TokenizerError::Status { status: 503, .. }));
    }

    #[tokio::test]
    async fn models_and_healthz() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "models": [
                    { "alias": "a", "hf_repo": "org/a", "tokenizer_version": "v1" }
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/healthz"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"status":"ok"})))
            .mount(&server)
            .await;

        let client = TokenizerClient::new(server.uri());
        let models = client.models().await.unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].alias, "a");
        assert!(client.healthz().await.unwrap());
    }
}
