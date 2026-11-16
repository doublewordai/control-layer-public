use std::time::Duration;

use async_trait::async_trait;
use axum::response::IntoResponse;
use hyper_util::{client::legacy::Client, rt::TokioExecutor};

pub type HyperClient = Client<
    hyper_tls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    axum::body::Body,
>;

#[async_trait]
pub trait HttpClient: std::fmt::Debug {
    async fn request(
        &self,
        req: axum::extract::Request,
    ) -> Result<axum::response::Response, Box<dyn std::error::Error + Send + Sync>>;
}

#[async_trait]
impl HttpClient for HyperClient {
    async fn request(
        &self,
        req: axum::extract::Request,
    ) -> Result<axum::response::Response, Box<dyn std::error::Error + Send + Sync>> {
        self.request(req)
            .await
            .map(|res| res.into_response())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

pub fn create_hyper_client(
    pool_max_idle_per_host: usize,
    pool_idle_timeout_secs: u64,
) -> HyperClient {
    let mut http_connector = hyper_util::client::legacy::connect::HttpConnector::new();

    http_connector.enforce_http(false);

    http_connector.set_keepalive(Some(Duration::from_secs(60)));

    http_connector.set_nodelay(true);

    let https = hyper_tls::HttpsConnector::new_with_connector(http_connector);

    tracing::info!(
        "Creating HTTP client with connection pool: max_idle_per_host={}, idle_timeout={}s, tcp_keepalive=60s",
        pool_max_idle_per_host,
        pool_idle_timeout_secs
    );

    Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(pool_idle_timeout_secs))
        .pool_max_idle_per_host(pool_max_idle_per_host)
        .pool_timer(hyper_util::rt::TokioTimer::new())
        .build(https)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_hyper_client_accepts_https_uris() {

        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let client = create_hyper_client(10, 60);

        let http_uri: hyper::Uri = format!("{}/test", mock_server.uri()).parse().unwrap();
        let http_request = axum::extract::Request::builder()
            .uri(http_uri)
            .method("GET")
            .body(axum::body::Body::empty())
            .unwrap();

        let result = client.request(http_request).await;
        assert!(result.is_ok(), "HTTP request should work");

        let https_uri: hyper::Uri = "https://localhost:1/test".parse().unwrap();
        let https_request = axum::extract::Request::builder()
            .uri(https_uri)
            .method("GET")
            .body(axum::body::Body::empty())
            .unwrap();

        let result = client.request(https_request).await;

        if let Err(e) = result {
            let error_string = e.to_string().to_lowercase();
            assert!(
                !error_string.contains("invalid uri") && !error_string.contains("scheme"),
                "Client rejected HTTPS URI at scheme level (enforce_http not disabled): {}",
                e
            );
        }
    }
}
