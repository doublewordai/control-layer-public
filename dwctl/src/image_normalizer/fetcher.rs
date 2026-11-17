use bytes::Bytes;
use rand::RngExt;
use std::env;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};
use url::Url;

use super::config::FetcherConfig;
use super::ip_filter;

const USER_AGENT: &str = "dwctl-image-fetcher/1.0 (+https://doubleword.ai; support@doubleword.ai)";

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("bad input: {0}")]
    BadInput(String),
    #[error("{0}")]
    Unfetchable(String),
    #[error("fetch failed: {0}")]
    FetchFailed(String),
    #[error("transient failure after retries: {0}")]
    Transient(String),
}

#[derive(Debug)]
pub struct FetchedImage {
    pub mime: String,
    pub bytes: Bytes,
}

#[derive(Clone)]
pub struct ImageFetcher {
    config: Arc<FetcherConfig>,
}

impl ImageFetcher {
    pub fn new(config: FetcherConfig) -> Self {
        Self { config: Arc::new(config) }
    }

    pub fn max_bytes(&self) -> u64 {
        self.config.max_bytes
    }

    pub fn mime_allowed(&self, mime: &str) -> bool {
        self.config.mime_allowed(mime)
    }

    pub async fn fetch(&self, url: &str) -> Result<FetchedImage, FetchError> {
        let attempts = self.config.max_retries as usize + 1;
        let mut last_err: Option<FetchError> = None;
        for attempt in 0..attempts {
            if attempt > 0 {
                let base = self.config.retry_base_delay();
                let multiplier = 1u32.checked_shl(attempt as u32 - 1).unwrap_or(u32::MAX);
                let mut delay = base.saturating_mul(multiplier);
                let jitter_ms = {
                    let mut rng = rand::rng();
                    rng.random_range(0..(delay.as_millis() as u64 / 5 + 1))
                };
                delay = delay.saturating_add(Duration::from_millis(jitter_ms));
                debug!(attempt, ?delay, "image fetcher retry sleeping");
                sleep(delay).await;
            }
            match self.fetch_once(url).await {
                Ok(image) => return Ok(image),
                Err(e @ FetchError::BadInput(_)) => return Err(e),
                Err(e @ FetchError::Unfetchable(_)) => return Err(e),
                Err(e @ FetchError::FetchFailed(_)) => return Err(e),
                Err(e @ FetchError::Transient(_)) => {
                    warn!(error = %e, attempt, "image fetch transient failure");
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| FetchError::Transient("no attempts made".into())))
    }

    async fn fetch_once(&self, original_url: &str) -> Result<FetchedImage, FetchError> {
        let mut url = Url::parse(original_url).map_err(|e| FetchError::BadInput(format!("invalid url: {e}")))?;

        for hop in 0..=self.config.max_redirects {
            if !matches!(url.scheme(), "http" | "https") {
                return Err(FetchError::BadInput(format!("unsupported scheme: {}", url.scheme())));
            }
            let host = url.host_str().ok_or_else(|| FetchError::BadInput("missing host".into()))?;
            let port = url
                .port_or_known_default()
                .ok_or_else(|| FetchError::BadInput("unknown port".into()))?;
            let resolved = resolve_first_allowed(host, port).await?;

            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(self.config.timeout())
                .resolve(host, resolved)
                .user_agent(USER_AGENT)
                .build()
                .map_err(|e| FetchError::Transient(format!("client build: {e}")))?;

            let resp = client.get(url.clone()).send().await.map_err(classify_send_error)?;

            let status = resp.status();
            if status.is_redirection() {
                let Some(location) = resp.headers().get(reqwest::header::LOCATION).and_then(|h| h.to_str().ok()) else {
                    return Err(FetchError::FetchFailed(format!("{status} without Location header")));
                };
                let next = url
                    .join(location)
                    .map_err(|e| FetchError::BadInput(format!("bad redirect target: {e}")))?;
                if hop == self.config.max_redirects {
                    return Err(FetchError::BadInput(format!(
                        "too many redirects (cap {})",
                        self.config.max_redirects
                    )));
                }
                debug!(?next, "image fetcher following redirect");
                url = next;
                continue;
            }

            classify_status(status)?;

            let mime = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|h| h.to_str().ok())
                .map(|s| s.split(';').next().unwrap_or("").trim().to_ascii_lowercase())
                .unwrap_or_default();
            if mime.is_empty() {
                return Err(FetchError::BadInput("missing Content-Type".into()));
            }
            if !self.config.mime_allowed(&mime) {
                return Err(FetchError::BadInput(format!("mime not allowed: {mime}")));
            }

            if let Some(len) = resp.content_length()
                && len > self.config.max_bytes
            {
                return Err(FetchError::BadInput(format!(
                    "content-length {} exceeds cap {}",
                    len, self.config.max_bytes
                )));
            }

            let max = self.config.max_bytes as usize;
            let bytes = read_bounded(resp, max).await?;

            return Ok(FetchedImage { mime, bytes });
        }
        Err(FetchError::BadInput("redirect cap exceeded".into()))
    }
}

fn classify_send_error(e: reqwest::Error) -> FetchError {
    if e.is_timeout() || e.is_connect() {
        FetchError::Transient(format!("connect/timeout: {}", e.without_url()))
    } else {
        FetchError::FetchFailed(format!("send: {}", e.without_url()))
    }
}

fn classify_status(status: reqwest::StatusCode) -> Result<(), FetchError> {
    use reqwest::StatusCode;
    if status.is_success() {
        return Ok(());
    }
    let msg = format!("origin {status}");
    if status == StatusCode::REQUEST_TIMEOUT || status == StatusCode::TOO_MANY_REQUESTS {
        return Err(FetchError::Transient(msg));
    }
    if status.is_server_error() {
        return Err(FetchError::Transient(msg));
    }
    if status.is_client_error() {
        return Err(FetchError::Unfetchable(msg));
    }
    Err(FetchError::FetchFailed(msg))
}

async fn resolve_first_allowed(host: &str, port: u16) -> Result<SocketAddr, FetchError> {
    let lookup = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| FetchError::Transient(format!("dns resolve {host}: {e}")))?;
    pick_address(host, lookup, route_check(proxy_configured()))
}

const PROXY_ENV_VARS: [&str; 6] = ["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"];

fn proxy_configured() -> bool {
    PROXY_ENV_VARS
        .iter()
        .any(|name| env::var_os(name).is_some_and(|value| !value.is_empty()))
}

fn route_check(proxied: bool) -> fn(&SocketAddr) -> bool {
    if proxied { |_| true } else { has_route }
}

fn pick_address(
    host: &str,
    resolved: impl IntoIterator<Item = SocketAddr>,
    routable: impl Fn(&SocketAddr) -> bool,
) -> Result<SocketAddr, FetchError> {
    let allowed: Vec<SocketAddr> = resolved.into_iter().filter(|addr| !ip_filter::is_denied(addr.ip())).collect();
    if allowed.is_empty() {
        return Err(FetchError::BadInput(format!(
            "all resolved addresses for {host} are in the deny-list"
        )));
    }
    if let Some(addr) = allowed.iter().copied().find(routable) {
        return Ok(addr);
    }
    if allowed.iter().all(SocketAddr::is_ipv6) {
        return Err(FetchError::BadInput(format!(
            "{host} only has IPv6 addresses and this server cannot connect over IPv6; send the image as a base64 data URL instead"
        )));
    }
    Err(FetchError::Transient(format!("no route to {host}")))
}

fn has_route(addr: &SocketAddr) -> bool {
    let unspecified: IpAddr = if addr.is_ipv4() {
        Ipv4Addr::UNSPECIFIED.into()
    } else {
        Ipv6Addr::UNSPECIFIED.into()
    };
    UdpSocket::bind((unspecified, 0)).and_then(|socket| socket.connect(addr)).is_ok()
}

async fn read_bounded(resp: reqwest::Response, max: usize) -> Result<Bytes, FetchError> {
    use futures::StreamExt;
    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| FetchError::Transient(format!("body read: {}", e.without_url())))?;
        if buf.len() + chunk.len() > max {
            return Err(FetchError::BadInput(format!("body exceeds cap {max}")));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejects_unsupported_scheme() {
        let f = ImageFetcher::new(FetcherConfig::default());
        let err = f.fetch("file:///etc/passwd").await.unwrap_err();
        assert!(matches!(err, FetchError::BadInput(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn rejects_loopback_after_dns_resolve() {
        let f = ImageFetcher::new(FetcherConfig::default());
        let err = f.fetch("http://localhost:9999/x.png").await.unwrap_err();
        assert!(matches!(err, FetchError::BadInput(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn rejects_link_local_metadata_ip_literal() {
        let f = ImageFetcher::new(FetcherConfig::default());
        let err = f.fetch("http://169.254.169.254/latest/meta-data/").await.unwrap_err();
        assert!(matches!(err, FetchError::BadInput(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn rejects_rfc1918_ip_literal() {
        let f = ImageFetcher::new(FetcherConfig::default());
        let err = f.fetch("http://10.0.0.1/x.png").await.unwrap_err();
        assert!(matches!(err, FetchError::BadInput(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn rejects_malformed_url() {
        let f = ImageFetcher::new(FetcherConfig::default());
        let err = f.fetch("not a url").await.unwrap_err();
        assert!(matches!(err, FetchError::BadInput(_)), "got {err:?}");
    }


    #[test]
    fn classify_status_allows_success() {
        assert!(classify_status(reqwest::StatusCode::OK).is_ok());
        assert!(classify_status(reqwest::StatusCode::NO_CONTENT).is_ok());
    }

    #[test]
    fn classify_status_maps_plain_4xx_to_unfetchable() {
        for s in [
            reqwest::StatusCode::UNAUTHORIZED,
            reqwest::StatusCode::FORBIDDEN,
            reqwest::StatusCode::NOT_FOUND,
            reqwest::StatusCode::GONE,
        ] {
            let err = classify_status(s).unwrap_err();
            assert!(matches!(err, FetchError::Unfetchable(_)), "{s} → {err:?}");
        }
    }

    #[test]
    fn classify_status_maps_busy_4xx_and_5xx_to_transient() {
        for s in [
            reqwest::StatusCode::REQUEST_TIMEOUT,
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            reqwest::StatusCode::BAD_GATEWAY,
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let err = classify_status(s).unwrap_err();
            assert!(matches!(err, FetchError::Transient(_)), "{s} → {err:?}");
        }
    }

    #[tokio::test]
    async fn classify_send_error_strips_signed_url_from_connect_failure() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let url = format!("http://127.0.0.1:{port}/a/b.png?X-Amz-Signature=supersecret");
        let reqwest_err = reqwest::Client::new().get(&url).send().await.unwrap_err();
        assert!(reqwest_err.to_string().contains("supersecret"));

        let fetch_err = classify_send_error(reqwest_err);
        assert!(matches!(fetch_err, FetchError::Transient(_)), "got {fetch_err:?}");
        let rendered = fetch_err.to_string();
        assert!(!rendered.contains("supersecret"), "leaked query string: {rendered}");
        assert!(!rendered.contains("/a/b.png"), "leaked url path: {rendered}");
    }

    #[test]
    fn pick_address_rejects_ipv6_only_host_without_an_ipv6_route() {
        let ipv6: SocketAddr = "[2001:db8::1]:443".parse().unwrap();
        let err = pick_address("images.example.com", [ipv6], |addr| addr.is_ipv4()).unwrap_err();
        assert!(
            matches!(&err, FetchError::BadInput(m) if m.contains("only has IPv6 addresses")),
            "got {err:?}"
        );
    }

    #[test]
    fn pick_address_skips_addresses_without_a_route() {
        let ipv6: SocketAddr = "[2001:db8::1]:443".parse().unwrap();
        let ipv4: SocketAddr = "1.1.1.1:443".parse().unwrap();
        assert_eq!(
            pick_address("images.example.com", [ipv6, ipv4], |addr| addr.is_ipv4()).unwrap(),
            ipv4
        );
    }

    #[test]
    fn pick_address_retries_when_no_ipv4_route_exists() {
        let ipv4: SocketAddr = "1.1.1.1:443".parse().unwrap();
        let err = pick_address("images.example.com", [ipv4], |_| false).unwrap_err();
        assert!(matches!(err, FetchError::Transient(_)), "got {err:?}");
    }

    #[test]
    fn pick_address_leaves_routing_to_a_configured_proxy() {
        let ipv6: SocketAddr = "[2001:db8::1]:443".parse().unwrap();
        assert_eq!(pick_address("images.example.com", [ipv6], route_check(true)).unwrap(), ipv6);
    }

    #[test]
    fn has_route_to_loopback() {
        assert!(has_route(&"127.0.0.1:443".parse().unwrap()));
    }
}
