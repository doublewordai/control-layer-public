use sqlx_pool_router::PoolProvider;

use crate::AppState;
use crate::api::models::provider_display_configs::{
    CreateProviderDisplayConfig, ProviderDisplayConfigResponse, UpdateProviderDisplayConfig,
};
use crate::auth::permissions::{RequiresPermission, operation, resource};
use crate::db::handlers::ProviderDisplayConfigs;
use crate::db::models::provider_display_configs::{ProviderDisplayConfigCreateDBRequest, ProviderDisplayConfigUpdateDBRequest};
use crate::errors::{Error, Result};
use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::Response,
};
use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use url::Url;

fn normalize_provider_key(value: &str) -> String {
    value.trim().to_lowercase()
}

fn validate_provider_key(provider_key: &str) -> Result<()> {
    if provider_key.trim().is_empty() {
        return Err(Error::BadRequest {
            message: "provider_key must not be empty".to_string(),
        });
    }
    Ok(())
}

fn validate_icon(icon: Option<&str>) -> Result<()> {
    let Some(icon) = icon.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };

    let is_builtin = matches!(icon, "anthropic" | "google" | "openai" | "onwards" | "snowflake");
    let is_url = icon.starts_with("https://") || icon.starts_with("/");
    if !is_builtin && !is_url {
        return Err(Error::BadRequest {
            message: "icon must be an https URL, root-relative asset path, or built-in icon key".to_string(),
        });
    }
    Ok(())
}

#[utoipa::path(
    get,
    path = "/provider-display-configs",
    tag = "provider-display-configs",
    summary = "List provider display configs",
    responses(
        (status = 200, description = "Provider display configs", body = Vec<ProviderDisplayConfigResponse>)
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
pub async fn list_provider_display_configs<P: PoolProvider>(
    State(state): State<AppState<P>>,
    _: RequiresPermission<resource::Models, operation::ReadOwn>,
) -> Result<Json<Vec<ProviderDisplayConfigResponse>>> {
    let mut conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = ProviderDisplayConfigs::new(&mut conn);

    let known = repo.list_known_providers().await?;
    let configs = repo.list().await?;

    let config_map: HashMap<_, _> = configs.into_iter().map(|config| (config.provider_key.clone(), config)).collect();
    let known_map: HashMap<_, _> = known
        .into_iter()
        .map(|provider| (provider.provider_key.clone(), provider))
        .collect();

    let mut keys = BTreeMap::new();
    for key in config_map.keys() {
        keys.insert(key.clone(), ());
    }
    for key in known_map.keys() {
        keys.insert(key.clone(), ());
    }

    let mut response = Vec::new();
    for key in keys.into_keys() {
        response.push(ProviderDisplayConfigResponse::from_parts(
            config_map.get(&key).cloned(),
            known_map.get(&key).cloned(),
        ));
    }

    response.sort_by(|a, b| a.display_name.cmp(&b.display_name));

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/provider-display-configs/{provider_key}",
    tag = "provider-display-configs",
    summary = "Get provider display config",
    params(
        ("provider_key" = String, Path)
    ),
    responses(
        (status = 200, description = "Provider display config", body = ProviderDisplayConfigResponse),
        (status = 404, description = "Provider display config not found")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
pub async fn get_provider_display_config<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(provider_key): Path<String>,
    _: RequiresPermission<resource::Models, operation::ReadOwn>,
) -> Result<Json<ProviderDisplayConfigResponse>> {
    let provider_key = normalize_provider_key(&provider_key);
    let mut conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = ProviderDisplayConfigs::new(&mut conn);
    let config = repo.get_by_key(&provider_key).await?;
    let known = repo
        .list_known_providers()
        .await?
        .into_iter()
        .find(|provider| provider.provider_key == provider_key);

    match (config, known) {
        (None, None) => Err(Error::NotFound {
            resource: "provider display config".to_string(),
            id: provider_key,
        }),
        (config, known) => Ok(Json(ProviderDisplayConfigResponse::from_parts(config, known))),
    }
}

#[utoipa::path(
    post,
    path = "/provider-display-configs",
    tag = "provider-display-configs",
    summary = "Create provider display config",
    request_body = CreateProviderDisplayConfig,
    responses(
        (status = 201, description = "Provider display config created", body = ProviderDisplayConfigResponse)
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
pub async fn create_provider_display_config<P: PoolProvider>(
    State(state): State<AppState<P>>,
    current_user: RequiresPermission<resource::Models, operation::UpdateAll>,
    Json(create): Json<CreateProviderDisplayConfig>,
) -> Result<(StatusCode, Json<ProviderDisplayConfigResponse>)> {
    let provider_key = normalize_provider_key(&create.provider_key);
    validate_provider_key(&provider_key)?;
    validate_icon(create.icon.as_deref())?;

    let display_name = create
        .display_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(create.provider_key.trim())
        .to_string();

    let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = ProviderDisplayConfigs::new(&mut conn);
    let config = repo
        .create(&ProviderDisplayConfigCreateDBRequest {
            provider_key: provider_key.clone(),
            display_name,
            icon: create.icon.filter(|value| !value.trim().is_empty()),
            created_by: current_user.id,
        })
        .await?;

    let known = repo
        .list_known_providers()
        .await?
        .into_iter()
        .find(|provider| provider.provider_key == provider_key);

    Ok((
        StatusCode::CREATED,
        Json(ProviderDisplayConfigResponse::from_parts(Some(config), known)),
    ))
}

#[utoipa::path(
    patch,
    path = "/provider-display-configs/{provider_key}",
    tag = "provider-display-configs",
    summary = "Update provider display config",
    request_body = UpdateProviderDisplayConfig,
    params(
        ("provider_key" = String, Path)
    ),
    responses(
        (status = 200, description = "Provider display config updated", body = ProviderDisplayConfigResponse)
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
pub async fn update_provider_display_config<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(provider_key): Path<String>,
    _: RequiresPermission<resource::Models, operation::UpdateAll>,
    Json(update): Json<UpdateProviderDisplayConfig>,
) -> Result<Json<ProviderDisplayConfigResponse>> {
    let provider_key = normalize_provider_key(&provider_key);
    validate_provider_key(&provider_key)?;
    validate_icon(update.icon.as_ref().and_then(|value| value.as_deref()))?;

    let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = ProviderDisplayConfigs::new(&mut conn);
    let config = repo
        .update(
            &provider_key,
            &ProviderDisplayConfigUpdateDBRequest {
                display_name: update.display_name.and_then(|value| {
                    let trimmed = value.trim().to_string();
                    (!trimmed.is_empty()).then_some(trimmed)
                }),
                icon: update.icon.map(|value| {
                    value.and_then(|icon| {
                        let trimmed = icon.trim().to_string();
                        (!trimmed.is_empty()).then_some(trimmed)
                    })
                }),
            },
        )
        .await?;

    let known = repo
        .list_known_providers()
        .await?
        .into_iter()
        .find(|provider| provider.provider_key == provider_key);

    Ok(Json(ProviderDisplayConfigResponse::from_parts(Some(config), known)))
}

#[utoipa::path(
    delete,
    path = "/provider-display-configs/{provider_key}",
    tag = "provider-display-configs",
    summary = "Delete provider display config",
    params(
        ("provider_key" = String, Path)
    ),
    responses(
        (status = 204, description = "Provider display config deleted")
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
pub async fn delete_provider_display_config<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(provider_key): Path<String>,
    _: RequiresPermission<resource::Models, operation::UpdateAll>,
) -> Result<StatusCode> {
    let provider_key = normalize_provider_key(&provider_key);
    let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = ProviderDisplayConfigs::new(&mut conn);
    let deleted = repo.delete(&provider_key).await?;
    if !deleted {
        return Err(Error::NotFound {
            resource: "provider display config".to_string(),
            id: provider_key,
        });
    }
    Ok(StatusCode::NO_CONTENT)
}


const MAX_ICON_SIZE_BYTES: u64 = 2 * 1024 * 1024;

const ICON_FETCH_TIMEOUT: Duration = Duration::from_secs(5);

const ICON_CACHE_CONTROL: &str = "public, max-age=3600";

const ICON_NOT_FOUND_CACHE_CONTROL: &str = "public, max-age=60";

const ICON_FETCH_ATTEMPTS: usize = 3;

fn is_retryable_icon_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

async fn fetch_icon_with_retries(client: &reqwest::Client, icon_url: &Url) -> Option<reqwest::Response> {
    for attempt in 1..=ICON_FETCH_ATTEMPTS {
        match client.get(icon_url.clone()).send().await {
            Ok(upstream) if upstream.status().is_success() => return Some(upstream),
            Ok(upstream) if is_retryable_icon_status(upstream.status()) && attempt < ICON_FETCH_ATTEMPTS => {}
            Ok(upstream) => {
                tracing::debug!(
                    status = %upstream.status(),
                    attempts = attempt,
                    "Provider icon upstream returned no usable response"
                );
                return None;
            }
            Err(error) if attempt < ICON_FETCH_ATTEMPTS => {
                tracing::debug!(
                    error = %error,
                    attempt,
                    "Transient provider icon fetch failed; retrying"
                );
            }
            Err(error) => {
                tracing::debug!(
                    error = %error,
                    attempts = attempt,
                    "Provider icon fetch attempts exhausted"
                );
                return None;
            }
        }
    }

    None
}

fn icon_not_found_response() -> Response {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(axum::http::header::CACHE_CONTROL, ICON_NOT_FOUND_CACHE_CONTROL)
        .body(Body::empty())
        .expect("static provider icon not-found response is valid")
}

fn is_globally_routable(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => ipv4_is_globally_routable(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => ipv4_is_globally_routable(v4),
            None => ipv6_is_globally_routable(v6),
        },
    }
}

fn ipv4_is_globally_routable(ip: Ipv4Addr) -> bool {
    if ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
    {
        return false;
    }
    let [a, b, _, _] = ip.octets();
    if a == 100 && (0x40..=0x7f).contains(&b) {
        return false;
    }
    if a == 192 && b == 0 && ip.octets()[2] == 0 {
        return false;
    }
    if a == 198 && (b == 18 || b == 19) {
        return false;
    }
    if a >= 240 {
        return false;
    }
    true
}

fn ipv6_is_globally_routable(ip: Ipv6Addr) -> bool {
    if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
        return false;
    }
    let s = ip.segments();
    if (s[0] & 0xfe00) == 0xfc00 {
        return false;
    }
    if (s[0] & 0xffc0) == 0xfe80 {
        return false;
    }
    if s[0] == 0x2001 && s[1] == 0x0db8 {
        return false;
    }
    if s[0] == 0x0100 && s[1] == 0 && s[2] == 0 && s[3] == 0 {
        return false;
    }
    true
}

fn url_host_is_globally_routable(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Ipv4(ip)) => is_globally_routable(IpAddr::V4(ip)),
        Some(url::Host::Ipv6(ip)) => is_globally_routable(IpAddr::V6(ip)),
        _ => true,
    }
}

#[derive(Debug, Default)]
struct SsrfSafeResolver;

impl reqwest::dns::Resolve for SsrfSafeResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let resolved = tokio::net::lookup_host((host.as_str(), 0)).await?;
            let filtered: Vec<SocketAddr> = resolved.filter(|sa| is_globally_routable(sa.ip())).collect();
            if filtered.is_empty() {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("no globally routable address for {host}"),
                )) as Box<dyn std::error::Error + Send + Sync>);
            }
            Ok(Box::new(filtered.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

fn icon_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(ICON_FETCH_TIMEOUT)
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if attempt.previous().len() >= 3 {
                    return attempt.error("too many redirects");
                }
                if attempt.url().scheme() != "https" {
                    return attempt.error("redirect scheme is not https");
                }
                if !url_host_is_globally_routable(attempt.url()) {
                    return attempt.error("redirect host is not globally routable");
                }
                attempt.follow()
            }))
            .dns_resolver(Arc::new(SsrfSafeResolver))
            .user_agent(concat!(
                "dwctl-icon-proxy/",
                env!("CARGO_PKG_VERSION"),
                " (+https://github.com/doublewordai/control-layer)",
            ))
            .build()
            .expect("icon proxy reqwest client builds with a static config")
    })
}

#[utoipa::path(
    get,
    path = "/provider-display-configs/{provider_key}/icon",
    tag = "provider-display-configs",
    summary = "Proxy the provider's configured icon",
    description = "Fetches the operator-set icon URL server-side and streams it back. Lets the SPA keep a tight CSP regardless of where the icon URL points.",
    params(("provider_key" = String, Path)),
    responses(
        (status = 200, description = "Icon bytes"),
        (status = 404, description = "Provider icon not found, unavailable, or invalid"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all, fields(provider_key = %provider_key))]
pub async fn get_provider_display_config_icon<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(provider_key): Path<String>,
    _: RequiresPermission<resource::Models, operation::ReadOwn>,
) -> Result<Response> {
    let provider_key = normalize_provider_key(&provider_key);

    let icon_value = {
        let mut conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let mut repo = ProviderDisplayConfigs::new(&mut conn);
        repo.get_by_key(&provider_key)
            .await?
            .ok_or_else(|| Error::NotFound {
                resource: "provider display config".to_string(),
                id: provider_key.clone(),
            })?
            .icon
            .unwrap_or_default()
    };
    let icon_value = icon_value.trim();

    let icon_url = match Url::parse(icon_value) {
        Ok(u) if u.scheme() == "https" && url_host_is_globally_routable(&u) => u,
        _ => {
            return Err(Error::NotFound {
                resource: "proxyable icon for provider".to_string(),
                id: provider_key,
            });
        }
    };

    let Some(resp) = fetch_icon_with_retries(icon_http_client(), &icon_url).await else {
        return Ok(icon_not_found_response());
    };

    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string());

    if !content_type.to_ascii_lowercase().starts_with("image/") {
        tracing::debug!(%content_type, "Provider icon upstream returned a non-image response");
        return Ok(icon_not_found_response());
    }

    let body_bytes = match read_capped_body(resp, MAX_ICON_SIZE_BYTES).await {
        Ok(body) => body,
        Err(error) => {
            tracing::debug!(%error, "Provider icon upstream returned an unusable body");
            return Ok(icon_not_found_response());
        }
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, content_type)
        .header(axum::http::header::CACHE_CONTROL, ICON_CACHE_CONTROL)
        .body(Body::from(body_bytes))
        .map_err(|e| Error::Other(anyhow::anyhow!("build icon response: {e}")))
}

async fn read_capped_body(mut resp: reqwest::Response, max: u64) -> anyhow::Result<bytes::Bytes> {
    let mut buf = bytes::BytesMut::new();
    while let Some(chunk) = resp.chunk().await? {
        if (buf.len() as u64) + (chunk.len() as u64) > max {
            anyhow::bail!("icon body exceeds {max} bytes");
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf.freeze())
}

#[cfg(test)]
mod ssrf_filter_tests {
    use super::is_globally_routable;
    use std::net::IpAddr;

    fn check(s: &str) -> bool {
        is_globally_routable(s.parse::<IpAddr>().unwrap())
    }

    #[test]
    fn allows_public_v4() {
        assert!(check("8.8.8.8"));
        assert!(check("1.1.1.1"));
        assert!(check("142.250.190.78"));
    }

    #[test]
    fn allows_public_v6() {
        assert!(check("2606:4700:4700::1111"));
        assert!(check("2001:4860:4860::8888"));
    }

    #[test]
    fn blocks_rfc1918() {
        assert!(!check("10.0.0.1"));
        assert!(!check("10.255.255.255"));
        assert!(!check("172.16.0.1"));
        assert!(!check("172.31.255.255"));
        assert!(!check("192.168.1.1"));
    }

    #[test]
    fn blocks_loopback() {
        assert!(!check("127.0.0.1"));
        assert!(!check("127.255.255.254"));
        assert!(!check("::1"));
    }

    #[test]
    fn blocks_link_local_and_metadata() {
        assert!(!check("169.254.169.254"));
        assert!(!check("169.254.0.1"));
        assert!(!check("fe80::1"));
    }

    #[test]
    fn blocks_cgnat() {
        assert!(!check("100.64.0.1"));
        assert!(!check("100.127.255.254"));
        assert!(check("100.63.255.254"));
        assert!(check("100.128.0.1"));
    }

    #[test]
    fn blocks_unspecified_and_broadcast() {
        assert!(!check("0.0.0.0"));
        assert!(!check("255.255.255.255"));
        assert!(!check("::"));
    }

    #[test]
    fn blocks_multicast() {
        assert!(!check("224.0.0.1"));
        assert!(!check("239.255.255.255"));
        assert!(!check("ff02::1"));
    }

    #[test]
    fn blocks_reserved_class_e_and_documentation() {
        assert!(!check("240.0.0.1"));
        assert!(!check("254.0.0.1"));
        assert!(!check("192.0.2.1"));
        assert!(!check("198.51.100.1"));
        assert!(!check("203.0.113.1"));
        assert!(!check("198.18.0.1"));
        assert!(!check("192.0.0.1"));
        assert!(!check("2001:db8::1"));
    }

    #[test]
    fn blocks_ipv6_unique_local() {
        assert!(!check("fc00::1"));
        assert!(!check("fdff:ffff::1"));
    }

    #[test]
    fn blocks_ipv4_mapped_ipv6_loopback() {
        assert!(!check("::ffff:127.0.0.1"));
        assert!(!check("::ffff:10.0.0.1"));
        assert!(!check("::ffff:169.254.169.254"));
    }

    use super::url_host_is_globally_routable;
    use url::Url;

    fn url_ok(s: &str) -> bool {
        url_host_is_globally_routable(&Url::parse(s).unwrap())
    }

    #[test]
    fn url_helper_passes_hostnames_through() {
        assert!(url_ok("https://example.com/foo"));
        assert!(url_ok("https://registry.npmmirror.com/x.svg"));
    }

    #[test]
    fn url_helper_blocks_ip_literal_private() {
        assert!(!url_ok("https://10.0.0.1/foo"));
        assert!(!url_ok("https://192.168.1.1/foo"));
        assert!(!url_ok("https://127.0.0.1/foo"));
        assert!(!url_ok("https://169.254.169.254/latest/meta-data/"));
        assert!(!url_ok("https://[::1]/foo"));
        assert!(!url_ok("https://[fc00::1]/foo"));
        assert!(!url_ok("https://[::ffff:10.0.0.1]/foo"));
    }

    #[test]
    fn url_helper_allows_ip_literal_public() {
        assert!(url_ok("https://1.1.1.1/foo"));
        assert!(url_ok("https://[2606:4700:4700::1111]/foo"));
    }
}

#[cfg(test)]
mod icon_fetch_tests {
    use super::{fetch_icon_with_retries, icon_not_found_response, is_retryable_icon_status};
    use axum::http::header::CACHE_CONTROL;
    use reqwest::StatusCode;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use url::Url;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    #[test]
    fn test_retryable_icon_status() {
        for status in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            assert!(is_retryable_icon_status(status), "{status} should be retryable");
        }

        for status in [StatusCode::BAD_REQUEST, StatusCode::FORBIDDEN, StatusCode::NOT_FOUND] {
            assert!(!is_retryable_icon_status(status), "{status} should not be retryable");
        }
    }

    async fn sequence_server(statuses: Vec<u16>) -> (MockServer, Arc<AtomicUsize>) {
        let server = MockServer::start().await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let responder_attempts = attempts.clone();

        Mock::given(method("GET"))
            .respond_with(move |_: &Request| {
                let attempt = responder_attempts.fetch_add(1, Ordering::SeqCst);
                let status = statuses.get(attempt).copied().unwrap_or(*statuses.last().unwrap());
                let response = ResponseTemplate::new(status);
                if status == 200 {
                    response
                        .insert_header("content-type", "image/png")
                        .set_body_bytes(b"image".to_vec())
                } else {
                    response
                }
            })
            .mount(&server)
            .await;

        (server, attempts)
    }

    #[tokio::test]
    async fn retries_two_transient_failures_before_success() {
        let (server, attempts) = sequence_server(vec![503, 429, 200]).await;
        let client = reqwest::Client::new();
        let url = Url::parse(&server.uri()).unwrap();

        let response = fetch_icon_with_retries(&client, &url).await;

        assert_eq!(response.unwrap().status(), StatusCode::OK);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn exhausts_transient_retries() {
        let (server, attempts) = sequence_server(vec![503]).await;
        let client = reqwest::Client::new();
        let url = Url::parse(&server.uri()).unwrap();

        let response = fetch_icon_with_retries(&client, &url).await;

        assert!(response.is_none());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn does_not_retry_permanent_upstream_failure() {
        let (server, attempts) = sequence_server(vec![403]).await;
        let client = reqwest::Client::new();
        let url = Url::parse(&server.uri()).unwrap();

        let response = fetch_icon_with_retries(&client, &url).await;

        assert!(response.is_none());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn icon_not_found_response_is_negatively_cached() {
        let response = icon_not_found_response();

        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
        assert_eq!(response.headers()[CACHE_CONTROL], "public, max-age=60");
    }
}

#[cfg(test)]
mod tests {
    use crate::api::models::provider_display_configs::ProviderDisplayConfigResponse;
    use crate::api::models::users::Role;
    use crate::test::utils::*;
    use sqlx::PgPool;

    async fn create_model_with_provider(pool: &PgPool, alias: &str, provider: &str, created_by: uuid::Uuid) -> uuid::Uuid {
        let endpoint_id = get_test_endpoint_id(pool).await;
        let deployment_id = uuid::Uuid::new_v4();
        sqlx::query!(
            r#"
            INSERT INTO deployed_models (id, model_name, alias, hosted_on, created_by, deleted, metadata)
            VALUES ($1, $2, $3, $4, $5, false, $6)
            "#,
            deployment_id,
            alias,
            alias,
            endpoint_id,
            created_by,
            serde_json::json!({ "provider": provider }),
        )
        .execute(pool)
        .await
        .expect("Failed to create test model with provider");
        deployment_id
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_standard_user_can_read_all_provider_display_configs(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let anthropic_model = create_model_with_provider(&pool, "claude-3", "Anthropic", admin.id).await;
        let group = create_test_group(&pool).await;
        add_deployment_to_group(&pool, anthropic_model, group.id, admin.id).await;
        add_user_to_group(&pool, user.id, group.id).await;
        sqlx::query!(
            r#"
            INSERT INTO provider_display_configs (provider_key, display_name, icon, created_by)
            VALUES ($1, $2, $3, $4)
            "#,
            "openai",
            "OpenAI",
            Some("openai"),
            admin.id
        )
        .execute(&pool)
        .await
        .expect("Failed to create provider display config");

        let headers = add_auth_headers(&user);
        let response = app
            .get("/admin/api/v1/provider-display-configs")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;

        response.assert_status_ok();
        let providers: Vec<ProviderDisplayConfigResponse> = response.json();
        let provider_keys: Vec<&str> = providers.iter().map(|p| p.provider_key.as_str()).collect();
        assert!(provider_keys.contains(&"anthropic"), "Should see deployed provider");
        assert!(provider_keys.contains(&"openai"), "Should see config-only provider");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_admin_sees_all_providers(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;

        create_model_with_provider(&pool, "claude-3", "Anthropic", admin.id).await;
        create_model_with_provider(&pool, "gpt-4", "OpenAI", admin.id).await;

        let headers = add_auth_headers(&admin);
        let response = app
            .get("/admin/api/v1/provider-display-configs")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;

        response.assert_status_ok();
        let providers: Vec<ProviderDisplayConfigResponse> = response.json();
        let provider_keys: Vec<&str> = providers.iter().map(|p| p.provider_key.as_str()).collect();
        assert!(provider_keys.contains(&"anthropic"), "Admin should see Anthropic");
        assert!(provider_keys.contains(&"openai"), "Admin should see OpenAI");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_standard_user_sees_everyone_group_providers(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let model_id = create_model_with_provider(&pool, "gemini-pro", "Google", admin.id).await;
        let everyone_group_id = uuid::Uuid::nil();
        add_deployment_to_group(&pool, model_id, everyone_group_id, admin.id).await;

        let headers = add_auth_headers(&user);
        let response = app
            .get("/admin/api/v1/provider-display-configs")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;

        response.assert_status_ok();
        let providers: Vec<ProviderDisplayConfigResponse> = response.json();
        let provider_keys: Vec<&str> = providers.iter().map(|p| p.provider_key.as_str()).collect();
        assert!(provider_keys.contains(&"google"), "Should see provider from Everyone group");
    }
}
