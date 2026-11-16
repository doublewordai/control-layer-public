
use dwctl::config::{CorsConfig, CorsOrigin};

#[test]
fn default_cors_origins_include_http_localhost_vite_frontend() {
    let cfg = CorsConfig::default();
    let has_vite_origin = cfg.allowed_origins.iter().any(|origin| {
        matches!(
            origin,
            CorsOrigin::Url(url)
                if url.scheme() == "http"
                    && url.host_str() == Some("localhost")
                    && url.port() == Some(3001)
        )
    });
    assert!(
        has_vite_origin,
        "default CORS allowlist should include http://localhost:3001; got {:?}",
        cfg.allowed_origins
    );
}
