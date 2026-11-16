use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

use crate::image_normalizer::{
    ImageInput, ImageNormalizer, ImageToken, Mode, NormalizeError, TokenParseError, image_label, sealed, walker,
};
use crate::inference::rejections;
use sqlx::PgPool;

#[derive(Clone)]
pub struct ImageNormalizerMiddlewareState {
    pub enabled: bool,
    pub normalizer: Arc<dyn ImageNormalizer>,
    pub realtime_ttl: Duration,
    pub token_ttl: Duration,
    pub pool: Option<sqlx_pool_router::DynPools>,
    pub keystore: Option<crate::keystore::Keystore>,
    pub key_policy_cache: crate::sync::key_policy::KeyPolicyCache,
}

fn extract_bearer_token(request: &Request<Body>) -> Option<String> {
    let auth = request.headers().get(axum::http::header::AUTHORIZATION)?.to_str().ok()?;
    let auth = auth.trim();
    if auth.len() > 7 && auth[..7].eq_ignore_ascii_case("bearer ") {
        Some(auth[7..].to_string())
    } else {
        None
    }
}

pub async fn image_normalizer_middleware(
    State(state): State<ImageNormalizerMiddlewareState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    if !state.enabled {
        return next.run(request).await;
    }

    if !path_accepts_images(request.uri().path()) {
        return next.run(request).await;
    }

    let body_bytes = match axum::body::to_bytes(std::mem::take(request.body_mut()), usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "Failed to read request body in image_normalizer middleware");
            let body = serde_json::json!({
                "error": {
                    "message": format!("failed to read request body: {e}"),
                    "type": "invalid_request_error",
                    "code": "body_read_failed",
                }
            });
            let response = (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
            return rejections::refuse(&request, response, "body_read_failed", None, None);
        }
    };

    let mut body_value: Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(_) => {
            *request.body_mut() = Body::from(body_bytes);
            return next.run(request).await;
        }
    };

    let mode = Mode::AllAndTokens;

    if !walker::has_inputs(&body_value, mode) {
        *request.body_mut() = Body::from(body_bytes);
        return next.run(request).await;
    }

    let presented_key =
        crate::auth::internal_credential::presented_key_hash(request.extensions(), extract_bearer_token(&request).as_deref());
    let caller_lookup: Result<Option<crate::api::handlers::images::ResolvedCaller>, ()> = match (state.pool.as_ref(), presented_key) {
        (Some(pool), Some(key)) => crate::api::handlers::images::try_resolve_caller(&pool.write(), &key)
            .await
            .map_err(|e| warn!(error = %e, "Caller lookup failed in image_normalizer middleware")),
        _ => Ok(None),
    };
    let attribution_for_access = caller_lookup.as_ref().ok().copied().flatten().map(|c| c.attribution);

    let zdr_dispatch_id = zdr_dispatch_request_id(&request);
    let zdr = zdr_dispatch_id.is_some() || crate::inference::zdr::is_zdr_request(&state.key_policy_cache, presented_key.as_ref());

    let normalizer = state.normalizer.clone();
    let is_daemon_dispatch = match caller_lookup {
        Ok(Some(caller)) => caller.is_daemon_dispatch,
        Ok(None) => false,
        Err(()) => true,
    };
    let sign_ttl = if is_daemon_dispatch { state.token_ttl } else { state.realtime_ttl };

    let grants: Result<Arc<std::collections::HashSet<ImageToken>>, ()> = {
        let tokens = walker::tokens(&body_value);
        match (&caller_lookup, state.pool.as_ref()) {
            (Err(()), _) => Err(()),
            (Ok(Some(caller)), Some(pool)) if !tokens.is_empty() => {
                crate::api::handlers::images::accessible_tokens(&pool.write(), &caller.attribution, &tokens)
                    .await
                    .map(Arc::new)
                    .map_err(|e| warn!(error = %e, "image_access lookup failed while signing tokens"))
            }
            _ => Ok(Arc::new(std::collections::HashSet::new())),
        }
    };

    let zdr_request_key: Result<Option<Arc<Vec<u8>>>, String> = match (zdr_dispatch_id, state.keystore.as_ref()) {
        (Some(request_id), Some(keystore)) if !walker::tokens(&body_value).is_empty() => keystore
            .get(&crate::inference::zdr::key_id(&request_id, crate::inference::zdr::KeyKind::Request))
            .await
            .map(|key| key.map(Arc::new))
            .map_err(|e| e.to_string()),
        _ => Ok(None),
    };

    let pool_for_access = state.pool.clone();
    let mut position = 0usize;
    let substitute = move |url: String| {
        position += 1;
        let label = image_label(position, &url);
        let normalizer = normalizer.clone();
        let pool_for_access = pool_for_access.clone();
        let grants = grants.clone();
        let zdr_request_key = zdr_request_key.clone();
        let is_data_uri = url.starts_with("data:");
        async move {
            let result = async move {
                if ImageToken::looks_like_token(&url) {
                    let token: ImageToken = url
                        .parse()
                        .map_err(|e: TokenParseError| NormalizeError::BadInput(format!("invalid dw-img token: {e}")))?;
                    match grants {
                        Err(()) => return Err(NormalizeError::AccessUnavailable),
                        Ok(set) if !set.contains(&token) => return Err(NormalizeError::Forbidden),
                        Ok(_) => {}
                    }
                    if zdr_dispatch_id.is_some()
                        && let Some(blob) = normalizer.read_sealed(token).await?
                    {
                        return open_sealed_as_data_uri(&zdr_request_key, &blob);
                    }
                    let signed = normalizer.sign(token, sign_ttl).await?;
                    return Ok::<String, NormalizeError>(signed.url);
                }
                if !is_data_uri && !zdr && normalizer.owns_url(&url) {
                    return Ok::<String, NormalizeError>(url);
                }
                let input = if is_data_uri {
                    ImageInput::DataUri(url)
                } else {
                    ImageInput::HttpUrl(url)
                };
                if zdr {
                    let (mime, bytes) = normalizer.load(input).await?;
                    return Ok(sealed::to_data_uri(&mime, &bytes));
                }
                let ingested = normalizer.ingest(input).await?;
                let signed = normalizer.sign(ingested.token, sign_ttl).await?;
                if let (Some(pool), Some(attribution)) = (pool_for_access, attribution_for_access) {
                    let mime = ingested.mime.clone();
                    let bytes_len = ingested.bytes_len;
                    let token = ingested.token;
                    tokio::spawn(async move {
                        crate::api::handlers::images::record_image_access(&pool.write(), attribution, token, &mime, bytes_len).await;
                    });
                }
                Ok::<String, NormalizeError>(signed.url)
            }
            .await;
            result.map_err(|e| e.for_image(&label))
        }
    };

    let substituted = match walker::substitute_with(&mut body_value, mode, substitute).await {
        Ok(n) => n,
        Err(e) => {
            warn!(error = %e, "image normalisation failed");
            return refuse_normalize_error(&request, e, body_value["model"].as_str());
        }
    };

    debug!(substituted, "image normaliser replaced URLs in request body");
    let new_bytes = match serde_json::to_vec(&body_value) {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "Failed to re-serialise body after image normalisation");
            let body = serde_json::json!({
                "error": {
                    "message": format!("failed to re-serialise request body: {e}"),
                    "type": "internal_error",
                    "code": "body_reserialize_failed",
                }
            });
            return (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(body)).into_response();
        }
    };
    let len = new_bytes.len();
    request.headers_mut().insert(
        axum::http::header::CONTENT_LENGTH,
        len.to_string().parse().expect("digit string is a valid header value"),
    );
    *request.body_mut() = Body::from(new_bytes);

    next.run(request).await
}

fn zdr_dispatch_request_id(request: &Request<Body>) -> Option<uuid::Uuid> {
    let headers = request.headers();
    let marked = headers.get(crate::inference::zdr::ZDR_MARKER_HEADER).and_then(|v| v.to_str().ok()) == Some("1");
    if !marked {
        return None;
    }
    headers
        .get("x-fusillade-request-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
}

fn open_sealed_as_data_uri(key: &Result<Option<Arc<Vec<u8>>>, String>, blob: &[u8]) -> Result<String, NormalizeError> {
    let key = match key {
        Ok(Some(key)) => key,
        Ok(None) => return Err(NormalizeError::StoreFailed("ZDR request key unavailable for sealed image".into())),
        Err(e) => return Err(NormalizeError::StoreFailed(format!("ZDR keystore unavailable: {e}"))),
    };
    let (mime, bytes) =
        sealed::open(key, blob).map_err(|e| NormalizeError::StoreFailed(format!("sealed image could not be opened: {e}")))?;
    Ok(sealed::to_data_uri(&mime, &bytes))
}

pub(crate) async fn normalize_value_to_tokens(
    body: &mut Value,
    normalizer: &Arc<dyn ImageNormalizer>,
    access_pool: Option<PgPool>,
    attribution: Option<crate::api::handlers::images::ImageAttribution>,
    seal_key: Option<&[u8]>,
) -> Result<usize, NormalizeError> {
    let normalizer = normalizer.clone();
    let seal_key: Option<Arc<[u8]>> = seal_key.map(Arc::from);
    let mut position = 0usize;
    let substitute = move |url: String| {
        position += 1;
        let label = image_label(position, &url);
        let normalizer = normalizer.clone();
        let access_pool = access_pool.clone();
        let seal_key = seal_key.clone();
        let is_data_uri = url.starts_with("data:");
        async move {
            let result = async move {
                let input = if is_data_uri {
                    ImageInput::DataUri(url)
                } else {
                    ImageInput::HttpUrl(url)
                };
                let ingested = match seal_key {
                    Some(key) => normalizer.ingest_sealed(input, &key).await?,
                    None => normalizer.ingest(input).await?,
                };
                if let Some(pool) = access_pool {
                    let Some(attribution) = attribution else {
                        return Err(NormalizeError::AccessUnavailable);
                    };
                    crate::api::handlers::images::try_record_image_access(
                        &pool,
                        attribution,
                        ingested.token,
                        &ingested.mime,
                        ingested.bytes_len,
                    )
                    .await
                    .map_err(|e| {
                        warn!(error = %e, "image_access bookkeeping failed on flex enqueue");
                        NormalizeError::AccessUnavailable
                    })?;
                }
                Ok::<String, NormalizeError>(ingested.token.to_dw_img_uri())
            }
            .await;
            result.map_err(|e| e.for_image(&label))
        }
    };
    walker::substitute_with(body, Mode::All, substitute).await
}

pub(crate) fn normalize_error_response(err: NormalizeError) -> Response {
    let (status, code) = normalize_error_status(&err);
    let body = serde_json::json!({
        "error": {
            "message": err.to_string(),
            "type": "invalid_request_error",
            "code": code,
        }
    });
    (status, axum::Json(body)).into_response()
}

pub(crate) fn refuse_normalize_error<'a>(origin: impl Into<rejections::Origin<'a>>, err: NormalizeError, model: Option<&str>) -> Response {
    let (status, code) = normalize_error_status(&err);
    let response = normalize_error_response(err);
    if status.is_client_error() {
        rejections::refuse(origin, response, code, Some("image_url"), model)
    } else {
        response
    }
}

fn normalize_error_status(err: &NormalizeError) -> (StatusCode, &'static str) {
    match err {
        NormalizeError::BadInput(_) => (StatusCode::BAD_REQUEST, "image_url_rejected"),
        NormalizeError::Unfetchable(_) => (StatusCode::UNPROCESSABLE_ENTITY, "image_url_unfetchable"),
        NormalizeError::Transient(_) => (StatusCode::SERVICE_UNAVAILABLE, "image_fetch_transient"),
        NormalizeError::FetchFailed(_) => (StatusCode::BAD_GATEWAY, "image_fetch_failed"),
        NormalizeError::StoreFailed(_) => (StatusCode::SERVICE_UNAVAILABLE, "image_store_failed"),
        NormalizeError::NotFound => (StatusCode::INTERNAL_SERVER_ERROR, "image_token_not_found"),
        NormalizeError::Forbidden => (StatusCode::FORBIDDEN, "image_token_forbidden"),
        NormalizeError::AccessUnavailable => (StatusCode::SERVICE_UNAVAILABLE, "image_access_unavailable"),
    }
}

fn path_accepts_images(path: &str) -> bool {
    path.ends_with("/chat/completions") || path.ends_with("/responses")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::image_normalizer::{DefaultImageNormalizer, DisabledNormalizer, ImageNormalizer, MemoryStore, config::FetcherConfig};
    use axum::{Router, body::to_bytes, http::Method, middleware, routing::post};
    use serde_json::json;
    use sqlx::postgres::PgPoolOptions;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    const TINY_PNG_DATA_URI: &str =
        "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=";

    fn build_router(state: ImageNormalizerMiddlewareState) -> Router {
        let inner = post(|body: axum::body::Bytes| async move { (StatusCode::OK, body) });
        Router::new()
            .route("/chat/completions", inner.clone())
            .route("/responses", inner.clone())
            .route("/embeddings", inner)
            .layer(middleware::from_fn_with_state(state, image_normalizer_middleware))
    }

    fn state_for_tests() -> ImageNormalizerMiddlewareState {
        let store = Arc::new(MemoryStore::new().with_base_url("http://test.local/dw-img"));
        let normalizer = Arc::new(DefaultImageNormalizer::new(FetcherConfig::default(), store).with_unique_upload_keys(true));
        ImageNormalizerMiddlewareState {
            enabled: true,
            normalizer,
            realtime_ttl: Duration::from_secs(900),
            token_ttl: Duration::from_secs(1800),
            pool: None,
            keystore: None,
            key_policy_cache: crate::sync::key_policy::KeyPolicyCache::empty(),
        }
    }

    async fn post_json(router: Router, path: &str, body: Value) -> (StatusCode, Value) {
        let resp = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, v)
    }

    #[tokio::test]
    async fn passes_through_when_no_image_urls_present() {
        let router = build_router(state_for_tests());
        let (status, echoed) = post_json(
            router,
            "/chat/completions",
            json!({ "model": "vision", "messages": [ { "role": "user", "content": "hi" } ] }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(echoed["messages"][0]["content"], "hi");
    }

    #[tokio::test]
    async fn only_a_request_with_images_looks_up_its_caller() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let connections = Arc::new(AtomicUsize::new(0));
        tokio::spawn({
            let connections = Arc::clone(&connections);
            async move {
                while listener.accept().await.is_ok() {
                    connections.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy(&format!("postgres://caller:lookup@{address}/dwctl"))
            .unwrap();
        let mut state = state_for_tests();
        state.pool = Some(sqlx_pool_router::DynPools::new(pool));
        let router = build_router(state);

        let text_only = json!({ "model": "vision", "messages": [ { "role": "user", "content": "hi" } ] });
        let (status, echoed) = post_json_as(router.clone(), Some("sk-test"), text_only.clone()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(echoed, text_only);
        assert_eq!(connections.load(Ordering::SeqCst), 0);

        let with_image = json!({
            "model": "vision",
            "messages": [{ "role": "user", "content": [
                { "type": "image_url", "image_url": { "url": TINY_PNG_DATA_URI } }
            ]}]
        });
        let (status, _) = post_json_as(router, Some("sk-test"), with_image).await;
        assert_eq!(status, StatusCode::OK);
        assert!(connections.load(Ordering::SeqCst) > 0);
    }

    #[tokio::test]
    async fn substitutes_data_uri_with_signed_url_in_all_mode() {
        let router = build_router(state_for_tests());
        let (status, echoed) = post_json(
            router,
            "/chat/completions",
            json!({
                "model": "vision",
                "messages": [{
                    "role": "user",
                    "content": [{ "type": "image_url", "image_url": { "url": TINY_PNG_DATA_URI } }]
                }]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let substituted = echoed["messages"][0]["content"][0]["image_url"]["url"]
            .as_str()
            .expect("image_url.url should still be a string");
        assert_ne!(
            substituted, TINY_PNG_DATA_URI,
            "data: URI should be substituted, not passed through"
        );
        assert!(
            substituted.starts_with("http://test.local/dw-img/"),
            "expected MemoryStore-backed signed URL, got: {substituted}",
        );
    }

    #[tokio::test]
    async fn passes_through_url_already_in_our_store_unchanged() {
        let already_ours = "http://test.local/dw-img/abcdef0123456789?expires=9999999999";
        let router = build_router(state_for_tests());
        let (status, echoed) = post_json(
            router,
            "/chat/completions",
            json!({
                "model": "vision",
                "messages": [{
                    "role": "user",
                    "content": [{ "type": "image_url", "image_url": { "url": already_ours } }]
                }]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let forwarded = echoed["messages"][0]["content"][0]["image_url"]["url"]
            .as_str()
            .expect("image_url.url should still be a string");
        assert_eq!(
            forwarded, already_ours,
            "a URL already in our store must pass through unchanged (no re-sign / TTL clobber)"
        );
    }

    #[tokio::test]
    async fn rejects_http_url_to_link_local_with_400() {
        let router = build_router(state_for_tests());
        let (status, body) = post_json(
            router,
            "/chat/completions",
            json!({
                "model": "vision",
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "image_url",
                        "image_url": { "url": "http://169.254.169.254/latest/meta-data/" }
                    }]
                }]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "image_url_rejected");
    }

    #[test]
    fn counts_refused_images_but_not_failures_on_our_side() {
        use crate::inference::rejections::{self, test_support};
        let (lines, status) = test_support::rejections(async {
            let router = build_router(state_for_tests()).layer(test_support::layer("vision"));
            let body = json!({
                "model": "vision",
                "messages": [{"role": "user", "content": [{
                    "type": "image_url",
                    "image_url": { "url": "http://169.254.169.254/latest/meta-data/" }
                }]}]
            });
            let (status, _) = post_json(router, "/chat/completions", body).await;

            let recorder = test_support::recorder("vision");
            let headers = axum::http::HeaderMap::new();
            let origin = rejections::Origin::new(Some(&recorder), &headers, "/chat/completions");
            let unavailable = refuse_normalize_error(origin, NormalizeError::StoreFailed("down".into()), Some("vision"));
            assert!(unavailable.extensions().get::<rejections::RejectionCode>().is_none());
            status
        });
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(lines.len(), 1, "a 503 is not a rejection: {lines:?}");
        assert!(
            lines[0].contains("code=\"image_url_rejected\"") && lines[0].contains("model=\"vision\""),
            "{lines:?}"
        );
    }

    #[tokio::test]
    async fn does_not_touch_unrelated_paths() {
        let router = build_router(state_for_tests());
        let resp = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/embeddings")
                    .header("content-type", "application/json")
                    .body(Body::from("not json at all"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&bytes[..], b"not json at all");
    }

    #[tokio::test]
    async fn non_json_body_on_image_path_passes_through() {
        let router = build_router(state_for_tests());
        let resp = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/chat/completions")
                    .header("content-type", "application/json")
                    .body(Body::from("garbage"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&bytes[..], b"garbage");
    }

    #[tokio::test]
    async fn disabled_passes_image_request_through_unchanged() {
        let state = ImageNormalizerMiddlewareState {
            enabled: false,
            normalizer: Arc::new(DisabledNormalizer),
            realtime_ttl: Duration::from_secs(900),
            token_ttl: Duration::from_secs(1800),
            pool: None,
            keystore: None,
            key_policy_cache: crate::sync::key_policy::KeyPolicyCache::empty(),
        };
        let router = build_router(state);
        let body = json!({
            "model": "m",
            "messages": [{"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": TINY_PNG_DATA_URI}}
            ]}]
        });
        let (status, echoed) = post_json(router, "/chat/completions", body.clone()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(echoed, body);
    }

    #[tokio::test]
    async fn normalize_value_to_tokens_replaces_image_with_token() {
        let store = Arc::new(MemoryStore::new());
        let normalizer: Arc<dyn ImageNormalizer> = Arc::new(DefaultImageNormalizer::new(FetcherConfig::default(), store));
        let mut body = json!({
            "model": "m",
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "hi"},
                {"type": "image_url", "image_url": {"url": TINY_PNG_DATA_URI}}
            ]}]
        });
        let n = normalize_value_to_tokens(&mut body, &normalizer, None, None, None).await.unwrap();
        assert_eq!(n, 1);
        let url = body["messages"][0]["content"][1]["image_url"]["url"].as_str().unwrap();
        assert!(url.starts_with("dw-img://"), "expected a dw-img token, got {url}");
        assert!(!url.contains("base64"), "raw base64 must be replaced");
    }


    fn state_over(store: Arc<MemoryStore>) -> ImageNormalizerMiddlewareState {
        let mut state = state_for_tests();
        state.normalizer = Arc::new(DefaultImageNormalizer::new(FetcherConfig::default(), store).with_unique_upload_keys(true));
        state
    }

    #[tokio::test]
    async fn a_realtime_zdr_callers_image_is_inlined_and_never_stored() {
        let store = Arc::new(MemoryStore::new().with_base_url("http://test.local/dw-img"));
        let mut state = state_over(store.clone());
        state.key_policy_cache = crate::sync::key_policy::KeyPolicyCache::from_pairs([("sk-zdr".to_string(), true)]);
        let body = json!({
            "model": "m",
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "hi"},
                {"type": "image_url", "image_url": {"url": TINY_PNG_DATA_URI}}
            ]}]
        });

        let (status, echoed) = post_json_as(build_router(state), Some("sk-zdr"), body).await;

        assert_eq!(status, StatusCode::OK, "{echoed}");
        let url = echoed["messages"][0]["content"][1]["image_url"]["url"].as_str().unwrap();
        assert_eq!(url, TINY_PNG_DATA_URI, "validated and inlined, not swapped for a signed URL");
        assert!(store.objects().is_empty(), "a ZDR image must never reach the store");
    }

    #[tokio::test]
    async fn a_zdr_caller_echoing_one_of_our_urls_does_not_get_it_forwarded() {
        let store = Arc::new(MemoryStore::new().with_base_url("http://test.local/dw-img"));
        let mut state = state_over(store);
        state.key_policy_cache =
            crate::sync::key_policy::KeyPolicyCache::from_pairs([("sk-zdr".to_string(), true), ("sk-plain".to_string(), false)]);
        let ours = "http://test.local/dw-img/abcd?expires=1";
        let body = json!({
            "model": "m",
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "hi"},
                {"type": "image_url", "image_url": {"url": ours}}
            ]}]
        });

        let (status, echoed) = post_json_as(build_router(state.clone()), Some("sk-plain"), body.clone()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(echoed["messages"][0]["content"][1]["image_url"]["url"], ours);

        let (status, echoed) = post_json_as(build_router(state), Some("sk-zdr"), body).await;
        assert_ne!(status, StatusCode::OK, "{echoed}");
        assert!(!echoed.to_string().contains(ours), "ZDR must not forward a store URL: {echoed}");
    }

    #[tokio::test]
    async fn a_non_zdr_callers_image_is_still_stored_and_signed() {
        let store = Arc::new(MemoryStore::new().with_base_url("http://test.local/dw-img"));
        let mut state = state_over(store.clone());
        state.key_policy_cache = crate::sync::key_policy::KeyPolicyCache::from_pairs([("sk-plain".to_string(), false)]);
        let body = json!({
            "model": "m",
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "hi"},
                {"type": "image_url", "image_url": {"url": TINY_PNG_DATA_URI}}
            ]}]
        });

        let (status, echoed) = post_json_as(build_router(state), Some("sk-plain"), body).await;

        assert_eq!(status, StatusCode::OK, "{echoed}");
        let url = echoed["messages"][0]["content"][1]["image_url"]["url"].as_str().unwrap();
        assert!(url.starts_with("http://test.local/dw-img/"), "{url}");
        assert_eq!(store.objects().len(), 1);
    }

    #[tokio::test]
    async fn flex_enqueue_with_a_zdr_key_stores_only_ciphertext() {
        let store = Arc::new(MemoryStore::new());
        let normalizer: Arc<dyn ImageNormalizer> = Arc::new(DefaultImageNormalizer::new(FetcherConfig::default(), store.clone()));
        let key = crate::keystore::generate_key();
        let mut body = json!({
            "model": "m",
            "messages": [{"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": TINY_PNG_DATA_URI}}
            ]}]
        });

        let n = normalize_value_to_tokens(&mut body, &normalizer, None, None, Some(&key))
            .await
            .unwrap();

        assert_eq!(n, 1);
        let url = body["messages"][0]["content"][0]["image_url"]["url"].as_str().unwrap();
        assert!(url.starts_with("dw-img://"), "{url}");
        let objects = store.objects();
        assert_eq!(objects.len(), 1);
        assert!(sealed::is_sealed(&objects[0]), "ZDR image must be sealed before it is stored");
        let inlined = open_sealed_as_data_uri(&Ok(Some(Arc::new(key.to_vec()))), &objects[0]).unwrap();
        assert_eq!(inlined, TINY_PNG_DATA_URI);
    }

    #[test]
    fn a_sealed_image_without_its_key_is_a_retryable_failure_not_a_url() {
        let key = crate::keystore::generate_key();
        let blob = sealed::seal(&key, "image/png", b"img").unwrap();
        assert!(matches!(
            open_sealed_as_data_uri(&Ok(None), &blob),
            Err(NormalizeError::StoreFailed(_))
        ));
        assert!(matches!(
            open_sealed_as_data_uri(&Err("redis down".into()), &blob),
            Err(NormalizeError::StoreFailed(_))
        ));
        let wrong = Arc::new(crate::keystore::generate_key().to_vec());
        assert!(matches!(
            open_sealed_as_data_uri(&Ok(Some(wrong)), &blob),
            Err(NormalizeError::StoreFailed(_))
        ));
    }

    #[test]
    fn zdr_dispatch_needs_the_marker_and_a_request_id() {
        let id = uuid::Uuid::new_v4();
        let req = |headers: &[(&str, &str)]| {
            let mut b = Request::builder();
            for (k, v) in headers {
                b = b.header(*k, *v);
            }
            b.body(Body::empty()).unwrap()
        };
        let id_str = id.to_string();
        assert_eq!(
            zdr_dispatch_request_id(&req(&[("x-fusillade-batch-zdr", "1"), ("x-fusillade-request-id", &id_str)])),
            Some(id)
        );
        assert_eq!(zdr_dispatch_request_id(&req(&[("x-fusillade-request-id", &id_str)])), None);
        assert_eq!(
            zdr_dispatch_request_id(&req(&[("x-fusillade-batch-zdr", "0"), ("x-fusillade-request-id", &id_str)])),
            None
        );
        assert_eq!(zdr_dispatch_request_id(&req(&[("x-fusillade-batch-zdr", "1")])), None);
    }

    #[dwctl_test_macros::test]
    async fn a_zdr_dispatch_inlines_or_refuses_sealed_tokens_and_signs_plaintext_ones(pool: sqlx::PgPool) {
        use crate::api::models::users::Role;
        use crate::test::utils::create_test_user;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let batch_key = hidden_batch_key_for(&pool, user.id).await;
        let state = state_with_pool(&pool);
        let attribution = crate::api::handlers::images::resolve_image_attribution(&pool, &batch_key)
            .await
            .expect("key resolves");
        let key = crate::keystore::generate_key();
        let sealed_ingest = state
            .normalizer
            .ingest_sealed(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string()), &key)
            .await
            .unwrap();
        crate::api::handlers::images::record_image_access(
            &pool,
            attribution,
            sealed_ingest.token,
            &sealed_ingest.mime,
            sealed_ingest.bytes_len,
        )
        .await;
        let plain_token = ingest_for_key(&pool, &state, &batch_key).await;

        let dispatch = |token: crate::image_normalizer::ImageToken| {
            Request::builder()
                .method(Method::POST)
                .uri("/chat/completions")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {batch_key}"))
                .header("x-fusillade-batch-zdr", "1")
                .header("x-fusillade-request-id", uuid::Uuid::new_v4().to_string())
                .body(Body::from(serde_json::to_vec(&body_with_token(token)).unwrap()))
                .unwrap()
        };

        let resp = build_router(state.clone()).oneshot(dispatch(sealed_ingest.token)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains(&sealed_ingest.token.to_hex()));

        let resp = build_router(state).oneshot(dispatch(plain_token)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let echoed: Value = serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
        let (url, _) = signed_url_and_ttl(&echoed);
        assert!(url.contains(&plain_token.to_hex()), "{url}");
    }


    async fn post_json_as(router: Router, bearer: Option<&str>, body: Value) -> (StatusCode, Value) {
        let mut req = Request::builder()
            .method(Method::POST)
            .uri("/chat/completions")
            .header("content-type", "application/json");
        if let Some(b) = bearer {
            req = req.header("authorization", format!("Bearer {b}"));
        }
        let resp = router
            .oneshot(req.body(Body::from(serde_json::to_vec(&body).unwrap())).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    fn body_with_token(token: crate::image_normalizer::ImageToken) -> Value {
        json!({
            "model": "vision",
            "messages": [{ "role": "user", "content": [
                { "type": "text", "text": "what is this?" },
                { "type": "image_url", "image_url": { "url": token.to_dw_img_uri() } }
            ]}]
        })
    }

    fn state_with_pool(pool: &sqlx::PgPool) -> ImageNormalizerMiddlewareState {
        let mut state = state_for_tests();
        state.pool = Some(sqlx_pool_router::DynPools::new(pool.clone()));
        state
    }

    fn signed_url_and_ttl(echoed: &Value) -> (String, i64) {
        let url = echoed["messages"][0]["content"][1]["image_url"]["url"]
            .as_str()
            .unwrap()
            .to_string();
        let expires: i64 = url.split("expires=").nth(1).expect("expires param").parse().expect("unix ts");
        (url, expires - chrono::Utc::now().timestamp())
    }

    async fn create_org_key_for_member(pool: &sqlx::PgPool, org: crate::types::UserId, member: crate::types::UserId) -> String {
        use crate::api::models::api_keys::ApiKeyCreate;
        use crate::db::handlers::api_keys::ApiKeys;
        use crate::db::handlers::repository::Repository;
        use crate::db::models::api_keys::{ApiKeyCreateDBRequest, ApiKeyPurpose};

        let mut conn = pool.acquire().await.expect("acquire");
        let mut repo = ApiKeys::new(&mut conn);
        let request = ApiKeyCreateDBRequest::new(
            org,
            member,
            ApiKeyCreate {
                name: format!("org key {}", uuid::Uuid::new_v4().simple()),
                description: None,
                purpose: ApiKeyPurpose::Realtime,
                member_id: None,
                spend_limit: None,
                spend_limit_interval: None,
            },
        );
        repo.create(&request).await.expect("create org key").secret
    }

    async fn hidden_batch_key_for(pool: &sqlx::PgPool, user: crate::types::UserId) -> String {
        use crate::db::handlers::api_keys::ApiKeys;
        use crate::db::models::api_keys::ApiKeyPurpose;

        let mut conn = pool.acquire().await.expect("acquire");
        let mut repo = ApiKeys::new(&mut conn);
        let id = repo
            .get_or_create_hidden_key(user, ApiKeyPurpose::Batch, user)
            .await
            .expect("hidden batch key");
        crate::test::utils::bearer_for_key(pool, id).await
    }

    async fn ingest_unrecorded(state: &ImageNormalizerMiddlewareState) -> crate::image_normalizer::ImageToken {
        state
            .normalizer
            .ingest(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string()))
            .await
            .expect("ingest")
            .token
    }

    async fn ingest_for_key(
        pool: &sqlx::PgPool,
        state: &ImageNormalizerMiddlewareState,
        key_secret: &str,
    ) -> crate::image_normalizer::ImageToken {
        let ingested = state
            .normalizer
            .ingest(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string()))
            .await
            .expect("ingest");
        let attribution = crate::api::handlers::images::resolve_image_attribution(pool, key_secret)
            .await
            .expect("key resolves");
        crate::api::handlers::images::record_image_access(pool, attribution, ingested.token, &ingested.mime, ingested.bytes_len).await;
        ingested.token
    }

    #[dwctl_test_macros::test]
    async fn a_daemon_dispatch_signs_a_token_its_principal_submitted_with_the_dispatch_ttl(pool: sqlx::PgPool) {
        use crate::api::models::users::Role;
        use crate::test::utils::create_test_user;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let batch_key = hidden_batch_key_for(&pool, user.id).await;
        let state = state_with_pool(&pool);
        let token = ingest_for_key(&pool, &state, &batch_key).await;

        let (status, echoed) = post_json_as(build_router(state), Some(&batch_key), body_with_token(token)).await;

        assert_eq!(status, StatusCode::OK, "{echoed}");
        let (url, ttl) = signed_url_and_ttl(&echoed);
        assert!(url.contains(&token.to_hex()), "{url}");
        assert!((1800 - 60..=1800).contains(&ttl), "dispatch TTL expected, got {ttl}s");
        assert_eq!(echoed["messages"][0]["content"][0]["text"], "what is this?");
    }

    #[dwctl_test_macros::test]
    async fn a_daemon_dispatch_is_refused_a_token_its_principal_never_submitted(pool: sqlx::PgPool) {
        use crate::api::models::users::Role;
        use crate::test::utils::create_test_user;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let batch_key = hidden_batch_key_for(&pool, user.id).await;
        let state = state_with_pool(&pool);
        let token = ingest_unrecorded(&state).await;

        let (status, body) = post_json_as(build_router(state), Some(&batch_key), body_with_token(token)).await;

        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["code"], "image_token_forbidden");
    }

    #[dwctl_test_macros::test]
    async fn a_daemon_dispatch_ingesting_a_raw_image_signs_it_with_the_dispatch_ttl(pool: sqlx::PgPool) {
        use crate::api::models::users::Role;
        use crate::test::utils::create_test_user;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let batch_key = hidden_batch_key_for(&pool, user.id).await;
        let state = state_with_pool(&pool);
        let body = json!({
            "model": "vision",
            "messages": [{ "role": "user", "content": [
                { "type": "text", "text": "what is this?" },
                { "type": "image_url", "image_url": { "url": TINY_PNG_DATA_URI } }
            ]}]
        });

        let (status, echoed) = post_json_as(build_router(state), Some(&batch_key), body).await;

        assert_eq!(status, StatusCode::OK, "{echoed}");
        let (url, ttl) = signed_url_and_ttl(&echoed);
        assert!(url.starts_with("http://test.local/dw-img/"), "{url}");
        assert!((1800 - 60..=1800).contains(&ttl), "dispatch TTL expected, got {ttl}s");
    }

    #[dwctl_test_macros::test]
    async fn a_client_resending_its_own_token_is_signed_with_the_realtime_ttl(pool: sqlx::PgPool) {
        use crate::api::models::users::Role;
        use crate::test::utils::{create_test_api_key_for_user, create_test_user};

        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;
        let state = state_with_pool(&pool);
        let token = ingest_for_key(&pool, &state, &key.secret).await;

        let (status, echoed) = post_json_as(build_router(state), Some(&key.secret), body_with_token(token)).await;

        assert_eq!(status, StatusCode::OK, "{echoed}");
        let (url, ttl) = signed_url_and_ttl(&echoed);
        assert!(url.contains(&token.to_hex()), "{url}");
        assert!((900 - 60..=900).contains(&ttl), "realtime TTL expected, got {ttl}s");
    }

    #[dwctl_test_macros::test]
    async fn a_client_may_resend_a_token_another_member_of_its_organization_submitted(pool: sqlx::PgPool) {
        use crate::api::models::users::Role;
        use crate::test::utils::{add_org_member, create_test_org, create_test_user};

        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, alice.id).await;
        add_org_member(&pool, org.id, bob.id, "member").await;
        let alice_org_key = create_org_key_for_member(&pool, org.id, alice.id).await;
        let bob_org_key = create_org_key_for_member(&pool, org.id, bob.id).await;
        let state = state_with_pool(&pool);
        let token = ingest_for_key(&pool, &state, &alice_org_key).await;

        let (status, echoed) = post_json_as(build_router(state), Some(&bob_org_key), body_with_token(token)).await;

        assert_eq!(status, StatusCode::OK, "{echoed}");
        let (url, _) = signed_url_and_ttl(&echoed);
        assert!(url.contains(&token.to_hex()), "{url}");
    }

    #[dwctl_test_macros::test]
    async fn a_client_is_refused_a_token_it_never_submitted(pool: sqlx::PgPool) {
        use crate::api::models::users::Role;
        use crate::test::utils::{create_test_api_key_for_user, create_test_user};

        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_key = create_test_api_key_for_user(&pool, owner.id).await;
        let other = create_test_user(&pool, Role::StandardUser).await;
        let other_key = create_test_api_key_for_user(&pool, other.id).await;
        let state = state_with_pool(&pool);
        let token = ingest_for_key(&pool, &state, &owner_key.secret).await;

        let (status, body) = post_json_as(build_router(state), Some(&other_key.secret), body_with_token(token)).await;

        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["code"], "image_token_forbidden");
    }

    #[dwctl_test_macros::test]
    async fn an_unattributable_client_is_refused_a_token(pool: sqlx::PgPool) {
        use crate::api::models::users::Role;
        use crate::test::utils::{create_test_api_key_for_user, create_test_user};

        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_key = create_test_api_key_for_user(&pool, owner.id).await;
        let state = state_with_pool(&pool);
        let token = ingest_for_key(&pool, &state, &owner_key.secret).await;

        let (status, body) = post_json_as(build_router(state), None, body_with_token(token)).await;

        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    }


    async fn recording_upstream(
        axum::extract::State(seen): axum::extract::State<Arc<std::sync::Mutex<Vec<String>>>>,
        body: axum::body::Bytes,
    ) -> axum::Json<Value> {
        let v: Value = serde_json::from_slice(&body).unwrap();
        let url = v["messages"][0]["content"][1]["image_url"]["url"].as_str().unwrap().to_string();
        seen.lock().unwrap().push(url);
        axum::Json(json!({
            "id": "chatcmpl-1", "object": "chat.completion",
            "choices": [{"index":0,"message":{"role":"assistant","content":"a pixel"},"finish_reason":"stop"}],
            "usage": {"prompt_tokens": 2000, "completion_tokens": 2, "total_tokens": 2002}
        }))
    }

    #[dwctl_test_macros::test]
    async fn a_repeated_dispatch_reads_the_image_prefix_it_wrote_while_upstream_gets_signed_urls(pool: sqlx::PgPool) {
        use crate::api::models::users::Role;
        use crate::prompt_cache::{
            CacheIndex, CacheLayerState, Classifier, IndexScope, ModelConfigResolver, PostgresIndex, PrincipalResolver, TelemetryPolicy,
            TierPolicy, TokenizerClient, cache_middleware, parse_chat_completions,
        };
        use crate::test::utils::{create_test_endpoint, create_test_model, create_test_user};
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        const ALIAS: &str = "vision-cached";
        const TOK_VER: &str = "sha256:img1";

        let user = create_test_user(&pool, Role::StandardUser).await;
        let batch_key = hidden_batch_key_for(&pool, user.id).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;
        let model_id = create_test_model(&pool, "m", ALIAS, endpoint, user.id).await;
        sqlx::query!(
            r#"INSERT INTO model_cache_tariffs
                 (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, min_prefix_tokens)
               VALUES ($1, 1.25, 2.0, 2.5, 1024)"#,
            model_id
        )
        .execute(&pool)
        .await
        .unwrap();

        let tok = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "models": [{"alias": ALIAS, "hf_repo": "o/m", "tokenizer_version": TOK_VER}]
            })))
            .mount(&tok)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/tokenize"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "virtual_model": ALIAS, "tokenizer_version": TOK_VER,
                "segment_counts": [1400, 100], "cumulative": [1400, 1500], "total": 1500
            })))
            .mount(&tok)
            .await;
        let tiers = TierPolicy::from_config(&["5m".to_string(), "1h".to_string()], "5m");
        let classifier = Classifier::new(
            PrincipalResolver::new(pool.clone()),
            ModelConfigResolver::new(pool.clone()),
            TokenizerClient::new(tok.uri()),
            Arc::new(PostgresIndex::new(pool.clone(), 1)),
            tiers.clone(),
            TelemetryPolicy::default(),
            false,
        );

        let state = state_with_pool(&pool);
        let token = ingest_for_key(&pool, &state, &batch_key).await;
        let other_copy = ingest_for_key(&pool, &state, &batch_key).await;
        assert_eq!(token.0, other_copy.0);
        assert_ne!(token, other_copy);
        let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
        let app = Router::new()
            .route("/v1/chat/completions", post(recording_upstream).with_state(seen.clone()))
            .layer(middleware::from_fn_with_state(state, image_normalizer_middleware))
            .layer(middleware::from_fn_with_state(
                CacheLayerState::new(classifier, usize::MAX, Duration::from_secs(5)),
                cache_middleware,
            ));
        let server = axum_test::TestServer::new(app).unwrap();

        let body_for = |t: crate::image_normalizer::ImageToken| {
            json!({
                "model": ALIAS,
                "messages": [{ "role": "user", "content": [
                    { "type": "text", "text": "what is this?" },
                    { "type": "image_url", "image_url": { "url": t.to_dw_img_uri() },
                      "cache_control": { "type": "ephemeral", "ttl": "1h" } }
                ]}]
            })
        };
        let stored_body = body_for(token);
        let dispatch_body = |body: &Value| {
            server
                .post("/v1/chat/completions")
                .add_header("authorization", format!("Bearer {batch_key}"))
                .json(body)
        };
        let dispatch = || dispatch_body(&stored_body);

        let r1 = dispatch().await;
        r1.assert_status_ok();
        let v1: Value = r1.json();
        assert_eq!(v1["usage"]["cache_creation_input_tokens"], 1500, "{v1}");
        assert_eq!(v1["usage"]["cache_read_input_tokens"], 0);

        let scope = IndexScope {
            principal_id: user.id,
            virtual_model: ALIAS.into(),
            tokenizer_version: TOK_VER.into(),
        };
        let parsed = parse_chat_completions(&serde_json::to_vec(&stored_body).unwrap(), &tiers, &TelemetryPolicy::default()).unwrap();
        let hash = parsed.cumulative_hashes[parsed.breakpoints[0].block_index].clone();
        let idx = PostgresIndex::new(pool.clone(), 1);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while idx.lookup(&scope, std::slice::from_ref(&hash)).await.unwrap().is_empty() {
            assert!(std::time::Instant::now() < deadline, "the write did not commit within 5s");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let r2 = dispatch().await;
        r2.assert_status_ok();
        let v2: Value = r2.json();
        assert_eq!(v2["usage"]["cache_read_input_tokens"], 1500, "{v2}");
        assert_eq!(v2["usage"]["cache_creation_input_tokens"], 0);

        let r3 = dispatch_body(&body_for(other_copy)).await;
        r3.assert_status_ok();
        let v3: Value = r3.json();
        assert_eq!(v3["usage"]["cache_read_input_tokens"], 1500, "{v3}");
        assert_eq!(v3["usage"]["cache_creation_input_tokens"], 0);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        for (url, t) in seen.iter().zip([token, token, other_copy]) {
            assert!(url.starts_with("http://test.local/dw-img/"), "{url}");
            assert!(url.contains(&t.to_hex()), "{url}");
            assert!(!url.contains("dw-img://"), "{url}");
        }
    }

    #[test]
    fn error_response_maps_each_variant_to_the_right_status() {
        let cases = [
            (NormalizeError::BadInput("x".into()), StatusCode::BAD_REQUEST),
            (
                NormalizeError::Unfetchable("origin 403 Forbidden".into()),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (NormalizeError::Transient("x".into()), StatusCode::SERVICE_UNAVAILABLE),
            (NormalizeError::FetchFailed("x".into()), StatusCode::BAD_GATEWAY),
            (NormalizeError::StoreFailed("x".into()), StatusCode::SERVICE_UNAVAILABLE),
            (NormalizeError::NotFound, StatusCode::INTERNAL_SERVER_ERROR),
            (NormalizeError::Forbidden, StatusCode::FORBIDDEN),
            (NormalizeError::AccessUnavailable, StatusCode::SERVICE_UNAVAILABLE),
        ];
        for (err, expected) in cases {
            assert_eq!(normalize_error_response(err).status(), expected);
        }
    }
}
