
use axum::{
    body::Body,
    extract::State,
    http::{Response, StatusCode, Uri},
    response::{Html, IntoResponse},
};
use tracing::{debug, instrument};

use crate::{AppState, static_assets};
use sqlx_pool_router::PoolProvider;

const DEFAULT_TITLE: &str = "Doubleword Control Layer";

fn inject_title(html: &str, title: Option<&str>) -> String {
    let title = title.unwrap_or(DEFAULT_TITLE);
    html.replace(&format!("<title>{}</title>", DEFAULT_TITLE), &format!("<title>{}</title>", title))
}

#[instrument(skip(state))]
pub async fn serve_embedded_asset<P: PoolProvider + Clone>(State(state): State<AppState<P>>, uri: Uri) -> impl IntoResponse {
    let config = state.current_config();
    let mut path = uri.path().trim_start_matches('/');

    if path.is_empty() || path.ends_with('/') {
        path = "index.html";
    }

    if let Some(content) = static_assets::Assets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();

        let cache_control = if path.starts_with("assets/") {
            "public, max-age=31536000, immutable"
        } else {
            "no-cache"
        };

        if path == "index.html" {
            let html = String::from_utf8_lossy(&content.data);
            let html_with_title = inject_title(&html, config.metadata.title.as_deref());
            return Response::builder()
                .header(axum::http::header::CONTENT_TYPE, mime.as_ref())
                .header(axum::http::header::CACHE_CONTROL, cache_control)
                .body(Body::from(html_with_title))
                .unwrap();
        }

        return Response::builder()
            .header(axum::http::header::CONTENT_TYPE, mime.as_ref())
            .header(axum::http::header::CACHE_CONTROL, cache_control)
            .body(Body::from(content.data.into_owned()))
            .unwrap();
    }

    if let Some(index) = static_assets::Assets::get("index.html") {
        let html = String::from_utf8_lossy(&index.data);
        let html_with_title = inject_title(&html, config.metadata.title.as_deref());
        return Response::builder()
            .header(axum::http::header::CONTENT_TYPE, "text/html")
            .header(axum::http::header::CACHE_CONTROL, "no-cache")
            .body(Body::from(html_with_title))
            .unwrap();
    }

    Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty()).unwrap()
}

#[instrument(skip(state), err)]
pub async fn spa_fallback<P: PoolProvider + Clone>(State(state): State<AppState<P>>, uri: Uri) -> Result<Html<String>, StatusCode> {
    debug!("Hitting SPA fallback for: {}", uri.path());
    let config = state.current_config();

    if let Some(index) = static_assets::Assets::get("index.html") {
        let html = String::from_utf8_lossy(&index.data);
        let html_with_title = inject_title(&html, config.metadata.title.as_deref());
        Ok(Html(html_with_title))
    } else {
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}
