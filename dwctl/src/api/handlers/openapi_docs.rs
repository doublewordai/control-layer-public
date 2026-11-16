
use axum::{
    Json,
    response::{Html, IntoResponse, Response},
};
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};

use crate::{
    api::models::users::CurrentUser,
    auth::permissions::{RequiresPermission, operation, resource},
    openapi::{AdminApiDoc, AiApiDoc},
};

const SCALAR_JS_URL: &str = "https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.64.0/dist/browser/standalone.js";
const SCALAR_JS_SRI: &str = "sha384-ei8P62VHbV+6AdLO3hN333PsTEYp6k9OAVhlYvpmer+zdPIf8jSbdgh9ojiLWX3T";

fn scalar_html(title: &str) -> String {
    format!(
        r#"<!doctype html>
<html>
<head>
    <title>{title}</title>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
</head>
<body>
<script id="api-reference" type="application/json">
    $spec
</script>
<script src="{SCALAR_JS_URL}" integrity="{SCALAR_JS_SRI}" crossorigin="anonymous"></script>
</body>
</html>"#
    )
}

#[tracing::instrument(skip_all)]
pub async fn admin_openapi_json(_: RequiresPermission<resource::System, operation::ReadAll>) -> Json<utoipa::openapi::OpenApi> {
    Json(AdminApiDoc::openapi())
}

#[tracing::instrument(skip_all)]
pub async fn admin_openapi_docs(_: RequiresPermission<resource::System, operation::ReadAll>) -> Response {
    let scalar = Scalar::with_url("/admin/openapi.json", AdminApiDoc::openapi()).custom_html(scalar_html("Admin API"));
    Html(scalar.to_html()).into_response()
}

#[tracing::instrument(skip_all)]
pub async fn ai_openapi_json(_: CurrentUser) -> Json<utoipa::openapi::OpenApi> {
    Json(AiApiDoc::openapi())
}

#[tracing::instrument(skip_all)]
pub async fn ai_openapi_docs(_: CurrentUser) -> Response {
    let scalar = Scalar::with_url("/ai/openapi.json", AiApiDoc::openapi()).custom_html(scalar_html("AI API"));
    Html(scalar.to_html()).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_html_renders_pinned_bundle_and_inline_spec() {
        let html = Scalar::with_url("/ai/openapi.json", AiApiDoc::openapi())
            .custom_html(scalar_html("AI API"))
            .to_html();

        assert!(html.contains(&format!(
            r#"src="{SCALAR_JS_URL}" integrity="{SCALAR_JS_SRI}" crossorigin="anonymous""#
        )));
        assert!(!html.contains("$spec"));
        assert!(html.contains(r#""openapi""#));
        assert!(!html.contains(r#"src="https://cdn.jsdelivr.net/npm/@scalar/api-reference""#));
    }
}
