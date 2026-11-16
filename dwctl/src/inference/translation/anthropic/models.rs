
use axum::http::{HeaderMap, StatusCode, request::Parts};
use bytes::Bytes;
use serde_json::Value;

use super::super::{ProtocolTranslator, StreamReframer, TranslatedRequest, TranslationError};
use super::model::{ModelObject, ModelObjectType, ModelsListResponse};
use super::{normalize_auth, response};

pub struct AnthropicModels;

impl ProtocolTranslator for AnthropicModels {
    fn name(&self) -> &'static str {
        "anthropic_models"
    }

    fn detect(&self, path: &str, headers: &HeaderMap) -> bool {
        path.ends_with("/models") && headers.contains_key("anthropic-version")
    }

    fn translates_request_body(&self) -> bool {
        false
    }

    fn translate_request(&self, parts: &Parts, body: Bytes) -> Result<TranslatedRequest, TranslationError> {
        let mut headers = parts.headers.clone();
        normalize_auth(&mut headers);
        Ok(TranslatedRequest {
            uri: parts.uri.clone(),
            headers,
            body,
        })
    }

    fn translate_response(&self, _request: &Bytes, _response_id: Option<&str>, body: Bytes) -> Result<Bytes, TranslationError> {
        from_openai_models(body)
    }

    fn translate_error(&self, status: StatusCode, body: Bytes) -> (StatusCode, Bytes) {
        response::error_to_anthropic(status, body)
    }

    fn error_from_message(&self, status: StatusCode, message: &str) -> (StatusCode, Bytes) {
        response::anthropic_error(status, message.to_string())
    }

    fn stream_reframer(&self, _request: &Bytes, _response_id: Option<&str>) -> Box<dyn StreamReframer> {
        Box::new(NoopReframer)
    }
}

fn from_openai_models(body: Bytes) -> Result<Bytes, TranslationError> {
    let resp: Value = serde_json::from_slice(&body).map_err(|e| TranslationError::Internal(format!("parse models response: {e}")))?;

    let models = resp
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| TranslationError::Internal("upstream models response has no \"data\" array".into()))?;

    let data: Vec<ModelObject> = models
        .iter()
        .map(|m| {
            let id = m
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| TranslationError::Internal("model entry is missing a string \"id\"".into()))?
                .to_string();
            let secs = m.get("created").and_then(Value::as_i64).unwrap_or(0);
            let created_at = chrono::DateTime::from_timestamp(secs, 0).unwrap_or_default().to_rfc3339();
            Ok(ModelObject {
                object_type: ModelObjectType::Model,
                display_name: id.clone(),
                id,
                created_at,
            })
        })
        .collect::<Result<_, TranslationError>>()?;

    let first_id = data.first().map(|m| m.id.clone());
    let last_id = data.last().map(|m| m.id.clone());

    let out = ModelsListResponse {
        data,
        has_more: false,
        first_id,
        last_id,
    };

    serde_json::to_vec(&out)
        .map(Bytes::from)
        .map_err(|e| TranslationError::Internal(e.to_string()))
}

struct NoopReframer;

impl StreamReframer for NoopReframer {
    fn push(&mut self, _chunk: &Value) -> Vec<u8> {
        Vec::new()
    }

    fn error(&mut self, _message: &str) -> Vec<u8> {
        Vec::new()
    }

    fn finish(&mut self) -> Vec<u8> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use serde_json::json;

    #[test]
    fn detect_requires_models_path_and_anthropic_header() {
        let t = AnthropicModels;
        let mut h = HeaderMap::new();
        h.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        assert!(t.detect("/v1/models", &h));
        assert!(t.detect("/models", &h));
        assert!(!t.detect("/v1/models", &HeaderMap::new()));
        assert!(!t.detect("/v1/models/claude-x", &h));
        assert!(!t.detect("/v1/messages", &h));
    }

    #[test]
    fn request_promotes_x_api_key_and_leaves_path_unchanged() {
        let req = axum::http::Request::builder()
            .uri("/ai/v1/models")
            .header("x-api-key", "sk-test")
            .body(())
            .unwrap();
        let (parts, ()) = req.into_parts();
        let out = AnthropicModels.translate_request(&parts, Bytes::new()).unwrap();
        assert_eq!(out.uri.path(), "/ai/v1/models");
        assert_eq!(out.headers.get(axum::http::header::AUTHORIZATION).unwrap(), "Bearer sk-test");
    }

    #[test]
    fn response_reshapes_openai_list_to_anthropic() {
        let openai = json!({
            "object": "list",
            "data": [
                { "id": "model-a", "created": 1_700_000_000, "object": "model", "owned_by": "None" },
                { "id": "model-b", "created": 1_700_000_500, "object": "model", "owned_by": "None" }
            ]
        });
        let bytes = from_openai_models(Bytes::from(serde_json::to_vec(&openai).unwrap())).unwrap();
        let out: Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(out["has_more"], false);
        assert_eq!(out["first_id"], "model-a");
        assert_eq!(out["last_id"], "model-b");
        let data = out["data"].as_array().unwrap();
        assert_eq!(data.len(), 2);
        assert_eq!(data[0]["type"], "model");
        assert_eq!(data[0]["id"], "model-a");
        assert_eq!(data[0]["display_name"], "model-a");
        assert_eq!(data[0]["created_at"], "2023-11-14T22:13:20+00:00");
        assert!(data[0].get("object").is_none());
        assert!(data[0].get("owned_by").is_none());
    }

    #[test]
    fn response_handles_empty_list() {
        let openai = json!({ "object": "list", "data": [] });
        let bytes = from_openai_models(Bytes::from(serde_json::to_vec(&openai).unwrap())).unwrap();
        let out: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(out["data"].as_array().unwrap().len(), 0);
        assert_eq!(out["has_more"], false);
        assert!(out.get("first_id").is_none());
        assert!(out.get("last_id").is_none());
    }

    #[test]
    fn response_errors_when_data_is_missing_or_not_a_list() {
        for bad in [json!({ "object": "list" }), json!({ "data": "nope" }), json!("garbage")] {
            let err = from_openai_models(Bytes::from(serde_json::to_vec(&bad).unwrap()));
            assert!(err.is_err(), "expected error for {bad}");
        }
    }

    #[test]
    fn response_errors_on_entry_missing_id() {
        let openai = json!({ "object": "list", "data": [ { "id": "ok", "created": 1 }, { "created": 2 } ] });
        let err = from_openai_models(Bytes::from(serde_json::to_vec(&openai).unwrap()));
        assert!(err.is_err());
    }
}
