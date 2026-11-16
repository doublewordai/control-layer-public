
use super::pagination::Pagination;
use crate::db::models::inference_endpoints::{EndpointKind, InferenceEndpointDBResponse};
use crate::reasoning::ReasoningTranslationConfig;
use crate::types::{InferenceEndpointId, UserId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::rust::double_option;
use std::collections::HashMap;
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OpenAIModel {
    pub id: String,
    pub object: String,
    pub created: Option<i64>,
    pub owned_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OpenAIModelsResponse {
    pub object: String,
    pub data: Vec<OpenAIModel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AnthropicModel {
    pub created_at: String,
    pub display_name: String,
    pub id: String,
    pub r#type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AnthropicModelsResponse {
    pub data: Vec<AnthropicModel>,
    pub first_id: String,
    pub has_more: bool,
    pub last_id: String,
}

impl From<AnthropicModelsResponse> for OpenAIModelsResponse {
    fn from(anthropic: AnthropicModelsResponse) -> Self {
        let data = anthropic
            .data
            .into_iter()
            .map(|model| OpenAIModel {
                id: model.id,
                object: "model".to_string(),
                created: Some(0),
                owned_by: "anthropic".to_string(),
            })
            .collect();
        Self {
            object: "list".to_string(),
            data,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OpenRouterModel {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub created: Option<i64>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OpenRouterModelsResponse {
    pub data: Vec<OpenRouterModel>,
}

impl From<OpenRouterModelsResponse> for OpenAIModelsResponse {
    fn from(openrouter: OpenRouterModelsResponse) -> Self {
        let data = openrouter
            .data
            .into_iter()
            .map(|model| OpenAIModel {
                id: model.id,
                object: "model".to_string(),
                created: model.created,
                owned_by: "openrouter".to_string(),
            })
            .collect();
        Self {
            object: "list".to_string(),
            data,
        }
    }
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ListEndpointsQuery {
    #[serde(flatten)]
    #[param(inline)]
    pub pagination: Pagination,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InferenceEndpointCreate {
    pub name: String,
    pub description: Option<String>,
    pub url: String,
    pub api_key: Option<String>,
    pub model_filter: Option<Vec<String>>,
    #[serde(default)]
    pub alias_mapping: Option<HashMap<String, String>>,
    pub auth_header_name: Option<String>,
    pub auth_header_prefix: Option<String>,
    #[serde(default = "default_sync")]
    pub sync: bool,
    #[serde(default)]
    pub skip_fetch: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_translation: Option<ReasoningTranslationConfig>,
    #[serde(default)]
    pub accepts_scheduling_priority: bool,
    #[serde(default)]
    pub kind: Option<EndpointKind>,
}

impl InferenceEndpointCreate {
    pub fn resolved_kind(&self) -> EndpointKind {
        self.kind.unwrap_or(if self.accepts_scheduling_priority {
            EndpointKind::Dynamo
        } else {
            EndpointKind::External
        })
    }
}

fn default_sync() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InferenceEndpointUpdate {
    pub name: Option<String>,
    pub description: Option<String>,
    pub url: Option<String>,
    pub api_key: Option<Option<String>>,
    pub model_filter: Option<Option<Vec<String>>>,
    #[serde(default)]
    pub alias_mapping: Option<HashMap<String, String>>,
    pub auth_header_name: Option<String>,
    pub auth_header_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub reasoning_translation: Option<Option<ReasoningTranslationConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepts_scheduling_priority: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<EndpointKind>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InferenceEndpointValidate {
    New {
        url: String,
        api_key: Option<String>,
        auth_header_name: Option<String>,
        auth_header_prefix: Option<String>,
    },
    Existing {
        #[schema(value_type = String, format = "uuid")]
        endpoint_id: InferenceEndpointId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InferenceEndpointValidateResponse {
    pub status: String,
    pub models: Option<OpenAIModelsResponse>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InferenceEndpointResponse {
    #[schema(value_type = String, format = "uuid")]
    pub id: InferenceEndpointId,
    pub name: String,
    pub description: Option<String>,
    pub url: String,
    pub model_filter: Option<Vec<String>>,
    pub requires_api_key: bool,
    pub auth_header_name: String,
    pub auth_header_prefix: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_translation: Option<ReasoningTranslationConfig>,
    pub accepts_scheduling_priority: bool,
    pub kind: EndpointKind,
    #[schema(value_type = String, format = "uuid")]
    pub created_by: UserId,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<InferenceEndpointDBResponse> for InferenceEndpointResponse {
    fn from(db: InferenceEndpointDBResponse) -> Self {
        Self {
            id: db.id,
            name: db.name,
            description: db.description,
            url: db.url.to_string(),
            model_filter: db.model_filter,
            requires_api_key: db.api_key.is_some() && !db.api_key.as_ref().unwrap().is_empty(),
            auth_header_name: db.auth_header_name,
            auth_header_prefix: db.auth_header_prefix,
            reasoning_translation: db.reasoning_translation,
            accepts_scheduling_priority: db.accepts_scheduling_priority,
            kind: db.kind,
            created_by: db.created_by,
            created_at: db.created_at,
            updated_at: db.updated_at,
        }
    }
}
