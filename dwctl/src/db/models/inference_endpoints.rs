
use crate::reasoning::ReasoningTranslationConfig;
use crate::types::{InferenceEndpointId, UserId};
use chrono::{DateTime, Utc};
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EndpointKind {
    Dynamo,
    Hosted,
    #[default]
    External,
}

impl EndpointKind {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Dynamo => "dynamo",
            Self::Hosted => "hosted",
            Self::External => "external",
        }
    }

    pub fn from_db_str(value: &str) -> Option<Self> {
        match value {
            "dynamo" => Some(Self::Dynamo),
            "hosted" => Some(Self::Hosted),
            "external" => Some(Self::External),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InferenceEndpointCreateDBRequest {
    pub created_by: UserId,
    pub name: String,
    pub description: Option<String>,
    pub url: Url,
    pub api_key: Option<String>,
    pub model_filter: Option<Vec<String>>,
    pub auth_header_name: Option<String>,
    pub auth_header_prefix: Option<String>,
    pub reasoning_translation: Option<ReasoningTranslationConfig>,
    pub accepts_scheduling_priority: bool,
    pub kind: EndpointKind,
}

#[derive(Debug, Clone, Default)]
pub struct InferenceEndpointUpdateDBRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub url: Option<Url>,
    pub api_key: Option<Option<String>>,
    pub model_filter: Option<Option<Vec<String>>>,
    pub auth_header_name: Option<String>,
    pub auth_header_prefix: Option<String>,
    pub reasoning_translation: Option<Option<ReasoningTranslationConfig>>,
    pub accepts_scheduling_priority: Option<bool>,
    pub kind: Option<EndpointKind>,
}

#[derive(Debug, Clone)]
pub struct InferenceEndpointDBResponse {
    pub id: InferenceEndpointId,
    pub name: String,
    pub description: Option<String>,
    pub url: Url,
    pub api_key: Option<String>,
    pub model_filter: Option<Vec<String>>,
    pub auth_header_name: String,
    pub auth_header_prefix: String,
    pub reasoning_translation: Option<ReasoningTranslationConfig>,
    pub accepts_scheduling_priority: bool,
    pub kind: EndpointKind,
    pub created_by: UserId,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
