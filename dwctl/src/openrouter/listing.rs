
use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::document::{MAX_SAFE_INTEGER, MediaInput, MediaSource, ParameterDescriptor};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OpenRouterListing {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hugging_face_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
    #[serde(default)]
    pub is_ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 9_007_199_254_740_991u64))]
    pub max_output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>", extend("format" = "date"))]
    pub deprecation_date: Option<NaiveDate>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub media_inputs: BTreeMap<MediaInput, Vec<MediaSource>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub supported_parameters: BTreeMap<String, Option<ParameterDescriptor>>,
}

impl OpenRouterListing {
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [("hugging_face_id", &self.hugging_face_id), ("slug", &self.slug)] {
            if value.as_deref().is_some_and(|value| value.trim().is_empty()) {
                return Err(format!("{field} must not be empty"));
            }
        }
        match self.max_output_tokens {
            Some(0) => return Err("max_output_tokens must be positive".to_string()),
            Some(tokens) if tokens > MAX_SAFE_INTEGER => {
                return Err(format!("max_output_tokens must be at most {MAX_SAFE_INTEGER}"));
            }
            _ => {}
        }
        for (kind, sources) in &self.media_inputs {
            if sources.iter().collect::<BTreeSet<_>>().len() != sources.len() {
                return Err(format!("media_inputs.{kind:?} lists a source twice").to_lowercase());
            }
        }
        for (name, descriptor) in &self.supported_parameters {
            if name.trim().is_empty() {
                return Err("supported_parameters has an empty parameter name".to_string());
            }
            match descriptor {
                Some(ParameterDescriptor::Range {
                    min: Some(min),
                    max: Some(max),
                    ..
                }) if min > max => return Err(format!("supported_parameters.{name}: min is above max")),
                Some(ParameterDescriptor::Integer {
                    min: Some(min),
                    max: Some(max),
                    ..
                }) if min > max => return Err(format!("supported_parameters.{name}: min is above max")),
                Some(ParameterDescriptor::Array { max_items: Some(0) }) => {
                    return Err(format!("supported_parameters.{name}: max_items must be positive"));
                }
                Some(ParameterDescriptor::Array { max_items: Some(items) }) if *items > MAX_SAFE_INTEGER => {
                    return Err(format!("supported_parameters.{name}: max_items must be at most {MAX_SAFE_INTEGER}"));
                }
                _ => {}
            }
        }
        Ok(())
    }
}
