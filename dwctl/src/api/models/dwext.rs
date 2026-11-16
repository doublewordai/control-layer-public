
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct BatchDwExtResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_file: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_id: Option<String>,
}

impl BatchDwExtResponse {
    pub fn is_empty(&self) -> bool {
        self.source.is_none()
            && self.source_name.is_none()
            && self.source_id.is_none()
            && self.source_file.is_none()
            && self.sync_id.is_none()
    }
}
