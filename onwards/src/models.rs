use std::borrow::Cow;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ExtractedModel<'a> {
    #[serde(borrow)]
    pub(crate) model: Cow<'a, str>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct Model {
    pub(crate) id: String,
    pub(crate) created: u32,
    pub(crate) object: String,
    pub(crate) owned_by: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct ListModelResponse {
    pub object: String,
    pub data: Vec<Model>,
}

impl ListModelResponse {
    pub(crate) fn from_model_names(model_names: &[String]) -> Self {
        let data = model_names
            .iter()
            .map(|name| Model {
                id: name.clone(),
                created: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as u32,
                object: "model".into(),
                owned_by: "None".into(),
            })
            .collect::<Vec<_>>();
        ListModelResponse {
            object: "list".into(),
            data,
        }
    }
}
