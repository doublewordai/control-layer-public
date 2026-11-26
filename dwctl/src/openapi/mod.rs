
use serde_json::{Map, Value};
use utoipa::{OpenApi, openapi::OpenApi as OpenApiDocument};

pub mod admin;
pub mod ai;
mod extra_types;

pub use admin::AdminApiDoc;
pub use ai::AiApiDoc;

pub fn canonical_json(document: OpenApiDocument) -> String {
    let value = serde_json::to_value(document).expect("an OpenAPI document is serializable");
    let mut output = serde_json::to_string_pretty(&sort_keys(value)).expect("JSON is serializable");
    output.push('\n');
    output
}

pub fn admin_json() -> String {
    canonical_json(AdminApiDoc::openapi())
}

pub fn ai_json() -> String {
    canonical_json(AiApiDoc::openapi())
}

fn sort_keys(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = map.into_iter().collect();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, sort_keys(value)))
                    .collect::<Map<String, Value>>(),
            )
        }
        Value::Array(items) => Value::Array(items.into_iter().map(sort_keys).collect()),
        scalar => scalar,
    }
}
