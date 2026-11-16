use serde_json::{Map, Value};

pub(crate) fn ensure_field(
    object: &mut Map<String, Value>,
    key: &str,
    default: impl FnOnce() -> Value,
) {
    if !object.contains_key(key) {
        object.insert(key.to_string(), default());
    }
}
