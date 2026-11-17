
pub mod document;
pub mod feed;
pub mod listing;

#[cfg(test)]
pub(crate) mod schema_check {
    use serde_json::{Value, json};

    use super::document::SCHEMA_VERSION;

    const OPENROUTER_SCHEMA: &str = include_str!("../../tests/fixtures/openrouter/provider-monitor-schema-v2.openapi.json");

    pub(crate) fn assert_valid(feed: &Value) {
        let mut schema: Value = serde_json::from_str(OPENROUTER_SCHEMA).unwrap();
        assert_eq!(
            schema["info"]["version"], SCHEMA_VERSION,
            "the document types follow the vendored schema"
        );
        let root = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$ref": "#/components/schemas/ModelDocumentV2",
            "components": schema["components"].take(),
        });
        let validator = jsonschema::draft202012::options()
            .should_validate_formats(true)
            .build(&root)
            .expect("the vendored OpenRouter schema compiles");
        let documents = feed["data"].as_array().expect("a feed has a data array");
        assert!(!documents.is_empty(), "nothing to validate");
        for document in documents {
            let errors: Vec<String> = validator
                .iter_errors(document)
                .map(|error| format!("{error} at {}", error.instance_path()))
                .collect();
            assert!(
                errors.is_empty(),
                "document {} fails OpenRouter's schema: {errors:#?}",
                document["id"]
            );
        }
    }
}
