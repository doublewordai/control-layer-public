
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::LazyLock;

use jsonschema::{ReferencingError, error::ValidationErrorKind};
use moka::sync::Cache;
use serde_json::Value;

pub const NOT_OBJECT: &str = "not_object";
pub const UNKNOWN_DRAFT: &str = "unknown_draft";
pub const UNRESOLVED_REF: &str = "unresolved_ref";
pub const META_SCHEMA: &str = "meta_schema";
pub const INVALID_PATTERN: &str = "invalid_pattern";

const DATA_KEYWORDS: [&str; 4] = ["const", "default", "enum", "examples"];

const CACHE_CAPACITY: u64 = 10_000;

const REQUEST_BUDGET: usize = 10_000;

const BUILD_COST: usize = 32;

static HASHER: LazyLock<ahash::RandomState> = LazyLock::new(ahash::RandomState::new);

static RESULTS: LazyLock<Cache<u64, Option<&'static str>>> = LazyLock::new(|| Cache::new(CACHE_CAPACITY));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Validity {
    Valid,
    Invalid(&'static str),
    Unchecked,
}

#[derive(Debug)]
pub struct Budget(usize);

impl Budget {
    pub fn per_request() -> Self {
        Self(REQUEST_BUDGET)
    }
}

pub fn check(schema: &Value, budget: &mut Budget) -> Validity {
    let mut hasher = HASHER.build_hasher();
    let cost = BUILD_COST + fingerprint(schema, &mut hasher);
    let key = hasher.finish();
    let result = match RESULTS.get(&key) {
        Some(result) => result,
        None if cost > budget.0 => return Validity::Unchecked,
        None => {
            budget.0 -= cost;
            let result = build(schema);
            RESULTS.insert(key, result);
            result
        }
    };
    result.map_or(Validity::Valid, Validity::Invalid)
}

fn build(schema: &Value) -> Option<&'static str> {
    if !schema.is_object() && !schema.is_boolean() {
        return Some(NOT_OBJECT);
    }
    let mut without_patterns = schema.clone();
    let patterns_valid = strip_patterns(&mut without_patterns);
    if let Err(error) = jsonschema::options().offline().build(&without_patterns) {
        return Some(match error.kind() {
            ValidationErrorKind::Referencing(ReferencingError::UnknownSpecification { .. }) => UNKNOWN_DRAFT,
            ValidationErrorKind::Referencing(_) => UNRESOLVED_REF,
            _ => META_SCHEMA,
        });
    }
    (!patterns_valid).then_some(INVALID_PATTERN)
}

fn strip_patterns(value: &mut Value) -> bool {
    let mut valid = true;
    match value {
        Value::Array(items) => {
            for item in items {
                valid &= strip_patterns(item);
            }
        }
        Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                match (key.as_str(), item) {
                    (keyword, _) if DATA_KEYWORDS.contains(&keyword) => {}
                    ("pattern", Value::String(pattern)) => {
                        valid &= jsonschema_regex::is_valid_ecma_regex(pattern);
                        pattern.clear();
                    }
                    ("patternProperties", Value::Object(patterns)) => {
                        *patterns = std::mem::take(patterns)
                            .into_iter()
                            .enumerate()
                            .map(|(i, (pattern, mut schema))| {
                                valid &= jsonschema_regex::is_valid_ecma_regex(&pattern);
                                valid &= strip_patterns(&mut schema);
                                (format!("^{i}$"), schema)
                            })
                            .collect();
                    }
                    (_, item) => valid &= strip_patterns(item),
                }
            }
        }
        _ => {}
    }
    valid
}

fn fingerprint(value: &Value, hasher: &mut impl Hasher) -> usize {
    let mut cost = 1;
    match value {
        Value::Null => 0u8.hash(hasher),
        Value::Bool(b) => (1u8, b).hash(hasher),
        Value::Number(n) => {
            2u8.hash(hasher);
            if let Some(i) = n.as_i64() {
                (0u8, i).hash(hasher);
            } else if let Some(u) = n.as_u64() {
                (1u8, u).hash(hasher);
            } else {
                (2u8, n.as_f64().map(f64::to_bits)).hash(hasher);
            }
        }
        Value::String(s) => {
            (3u8, s).hash(hasher);
            cost += s.len() / 64;
        }
        Value::Array(items) => {
            (4u8, items.len()).hash(hasher);
            for item in items {
                cost += fingerprint(item, hasher);
            }
        }
        Value::Object(map) => {
            (5u8, map.len()).hash(hasher);
            for (key, item) in map {
                key.hash(hasher);
                cost += key.len() / 64 + fingerprint(item, hasher);
            }
        }
    }
    cost
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn schemas_are_classified_by_bounded_labels() {
        let cases = [
            (
                "valid",
                json!({"type": "object", "properties": {"a": {"type": "string"}}, "required": ["a"]}),
                Ok(()),
            ),
            ("empty", json!({}), Ok(())),
            ("boolean", json!(true), Ok(())),
            (
                "recursive",
                json!({
                    "$defs": {"node": {"type": "object", "properties": {"children": {"type": "array", "items": {"$ref": "#/$defs/node"}}}}},
                    "$ref": "#/$defs/node",
                }),
                Ok(()),
            ),
            (
                "refers to its root",
                json!({"type": "object", "properties": {"next": {"$ref": "#"}}}),
                Ok(()),
            ),
            (
                "draft-07 definitions",
                json!({
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "definitions": {"a": {"type": "string"}},
                    "properties": {"x": {"$ref": "#/definitions/a"}},
                }),
                Ok(()),
            ),
            ("unknown type", json!({"type": "objekt"}), Err(META_SCHEMA)),
            (
                "required not an array",
                json!({"type": "object", "required": "a"}),
                Err(META_SCHEMA),
            ),
            (
                "invalid pattern",
                json!({"type": "string", "pattern": "(unclosed"}),
                Err(INVALID_PATTERN),
            ),
            (
                "invalid patternProperties key",
                json!({"type": "object", "patternProperties": {"(unclosed": {"type": "string"}}}),
                Err(INVALID_PATTERN),
            ),
            (
                "ref to nowhere under patternProperties",
                json!({"type": "object", "patternProperties": {"^a": {"$ref": "#/$defs/missing"}}}),
                Err(UNRESOLVED_REF),
            ),
            (
                "a pattern that compiles to megabytes is valid, and never compiled",
                json!({"type": "string", "pattern": "(?:\\p{L}{100}){100}"}),
                Ok(()),
            ),
            (
                "lookaround is ECMA-262",
                json!({"type": "string", "pattern": "^(?=a)\\w+$"}),
                Ok(()),
            ),
            (
                "a pattern in instance data is not a pattern",
                json!({"type": "object", "default": {"pattern": "**/*.rs"}, "examples": [{"pattern": "("}]}),
                Ok(()),
            ),
            (
                "ref to nowhere",
                json!({"type": "object", "properties": {"a": {"$ref": "#/$defs/missing"}}}),
                Err(UNRESOLVED_REF),
            ),
            (
                "ref to another document",
                json!({"$ref": "https://example.com/schema.json"}),
                Err(UNRESOLVED_REF),
            ),
            (
                "unknown draft",
                json!({"$schema": "https://example.com/meta", "type": "object"}),
                Err(UNKNOWN_DRAFT),
            ),
            ("string", json!("object"), Err(NOT_OBJECT)),
            ("array", json!([{"type": "object"}]), Err(NOT_OBJECT)),
        ];
        for (name, schema, expected) in cases {
            let expected = expected.map_or_else(Validity::Invalid, |()| Validity::Valid);
            let mut budget = Budget::per_request();
            assert_eq!(check(&schema, &mut budget), expected, "{name}");
            let spent = budget.0;
            assert_eq!(check(&schema, &mut budget), expected, "{name}, from the cache");
            assert_eq!(budget.0, spent, "{name}: a cached result costs nothing");
        }
    }

    #[test]
    fn equal_schemas_hash_equal_and_different_ones_differ() {
        let hash = |value: &Value| {
            let mut hasher = HASHER.build_hasher();
            fingerprint(value, &mut hasher);
            hasher.finish()
        };
        let schema = json!({"type": "object", "properties": {"n": {"type": "integer", "minimum": 1}}});
        assert_eq!(hash(&schema), hash(&schema.clone()));
        for other in [
            json!({"type": "object", "properties": {"n": {"type": "integer", "minimum": 2}}}),
            json!({"type": "object", "properties": {"n": {"type": "integer", "minimum": 1.5}}}),
            json!({"type": "object", "properties": {"n": {"type": "integer", "minimum": "1"}}}),
            json!({"type": "object", "properties": {"m": {"type": "integer", "minimum": 1}}}),
            json!({"type": "object", "properties": {"n": {"type": ["integer"], "minimum": 1}}}),
        ] {
            assert_ne!(hash(&schema), hash(&other), "{other}");
        }
    }

    #[test]
    fn uncached_schemas_are_checked_within_the_request_budget() {
        let nonce = uuid::Uuid::new_v4().to_string();
        let small = json!({"type": "object", "title": nonce});
        let properties: serde_json::Map<String, Value> =
            (0..REQUEST_BUDGET).map(|i| (format!("p{i}"), json!({"type": "string"}))).collect();
        let large = json!({"type": "object", "title": nonce, "properties": properties});

        let mut hasher = HASHER.build_hasher();
        assert!(fingerprint(&large, &mut hasher) > REQUEST_BUDGET);
        let mut budget = Budget::per_request();
        assert_eq!(check(&large, &mut budget), Validity::Unchecked);
        assert_eq!(budget.0, REQUEST_BUDGET, "an unchecked schema spends nothing");
        assert_eq!(check(&small, &mut budget), Validity::Valid, "a smaller one still fits");
        assert!(budget.0 < REQUEST_BUDGET);

        let mut spent = Budget(0);
        assert_eq!(check(&small, &mut spent), Validity::Valid);
        assert_eq!(check(&large, &mut spent), Validity::Unchecked, "never checked, so never cached");
    }

    #[test]
    fn long_strings_count_against_the_budget() {
        let mut hasher = HASHER.build_hasher();
        let described = json!({"type": "string", "description": "x".repeat(64 * REQUEST_BUDGET)});
        assert!(fingerprint(&described, &mut hasher) > REQUEST_BUDGET);
    }

    #[test]
    fn a_typical_new_tool_list_fits_the_budget() {
        let nonce = uuid::Uuid::new_v4().to_string();
        let mut budget = Budget::per_request();
        for i in 0..44 {
            let tool = serde_json::json!({"type": "object", "title": format!("{nonce}-{i}"), "properties": {
                "path": {"type": "string", "description": "A path, relative to the workspace root"},
                "offset": {"type": "integer", "minimum": 0},
                "limit": {"type": "integer", "minimum": 1, "maximum": 2000},
                "mode": {"type": "string", "enum": ["read", "write", "append"]},
                "options": {"type": "object", "properties": {"glob": {"type": "string", "pattern": "^[a-z*./]+$"}}, "additionalProperties": false},
                "tags": {"type": "array", "items": {"type": "string"}, "maxItems": 10},
            }, "required": ["path"], "additionalProperties": false});
            assert_eq!(check(&tool, &mut budget), Validity::Valid, "tool {i}");
        }
    }
}
