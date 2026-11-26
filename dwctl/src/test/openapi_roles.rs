
use std::collections::BTreeSet;

use serde_json::Value;
use utoipa::OpenApi;

use crate::{api::models::users::Role, openapi::AdminApiDoc};

fn admin_spec() -> Value {
    serde_json::to_value(AdminApiDoc::openapi()).expect("admin spec serializes")
}

fn enum_names(spec: &Value, schema: &Value) -> BTreeSet<String> {
    let schema = match schema.get("$ref").and_then(Value::as_str) {
        Some(reference) => {
            let name = reference
                .strip_prefix("#/components/schemas/")
                .unwrap_or_else(|| panic!("unexpected schema reference {reference}"));
            &spec["components"]["schemas"][name]
        }
        None => schema,
    };

    let mut values: Vec<&Value> = Vec::new();
    values.extend(schema.get("enum").and_then(Value::as_array).into_iter().flatten());
    for variant in schema.get("oneOf").and_then(Value::as_array).into_iter().flatten() {
        values.extend(variant.get("enum").and_then(Value::as_array).into_iter().flatten());
    }

    values
        .into_iter()
        .map(|value| {
            value
                .as_str()
                .unwrap_or_else(|| panic!("enum value is not a string: {value}"))
                .to_string()
        })
        .collect()
}

fn roles_values(spec: &Value, schema: &str) -> BTreeSet<String> {
    let roles = &spec["components"]["schemas"][schema]["properties"]["roles"];
    let is_array = roles["type"] == "array"
        || roles["type"]
            .as_array()
            .is_some_and(|types| types.iter().any(|kind| kind == "array"));
    assert!(is_array, "{schema}.roles should be documented as an array: {roles}");
    enum_names(spec, &roles["items"])
}

#[test]
fn user_request_schemas_list_only_assignable_roles() {
    let spec = admin_spec();
    let assignable: BTreeSet<String> = [
        "PlatformManager",
        "RequestViewer",
        "StandardUser",
        "BillingManager",
        "BatchAPIUser",
        "BackgroundInferenceUser",
        "ConnectionsUser",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();

    for request in ["UserCreate", "UserUpdate"] {
        assert_eq!(
            roles_values(&spec, request),
            assignable,
            "{request}.roles should offer exactly the assignable roles"
        );
    }
}

#[test]
fn user_response_schemas_keep_every_role() {
    let spec = admin_spec();
    let all: BTreeSet<String> = enum_names(&spec, &spec["components"]["schemas"]["Role"]);
    assert_eq!(all.len(), 11, "Role schema should describe eleven roles: {all:?}");

    for response in ["UserResponse", "CurrentUser"] {
        assert_eq!(roles_values(&spec, response), all, "{response}.roles should still offer every role");
    }
}

#[test]
fn assignable_role_schema_matches_the_role_service_split() {
    let spec = admin_spec();
    let all = enum_names(&spec, &spec["components"]["schemas"]["Role"]);
    let assignable = enum_names(&spec, &spec["components"]["schemas"]["AssignableRole"]);

    for name in &all {
        let role: Role = serde_json::from_value(Value::String(name.clone()))
            .unwrap_or_else(|error| panic!("Role schema value {name} does not deserialize to a Role: {error}"));
        assert_eq!(
            !role.is_service_role(),
            assignable.contains(name),
            "{name}: `Role::is_service_role` and the AssignableRole schema disagree"
        );
    }

    assert!(
        assignable.is_subset(&all),
        "AssignableRole offers names Role does not define: {:?}",
        assignable.difference(&all).collect::<Vec<_>>()
    );
    assert_eq!(assignable.len(), 7, "AssignableRole should offer seven roles: {assignable:?}");
}
