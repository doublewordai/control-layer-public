
use axum::http::Uri;
use axum::http::uri::PathAndQuery;
use serde_json::{Value, json};

pub const CACHE_BREAKPOINT_PARAM: &str = "cacheBreakpoint";

const LAST_USER_MESSAGE: &str = "lastUserMessage";

fn last_user_message_marker() -> Value {
    json!({"type": "ephemeral", "ttl": "1h"})
}

#[derive(Debug, PartialEq, Eq)]
pub struct InvalidBreakpointValue(pub String);

pub fn breakpoint_marker(query: Option<&str>) -> Result<Option<Value>, InvalidBreakpointValue> {
    let Some(query) = query else { return Ok(None) };
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key != CACHE_BREAKPOINT_PARAM {
            continue;
        }
        return match value {
            LAST_USER_MESSAGE => Ok(Some(last_user_message_marker())),
            other => Err(InvalidBreakpointValue(other.to_string())),
        };
    }
    Ok(None)
}

#[derive(Debug, PartialEq, Eq)]
pub enum Inject {
    Applied,
    BodyFieldWins,
    NotAnObject,
}

pub fn inject_marker(body: &mut Value, marker: Value) -> Inject {
    let Some(obj) = body.as_object_mut() else {
        return Inject::NotAnObject;
    };
    match obj.get("cache_control") {
        Some(cc) if !cc.is_null() => Inject::BodyFieldWins,
        _ => {
            obj.insert("cache_control".into(), marker);
            Inject::Applied
        }
    }
}

pub fn strip_param(uri: &Uri) -> Uri {
    fn param_key(pair: &str) -> &str {
        pair.split_once('=').map(|(k, _)| k).unwrap_or(pair)
    }
    let Some(query) = uri.query() else { return uri.clone() };
    if !query.split('&').any(|p| param_key(p) == CACHE_BREAKPOINT_PARAM) {
        return uri.clone();
    }
    let kept: Vec<&str> = query.split('&').filter(|p| param_key(p) != CACHE_BREAKPOINT_PARAM).collect();
    let path_and_query = if kept.is_empty() {
        uri.path().to_string()
    } else {
        format!("{}?{}", uri.path(), kept.join("&"))
    };
    let mut parts = uri.clone().into_parts();
    match path_and_query.parse::<PathAndQuery>() {
        Ok(pq) => parts.path_and_query = Some(pq),
        Err(_) => return uri.clone(),
    }
    Uri::from_parts(parts).unwrap_or_else(|_| uri.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_param_is_none() {
        assert_eq!(breakpoint_marker(None), Ok(None));
        assert_eq!(breakpoint_marker(Some("")), Ok(None));
        assert_eq!(breakpoint_marker(Some("foo=bar&stream=true")), Ok(None));
    }

    #[test]
    fn supported_value_yields_1h_marker() {
        let marker = breakpoint_marker(Some("cacheBreakpoint=lastUserMessage")).unwrap().unwrap();
        assert_eq!(marker, json!({"type": "ephemeral", "ttl": "1h"}));
        let marker = breakpoint_marker(Some("a=b&cacheBreakpoint=lastUserMessage&c=d")).unwrap().unwrap();
        assert_eq!(marker, json!({"type": "ephemeral", "ttl": "1h"}));
    }

    #[test]
    fn unknown_or_empty_value_is_invalid() {
        assert_eq!(
            breakpoint_marker(Some("cacheBreakpoint=lastusermessage")),
            Err(InvalidBreakpointValue("lastusermessage".into()))
        );
        assert_eq!(breakpoint_marker(Some("cacheBreakpoint=")), Err(InvalidBreakpointValue("".into())));
        assert_eq!(breakpoint_marker(Some("cacheBreakpoint")), Err(InvalidBreakpointValue("".into())));
    }

    #[test]
    fn first_occurrence_wins_and_name_is_case_sensitive() {
        assert_eq!(
            breakpoint_marker(Some("cacheBreakpoint=bogus&cacheBreakpoint=lastUserMessage")),
            Err(InvalidBreakpointValue("bogus".into()))
        );
        assert_eq!(breakpoint_marker(Some("cachebreakpoint=lastUserMessage")), Ok(None));
    }

    #[test]
    fn inject_applies_and_explicit_body_field_wins() {
        let mut body = json!({"model": "m", "messages": []});
        assert_eq!(inject_marker(&mut body, last_user_message_marker()), Inject::Applied);
        assert_eq!(body["cache_control"], json!({"type": "ephemeral", "ttl": "1h"}));

        let mut body = json!({"model": "m", "cache_control": {"type": "ephemeral", "ttl": "5m"}});
        assert_eq!(inject_marker(&mut body, last_user_message_marker()), Inject::BodyFieldWins);
        assert_eq!(body["cache_control"]["ttl"], "5m");

        let mut body = json!({"model": "m", "cache_control": null});
        assert_eq!(inject_marker(&mut body, last_user_message_marker()), Inject::Applied);
        assert_eq!(body["cache_control"]["ttl"], "1h");

        let mut body = json!(["not", "an", "object"]);
        assert_eq!(inject_marker(&mut body, last_user_message_marker()), Inject::NotAnObject);
    }

    #[test]
    fn strip_removes_param_and_preserves_others() {
        let uri: Uri = "/v1/chat/completions?foo=bar&cacheBreakpoint=lastUserMessage&baz=1"
            .parse()
            .unwrap();
        assert_eq!(strip_param(&uri).to_string(), "/v1/chat/completions?foo=bar&baz=1");

        let uri: Uri = "/v1/chat/completions?cacheBreakpoint=lastUserMessage".parse().unwrap();
        assert_eq!(strip_param(&uri).to_string(), "/v1/chat/completions");

        let uri: Uri = "/v1/chat/completions?cacheBreakpoint=a&x=y&cacheBreakpoint=b".parse().unwrap();
        assert_eq!(strip_param(&uri).to_string(), "/v1/chat/completions?x=y");

        let uri: Uri = "/v1/chat/completions?foo=bar".parse().unwrap();
        assert_eq!(strip_param(&uri).to_string(), "/v1/chat/completions?foo=bar");
        let uri: Uri = "/v1/chat/completions".parse().unwrap();
        assert_eq!(strip_param(&uri).to_string(), "/v1/chat/completions");
    }
}
