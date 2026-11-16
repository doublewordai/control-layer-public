
use serde_json::Value;

#[derive(Debug)]
pub enum DeathEvent<'a> {
    TransportError { finished: bool },
    Eof { saw_finish_reason: bool, saw_done: bool },
    Frame(&'a Value),
    Stall { finished: bool },
    ClientDisconnect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Alive,
    Complete,
    LostTrailer,
    Resume(&'static str),
    NoResume(&'static str),
}

pub fn classify(event: &DeathEvent) -> Verdict {
    match event {
        DeathEvent::TransportError { finished: true } => Verdict::LostTrailer,
        DeathEvent::TransportError { finished: false } => Verdict::Resume("transport_error"),

        DeathEvent::Eof { saw_done: true, .. } => Verdict::Complete,
        DeathEvent::Eof {
            saw_finish_reason: true,
            saw_done: false,
        } => Verdict::LostTrailer,
        DeathEvent::Eof { .. } => Verdict::Resume("truncated"),

        DeathEvent::Frame(frame) => classify_frame(frame),

        DeathEvent::Stall { finished: true } => Verdict::LostTrailer,
        DeathEvent::Stall { finished: false } => Verdict::Resume("stall"),

        DeathEvent::ClientDisconnect => Verdict::NoResume("client_disconnect"),
    }
}

fn classify_frame(frame: &Value) -> Verdict {
    let Some(error) = error_object(frame) else {
        return Verdict::Alive;
    };

    let error_type = error.get("type").and_then(Value::as_str).unwrap_or("");
    match error_type {
        "request_cancelled" => return Verdict::Resume("cancelled_499"),
        "client_disconnected" => return Verdict::NoResume("client_disconnect"),
        _ => {}
    }

    match error_code(error) {
        Some(code) if (400..500).contains(&code) => Verdict::Resume("error_envelope_4xx"),
        _ => Verdict::Resume("error_envelope"),
    }
}

fn error_object(frame: &Value) -> Option<&serde_json::Map<String, Value>> {
    if let Some(err) = frame.get("error").and_then(Value::as_object) {
        return Some(err);
    }
    if frame.get("object").and_then(Value::as_str) == Some("error") {
        return frame.as_object();
    }
    None
}

fn error_code(error: &serde_json::Map<String, Value>) -> Option<i64> {
    match error.get("code") {
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn death_table() {
        let cancelled = json!({"error": {"code": 499, "message": "CancelledError: ", "type": "request_cancelled"}});
        let disconnected = json!({"error": {"code": 499, "message": "client disconnected", "type": "client_disconnected"}});
        let bad_request = json!({"error": {"code": 400, "message": "input too long", "type": "invalid_request_error"}});
        let upstream = json!({"error": {"code": 502, "message": "Provider returned error", "metadata": {"provider_name": "x"}}});
        let content = json!({"choices": [{"delta": {"content": "hi"}}]});

        let cases: Vec<(&str, DeathEvent, Verdict)> = vec![
            (
                "transport error mid-stream",
                DeathEvent::TransportError { finished: false },
                Verdict::Resume("transport_error"),
            ),
            (
                "transport error after the generation finished",
                DeathEvent::TransportError { finished: true },
                Verdict::LostTrailer,
            ),
            (
                "stream ends with no finish_reason and no [DONE]",
                DeathEvent::Eof {
                    saw_finish_reason: false,
                    saw_done: false,
                },
                Verdict::Resume("truncated"),
            ),
            (
                "error envelope inside a 200 stream",
                DeathEvent::Frame(&upstream),
                Verdict::Resume("error_envelope"),
            ),
            (
                "dynamo 499 request_cancelled",
                DeathEvent::Frame(&cancelled),
                Verdict::Resume("cancelled_499"),
            ),
            (
                "4xx-shaped envelope after partial output",
                DeathEvent::Frame(&bad_request),
                Verdict::Resume("error_envelope_4xx"),
            ),
            (
                "499 client_disconnected",
                DeathEvent::Frame(&disconnected),
                Verdict::NoResume("client_disconnect"),
            ),
            (
                "client disconnected",
                DeathEvent::ClientDisconnect,
                Verdict::NoResume("client_disconnect"),
            ),
            (
                "stall past the deadline",
                DeathEvent::Stall { finished: false },
                Verdict::Resume("stall"),
            ),
            (
                "stall while waiting for the trailer of a finished generation",
                DeathEvent::Stall { finished: true },
                Verdict::LostTrailer,
            ),
            ("ordinary content frame", DeathEvent::Frame(&content), Verdict::Alive),
            (
                "finished, then [DONE]",
                DeathEvent::Eof {
                    saw_finish_reason: true,
                    saw_done: true,
                },
                Verdict::Complete,
            ),
            (
                "finished but no [DONE] (lost trailer)",
                DeathEvent::Eof {
                    saw_finish_reason: true,
                    saw_done: false,
                },
                Verdict::LostTrailer,
            ),
        ];

        for (name, event, expected) in cases {
            assert_eq!(classify(&event), expected, "case: {name}");
        }
    }

    #[test]
    fn a_499_is_never_classified_by_its_code_alone() {
        let worker = json!({"error": {"code": 499, "type": "request_cancelled"}});
        let client = json!({"error": {"code": 499, "type": "client_disconnected"}});
        assert_eq!(classify(&DeathEvent::Frame(&worker)), Verdict::Resume("cancelled_499"));
        assert_eq!(classify(&DeathEvent::Frame(&client)), Verdict::NoResume("client_disconnect"));
        let bare = json!({"error": {"code": 499}});
        assert_eq!(classify(&DeathEvent::Frame(&bare)), Verdict::Resume("error_envelope_4xx"));
    }

    #[test]
    fn numeric_string_codes_are_read_as_numbers() {
        let as_string = json!({"error": {"code": "400", "message": "bad"}});
        assert_eq!(classify(&DeathEvent::Frame(&as_string)), Verdict::Resume("error_envelope_4xx"));
    }

    #[test]
    fn shapeless_and_bare_error_envelopes_resume() {
        let shapeless = json!({"error": {"message": "upstream exploded"}});
        assert_eq!(classify(&DeathEvent::Frame(&shapeless)), Verdict::Resume("error_envelope"));
        let typed = json!({"error": {"code": "server_error", "message": "boom"}});
        assert_eq!(classify(&DeathEvent::Frame(&typed)), Verdict::Resume("error_envelope"));
        let bare = json!({"object": "error", "message": "boom", "code": 503});
        assert_eq!(classify(&DeathEvent::Frame(&bare)), Verdict::Resume("error_envelope"));
    }

    #[test]
    fn a_content_frame_that_merely_mentions_error_is_not_an_envelope() {
        let text = json!({"choices": [{"delta": {"content": "error: not really"}}], "error": null});
        assert_eq!(classify(&DeathEvent::Frame(&text)), Verdict::Alive);
    }
}
