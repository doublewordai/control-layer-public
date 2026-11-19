
use bytes::BytesMut;
use serde_json::Value;

const MAX_PENDING_EVENT: usize = 64 * 1024;

pub struct FirstTokenDetector {
    armed: bool,
    pending: BytesMut,
}

impl Default for FirstTokenDetector {
    fn default() -> Self {
        Self {
            armed: true,
            pending: BytesMut::new(),
        }
    }
}

fn event_end(buf: &[u8]) -> Option<usize> {
    let lf = buf.windows(2).position(|w| w == b"\n\n").map(|i| i + 2);
    let crlf = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4);
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

enum Verdict {
    Output,
    Error,
    Neither,
}

fn judge(event: &[u8]) -> Verdict {
    let Ok(text) = std::str::from_utf8(event) else {
        return Verdict::Neither;
    };
    let data: Vec<&str> = text
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|d| d.strip_prefix(' ').unwrap_or(d))
        .collect();
    let Ok(payload) = serde_json::from_str::<Value>(&data.join("\n")) else {
        return Verdict::Neither;
    };
    if payload.get("error").is_some_and(|e| !e.is_null()) {
        Verdict::Error
    } else if payload
        .get("choices")
        .and_then(Value::as_array)
        .is_some_and(|choices| choices.iter().any(carries_output))
    {
        Verdict::Output
    } else {
        Verdict::Neither
    }
}

fn carries_output(choice: &Value) -> bool {
    let non_empty = |v: Option<&Value>| match v {
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(a)) => !a.is_empty(),
        Some(Value::Object(o)) => !o.is_empty(),
        _ => false,
    };
    if non_empty(choice.get("text")) {
        return true;
    }
    let Some(delta) = choice.get("delta") else {
        return false;
    };
    [
        "content",
        "reasoning_content",
        "reasoning",
        "refusal",
        "tool_calls",
        "function_call",
    ]
    .iter()
    .any(|key| non_empty(delta.get(*key)))
        || delta
            .get("reasoning_details")
            .and_then(Value::as_array)
            .is_some_and(|details| {
                details
                    .iter()
                    .any(|d| ["text", "summary", "data"].iter().any(|key| non_empty(d.get(*key))))
            })
}

impl FirstTokenDetector {
    pub fn observe(&mut self, data: &[u8]) -> bool {
        if !self.armed {
            return false;
        }
        self.pending.extend_from_slice(data);
        let mut fired = false;
        while self.armed
            && let Some(end) = event_end(&self.pending)
        {
            let event = self.pending.split_to(end);
            match judge(&event) {
                Verdict::Output => {
                    fired = true;
                    self.armed = false;
                }
                Verdict::Error => self.armed = false,
                Verdict::Neither => {}
            }
        }
        if !self.armed || self.pending.len() > MAX_PENDING_EVENT {
            self.armed = false;
            self.pending = BytesMut::new();
        }
        fired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fired(frames: &[&str]) -> usize {
        let mut d = FirstTokenDetector::default();
        frames.iter().filter(|f| d.observe(f.as_bytes())).count()
    }

    #[test]
    fn fires_once_on_the_first_chunk_with_output() {
        let frames = [
            ": keep-alive\n\n",
            "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n",
            "data: [DONE]\n\n",
        ];
        let mut d = FirstTokenDetector::default();
        let fired_at: Vec<bool> = frames.iter().map(|f| d.observe(f.as_bytes())).collect();
        assert_eq!(
            fired_at,
            vec![false, false, true, false],
            "the role-only opening delta is not output"
        );
    }

    #[test]
    fn an_error_before_any_output_disarms() {
        assert_eq!(
            fired(&[
                "data: {\"error\":{\"message\":\"overloaded\"}}\n\n",
                "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"
            ]),
            0
        );
    }

    #[test]
    fn content_that_reads_error_is_still_output() {
        assert_eq!(fired(&["data: {\"choices\":[{\"delta\":{\"content\":\"error\"}}]}\n\n"]), 1);
        assert_eq!(
            fired(&["data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}],\"error\":null}\n\n"]),
            1,
            "a null error is no error"
        );
    }

    #[test]
    fn a_role_only_opening_then_an_error_writes_nothing() {
        assert_eq!(
            fired(&[
                "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n",
                "data: {\"error\":{\"message\":\"overloaded\"}}\n\n"
            ]),
            0
        );
        for output in [
            r#"{"choices":[{"delta":{"reasoning_content":"thinking"}}]}"#,
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"f"}}]}}]}"#,
            r#"{"choices":[{"text":"legacy"}]}"#,
            r#"{"choices":[{"delta":{"reasoning_details":[{"type":"reasoning.text","text":"step"}]}}]}"#,
        ] {
            assert_eq!(fired(&[&format!("data: {output}\n\n")]), 1, "{output}");
        }
    }

    #[test]
    fn empty_reasoning_details_are_not_output() {
        assert_eq!(
            fired(&[
                "data: {\"choices\":[{\"delta\":{\"reasoning_details\":[{\"type\":\"reasoning.text\",\"text\":\"\"}]}}]}\n\n",
                "data: {\"error\":{\"message\":\"cut\"}}\n\n"
            ]),
            0
        );
    }

    #[test]
    fn usage_only_events_are_not_output() {
        assert_eq!(
            fired(&[
                "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1}}\n\n",
                "data: {\"error\":{\"message\":\"cut\"}}\n\n"
            ]),
            0,
            "a usage frame before the error is not output, so nothing is written"
        );
    }

    #[test]
    fn an_error_event_with_empty_choices_is_not_output() {
        assert_eq!(
            fired(&[
                "data: {\"choices\":[],\"error\":{\"message\":\"overloaded\"}}\n\n",
                "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"
            ]),
            0
        );
        assert_eq!(
            fired(&["data: {\"choices\":[{\"delta\":{\"content\":\"the \\\"error\\\" key\"}}]}\n\n"]),
            1,
            "an escaped \"error\" inside content is not an error event"
        );
    }

    #[test]
    fn an_error_after_output_does_not_undo_it() {
        assert_eq!(
            fired(&[
                "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n",
                "data: {\"error\":{\"message\":\"cut\"}}\n\n"
            ]),
            1
        );
    }

    #[test]
    fn an_event_split_across_frames_still_fires() {
        let mut d = FirstTokenDetector::default();
        assert!(!d.observe(b"data: {\"cho"));
        assert!(!d.observe(b"ices\":[{\"delta\":{\"content\":\"x\"}}]}"));
        assert!(d.observe(b"\r\n\r\n"), "fires on the frame that completes the event");
    }

    #[test]
    fn an_error_coalesced_ahead_of_output_disarms() {
        assert_eq!(
            fired(&["data: {\"error\":{\"message\":\"overloaded\"}}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"]),
            0
        );
    }

    #[test]
    fn an_unterminated_oversized_event_gives_up() {
        let big = format!("data: {{\"pad\":\"{}\"", "x".repeat(MAX_PENDING_EVENT));
        assert_eq!(
            fired(&[&big, ",\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"]),
            0,
            "left to the completion write"
        );
    }
}
