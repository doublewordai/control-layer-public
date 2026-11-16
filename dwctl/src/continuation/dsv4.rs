
use std::fmt;
use std::io;

use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::ser::Formatter;
use serde_json::{Number, Value};

use super::accumulate::{AccumulateError, StreamAccumulator, capture_envelope, single_choice};
use super::rewrap::Envelope;

const TOOL_CALLS_OPEN: &str = "<｜DSML｜tool_calls>";
const TOOL_CALLS_CLOSE: &str = "</｜DSML｜tool_calls>";
const INVOKE_OPEN: &str = "<｜DSML｜invoke name=\"";
const INVOKE_CLOSE: &str = "</｜DSML｜invoke>";
const PARAMETER_OPEN: &str = "<｜DSML｜parameter name=\"";
const PARAMETER_CLOSE: &str = "</｜DSML｜parameter>";
const THINK_END: &str = "</think>";
const TOOL_BLOCK_SEPARATOR: &str = "\n\n";

const CLOSING_TAGS: [&str; 2] = [PARAMETER_CLOSE, INVOKE_CLOSE];


#[derive(Debug, PartialEq)]
enum Ordered {
    Null,
    Bool(bool),
    Number(Number),
    Str(String),
    Array(Vec<Ordered>),
    Object(Vec<(String, Ordered)>),
}

impl Serialize for Ordered {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        match self {
            Ordered::Null => ser.serialize_unit(),
            Ordered::Bool(b) => ser.serialize_bool(*b),
            Ordered::Number(n) => n.serialize(ser),
            Ordered::Str(s) => ser.serialize_str(s),
            Ordered::Array(items) => ser.collect_seq(items),
            Ordered::Object(pairs) => {
                let mut map = ser.serialize_map(Some(pairs.len()))?;
                for (k, v) in pairs {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Ordered {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        struct OrderedVisitor;

        impl<'de> Visitor<'de> for OrderedVisitor {
            type Value = Ordered;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("any JSON value")
            }

            fn visit_unit<E>(self) -> Result<Ordered, E> {
                Ok(Ordered::Null)
            }

            fn visit_none<E>(self) -> Result<Ordered, E> {
                Ok(Ordered::Null)
            }

            fn visit_bool<E>(self, v: bool) -> Result<Ordered, E> {
                Ok(Ordered::Bool(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Ordered, E> {
                Ok(Ordered::Number(v.into()))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Ordered, E> {
                Ok(Ordered::Number(v.into()))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Ordered, E> {
                Ok(Number::from_f64(v).map_or(Ordered::Null, Ordered::Number))
            }

            fn visit_str<E>(self, v: &str) -> Result<Ordered, E> {
                Ok(Ordered::Str(v.to_string()))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Ordered, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(Ordered::Array(items))
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Ordered, A::Error> {
                let mut pairs = Vec::new();
                while let Some((k, v)) = map.next_entry()? {
                    pairs.push((k, v));
                }
                Ok(Ordered::Object(pairs))
            }
        }

        de.deserialize_any(OrderedVisitor)
    }
}

struct PyFormatter;

impl Formatter for PyFormatter {
    fn begin_array_value<W: ?Sized + io::Write>(&mut self, w: &mut W, first: bool) -> io::Result<()> {
        if first { Ok(()) } else { w.write_all(b", ") }
    }

    fn begin_object_key<W: ?Sized + io::Write>(&mut self, w: &mut W, first: bool) -> io::Result<()> {
        if first { Ok(()) } else { w.write_all(b", ") }
    }

    fn begin_object_value<W: ?Sized + io::Write>(&mut self, w: &mut W) -> io::Result<()> {
        w.write_all(b": ")
    }
}

fn py_json(value: &Ordered) -> String {
    let mut buf = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut buf, PyFormatter);
    if value.serialize(&mut ser).is_err() {
        return String::new();
    }
    String::from_utf8(buf).unwrap_or_default()
}


#[derive(Debug, PartialEq)]
enum Tail {
    Key(String),
    KeyDone(String),
    Value { key: String, is_string: bool, frag: String },
}

fn char_len(s: &str, i: usize) -> usize {
    s[i..].chars().next().map(char::len_utf8).unwrap_or(1)
}

fn skip_ws(b: &[u8], i: &mut usize) {
    while *i < b.len() && matches!(b[*i], b' ' | b'\t' | b'\r' | b'\n') {
        *i += 1;
    }
}

fn decode_fragment(frag: &str) -> String {
    serde_json::from_str::<String>(&format!("\"{frag}\"")).unwrap_or_else(|_| frag.to_string())
}

fn scan_string(s: &str, start: usize) -> Option<usize> {
    let b = s.as_bytes();
    let mut j = start + 1;
    while j < b.len() {
        if b[j] == b'\\' && j + 1 < b.len() {
            j += 1 + char_len(s, j + 1);
            continue;
        }
        if b[j] == b'"' {
            return Some(j);
        }
        j += char_len(s, j);
    }
    None
}

fn parse_partial_args(s: &str) -> (Vec<(String, Ordered)>, Option<Tail>) {
    let b = s.as_bytes();
    let n = b.len();
    let mut pairs: Vec<(String, Ordered)> = Vec::new();
    let mut i = 0usize;

    skip_ws(b, &mut i);
    if i < n && b[i] == b'{' {
        i += 1;
    } else {
        return (pairs, None);
    }

    loop {
        skip_ws(b, &mut i);
        if i >= n || b[i] == b'}' {
            return (pairs, None);
        }
        if b[i] == b',' {
            i += 1;
            skip_ws(b, &mut i);
        }
        if i >= n || b[i] != b'"' {
            return (pairs, None);
        }

        let key_start = i;
        let Some(j) = scan_string(s, key_start) else {
            return (pairs, Some(Tail::Key(decode_fragment(&s[key_start + 1..]))));
        };
        let Ok(key) = serde_json::from_str::<String>(&s[key_start..=j]) else {
            return (pairs, Some(Tail::Key(s[key_start + 1..j].to_string())));
        };
        i = j + 1;

        skip_ws(b, &mut i);
        if i >= n || b[i] != b':' {
            return (pairs, Some(Tail::KeyDone(key)));
        }
        i += 1;
        skip_ws(b, &mut i);
        if i >= n {
            return (pairs, Some(Tail::KeyDone(key)));
        }

        let val_start = i;
        if b[i] == b'"' {
            let Some(j) = scan_string(s, val_start) else {
                let frag = decode_fragment(&s[val_start + 1..]);
                return (
                    pairs,
                    Some(Tail::Value {
                        key,
                        is_string: true,
                        frag,
                    }),
                );
            };
            let Ok(value) = serde_json::from_str::<Ordered>(&s[val_start..=j]) else {
                let frag = decode_fragment(&s[val_start + 1..j]);
                return (
                    pairs,
                    Some(Tail::Value {
                        key,
                        is_string: true,
                        frag,
                    }),
                );
            };
            pairs.push((key, value));
            i = j + 1;
        } else {
            let (mut depth, mut j, mut in_str, mut esc) = (0i32, i, false, false);
            while j < n {
                let c = b[j];
                if in_str {
                    if esc {
                        esc = false;
                    } else if c == b'\\' {
                        esc = true;
                    } else if c == b'"' {
                        in_str = false;
                    }
                } else if c == b'"' {
                    in_str = true;
                } else if c == b'[' || c == b'{' {
                    depth += 1;
                } else if c == b']' || c == b'}' {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                } else if c == b',' && depth == 0 {
                    break;
                }
                j += char_len(s, j);
            }
            let frag = s[val_start..j].trim();
            let Ok(value) = serde_json::from_str::<Ordered>(frag) else {
                return (
                    pairs,
                    Some(Tail::Value {
                        key,
                        is_string: false,
                        frag: frag.to_string(),
                    }),
                );
            };
            if j >= n {
                return (
                    pairs,
                    Some(Tail::Value {
                        key,
                        is_string: false,
                        frag: frag.to_string(),
                    }),
                );
            }
            pairs.push((key, value));
            i = j;
        }
    }
}

fn param_line(key: &str, value: &Ordered) -> String {
    match value {
        Ordered::Str(s) => format!("{PARAMETER_OPEN}{key}\" string=\"true\">{s}{PARAMETER_CLOSE}"),
        other => format!("{PARAMETER_OPEN}{key}\" string=\"false\">{}{PARAMETER_CLOSE}", py_json(other)),
    }
}

fn encode_tool_call(name: Option<&str>, arguments: &str, partial: bool) -> String {
    let Some(name) = name else {
        return String::new();
    };
    let open = format!("{INVOKE_OPEN}{name}\">");
    let (pairs, tail) = parse_partial_args(arguments);
    let mut lines: Vec<String> = pairs.iter().map(|(k, v)| param_line(k, v)).collect();

    let closed = arguments.trim_end().ends_with('}') && tail.is_none();
    if !partial || !closed {
        match tail {
            Some(Tail::Key(p)) => lines.push(format!("{PARAMETER_OPEN}{p}")),
            Some(Tail::KeyDone(k)) => lines.push(format!("{PARAMETER_OPEN}{k}\"")),
            Some(Tail::Value { key, is_string, frag }) => {
                let flag = if is_string { "true" } else { "false" };
                lines.push(format!("{PARAMETER_OPEN}{key}\" string=\"{flag}\">{frag}"));
            }
            None => {}
        }
    }

    let body = lines.join("\n");
    if closed || !partial {
        return format!("{open}\n{body}\n{INVOKE_CLOSE}");
    }
    if body.is_empty() { open } else { format!("{open}\n{body}") }
}

fn seam_safe(text: String) -> String {
    if CLOSING_TAGS.iter().any(|t| text.ends_with(t)) {
        return text + "\n";
    }
    text
}


struct ToolSlot {
    index: i64,
    name: Option<String>,
    arguments: String,
}

pub struct Dsv4Reconstructor {
    reasoning: String,
    content: String,
    tools: Vec<ToolSlot>,
    saw_any_tool_frame: bool,
    thinking: bool,
    cap: usize,
    envelope: Option<Envelope>,
    finish_reason: bool,
    disarmed: Option<AccumulateError>,
}

impl Dsv4Reconstructor {
    pub fn new(cap: usize, thinking: bool) -> Self {
        Self {
            reasoning: String::new(),
            content: String::new(),
            tools: Vec::new(),
            saw_any_tool_frame: false,
            thinking,
            cap,
            envelope: None,
            finish_reason: false,
            disarmed: None,
        }
    }

    #[cfg(test)]
    fn chat_mode(cap: usize) -> Self {
        Self::new(cap, false)
    }

    fn disarm(&mut self, cause: AccumulateError) -> Result<(), AccumulateError> {
        self.reasoning = String::new();
        self.content = String::new();
        self.tools = Vec::new();
        let cause = *self.disarmed.get_or_insert(cause);
        Err(cause)
    }

    fn fits(&self, extra: usize) -> bool {
        self.len_bytes() + extra <= self.cap
    }

    fn slot(&mut self, index: i64) -> &mut ToolSlot {
        if let Some(pos) = self.tools.iter().position(|t| t.index == index) {
            return &mut self.tools[pos];
        }
        self.tools.push(ToolSlot {
            index,
            name: None,
            arguments: String::new(),
        });
        self.tools.last_mut().expect("just pushed")
    }

    fn reconstruct(&self) -> String {
        let mut out = String::new();
        let started_body = !self.content.is_empty() || self.saw_any_tool_frame;

        out.push_str(&self.reasoning);
        if self.thinking && started_body {
            out.push_str(THINK_END);
        }
        out.push_str(&self.content);

        if !self.saw_any_tool_frame {
            return out;
        }

        if !self.content.ends_with(TOOL_BLOCK_SEPARATOR) {
            out.push_str(TOOL_BLOCK_SEPARATOR);
        }
        out.push_str(TOOL_CALLS_OPEN);
        out.push('\n');

        let terminal = self.finish_reason;
        let last = self.tools.len().saturating_sub(1);
        let blocks: Vec<String> = self
            .tools
            .iter()
            .enumerate()
            .map(|(i, slot)| {
                let partial = i == last && !terminal;
                let mut block = encode_tool_call(slot.name.as_deref(), &slot.arguments, partial);
                if !block.is_empty() && !block.contains('\n') && block.ends_with("\">") {
                    block.push('\n');
                }
                block
            })
            .collect();

        let joined = blocks.iter().filter(|b| !b.is_empty()).cloned().collect::<Vec<_>>().join("\n");
        out.push_str(&joined);
        if terminal && blocks.last().is_some_and(|b| b.ends_with(INVOKE_CLOSE)) {
            out.push('\n');
            out.push_str(TOOL_CALLS_CLOSE);
        }
        out
    }
}

impl StreamAccumulator for Dsv4Reconstructor {
    fn ingest(&mut self, chunk: &Value) -> Result<(), AccumulateError> {
        if let Some(cause) = self.disarmed {
            return Err(cause);
        }
        capture_envelope(&mut self.envelope, chunk);

        let choice = match single_choice(chunk) {
            Ok(Some(choice)) => choice,
            Ok(None) => return Ok(()),
            Err(cause) => return self.disarm(cause),
        };
        if choice.get("finish_reason").is_some_and(|f| !f.is_null()) {
            self.finish_reason = true;
        }
        let Some(delta) = choice.get("delta").and_then(Value::as_object) else {
            return Ok(());
        };

        let present = |k: &str| delta.get(k).is_some_and(|v| !v.is_null());
        if present("function_call") || (present("reasoning") && !present("reasoning_content")) {
            return self.disarm(AccumulateError::UnsupportedDelta);
        }

        if let Some(r) = delta.get("reasoning_content").and_then(Value::as_str) {
            if !self.fits(r.len()) {
                return self.disarm(AccumulateError::CapExceeded);
            }
            self.reasoning.push_str(r);
        }
        if let Some(c) = delta.get("content").and_then(Value::as_str) {
            if !self.fits(c.len()) {
                return self.disarm(AccumulateError::CapExceeded);
            }
            self.content.push_str(c);
        }
        if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                self.saw_any_tool_frame = true;
                let index = call.get("index").and_then(Value::as_i64).unwrap_or(0);
                let function = call.get("function");
                let name = function
                    .and_then(|f| f.get("name"))
                    .and_then(Value::as_str)
                    .filter(|n| !n.is_empty())
                    .map(str::to_string);
                let args = function
                    .and_then(|f| f.get("arguments"))
                    .and_then(Value::as_str)
                    .filter(|a| !a.is_empty())
                    .unwrap_or_default()
                    .to_string();
                if !self.fits(name.as_ref().map_or(0, String::len) + args.len()) {
                    return self.disarm(AccumulateError::CapExceeded);
                }
                if self.tools.last().is_some_and(|last| last.index != index) && self.tools.iter().any(|t| t.index == index) {
                    return self.disarm(AccumulateError::UnsupportedDelta);
                }
                let slot = self.slot(index);
                if name.is_some() {
                    slot.name = name;
                }
                slot.arguments.push_str(&args);
            }
        }
        Ok(())
    }

    fn continuation_text(&self) -> Option<String> {
        if self.disarmed.is_some() {
            return None;
        }
        let text = seam_safe(self.reconstruct());
        (!text.is_empty()).then_some(text)
    }

    fn len_bytes(&self) -> usize {
        self.reasoning.len()
            + self.content.len()
            + self
                .tools
                .iter()
                .map(|t| t.name.as_ref().map_or(0, String::len) + t.arguments.len())
                .sum::<usize>()
    }

    fn envelope(&self) -> Option<&Envelope> {
        self.envelope.as_ref()
    }

    fn saw_finish_reason(&self) -> bool {
        self.finish_reason
    }

    fn disarmed(&self) -> Option<AccumulateError> {
        self.disarmed
    }

    fn disarm_externally(&mut self, cause: AccumulateError) {
        let _ = self.disarm(cause);
    }

    fn plain_resume_ok(&self) -> bool {
        !self.saw_any_tool_frame && (!self.thinking || !self.content.is_empty())
    }
}

#[cfg(test)]
mod tests;
