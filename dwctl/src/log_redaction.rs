
use std::borrow::Cow;
use std::io;
use std::sync::LazyLock;
use std::time::Duration;

use opentelemetry::trace::Status;
use opentelemetry::{Array, KeyValue, StringValue, Value};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use regex::Regex;
use tracing::Metadata;
use tracing_subscriber::fmt::MakeWriter;

pub const REDACTED: &str = "<redacted>";

static SECRET_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?P<bearer>\bbearer\s+)[A-Za-z0-9._~+/=-]{16,}|sk-[A-Za-z0-9_-]{20,}").expect("secret pattern is valid")
});

pub fn redact_secrets(text: &str) -> Cow<'_, str> {
    SECRET_PATTERN.replace_all(text, |captures: &regex::Captures<'_>| match captures.name("bearer") {
        Some(prefix) => format!("{}{REDACTED}", prefix.as_str()),
        None => REDACTED.to_string(),
    })
}

pub struct RedactingMakeWriter<M>(pub M);

impl<'a, M: MakeWriter<'a>> MakeWriter<'a> for RedactingMakeWriter<M> {
    type Writer = RedactingWriter<M::Writer>;

    fn make_writer(&'a self) -> Self::Writer {
        RedactingWriter::new(self.0.make_writer())
    }

    fn make_writer_for(&'a self, meta: &Metadata<'_>) -> Self::Writer {
        RedactingWriter::new(self.0.make_writer_for(meta))
    }
}

pub struct RedactingWriter<W: io::Write> {
    inner: W,
    pending: Vec<u8>,
}

impl<W: io::Write> RedactingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            pending: Vec::new(),
        }
    }

    fn write_pending(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.pending);
        let text = String::from_utf8_lossy(&pending);
        self.inner.write_all(redact_secrets(&text).as_bytes())
    }
}

impl<W: io::Write> io::Write for RedactingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.pending.extend_from_slice(buf);
        if buf.ends_with(b"\n") {
            self.write_pending()?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.write_pending()?;
        self.inner.flush()
    }
}

impl<W: io::Write> Drop for RedactingWriter<W> {
    fn drop(&mut self) {
        let _ = self.write_pending();
    }
}

#[derive(Debug)]
pub struct RedactingSpanExporter<E>(pub E);

impl<E: SpanExporter> SpanExporter for RedactingSpanExporter<E> {
    async fn export(&self, mut batch: Vec<SpanData>) -> OTelSdkResult {
        batch.iter_mut().for_each(redact_span);
        self.0.export(batch).await
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.0.force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.0.set_resource(resource);
    }
}

fn redact_span(span: &mut SpanData) {
    redact_cow(&mut span.name);
    redact_attributes(&mut span.attributes);
    for event in span.events.events.iter_mut() {
        redact_cow(&mut event.name);
        redact_attributes(&mut event.attributes);
    }
    for link in span.links.links.iter_mut() {
        redact_attributes(&mut link.attributes);
    }
    if let Status::Error { description } = &mut span.status {
        redact_cow(description);
    }
}

fn redact_cow(text: &mut Cow<'static, str>) {
    if let Cow::Owned(redacted) = redact_secrets(text) {
        *text = Cow::Owned(redacted);
    }
}

fn redact_attributes(attributes: &mut [KeyValue]) {
    for attribute in attributes {
        match &mut attribute.value {
            Value::String(value) => redact_string_value(value),
            Value::Array(Array::String(values)) => values.iter_mut().for_each(redact_string_value),
            _ => {}
        }
    }
}

fn redact_string_value(value: &mut StringValue) {
    if let Cow::Owned(redacted) = redact_secrets(value.as_str()) {
        *value = redacted.into();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "sk-AbCdEfGhIjKlMnOpQrStUvWxYz0123456789_-AbCdE";

    #[test]
    fn redacts_api_keys_wherever_they_appear() {
        assert_eq!(redact_secrets(KEY), REDACTED);
        assert_eq!(
            redact_secrets(&format!(r#"BatchInput {{ api_key: Some("{KEY}") }}"#)),
            r#"BatchInput { api_key: Some("<redacted>") }"#
        );
        assert_eq!(
            redact_secrets(&format!(r#"{{"api_key":"{KEY}","n":1}}"#)),
            r#"{"api_key":"<redacted>","n":1}"#
        );
        assert_eq!(redact_secrets(&format!("key=%22{KEY}%22")), "key=%22<redacted>%22");
        assert_eq!(redact_secrets(&format!(r"first line\n{KEY}")), r"first line\n<redacted>");
        assert_eq!(redact_secrets(&format!("apikey{KEY}")), "apikey<redacted>");
    }

    #[test]
    fn redacts_bearer_tokens_and_keeps_the_scheme() {
        assert_eq!(
            redact_secrets("authorization: Bearer 0123456789abcdef.ghijklmnop"),
            "authorization: Bearer <redacted>"
        );
        assert_eq!(
            redact_secrets("authorization: bearer 0123456789abcdef"),
            "authorization: bearer <redacted>"
        );
    }

    #[test]
    fn leaves_text_without_credentials_borrowed() {
        for text in [
            "sk-short",
            "risk-assessment",
            "task-1234",
            "no secrets here",
            "Bearer token validation successful",
            "Bearer token validation failed",
        ] {
            assert!(matches!(redact_secrets(text), Cow::Borrowed(t) if t == text), "{text}");
        }
    }

    #[test]
    fn writer_redacts_each_write() {
        let mut out = Vec::new();
        io::Write::write_all(&mut RedactingWriter::new(&mut out), format!("key={KEY}\n").as_bytes()).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), "key=<redacted>\n");
    }

    #[test]
    fn writer_redacts_a_credential_split_across_writes() {
        let (head, tail) = KEY.split_at(10);
        let mut out = Vec::new();
        {
            let mut writer = RedactingWriter::new(&mut out);
            for piece in ["authorization: Bearer ", head, tail, "\n", "key=", head, tail] {
                io::Write::write_all(&mut writer, piece.as_bytes()).unwrap();
            }
        }
        assert_eq!(String::from_utf8(out).unwrap(), "authorization: Bearer <redacted>\nkey=<redacted>");
    }

    #[derive(Default)]
    struct FlakySink {
        out: Vec<u8>,
        calls: usize,
    }

    impl io::Write for FlakySink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.calls += 1;
            let accepted = match self.calls {
                1 => &buf[..5],
                2 => return Err(io::Error::other("sink failed")),
                _ => buf,
            };
            self.out.extend_from_slice(accepted);
            Ok(accepted.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn writer_does_not_repeat_a_line_after_a_failed_write() {
        let mut sink = FlakySink::default();
        {
            let mut writer = RedactingWriter::new(&mut sink);
            assert!(io::Write::write_all(&mut writer, b"first line\n").is_err());
            io::Write::write_all(&mut writer, b"second line\n").unwrap();
        }
        assert_eq!(String::from_utf8(sink.out).unwrap(), "firstsecond line\n");
    }

    #[test]
    fn exporter_redacts_name_attributes_events_links_and_status() {
        let mut span = test_span_data();
        span.name = format!("POST {KEY}").into();
        span.attributes = vec![
            KeyValue::new("input", format!("api_key: {KEY:?}")),
            KeyValue::new("keys", Value::Array(Array::String(vec![KEY.to_string().into()]))),
            KeyValue::new("count", 3),
        ];
        span.events.events.push(opentelemetry::trace::Event::new(
            format!("failed with {KEY}"),
            std::time::SystemTime::now(),
            vec![KeyValue::new("authorization", format!("Bearer {KEY}"))],
            0,
        ));
        span.links.links.push(opentelemetry::trace::Link::new(
            span.span_context.clone(),
            vec![KeyValue::new("source", format!("batch created with {KEY}"))],
            0,
        ));
        span.status = Status::error(format!("rejected {KEY}"));

        redact_span(&mut span);

        let debug = format!("{span:?}");
        assert!(!debug.contains(KEY), "secret exported: {debug}");
        assert_eq!(span.name, "POST <redacted>");
        assert_eq!(span.attributes[2], KeyValue::new("count", 3));
    }

    fn test_span_data() -> SpanData {
        use opentelemetry::InstrumentationScope;
        use opentelemetry::trace::{SpanContext, SpanId, SpanKind, TraceFlags, TraceId, TraceState};
        use opentelemetry_sdk::trace::{SpanEvents, SpanLinks};

        SpanData {
            span_context: SpanContext::new(TraceId::from(1), SpanId::from(1), TraceFlags::SAMPLED, false, TraceState::default()),
            parent_span_id: SpanId::INVALID,
            parent_span_is_remote: false,
            span_kind: SpanKind::Internal,
            name: "span".into(),
            start_time: std::time::SystemTime::now(),
            end_time: std::time::SystemTime::now(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            events: SpanEvents::default(),
            links: SpanLinks::default(),
            status: Status::Unset,
            instrumentation_scope: InstrumentationScope::builder("test").build(),
        }
    }
}
