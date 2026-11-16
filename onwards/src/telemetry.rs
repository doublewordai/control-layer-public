
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
pub use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::trace::Tracer;
use std::collections::HashMap;
use tracing::{Subscriber, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

pub const OTEL_TRACES_FILTER: &str = "OTEL_TRACES_FILTER";

pub fn init_telemetry() -> anyhow::Result<Option<SdkTracerProvider>> {
    let env_filter =
        EnvFilter::new(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()));

    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        let (tracer, provider) = create_otlp_tracer()?;
        let traces_filter =
            std::env::var(OTEL_TRACES_FILTER).unwrap_or_else(|_| "info".to_string());

        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().compact())
            .with(otel_layer(tracer, &traces_filter))
            .try_init()?;

        info!(
            traces_filter,
            "Telemetry initialized with OTLP export enabled"
        );
        Ok(Some(provider))
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().compact())
            .try_init()?;

        info!(
            "Telemetry initialized (OTLP export disabled — set OTEL_EXPORTER_OTLP_ENDPOINT to enable)"
        );
        Ok(None)
    }
}

fn otel_layer<S>(tracer: Tracer, directives: &str) -> impl Layer<S>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_location(false)
        .with_threads(false)
        .with_tracked_inactivity(false)
        .with_filter(EnvFilter::new(directives))
}

fn create_otlp_tracer() -> anyhow::Result<(Tracer, SdkTracerProvider)> {
    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "onwards".to_string());

    let base = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4318".to_string());
    let endpoint = if base.ends_with("/v1/traces") {
        base
    } else {
        format!("{}/v1/traces", base.trim_end_matches('/'))
    };

    let mut headers = HashMap::new();
    if let Ok(headers_str) = std::env::var("OTEL_EXPORTER_OTLP_HEADERS") {
        let decoded = headers_str.replace("%20", " ");
        for pair in decoded.split(',') {
            if let Some((key, value)) = pair.split_once('=') {
                headers.insert(key.trim().to_string(), value.trim().to_string());
            }
        }
    }

    let protocol = match std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
        .as_deref()
        .unwrap_or("http/protobuf")
    {
        "http/protobuf" => Protocol::HttpBinary,
        "http/json" => Protocol::HttpJson,
        _ => Protocol::HttpBinary,
    };

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(&endpoint)
        .with_protocol(protocol)
        .with_headers(headers)
        .build()?;

    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name(service_name.clone())
        .build();

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    let tracer = tracer_provider.tracer(service_name);

    Ok((tracer, tracer_provider))
}
