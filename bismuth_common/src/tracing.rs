use anyhow::Result;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::{
    trace::{RandomIdGenerator, Sampler},
    Resource,
};
use std::time::Duration;

pub fn init_tracer(
    service: &str,
) -> Result<opentelemetry_sdk::trace::Tracer, opentelemetry::trace::TraceError> {
    // Required for axum-otel to work
    std::env::set_var(
        "RUST_LOG",
        format!(
            // `otel::tracing` should be a level trace to emit opentelemetry trace & span
            // `otel::setup` set to debug to log detected resources, configuration read and infered
            "{},otel::tracing=trace,otel=debug",
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string())
        ),
    );

    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    if let Ok(opentelemetry_endpoint) =
        std::env::var(opentelemetry_otlp::OTEL_EXPORTER_OTLP_ENDPOINT)
    {
        // So many hours wasted on this. The endpoint must not end with a slash for the rust libraries, since it blindly concats /v1/...
        let opentelemetry_endpoint = if opentelemetry_endpoint.ends_with('/') {
            opentelemetry_endpoint[0..opentelemetry_endpoint.len() - 1].to_string()
        } else {
            opentelemetry_endpoint
        };
        std::env::set_var(
            opentelemetry_otlp::OTEL_EXPORTER_OTLP_ENDPOINT,
            opentelemetry_endpoint,
        );

        opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .http()
                    .with_http_client(reqwest::Client::new())
                    .with_timeout(Duration::from_secs(3)),
            )
            .with_trace_config(
                opentelemetry_sdk::trace::config()
                    .with_sampler(Sampler::AlwaysOn)
                    .with_id_generator(RandomIdGenerator::default())
                    // .with_max_events_per_span(64)
                    // .with_max_attributes_per_span(16)
                    // .with_max_events_per_span(16)
                    .with_resource(Resource::new(vec![KeyValue::new(
                        "service.name",
                        service.to_string(),
                    )])),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)
    } else if cfg!(debug_assertions) {
        eprintln!("DEV OTEL_EXPORTER_OTLP_ENDPOINT not found, sinking to file");

        let file = std::fs::File::create(format!("/tmp/{}_trace.log", service)).unwrap();
        let exporter = opentelemetry_stdout::SpanExporter::builder()
            .with_writer(file)
            .build();
        let provider = opentelemetry_sdk::trace::TracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();
        let tracer = provider.tracer(service.to_string());
        opentelemetry::global::set_tracer_provider(provider);

        Ok(tracer)
    } else {
        eprintln!("OTEL_EXPORTER_OTLP_ENDPOINT not found!");

        let exporter = opentelemetry_stdout::SpanExporter::builder()
            .with_writer(std::io::sink())
            .build();
        let provider = opentelemetry_sdk::trace::TracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();
        let tracer = provider.tracer(service.to_string());
        opentelemetry::global::set_tracer_provider(provider);

        Ok(tracer)
    }
}
