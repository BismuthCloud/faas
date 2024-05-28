use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::metrics::{
    reader::{DefaultAggregationSelector, DefaultTemporalitySelector},
    MeterProvider, PeriodicReader,
};
use opentelemetry_sdk::resource::SdkProvidedResourceDetector;
use opentelemetry_sdk::Resource;
use std::time::Duration;

mod axum_metrics;
pub use axum_metrics::*;

pub fn init_metrics(static_attrs: &[opentelemetry::KeyValue]) {
    let reader = PeriodicReader::builder(
        opentelemetry_otlp::new_exporter()
            .http()
            .with_http_client(reqwest::Client::new())
            .with_timeout(Duration::from_secs(3))
            .build_metrics_exporter(
                Box::new(DefaultAggregationSelector::new()),
                Box::new(DefaultTemporalitySelector::new()),
            )
            .unwrap(),
        opentelemetry_sdk::runtime::Tokio,
    )
    .with_interval(Duration::from_secs(30))
    .build();

    let provider = MeterProvider::builder()
        .with_reader(reader)
        .with_resource(
            Resource::from_detectors(
                Duration::from_secs(5),
                vec![Box::new(SdkProvidedResourceDetector)],
            )
            .merge(&Resource::new(static_attrs.to_vec())),
        )
        .build();

    opentelemetry::global::set_meter_provider(provider.clone());
}
