use anyhow::{anyhow, Result};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::{
    trace::{RandomIdGenerator, Sampler},
    Resource,
};
use sentry::integrations::anyhow::capture_anyhow;
use std::{net::Ipv4Addr};
use std::{str::FromStr, time::Duration};
use url::Url;
use uuid::Uuid;

use serde::{Deserialize, Serialize};

pub mod test;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InvokeMode {
    /// The container runs executables, one per request.
    /// Requests are sent to the program's stdin, and the response is read from its stdout.
    Executable(Vec<String>),

    /// The container is running the specified program (a web server) on the specified port.
    /// Requests are proxied to the container's HTTP server, and the response is proxied back.
    Server(Vec<String>, u16),
}

impl FromStr for InvokeMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.splitn(2, ':').collect();
        match parts.as_slice() {
            ["exec", args] => Ok(InvokeMode::Executable(
                args.split(' ').map(str::to_string).collect(),
            )),
            ["server", port_and_exec] => {
                let parts: Vec<&str> = port_and_exec.splitn(2, ':').collect();
                let port = parts[0]
                    .parse()
                    .map_err(|e| format!("Invalid port: {}", e))?;
                let args = parts[1].split(' ').map(str::to_string).collect();
                Ok(InvokeMode::Server(args, port))
            }
            _ => Err(
                "Invoke mode must be one of 'exec:args' or 'server:listen_port:args'".to_string(),
            ),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FunctionDefinition {
    /// Image to use for the container.
    pub image: String,

    /// URL of the git repository and branch to clone into the container.
    pub repo: Option<(Url, String)>,

    /// CPU limit, as a fraction of a core.
    pub cpu: f32,

    /// Memory limit, in bytes.
    pub memory: u64,

    /// How to communicate with the container.
    pub invoke_mode: InvokeMode,

    /// Maximum number of instances of this function to run.
    pub max_instances: u32,
}

pub const BACKEND_PORT: u16 = 8001;
pub const UUID_PACKED_LEN: usize = 16;
pub const UUID_STR_LEN: usize = 36;

#[derive(Clone, Debug, Serialize)]
pub struct Backend {
    pub ip: Ipv4Addr,
    pub container_id: Uuid,
}

impl conhash::Node for Backend {
    fn name(&self) -> String {
        format!("{}:{}", self.ip, self.container_id)
    }
}

pub fn unpack_backends(data: &[u8]) -> Result<Vec<Backend>> {
    if data.len() % (4 + UUID_PACKED_LEN) != 0 {
        return Err(anyhow!("Invalid backend data length: {}", data.len()));
    }

    let mut backends = Vec::new();
    // 4 = size of an IPv4 address
    for chunk in data.chunks(4 + UUID_PACKED_LEN) {
        let backend_ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        let container_id = Uuid::from_slice(&chunk[4..])?;
        backends.push(Backend {
            ip: backend_ip,
            container_id,
        });
    }
    Ok(backends)
}

pub fn pack_backends(backends: &[Backend]) -> Vec<u8> {
    let mut data = Vec::new();
    for backend in backends {
        data.extend(backend.ip.octets().iter());
        data.extend(backend.container_id.as_bytes());
    }
    data
}

#[derive(Debug, Serialize, PartialEq)]
#[repr(u8)]
pub enum ContainerState {
    Invalid = 0,
    Starting = 1,
    Running = 2,
    Paused = 3,
    Failed = 4,
}

// Broad error types that can be returned anywhere and will bubble up to the API layer,
// returning the appropriate HTTP status code.
#[derive(thiserror::Error, Debug)]
pub enum GenericError {
    #[error("Not found")]
    NotFound,
    #[error("Unavailable")]
    Unavailable,
}

// axum error type which wraps `anyhow::Error`.
// https://github.com/tokio-rs/axum/blob/v0.6.x/examples/anyhow-error-response/src/main.rs
pub enum ApiError {
    NotFound,
    Status(StatusCode),
    Error(anyhow::Error),
    Response(axum::http::Response<axum::body::BoxBody>),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        match self {
            ApiError::NotFound => StatusCode::NOT_FOUND.into_response(),
            ApiError::Status(status) => status.into_response(),
            ApiError::Error(err) => {
                if let Some(ge) = err.downcast_ref::<GenericError>() {
                    match ge {
                        GenericError::NotFound => StatusCode::NOT_FOUND.into_response(),
                        GenericError::Unavailable => {
                            StatusCode::SERVICE_UNAVAILABLE.into_response()
                        }
                    }
                } else {
                    capture_anyhow(&err);
                    // In debug mode, the entire error and backtrace is dumped back for easier debugging.
                    if cfg!(debug_assertions) {
                        (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", err)).into_response()
                    } else {
                        StatusCode::INTERNAL_SERVER_ERROR.into_response()
                    }
                }
            }
            ApiError::Response(resp) => resp,
        }
    }
}

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        ApiError::Error(err.into())
    }
}

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

    if let Ok(opentelemetry_endpoint) = std::env::var(opentelemetry_otlp::OTEL_EXPORTER_OTLP_ENDPOINT) {
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

pub fn init_sentry() -> Option<sentry::ClientInitGuard> {
    if let Ok(dsn) = std::env::var("SENTRY_DSN") {
        eprintln!("Sentry DSN found, initializing Sentry");
        Some(sentry::init((
            dsn,
            sentry::ClientOptions {
                release: sentry::release_name!(),
                ..Default::default()
            },
        )))
    } else {
        None
    }
}
