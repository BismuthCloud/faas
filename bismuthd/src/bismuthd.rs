use anyhow::{anyhow, Context, Result};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::Event;
use axum::response::{IntoResponse, Sse};
use axum::routing::{any, get};
use clap::Parser;
use futures::stream::TryStreamExt as _;
use nix::libc::{kill, SIGKILL};
use opentelemetry::trace::TraceContextExt as _;
use prost_types::Any;
use sentry::integrations::tower::{NewSentryLayer, SentryHttpLayer};
use serde::Deserialize;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::AsyncReadExt as _;
use tokio::time::{sleep, timeout};
use tower::ServiceBuilder;
use tracing::{event, instrument, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};
use uuid::Uuid;

// Maybe use OCI instead of containerd's specific API?
use containerd_client::services::v1::{
    tasks_client::TasksClient, DeleteProcessRequest, ExecProcessRequest, StartRequest, WaitRequest,
};
// Needed with with_namespace
use containerd_client::tonic::Request;
use containerd_client::with_namespace;

use bismuth_common::{
    init_metrics, init_sentry, init_tracer, ApiError, ContainerState, InvokeMode,
    OtelAxumMetricsLayer, BACKEND_PORT,
};

pub mod consts;
pub mod container;
pub mod container_manager;

use consts::*;
use container::SvcProviderOptions;
use container_manager::ContainerManager;

/// bismuthd
#[derive(Debug, Parser)]
#[clap(name = "bismuthd", version)]
struct Cli {
    /// ZooKeeper IP:port
    #[clap(long, global = true, default_value = "127.0.0.1:2181")]
    zookeeper: String,

    /// ZooKeeper environment name (e.g. "dev", "test", "default")
    #[clap(long, global = true, default_value = "default")]
    zookeeper_env: String,

    /// Bind IP
    #[clap(long)]
    bind: Ipv4Addr,

    /// Arguments that will be passed to svcprovider.
    #[clap(long)]
    svcprovider_args: Vec<String>,
}

#[instrument(skip(container_manager, http_client, req))]
#[axum::debug_handler]
async fn invoke_path(
    State((container_manager, http_client)): State<(
        Arc<ContainerManager>,
        hyper::client::Client<hyper::client::HttpConnector, Body>,
    )>,
    Path((container_id, reqpath)): Path<(Uuid, String)>,
    req: axum::http::Request<Body>,
) -> Result<axum::response::Response, ApiError> {
    let container = container_manager.get_container(container_id).await?;
    let container = container.read().await;

    match &container.definition.invoke_mode {
        InvokeMode::Executable(args) => {
            // TODO pipes/fifos and remove tempfile as a dependency
            let iodir = tempfile::tempdir()?;
            let outpath = iodir.path().join("out");
            let errpath = iodir.path().join("err");
            File::create(&outpath).await?;
            File::create(&errpath).await?;

            let exec_id = tracing::Span::current()
                .context()
                .span()
                .span_context()
                .trace_id()
                .to_string();
            event!(Level::TRACE, exec_id = %exec_id, container_id = %container.id, "Executing {:?}", args);

            let mut task_client = TasksClient::new(container.containerd.clone());
            let req = ExecProcessRequest {
                container_id: container.containerd_id.clone(),
                stdin: "/dev/null".to_string(),
                stdout: outpath.to_str().unwrap().to_string(),
                stderr: errpath.to_str().unwrap().to_string(),
                terminal: false,
                spec: Some(Any {
                    type_url: "types.containerd.io/opencontainers/runtime-spec/1/Process"
                        .to_string(),
                    value: serde_json::to_string(
                        &oci_spec::runtime::ProcessBuilder::default()
                            .user(
                                oci_spec::runtime::UserBuilder::default()
                                    .uid(1000u32)
                                    .gid(1000u32)
                                    .build()?,
                            )
                            .args(args.clone())
                            .env(vec![format!(
                                "BISMUTH_AUTH={}",
                                container.node_data.setup.auth_token
                            )])
                            .build()?,
                    )?
                    .into(),
                }),
                exec_id: exec_id.clone(),
            };
            let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
            task_client.exec(req).await?;

            let req = StartRequest {
                container_id: container.containerd_id.clone(),
                exec_id: exec_id.clone(),
            };
            let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
            let start_resp = task_client.start(req).await?;

            let req = WaitRequest {
                container_id: container.containerd_id.clone(),
                exec_id: exec_id.clone(),
            };
            let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
            match timeout(Duration::from_secs(60), task_client.wait(req)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    event!(Level::ERROR, %e, "Error waiting for process");
                    return Err(ApiError::Status(StatusCode::INTERNAL_SERVER_ERROR));
                }
                Err(_) => {
                    event!(Level::INFO, "Timeout waiting for process");
                    unsafe { kill(start_resp.into_inner().pid as _, SIGKILL) };
                    sleep(Duration::from_secs(1)).await;
                }
            }

            let req = DeleteProcessRequest {
                container_id: container.containerd_id.clone(),
                exec_id: exec_id.clone(),
            };
            let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
            task_client.delete_process(req).await?;

            let outf = File::open(outpath).await?;
            let reader_stream = tokio_util::io::ReaderStream::new(outf);
            Ok(axum::body::boxed(axum::body::StreamBody::new(reader_stream)).into_response())
        }
        InvokeMode::Server(_, dport) => {
            event!(Level::TRACE, reqpath = %reqpath, container_id = %container.id, "Proxying request");

            if !container
                .node_data
                .runtime
                .as_ref()
                .map(|r| r.state == ContainerState::Running)
                .unwrap_or_default()
            {
                return Err(ApiError::Status(StatusCode::SERVICE_UNAVAILABLE));
            }

            let mut req = req;
            *req.uri_mut() = format!(
                "http://{}:{}/{}",
                container.node_data.runtime.as_ref().unwrap().ip,
                *dport,
                reqpath
            )
            .parse()?;
            let cx = tracing::Span::current().context();
            opentelemetry::global::get_text_map_propagator(|propagator| {
                propagator.inject_context(
                    &cx,
                    &mut opentelemetry_http::HeaderInjector(req.headers_mut()),
                )
            });

            let resp = http_client.request(req).await.map_err(|e| {
                ApiError::Response(
                    axum::response::Response::builder()
                        .status(StatusCode::BAD_GATEWAY)
                        .header("X-Bismuth-Container-ID", container_id.to_string())
                        .body(axum::body::boxed(axum::body::Empty::new()))
                        .unwrap(),
                )
            })?;
            let mut axum_resp = axum::response::Response::builder().status(resp.status());
            *axum_resp.headers_mut().unwrap() = resp.headers().clone();
            let axum_resp = axum_resp.header("X-Bismuth-Container-ID", container_id.to_string());
            let body = http_body::Body::map_err(resp.into_body(), axum::Error::new);
            Ok(axum_resp.body(axum::body::boxed(body))?)
        }
    }
}

async fn invoke(
    state: State<(
        Arc<ContainerManager>,
        hyper::client::Client<hyper::client::HttpConnector, Body>,
    )>,
    Path(container_id): Path<Uuid>,
    req: axum::http::Request<Body>,
) -> Result<axum::response::Response, ApiError> {
    invoke_path(state, Path((container_id, "".to_string())), req).await
}

#[derive(Deserialize)]
struct LogsParams {
    follow: Option<bool>,
}

#[instrument(skip(container_manager, params))]
#[axum::debug_handler]
async fn get_logs(
    State((container_manager, _)): State<(
        Arc<ContainerManager>,
        hyper::client::Client<hyper::client::HttpConnector, Body>,
    )>,
    Path(container_id): Path<Uuid>,
    Query(params): Query<LogsParams>,
) -> Result<axum::response::Response, ApiError> {
    tracing::Span::current().record("follow", params.follow.unwrap_or_default());
    let container = match container_manager.get_container(container_id).await {
        Ok(c) => c,
        Err(e)
            if (e.downcast_ref::<zookeeper_client::Error>()
                == Some(&zookeeper_client::Error::NoNode)) =>
        {
            return Err(ApiError::NotFound);
        }
        Err(e) => return Err(e.into()),
    };

    let runtime_data = &container.read().await.node_data.runtime;
    if let Some(runtime_data) = runtime_data {
        let mut stdout = File::open(&runtime_data.stdout).await?;
        let mut stderr = File::open(&runtime_data.stderr).await?;

        if !params.follow.unwrap_or(false) {
            let stream = tokio_util::io::ReaderStream::new(stderr);
            return Ok(axum::body::boxed(axum::body::StreamBody::new(stream)).into_response());
        }

        event!(Level::TRACE, container_id = %container_id, "Tailing logs");
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<String>>(1);
        tokio::spawn(async move {
            loop {
                if tx.is_closed() {
                    event!(Level::TRACE, "Streaming logs connection closed");
                    break;
                }

                // Check if the container still exists
                if container_manager.get_container(container_id).await.is_err() {
                    event!(
                        Level::TRACE,
                        "Terminating streaming logs connection: container no longer exists"
                    );
                    break;
                }

                let mut outbuf = [0; 4096];
                let mut errbuf = [0; 4096];
                let result = tokio::select! {
                    r = stdout.read(&mut outbuf) => (1, r),
                    r = stderr.read(&mut errbuf) => (2, r),
                };
                match result {
                    (_, Ok(0)) => {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    (_, Ok(n)) => {
                        // TODO: it's possible this fixed size read lands us in the middle
                        // of a code point, making the output invalid utf8.
                        let buf = match result {
                            (1, _) => &outbuf,
                            (2, _) => &errbuf,
                            _ => unreachable!(),
                        };
                        let data = String::from_utf8(buf[..n].to_vec()).unwrap();
                        tx.send(Ok(data)).await.unwrap();
                    }
                    (_, Err(e)) => {
                        event!(Level::WARN, %e, "Error reading from stderr");
                        break;
                    }
                }
            }
        });

        let stream =
            tokio_stream::wrappers::ReceiverStream::new(rx).map_ok(|v| Event::default().data(v));
        Ok(Sse::new(stream)
            .keep_alive(axum::response::sse::KeepAlive::new())
            .into_response())
    } else {
        Err(anyhow!("Container not running").into())
    }
}

pub fn app() -> axum::Router<(
    Arc<ContainerManager>,
    hyper::client::Client<hyper::client::HttpConnector, Body>,
)> {
    axum::Router::new()
        .route("/invoke/:container_id", any(invoke))
        .route("/invoke/:container_id/", any(invoke))
        .route("/invoke/:container_id/*reqpath", any(invoke_path))
        .route("/logs/:container_id", get(get_logs))
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut _lockfile = fd_lock::RwLock::new(
        File::create(LOCKFILE_PATH)
            .await
            .context("Error opening lockfile")?,
    );
    let _lockguard = _lockfile
        .try_write()
        .map_err(|_| anyhow!("bismuthd is already running"))?;

    let _sentry_guard = init_sentry();
    let tracer = init_tracer(env!("CARGO_PKG_NAME"))?;
    init_metrics(&[opentelemetry::KeyValue::new(
        "service.name",
        env!("CARGO_PKG_NAME"),
    )]);

    let args = Cli::parse();

    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let manager = ContainerManager::new(
        args.bind,
        &args.zookeeper,
        &args.zookeeper_env,
        SvcProviderOptions {
            path: std::env::current_exe()
                .unwrap()
                .parent()
                .unwrap()
                .join("svcprovider"),
            args: args.svcprovider_args,
        },
    )
    .await
    .unwrap();

    let http_client = hyper::Client::new();

    let app = app()
        .layer(axum_tracing_opentelemetry::middleware::OtelAxumLayer::default())
        .with_state((manager, http_client))
        .layer(OtelAxumMetricsLayer::new())
        .route("/healthz", get(|| async { (StatusCode::OK, "OK") }))
        .layer(
            ServiceBuilder::new()
                .layer(NewSentryLayer::new_from_top())
                .layer(SentryHttpLayer::with_transaction()),
        );

    Ok(
        axum::Server::bind(&SocketAddr::from((args.bind, BACKEND_PORT)))
            .serve(app.into_make_service())
            .await?,
    )
}
