use anyhow::{anyhow, Context, Result};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::Json;
use clap::Parser;
use rand::seq::SliceRandom as _;
use sentry::integrations::tower::{NewSentryLayer, SentryHttpLayer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::str::FromStr as _;
use std::sync::Arc;
use tower::ServiceBuilder;
use tracing::instrument;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use uuid::Uuid;

use bismuth_common::{
    init_sentry, init_tracer, pack_backends, unpack_backends, ApiError, Backend, ContainerState,
    FunctionDefinition, BACKEND_PORT,
};

pub struct ControlPlaneState {
    pub zookeeper: String,
    pub zookeeper_env: String,
    pub http_client: hyper::client::Client<hyper::client::HttpConnector, hyper::Body>,
    pub hosted_db: sqlx::postgres::PgPool,
}

impl ControlPlaneState {
    pub async fn zk(&self) -> Result<zookeeper_client::Client> {
        let zk = zookeeper_client::Client::connect(&self.zookeeper)
            .await
            .context("Error connecting to ZooKeeper")?;
        let zk = zk
            .chroot(format!("/{}", &self.zookeeper_env))
            .map_err(|_| anyhow!("Failed to chroot to env {}", &self.zookeeper_env))?;
        Ok(zk)
    }
}

#[derive(Serialize, Debug)]
struct BackendStatus {
    backend: Backend,
    status: ContainerState,
}

#[derive(Serialize, Debug)]
struct FunctionStatus {
    definition: FunctionDefinition,
    backends: Vec<BackendStatus>,
}

pub async fn pick_backend(zk: &zookeeper_client::Client) -> Result<Backend> {
    let container_id = Uuid::new_v4();

    let nodes: Vec<Ipv4Addr> = zk
        .get_children("/node")
        .await
        .context("Error listing nodes")?
        .0
        .iter()
        .map(|ip| Ipv4Addr::from_str(&ip).unwrap())
        .collect();

    // TODO: consistent hash here as well?
    let node = nodes.choose(&mut rand::thread_rng()).unwrap();
    let backend = Backend {
        ip: *node,
        container_id,
    };

    Ok(backend)
}

#[instrument(skip(state))]
#[axum::debug_handler]
async fn function_status(
    State(state): State<Arc<ControlPlaneState>>,
    Path(function_id): Path<Uuid>,
) -> Result<Json<FunctionStatus>, ApiError> {
    let zk = state.zk().await?;
    let function_definition: FunctionDefinition = serde_json::from_slice(
        &zk.get_data(&format!("/function/{}", &function_id))
            .await
            .map_err(|e| {
                if e == zookeeper_client::Error::NoNode {
                    ApiError::NotFound
                } else {
                    ApiError::Error(e.into())
                }
            })?
            .0,
    )?;

    let function_backends_key = format!("/function/{}/backends", &function_id);
    let (function_backends_raw, _) = zk
        .get_data(&function_backends_key)
        .await
        .context("Error getting function backends")?;
    let backends = unpack_backends(&function_backends_raw)?;

    let mut reader = zk.new_multi_reader();
    for backend in &backends {
        reader.add_get_data(&format!(
            "/node/{}/container/{}/status",
            backend.ip, backend.container_id
        ))?;
    }
    let statuses = reader
        .commit()
        .await
        .context("Error getting container statuses")?;

    Ok(FunctionStatus {
        definition: function_definition,
        backends: backends
            .iter()
            .zip(statuses)
            .map(|(backend, status)| {
                let zookeeper_client::MultiReadResult::Data { data, stat: _ } = status else {
                    unreachable!()
                };
                BackendStatus {
                    backend: backend.clone(),
                    status: unsafe { std::mem::transmute::<u8, ContainerState>(data[0]) },
                }
            })
            .collect(),
    }
    .into())
}

#[derive(Deserialize)]
struct LogsParams {
    follow: Option<bool>,
}

#[instrument(skip(state, params))]
#[axum::debug_handler]
async fn function_logs(
    State(state): State<Arc<ControlPlaneState>>,
    Path(function_id): Path<Uuid>,
    Query(params): Query<LogsParams>,
) -> Result<axum::response::Response<hyper::Body>, ApiError> {
    tracing::Span::current().record("follow", params.follow.unwrap_or_default());
    let zk = state.zk().await?;
    let function_backends_key = format!("/function/{}/backends", &function_id);
    let (function_backends_raw, _) = zk
        .get_data(&function_backends_key)
        .await
        .context("Error getting function backends")?;
    let backends = unpack_backends(&function_backends_raw)?;
    if backends.is_empty() {
        return Err(ApiError::Status(StatusCode::SERVICE_UNAVAILABLE));
    }
    // TODO
    let backend = backends.first().unwrap();

    Ok(state
        .http_client
        .get(
            format!(
                "http://{}:{}/logs/{}?follow={}",
                backend.ip,
                BACKEND_PORT,
                backend.container_id,
                params.follow.unwrap_or(false)
            )
            .parse()?,
        )
        .await?)
}

#[instrument(skip(state))]
#[axum::debug_handler]
async fn function_create(
    State(state): State<Arc<ControlPlaneState>>,
    Json(new_definition): Json<FunctionDefinition>,
) -> Result<Json<HashMap<String, String>>, ApiError> {
    let zk = state.zk().await?;
    let function_id = Uuid::new_v4();

    let mut multi = zk.new_multi_writer();

    let backend = pick_backend(&zk).await?;

    multi.add_create(
        &format!("/function/{}", &function_id),
        &serde_json::to_vec(&new_definition)?,
        &zookeeper_client::CreateMode::Persistent.with_acls(zookeeper_client::Acls::anyone_all()),
    )?;

    multi.add_create(
        &format!("/function/{}/backends", &function_id),
        &pack_backends(&[backend.clone()]),
        &zookeeper_client::CreateMode::Persistent.with_acls(zookeeper_client::Acls::anyone_all()),
    )?;

    multi.add_create(
        &format!("/node/{}/container/{}", &backend.ip, &backend.container_id),
        function_id.as_bytes(),
        &zookeeper_client::CreateMode::Persistent.with_acls(zookeeper_client::Acls::anyone_all()),
    )?;
    multi.add_create(
        &format!(
            "/node/{}/container/{}/status",
            &backend.ip, &backend.container_id
        ),
        &[ContainerState::Starting as u8],
        &zookeeper_client::CreateMode::Persistent.with_acls(zookeeper_client::Acls::anyone_all()),
    )?;

    for route in &new_definition.routes {
        multi.add_create(
            &format!("/route/{}", route.hostname),
            function_id.as_bytes(),
            &zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all()),
        )?;
    }

    sqlx::raw_sql(&format!(
        r#"
CREATE ROLE "{0}_db" NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOLOGIN;
CREATE ROLE "{0}_user" NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN ENCRYPTED PASSWORD '{0}';
GRANT "{0}_db" TO "{0}_user";
        "#,
        function_id.as_simple().to_string()
    ))
    .execute(&state.hosted_db)
    .await
    .context("Error creating function hosted DB")?;

    sqlx::raw_sql(&format!(
        r#"
CREATE DATABASE "{0}_db" WITH OWNER="{0}_user";
        "#,
        function_id.as_simple().to_string()
    ))
    .execute(&state.hosted_db)
    .await
    .context("Error creating function hosted DB")?;

    sqlx::raw_sql(&format!(
        r#"
REVOKE ALL ON DATABASE "{0}_db" FROM public;
        "#,
        function_id.as_simple().to_string()
    ))
    .execute(&state.hosted_db)
    .await
    .context("Error creating function hosted DB")?;

    let connect_opts = (*state.hosted_db.connect_options())
        .clone()
        .database(&format!("{}_db", function_id.as_simple().to_string()));
    let new_db = sqlx::PgPool::connect_with(connect_opts)
        .await
        .context("Error connecting to function hosted DB")?;
    sqlx::raw_sql(&format!(
        r#"
GRANT ALL ON SCHEMA public TO "{0}_user" WITH GRANT OPTION;
        "#,
        function_id.as_simple().to_string()
    ))
    .execute(&new_db)
    .await
    .context("Error granting permissions to function hosted DB")?;

    multi
        .commit()
        .await
        .context("Error creating function znodes")?;

    let mut res = HashMap::new();
    res.insert("id".to_string(), function_id.to_string());
    Ok(Json(res))
}

#[instrument(skip(state))]
#[axum::debug_handler]
async fn function_update(
    State(state): State<Arc<ControlPlaneState>>,
    Path(function_id): Path<Uuid>,
    new_definition: Option<Json<FunctionDefinition>>,
) -> Result<Json<FunctionStatus>, ApiError> {
    let zk = state.zk().await?;

    {
        let old_definition: FunctionDefinition = serde_json::from_slice(
            &zk.get_data(&format!("/function/{}", &function_id))
                .await
                .map_err(|e| {
                    if e == zookeeper_client::Error::NoNode {
                        ApiError::NotFound
                    } else {
                        ApiError::Error(e.into())
                    }
                })?
                .0,
        )?;

        // Get current backends
        let function_backends_key = format!("/function/{}/backends", &function_id);
        let (function_backends_raw, functions_backends_stat) = zk
            .get_data(&function_backends_key)
            .await
            .context("Error getting function backends")?;

        let new_backend = pick_backend(&zk).await?;

        let mut multi = zk.new_multi_writer();

        // No body means just force redeploy (e.g. to update cloned code)
        if let Some(Json(new_definition)) = new_definition {
            multi.add_set_data(
                &format!("/function/{}", &function_id),
                &serde_json::to_vec(&new_definition)?,
                None,
            )?;

            for route in &old_definition.routes {
                multi.add_delete(&format!("/route/{}", route.hostname), None)?;
            }

            for route in &new_definition.routes {
                multi.add_create(
                    &format!("/route/{}", route.hostname),
                    function_id.as_bytes(),
                    &zookeeper_client::CreateMode::Persistent
                        .with_acls(zookeeper_client::Acls::anyone_all()),
                )?;
            }
        }

        // Clear the function's backend list
        multi.add_set_data(
            &format!("/function/{}/backends", &function_id),
            &pack_backends(&[new_backend.clone()]),
            Some(functions_backends_stat.version),
        )?;

        // And remove each container/backend
        for backend in unpack_backends(&function_backends_raw)? {
            multi.add_delete(
                &format!(
                    "/node/{}/container/{}/status",
                    backend.ip, backend.container_id
                ),
                None,
            )?;
            multi.add_delete(
                &format!("/node/{}/container/{}", backend.ip, backend.container_id),
                None,
            )?;
        }

        // And add the new backend
        multi.add_create(
            &format!(
                "/node/{}/container/{}",
                &new_backend.ip, &new_backend.container_id
            ),
            function_id.as_bytes(),
            &zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all()),
        )?;
        multi.add_create(
            &format!(
                "/node/{}/container/{}/status",
                &new_backend.ip, &new_backend.container_id
            ),
            &[ContainerState::Starting as u8],
            &zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all()),
        )?;

        multi.commit().await.context("Error updating function")?;
    }

    function_status(State(state), Path(function_id)).await
}

#[instrument(skip(state))]
#[axum::debug_handler]
async fn function_delete(
    State(state): State<Arc<ControlPlaneState>>,
    Path(function_id): Path<Uuid>,
) -> Result<(), ApiError> {
    let zk = state.zk().await?;

    let function_definition: FunctionDefinition = serde_json::from_slice(
        &zk.get_data(&format!("/function/{}", &function_id))
            .await
            .map_err(|e| {
                if e == zookeeper_client::Error::NoNode {
                    ApiError::NotFound
                } else {
                    ApiError::Error(e.into())
                }
            })?
            .0,
    )?;

    // Get current backends
    let function_backends_key = format!("/function/{}/backends", &function_id);
    let (function_backends_raw, functions_backends_stat) = zk
        .get_data(&function_backends_key)
        .await
        .context("Error getting function backends")?;

    let mut multi = zk.new_multi_writer();

    // Delete the function's backend list
    multi.add_delete(
        &format!("/function/{}/backends", &function_id),
        Some(functions_backends_stat.version),
    )?;

    multi.add_delete(&format!("/function/{}", &function_id), None)?;

    // And remove each container/backend
    for backend in unpack_backends(&function_backends_raw)? {
        multi.add_delete(
            &format!(
                "/node/{}/container/{}/status",
                backend.ip, backend.container_id
            ),
            None,
        )?;
        multi.add_delete(
            &format!("/node/{}/container/{}", backend.ip, backend.container_id),
            None,
        )?;
    }

    for route in &function_definition.routes {
        multi.add_delete(&format!("/route/{}", route.hostname), None)?;
    }

    multi.commit().await.context("Error deleting function")?;

    sqlx::raw_sql(&format!(
        r#"
DROP DATABASE "{0}_db";
        "#,
        function_id.as_simple().to_string()
    ))
    .execute(&state.hosted_db)
    .await
    .context("Error deleting hosted DB")?;

    sqlx::raw_sql(&format!(
        r#"
DROP ROLE "{0}_db";
DROP ROLE "{0}_user";
        "#,
        function_id.as_simple().to_string()
    ))
    .execute(&state.hosted_db)
    .await
    .context("Error deleting hosted DB roles")?;

    Ok(())
}

pub fn app() -> axum::Router<Arc<ControlPlaneState>> {
    axum::Router::new()
        .route("/function", post(function_create))
        .route(
            "/function/:function_id",
            get(function_status)
                .put(function_update)
                .delete(function_delete),
        )
        .route("/function/:function_id/logs", get(function_logs))
}

/// FaaS API (controlplane)
#[derive(Debug, Parser)]
#[clap(name = "faas-api", version)]
struct Cli {
    /// ZooKeeper IP:port
    #[clap(long, global = true, default_value = "127.0.0.1:2181")]
    zookeeper: String,

    /// ZooKeeper environment name (e.g. "dev", "test", "default")
    #[clap(long, global = true, default_value = "default")]
    zookeeper_env: String,

    /// Bind IP:port
    #[clap(long, global = true, default_value = "0.0.0.0:8002")]
    bind: SocketAddrV4,

    /// Postgres URI for the server hosting the managed DB service
    #[clap(long)]
    hosted_db: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _sentry_guard = init_sentry();
    let tracer = init_tracer("faas-api")?;

    let args = Cli::parse();

    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let http_client = hyper::Client::new();

    let hosted_db = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .connect_lazy(&args.hosted_db)?;

    let app = app()
        .layer(axum_tracing_opentelemetry::middleware::OtelAxumLayer::default())
        .route("/healthz", get(|| async { (StatusCode::OK, "OK") }))
        .with_state(Arc::new(ControlPlaneState {
            zookeeper: args.zookeeper,
            zookeeper_env: args.zookeeper_env,
            http_client,
            hosted_db,
        }))
        .layer(
            ServiceBuilder::new()
                .layer(NewSentryLayer::new_from_top())
                .layer(SentryHttpLayer::with_transaction()),
        );

    Ok(axum::Server::bind(&SocketAddr::from(args.bind))
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await?)
}
