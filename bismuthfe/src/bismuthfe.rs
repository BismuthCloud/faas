use anyhow::{anyhow, Context, Result};
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{Request, StatusCode};
use axum::routing::{any, get};
use clap::Parser;
use conhash::ConsistentHash;
use hyper::body::Body;
use sentry::integrations::tower::{NewSentryLayer, SentryHttpLayer};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tower::ServiceBuilder;
use tracing::{event, instrument, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};
use uuid::Uuid;

use bismuth_common::{
    init_metrics, init_sentry, init_tracer, unpack_backends, ApiError, Backend, GenericError,
    OtelAxumMetricsLayer, BACKEND_PORT,
};

const CONHASH_REPLICAS: usize = 20;

/// bismuthfe
#[derive(Debug, Parser)]
#[clap(name = "bismuthfe", version)]
struct Cli {
    /// ZooKeeper IP:port
    #[clap(long, global = true, default_value = "127.0.0.1:2181")]
    zookeeper: String,

    /// ZooKeeper environment name (e.g. "dev", "test", "default")
    #[clap(long, global = true, default_value = "default")]
    zookeeper_env: String,

    /// Bind IP:port
    #[clap(long, global = true, default_value = "0.0.0.0:8000")]
    bind: SocketAddrV4,
}

pub struct BackendMonitor {
    pub backends: RwLock<HashMap<Uuid, ConsistentHash<Backend>>>,
    pub routes: RwLock<HashMap<String, Uuid>>,
}

impl BackendMonitor {
    pub async fn new(zk_cluster: &str, zk_env: &str) -> Result<Arc<Self>> {
        let monitor = Arc::new(Self {
            backends: RwLock::new(HashMap::new()),
            routes: RwLock::new(HashMap::new()),
        });

        let mon_ = monitor.clone();
        let zk_cluster = zk_cluster.to_string();
        let zk_env = zk_env.to_string();

        tokio::spawn(async move {
            loop {
                match Self::watch(mon_.clone(), &zk_cluster, &zk_env).await {
                    Ok(_) => continue, // unreachable
                    Err(e) => {
                        event!(Level::ERROR, error = %e, "Error in watch loop");
                    }
                }
                sleep(std::time::Duration::from_secs(1)).await;
            }
        });

        Ok(monitor)
    }

    async fn watch(mon: Arc<Self>, zk_cluster: &str, zk_env: &str) -> Result<()> {
        let zk = zookeeper_client::Client::connect(&zk_cluster)
            .await
            .context("Error connecting to ZooKeeper")?;
        let zk = zk
            .chroot(format!("/{}", zk_env))
            .map_err(|_| anyhow!("Failed to chroot to env {}", zk_env))?;
        event!(Level::TRACE, "Connected to ZooKeeper");

        let mut func_watcher = zk
            .watch(
                "/function",
                zookeeper_client::AddWatchMode::PersistentRecursive,
            )
            .await?;

        let mut route_watcher = zk
            .watch(
                "/route",
                zookeeper_client::AddWatchMode::PersistentRecursive,
            )
            .await?;

        let functions = zk
            .list_children("/function")
            .await
            .context("Error listing functions")?;

        mon.backends.write().await.clear();
        for function in &functions {
            mon.load_backends(&zk, Uuid::parse_str(function)?).await?;
        }
        event!(
            Level::DEBUG,
            "Loaded backends for {} functions",
            functions.len()
        );

        let routes = zk
            .list_children("/route")
            .await
            .context("Error listing routes")?;

        mon.routes.write().await.clear();
        for route in &routes {
            let target = Uuid::from_slice(
                &zk.get_data(&format!("/route/{}", route))
                    .await
                    .context("Error getting route target")?
                    .0,
            )?;
            mon.routes.write().await.insert(route.clone(), target);
        }
        event!(Level::DEBUG, "Loaded {} routes", routes.len());

        let mon_ = mon.clone();
        let zk_ = zk.clone();
        let func_watch: JoinHandle<Result<(), anyhow::Error>> = tokio::spawn(async move {
            loop {
                let event = func_watcher.changed().await;
                event!(Level::TRACE, "ZooKeeper event: {:?}", event);

                if event.event_type == zookeeper_client::EventType::Session
                    && (event.session_state == zookeeper_client::SessionState::Disconnected
                        || event.session_state == zookeeper_client::SessionState::Expired
                        || event.session_state == zookeeper_client::SessionState::Closed)
                {
                    event!(Level::ERROR, "ZooKeeper session disconnected or terminal");
                    return Err(anyhow!("ZooKeeper session disconnected or terminal"));
                }

                if !event.path.ends_with("/backends") {
                    continue;
                }

                match event.event_type {
                    zookeeper_client::EventType::NodeCreated => {
                        let function = Uuid::parse_str(
                            event
                                .path
                                .split('/')
                                .nth(2)
                                .ok_or(anyhow!("Invalid function znode path"))?,
                        )?;
                        event!(Level::DEBUG, function = %function, "Function created");
                        mon_.load_backends(&zk_, function).await?;
                    }
                    zookeeper_client::EventType::NodeDeleted => {
                        let function = Uuid::parse_str(
                            event
                                .path
                                .split('/')
                                .nth(2)
                                .ok_or(anyhow!("Invalid function znode path"))?,
                        )?;
                        event!(Level::DEBUG, function = %function, "Function deleted");
                        mon_.backends.write().await.remove(&function);
                    }
                    zookeeper_client::EventType::NodeDataChanged => {
                        let function = Uuid::parse_str(
                            event
                                .path
                                .split('/')
                                .nth(2)
                                .ok_or(anyhow!("Invalid function znode path"))?,
                        )?;
                        event!(Level::DEBUG, function = %function, "Function backends updated");
                        mon_.load_backends(&zk_, function).await?;
                    }
                    _ => {
                        event!(Level::WARN, "Unexpected ZooKeeper event: {:?}", event);
                    }
                }
            }
        });

        let mon_ = mon.clone();
        let zk_ = zk.clone();
        let route_watch: JoinHandle<Result<(), anyhow::Error>> = tokio::spawn(async move {
            loop {
                let event = route_watcher.changed().await;
                event!(Level::TRACE, "ZooKeeper event: {:?}", event);

                if event.event_type == zookeeper_client::EventType::Session
                    && (event.session_state == zookeeper_client::SessionState::Disconnected
                        || event.session_state == zookeeper_client::SessionState::Expired
                        || event.session_state == zookeeper_client::SessionState::Closed)
                {
                    event!(Level::ERROR, "ZooKeeper session disconnected or terminal");
                    return Err(anyhow!("ZooKeeper session disconnected or terminal"));
                }

                match event.event_type {
                    zookeeper_client::EventType::NodeCreated => {
                        let route = event
                            .path
                            .split('/')
                            .nth(2)
                            .ok_or(anyhow!("Invalid route znode path"))?
                            .to_string();
                        let target = Uuid::from_slice(
                            &zk_.get_data(&event.path)
                                .await
                                .context("Error getting route target")?
                                .0,
                        )?;
                        event!(Level::DEBUG, route = %route, function = %target, "Route created");
                        mon_.routes.write().await.insert(route, target);
                    }
                    zookeeper_client::EventType::NodeDeleted => {
                        let route = event
                            .path
                            .split('/')
                            .nth(2)
                            .ok_or(anyhow!("Invalid route znode path"))?
                            .to_string();
                        event!(Level::DEBUG, route = %route, "Route deleted");
                        mon_.routes.write().await.remove(&route);
                    }
                    zookeeper_client::EventType::NodeDataChanged => {
                        let route = event
                            .path
                            .split('/')
                            .nth(2)
                            .ok_or(anyhow!("Invalid route znode path"))?
                            .to_string();
                        let target = Uuid::from_slice(
                            &zk_.get_data(&event.path)
                                .await
                                .context("Error getting route target")?
                                .0,
                        )?;
                        event!(Level::DEBUG, route = %route, function = %target, "Route created");
                        mon_.routes.write().await.insert(route, target);
                    }
                    _ => {
                        event!(Level::WARN, "Unexpected ZooKeeper event: {:?}", event);
                    }
                }
            }
        });

        let _ = tokio::try_join!(func_watch, route_watch)?;
        Ok(())
    }

    async fn load_backends(&self, zk: &zookeeper_client::Client, function_id: Uuid) -> Result<()> {
        let (backends_raw, _) = zk
            .get_data(&format!("/function/{}/backends", &function_id))
            .await
            .context("Error getting function backends")?;

        let mut hash = ConsistentHash::new();
        for backend in unpack_backends(&backends_raw)? {
            hash.add(&backend, CONHASH_REPLICAS);
        }

        event!(
            Level::TRACE,
            "Updating backends for function {}: old={:?}, new={:?}",
            function_id,
            self.backends
                .read()
                .await
                .get(&function_id)
                .map(|h| h.len() / CONHASH_REPLICAS)
                .unwrap_or(0),
            hash.len() / CONHASH_REPLICAS
        );

        self.backends.write().await.insert(function_id, hash);

        Ok(())
    }

    async fn pick_backend(&self, function_id: &Uuid, peer_ip: &IpAddr) -> Result<Backend> {
        Ok(self
            .backends
            .read()
            .await
            .get(function_id)
            .ok_or(GenericError::NotFound)?
            .get(peer_ip.to_string().as_bytes())
            .map(|b| b.clone())
            .ok_or(GenericError::Unavailable)?)
    }
}

#[instrument(skip(monitor, http_client, req))]
#[axum::debug_handler]
async fn invoke_function(
    State((monitor, http_client)): State<(
        Arc<BackendMonitor>,
        hyper::client::Client<hyper::client::HttpConnector, Body>,
    )>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Result<axum::response::Response<hyper::Body>, ApiError> {
    invoke_function_path(
        State((monitor, http_client)),
        Path("".to_string()),
        ConnectInfo(addr),
        req,
    )
    .await
}

#[instrument(skip(monitor, http_client, req))]
#[axum::debug_handler]
async fn invoke_function_path(
    State((monitor, http_client)): State<(
        Arc<BackendMonitor>,
        hyper::client::Client<hyper::client::HttpConnector, Body>,
    )>,
    Path(reqpath): Path<String>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Result<axum::response::Response<hyper::Body>, ApiError> {
    let function_id = monitor
        .routes
        .read()
        .await
        .get(
            &req.headers()
                .get("Host")
                .ok_or(GenericError::NotFound)?
                .to_str()
                .map_err(|_| GenericError::NotFound)?
                .to_string(),
        )
        .ok_or(GenericError::NotFound)?
        .clone();
    let backend = monitor.pick_backend(&function_id, &addr.ip()).await?;

    let mut req = req;
    *req.uri_mut() = format!(
        "http://{}:{}/invoke/{}/{}?{}",
        backend.ip,
        BACKEND_PORT,
        backend.container_id,
        reqpath,
        req.uri().query().unwrap_or("")
    )
    .parse()?;
    req.headers_mut().insert(
        "SCRIPT_NAME",
        "/".parse().unwrap(),
        //format!("/invoke/{}", function_id).parse().unwrap(),
    );
    let cx = tracing::Span::current().context();
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(
            &cx,
            &mut opentelemetry_http::HeaderInjector(req.headers_mut()),
        )
    });
    Ok(http_client.request(req).await?)
}

pub fn app() -> axum::Router<(
    Arc<BackendMonitor>,
    hyper::client::Client<hyper::client::HttpConnector, Body>,
)> {
    axum::Router::new()
        .route("/", any(invoke_function))
        .route("/*reqpath", any(invoke_function_path))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    let monitor = BackendMonitor::new(&args.zookeeper, &args.zookeeper_env).await?;
    let http_client = hyper::Client::new();

    let app = app()
        .layer(axum_tracing_opentelemetry::middleware::OtelInResponseLayer::default())
        .layer(axum_tracing_opentelemetry::middleware::OtelAxumLayer::default())
        .layer(OtelAxumMetricsLayer::new())
        //.route("/healthz", get(|| async { (StatusCode::OK, "OK") }))
        .with_state((monitor, http_client))
        .layer(
            ServiceBuilder::new()
                .layer(NewSentryLayer::new_from_top())
                .layer(SentryHttpLayer::with_transaction()),
        );

    Ok(axum::Server::bind(&SocketAddr::from(args.bind))
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await?)
}

#[cfg(test)]
mod tests {
    use tokio::time::sleep;

    use super::*;
    use std::net::Ipv4Addr;

    use bismuth_common::pack_backends;

    // Equivalent of C's __func__
    // https://stackoverflow.com/a/40234666
    macro_rules! function {
        () => {{
            fn f() {}
            fn type_name_of<T>(_: T) -> &'static str {
                std::any::type_name::<T>()
            }
            let name = type_name_of(f);
            name.strip_suffix("::f").unwrap()
        }};
    }

    #[tokio::test]
    async fn test_backend_monitor() {
        let zookeeper_cluster =
            std::env::var("ZOOKEEPER_CLUSTER").unwrap_or("zookeeper1:2181".to_string());

        let env = function!();
        let zk = bismuth_common::test::zk_bootstrap(&zookeeper_cluster, &env).await;

        let monitor = BackendMonitor::new(&zookeeper_cluster, env).await.unwrap();
        assert_eq!(monitor.backends.read().await.len(), 0);

        let function_id = Uuid::new_v4();

        zk.create(
            &format!("/function/{}", function_id),
            &b""[..],
            &zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all()),
        )
        .await
        .unwrap();
        let (stat, _) = zk
            .create(
                &format!("/function/{}/backends", function_id),
                &b""[..],
                &zookeeper_client::CreateMode::Persistent
                    .with_acls(zookeeper_client::Acls::anyone_all()),
            )
            .await
            .unwrap();

        sleep(std::time::Duration::from_millis(10)).await;
        {
            let backends = monitor.backends.read().await;
            assert_eq!(backends.len(), 1);
            assert!(backends.contains_key(&function_id));
            assert_eq!(backends.get(&function_id).unwrap().len(), 0);
        }

        zk.set_data(
            &format!("/function/{}/backends", function_id),
            &pack_backends(&[Backend {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                container_id: Uuid::new_v4(),
            }]),
            Some(stat.version),
        )
        .await
        .unwrap();

        sleep(std::time::Duration::from_millis(10)).await;
        {
            let backends = monitor.backends.read().await;
            assert_eq!(backends.len(), 1);
            assert!(backends.contains_key(&function_id));
            assert_eq!(backends.get(&function_id).unwrap().len(), CONHASH_REPLICAS);
        }

        bismuth_common::test::delete_all(&zk, &format!("/function/{}", function_id))
            .await
            .unwrap();
        sleep(std::time::Duration::from_millis(10)).await;
        {
            let backends = monitor.backends.read().await;
            assert_eq!(backends.len(), 0);
        }
    }
}
