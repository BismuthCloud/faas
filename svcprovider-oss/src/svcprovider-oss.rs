use anyhow::Result;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use bismuth_common::init_tracer;
use clap::Parser;
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};
use tracing::{event, instrument, Level};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

/// svcprovider
#[derive(Debug, Parser)]
#[clap(name = "svcprovider", version)]
struct Cli {
    /// Bind IP
    #[clap(long)]
    bind: Ipv4Addr,

    /// Function ID
    #[clap(long)]
    function: Uuid,

    /// Auth token
    #[clap(long)]
    auth_token: String,
}

struct SVCProviderState {
    function_id: Uuid,
    config: aws_config::SdkConfig,
    dynamodb: aws_sdk_dynamodb::Client,
    s3: aws_sdk_s3::Client,
    bucket: String,
    secretsmanager: aws_sdk_secretsmanager::Client,
}

impl SVCProviderState {
    async fn init_resources(&self) -> Result<()> {
        if let Err(e) = self
            .dynamodb
            .create_table()
            .table_name(self.function_id)
            .attribute_definitions(
                aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .attribute_name("k")
                    .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                    .build()?,
            )
            .key_schema(
                aws_sdk_dynamodb::types::KeySchemaElement::builder()
                    .attribute_name("k")
                    .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                    .build()?,
            )
            .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
            .send()
            .await
        {
            if !e
                .as_service_error()
                .map_or(false, |se| se.is_resource_in_use_exception())
            {
                return Err(e.into());
            }
        }

        if let Err(e) = self.s3.create_bucket().bucket(&self.bucket).send().await {
            if !e
                .as_service_error()
                .map_or(false, |se| se.is_bucket_already_owned_by_you())
            {
                return Err(e.into());
            }
        }

        Ok(())
    }
}

#[instrument(skip(shared_state))]
#[axum::debug_handler]
async fn kv_get(
    State(shared_state): State<Arc<SVCProviderState>>,
    Path(key): Path<String>,
) -> Result<impl IntoResponse, StatusCode> {
    let resp = shared_state
        .dynamodb
        .get_item()
        .table_name(shared_state.function_id)
        .key("k", aws_sdk_dynamodb::types::AttributeValue::S(key))
        .send()
        .await
        .map_err(|e| {
            event!(Level::ERROR, "Failed to get key: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    match resp.item() {
        Some(value) => Ok(base64::decode(value["v"].as_s().unwrap()).unwrap()),
        None => Err(StatusCode::NOT_FOUND),
    }
}

#[instrument(skip(shared_state))]
#[axum::debug_handler]
async fn kv_set(
    State(shared_state): State<Arc<SVCProviderState>>,
    Path(key): Path<String>,
    value: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    match shared_state
        .dynamodb
        .put_item()
        .table_name(shared_state.function_id)
        .item("k", aws_sdk_dynamodb::types::AttributeValue::S(key))
        .item(
            "v",
            aws_sdk_dynamodb::types::AttributeValue::S(base64::encode(value)),
        )
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            event!(Level::ERROR, "Failed to set key: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[instrument(skip(shared_state))]
#[axum::debug_handler]
async fn kv_delete(
    State(shared_state): State<Arc<SVCProviderState>>,
    Path(key): Path<String>,
) -> Result<impl IntoResponse, StatusCode> {
    match shared_state
        .dynamodb
        .delete_item()
        .table_name(shared_state.function_id)
        .key("k", aws_sdk_dynamodb::types::AttributeValue::S(key))
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            event!(Level::ERROR, "Failed to delete key: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[instrument(skip(shared_state))]
#[axum::debug_handler]
async fn secret_get(
    State(shared_state): State<Arc<SVCProviderState>>,
    Path(key): Path<String>,
) -> Result<impl IntoResponse, StatusCode> {
    let resp = shared_state
        .secretsmanager
        .get_secret_value()
        .secret_id(format!("{}-{}", shared_state.function_id, key))
        .send()
        .await
        .map_err(|e| {
            event!(Level::ERROR, "Failed to get secret: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    match resp.secret_string() {
        Some(value) => Ok(value.to_string()),
        None => Err(StatusCode::NOT_FOUND),
    }
}

// Blobs
#[instrument(skip(shared_state))]
#[axum::debug_handler]
async fn blob_get(
    State(shared_state): State<Arc<SVCProviderState>>,
    Path(key): Path<String>,
) -> Result<impl IntoResponse, StatusCode> {
    let resp = shared_state
        .s3
        .get_object()
        .bucket(&shared_state.bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| {
            if e.as_service_error().map_or(false, |se| se.is_no_such_key()) {
                return StatusCode::NOT_FOUND;
            }
            event!(Level::ERROR, "Failed to get blob: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(resp
        .body
        .collect()
        .await
        .map_err(|e| {
            event!(Level::ERROR, "Failed to get blob: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .into_bytes())
}

#[instrument(skip(shared_state, body))]
#[axum::debug_handler]
async fn blob_create(
    State(shared_state): State<Arc<SVCProviderState>>,
    Path(key): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    match shared_state
        .s3
        .head_object()
        .bucket(&shared_state.bucket)
        .key(&key)
        .send()
        .await
    {
        Ok(_) => Err(StatusCode::CONFLICT),
        Err(e) => {
            if e.as_service_error().map_or(false, |se| se.is_not_found()) {
                match shared_state
                    .s3
                    .put_object()
                    .bucket(&shared_state.bucket)
                    .key(key)
                    .body(aws_sdk_s3::primitives::ByteStream::from(body))
                    .send()
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        event!(Level::ERROR, "Failed to create blob: {:?}", e);
                        Err(StatusCode::INTERNAL_SERVER_ERROR)
                    }
                }
            } else {
                event!(Level::ERROR, "Failed to create blob: {:?}", e);
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }
}

#[instrument(skip(shared_state, body))]
#[axum::debug_handler]
async fn blob_set(
    State(shared_state): State<Arc<SVCProviderState>>,
    Path(key): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    match shared_state
        .s3
        .head_object()
        .bucket(&shared_state.bucket)
        .key(&key)
        .send()
        .await
    {
        Ok(_) => {
            match shared_state
                .s3
                .put_object()
                .bucket(&shared_state.bucket)
                .key(key)
                .body(aws_sdk_s3::primitives::ByteStream::from(body))
                .send()
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => {
                    event!(Level::ERROR, "Failed to set blob: {:?}", e);
                    Err(StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        }
        Err(e) => {
            if e.as_service_error().map_or(false, |se| se.is_not_found()) {
                Err(StatusCode::NOT_FOUND)
            } else {
                event!(Level::ERROR, "Failed to set blob: {:?}", e);
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }
}

#[instrument(skip(shared_state))]
#[axum::debug_handler]
async fn blob_delete(
    State(shared_state): State<Arc<SVCProviderState>>,
    Path(key): Path<String>,
) -> Result<impl IntoResponse, StatusCode> {
    match shared_state
        .s3
        .delete_object()
        .bucket(&shared_state.bucket)
        .key(key)
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            event!(Level::ERROR, "Failed to delete blob: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[instrument(skip(shared_state))]
#[axum::debug_handler]
async fn blob_list(
    State(shared_state): State<Arc<SVCProviderState>>,
) -> Result<impl IntoResponse, StatusCode> {
    let resp = shared_state
        .s3
        .list_objects_v2()
        .bucket(&shared_state.bucket)
        .send()
        .await
        .map_err(|e| {
            event!(Level::ERROR, "Failed to list blobs: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let mut all_kvs = HashMap::new();
    for obj in resp.contents() {
        let key = obj.key.as_ref().unwrap().clone();
        let value = shared_state
            .s3
            .get_object()
            .bucket(&shared_state.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                event!(Level::ERROR, "Failed to list blobs: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
            .body
            .collect()
            .await
            .map_err(|e| {
                event!(Level::ERROR, "Failed to list blobs: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
            .to_vec();
        all_kvs.insert(key, value);
    }
    Ok(axum::Json(all_kvs))
}

fn app() -> axum::Router<Arc<SVCProviderState>> {
    Router::new()
        .route("/kv/v1/:key", get(kv_get).post(kv_set).delete(kv_delete))
        .route("/secrets/v1/:key", get(secret_get))
        .route("/blob/v1/", get(blob_list))
        .route(
            "/blob/v1/:key",
            get(blob_get)
                .post(blob_create)
                .put(blob_set)
                .delete(blob_delete),
        )
        .layer(axum_tracing_opentelemetry::middleware::OtelAxumLayer::default())
        .route("/healthz", get(|| async { "OK" }))
}

#[tokio::main]
async fn main() -> Result<()> {
    let tracer = init_tracer("svcprovider")?;

    let args = Cli::parse();

    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "svcprovider=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = aws_config::load_from_env().await;
    let shared_state = Arc::new(SVCProviderState {
        function_id: args.function,
        dynamodb: aws_sdk_dynamodb::Client::new(&config),
        s3: aws_sdk_s3::Client::new(&config),
        bucket: format!("bismuth-{}", args.function),
        secretsmanager: aws_sdk_secretsmanager::Client::new(&config),
        config,
    });

    shared_state.init_resources().await?;

    let app = app().with_state(shared_state.clone()).route_layer(
        tower_http::validate_request::ValidateRequestHeaderLayer::bearer(&args.auth_token),
    );

    Ok(axum::Server::bind(&SocketAddr::from((args.bind, 9000)))
        .serve(app.into_make_service())
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, BoxBody, HttpBody},
        http::{self, Request, StatusCode},
    };
    use serde_json::json;
    use std::net::ToSocketAddrs;
    use tower::{Service, ServiceExt};
    use tracing_test::traced_test;

    /// Create a test app with a shared state referencing localstack.
    macro_rules! test_app {
        () => {{
            // Setting endpoint_url directly to host.docker.internal:4566 doesn't work.
            let sa = "host.docker.internal:4566"
                .to_socket_addrs()
                .unwrap()
                .next()
                .unwrap();
            let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .test_credentials()
                .region("us-east-1")
                .endpoint_url(format!("http://{}:{}/", sa.ip(), sa.port()))
                .load()
                .await;
            let function_id = Uuid::new_v4();
            let shared_state = Arc::new(SVCProviderState {
                function_id,
                dynamodb: aws_sdk_dynamodb::Client::new(&config),
                s3: aws_sdk_s3::Client::new(&config),
                bucket: format!("bismuth-{}", function_id),
                secretsmanager: aws_sdk_secretsmanager::Client::new(&config),
                config,
            });
            shared_state.init_resources().await.unwrap();

            app().with_state(shared_state)
        }};
    }

    async fn make_request(
        app: &mut axum::routing::Router,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> http::Response<BoxBody> {
        let request = Request::builder().method(method).uri(path);
        let request = match body {
            Some(body) => request.body(Body::from(body.to_vec())).unwrap(),
            None => request.body(Body::empty()).unwrap(),
        };

        app.ready().await.unwrap().call(request).await.unwrap()
    }

    #[traced_test]
    #[tokio::test]
    async fn test_kv() {
        let mut app = test_app!();

        let response = make_request(&mut app, "GET", "/kv/v1/key", None).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = make_request(&mut app, "POST", "/kv/v1/key", Some(b"value")).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = make_request(&mut app, "GET", "/kv/v1/key", None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            b"value".as_slice()
        );

        let response = make_request(&mut app, "POST", "/kv/v1/key", Some(b"value2")).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = make_request(&mut app, "GET", "/kv/v1/key", None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            b"value2".as_slice()
        );

        let response = make_request(&mut app, "DELETE", "/kv/v1/key", None).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = make_request(&mut app, "GET", "/kv/v1/key", None).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_blob_ops() {
        let mut app = test_app!();

        let response = make_request(&mut app, "GET", "/blob/v1/foo", None).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = make_request(&mut app, "PUT", "/blob/v1/foo", Some(b"no put")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = make_request(&mut app, "POST", "/blob/v1/foo", Some(b"foovalue")).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response =
            make_request(&mut app, "POST", "/blob/v1/foo", Some(b"conflict foovalue")).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let response = make_request(&mut app, "GET", "/blob/v1/foo", None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            b"foovalue".as_slice()
        );

        let response = make_request(&mut app, "PUT", "/blob/v1/foo", Some(b"put foovalue")).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = make_request(&mut app, "GET", "/blob/v1/foo", None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            b"put foovalue".as_slice()
        );

        let response = make_request(&mut app, "GET", "/blob/v1/", None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            json!({"foo": b"put foovalue"}).to_string().as_bytes()
        );

        let response = make_request(&mut app, "DELETE", "/blob/v1/foo", None).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = make_request(&mut app, "GET", "/blob/v1/foo", None).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_blob_large_object() {
        let mut app = test_app!();

        let mut bigdata = vec![];
        for i in 0..100_000 {
            bigdata.extend(format!("{} ", i).as_bytes());
        }

        let response = make_request(&mut app, "POST", "/blob/v1/big", Some(&bigdata)).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = make_request(&mut app, "GET", "/blob/v1/big", None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            &bigdata,
        );
    }
}
