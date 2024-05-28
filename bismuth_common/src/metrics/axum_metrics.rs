use axum::http::Response;
use axum::{extract::MatchedPath, http::Request};
use opentelemetry::metrics::Counter;
use opentelemetry::KeyValue;
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};

#[derive(Clone)]
struct Metrics {
    requests_total: Counter<u64>,
}

#[derive(Clone)]
pub struct OtelAxumMetricsLayer {
    metrics: Metrics,
}

impl OtelAxumMetricsLayer {
    pub fn new() -> Self {
        let meter = opentelemetry::global::meter("axum");
        let requests_total = meter
            .u64_counter("requests")
            .with_description("Total number of HTTP requests")
            .init();
        Self {
            metrics: Metrics { requests_total },
        }
    }
}

impl<S> Layer<S> for OtelAxumMetricsLayer {
    type Service = OtelAxumMetricsService<S>;

    fn layer(&self, service: S) -> Self::Service {
        OtelAxumMetricsService {
            service,
            metrics: self.metrics.clone(),
        }
    }
}

#[derive(Clone)]
pub struct OtelAxumMetricsService<S> {
    service: S,
    metrics: Metrics,
}

pin_project! {
    pub struct ResponseFuture<F> {
        #[pin]
        inner: F,
        metrics: Metrics,
        method: String,
        path: String,
    }
}

impl<S, ReqB, ResB> Service<Request<ReqB>> for OtelAxumMetricsService<S>
where
    S: Service<Request<ReqB>, Response = Response<ResB>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqB>) -> Self::Future {
        let method = req.method().clone().to_string();
        let path = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
            matched_path.as_str().to_owned()
        } else {
            "".to_owned()
        };

        ResponseFuture {
            inner: self.service.call(req),
            metrics: self.metrics.clone(),
            method,
            path,
        }
    }
}

impl<Fut, B, E> Future for ResponseFuture<Fut>
where
    Fut: Future<Output = Result<Response<B>, E>>,
{
    type Output = Result<Response<B>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = futures_util::ready!(this.inner.poll(cx))?;
        let attrs = [
            KeyValue::new("http.request.method", this.method.clone()),
            KeyValue::new("http.route", this.path.clone()),
            KeyValue::new(
                "http.response.status_code",
                response.status().as_u16().to_string(),
            ),
        ];
        this.metrics.requests_total.add(1, &attrs);
        Poll::Ready(Ok(response))
    }
}
