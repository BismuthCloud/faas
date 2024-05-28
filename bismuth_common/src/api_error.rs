use axum::http::StatusCode;
use axum::response::IntoResponse;
use sentry::integrations::anyhow::capture_anyhow;

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
