//! Serving Layer module.
//!
//! The Serving Layer owns HTTP content negotiation, origin proxying, and future
//! agent-facing protocol endpoints. This T1.1 skeleton implements the
//! standalone reverse-proxy shape only. It does not render per request, does
//! not run the conversion pipeline, does not call the Inducer, and only performs
//! the origin network call required by the reverse proxy deployment shape.

#![forbid(unsafe_code)]

use axum::body::Body;
use axum::extract::State;
use axum::http::header::{HeaderName, CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, Request, Response, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use http_body_util::Limited;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::timeout;

const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAX_BODY_BYTES: u64 = 10 * 1024 * 1024;

/// Runtime configuration for the reverse proxy and admin server.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GatewayConfig {
    /// Origin base URL that receives proxied browser requests.
    pub origin_url: String,
    /// Public listener for browser traffic.
    pub listen_addr: SocketAddr,
    /// Separate owner-local admin listener.
    pub admin_addr: SocketAddr,
    /// Maximum time to wait for the upstream response headers.
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    /// Maximum request body size accepted by the Gateway.
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: u64,
}

impl GatewayConfig {
    /// Validates deployment configuration before any listener starts.
    pub fn validate(&self) -> Result<ValidatedConfig, ServingError> {
        let origin_uri =
            Uri::from_str(&self.origin_url).map_err(|_| ServingError::InvalidOriginUrl)?;
        let scheme = origin_uri
            .scheme_str()
            .ok_or(ServingError::InvalidOriginUrl)?;
        if scheme != "http" {
            return Err(ServingError::UnsupportedOriginScheme);
        }
        let authority = origin_uri
            .authority()
            .ok_or(ServingError::InvalidOriginUrl)?
            .clone();
        if self.request_timeout_ms == 0 {
            return Err(ServingError::InvalidTimeout);
        }
        if self.max_body_bytes == 0 {
            return Err(ServingError::InvalidBodyLimit);
        }

        Ok(ValidatedConfig {
            origin_scheme: scheme.to_owned(),
            origin_authority: authority.to_string(),
            listen_addr: self.listen_addr,
            admin_addr: self.admin_addr,
            request_timeout: Duration::from_millis(self.request_timeout_ms),
            max_body_bytes: self.max_body_bytes,
        })
    }
}

/// Validated deployment configuration used by serving tasks.
#[derive(Clone, Debug)]
pub struct ValidatedConfig {
    origin_scheme: String,
    origin_authority: String,
    listen_addr: SocketAddr,
    admin_addr: SocketAddr,
    request_timeout: Duration,
    max_body_bytes: u64,
}

/// Errors raised by the Serving Layer.
#[derive(Debug, Error)]
pub enum ServingError {
    /// The configured origin URL is missing a required URI component.
    #[error("origin_url must be an absolute URL with scheme and authority")]
    InvalidOriginUrl,
    /// Only HTTP origins are supported by the T1.1 reverse-proxy skeleton.
    #[error("origin_url must use http for T1.1")]
    UnsupportedOriginScheme,
    /// The configured request timeout must be greater than zero.
    #[error("request_timeout_ms must be greater than zero")]
    InvalidTimeout,
    /// The configured body-size limit must be greater than zero.
    #[error("max_body_bytes must be greater than zero")]
    InvalidBodyLimit,
    /// A listener could not bind before startup completed.
    #[error("listener bind failed")]
    BindFailed(#[source] std::io::Error),
    /// The public proxy server failed.
    #[error("proxy server failed")]
    ProxyServerFailed(#[source] std::io::Error),
    /// The admin server failed.
    #[error("admin server failed")]
    AdminServerFailed(#[source] std::io::Error),
}

/// Owner-local counters exported by the admin server.
#[derive(Debug, Default)]
pub struct Metrics {
    requests_total: AtomicU64,
    upstream_errors_total: AtomicU64,
}

impl Metrics {
    /// Increments the proxied request counter.
    pub fn increment_requests_total(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the upstream error counter.
    pub fn increment_upstream_errors_total(&self) {
        self.upstream_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Renders the metrics endpoint body.
    pub fn render(&self) -> String {
        let requests_total = self.requests_total.load(Ordering::Relaxed);
        let upstream_errors_total = self.upstream_errors_total.load(Ordering::Relaxed);
        format!(
            "requests_total {}\nupstream_errors_total {}\n",
            requests_total, upstream_errors_total
        )
    }
}

#[derive(Clone)]
struct ProxyState {
    config: ValidatedConfig,
    client: Client<HttpConnector, Body>,
    metrics: Arc<Metrics>,
}

/// Runs the public proxy listener and separate admin listener until shutdown.
pub async fn serve_until_shutdown(
    config: GatewayConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServingError> {
    let config = config.validate()?;
    let public_listener = TcpListener::bind(config.listen_addr)
        .await
        .map_err(ServingError::BindFailed)?;
    let admin_listener = TcpListener::bind(config.admin_addr)
        .await
        .map_err(ServingError::BindFailed)?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown.await;
        let send_result = shutdown_tx.send(true);
        if send_result.is_err() {
            // Both servers have already stopped.
        }
    });

    let metrics = Arc::new(Metrics::default());
    let state = Arc::new(ProxyState {
        config,
        client: build_client(),
        metrics,
    });

    let proxy_app = proxy_router(state.clone());
    let admin_app = admin_router(state.metrics.clone());

    let proxy_shutdown = shutdown_signal(shutdown_rx.clone());
    let admin_shutdown = shutdown_signal(shutdown_rx);

    let proxy = axum::serve(public_listener, proxy_app).with_graceful_shutdown(proxy_shutdown);
    let admin = axum::serve(admin_listener, admin_app).with_graceful_shutdown(admin_shutdown);

    tokio::try_join!(
        async { proxy.await.map_err(ServingError::ProxyServerFailed) },
        async { admin.await.map_err(ServingError::AdminServerFailed) }
    )?;

    Ok(())
}

fn build_client() -> Client<HttpConnector, Body> {
    let mut connector = HttpConnector::new();
    connector.enforce_http(false);
    Client::builder(TokioExecutor::new())
        .pool_timer(TokioTimer::new())
        .build(connector)
}

fn proxy_router(state: Arc<ProxyState>) -> Router {
    Router::new().fallback(proxy_request).with_state(state)
}

fn admin_router(metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_endpoint))
        .with_state(metrics)
}

async fn shutdown_signal(mut receiver: watch::Receiver<bool>) {
    loop {
        if *receiver.borrow() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

async fn proxy_request(
    State(state): State<Arc<ProxyState>>,
    request: Request<Body>,
) -> Result<Response<Body>, ProxyHttpError> {
    state.metrics.increment_requests_total();
    let upstream_request = build_upstream_request(&state.config, request)?;
    let response_result = timeout(
        state.config.request_timeout,
        state.client.request(upstream_request),
    )
    .await;

    let response = match response_result {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            state.metrics.increment_upstream_errors_total();
            return Err(ProxyHttpError::Upstream(error));
        }
        Err(_) => {
            state.metrics.increment_upstream_errors_total();
            return Err(ProxyHttpError::UpstreamTimeout);
        }
    };

    let (mut parts, body) = response.into_parts();
    strip_hop_by_hop_headers(&mut parts.headers);
    Ok(Response::from_parts(parts, Body::new(body)))
}

fn build_upstream_request(
    config: &ValidatedConfig,
    request: Request<Body>,
) -> Result<Request<Body>, ProxyHttpError> {
    if let Some(content_length) = declared_content_length(request.headers())? {
        if content_length > config.max_body_bytes {
            return Err(ProxyHttpError::RequestBodyTooLarge);
        }
    }

    let (mut parts, body) = request.into_parts();
    let path_and_query = match parts.uri.path_and_query() {
        Some(value) => value.as_str(),
        None => "/",
    };
    let uri = format!(
        "{}://{}{}",
        config.origin_scheme, config.origin_authority, path_and_query
    )
    .parse::<Uri>()
    .map_err(|_| ProxyHttpError::InvalidUpstreamUri)?;

    parts.uri = uri;
    strip_hop_by_hop_headers(&mut parts.headers);
    let limited_body = Limited::new(body, config.max_body_bytes as usize);
    Ok(Request::from_parts(parts, Body::new(limited_body)))
}

fn declared_content_length(headers: &HeaderMap) -> Result<Option<u64>, ProxyHttpError> {
    let Some(value) = headers.get(CONTENT_LENGTH) else {
        return Ok(None);
    };
    let text = value
        .to_str()
        .map_err(|_| ProxyHttpError::InvalidContentLength)?;
    let parsed = text
        .parse::<u64>()
        .map_err(|_| ProxyHttpError::InvalidContentLength)?;
    Ok(Some(parsed))
}

fn strip_hop_by_hop_headers(headers: &mut HeaderMap) {
    let mut names = hop_by_hop_names(headers);
    for name in headers.keys() {
        let name_text = name.as_str().to_ascii_lowercase();
        if name_text.starts_with("proxy-") {
            names.push(name.clone());
        }
    }
    for name in names {
        headers.remove(name);
    }
}

fn hop_by_hop_names(headers: &HeaderMap) -> Vec<HeaderName> {
    let mut names = vec![
        HeaderName::from_static("connection"),
        HeaderName::from_static("keep-alive"),
        HeaderName::from_static("te"),
        HeaderName::from_static("trailer"),
        HeaderName::from_static("transfer-encoding"),
        HeaderName::from_static("upgrade"),
    ];

    for value in headers.get_all("connection") {
        if let Ok(text) = value.to_str() {
            for token in text.split(',') {
                if let Ok(name) = HeaderName::from_str(token.trim()) {
                    names.push(name);
                }
            }
        }
    }

    names
}

#[derive(Debug, Error)]
enum ProxyHttpError {
    #[error("content-length invalid")]
    InvalidContentLength,
    #[error("request body too large")]
    RequestBodyTooLarge,
    #[error("upstream uri invalid")]
    InvalidUpstreamUri,
    #[error("upstream timed out")]
    UpstreamTimeout,
    #[error("upstream request failed")]
    Upstream(#[source] hyper_util::client::legacy::Error),
}

impl ProxyHttpError {
    fn status(&self) -> StatusCode {
        match self {
            Self::InvalidContentLength | Self::RequestBodyTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::InvalidUpstreamUri | Self::UpstreamTimeout | Self::Upstream(_) => {
                StatusCode::BAD_GATEWAY
            }
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Self::InvalidContentLength => "AJAR_GATEWAY_INVALID_CONTENT_LENGTH",
            Self::RequestBodyTooLarge => "AJAR_GATEWAY_REQUEST_BODY_TOO_LARGE",
            Self::InvalidUpstreamUri => "AJAR_GATEWAY_INVALID_UPSTREAM_URI",
            Self::UpstreamTimeout => "AJAR_GATEWAY_UPSTREAM_TIMEOUT",
            Self::Upstream(_) => "AJAR_GATEWAY_UPSTREAM_ERROR",
        }
    }
}

impl IntoResponse for ProxyHttpError {
    fn into_response(self) -> axum::response::Response {
        let status = self.status();
        let code = self.code();
        let body = Json(ProblemDetails {
            status: status.as_u16(),
            title: match status.canonical_reason() {
                Some(reason) => reason.to_owned(),
                None => "Gateway error".to_owned(),
            },
            code,
        });
        let mut response = (status, body).into_response();
        response.headers_mut().insert(
            HeaderName::from_static("ajar-error-code"),
            HeaderValue::from_static(code),
        );
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        response
    }
}

#[derive(Serialize)]
struct ProblemDetails {
    status: u16,
    title: String,
    code: &'static str,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

async fn healthz() -> Json<Health> {
    Json(Health { status: "ok" })
}

async fn metrics_endpoint(State(metrics): State<Arc<Metrics>>) -> Response<Body> {
    let mut response = Response::new(Body::from(metrics.render()));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

fn default_request_timeout_ms() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_MS
}

fn default_max_body_bytes() -> u64 {
    DEFAULT_MAX_BODY_BYTES
}

#[cfg(test)]
mod tests {
    use super::{serve_until_shutdown, GatewayConfig};
    use axum::body::{to_bytes, Body};
    use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE};
    use axum::http::{HeaderValue, Method, Request, Response, StatusCode, Uri};
    use axum::response::IntoResponse;
    use axum::routing::{any, get};
    use axum::Router;
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper_util::client::legacy::connect::HttpConnector;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;
    use std::error::Error;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn get_request_passes_through_status_headers_and_body() -> Result<(), Box<dyn Error>> {
        let origin = spawn_origin().await?;
        let gateway = spawn_gateway(origin.addr, 1024).await?;
        let client = test_client();
        let uri: Uri = format!("http://{}/get?x=1", gateway.public_addr).parse()?;
        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri(uri)
            .header("x-custom", "kept")
            .body(Full::new(Bytes::new()))?;

        let response = client.request(request).await?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.into_body().collect().await?.to_bytes();

        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            headers.get("x-origin"),
            Some(&HeaderValue::from_static("yes"))
        );
        assert_eq!(
            headers.get("x-seen-custom"),
            Some(&HeaderValue::from_static("kept"))
        );
        assert_eq!(body, Bytes::from_static(b"get:/get?x=1"));
        gateway.shutdown();
        origin.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn post_body_streams_through() -> Result<(), Box<dyn Error>> {
        let origin = spawn_origin().await?;
        let gateway = spawn_gateway(origin.addr, 1024).await?;
        let client = test_client();
        let uri: Uri = format!("http://{}/post", gateway.public_addr).parse()?;
        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(Full::new(Bytes::from_static(b"posted-body")))?;

        let response = client.request(request).await?;
        let body = response.into_body().collect().await?.to_bytes();

        assert_eq!(body, Bytes::from_static(b"posted-body"));
        gateway.shutdown();
        origin.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn hop_by_hop_headers_are_stripped() -> Result<(), Box<dyn Error>> {
        let origin = spawn_origin().await?;
        let gateway = spawn_gateway(origin.addr, 1024).await?;
        let client = test_client();
        let uri: Uri = format!("http://{}/hop", gateway.public_addr).parse()?;
        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri(uri)
            .header("connection", "x-remove")
            .header("x-remove", "present")
            .header("keep-alive", "timeout=5")
            .body(Full::new(Bytes::new()))?;

        let response = client.request(request).await?;
        let headers = response.headers().clone();
        let body = response.into_body().collect().await?.to_bytes();

        assert_eq!(headers.get("connection"), None);
        assert_eq!(body, Bytes::from_static(b"stripped"));
        gateway.shutdown();
        origin.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn admin_healthz_and_metrics_respond() -> Result<(), Box<dyn Error>> {
        let origin = spawn_origin().await?;
        let gateway = spawn_gateway(origin.addr, 1024).await?;
        let client = test_client();

        let health_uri: Uri = format!("http://{}/healthz", gateway.admin_addr).parse()?;
        let health_response = client
            .request(
                axum::http::Request::builder()
                    .uri(health_uri)
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        let health_body = health_response.into_body().collect().await?.to_bytes();

        let metrics_uri: Uri = format!("http://{}/metrics", gateway.admin_addr).parse()?;
        let metrics_response = client
            .request(
                axum::http::Request::builder()
                    .uri(metrics_uri)
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        let metrics_body = metrics_response.into_body().collect().await?.to_bytes();

        assert_eq!(health_body, Bytes::from_static(br#"{"status":"ok"}"#));
        assert!(String::from_utf8(metrics_body.to_vec())?.contains("requests_total"));
        gateway.shutdown();
        origin.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn oversized_body_is_rejected() -> Result<(), Box<dyn Error>> {
        let origin = spawn_origin().await?;
        let gateway = spawn_gateway(origin.addr, 4).await?;
        let client = test_client();
        let uri: Uri = format!("http://{}/post", gateway.public_addr).parse()?;
        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header(CONTENT_LENGTH, "5")
            .body(Full::new(Bytes::from_static(b"12345")))?;

        let response = client.request(request).await?;

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            response.headers().get("ajar-error-code"),
            Some(&HeaderValue::from_static(
                "AJAR_GATEWAY_REQUEST_BODY_TOO_LARGE"
            ))
        );
        gateway.shutdown();
        origin.shutdown();
        Ok(())
    }

    async fn spawn_origin() -> Result<RunningServer, Box<dyn Error>> {
        let app = Router::new()
            .route("/get", get(origin_get))
            .route("/post", any(origin_post))
            .route("/hop", get(origin_hop));
        spawn_app(app).await
    }

    async fn origin_get(request: Request<Body>) -> Response<Body> {
        let custom = match request.headers().get("x-custom") {
            Some(value) => value.clone(),
            None => HeaderValue::from_static(""),
        };
        let path = match request.uri().path_and_query() {
            Some(value) => value.as_str().to_owned(),
            None => "/".to_owned(),
        };
        let mut response = Response::new(Body::from(format!("get:{path}")));
        *response.status_mut() = StatusCode::PARTIAL_CONTENT;
        response
            .headers_mut()
            .insert("x-origin", HeaderValue::from_static("yes"));
        response.headers_mut().insert("x-seen-custom", custom);
        response
    }

    async fn origin_post(request: Request<Body>) -> impl IntoResponse {
        let bytes = match to_bytes(request.into_body(), 1024).await {
            Ok(bytes) => bytes,
            Err(_) => Bytes::new(),
        };
        (StatusCode::CREATED, bytes)
    }

    async fn origin_hop(request: Request<Body>) -> impl IntoResponse {
        let stripped = request.headers().get("x-remove").is_none()
            && request.headers().get("keep-alive").is_none();
        let mut response = Response::new(Body::from(if stripped { "stripped" } else { "leaked" }));
        response
            .headers_mut()
            .insert("connection", HeaderValue::from_static("close"));
        response
    }

    async fn spawn_gateway(
        origin_addr: SocketAddr,
        max_body_bytes: u64,
    ) -> Result<RunningGateway, Box<dyn Error>> {
        let public_listener = TcpListener::bind("127.0.0.1:0").await?;
        let public_addr = public_listener.local_addr()?;
        drop(public_listener);

        let admin_listener = TcpListener::bind("127.0.0.1:0").await?;
        let admin_addr = admin_listener.local_addr()?;
        drop(admin_listener);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let config = GatewayConfig {
            origin_url: format!("http://{origin_addr}"),
            listen_addr: public_addr,
            admin_addr,
            request_timeout_ms: 5_000,
            max_body_bytes,
        };
        tokio::spawn(async move {
            let _serve_result = serve_until_shutdown(config, async {
                let _shutdown_result = shutdown_rx.await;
            })
            .await;
        });
        sleep(Duration::from_millis(25)).await;

        Ok(RunningGateway {
            public_addr,
            admin_addr,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    async fn spawn_app(app: Router) -> Result<RunningServer, Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let serve = axum::serve(listener, app).with_graceful_shutdown(async {
                let _shutdown_result = shutdown_rx.await;
            });
            let _serve_result = serve.await;
        });
        sleep(Duration::from_millis(25)).await;
        Ok(RunningServer {
            addr,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    fn test_client() -> Client<HttpConnector, Full<Bytes>> {
        Client::builder(TokioExecutor::new()).build(HttpConnector::new())
    }

    struct RunningServer {
        addr: SocketAddr,
        shutdown_tx: Option<oneshot::Sender<()>>,
    }

    impl RunningServer {
        fn shutdown(mut self) {
            if let Some(sender) = self.shutdown_tx.take() {
                let _send_result = sender.send(());
            }
        }
    }

    struct RunningGateway {
        public_addr: SocketAddr,
        admin_addr: SocketAddr,
        shutdown_tx: Option<oneshot::Sender<()>>,
    }

    impl RunningGateway {
        fn shutdown(mut self) {
            if let Some(sender) = self.shutdown_tx.take() {
                let _send_result = sender.send(());
            }
        }
    }
}
