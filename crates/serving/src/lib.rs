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
use axum::http::header::{
    HeaderName, ACCEPT, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG, IF_NONE_MATCH, LINK,
    VARY,
};
use axum::http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use http_body_util::Limited;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use store::{Clock, PreparedContentStore, StoredView, SystemClock};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::timeout;

const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAX_BODY_BYTES: u64 = 10 * 1024 * 1024;
const MANIFEST_PATH: &str = "/.well-known/ajar.json";
const PROBLEM_JSON: &str = "application/problem+json";
const AJAR_JSON: &str = "application/ajar+json";
const MARKDOWN: &str = "text/markdown";
const AJAR_MANIFEST_LINK: &str = r#"</.well-known/ajar.json>; rel="ajar-manifest""#;

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
    /// Optional prepared content store directory for owner-approved Ajar artifacts.
    pub store_dir: Option<PathBuf>,
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
            store_dir: self.store_dir.clone(),
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
    store_dir: Option<PathBuf>,
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
    /// The prepared content store failed startup validation.
    #[error("prepared content store invalid")]
    StoreLoadFailed(#[from] store::StoreLoadError),
}

/// Owner-local counters exported by the admin server.
#[derive(Debug, Default)]
pub struct Metrics {
    requests_total: AtomicU64,
    upstream_errors_total: AtomicU64,
    views_served_total: AtomicU64,
    manifest_served_total: AtomicU64,
    negotiation_passthrough_total: AtomicU64,
    store_reloads_total: AtomicU64,
    store_reload_failures_total: AtomicU64,
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

    /// Increments the negotiated view response counter.
    pub fn increment_views_served_total(&self) {
        self.views_served_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the manifest response counter.
    pub fn increment_manifest_served_total(&self) {
        self.manifest_served_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the same-URL negotiation passthrough counter.
    pub fn increment_negotiation_passthrough_total(&self) {
        self.negotiation_passthrough_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the successful store reload counter.
    pub fn increment_store_reloads_total(&self) {
        self.store_reloads_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the failed store reload counter.
    pub fn increment_store_reload_failures_total(&self) {
        self.store_reload_failures_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Renders the metrics endpoint body.
    pub fn render(&self) -> String {
        let requests_total = self.requests_total.load(Ordering::Relaxed);
        let upstream_errors_total = self.upstream_errors_total.load(Ordering::Relaxed);
        let views_served_total = self.views_served_total.load(Ordering::Relaxed);
        let manifest_served_total = self.manifest_served_total.load(Ordering::Relaxed);
        let negotiation_passthrough_total =
            self.negotiation_passthrough_total.load(Ordering::Relaxed);
        let store_reloads_total = self.store_reloads_total.load(Ordering::Relaxed);
        let store_reload_failures_total = self.store_reload_failures_total.load(Ordering::Relaxed);
        format!(
            "requests_total {}\nupstream_errors_total {}\nviews_served_total {}\nmanifest_served_total {}\nnegotiation_passthrough_total {}\nstore_reloads_total {}\nstore_reload_failures_total {}\n",
            requests_total,
            upstream_errors_total,
            views_served_total,
            manifest_served_total,
            negotiation_passthrough_total,
            store_reloads_total,
            store_reload_failures_total
        )
    }
}

type SharedPreparedContentStore = Arc<RwLock<Arc<PreparedContentStore>>>;

struct ProxyState {
    config: ValidatedConfig,
    client: Client<HttpConnector, Body>,
    metrics: Arc<Metrics>,
    store: Option<SharedPreparedContentStore>,
    clock: Arc<dyn Clock>,
    manifest_expiry_logged: AtomicBool,
}

/// Runs the public proxy listener and separate admin listener until shutdown.
pub async fn serve_until_shutdown(
    config: GatewayConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServingError> {
    serve_until_shutdown_with_clock(config, shutdown, Arc::new(SystemClock)).await
}

/// Runs the public proxy listener with an injected clock for freshness tests.
pub async fn serve_until_shutdown_with_clock(
    config: GatewayConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
    clock: Arc<dyn Clock>,
) -> Result<(), ServingError> {
    let config = config.validate()?;
    let store = match &config.store_dir {
        Some(path) => Some(Arc::new(RwLock::new(Arc::new(
            PreparedContentStore::load_from_dir(path, clock.as_ref())?,
        )))),
        None => None,
    };
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
        store,
        clock,
        manifest_expiry_logged: AtomicBool::new(false),
    });

    let proxy_app = proxy_router(state.clone());
    let admin_app = admin_router(state.clone());

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

fn admin_router(state: Arc<ProxyState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_endpoint))
        .route("/reload", post(reload_store))
        .with_state(state)
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
    let current_store = current_store(&state)?;
    if let Some(store) = current_store.as_ref() {
        if request.method() == Method::GET && request.uri().path() == MANIFEST_PATH {
            return Ok(serve_manifest(&state, store, request.headers()));
        }
        if request.method() == Method::GET && request.uri().path() == store.manifest().views_index()
        {
            return Ok(serve_view_index(&state, store, request.headers()));
        }
        if request.method() == Method::GET {
            let target = request_target(request.uri());
            if let Some(view) = store.view_for_request_target(&target) {
                let accept = combined_accept_header(request.headers());
                match negotiate(accept.as_deref(), true) {
                    Representation::AjarJson => {
                        return Ok(serve_view_json(&state, store, view, request.headers()));
                    }
                    Representation::Markdown => {
                        return Ok(serve_view_markdown(&state, store, view, request.headers()));
                    }
                    Representation::Passthrough => {
                        state.metrics.increment_negotiation_passthrough_total();
                    }
                }
            }
        }
    }

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
    if current_store.is_some() && is_html_content_type(&parts.headers) {
        parts
            .headers
            .append(LINK, HeaderValue::from_static(AJAR_MANIFEST_LINK));
    }
    Ok(Response::from_parts(parts, Body::new(body)))
}

fn serve_manifest(
    state: &ProxyState,
    store: &PreparedContentStore,
    headers: &HeaderMap,
) -> Response<Body> {
    let manifest = store.manifest();
    let now = state.clock.now_unix_seconds();
    if manifest.expires_at_unix() <= now {
        log_manifest_expired_once(state);
        return problem_response(
            StatusCode::NOT_FOUND,
            "AJAR-VERIFY-EXPIRED",
            "Manifest expired",
            "The configured Ajar manifest has expired.",
        );
    }
    if if_none_match_matches(headers, manifest.etag()) {
        return empty_response(StatusCode::NOT_MODIFIED);
    }

    let seconds_until_expiry = manifest.expires_at_unix() - now;
    let cache_seconds = seconds_until_expiry.clamp(0, 3_600);
    let mut response = Response::new(Body::from(manifest.bytes().to_vec()));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    insert_header(
        response.headers_mut(),
        CACHE_CONTROL,
        &format!("max-age={cache_seconds}"),
    );
    insert_header(response.headers_mut(), ETAG, manifest.etag());
    state.metrics.increment_manifest_served_total();
    response
}

fn serve_view_index(
    state: &ProxyState,
    store: &PreparedContentStore,
    headers: &HeaderMap,
) -> Response<Body> {
    if let Some(response) = expired_manifest_problem(state, store) {
        return response;
    }
    let index = store.view_index();
    if if_none_match_matches(headers, index.etag()) {
        return empty_response(StatusCode::NOT_MODIFIED);
    }

    let mut response = Response::new(Body::from(index.bytes().to_vec()));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    insert_header(response.headers_mut(), ETAG, index.etag());
    state.metrics.increment_views_served_total();
    response
}

fn serve_view_json(
    state: &ProxyState,
    store: &PreparedContentStore,
    view: &StoredView,
    headers: &HeaderMap,
) -> Response<Body> {
    if let Some(response) = expired_manifest_problem(state, store) {
        return response;
    }
    if if_none_match_matches(headers, &view.view().etag) {
        return empty_response(StatusCode::NOT_MODIFIED);
    }

    let mut response = Response::new(Body::from(view.bytes().to_vec()));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(AJAR_JSON));
    insert_header(response.headers_mut(), ETAG, &view.view().etag);
    insert_header(
        response.headers_mut(),
        HeaderName::from_static("ajar-content-signature"),
        &view.view().signature.sig,
    );
    response
        .headers_mut()
        .insert(VARY, HeaderValue::from_static("Accept"));
    state.metrics.increment_views_served_total();
    response
}

fn serve_view_markdown(
    state: &ProxyState,
    store: &PreparedContentStore,
    view: &StoredView,
    headers: &HeaderMap,
) -> Response<Body> {
    if let Some(response) = expired_manifest_problem(state, store) {
        return response;
    }
    if if_none_match_matches(headers, &view.view().etag) {
        return empty_response(StatusCode::NOT_MODIFIED);
    }

    let mut response = Response::new(Body::from(render_markdown(view)));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/markdown; charset=utf-8"),
    );
    insert_header(response.headers_mut(), ETAG, &view.view().etag);
    // The header carries the signature over the canonical View object that this
    // deterministic markdown representation derives from.
    insert_header(
        response.headers_mut(),
        HeaderName::from_static("ajar-content-signature"),
        &view.view().signature.sig,
    );
    response
        .headers_mut()
        .insert(VARY, HeaderValue::from_static("Accept"));
    state.metrics.increment_views_served_total();
    response
}

fn empty_response(status: StatusCode) -> Response<Body> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

fn problem_response(
    status: StatusCode,
    code: &'static str,
    title: &str,
    detail: &str,
) -> Response<Body> {
    let body = Json(ProblemDetails {
        problem_type: "about:blank",
        status: status.as_u16(),
        title: title.to_owned(),
        detail: detail.to_owned(),
        ajar_error_code: code,
    });
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        HeaderName::from_static("ajar-error-code"),
        HeaderValue::from_static(code),
    );
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(PROBLEM_JSON));
    response
}

fn expired_manifest_problem(
    state: &ProxyState,
    store: &PreparedContentStore,
) -> Option<Response<Body>> {
    if store.manifest().expires_at_unix() > state.clock.now_unix_seconds() {
        return None;
    }
    log_manifest_expired_once(state);
    Some(problem_response(
        StatusCode::NOT_FOUND,
        "AJAR-VERIFY-EXPIRED",
        "Manifest expired",
        "The configured Ajar manifest has expired.",
    ))
}

fn current_store(state: &ProxyState) -> Result<Option<Arc<PreparedContentStore>>, ProxyHttpError> {
    let Some(store) = &state.store else {
        return Ok(None);
    };
    match store.read() {
        Ok(guard) => Ok(Some(guard.clone())),
        Err(_) => Err(ProxyHttpError::StoreStateUnavailable),
    }
}

fn log_manifest_expired_once(state: &ProxyState) {
    if state
        .manifest_expiry_logged
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        eprintln!(
            "{{\"level\":\"error\",\"event\":\"ajar_manifest_expired\",\"fields\":{{\"code\":\"AJAR-VERIFY-EXPIRED\"}}}}"
        );
    }
}

async fn reload_store(State(state): State<Arc<ProxyState>>) -> Response<Body> {
    let Some(store_dir) = state.config.store_dir.as_ref() else {
        return reload_failed(
            &state,
            "no content store configured".to_owned(),
            "no_content_store_configured",
        );
    };
    let Some(shared_store) = state.store.as_ref() else {
        return reload_failed(
            &state,
            "no content store configured".to_owned(),
            "no_content_store_configured",
        );
    };

    let loaded_store = match PreparedContentStore::load_from_dir(store_dir, state.clock.as_ref()) {
        Ok(store) => Arc::new(store),
        Err(error) => {
            return reload_failed(&state, error.to_string(), "validation_failed");
        }
    };

    let views = loaded_store.view_count();
    let manifest_sequence = loaded_store.manifest_sequence();
    match shared_store.write() {
        Ok(mut guard) => {
            let current_sequence = guard.manifest_sequence();
            if manifest_sequence < current_sequence {
                return reload_failed(
                    &state,
                    "prepared manifest sequence is lower than the currently served sequence"
                        .to_owned(),
                    "sequence_rollback",
                );
            }
            *guard = loaded_store;
        }
        Err(_) => {
            return reload_failed(
                &state,
                "content store state unavailable".to_owned(),
                "store_state_unavailable",
            );
        }
    }

    state.metrics.increment_store_reloads_total();
    state.manifest_expiry_logged.store(false, Ordering::Relaxed);
    log_event(
        "info",
        "store_reloaded",
        serde_json::json!({
            "views": views,
            "manifest_sequence": manifest_sequence
        }),
    );

    (
        StatusCode::OK,
        Json(ReloadSuccess {
            status: "reloaded",
            views,
            manifest_sequence,
        }),
    )
        .into_response()
}

fn reload_failed(state: &ProxyState, detail: String, reason: &'static str) -> Response<Body> {
    state.metrics.increment_store_reload_failures_total();
    log_event(
        "error",
        "store_reload_failed",
        serde_json::json!({
            "reason": reason,
            "detail": detail
        }),
    );
    problem_response(
        StatusCode::CONFLICT,
        "AJAR_GATEWAY_STORE_RELOAD_FAILED",
        "Store reload failed",
        &detail,
    )
}

fn log_event(level: &str, event: &str, fields: serde_json::Value) {
    let line = serde_json::json!({
        "level": level,
        "event": event,
        "fields": fields
    });
    eprintln!("{line}");
}

/// Negotiated response representation for a same-URL Ajar view request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Representation {
    /// Serve the signed Ajar view object.
    AjarJson,
    /// Serve deterministic markdown derived from the signed view object.
    Markdown,
    /// Let the origin serve the browser-facing URL unchanged.
    Passthrough,
}

/// Selects an Ajar representation from an Accept header without touching I/O.
pub fn negotiate(accept_header: Option<&str>, has_view: bool) -> Representation {
    if !has_view {
        return Representation::Passthrough;
    }
    let Some(header) = accept_header else {
        return Representation::Passthrough;
    };

    let mut markdown = false;
    for item in header.split(',') {
        let mut parts = item.split(';');
        let media_type = match parts.next() {
            Some(value) => value.trim().to_ascii_lowercase(),
            None => continue,
        };
        if media_type != AJAR_JSON && media_type != MARKDOWN {
            continue;
        }
        let mut q = 1.0_f32;
        let mut invalid_q = false;
        for parameter in parts {
            let mut pair = parameter.trim().splitn(2, '=');
            let name = pair.next().unwrap_or("").trim();
            let value = pair.next().unwrap_or("").trim().trim_matches('"');
            if name.eq_ignore_ascii_case("q") {
                match value.parse::<f32>() {
                    Ok(parsed) => q = parsed,
                    Err(_) => invalid_q = true,
                }
            }
        }
        if invalid_q || q <= 0.0 {
            continue;
        }
        if media_type == AJAR_JSON {
            return Representation::AjarJson;
        }
        markdown = true;
    }

    if markdown {
        Representation::Markdown
    } else {
        Representation::Passthrough
    }
}

fn render_markdown(view: &StoredView) -> String {
    view.view()
        .chunks
        .iter()
        .map(|chunk| match chunk.chunk_type.as_str() {
            "heading" => format!("## {}", chunk.content),
            "paragraph" | "list" | "table" => chunk.content.clone(),
            _ => chunk.content.clone(),
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn request_target(uri: &Uri) -> String {
    match uri.path_and_query() {
        Some(value) => value.as_str().to_owned(),
        None => "/".to_owned(),
    }
}

fn combined_accept_header(headers: &HeaderMap) -> Option<String> {
    let mut values = Vec::new();
    for value in headers.get_all(ACCEPT) {
        if let Ok(text) = value.to_str() {
            values.push(text.to_owned());
        }
    }
    if values.is_empty() {
        None
    } else {
        Some(values.join(","))
    }
}

fn if_none_match_matches(headers: &HeaderMap, etag: &str) -> bool {
    for value in headers.get_all(IF_NONE_MATCH) {
        let Ok(text) = value.to_str() else {
            continue;
        };
        for candidate in text.split(',') {
            let candidate = candidate.trim();
            if candidate == "*" || candidate == etag {
                return true;
            }
        }
    }
    false
}

fn insert_header(headers: &mut HeaderMap, name: HeaderName, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

fn is_html_content_type(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(CONTENT_TYPE) else {
        return false;
    };
    let Ok(text) = value.to_str() else {
        return false;
    };
    text.split(';')
        .next()
        .map(|media_type| media_type.trim().eq_ignore_ascii_case("text/html"))
        .unwrap_or(false)
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
    #[error("content store state unavailable")]
    StoreStateUnavailable,
}

impl ProxyHttpError {
    fn status(&self) -> StatusCode {
        match self {
            Self::InvalidContentLength | Self::RequestBodyTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::InvalidUpstreamUri | Self::UpstreamTimeout | Self::Upstream(_) => {
                StatusCode::BAD_GATEWAY
            }
            Self::StoreStateUnavailable => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Self::InvalidContentLength => "AJAR_GATEWAY_INVALID_CONTENT_LENGTH",
            Self::RequestBodyTooLarge => "AJAR_GATEWAY_REQUEST_BODY_TOO_LARGE",
            Self::InvalidUpstreamUri => "AJAR_GATEWAY_INVALID_UPSTREAM_URI",
            Self::UpstreamTimeout => "AJAR_GATEWAY_UPSTREAM_TIMEOUT",
            Self::Upstream(_) => "AJAR_GATEWAY_UPSTREAM_ERROR",
            Self::StoreStateUnavailable => "AJAR_GATEWAY_STORE_STATE_UNAVAILABLE",
        }
    }
}

impl IntoResponse for ProxyHttpError {
    fn into_response(self) -> axum::response::Response {
        let status = self.status();
        let code = self.code();
        let body = Json(ProblemDetails {
            problem_type: "about:blank",
            status: status.as_u16(),
            title: match status.canonical_reason() {
                Some(reason) => reason.to_owned(),
                None => "Gateway error".to_owned(),
            },
            detail: "The gateway could not complete the request.".to_owned(),
            ajar_error_code: code,
        });
        let mut response = (status, body).into_response();
        response.headers_mut().insert(
            HeaderName::from_static("ajar-error-code"),
            HeaderValue::from_static(code),
        );
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static(PROBLEM_JSON));
        response
    }
}

#[derive(Serialize)]
struct ProblemDetails {
    #[serde(rename = "type")]
    problem_type: &'static str,
    status: u16,
    title: String,
    detail: String,
    ajar_error_code: &'static str,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[derive(Serialize)]
struct ReloadSuccess {
    status: &'static str,
    views: usize,
    manifest_sequence: i64,
}

async fn healthz() -> Json<Health> {
    Json(Health { status: "ok" })
}

async fn metrics_endpoint(State(state): State<Arc<ProxyState>>) -> Response<Body> {
    let mut response = Response::new(Body::from(state.metrics.render()));
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
    use super::{
        negotiate, serve_until_shutdown, serve_until_shutdown_with_clock, GatewayConfig,
        Representation,
    };
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
    use std::fs;
    use std::net::SocketAddr;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
    use std::sync::Arc;
    use store::Clock;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio::time::{sleep, Duration};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn negotiation_matrix() {
        assert_eq!(
            negotiate(Some("application/ajar+json"), true),
            Representation::AjarJson
        );
        assert_eq!(
            negotiate(Some("application/ajar+json; profile=core; q=0.7"), true),
            Representation::AjarJson
        );
        assert_eq!(
            negotiate(Some("text/markdown"), true),
            Representation::Markdown
        );
        assert_eq!(
            negotiate(Some("text/markdown;q=1, application/ajar+json;q=0.1"), true),
            Representation::AjarJson
        );
        assert_eq!(
            negotiate(Some("application/ajar+json;q=0, text/markdown;q=1"), true),
            Representation::Markdown
        );
        assert_eq!(
            negotiate(Some("text/markdown;q=0"), true),
            Representation::Passthrough
        );
        assert_eq!(negotiate(Some("*/*"), true), Representation::Passthrough);
        assert_eq!(
            negotiate(Some("this is not an accept header"), true),
            Representation::Passthrough
        );
        assert_eq!(
            negotiate(Some("application/ajar+json"), false),
            Representation::Passthrough
        );
        assert_eq!(negotiate(None, true), Representation::Passthrough);
    }

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
    async fn manifest_serves_200_304_and_html_proxy_link() -> Result<(), Box<dyn Error>> {
        let origin = spawn_origin().await?;
        let store_dir = write_store_fixture()?;
        let clock = Arc::new(TestClock::new(0));
        let gateway = spawn_gateway_with_store(origin.addr, 1024, store_dir, clock).await?;
        let client = test_client();

        let manifest_uri: Uri =
            format!("http://{}/.well-known/ajar.json", gateway.public_addr).parse()?;
        let manifest_response = client
            .request(
                axum::http::Request::builder()
                    .uri(manifest_uri.clone())
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        let manifest_headers = manifest_response.headers().clone();
        let manifest_body = manifest_response.into_body().collect().await?.to_bytes();

        assert_eq!(
            manifest_headers.get(CONTENT_TYPE),
            Some(&HeaderValue::from_static("application/json"))
        );
        assert!(manifest_headers.get("cache-control").is_some());
        assert_eq!(
            manifest_body,
            Bytes::from_static(MANIFEST_FIXTURE.as_bytes())
        );

        let etag = manifest_headers
            .get("etag")
            .ok_or_else(|| std::io::Error::other("manifest missing etag"))?
            .clone();
        let not_modified = client
            .request(
                axum::http::Request::builder()
                    .uri(manifest_uri)
                    .header("if-none-match", etag)
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(not_modified.status(), StatusCode::NOT_MODIFIED);

        let html_uri: Uri = format!("http://{}/html", gateway.public_addr).parse()?;
        let html_response = client
            .request(
                axum::http::Request::builder()
                    .uri(html_uri)
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(
            html_response.headers().get("link"),
            Some(&HeaderValue::from_static(
                r#"</.well-known/ajar.json>; rel="ajar-manifest""#
            ))
        );

        gateway.shutdown();
        origin.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn view_json_serves_signature_etag_and_304() -> Result<(), Box<dyn Error>> {
        let origin = spawn_origin().await?;
        let store_dir = write_store_fixture()?;
        let clock = Arc::new(TestClock::new(0));
        let gateway = spawn_gateway_with_store(origin.addr, 1024, store_dir, clock).await?;
        let client = test_client();
        let uri: Uri = format!("http://{}/article?x=1", gateway.public_addr).parse()?;

        let response = client
            .request(
                axum::http::Request::builder()
                    .uri(uri.clone())
                    .header("accept", "application/ajar+json")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        let headers = response.headers().clone();
        let body = response.into_body().collect().await?.to_bytes();

        assert_eq!(
            headers.get(CONTENT_TYPE),
            Some(&HeaderValue::from_static("application/ajar+json"))
        );
        assert_eq!(
            headers.get("etag"),
            Some(&HeaderValue::from_static("\"view-article-001\""))
        );
        assert_eq!(
            headers.get("ajar-content-signature"),
            Some(&HeaderValue::from_static("viewSignature001"))
        );
        assert_eq!(
            headers.get("vary"),
            Some(&HeaderValue::from_static("Accept"))
        );
        assert_eq!(body, Bytes::from_static(VIEW_FIXTURE.as_bytes()));

        let not_modified = client
            .request(
                axum::http::Request::builder()
                    .uri(uri)
                    .header("accept", "application/ajar+json")
                    .header("if-none-match", "\"view-article-001\"")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(not_modified.status(), StatusCode::NOT_MODIFIED);

        gateway.shutdown();
        origin.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn markdown_rendering_matches_golden() -> Result<(), Box<dyn Error>> {
        let origin = spawn_origin().await?;
        let store_dir = write_store_fixture()?;
        let clock = Arc::new(TestClock::new(0));
        let gateway = spawn_gateway_with_store(origin.addr, 1024, store_dir, clock).await?;
        let client = test_client();
        let uri: Uri = format!("http://{}/article?x=1", gateway.public_addr).parse()?;

        let response = client
            .request(
                axum::http::Request::builder()
                    .uri(uri)
                    .header("accept", "text/markdown")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        let headers = response.headers().clone();
        let body = response.into_body().collect().await?.to_bytes();

        assert_eq!(
            headers.get(CONTENT_TYPE),
            Some(&HeaderValue::from_static("text/markdown; charset=utf-8"))
        );
        assert_eq!(
            String::from_utf8(body.to_vec())?,
            "## Example Notes\n\nSigned notes for readers and agents.\n\n- First\n- Second"
        );

        gateway.shutdown();
        origin.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn expired_manifest_returns_404_problem_after_startup() -> Result<(), Box<dyn Error>> {
        let origin = spawn_origin().await?;
        let store_dir = write_store_fixture()?;
        let clock = Arc::new(TestClock::new(0));
        let gateway = spawn_gateway_with_store(origin.addr, 1024, store_dir, clock.clone()).await?;
        clock.set(2_000_000_000);
        let client = test_client();
        let uri: Uri = format!("http://{}/.well-known/ajar.json", gateway.public_addr).parse()?;

        let response = client
            .request(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.into_body().collect().await?.to_bytes();

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(
            headers.get("ajar-error-code"),
            Some(&HeaderValue::from_static("AJAR-VERIFY-EXPIRED"))
        );
        assert!(String::from_utf8(body.to_vec())?
            .contains("\"ajar_error_code\":\"AJAR-VERIFY-EXPIRED\""));

        gateway.shutdown();
        origin.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn storeless_manifest_path_passes_through_to_origin() -> Result<(), Box<dyn Error>> {
        let origin = spawn_origin().await?;
        let gateway = spawn_gateway(origin.addr, 1024).await?;
        let client = test_client();
        let uri: Uri = format!("http://{}/.well-known/ajar.json", gateway.public_addr).parse()?;

        let response = client
            .request(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        let body = response.into_body().collect().await?.to_bytes();

        assert_eq!(body, Bytes::from_static(b"origin-manifest"));
        gateway.shutdown();
        origin.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn reload_swaps_content_store() -> Result<(), Box<dyn Error>> {
        let origin = spawn_origin().await?;
        let store_dir = write_store_fixture()?;
        let clock = Arc::new(TestClock::new(0));
        let gateway = spawn_gateway_with_store(origin.addr, 1024, store_dir.clone(), clock).await?;
        let client = test_client();

        let initial_body = get_markdown_body(&client, &gateway).await?;
        assert!(initial_body.contains("Signed notes for readers and agents."));

        rewrite_store_fixture(
            &store_dir,
            2,
            "Reloaded Notes",
            "Fresh signed content after reload.",
            "view-article-002",
        )?;
        let reload_response = post_reload(&client, &gateway).await?;
        let reload_status = reload_response.status();
        let reload_body = reload_response.into_body().collect().await?.to_bytes();

        assert_eq!(reload_status, StatusCode::OK);
        assert_eq!(
            reload_body,
            Bytes::from_static(br#"{"status":"reloaded","views":1,"manifest_sequence":2}"#)
        );
        let reloaded_body = get_markdown_body(&client, &gateway).await?;
        assert!(reloaded_body.contains("Fresh signed content after reload."));

        gateway.shutdown();
        origin.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn reload_invalid_store_keeps_current_content() -> Result<(), Box<dyn Error>> {
        let origin = spawn_origin().await?;
        let store_dir = write_store_fixture()?;
        let clock = Arc::new(TestClock::new(0));
        let gateway = spawn_gateway_with_store(origin.addr, 1024, store_dir.clone(), clock).await?;
        let client = test_client();

        fs::write(store_dir.join("manifest.json"), "{}")?;
        let reload_response = post_reload(&client, &gateway).await?;
        let status = reload_response.status();
        let headers = reload_response.headers().clone();
        let body = reload_response.into_body().collect().await?.to_bytes();

        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(
            headers.get("content-type"),
            Some(&HeaderValue::from_static("application/problem+json"))
        );
        assert!(
            String::from_utf8(body.to_vec())?.contains("prepared manifest required field missing")
        );
        let current_body = get_markdown_body(&client, &gateway).await?;
        assert!(current_body.contains("Signed notes for readers and agents."));

        gateway.shutdown();
        origin.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn reload_rejects_lower_manifest_sequence() -> Result<(), Box<dyn Error>> {
        let origin = spawn_origin().await?;
        let store_dir = write_store_fixture()?;
        rewrite_store_fixture(
            &store_dir,
            2,
            "Reloaded Notes",
            "Sequence two content.",
            "view-article-002",
        )?;
        let clock = Arc::new(TestClock::new(0));
        let gateway = spawn_gateway_with_store(origin.addr, 1024, store_dir.clone(), clock).await?;
        let client = test_client();

        rewrite_store_fixture(
            &store_dir,
            1,
            "Rollback Notes",
            "Rollback content must not serve.",
            "view-article-001",
        )?;
        let reload_response = post_reload(&client, &gateway).await?;
        let status = reload_response.status();
        let body = reload_response.into_body().collect().await?.to_bytes();

        assert_eq!(status, StatusCode::CONFLICT);
        assert!(String::from_utf8(body.to_vec())?
            .contains("prepared manifest sequence is lower than the currently served sequence"));
        let current_body = get_markdown_body(&client, &gateway).await?;
        assert!(current_body.contains("Sequence two content."));

        gateway.shutdown();
        origin.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn reload_without_store_config_returns_409_and_get_is_405() -> Result<(), Box<dyn Error>>
    {
        let origin = spawn_origin().await?;
        let gateway = spawn_gateway(origin.addr, 1024).await?;
        let client = test_client();

        let post_response = post_reload(&client, &gateway).await?;
        let post_status = post_response.status();
        let post_body = post_response.into_body().collect().await?.to_bytes();

        assert_eq!(post_status, StatusCode::CONFLICT);
        assert!(String::from_utf8(post_body.to_vec())?.contains("no content store configured"));

        let reload_uri: Uri = format!("http://{}/reload", gateway.admin_addr).parse()?;
        let get_response = client
            .request(
                axum::http::Request::builder()
                    .method(Method::GET)
                    .uri(reload_uri)
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(get_response.status(), StatusCode::METHOD_NOT_ALLOWED);

        gateway.shutdown();
        origin.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn reload_metrics_counters_increment() -> Result<(), Box<dyn Error>> {
        let origin = spawn_origin().await?;
        let store_dir = write_store_fixture()?;
        let clock = Arc::new(TestClock::new(0));
        let gateway = spawn_gateway_with_store(origin.addr, 1024, store_dir.clone(), clock).await?;
        let client = test_client();

        rewrite_store_fixture(
            &store_dir,
            2,
            "Reloaded Notes",
            "Metrics success content.",
            "view-article-002",
        )?;
        let success = post_reload(&client, &gateway).await?;
        assert_eq!(success.status(), StatusCode::OK);

        fs::write(store_dir.join("manifest.json"), "{}")?;
        let failure = post_reload(&client, &gateway).await?;
        assert_eq!(failure.status(), StatusCode::CONFLICT);

        let metrics_uri: Uri = format!("http://{}/metrics", gateway.admin_addr).parse()?;
        let metrics_response = client
            .request(
                axum::http::Request::builder()
                    .uri(metrics_uri)
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        let metrics_body = metrics_response.into_body().collect().await?.to_bytes();
        let metrics = String::from_utf8(metrics_body.to_vec())?;
        assert!(metrics.contains("store_reloads_total 1"));
        assert!(metrics.contains("store_reload_failures_total 1"));

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
            .route("/hop", get(origin_hop))
            .route("/html", get(origin_html))
            .route("/.well-known/ajar.json", get(origin_manifest));
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

    async fn origin_html() -> Response<Body> {
        let mut response = Response::new(Body::from("<!doctype html><title>Origin</title>"));
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        );
        response
    }

    async fn origin_manifest() -> Response<Body> {
        Response::new(Body::from("origin-manifest"))
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
            store_dir: None,
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

    async fn spawn_gateway_with_store(
        origin_addr: SocketAddr,
        max_body_bytes: u64,
        store_dir: PathBuf,
        clock: Arc<dyn Clock>,
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
            store_dir: Some(store_dir),
        };
        tokio::spawn(async move {
            let _serve_result = serve_until_shutdown_with_clock(
                config,
                async {
                    let _shutdown_result = shutdown_rx.await;
                },
                clock,
            )
            .await;
        });
        sleep(Duration::from_millis(25)).await;

        Ok(RunningGateway {
            public_addr,
            admin_addr,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    fn write_store_fixture() -> Result<PathBuf, Box<dyn Error>> {
        let nonce = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "ajar-gateway-serving-test-{}-{nonce}",
            std::process::id()
        ));
        rewrite_store_fixture(
            &dir,
            1,
            "Example Notes",
            "Signed notes for readers and agents.",
            "view-article-001",
        )?;
        Ok(dir)
    }

    fn rewrite_store_fixture(
        dir: &Path,
        sequence: i64,
        heading: &str,
        paragraph: &str,
        view_etag: &str,
    ) -> Result<(), Box<dyn Error>> {
        fs::create_dir_all(dir.join("views"))?;
        let manifest = MANIFEST_FIXTURE
            .replace("\"sequence\":1", &format!("\"sequence\":{sequence}"))
            .replace(
                "manifestNotesSignature001",
                &format!("manifestNotesSignature{sequence:03}"),
            );
        let view_index =
            format!(r#"{{"views":[{{"url":"/article?x=1","etag":"\"{view_etag}\""}}]}}"#);
        let view = VIEW_FIXTURE
            .replace("Example Notes", heading)
            .replace("Signed notes for readers and agents.", paragraph)
            .replace("view-article-001", view_etag)
            .replace("viewSignature001", &format!("viewSignature{sequence:03}"));
        fs::write(dir.join("manifest.json"), manifest)?;
        fs::write(dir.join("view-index.json"), view_index)?;
        fs::write(dir.join("views").join("article.json"), view)?;
        Ok(())
    }

    async fn get_markdown_body(
        client: &Client<HttpConnector, Full<Bytes>>,
        gateway: &RunningGateway,
    ) -> Result<String, Box<dyn Error>> {
        let uri: Uri = format!("http://{}/article?x=1", gateway.public_addr).parse()?;
        let response = client
            .request(
                axum::http::Request::builder()
                    .uri(uri)
                    .header("accept", "text/markdown")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        let body = response.into_body().collect().await?.to_bytes();
        Ok(String::from_utf8(body.to_vec())?)
    }

    async fn post_reload(
        client: &Client<HttpConnector, Full<Bytes>>,
        gateway: &RunningGateway,
    ) -> Result<Response<hyper::body::Incoming>, Box<dyn Error>> {
        let uri: Uri = format!("http://{}/reload", gateway.admin_addr).parse()?;
        let response = client
            .request(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri(uri)
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        Ok(response)
    }

    struct TestClock {
        now: AtomicI64,
    }

    impl TestClock {
        fn new(now: i64) -> Self {
            Self {
                now: AtomicI64::new(now),
            }
        }

        fn set(&self, now: i64) {
            self.now.store(now, Ordering::Relaxed);
        }
    }

    impl Clock for TestClock {
        fn now_unix_seconds(&self) -> i64 {
            self.now.load(Ordering::Relaxed)
        }
    }

    const MANIFEST_FIXTURE: &str = r#"{"ajar_version":"0.1","supported_versions":["0.1"],"profiles":["CORE"],"site":{"name":"Example Notes","domain":"notes.example","description":"Independent publication with signed semantic article views.","languages":["en"],"contact":"agents@notes.example"},"keys":{"owner":{"kty":"OKP","crv":"Ed25519","x":"ownerNotesPublicKeyExample001","kid":"owner-2026"},"operational":[{"key":{"kty":"OKP","crv":"Ed25519","x":"opsNotesPublicKeyExample001","kid":"content-2026-07"},"scope":["content-signing"],"valid_until":"2026-10-01T00:00:00Z","certification":{"alg":"Ed25519","kid":"owner-2026","sig":"certNotesContentSigner001"}}]},"views":{"negotiation":["application/ajar+json","text/markdown"],"index":"/ajar/views/index","chunking":{"stable_ids":true,"diff":"etag-per-chunk"},"license":{"read":"allowed","train":"denied","terms":"/ajar/license"}},"policy_summary":{"audience_tiers":["anonymous","signed"],"rate_limits":{"anonymous":"120/h","signed":"1200/h"},"requires_mandate_from_risk":"R2"},"issued_at":"2026-07-02T00:00:00Z","expires_at":"2026-10-01T00:00:00Z","sequence":1,"signature":{"alg":"Ed25519","kid":"owner-2026","sig":"manifestNotesSignature001"}}"#;

    const VIEW_FIXTURE: &str = r#"{"ajar_version":"0.1","url":"https://notes.example/article?x=1","content_type":"application/ajar+json","etag":"\"view-article-001\"","language":"en","chunks":[{"id":"article.title","type":"heading","content":"Example Notes","hash":"sha256:articleTitleHash001","links":[]},{"id":"article.summary","type":"paragraph","content":"Signed notes for readers and agents.","hash":"sha256:articleSummaryHash001","links":[]},{"id":"article.list","type":"list","content":"- First\n- Second","hash":"sha256:articleListHash001","links":[]}],"signature":{"alg":"Ed25519","kid":"content-2026-07","sig":"viewSignature001"}}"#;

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
