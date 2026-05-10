use std::{
    error::Error,
    fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, SyncSender, TrySendError},
        Arc, Mutex, RwLock,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::{to_bytes, Body},
    extract::{connect_info::ConnectInfo, State},
    http::{header::HOST, HeaderName, HeaderValue, Request, Response, StatusCode},
    routing::any,
    Router,
};
use serde_json::json;
use tokio::{net::TcpListener, sync::oneshot};

use crate::{HostValue, LoadedScript, Program, Result, Runtime, RuntimePolicy, ZuzuRustError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebRequest {
    pub method: String,
    pub protocol: String,
    pub scheme: String,
    pub host: String,
    pub server_name: String,
    pub server_port: u16,
    pub remote_addr: Option<String>,
    pub remote_host: Option<String>,
    pub remote_user: Option<String>,
    pub script_name: String,
    pub path: String,
    pub raw_path: String,
    pub request_uri: String,
    pub query_string: String,
    pub headers: HeaderPairs,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyTooLarge {
    pub limit: usize,
    pub actual: usize,
}

pub struct WebAppWorker {
    _runtime: Runtime,
    loaded: LoadedScript,
}

pub struct WebAppPool {
    slots: Vec<WebAppPoolSlot>,
    next_slot: Mutex<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebAppPoolResponse {
    pub worker_id: usize,
    pub response: WebAppResponse,
}

pub struct WebHttpServerHandle {
    local_addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    reloader: WebAppPoolManager,
}

#[derive(Clone)]
pub struct WebAppPoolConfig {
    pub worker_count: usize,
    pub queue_bound: usize,
    pub max_requests_per_worker: usize,
    pub module_roots: Vec<PathBuf>,
    pub runtime_policy: RuntimePolicy,
}

#[derive(Clone)]
pub struct WebHttpServerConfig {
    pub bind_addr: SocketAddr,
    pub max_body_bytes: usize,
    pub pool_config: WebAppPoolConfig,
    pub access_log: AccessLogSink,
    pub debug_responses: bool,
}

#[derive(Debug, Clone)]
pub struct AccessLogRecord {
    pub timestamp: SystemTime,
    pub remote_addr: Option<String>,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub response_bytes: usize,
    pub elapsed: Duration,
    pub worker_id: Option<usize>,
}

pub type AccessLogSink = Arc<dyn Fn(AccessLogRecord) + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessLogFormat {
    Text,
    Json,
}

#[derive(Debug)]
pub enum WebAppWorkerError {
    AppLoadFailed(ZuzuRustError),
    MissingRequestHandler,
    RequestFailed(ZuzuRustError),
    InvalidResponse(ZuzuRustError),
}

#[derive(Debug)]
pub enum WebAppPoolError {
    InvalidConfig(String),
    WorkerStartupFailed {
        slot: usize,
        source: WebAppWorkerError,
    },
    WorkerFailed {
        worker_id: usize,
        source: WebAppWorkerError,
    },
    WorkerStopped {
        worker_id: usize,
    },
    Saturated,
    NoHealthyWorkers,
}

#[derive(Debug)]
pub enum WebHttpServerError {
    Bind(std::io::Error),
    PoolStartup(WebAppPoolError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebAppResponse {
    pub status: u16,
    pub headers: HeaderPairs,
    pub body: ResponseBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderPairs(pub Vec<(String, String)>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseBody {
    Empty,
    Chunks(Vec<Vec<u8>>),
    Path(PathBuf),
}

struct WebAppPoolSlot {
    sender: Option<SyncSender<WebAppPoolJob>>,
    healthy: Arc<AtomicBool>,
    join: Mutex<Option<JoinHandle<()>>>,
}

struct WebAppPoolJob {
    request: WebRequest,
    response: mpsc::Sender<std::result::Result<WebAppPoolResponse, WebAppWorkerError>>,
}

type WebAppWorkerBuilder =
    Arc<dyn Fn(usize) -> std::result::Result<WebAppWorker, WebAppWorkerError> + Send + Sync>;

#[derive(Clone)]
pub struct WebAppPoolManager {
    current: Arc<RwLock<Arc<WebAppPool>>>,
}

#[derive(Clone)]
struct WebHttpState {
    pool: WebAppPoolManager,
    local_addr: SocketAddr,
    max_body_bytes: usize,
    module_roots: Vec<PathBuf>,
    access_log: AccessLogSink,
    debug_responses: bool,
}

struct HttpLogContext {
    remote_addr: Option<String>,
    method: String,
    path: String,
    start: Instant,
}

impl Default for WebAppPoolConfig {
    fn default() -> Self {
        Self {
            worker_count: 1,
            queue_bound: 16,
            max_requests_per_worker: 1000,
            module_roots: Vec::new(),
            runtime_policy: RuntimePolicy::default(),
        }
    }
}

impl Default for WebHttpServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:3000"
                .parse()
                .expect("default bind address should parse"),
            max_body_bytes: 1_048_576,
            pool_config: WebAppPoolConfig::default(),
            access_log: Arc::new(|record| eprintln!("{}", format_access_log_record(&record))),
            debug_responses: false,
        }
    }
}

impl std::ops::Deref for WebAppPoolResponse {
    type Target = WebAppResponse;

    fn deref(&self) -> &Self::Target {
        &self.response
    }
}

impl AccessLogFormat {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "text" => Some(Self::Text),
            "json" => Some(Self::Json),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Json => "json",
        }
    }
}

impl WebRequest {
    pub fn to_env(&self) -> HostValue {
        HostValue::Dict(
            [
                ("method".to_owned(), HostValue::String(self.method.clone())),
                (
                    "protocol".to_owned(),
                    HostValue::String(self.protocol.clone()),
                ),
                (
                    "server_protocol".to_owned(),
                    HostValue::String(self.protocol.clone()),
                ),
                ("scheme".to_owned(), HostValue::String(self.scheme.clone())),
                ("host".to_owned(), HostValue::String(self.host.clone())),
                (
                    "server_name".to_owned(),
                    HostValue::String(self.server_name.clone()),
                ),
                (
                    "server_port".to_owned(),
                    HostValue::Number(self.server_port as f64),
                ),
                (
                    "remote_addr".to_owned(),
                    self.remote_addr
                        .as_ref()
                        .map(|value| HostValue::String(value.clone()))
                        .unwrap_or(HostValue::Null),
                ),
                (
                    "remote_host".to_owned(),
                    self.remote_host
                        .as_ref()
                        .map(|value| HostValue::String(value.clone()))
                        .unwrap_or(HostValue::Null),
                ),
                (
                    "remote_user".to_owned(),
                    self.remote_user
                        .as_ref()
                        .map(|value| HostValue::String(value.clone()))
                        .unwrap_or(HostValue::Null),
                ),
                (
                    "script_name".to_owned(),
                    HostValue::String(self.script_name.clone()),
                ),
                ("path".to_owned(), HostValue::String(self.path.clone())),
                (
                    "raw_path".to_owned(),
                    HostValue::String(self.raw_path.clone()),
                ),
                (
                    "request_uri".to_owned(),
                    HostValue::String(self.request_uri.clone()),
                ),
                (
                    "query_string".to_owned(),
                    HostValue::String(self.query_string.clone()),
                ),
                (
                    "headers".to_owned(),
                    HostValue::PairList(
                        self.headers
                            .0
                            .iter()
                            .map(|(name, value)| (name.clone(), HostValue::String(value.clone())))
                            .collect(),
                    ),
                ),
                ("body".to_owned(), HostValue::Binary(self.body.clone())),
                (
                    "body_text".to_owned(),
                    String::from_utf8(self.body.clone())
                        .map(HostValue::String)
                        .unwrap_or(HostValue::Null),
                ),
            ]
            .into_iter()
            .collect(),
        )
    }

    pub fn body_len(&self) -> usize {
        self.body.len()
    }

    pub fn check_body_size(&self, max_body_bytes: usize) -> std::result::Result<(), BodyTooLarge> {
        let actual = self.body_len();
        if actual > max_body_bytes {
            return Err(BodyTooLarge {
                limit: max_body_bytes,
                actual,
            });
        }
        Ok(())
    }
}

impl WebAppWorker {
    pub fn new(
        runtime: Runtime,
        program: &Program,
        source_file: Option<&str>,
    ) -> std::result::Result<Self, WebAppWorkerError> {
        let loaded = runtime
            .load_program_without_main(program, source_file)
            .map_err(WebAppWorkerError::AppLoadFailed)?;
        if !loaded.has_function("__request__") {
            return Err(WebAppWorkerError::MissingRequestHandler);
        }
        Ok(Self {
            _runtime: runtime,
            loaded,
        })
    }

    pub fn handle(
        &self,
        request: WebRequest,
    ) -> std::result::Result<WebAppResponse, WebAppWorkerError> {
        let response = self
            .loaded
            .call_request(request.to_env())
            .map_err(WebAppWorkerError::RequestFailed)?;
        parse_web_app_response(response).map_err(WebAppWorkerError::InvalidResponse)
    }
}

impl WebAppPool {
    pub fn new(
        program: &Program,
        source_file: Option<&str>,
        config: WebAppPoolConfig,
    ) -> std::result::Result<Self, WebAppPoolError> {
        let program = Arc::new(program.clone());
        let source_file = source_file.map(str::to_owned);
        let module_roots = config.module_roots.clone();
        let runtime_policy = config.runtime_policy.clone();
        let builder: WebAppWorkerBuilder = Arc::new(move |_slot| {
            let runtime = Runtime::with_policy(module_roots.clone(), runtime_policy.clone());
            WebAppWorker::new(runtime, &program, source_file.as_deref())
        });

        Self::new_with_builder(
            config.worker_count,
            config.queue_bound,
            config.max_requests_per_worker,
            builder,
        )
    }

    fn new_with_builder(
        worker_count: usize,
        queue_bound: usize,
        max_requests_per_worker: usize,
        builder: WebAppWorkerBuilder,
    ) -> std::result::Result<Self, WebAppPoolError> {
        if worker_count == 0 {
            return Err(WebAppPoolError::InvalidConfig(
                "web app pool requires at least one worker".to_owned(),
            ));
        }

        let mut slots = Vec::with_capacity(worker_count);
        for slot_index in 0..worker_count {
            let (job_tx, job_rx) = mpsc::sync_channel(queue_bound);
            let (init_tx, init_rx) = mpsc::channel();
            let healthy = Arc::new(AtomicBool::new(true));
            let thread_builder = Arc::clone(&builder);
            let thread_healthy = Arc::clone(&healthy);
            let join = thread::spawn(move || {
                run_web_app_pool_slot(
                    slot_index,
                    job_rx,
                    init_tx,
                    thread_builder,
                    thread_healthy,
                    max_requests_per_worker,
                );
            });

            match init_rx.recv() {
                Ok(Ok(())) => slots.push(WebAppPoolSlot {
                    sender: Some(job_tx),
                    healthy,
                    join: Mutex::new(Some(join)),
                }),
                Ok(Err(source)) => {
                    let _ = join.join();
                    shutdown_web_app_pool_slots(&mut slots);
                    return Err(WebAppPoolError::WorkerStartupFailed {
                        slot: slot_index,
                        source,
                    });
                }
                Err(_) => {
                    let _ = join.join();
                    shutdown_web_app_pool_slots(&mut slots);
                    return Err(WebAppPoolError::WorkerStartupFailed {
                        slot: slot_index,
                        source: WebAppWorkerError::AppLoadFailed(ZuzuRustError::runtime(
                            "worker thread exited during startup",
                        )),
                    });
                }
            }
        }

        Ok(Self {
            slots,
            next_slot: Mutex::new(0),
        })
    }

    pub fn handle(
        &self,
        request: WebRequest,
    ) -> std::result::Result<WebAppPoolResponse, WebAppPoolError> {
        let (response_tx, response_rx) = mpsc::channel();
        let mut job = WebAppPoolJob {
            request,
            response: response_tx,
        };
        let mut next_slot = self
            .next_slot
            .lock()
            .expect("web app pool dispatcher lock should not be poisoned");
        let start = *next_slot % self.slots.len();
        let mut saw_full_queue = false;

        for offset in 0..self.slots.len() {
            let slot_index = (start + offset) % self.slots.len();
            let slot = &self.slots[slot_index];
            if !slot.healthy.load(Ordering::Acquire) {
                continue;
            }
            let Some(sender) = slot.sender.as_ref() else {
                continue;
            };
            match sender.try_send(job) {
                Ok(()) => {
                    *next_slot = (slot_index + 1) % self.slots.len();
                    drop(next_slot);
                    return match response_rx.recv() {
                        Ok(Ok(response)) => Ok(response),
                        Ok(Err(source)) => Err(WebAppPoolError::WorkerFailed {
                            worker_id: slot_index,
                            source,
                        }),
                        Err(_) => Err(WebAppPoolError::WorkerStopped {
                            worker_id: slot_index,
                        }),
                    };
                }
                Err(TrySendError::Full(returned_job)) => {
                    saw_full_queue = true;
                    job = returned_job;
                }
                Err(TrySendError::Disconnected(returned_job)) => {
                    slot.healthy.store(false, Ordering::Release);
                    job = returned_job;
                }
            }
        }

        if saw_full_queue {
            Err(WebAppPoolError::Saturated)
        } else {
            Err(WebAppPoolError::NoHealthyWorkers)
        }
    }
}

impl WebAppPoolManager {
    pub fn new(pool: WebAppPool) -> Self {
        Self {
            current: Arc::new(RwLock::new(Arc::new(pool))),
        }
    }

    pub fn current(&self) -> Arc<WebAppPool> {
        Arc::clone(
            &self
                .current
                .read()
                .expect("web app pool reload lock should not be poisoned"),
        )
    }

    pub fn replace(
        &self,
        program: &Program,
        source_file: Option<&str>,
        config: WebAppPoolConfig,
    ) -> std::result::Result<(), WebAppPoolError> {
        let new_pool = Arc::new(WebAppPool::new(program, source_file, config)?);
        *self
            .current
            .write()
            .expect("web app pool reload lock should not be poisoned") = new_pool;
        Ok(())
    }
}

impl Drop for WebAppPool {
    fn drop(&mut self) {
        shutdown_web_app_pool_slots(&mut self.slots);
    }
}

impl WebHttpServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn reloader(&self) -> WebAppPoolManager {
        self.reloader.clone()
    }

    pub async fn shutdown(mut self) -> std::io::Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(join) = self.join.take() {
            join.await.map_err(join_error_to_io)??;
        }
        Ok(())
    }
}

impl Drop for WebHttpServerHandle {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

pub async fn serve_web_app(
    program: &Program,
    source_file: Option<&str>,
    config: WebHttpServerConfig,
) -> std::result::Result<WebHttpServerHandle, WebHttpServerError> {
    let module_roots = config.pool_config.module_roots.clone();
    let pool = WebAppPool::new(program, source_file, config.pool_config)
        .map_err(WebHttpServerError::PoolStartup)?;
    let pool_manager = WebAppPoolManager::new(pool);
    let listener = TcpListener::bind(config.bind_addr)
        .await
        .map_err(WebHttpServerError::Bind)?;
    let local_addr = listener.local_addr().map_err(WebHttpServerError::Bind)?;
    let state = Arc::new(WebHttpState {
        pool: pool_manager.clone(),
        local_addr,
        max_body_bytes: config.max_body_bytes,
        module_roots,
        access_log: config.access_log,
        debug_responses: config.debug_responses,
    });
    let router = Router::new()
        .fallback(any(handle_web_http_request))
        .with_state(state);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        let _ = shutdown_rx.await;
    });
    let join = tokio::spawn(async move { server.await });

    Ok(WebHttpServerHandle {
        local_addr,
        shutdown: Some(shutdown_tx),
        join: Some(join),
        reloader: pool_manager,
    })
}

async fn handle_web_http_request(
    State(state): State<Arc<WebHttpState>>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    request: Request<Body>,
) -> Response<Body> {
    let start = Instant::now();
    let method = request.method().to_string();
    let uri = request.uri().clone();
    let path = uri.path().to_owned();
    let log_context = HttpLogContext {
        remote_addr: Some(remote_addr.to_string()),
        method: method.clone(),
        path: path.clone(),
        start,
    };

    let response_parts =
        handle_web_http_request_inner(Arc::clone(&state), remote_addr, method, uri, request).await;
    response_with_access_log(&state, log_context, response_parts)
}

async fn handle_web_http_request_inner(
    state: Arc<WebHttpState>,
    remote_addr: SocketAddr,
    method: String,
    uri: axum::http::Uri,
    request: Request<Body>,
) -> HttpResponseParts {
    let headers = request.headers().clone();
    let protocol = format_http_version(request.version());
    let body = match to_bytes(request.into_body(), state.max_body_bytes).await {
        Ok(body) => body.to_vec(),
        Err(_) => {
            return HttpResponseParts::plain(StatusCode::PAYLOAD_TOO_LARGE, "Payload Too Large\n")
        }
    };
    let host = headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .unwrap_or_else(|| state.local_addr.to_string());
    let query_string = uri.query().unwrap_or_default().to_owned();
    let raw_path = uri.path().to_owned();
    let request_uri = uri
        .path_and_query()
        .map(|value| value.as_str().to_owned())
        .unwrap_or_else(|| raw_path.clone());
    let request = WebRequest {
        method,
        protocol,
        scheme: "http".to_owned(),
        host,
        server_name: state.local_addr.ip().to_string(),
        server_port: state.local_addr.port(),
        remote_addr: Some(remote_addr.to_string()),
        remote_host: None,
        remote_user: None,
        script_name: String::new(),
        path: raw_path.clone(),
        raw_path,
        request_uri,
        query_string,
        headers: HeaderPairs(
            headers
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_owned(),
                        value.to_str().unwrap_or_default().to_owned(),
                    )
                })
                .collect(),
        ),
        body,
    };
    let pool = state.pool.current();
    match tokio::task::spawn_blocking(move || pool.handle(request)).await {
        Ok(Ok(response)) => {
            web_app_response_to_http_parts(response, &state.module_roots, state.debug_responses)
                .await
        }
        Ok(Err(err)) => pool_error_to_http_parts(err, state.debug_responses),
        Err(err) => {
            eprintln!("web app pool dispatch task failed: {err}");
            internal_server_error_parts(
                state.debug_responses,
                None,
                Some(format!("web app pool dispatch task failed: {err}")),
            )
        }
    }
}

fn format_http_version(version: axum::http::Version) -> String {
    match version {
        axum::http::Version::HTTP_09 => "HTTP/0.9".to_owned(),
        axum::http::Version::HTTP_10 => "HTTP/1.0".to_owned(),
        axum::http::Version::HTTP_11 => "HTTP/1.1".to_owned(),
        axum::http::Version::HTTP_2 => "HTTP/2.0".to_owned(),
        axum::http::Version::HTTP_3 => "HTTP/3.0".to_owned(),
        _ => format!("{version:?}"),
    }
}

struct HttpResponseParts {
    status: StatusCode,
    headers: HeaderPairs,
    body: Vec<u8>,
    worker_id: Option<usize>,
}

impl HttpResponseParts {
    fn plain(status: StatusCode, body: &str) -> Self {
        Self::plain_with_worker_id(status, body, None)
    }

    fn plain_with_worker_id(status: StatusCode, body: &str, worker_id: Option<usize>) -> Self {
        Self {
            status,
            headers: HeaderPairs(vec![(
                "Content-Type".to_owned(),
                "text/plain; charset=utf-8".to_owned(),
            )]),
            body: body.as_bytes().to_vec(),
            worker_id,
        }
    }
}

async fn web_app_response_to_http_parts(
    pool_response: WebAppPoolResponse,
    module_roots: &[PathBuf],
    debug_responses: bool,
) -> HttpResponseParts {
    let worker_id = Some(pool_response.worker_id);
    let response = pool_response.response;
    let body = match response.body {
        ResponseBody::Empty => Vec::new(),
        ResponseBody::Chunks(chunks) => chunks.into_iter().flatten().collect(),
        ResponseBody::Path(path) => {
            return path_response_to_http_parts(
                response.status,
                response.headers,
                path,
                module_roots,
                worker_id,
                debug_responses,
            )
            .await;
        }
    };
    let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    HttpResponseParts {
        status,
        headers: response.headers,
        body,
        worker_id,
    }
}

async fn path_response_to_http_parts(
    status: u16,
    headers: HeaderPairs,
    path: PathBuf,
    module_roots: &[PathBuf],
    worker_id: Option<usize>,
    debug_responses: bool,
) -> HttpResponseParts {
    let fs_path = resolve_web_fs_path(module_roots, &path);
    match tokio::fs::metadata(&fs_path).await {
        Ok(metadata) if metadata.is_dir() => {
            HttpResponseParts::plain_with_worker_id(StatusCode::FORBIDDEN, "Forbidden\n", worker_id)
        }
        Ok(_) => match tokio::fs::read(&fs_path).await {
            Ok(body) => {
                let status =
                    StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                HttpResponseParts {
                    status,
                    headers: with_inferred_content_type(headers, &fs_path),
                    body,
                    worker_id,
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                HttpResponseParts::plain_with_worker_id(
                    StatusCode::NOT_FOUND,
                    "Not Found\n",
                    worker_id,
                )
            }
            Err(err) => {
                eprintln!("web static file read failed: {err}");
                internal_server_error_parts(
                    debug_responses,
                    worker_id,
                    Some(format!("web static file read failed: {err}")),
                )
            }
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            HttpResponseParts::plain_with_worker_id(StatusCode::NOT_FOUND, "Not Found\n", worker_id)
        }
        Err(err) => {
            eprintln!("web static file metadata lookup failed: {err}");
            internal_server_error_parts(
                debug_responses,
                worker_id,
                Some(format!("web static file metadata lookup failed: {err}")),
            )
        }
    }
}

fn with_inferred_content_type(mut headers: HeaderPairs, path: &Path) -> HeaderPairs {
    if !headers
        .0
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("Content-Type"))
    {
        headers.0.push((
            "Content-Type".to_owned(),
            mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string(),
        ));
    }
    headers
}

fn resolve_web_fs_path(module_roots: &[PathBuf], path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        web_repo_root(module_roots).join(path)
    }
}

fn web_repo_root(module_roots: &[PathBuf]) -> PathBuf {
    if let Ok(mut current) = std::env::current_dir() {
        loop {
            if current.join("modules").join("std").is_dir()
                || current.join("stdlib").join("modules").join("std").is_dir()
            {
                return current;
            }
            if !current.pop() {
                break;
            }
        }
    }

    for module_root in module_roots {
        if module_root.join("std").is_dir() {
            if let Some(root) = module_root.parent() {
                if root.file_name().and_then(|name| name.to_str()) == Some("stdlib") {
                    if let Some(project_root) = root.parent() {
                        return project_root.to_path_buf();
                    }
                }
                return root.to_path_buf();
            }
        }
    }

    module_roots
        .first()
        .and_then(|module_root| module_root.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}

fn pool_error_to_http_parts(err: WebAppPoolError, debug_responses: bool) -> HttpResponseParts {
    match err {
        WebAppPoolError::Saturated | WebAppPoolError::NoHealthyWorkers => {
            eprintln!("web app pool unavailable: {err}");
            HttpResponseParts::plain(StatusCode::SERVICE_UNAVAILABLE, "Service Unavailable\n")
        }
        WebAppPoolError::WorkerStartupFailed { .. } | WebAppPoolError::InvalidConfig(_) => {
            eprintln!("web app pool configuration/startup error during request: {err}");
            internal_server_error_parts(debug_responses, err.worker_id(), Some(err.to_string()))
        }
        WebAppPoolError::WorkerFailed { .. } | WebAppPoolError::WorkerStopped { .. } => {
            eprintln!("web app worker request failed: {err}");
            internal_server_error_parts(debug_responses, err.worker_id(), Some(err.to_string()))
        }
    }
}

fn internal_server_error_parts(
    debug_responses: bool,
    worker_id: Option<usize>,
    detail: Option<String>,
) -> HttpResponseParts {
    if debug_responses {
        let detail = detail.unwrap_or_else(|| "unknown web server error".to_owned());
        HttpResponseParts::plain_with_worker_id(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Internal Server Error\n{detail}\n"),
            worker_id,
        )
    } else {
        HttpResponseParts::plain_with_worker_id(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal Server Error\n",
            worker_id,
        )
    }
}

fn response_with_access_log(
    state: &WebHttpState,
    context: HttpLogContext,
    parts: HttpResponseParts,
) -> Response<Body> {
    let response_bytes = parts.body.len();
    let status = parts.status;
    let mut builder = Response::builder().status(status);
    if let Some(headers) = builder.headers_mut() {
        for (name, value) in parts.headers.0 {
            if let (Ok(name), Ok(value)) = (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_str(&value),
            ) {
                headers.append(name, value);
            }
        }
    }
    let response = builder
        .body(Body::from(parts.body))
        .unwrap_or_else(|_| Response::new(Body::from("Internal Server Error\n")));
    (state.access_log)(AccessLogRecord {
        timestamp: SystemTime::now(),
        remote_addr: context.remote_addr,
        method: context.method,
        path: context.path,
        status: status.as_u16(),
        response_bytes,
        elapsed: context.start.elapsed(),
        worker_id: parts.worker_id,
    });
    response
}

pub fn format_access_log_record(record: &AccessLogRecord) -> String {
    let timestamp = record
        .timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    let remote_addr = record.remote_addr.as_deref().unwrap_or("-");
    let worker_id = record
        .worker_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| "-".to_owned());
    format!(
        "{timestamp:.3} {remote_addr} \"{} {}\" {} {} {}ms worker={worker_id}",
        record.method,
        record.path,
        record.status,
        record.response_bytes,
        record.elapsed.as_millis(),
    )
}

pub fn format_access_log_record_json(record: &AccessLogRecord) -> String {
    let timestamp = record
        .timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    json!({
        "timestamp": timestamp,
        "remote_addr": record.remote_addr.as_deref(),
        "method": record.method.as_str(),
        "path": record.path.as_str(),
        "status": record.status,
        "response_bytes": record.response_bytes,
        "elapsed_ms": record.elapsed.as_millis(),
        "worker_id": record.worker_id,
    })
    .to_string()
}

fn join_error_to_io(err: tokio::task::JoinError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, err)
}

fn run_web_app_pool_slot(
    slot_index: usize,
    receiver: mpsc::Receiver<WebAppPoolJob>,
    init_sender: mpsc::Sender<std::result::Result<(), WebAppWorkerError>>,
    builder: WebAppWorkerBuilder,
    healthy: Arc<AtomicBool>,
    max_requests_per_worker: usize,
) {
    let mut worker = match builder(slot_index) {
        Ok(worker) => {
            let _ = init_sender.send(Ok(()));
            worker
        }
        Err(err) => {
            healthy.store(false, Ordering::Release);
            let _ = init_sender.send(Err(err));
            return;
        }
    };
    let mut handled_requests = 0usize;

    while let Ok(job) = receiver.recv() {
        let result = worker
            .handle(job.request)
            .map(|response| WebAppPoolResponse {
                worker_id: slot_index,
                response,
            });
        handled_requests += 1;

        let replacement_failed =
            if max_requests_per_worker > 0 && handled_requests >= max_requests_per_worker {
                healthy.store(false, Ordering::Release);
                match builder(slot_index) {
                    Ok(new_worker) => {
                        worker = new_worker;
                        handled_requests = 0;
                        healthy.store(true, Ordering::Release);
                        false
                    }
                    Err(_err) => true,
                }
            } else {
                false
            };

        let _ = job.response.send(result);

        if replacement_failed {
            healthy.store(false, Ordering::Release);
            break;
        }
    }

    healthy.store(false, Ordering::Release);
}

fn shutdown_web_app_pool_slots(slots: &mut [WebAppPoolSlot]) {
    for slot in slots.iter_mut() {
        slot.sender.take();
    }
    for slot in slots.iter_mut() {
        if let Ok(join) = slot.join.get_mut() {
            if let Some(join) = join.take() {
                let _ = join.join();
            }
        }
    }
}

impl fmt::Display for WebAppWorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AppLoadFailed(err) => write!(f, "web app load failed: {err}"),
            Self::MissingRequestHandler => {
                write!(f, "web app is missing __request__ handler")
            }
            Self::RequestFailed(err) => write!(f, "web app request failed: {err}"),
            Self::InvalidResponse(err) => write!(f, "web app response is invalid: {err}"),
        }
    }
}

impl Error for WebAppWorkerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::AppLoadFailed(err) | Self::RequestFailed(err) | Self::InvalidResponse(err) => {
                Some(err)
            }
            Self::MissingRequestHandler => None,
        }
    }
}

impl fmt::Display for WebAppPoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => {
                write!(f, "web app pool configuration is invalid: {message}")
            }
            Self::WorkerStartupFailed { slot, source } => {
                write!(f, "web app pool worker {slot} failed to start: {source}")
            }
            Self::WorkerFailed { worker_id, source } => write!(
                f,
                "web app pool worker {worker_id} failed request: {source}"
            ),
            Self::WorkerStopped { worker_id } => {
                write!(f, "web app pool worker {worker_id} stopped before replying")
            }
            Self::Saturated => write!(f, "web app pool queues are full"),
            Self::NoHealthyWorkers => write!(f, "web app pool has no healthy workers"),
        }
    }
}

impl Error for WebAppPoolError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::WorkerStartupFailed { source, .. } | Self::WorkerFailed { source, .. } => {
                Some(source)
            }
            Self::InvalidConfig(_)
            | Self::WorkerStopped { .. }
            | Self::Saturated
            | Self::NoHealthyWorkers => None,
        }
    }
}

impl WebAppPoolError {
    fn worker_id(&self) -> Option<usize> {
        match self {
            Self::WorkerFailed { worker_id, .. } | Self::WorkerStopped { worker_id } => {
                Some(*worker_id)
            }
            _ => None,
        }
    }
}

impl fmt::Display for WebHttpServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind(err) => write!(f, "web HTTP server bind failed: {err}"),
            Self::PoolStartup(err) => write!(f, "web app pool startup failed: {err}"),
        }
    }
}

impl Error for WebHttpServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Bind(err) => Some(err),
            Self::PoolStartup(err) => Some(err),
        }
    }
}

pub fn parse_web_app_response(value: HostValue) -> Result<WebAppResponse> {
    let HostValue::Array(mut values) = value else {
        return Err(web_response_error("web response must be an Array"));
    };
    if values.len() != 3 {
        return Err(web_response_error(
            "web response must be [status, headers, body]",
        ));
    }
    let body = values.pop().expect("checked response arity");
    let headers = values.pop().expect("checked response arity");
    let status = values.pop().expect("checked response arity");

    Ok(WebAppResponse {
        status: parse_status(status)?,
        headers: parse_headers(headers)?,
        body: parse_body(body)?,
    })
}

fn parse_status(value: HostValue) -> Result<u16> {
    let HostValue::Number(status) = value else {
        return Err(web_response_error(
            "web response status must be an integer from 100 to 599",
        ));
    };
    if !status.is_finite() || status.fract() != 0.0 || !(100.0..=599.0).contains(&status) {
        return Err(web_response_error(
            "web response status must be an integer from 100 to 599",
        ));
    }
    Ok(status as u16)
}

fn parse_headers(value: HostValue) -> Result<HeaderPairs> {
    match value {
        HostValue::PairList(values) => values
            .into_iter()
            .map(parse_header_pair)
            .collect::<Result<Vec<_>>>()
            .map(HeaderPairs),
        HostValue::Dict(values) => {
            let mut values = values.into_iter().collect::<Vec<_>>();
            values.sort_by(|(left, _), (right, _)| left.cmp(right));
            values
                .into_iter()
                .map(parse_header_pair)
                .collect::<Result<Vec<_>>>()
                .map(HeaderPairs)
        }
        _ => Err(web_response_error(
            "web response headers must be a PairList or Dict",
        )),
    }
}

fn parse_header_pair((name, value): (String, HostValue)) -> Result<(String, String)> {
    validate_header_name(&name)?;
    let HostValue::String(value) = value else {
        return Err(web_response_error(
            "web response header values must be Strings",
        ));
    };
    validate_header_value(&value)?;
    Ok((name, value))
}

fn parse_body(value: HostValue) -> Result<ResponseBody> {
    match value {
        HostValue::Null => Ok(ResponseBody::Empty),
        HostValue::String(value) => Ok(ResponseBody::Chunks(vec![value.into_bytes()])),
        HostValue::Binary(value) => Ok(ResponseBody::Chunks(vec![value])),
        HostValue::Array(values) => values
            .into_iter()
            .map(parse_body_chunk)
            .collect::<Result<Vec<_>>>()
            .map(ResponseBody::Chunks),
        HostValue::Path(path) => Ok(ResponseBody::Path(path)),
        _ => Err(web_response_error(
            "web response body must be null, String, BinaryString, Array, or Path",
        )),
    }
}

fn parse_body_chunk(value: HostValue) -> Result<Vec<u8>> {
    match value {
        HostValue::String(value) => Ok(value.into_bytes()),
        HostValue::Binary(value) => Ok(value),
        _ => Err(web_response_error(
            "web response body array items must be Strings or BinaryStrings",
        )),
    }
}

fn validate_header_name(name: &str) -> Result<()> {
    if name.is_empty() || !name.bytes().all(is_header_name_byte) {
        return Err(web_response_error("web response header name is invalid"));
    }
    Ok(())
}

fn validate_header_value(value: &str) -> Result<()> {
    if value.bytes().any(|byte| matches!(byte, b'\r' | b'\n' | 0)) {
        return Err(web_response_error("web response header value is invalid"));
    }
    Ok(())
}

fn is_header_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn web_response_error(message: impl Into<String>) -> ZuzuRustError {
    ZuzuRustError::runtime(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parse_program, parse_program_with_options};
    use std::{
        collections::HashMap,
        fs,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        thread,
        time::Duration,
    };

    fn response(status: HostValue, headers: HostValue, body: HostValue) -> HostValue {
        HostValue::Array(vec![status, headers, body])
    }

    fn empty_headers() -> HostValue {
        HostValue::PairList(Vec::new())
    }

    fn web_request(body: Vec<u8>) -> WebRequest {
        WebRequest {
            method: "POST".to_owned(),
            protocol: "HTTP/1.1".to_owned(),
            scheme: "http".to_owned(),
            host: "example.com".to_owned(),
            server_name: "127.0.0.1".to_owned(),
            server_port: 3000,
            remote_addr: Some("127.0.0.1:4000".to_owned()),
            remote_host: None,
            remote_user: None,
            script_name: String::new(),
            path: "/submit".to_owned(),
            raw_path: "/submit%20raw".to_owned(),
            request_uri: "/submit%20raw?a=1&b=2".to_owned(),
            query_string: "a=1&b=2".to_owned(),
            headers: HeaderPairs(vec![
                ("X-Test".to_owned(), "first".to_owned()),
                ("X-Test".to_owned(), "second".to_owned()),
            ]),
            body,
        }
    }

    fn web_worker(source: &str) -> std::result::Result<WebAppWorker, WebAppWorkerError> {
        let program = parse_program(source).expect("web app should parse");
        WebAppWorker::new(Runtime::new(Vec::new()), &program, Some("app.zzs"))
    }

    fn web_pool(
        source: &str,
        config: WebAppPoolConfig,
    ) -> std::result::Result<WebAppPool, WebAppPoolError> {
        let program = parse_program(source).expect("web app should parse");
        WebAppPool::new(&program, Some("app.zzs"), config)
    }

    fn stateful_worker() -> std::result::Result<WebAppWorker, WebAppWorkerError> {
        web_worker(
            r#"
            let counter := 0;

            function __request__ ( env ) {
                counter := counter + 1;
                return [ 200 + counter, {{}}, [] ];
            }
            "#,
        )
    }

    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    fn repo_module_roots() -> Vec<PathBuf> {
        let repo_root = repo_root();
        vec![
            repo_root.join("modules"),
            repo_root.join("stdlib").join("modules"),
        ]
    }

    fn logged_http_config(
        max_body_bytes: usize,
        pool_config: WebAppPoolConfig,
    ) -> (WebHttpServerConfig, Arc<Mutex<Vec<AccessLogRecord>>>) {
        logged_http_config_with_debug(max_body_bytes, pool_config, false)
    }

    fn logged_http_config_with_debug(
        max_body_bytes: usize,
        pool_config: WebAppPoolConfig,
        debug_responses: bool,
    ) -> (WebHttpServerConfig, Arc<Mutex<Vec<AccessLogRecord>>>) {
        let records = Arc::new(Mutex::new(Vec::new()));
        let sink_records = Arc::clone(&records);
        (
            WebHttpServerConfig {
                bind_addr: "127.0.0.1:0".parse().expect("test bind addr should parse"),
                max_body_bytes,
                pool_config,
                access_log: Arc::new(move |record| {
                    sink_records
                        .lock()
                        .expect("access log records lock should not be poisoned")
                        .push(record);
                }),
                debug_responses,
            },
            records,
        )
    }

    async fn start_logged_http_server(
        source: &str,
        max_body_bytes: usize,
        pool_config: WebAppPoolConfig,
    ) -> (
        WebHttpServerHandle,
        String,
        Arc<Mutex<Vec<AccessLogRecord>>>,
    ) {
        let program = parse_program(source).expect("web app should parse");
        let (config, records) = logged_http_config(max_body_bytes, pool_config);
        let server = serve_web_app(&program, Some("app.zzs"), config)
            .await
            .expect("web HTTP server should start");
        let base_url = format!("http://{}", server.local_addr());
        (server, base_url, records)
    }

    async fn start_debug_http_server(
        source: &str,
    ) -> (
        WebHttpServerHandle,
        String,
        Arc<Mutex<Vec<AccessLogRecord>>>,
    ) {
        let program = parse_program(source).expect("web app should parse");
        let (config, records) =
            logged_http_config_with_debug(1_048_576, WebAppPoolConfig::default(), true);
        let server = serve_web_app(&program, Some("app.zzs"), config)
            .await
            .expect("web HTTP server should start");
        let base_url = format!("http://{}", server.local_addr());
        (server, base_url, records)
    }

    fn assert_error_contains(value: HostValue, expected: &str) {
        let err = parse_web_app_response(value).expect_err("response should fail");
        assert!(
            err.to_string().contains(expected),
            "expected error containing {expected:?}, got {err}"
        );
    }

    #[test]
    fn formats_access_logs_as_text_and_json() {
        let record = AccessLogRecord {
            timestamp: UNIX_EPOCH + Duration::from_millis(1500),
            remote_addr: Some("127.0.0.1:4000".to_owned()),
            method: "GET".to_owned(),
            path: "/hello".to_owned(),
            status: 201,
            response_bytes: 12,
            elapsed: Duration::from_millis(34),
            worker_id: Some(3),
        };

        let text = format_access_log_record(&record);
        assert!(text.contains("\"GET /hello\""));
        assert!(text.contains(" 201 12 34ms worker=3"));

        let json: serde_json::Value = serde_json::from_str(&format_access_log_record_json(&record))
            .expect("JSON access log should parse");
        assert_eq!(json["timestamp"].as_f64(), Some(1.5));
        assert_eq!(json["remote_addr"], "127.0.0.1:4000");
        assert_eq!(json["method"], "GET");
        assert_eq!(json["path"], "/hello");
        assert_eq!(json["status"].as_u64(), Some(201));
        assert_eq!(json["response_bytes"].as_u64(), Some(12));
        assert_eq!(json["elapsed_ms"].as_u64(), Some(34));
        assert_eq!(json["worker_id"].as_u64(), Some(3));
    }

    #[test]
    fn parses_string_body_as_one_chunk() {
        let parsed = parse_web_app_response(response(
            HostValue::Number(200.0),
            HostValue::PairList(vec![(
                "Content-Type".to_owned(),
                HostValue::String("text/plain".to_owned()),
            )]),
            HostValue::String("hello".to_owned()),
        ))
        .expect("response should parse");

        assert_eq!(parsed.status, 200);
        assert_eq!(
            parsed.headers,
            HeaderPairs(vec![("Content-Type".to_owned(), "text/plain".to_owned())])
        );
        assert_eq!(parsed.body, ResponseBody::Chunks(vec![b"hello".to_vec()]));
    }

    #[test]
    fn parses_binary_null_chunked_and_path_bodies() {
        let binary = parse_web_app_response(response(
            HostValue::Number(201.0),
            empty_headers(),
            HostValue::Binary(vec![1, 2, 3]),
        ))
        .expect("binary response should parse");
        assert_eq!(binary.body, ResponseBody::Chunks(vec![vec![1, 2, 3]]));

        let empty = parse_web_app_response(response(
            HostValue::Number(204.0),
            empty_headers(),
            HostValue::Null,
        ))
        .expect("empty response should parse");
        assert_eq!(empty.body, ResponseBody::Empty);

        let chunked = parse_web_app_response(response(
            HostValue::Number(200.0),
            empty_headers(),
            HostValue::Array(vec![
                HostValue::String("a".to_owned()),
                HostValue::Binary(vec![98]),
            ]),
        ))
        .expect("chunked response should parse");
        assert_eq!(
            chunked.body,
            ResponseBody::Chunks(vec![b"a".to_vec(), b"b".to_vec()])
        );

        let path = parse_web_app_response(response(
            HostValue::Number(200.0),
            empty_headers(),
            HostValue::Path(PathBuf::from("asset.txt")),
        ))
        .expect("path response should parse");
        assert_eq!(path.body, ResponseBody::Path(PathBuf::from("asset.txt")));
    }

    #[test]
    fn parses_pairlist_headers_in_order_with_duplicates() {
        let parsed = parse_web_app_response(response(
            HostValue::Number(200.0),
            HostValue::PairList(vec![
                ("Set-Cookie".to_owned(), HostValue::String("a=1".to_owned())),
                ("X-Test".to_owned(), HostValue::String("ok".to_owned())),
                ("Set-Cookie".to_owned(), HostValue::String("b=2".to_owned())),
            ]),
            HostValue::Null,
        ))
        .expect("response should parse");

        assert_eq!(
            parsed.headers,
            HeaderPairs(vec![
                ("Set-Cookie".to_owned(), "a=1".to_owned()),
                ("X-Test".to_owned(), "ok".to_owned()),
                ("Set-Cookie".to_owned(), "b=2".to_owned()),
            ])
        );
    }

    #[test]
    fn parses_dict_headers_in_sorted_order() {
        let parsed = parse_web_app_response(response(
            HostValue::Number(200.0),
            HostValue::Dict(HashMap::from([
                ("X-Zed".to_owned(), HostValue::String("last".to_owned())),
                (
                    "Content-Type".to_owned(),
                    HostValue::String("text/plain".to_owned()),
                ),
            ])),
            HostValue::Null,
        ))
        .expect("response should parse");

        assert_eq!(
            parsed.headers,
            HeaderPairs(vec![
                ("Content-Type".to_owned(), "text/plain".to_owned()),
                ("X-Zed".to_owned(), "last".to_owned()),
            ])
        );
    }

    #[test]
    fn rejects_invalid_top_level_response_shapes() {
        assert_error_contains(HostValue::Null, "web response must be an Array");
        assert_error_contains(
            HostValue::Array(vec![HostValue::Number(200.0), empty_headers()]),
            "web response must be [status, headers, body]",
        );
    }

    #[test]
    fn rejects_invalid_status_values() {
        for status in [
            HostValue::String("200".to_owned()),
            HostValue::Number(99.0),
            HostValue::Number(600.0),
            HostValue::Number(200.5),
            HostValue::Number(f64::NAN),
            HostValue::Number(f64::INFINITY),
        ] {
            assert_error_contains(
                response(status, empty_headers(), HostValue::Null),
                "web response status must be an integer from 100 to 599",
            );
        }
    }

    #[test]
    fn rejects_invalid_headers() {
        assert_error_contains(
            response(
                HostValue::Number(200.0),
                HostValue::String("nope".to_owned()),
                HostValue::Null,
            ),
            "web response headers must be a PairList or Dict",
        );
        assert_error_contains(
            response(
                HostValue::Number(200.0),
                HostValue::PairList(vec![("X-Test".to_owned(), HostValue::Number(1.0))]),
                HostValue::Null,
            ),
            "web response header values must be Strings",
        );
        for name in ["", "Content Type", "Content:Type", "Bad\nName", "X-☃"] {
            assert_error_contains(
                response(
                    HostValue::Number(200.0),
                    HostValue::PairList(vec![(
                        name.to_owned(),
                        HostValue::String("ok".to_owned()),
                    )]),
                    HostValue::Null,
                ),
                "web response header name is invalid",
            );
        }
        for value in ["bad\rvalue", "bad\nvalue", "bad\0value"] {
            assert_error_contains(
                response(
                    HostValue::Number(200.0),
                    HostValue::PairList(vec![(
                        "X-Test".to_owned(),
                        HostValue::String(value.to_owned()),
                    )]),
                    HostValue::Null,
                ),
                "web response header value is invalid",
            );
        }
    }

    #[test]
    fn rejects_unsupported_body_values() {
        assert_error_contains(
            response(
                HostValue::Number(200.0),
                empty_headers(),
                HostValue::Bool(true),
            ),
            "web response body must be null, String, BinaryString, Array, or Path",
        );
        assert_error_contains(
            response(
                HostValue::Number(200.0),
                empty_headers(),
                HostValue::Array(vec![HostValue::Number(1.0)]),
            ),
            "web response body array items must be Strings or BinaryStrings",
        );
    }

    #[test]
    fn converts_request_to_complete_env_dict() {
        let request = web_request("hello".as_bytes().to_vec());
        let HostValue::Dict(env) = request.to_env() else {
            panic!("request env should be a Dict");
        };

        assert_eq!(
            env.get("method"),
            Some(&HostValue::String("POST".to_owned()))
        );
        assert_eq!(
            env.get("scheme"),
            Some(&HostValue::String("http".to_owned()))
        );
        assert_eq!(
            env.get("host"),
            Some(&HostValue::String("example.com".to_owned()))
        );
        assert_eq!(
            env.get("server_name"),
            Some(&HostValue::String("127.0.0.1".to_owned()))
        );
        assert_eq!(env.get("server_port"), Some(&HostValue::Number(3000.0)));
        assert_eq!(
            env.get("remote_addr"),
            Some(&HostValue::String("127.0.0.1:4000".to_owned()))
        );
        assert_eq!(
            env.get("path"),
            Some(&HostValue::String("/submit".to_owned()))
        );
        assert_eq!(
            env.get("raw_path"),
            Some(&HostValue::String("/submit%20raw".to_owned()))
        );
        assert_eq!(
            env.get("query_string"),
            Some(&HostValue::String("a=1&b=2".to_owned()))
        );
        assert_eq!(env.get("body"), Some(&HostValue::Binary(b"hello".to_vec())));
        assert_eq!(
            env.get("body_text"),
            Some(&HostValue::String("hello".to_owned()))
        );
    }

    #[test]
    fn request_env_preserves_duplicate_headers() {
        let request = web_request(Vec::new());
        let HostValue::Dict(env) = request.to_env() else {
            panic!("request env should be a Dict");
        };

        assert_eq!(
            env.get("headers"),
            Some(&HostValue::PairList(vec![
                ("X-Test".to_owned(), HostValue::String("first".to_owned())),
                ("X-Test".to_owned(), HostValue::String("second".to_owned())),
            ]))
        );
    }

    #[test]
    fn request_env_uses_null_body_text_for_invalid_utf8() {
        let request = web_request(vec![0xff, 0xfe]);
        let HostValue::Dict(env) = request.to_env() else {
            panic!("request env should be a Dict");
        };

        assert_eq!(env.get("body"), Some(&HostValue::Binary(vec![0xff, 0xfe])));
        assert_eq!(env.get("body_text"), Some(&HostValue::Null));
    }

    #[test]
    fn request_env_uses_null_for_absent_remote_addr() {
        let mut request = web_request(Vec::new());
        request.remote_addr = None;
        let HostValue::Dict(env) = request.to_env() else {
            panic!("request env should be a Dict");
        };

        assert_eq!(env.get("remote_addr"), Some(&HostValue::Null));
    }

    #[test]
    fn request_body_size_limit_reports_actual_size() {
        let request = web_request(vec![1, 2, 3]);

        assert_eq!(request.body_len(), 3);
        assert_eq!(request.check_body_size(3), Ok(()));
        assert_eq!(
            request.check_body_size(2),
            Err(BodyTooLarge {
                limit: 2,
                actual: 3,
            })
        );
        assert_eq!(
            request.check_body_size(0),
            Err(BodyTooLarge {
                limit: 0,
                actual: 3,
            })
        );
    }

    #[test]
    fn worker_handles_complete_request_response_round_trip() {
        let worker = web_worker(
            r#"
            function __request__ ( env ) {
                return [
                    201,
                    {{}},
                    [ env{method}, env{path}, env{body_text} ],
                ];
            }
            "#,
        )
        .expect("worker should load");

        let response = worker
            .handle(web_request(b"hello".to_vec()))
            .expect("request should succeed");

        assert_eq!(response.status, 201);
        assert_eq!(response.headers, HeaderPairs(Vec::new()));
        assert_eq!(
            response.body,
            ResponseBody::Chunks(vec![
                b"POST".to_vec(),
                b"/submit".to_vec(),
                b"hello".to_vec(),
            ])
        );
    }

    #[test]
    fn worker_startup_fails_without_request_handler() {
        let err = match web_worker("function helper () { return 1; }") {
            Ok(_) => panic!("missing __request__ should fail"),
            Err(err) => err,
        };

        assert!(matches!(err, WebAppWorkerError::MissingRequestHandler));
    }

    #[test]
    fn worker_startup_reports_app_load_failure() {
        let program = parse_program_with_options("return 1;", false, true)
            .expect("program should parse without semantic validation");

        let err = match WebAppWorker::new(Runtime::new(Vec::new()), &program, Some("app.zzs")) {
            Ok(_) => panic!("top-level return should fail worker startup"),
            Err(err) => err,
        };

        match err {
            WebAppWorkerError::AppLoadFailed(err) => assert!(err
                .to_string()
                .contains("return is not valid at top-level scope")),
            other => panic!("expected AppLoadFailed, got {other:?}"),
        }
    }

    #[test]
    fn worker_returns_request_error_for_thrown_exception() {
        let worker = web_worker(
            r#"
            function __request__ ( env ) {
                throw new Exception( message: "boom" );
            }
            "#,
        )
        .expect("worker should load");

        let err = worker
            .handle(web_request(Vec::new()))
            .expect_err("thrown exception should fail request");

        match err {
            WebAppWorkerError::RequestFailed(err) => {
                assert!(err.to_string().contains("boom"));
            }
            other => panic!("expected RequestFailed, got {other:?}"),
        }
    }

    #[test]
    fn worker_returns_invalid_response_error_for_bad_contract() {
        let worker = web_worker(
            r#"
            function __request__ ( env ) {
                return "not a response";
            }
            "#,
        )
        .expect("worker should load");

        let err = worker
            .handle(web_request(Vec::new()))
            .expect_err("invalid response should fail request");

        match err {
            WebAppWorkerError::InvalidResponse(err) => {
                assert!(err.to_string().contains("web response must be an Array"));
            }
            other => panic!("expected InvalidResponse, got {other:?}"),
        }
    }

    #[test]
    fn worker_retains_top_level_state_across_requests() {
        let worker = web_worker(
            r#"
            let counter := 0;

            function __request__ ( env ) {
                counter := counter + 1;
                return [ 200 + counter, {{}}, [] ];
            }
            "#,
        )
        .expect("worker should load");

        let first = worker
            .handle(web_request(Vec::new()))
            .expect("first request should succeed");
        let second = worker
            .handle(web_request(Vec::new()))
            .expect("second request should succeed");

        assert_eq!(first.status, 201);
        assert_eq!(second.status, 202);
    }

    #[test]
    fn separate_workers_have_separate_top_level_state() {
        let source = r#"
            let counter := 0;

            function __request__ ( env ) {
                counter := counter + 1;
                return [ 200 + counter, {{}}, [] ];
            }
            "#;
        let first_worker = web_worker(source).expect("first worker should load");
        let second_worker = web_worker(source).expect("second worker should load");

        let first_response = first_worker
            .handle(web_request(Vec::new()))
            .expect("first worker request should succeed");
        let second_response = second_worker
            .handle(web_request(Vec::new()))
            .expect("second worker request should succeed");
        let first_second_response = first_worker
            .handle(web_request(Vec::new()))
            .expect("first worker second request should succeed");

        assert_eq!(first_response.status, 201);
        assert_eq!(second_response.status, 201);
        assert_eq!(first_second_response.status, 202);
    }

    #[test]
    fn pool_handles_multiple_concurrent_jobs() {
        let pool = Arc::new(
            web_pool(
                r#"
                function __request__ ( env ) {
                    return [ 200, {{}}, [] ];
                }
                "#,
                WebAppPoolConfig {
                    worker_count: 2,
                    queue_bound: 2,
                    max_requests_per_worker: 0,
                    ..WebAppPoolConfig::default()
                },
            )
            .expect("pool should start"),
        );
        let handles = (0..4)
            .map(|_| {
                let pool = Arc::clone(&pool);
                thread::spawn(move || {
                    pool.handle(web_request(Vec::new()))
                        .expect("request should complete")
                        .status
                })
            })
            .collect::<Vec<_>>();

        let statuses = handles
            .into_iter()
            .map(|handle| handle.join().expect("request thread should join"))
            .collect::<Vec<_>>();

        assert_eq!(statuses, vec![200, 200, 200, 200]);
    }

    #[test]
    fn pool_reports_worker_id_for_completed_request() {
        let pool = web_pool(
            r#"
            function __request__ ( env ) {
                return [ 200, {{}}, [] ];
            }
            "#,
            WebAppPoolConfig {
                worker_count: 1,
                queue_bound: 1,
                max_requests_per_worker: 0,
                ..WebAppPoolConfig::default()
            },
        )
        .expect("pool should start");

        let response = pool
            .handle(web_request(Vec::new()))
            .expect("request should complete");

        assert_eq!(response.worker_id, 0);
        assert_eq!(response.status, 200);
    }

    #[test]
    fn pool_manager_replaces_pool_for_subsequent_requests() {
        let config = WebAppPoolConfig {
            worker_count: 1,
            queue_bound: 1,
            max_requests_per_worker: 0,
            ..WebAppPoolConfig::default()
        };
        let first_program = parse_program(
            r#"
            function __request__ ( env ) {
                return [ 201, {{}}, [] ];
            }
            "#,
        )
        .expect("first web app should parse");
        let second_program = parse_program(
            r#"
            function __request__ ( env ) {
                return [ 202, {{}}, [] ];
            }
            "#,
        )
        .expect("second web app should parse");
        let manager = WebAppPoolManager::new(
            WebAppPool::new(&first_program, Some("app.zzs"), config.clone())
                .expect("initial pool should start"),
        );

        assert_eq!(
            manager
                .current()
                .handle(web_request(Vec::new()))
                .expect("initial request should complete")
                .status,
            201
        );
        manager
            .replace(&second_program, Some("app.zzs"), config)
            .expect("replacement pool should start");
        assert_eq!(
            manager
                .current()
                .handle(web_request(Vec::new()))
                .expect("replacement request should complete")
                .status,
            202
        );
    }

    #[test]
    fn pool_manager_failed_replacement_keeps_current_pool() {
        let config = WebAppPoolConfig {
            worker_count: 1,
            queue_bound: 1,
            max_requests_per_worker: 0,
            ..WebAppPoolConfig::default()
        };
        let good_program = parse_program(
            r#"
            function __request__ ( env ) {
                return [ 203, {{}}, [] ];
            }
            "#,
        )
        .expect("good web app should parse");
        let bad_program = parse_program("function helper () { return 1; }")
            .expect("bad replacement should parse");
        let manager = WebAppPoolManager::new(
            WebAppPool::new(&good_program, Some("app.zzs"), config.clone())
                .expect("initial pool should start"),
        );

        manager
            .replace(&bad_program, Some("app.zzs"), config)
            .expect_err("replacement missing __request__ should fail");

        assert_eq!(
            manager
                .current()
                .handle(web_request(Vec::new()))
                .expect("current pool should still serve")
                .status,
            203
        );
    }

    #[test]
    fn pool_returns_saturation_error_when_all_queues_are_full() {
        let pool = Arc::new(
            web_pool(
                r#"
                from std/task import sleep;

                async function __request__ ( env ) {
                    await { sleep(0.2); };
                    return [ 200, {{}}, [] ];
                }
                "#,
                WebAppPoolConfig {
                    worker_count: 1,
                    queue_bound: 0,
                    max_requests_per_worker: 0,
                    ..WebAppPoolConfig::default()
                },
            )
            .expect("pool should start"),
        );
        let busy_pool = Arc::clone(&pool);
        let busy = thread::spawn(move || {
            busy_pool
                .handle(web_request(Vec::new()))
                .expect("busy request should eventually complete")
        });

        thread::sleep(Duration::from_millis(30));
        let err = pool
            .handle(web_request(Vec::new()))
            .expect_err("second request should saturate the pool");

        assert!(matches!(err, WebAppPoolError::Saturated));
        assert_eq!(
            busy.join().expect("busy request thread should join").status,
            200
        );
    }

    #[test]
    fn pool_recycles_workers_after_request_limit() {
        let pool = web_pool(
            r#"
            let counter := 0;

            function __request__ ( env ) {
                counter := counter + 1;
                return [ 200 + counter, {{}}, [] ];
            }
            "#,
            WebAppPoolConfig {
                worker_count: 1,
                queue_bound: 1,
                max_requests_per_worker: 2,
                ..WebAppPoolConfig::default()
            },
        )
        .expect("pool should start");

        let first = pool.handle(web_request(Vec::new())).expect("first request");
        let second = pool
            .handle(web_request(Vec::new()))
            .expect("second request");
        let third = pool.handle(web_request(Vec::new())).expect("third request");

        assert_eq!(first.status, 201);
        assert_eq!(second.status, 202);
        assert_eq!(third.status, 201);
    }

    #[test]
    fn pool_allows_disabling_worker_recycling() {
        let pool = web_pool(
            r#"
            let counter := 0;

            function __request__ ( env ) {
                counter := counter + 1;
                return [ 200 + counter, {{}}, [] ];
            }
            "#,
            WebAppPoolConfig {
                worker_count: 1,
                queue_bound: 1,
                max_requests_per_worker: 0,
                ..WebAppPoolConfig::default()
            },
        )
        .expect("pool should start");

        let first = pool.handle(web_request(Vec::new())).expect("first request");
        let second = pool
            .handle(web_request(Vec::new()))
            .expect("second request");
        let third = pool.handle(web_request(Vec::new())).expect("third request");

        assert_eq!(first.status, 201);
        assert_eq!(second.status, 202);
        assert_eq!(third.status, 203);
    }

    #[test]
    fn pool_marks_slot_unhealthy_when_replacement_startup_fails() {
        let starts = Arc::new(AtomicUsize::new(0));
        let builder_starts = Arc::clone(&starts);
        let builder: WebAppWorkerBuilder = Arc::new(move |_slot| {
            if builder_starts.fetch_add(1, Ordering::SeqCst) == 0 {
                stateful_worker()
            } else {
                Err(WebAppWorkerError::MissingRequestHandler)
            }
        });
        let pool = WebAppPool::new_with_builder(1, 1, 1, builder).expect("pool should start");

        let first = pool
            .handle(web_request(Vec::new()))
            .expect("first request should succeed");
        let err = pool
            .handle(web_request(Vec::new()))
            .expect_err("unhealthy slot should not accept another request");

        assert_eq!(first.status, 201);
        assert!(matches!(err, WebAppPoolError::NoHealthyWorkers));
        assert_eq!(starts.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn pool_startup_fails_if_any_initial_worker_fails() {
        let builder: WebAppWorkerBuilder = Arc::new(move |slot| {
            if slot == 1 {
                Err(WebAppWorkerError::MissingRequestHandler)
            } else {
                stateful_worker()
            }
        });

        let err = match WebAppPool::new_with_builder(2, 1, 0, builder) {
            Ok(_) => panic!("pool startup should fail"),
            Err(err) => err,
        };

        match err {
            WebAppPoolError::WorkerStartupFailed { slot, source } => {
                assert_eq!(slot, 1);
                assert!(matches!(source, WebAppWorkerError::MissingRequestHandler));
            }
            other => panic!("expected WorkerStartupFailed, got {other:?}"),
        }
    }

    #[test]
    fn pool_shutdown_joins_worker_threads() {
        let pool = web_pool(
            r#"
            function __request__ ( env ) {
                return [ 200, {{}}, [] ];
            }
            "#,
            WebAppPoolConfig {
                worker_count: 2,
                queue_bound: 1,
                max_requests_per_worker: 0,
                ..WebAppPoolConfig::default()
            },
        )
        .expect("pool should start");

        assert_eq!(
            pool.handle(web_request(Vec::new()))
                .expect("request should complete")
                .status,
            200
        );
        drop(pool);
    }

    #[tokio::test]
    async fn http_server_handles_get_and_propagates_headers() {
        let (server, base_url, _records) = start_logged_http_server(
            r#"
            function __request__ ( env ) {
                return [
                    201,
                    {{
                        "X-Method": env{method},
                        "Set-Cookie": "a=1",
                        "Set-Cookie": "b=2",
                    }},
                    [ env{path}, "?", env{query_string} ],
                ];
            }
            "#,
            1_048_576,
            WebAppPoolConfig {
                worker_count: 1,
                queue_bound: 4,
                max_requests_per_worker: 0,
                ..WebAppPoolConfig::default()
            },
        )
        .await;

        let response = reqwest::get(format!("{base_url}/hello?name=zuzu"))
            .await
            .expect("GET should succeed");
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.text().await.expect("body should decode");

        assert_eq!(status, 201);
        assert_eq!(
            headers
                .get("x-method")
                .and_then(|value| value.to_str().ok()),
            Some("GET")
        );
        assert_eq!(
            headers
                .get_all("set-cookie")
                .iter()
                .map(|value| value.to_str().expect("cookie should be text"))
                .collect::<Vec<_>>(),
            vec!["a=1", "b=2"]
        );
        assert_eq!(body, "/hello?name=zuzu");

        server.shutdown().await.expect("server should shut down");
    }

    #[tokio::test]
    async fn http_server_handles_post_body_text() {
        let (server, base_url, _records) = start_logged_http_server(
            r#"
            function __request__ ( env ) {
                return [ 200, {{}}, [ env{method}, ":", env{body_text} ] ];
            }
            "#,
            1_048_576,
            WebAppPoolConfig::default(),
        )
        .await;
        let client = reqwest::Client::new();

        let body = client
            .post(format!("{base_url}/submit"))
            .body("payload")
            .send()
            .await
            .expect("POST should succeed")
            .text()
            .await
            .expect("body should decode");

        assert_eq!(body, "POST:payload");

        server.shutdown().await.expect("server should shut down");
    }

    #[tokio::test]
    async fn http_server_returns_413_before_dispatching_to_app() {
        let (server, base_url, records) = start_logged_http_server(
            r#"
            let counter := 0;

            function __request__ ( env ) {
                counter := counter + 1;
                return [ 200 + counter, {{}}, [] ];
            }
            "#,
            3,
            WebAppPoolConfig {
                worker_count: 1,
                queue_bound: 4,
                max_requests_per_worker: 0,
                ..WebAppPoolConfig::default()
            },
        )
        .await;
        let client = reqwest::Client::new();

        let too_large = client
            .post(format!("{base_url}/upload"))
            .body("hello")
            .send()
            .await
            .expect("POST should receive response");
        let after = client
            .get(format!("{base_url}/after"))
            .send()
            .await
            .expect("GET should succeed");

        assert_eq!(too_large.status().as_u16(), 413);
        assert_eq!(after.status().as_u16(), 201);
        let records = records
            .lock()
            .expect("access log records lock should not be poisoned");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].method, "POST");
        assert_eq!(records[0].path, "/upload");
        assert_eq!(records[0].status, 413);
        assert_eq!(records[1].status, 201);
        assert_eq!(records[0].worker_id, None);
        assert_eq!(records[1].worker_id, Some(0));

        drop(records);
        server.shutdown().await.expect("server should shut down");
    }

    #[tokio::test]
    async fn http_server_maps_request_failures_to_500() {
        let (throw_server, throw_url, _records) = start_logged_http_server(
            r#"
            function __request__ ( env ) {
                throw new Exception( message: "boom" );
            }
            "#,
            1_048_576,
            WebAppPoolConfig::default(),
        )
        .await;
        let (invalid_server, invalid_url, _records) = start_logged_http_server(
            r#"
            function __request__ ( env ) {
                return "bad";
            }
            "#,
            1_048_576,
            WebAppPoolConfig::default(),
        )
        .await;

        let thrown = reqwest::get(format!("{throw_url}/throw"))
            .await
            .expect("throw request should receive response");
        let invalid = reqwest::get(format!("{invalid_url}/invalid"))
            .await
            .expect("invalid request should receive response");

        assert_eq!(thrown.status().as_u16(), 500);
        assert_eq!(invalid.status().as_u16(), 500);
        assert_eq!(
            thrown.text().await.expect("body should decode"),
            "Internal Server Error\n"
        );
        assert_eq!(
            invalid.text().await.expect("body should decode"),
            "Internal Server Error\n"
        );

        throw_server
            .shutdown()
            .await
            .expect("throw server should shut down");
        invalid_server
            .shutdown()
            .await
            .expect("invalid server should shut down");
    }

    #[tokio::test]
    async fn http_server_debug_response_includes_request_error_details() {
        let (server, base_url, records) = start_debug_http_server(
            r#"
            function __request__ ( env ) {
                throw new Exception( message: "debug-boom" );
            }
            "#,
        )
        .await;

        let response = reqwest::get(format!("{base_url}/throw"))
            .await
            .expect("throw request should receive response");
        let status = response.status().as_u16();
        let body = response.text().await.expect("body should decode");

        assert_eq!(status, 500);
        assert!(body.contains("Internal Server Error"));
        assert!(body.contains("debug-boom"));
        let records = records
            .lock()
            .expect("access log records lock should not be poisoned");
        assert_eq!(records[0].status, 500);
        assert_eq!(records[0].worker_id, Some(0));

        drop(records);
        server.shutdown().await.expect("server should shut down");
    }

    #[tokio::test]
    async fn http_server_maps_pool_saturation_to_503() {
        let mut pool_config = WebAppPoolConfig {
            worker_count: 1,
            queue_bound: 0,
            max_requests_per_worker: 0,
            ..WebAppPoolConfig::default()
        };
        pool_config.module_roots = repo_module_roots();
        let (server, base_url, _records) = start_logged_http_server(
            r#"
            from std/task import sleep;

            async function __request__ ( env ) {
                await { sleep(0.2); };
                return [ 200, {{}}, [] ];
            }
            "#,
            1_048_576,
            pool_config,
        )
        .await;
        let client = reqwest::Client::new();
        let busy_client = client.clone();
        let busy_url = base_url.clone();
        let busy = tokio::spawn(async move {
            busy_client
                .get(format!("{busy_url}/busy"))
                .send()
                .await
                .expect("busy request should receive response")
                .status()
                .as_u16()
        });

        tokio::time::sleep(Duration::from_millis(30)).await;
        let saturated = client
            .get(format!("{base_url}/saturated"))
            .send()
            .await
            .expect("saturated request should receive response");

        assert_eq!(saturated.status().as_u16(), 503);
        assert_eq!(busy.await.expect("busy task should join"), 200);

        server.shutdown().await.expect("server should shut down");
    }

    #[tokio::test]
    async fn http_server_serves_path_body_from_repo_relative_path() {
        let mut pool_config = WebAppPoolConfig::default();
        pool_config.module_roots = repo_module_roots();
        let (server, base_url, records) = start_logged_http_server(
            r#"
            from std/io import Path;

            function __request__ ( env ) {
                return [ 203, {{}}, new Path("docs/web-server-sketch.txt") ];
            }
            "#,
            1_048_576,
            pool_config,
        )
        .await;

        let response = reqwest::get(format!("{base_url}/static"))
            .await
            .expect("static request should receive response");
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = response
            .bytes()
            .await
            .expect("static body should be readable")
            .to_vec();
        let expected = fs::read(repo_root().join("docs/web-server-sketch.txt"))
            .expect("fixture file should be readable");

        assert_eq!(status, 203);
        assert_eq!(body, expected);
        assert_eq!(content_type.as_deref(), Some("text/plain"));
        let records = records
            .lock()
            .expect("access log records lock should not be poisoned");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, 203);
        assert_eq!(records[0].response_bytes, expected.len());

        drop(records);
        server.shutdown().await.expect("server should shut down");
    }

    #[tokio::test]
    async fn http_server_preserves_explicit_content_type_for_path_body() {
        let mut pool_config = WebAppPoolConfig::default();
        pool_config.module_roots = repo_module_roots();
        let (server, base_url, _records) = start_logged_http_server(
            r#"
            from std/io import Path;

            function __request__ ( env ) {
                return [
                    200,
                    { "content-type": "application/x-zuzu-test" },
                    new Path("docs/web-server-sketch.txt"),
                ];
            }
            "#,
            1_048_576,
            pool_config,
        )
        .await;

        let response = reqwest::get(format!("{base_url}/explicit-content-type"))
            .await
            .expect("static request should receive response");

        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("application/x-zuzu-test")
        );

        server.shutdown().await.expect("server should shut down");
    }

    #[tokio::test]
    async fn http_server_maps_missing_path_body_to_404() {
        let mut pool_config = WebAppPoolConfig::default();
        pool_config.module_roots = repo_module_roots();
        let (server, base_url, records) = start_logged_http_server(
            r#"
            from std/io import Path;

            function __request__ ( env ) {
                return [ 200, {{}}, new Path("docs/missing-phase-9-file.txt") ];
            }
            "#,
            1_048_576,
            pool_config,
        )
        .await;

        let response = reqwest::get(format!("{base_url}/missing"))
            .await
            .expect("missing request should receive response");

        assert_eq!(response.status().as_u16(), 404);
        assert_eq!(
            response.text().await.expect("body should decode"),
            "Not Found\n"
        );
        let records = records
            .lock()
            .expect("access log records lock should not be poisoned");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, 404);

        drop(records);
        server.shutdown().await.expect("server should shut down");
    }

    #[tokio::test]
    async fn http_server_maps_directory_path_body_to_403() {
        let mut pool_config = WebAppPoolConfig::default();
        pool_config.module_roots = repo_module_roots();
        let (server, base_url, records) = start_logged_http_server(
            r#"
            from std/io import Path;

            function __request__ ( env ) {
                return [ 200, {{}}, new Path("docs") ];
            }
            "#,
            1_048_576,
            pool_config,
        )
        .await;

        let response = reqwest::get(format!("{base_url}/directory"))
            .await
            .expect("directory request should receive response");

        assert_eq!(response.status().as_u16(), 403);
        assert_eq!(
            response.text().await.expect("body should decode"),
            "Forbidden\n"
        );
        let records = records
            .lock()
            .expect("access log records lock should not be poisoned");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, 403);

        drop(records);
        server.shutdown().await.expect("server should shut down");
    }

    #[tokio::test]
    async fn http_server_access_logs_success_and_server_error() {
        let (server, base_url, records) = start_logged_http_server(
            r#"
            function __request__ ( env ) {
                return [ 200, {{}}, [ "ok" ] ];
            }
            "#,
            2,
            WebAppPoolConfig::default(),
        )
        .await;
        let client = reqwest::Client::new();

        let ok = client
            .get(format!("{base_url}/ok"))
            .send()
            .await
            .expect("GET should succeed");
        let too_large = client
            .post(format!("{base_url}/too-big"))
            .body("secret-payload")
            .send()
            .await
            .expect("POST should receive response");

        assert_eq!(ok.status().as_u16(), 200);
        assert_eq!(too_large.status().as_u16(), 413);
        let records = records
            .lock()
            .expect("access log records lock should not be poisoned");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].method, "GET");
        assert_eq!(records[0].path, "/ok");
        assert_eq!(records[0].status, 200);
        assert_eq!(records[0].response_bytes, 2);
        assert!(records[0].elapsed >= Duration::ZERO);
        assert_eq!(records[1].method, "POST");
        assert_eq!(records[1].path, "/too-big");
        assert_eq!(records[1].status, 413);
        assert!(!format_access_log_record(&records[1]).contains("secret-payload"));

        drop(records);
        server.shutdown().await.expect("server should shut down");
    }
}
