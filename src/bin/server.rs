use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use zuzu_rust::web::{
    format_access_log_record, format_access_log_record_json, serve_web_app, AccessLogFormat,
    AccessLogRecord, AccessLogSink, WebAppPool, WebAppPoolConfig, WebAppPoolManager,
    WebHttpServerConfig,
};
use zuzu_rust::{
    module_search_roots, parse_program_with_compile_options_and_source_file, OptimizationOptions,
    ParseOptions, RuntimePolicy, ZuzuRustError,
};

type Result<T> = std::result::Result<T, ZuzuRustError>;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> Result<()> {
    let options = parse_args(env::args().skip(1).collect())?;
    if options.help {
        print_help();
        return Ok(());
    }
    let app_path = options
        .app_path
        .as_ref()
        .ok_or_else(|| ZuzuRustError::cli("usage: zuzu-rust-server [options] path/to/app.zzs"))?;
    let program = load_app_program(app_path)?;
    let module_roots = module_search_roots(options.include_dirs.clone());
    let access_log = access_log_sink(&options.access_log, options.access_log_format)?;
    let pool_config = pool_config_from_options(&options, &module_roots);

    if options.check {
        let check_config = WebAppPoolConfig {
            worker_count: 1,
            queue_bound: 1,
            ..pool_config
        };
        let pool = WebAppPool::new(&program, Some(&app_path.to_string_lossy()), check_config)
            .map_err(|err| ZuzuRustError::cli(err.to_string()))?;
        drop(pool);
        return Ok(());
    }

    let server = serve_web_app(
        &program,
        Some(&app_path.to_string_lossy()),
        WebHttpServerConfig {
            bind_addr: options.listen,
            max_body_bytes: options.max_body_bytes,
            pool_config: pool_config.clone(),
            access_log,
            debug_responses: options.debug_level > 0,
        },
    )
    .await
    .map_err(|err| ZuzuRustError::cli(err.to_string()))?;
    println!("listening on http://{}", server.local_addr());
    io::stdout().flush()?;
    log_startup(&options, app_path, server.local_addr(), &module_roots);

    if options.reload {
        let reloader = server.reloader();
        tokio::select! {
            result = run_reload_loop(
                app_path.to_path_buf(),
                Some(app_path.to_string_lossy().to_string()),
                pool_config,
                reloader,
            ) => result?,
            signal = tokio::signal::ctrl_c() => signal?,
        }
    } else {
        tokio::signal::ctrl_c().await?;
    }
    server
        .shutdown()
        .await
        .map_err(|err| ZuzuRustError::cli(format!("server shutdown failed: {err}")))?;
    Ok(())
}

#[derive(Debug)]
struct ServerOptions {
    listen: SocketAddr,
    workers: usize,
    queue_depth: usize,
    max_body_bytes: usize,
    max_requests_per_worker: usize,
    include_dirs: Vec<PathBuf>,
    denied_capabilities: Vec<String>,
    denied_modules: Vec<String>,
    access_log: String,
    access_log_format: AccessLogFormat,
    debug_level: u32,
    reload: bool,
    check: bool,
    help: bool,
    app_path: Option<PathBuf>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:3000"
                .parse()
                .expect("default listen address should parse"),
            workers: std::thread::available_parallelism()
                .map(|count| count.get())
                .unwrap_or(1),
            queue_depth: 64,
            max_body_bytes: 1_048_576,
            max_requests_per_worker: 1000,
            include_dirs: Vec::new(),
            denied_capabilities: Vec::new(),
            denied_modules: Vec::new(),
            access_log: "-".to_owned(),
            access_log_format: AccessLogFormat::Text,
            debug_level: 0,
            reload: false,
            check: false,
            help: false,
            app_path: None,
        }
    }
}

fn parse_args(args: Vec<String>) -> Result<ServerOptions> {
    let mut options = ServerOptions::default();
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        match arg.as_str() {
            "-h" | "--help" => {
                options.help = true;
            }
            "--check" => {
                options.check = true;
            }
            "--reload" => {
                options.reload = true;
            }
            "--listen" => {
                index += 1;
                options.listen =
                    parse_socket_addr(expect_arg(&args, index, "--listen")?, "--listen")?;
            }
            "--workers" => {
                index += 1;
                options.workers =
                    parse_positive_usize(expect_arg(&args, index, "--workers")?, "--workers")?;
            }
            "--queue-depth" => {
                index += 1;
                options.queue_depth =
                    parse_usize(expect_arg(&args, index, "--queue-depth")?, "--queue-depth")?;
            }
            "--max-body-bytes" => {
                index += 1;
                options.max_body_bytes = parse_usize(
                    expect_arg(&args, index, "--max-body-bytes")?,
                    "--max-body-bytes",
                )?;
            }
            "--max-requests-per-worker" => {
                index += 1;
                options.max_requests_per_worker = parse_usize(
                    expect_arg(&args, index, "--max-requests-per-worker")?,
                    "--max-requests-per-worker",
                )?;
            }
            "-I" => {
                index += 1;
                options
                    .include_dirs
                    .push(PathBuf::from(expect_arg(&args, index, "-I")?));
            }
            "--deny" => {
                index += 1;
                options
                    .denied_capabilities
                    .extend(split_csv(expect_arg(&args, index, "--deny")?));
            }
            "--denymodule" => {
                index += 1;
                options
                    .denied_modules
                    .extend(split_csv(expect_arg(&args, index, "--denymodule")?));
            }
            "--access-log" => {
                index += 1;
                options.access_log = expect_arg(&args, index, "--access-log")?.to_owned();
            }
            "--access-log-format" => {
                index += 1;
                options.access_log_format =
                    parse_access_log_format(expect_arg(&args, index, "--access-log-format")?)?;
            }
            "-d" => {
                options.debug_level = 1;
            }
            "--" => {
                index += 1;
                if options.app_path.is_some() {
                    return Err(ZuzuRustError::cli("expected only one app path"));
                }
                options.app_path = Some(PathBuf::from(expect_arg(&args, index, "--")?));
                if index + 1 < args.len() {
                    return Err(ZuzuRustError::cli(
                        "zuzu-rust-server does not accept app arguments",
                    ));
                }
                break;
            }
            _ if arg.starts_with("--listen=") => {
                options.listen = parse_socket_addr(&arg["--listen=".len()..], "--listen")?;
            }
            _ if arg.starts_with("--workers=") => {
                options.workers = parse_positive_usize(&arg["--workers=".len()..], "--workers")?;
            }
            _ if arg.starts_with("--queue-depth=") => {
                options.queue_depth = parse_usize(&arg["--queue-depth=".len()..], "--queue-depth")?;
            }
            _ if arg.starts_with("--max-body-bytes=") => {
                options.max_body_bytes =
                    parse_usize(&arg["--max-body-bytes=".len()..], "--max-body-bytes")?;
            }
            _ if arg.starts_with("--max-requests-per-worker=") => {
                options.max_requests_per_worker = parse_usize(
                    &arg["--max-requests-per-worker=".len()..],
                    "--max-requests-per-worker",
                )?;
            }
            _ if arg.starts_with("--deny=") => {
                options
                    .denied_capabilities
                    .extend(split_csv(&arg["--deny=".len()..]));
            }
            _ if arg.starts_with("--denymodule=") => {
                options
                    .denied_modules
                    .extend(split_csv(&arg["--denymodule=".len()..]));
            }
            _ if arg.starts_with("--access-log=") => {
                options.access_log = arg["--access-log=".len()..].to_owned();
            }
            _ if arg.starts_with("--access-log-format=") => {
                options.access_log_format =
                    parse_access_log_format(&arg["--access-log-format=".len()..])?;
            }
            _ if arg.starts_with("-I") && arg.len() > 2 => {
                options.include_dirs.push(PathBuf::from(&arg[2..]));
            }
            _ if arg.starts_with("-d=") => {
                options.debug_level = parse_debug_level(&arg[3..])?;
            }
            _ if is_debug_short_option(arg) => {
                options.debug_level = parse_debug_level(&arg[2..])?;
            }
            _ if arg.starts_with('-') => {
                return Err(ZuzuRustError::cli(format!("unsupported option: {arg}")));
            }
            _ => {
                if options.app_path.is_some() {
                    return Err(ZuzuRustError::cli("expected only one app path"));
                }
                options.app_path = Some(PathBuf::from(arg));
            }
        }
        index += 1;
    }
    Ok(options)
}

fn expect_arg<'a>(args: &'a [String], index: usize, option: &str) -> Result<&'a str> {
    args.get(index)
        .map(String::as_str)
        .ok_or_else(|| ZuzuRustError::cli(format!("expected value after {option}")))
}

fn parse_socket_addr(value: &str, option: &str) -> Result<SocketAddr> {
    value
        .parse()
        .map_err(|_| ZuzuRustError::cli(format!("{option} expects HOST:PORT")))
}

fn parse_usize(value: &str, option: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .map_err(|_| ZuzuRustError::cli(format!("{option} expects a non-negative integer")))
}

fn parse_positive_usize(value: &str, option: &str) -> Result<usize> {
    let value = parse_usize(value, option)?;
    if value == 0 {
        return Err(ZuzuRustError::cli(format!("{option} must be at least 1")));
    }
    Ok(value)
}

fn is_debug_short_option(arg: &str) -> bool {
    let Some(value) = arg.strip_prefix("-d") else {
        return false;
    };
    !value.is_empty() && value.chars().all(|ch| ch.is_ascii_digit())
}

fn parse_debug_level(value: &str) -> Result<u32> {
    if value.is_empty() {
        return Ok(1);
    }
    value
        .parse::<u32>()
        .map_err(|_| ZuzuRustError::cli("debug level must be a non-negative integer"))
}

fn parse_access_log_format(value: &str) -> Result<AccessLogFormat> {
    AccessLogFormat::parse(value)
        .ok_or_else(|| ZuzuRustError::cli("--access-log-format expects 'text' or 'json'"))
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect()
}

fn runtime_policy(denied_capabilities: Vec<String>, denied_modules: Vec<String>) -> RuntimePolicy {
    let mut policy = RuntimePolicy::new();
    for capability in denied_capabilities {
        policy = policy.deny_capability(capability);
    }
    for module in denied_modules {
        policy = policy.deny_module(module);
    }
    policy
}

fn pool_config_from_options(options: &ServerOptions, module_roots: &[PathBuf]) -> WebAppPoolConfig {
    WebAppPoolConfig {
        worker_count: options.workers,
        queue_bound: options.queue_depth,
        max_requests_per_worker: options.max_requests_per_worker,
        module_roots: module_roots.to_vec(),
        runtime_policy: runtime_policy(
            options.denied_capabilities.clone(),
            options.denied_modules.clone(),
        )
        .debug_level(options.debug_level),
    }
}

fn load_app_program(app_path: &Path) -> Result<zuzu_rust::Program> {
    let source = fs::read_to_string(app_path)?;
    let parse_options = ParseOptions::new(true, true, OptimizationOptions::default());
    parse_program_with_compile_options_and_source_file(
        &source,
        &parse_options,
        Some(&app_path.to_string_lossy()),
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReloadFingerprint {
    modified: Option<SystemTime>,
    len: u64,
}

fn reload_fingerprint(path: &Path) -> io::Result<ReloadFingerprint> {
    let metadata = fs::metadata(path)?;
    Ok(ReloadFingerprint {
        modified: metadata.modified().ok(),
        len: metadata.len(),
    })
}

async fn run_reload_loop(
    app_path: PathBuf,
    source_file: Option<String>,
    pool_config: WebAppPoolConfig,
    reloader: WebAppPoolManager,
) -> Result<()> {
    let mut last_seen = reload_fingerprint(&app_path)?;
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        let current = match reload_fingerprint(&app_path) {
            Ok(current) => current,
            Err(err) => {
                eprintln!("reload failed path={} error={err}", app_path.display());
                continue;
            }
        };
        if current == last_seen {
            continue;
        }

        last_seen = current;
        eprintln!("reload detected path={}", app_path.display());
        match load_app_program(&app_path).and_then(|program| {
            reloader
                .replace(&program, source_file.as_deref(), pool_config.clone())
                .map_err(|err| ZuzuRustError::cli(err.to_string()))
        }) {
            Ok(()) => eprintln!("reload activated path={}", app_path.display()),
            Err(err) => eprintln!("reload failed path={} error={err}", app_path.display()),
        }
    }
}

fn access_log_sink(path: &str, format: AccessLogFormat) -> Result<AccessLogSink> {
    if path == "-" {
        return Ok(Arc::new(move |record: AccessLogRecord| {
            eprintln!("{}", format_access_log_record_for_format(&record, format));
        }));
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| ZuzuRustError::cli(format!("could not open access log '{path}': {err}")))?;
    let file = Arc::new(Mutex::new(file));
    Ok(Arc::new(move |record: AccessLogRecord| {
        if let Ok(mut file) = file.lock() {
            let _ = writeln!(
                file,
                "{}",
                format_access_log_record_for_format(&record, format)
            );
        }
    }))
}

fn format_access_log_record_for_format(
    record: &AccessLogRecord,
    format: AccessLogFormat,
) -> String {
    match format {
        AccessLogFormat::Text => format_access_log_record(record),
        AccessLogFormat::Json => format_access_log_record_json(record),
    }
}

fn log_startup(
    options: &ServerOptions,
    app_path: &Path,
    listener_addr: SocketAddr,
    module_roots: &[PathBuf],
) {
    eprintln!("startup app_path={}", app_path.display());
    eprintln!("startup listener=http://{listener_addr}");
    eprintln!("startup workers={}", options.workers);
    eprintln!("startup queue_depth={}", options.queue_depth);
    eprintln!(
        "startup max_requests_per_worker={}",
        options.max_requests_per_worker
    );
    eprintln!(
        "startup access_log={} format={}",
        options.access_log,
        options.access_log_format.as_str()
    );
    eprintln!(
        "startup denied_capabilities={}",
        csv_or_dash(&options.denied_capabilities)
    );
    eprintln!(
        "startup denied_modules={}",
        csv_or_dash(&options.denied_modules)
    );
    eprintln!("startup module_roots={}", paths_csv_or_dash(module_roots));
    eprintln!("startup reload={}", options.reload);
}

fn csv_or_dash(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_owned()
    } else {
        values.join(",")
    }
}

fn paths_csv_or_dash(paths: &[PathBuf]) -> String {
    if paths.is_empty() {
        "-".to_owned()
    } else {
        paths
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn print_help() {
    println!(
        "\
Usage: zuzu-rust-server [options] path/to/app.zzs
Options:
  --listen HOST:PORT             listen address (default 127.0.0.1:3000)
  --workers N                    worker count (default available parallelism)
  --queue-depth N                per-worker queue depth (default 64)
  --max-body-bytes N             maximum request body bytes (default 1048576)
  --max-requests-per-worker N    recycle workers after N requests; 0 disables
  -I PATH, -IPATH                add module include directory
  --deny CAP                     deny runtime capability (repeatable or comma-separated)
  --denymodule MODULE            deny a specific module (repeatable or comma-separated)
  --access-log PATH              access log path, or - for stderr (default -)
  --access-log-format text|json  access log format (default text)
  --reload                       reload the app file when it changes
  -d[=N]                         set debug level
  --check                        parse and load app, then exit
  -h, --help                     show this help"
    );
}
