use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("repo root should exist")
        .to_path_buf()
}

fn temp_dir(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("zuzu-rust-server-{}-{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("temp dir should be created");
    dir
}

fn write_app(dir: &Path, name: &str, source: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, source).expect("app should be written");
    path
}

fn request_text_with_retry(client: &reqwest::blocking::Client, url: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match client.get(url).send() {
            Ok(response) => return response.text().expect("body should decode"),
            Err(err) if Instant::now() < deadline => {
                let _ = err;
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(err) => panic!("server did not accept request: {err}"),
        }
    }
}

fn wait_for_body(client: &reqwest::blocking::Client, url: &str, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let body = client
            .get(url)
            .send()
            .expect("request should receive response")
            .text()
            .expect("body should decode");
        if body == expected {
            return;
        }
        if Instant::now() >= deadline {
            panic!("expected body {expected:?}, got {body:?}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn run_server(args: &[String], cwd: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_zuzu-rust-server"))
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("zuzu-rust-server should run")
}

fn run_server_str(args: &[&str], cwd: &Path) -> std::process::Output {
    let args = args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>();
    run_server(&args, cwd)
}

#[test]
fn server_cli_help_lists_core_options() {
    let output = run_server_str(&["--help"], &repo_root());

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("zuzu-rust-server"));
    assert!(stdout.contains("--listen"));
    assert!(stdout.contains("--workers"));
    assert!(stdout.contains("--access-log"));
    assert!(stdout.contains("--access-log-format"));
    assert!(stdout.contains("--reload"));
    assert!(stdout.contains("--check"));
}

#[test]
fn server_cli_reports_missing_app_path() {
    let output = run_server_str(&[], &repo_root());

    assert!(!output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "");
    assert!(String::from_utf8_lossy(&output.stderr).contains("usage: zuzu-rust-server"));
}

#[test]
fn server_cli_reports_invalid_numeric_options() {
    for args in [
        vec!["--workers", "0"],
        vec!["--queue-depth", "nope"],
        vec!["--max-requests-per-worker", "nope"],
    ] {
        let output = run_server_str(&args, &repo_root());
        assert!(!output.status.success(), "{args:?} should fail");
    }
}

#[test]
fn server_cli_reports_invalid_access_log_format() {
    let dir = temp_dir("bad-access-log-format");
    let app = write_app(
        &dir,
        "app.zzs",
        r#"
        function __request__ ( env ) {
            return [ 200, {{}}, [] ];
        }
        "#,
    );
    let app = app.to_string_lossy().to_string();
    let output = run_server_str(
        &["--access-log-format", "xml", "--check", &app],
        &repo_root(),
    );

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--access-log-format"));
}

#[test]
fn server_cli_check_accepts_valid_app() {
    let dir = temp_dir("check-valid");
    let app = write_app(
        &dir,
        "app.zzs",
        r#"
        function __request__ ( env ) {
            return [ 200, {{}}, [ "ok" ] ];
        }
        "#,
    );
    let app = app.to_string_lossy().to_string();
    let output = run_server_str(&["--check", &app], &repo_root());

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "");
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
}

#[test]
fn server_cli_check_rejects_missing_request_handler() {
    let dir = temp_dir("check-missing-request");
    let app = write_app(&dir, "app.zzs", "function helper () { return 1; }");
    let app = app.to_string_lossy().to_string();
    let output = run_server_str(&["--check", &app], &repo_root());

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("__request__"));
}

#[test]
fn server_cli_check_allows_disabled_worker_recycling() {
    let dir = temp_dir("check-no-recycle");
    let app = write_app(
        &dir,
        "app.zzs",
        r#"
        function __request__ ( env ) {
            return [ 200, {{}}, [] ];
        }
        "#,
    );
    let app = app.to_string_lossy().to_string();
    let output = run_server_str(
        &["--max-requests-per-worker", "0", "--check", &app],
        &repo_root(),
    );

    assert!(output.status.success());
}

#[test]
fn server_cli_reports_invalid_access_log_path() {
    let dir = temp_dir("bad-access-log");
    let app = write_app(
        &dir,
        "app.zzs",
        r#"
        function __request__ ( env ) {
            return [ 200, {{}}, [] ];
        }
        "#,
    );
    let app = app.to_string_lossy().to_string();
    let bad_log = dir.join("missing").join("access.log");
    let bad_log = bad_log.to_string_lossy().to_string();
    let output = run_server_str(&["--access-log", &bad_log, "--check", &app], &repo_root());

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("could not open access log"));
}

#[test]
fn server_cli_smoke_serves_request_and_writes_access_log() {
    let dir = temp_dir("smoke");
    let app = write_app(
        &dir,
        "app.zzs",
        r#"
        function __request__ ( env ) {
            return [ 202, { "X-App": env{method} }, [ "hello:", env{path} ] ];
        }
        "#,
    );
    let log = dir.join("access.log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_zuzu-rust-server"))
        .arg("--listen")
        .arg("127.0.0.1:0")
        .arg("--access-log")
        .arg(&log)
        .arg(app)
        .current_dir(repo_root())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("zuzu-rust-server should start");
    let stdout = child.stdout.take().expect("stdout should be piped");
    let mut stdout = BufReader::new(stdout);
    let mut line = String::new();
    stdout
        .read_line(&mut line)
        .expect("server should print listen address");
    assert!(
        line.starts_with("listening on http://"),
        "stdout was {line:?}"
    );
    let base_url = line
        .trim()
        .strip_prefix("listening on ")
        .expect("listen line should include prefix")
        .to_owned();

    let client = reqwest::blocking::Client::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    let response = loop {
        match client.get(format!("{base_url}/smoke")).send() {
            Ok(response) => break response,
            Err(err) if Instant::now() < deadline => {
                let _ = err;
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(err) => panic!("server did not accept request: {err}"),
        }
    };
    assert_eq!(response.status().as_u16(), 202);
    assert_eq!(
        response
            .headers()
            .get("x-app")
            .and_then(|value| value.to_str().ok()),
        Some("GET")
    );
    assert_eq!(response.text().expect("body should decode"), "hello:/smoke");

    child.kill().expect("server should be killable");
    let _ = child.wait().expect("server should exit after kill");

    let log_text = fs::read_to_string(log).expect("access log should be written");
    assert!(log_text.contains("\"GET /smoke\""));
    assert!(log_text.contains(" 202 "));
    assert!(!log_text.contains("hello:/smoke"));
}

#[test]
fn server_cli_json_access_log_and_startup_diagnostics() {
    let dir = temp_dir("json-observability");
    let app = write_app(
        &dir,
        "app.zzs",
        r#"
        function __request__ ( env ) {
            if ( env{path} == "/fail" ) {
                throw new Exception( message: "observability-boom" );
            }
            return [ 200, {{}}, [ "secret response body" ] ];
        }
        "#,
    );
    let log = dir.join("access.jsonl");
    let mut child = Command::new(env!("CARGO_BIN_EXE_zuzu-rust-server"))
        .arg("--listen")
        .arg("127.0.0.1:0")
        .arg("--workers")
        .arg("1")
        .arg("--queue-depth")
        .arg("4")
        .arg("--max-requests-per-worker")
        .arg("2")
        .arg("--deny")
        .arg("fs")
        .arg("--denymodule")
        .arg("std/net/http")
        .arg("-I")
        .arg(&dir)
        .arg("--access-log")
        .arg(&log)
        .arg("--access-log-format")
        .arg("json")
        .arg(app)
        .current_dir(repo_root())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("zuzu-rust-server should start");
    let stdout = child.stdout.take().expect("stdout should be piped");
    let mut stderr = child.stderr.take().expect("stderr should be piped");
    let mut stdout = BufReader::new(stdout);
    let mut line = String::new();
    stdout
        .read_line(&mut line)
        .expect("server should print listen address");
    let base_url = line
        .trim()
        .strip_prefix("listening on ")
        .expect("listen line should include prefix")
        .to_owned();
    let client = reqwest::blocking::Client::new();
    let deadline = Instant::now() + Duration::from_secs(5);

    let ok = loop {
        match client
            .post(format!("{base_url}/ok"))
            .body("secret request body")
            .send()
        {
            Ok(response) => break response,
            Err(err) if Instant::now() < deadline => {
                let _ = err;
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(err) => panic!("server did not accept request: {err}"),
        }
    };
    let fail = client
        .get(format!("{base_url}/fail"))
        .send()
        .expect("failing request should receive response");

    assert_eq!(ok.status().as_u16(), 200);
    assert_eq!(fail.status().as_u16(), 500);
    assert_eq!(
        fail.text().expect("failure body should decode"),
        "Internal Server Error\n"
    );

    child.kill().expect("server should be killable");
    let _ = child.wait().expect("server should exit after kill");
    let mut stderr_text = String::new();
    stderr
        .read_to_string(&mut stderr_text)
        .expect("stderr should be readable");

    let log_text = fs::read_to_string(log).expect("access log should be written");
    let lines = log_text.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    let first: serde_json::Value =
        serde_json::from_str(lines[0]).expect("first access log line should be JSON");
    let second: serde_json::Value =
        serde_json::from_str(lines[1]).expect("second access log line should be JSON");
    assert_eq!(first["method"], "POST");
    assert_eq!(first["path"], "/ok");
    assert_eq!(first["status"], 200);
    assert_eq!(first["worker_id"], 0);
    assert_eq!(second["method"], "GET");
    assert_eq!(second["path"], "/fail");
    assert_eq!(second["status"], 500);
    assert_eq!(second["worker_id"], 0);

    assert!(stderr_text.contains("startup app_path="));
    assert!(stderr_text.contains("startup listener=http://"));
    assert!(stderr_text.contains("startup workers=1"));
    assert!(stderr_text.contains("startup module_roots="));
    assert!(stderr_text.contains("startup denied_capabilities=fs"));
    assert!(stderr_text.contains("startup denied_modules=std/net/http"));
    assert!(stderr_text.contains("startup max_requests_per_worker=2"));
    assert!(stderr_text.contains("format=json"));
    assert!(!stderr_text.contains("secret request body"));
    assert!(!stderr_text.contains("secret response body"));
    assert!(!log_text.contains("secret request body"));
    assert!(!log_text.contains("secret response body"));
}

#[test]
fn server_cli_reload_replaces_app_and_keeps_old_app_on_failure() {
    let dir = temp_dir("reload");
    let app = write_app(
        &dir,
        "app.zzs",
        r#"
        function __request__ ( env ) {
            return [ 200, {{}}, [ "one" ] ];
        }
        "#,
    );
    let mut child = Command::new(env!("CARGO_BIN_EXE_zuzu-rust-server"))
        .arg("--listen")
        .arg("127.0.0.1:0")
        .arg("--workers")
        .arg("1")
        .arg("--reload")
        .arg(&app)
        .current_dir(repo_root())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("zuzu-rust-server should start");
    let stdout = child.stdout.take().expect("stdout should be piped");
    let mut stderr = child.stderr.take().expect("stderr should be piped");
    let mut stdout = BufReader::new(stdout);
    let mut line = String::new();
    stdout
        .read_line(&mut line)
        .expect("server should print listen address");
    let base_url = line
        .trim()
        .strip_prefix("listening on ")
        .expect("listen line should include prefix")
        .to_owned();
    let client = reqwest::blocking::Client::new();

    assert_eq!(request_text_with_retry(&client, &base_url), "one");
    fs::write(
        &app,
        r#"
        function __request__ ( env ) {
            return [ 200, {{}}, [ "two" ] ];
        }
        "#,
    )
    .expect("updated app should be written");
    wait_for_body(&client, &base_url, "two");

    fs::write(&app, "function helper () { return 1; }").expect("bad app should be written");
    std::thread::sleep(Duration::from_millis(1200));
    assert_eq!(
        client
            .get(&base_url)
            .send()
            .expect("request should receive response")
            .text()
            .expect("body should decode"),
        "two"
    );

    fs::write(
        &app,
        r#"
        function __request__ ( env ) {
            return [ 200, {{}}, [ "three" ] ];
        }
        "#,
    )
    .expect("fixed app should be written");
    wait_for_body(&client, &base_url, "three");

    child.kill().expect("server should be killable");
    let _ = child.wait().expect("server should exit after kill");
    let mut stderr_text = String::new();
    stderr
        .read_to_string(&mut stderr_text)
        .expect("stderr should be readable");
    assert!(stderr_text.contains("startup reload=true"));
    assert!(stderr_text.contains("reload detected"));
    assert!(stderr_text.contains("reload activated"));
    assert!(stderr_text.contains("reload failed"));
}
