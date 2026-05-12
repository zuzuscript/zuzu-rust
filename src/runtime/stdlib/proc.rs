use std::cell::Cell;
use std::collections::HashMap;
use std::path::Path;
use std::process::{Command, Stdio};
use std::rc::Rc;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
#[cfg(windows)]
use std::os::windows::process::ExitStatusExt;

use super::super::{Runtime, Value};
use crate::error::{Result, ZuzuRustError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command as TokioCommand;

pub(super) fn exports() -> HashMap<String, Value> {
    HashMap::from([
        ("Proc".to_owned(), Value::builtin_class("Proc".to_owned())),
        ("Env".to_owned(), Value::builtin_class("Env".to_owned())),
        (
            "sleep".to_owned(),
            Value::native_function("sleep".to_owned()),
        ),
        (
            "sleep_async".to_owned(),
            Value::native_function("sleep_async".to_owned()),
        ),
    ])
}

pub(super) fn call(runtime: &Runtime, name: &str, args: &[Value]) -> Option<Result<Value>> {
    let value = match name {
        "sleep" => sleep(runtime, args),
        "sleep_async" => Ok(runtime.task_sleep(seconds_arg(args.first()))),
        _ => return None,
    };
    Some(value)
}

pub(super) fn call_class_method(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    let value = match (class_name, name) {
        ("Proc", "pid") => Ok(Value::Number(std::process::id() as f64)),
        ("Proc", "run") => proc_run(runtime, args),
        ("Proc", "run_async") => proc_run_async(runtime, args),
        ("Proc", "pipeline") => proc_pipeline(runtime, args),
        ("Proc", "pipeline_async") => proc_pipeline_async(runtime, args),
        ("Proc", "onsignal") => proc_onsignal(runtime, args),
        ("Proc", "kill") => proc_kill(runtime, args),
        ("Proc", "exit") => proc_exit(args),
        ("Proc", "is_success") => proc_is_success(args),
        ("Proc", "status_text") => Ok(Value::String(proc_status_text(args.first()))),
        ("Env", "get") => env_get(args),
        ("Env", "set") => env_set(args),
        ("Env", "remove") => env_remove(args),
        _ => return None,
    };
    Some(value)
}

fn proc_exit(args: &[Value]) -> Result<Value> {
    let code = match args.first() {
        Some(Value::Number(value)) => *value as i32,
        Some(Value::Boolean(value)) => {
            if *value {
                1
            } else {
                0
            }
        }
        Some(Value::String(value)) => value.parse::<i32>().unwrap_or(0),
        Some(Value::Null) | None => 0,
        Some(value) => {
            return Err(ZuzuRustError::runtime(format!(
                "TypeException: Proc.exit expects Number, got {}",
                value.type_name()
            )))
        }
    };
    std::process::exit(code);
}

#[derive(Clone)]
struct ProcOptions {
    capture_stdout: bool,
    capture_stderr: bool,
    merge_stderr: bool,
    stdin: String,
    timeout: Option<Duration>,
    env: Vec<(String, Option<String>)>,
}

#[derive(Clone)]
struct ProcRunSpec {
    command_parts: Vec<String>,
    options: ProcOptions,
    cwd: std::path::PathBuf,
}

fn proc_pipeline(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    runtime.warn_blocking_operation("std/proc Proc.pipeline")?;
    let Some(Value::Array(commands)) = args.first() else {
        return Err(ZuzuRustError::runtime(
            "Proc.pipeline() expects an array of commands",
        ));
    };
    let base_options = match args.get(1) {
        Some(Value::Dict(map)) => map.clone(),
        _ => HashMap::new(),
    };
    let mut stdin = String::new();
    let mut steps = Vec::new();
    for command in commands {
        let command_value = match command {
            Value::Shared(value) => value.borrow().clone(),
            other => other.clone(),
        };
        let mut run_args = match &command_value {
            Value::Array(parts) if !parts.is_empty() => {
                vec![parts[0].clone(), Value::Array(parts[1..].to_vec())]
            }
            other => vec![other.clone()],
        };
        let mut step_options = base_options.clone();
        if !stdin.is_empty() {
            step_options.insert("stdin".to_owned(), Value::String(stdin.clone()));
        }
        if !step_options.is_empty() {
            run_args.push(Value::Dict(step_options));
        }
        let result = proc_run(runtime, &run_args)?;
        if let Value::Dict(map) = &result {
            stdin = map.get("stdout").map(render_string).unwrap_or_default();
        }
        steps.push(result);
    }
    Ok(pipeline_result_from_steps(steps))
}

fn seconds_arg(value: Option<&Value>) -> f64 {
    match value {
        Some(Value::Number(value)) => *value,
        Some(Value::Boolean(value)) => {
            if *value {
                1.0
            } else {
                0.0
            }
        }
        Some(Value::String(value)) => value.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn proc_run(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    runtime.warn_blocking_operation("std/proc Proc.run")?;
    let spec = parse_run_spec(runtime, args, "Proc.run")?;
    let command_parts = spec.command_parts.clone();
    let options = spec.options.clone();
    let cwd = spec.cwd.clone();

    let mut command = Command::new(&command_parts[0]);
    command.args(&command_parts[1..]);
    command.current_dir(&cwd);
    for (name, value) in &options.env {
        match value {
            Some(value) => command.env(name, value),
            None => command.env_remove(name),
        };
    }
    if !options.stdin.is_empty() {
        command.stdin(Stdio::piped());
    }
    if options.capture_stdout || options.merge_stderr {
        command.stdout(Stdio::piped());
    }
    if options.capture_stderr || options.merge_stderr {
        command.stderr(Stdio::piped());
    }

    let output = if options.stdin.is_empty() {
        command.output()
    } else {
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(err) => {
                return Ok(process_error_result(
                    &command_parts,
                    process_spawn_error("Proc.run failed", &cwd, err),
                ))
            }
        };
        if let Some(mut child_stdin) = child.stdin.take() {
            use std::io::Write;
            child_stdin
                .write_all(options.stdin.as_bytes())
                .map_err(|err| ZuzuRustError::runtime(format!("Proc.run stdin failed: {err}")))?;
        }
        child.wait_with_output()
    };

    match output {
        Ok(output) => Ok(process_output_result(&command_parts, &options, output)),
        Err(err) => Ok(process_error_result(
            &command_parts,
            process_spawn_error("", &cwd, err),
        )),
    }
}

fn proc_run_async(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    let spec = parse_run_spec(runtime, args, "Proc.run")?;
    let cancel_requested = Rc::new(Cell::new(false));
    let future = run_process_async(spec, Rc::clone(&cancel_requested));
    Ok(runtime.task_native_async(future, Some(cancel_requested)))
}

fn proc_pipeline_async(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    let specs = parse_pipeline_specs(runtime, args, "Proc.pipeline")?;
    let cancel_requested = Rc::new(Cell::new(false));
    let future = run_pipeline_async(specs, Rc::clone(&cancel_requested));
    Ok(runtime.task_native_async(future, Some(cancel_requested)))
}

fn parse_run_spec(runtime: &Runtime, args: &[Value], name: &str) -> Result<ProcRunSpec> {
    if args.is_empty() || args.len() > 3 {
        return Err(ZuzuRustError::runtime(format!(
            "{name}() expects command, optional argv, and optional options"
        )));
    }

    let mut command_parts = command_parts_from_value(&args[0]);
    if let Some(argv) = args.get(1) {
        if let Value::Array(items) = argv {
            command_parts.extend(items.iter().map(render_string));
        }
    }
    if command_parts.is_empty() || command_parts[0].is_empty() {
        return Err(ZuzuRustError::runtime(format!(
            "{name}() command must not be empty"
        )));
    }

    Ok(ProcRunSpec {
        command_parts,
        options: proc_options(args.get(2)),
        cwd: proc_cwd(runtime, args.get(2)),
    })
}

fn parse_pipeline_specs(runtime: &Runtime, args: &[Value], name: &str) -> Result<Vec<ProcRunSpec>> {
    let Some(Value::Array(commands)) = args.first() else {
        return Err(ZuzuRustError::runtime(format!(
            "{name}() expects an array of commands"
        )));
    };
    let options = proc_options(args.get(1));
    let cwd = proc_cwd(runtime, args.get(1));
    let mut stdin = options.stdin.clone();
    let mut specs = Vec::new();
    for command in commands {
        let command_value = match command {
            Value::Shared(value) => value.borrow().clone(),
            other => other.clone(),
        };
        let command_parts = match &command_value {
            Value::Array(parts) => parts.iter().map(render_string).collect(),
            other => vec![render_string(other)],
        };
        if command_parts.is_empty() || command_parts[0].is_empty() {
            return Err(ZuzuRustError::runtime(format!(
                "{name}() command must not be empty"
            )));
        }
        let mut step_options = options.clone();
        step_options.stdin = stdin;
        step_options.capture_stdout = true;
        specs.push(ProcRunSpec {
            command_parts,
            options: step_options,
            cwd: cwd.clone(),
        });
        stdin = String::new();
    }
    Ok(specs)
}

fn proc_options(value: Option<&Value>) -> ProcOptions {
    ProcOptions {
        capture_stdout: dict_bool(value, "capture_stdout").unwrap_or(true),
        capture_stderr: dict_bool(value, "capture_stderr").unwrap_or(true),
        merge_stderr: dict_bool(value, "merge_stderr").unwrap_or(false),
        stdin: dict_string(value, "stdin").unwrap_or_default(),
        timeout: dict_get_number_from_option(value, "timeout")
            .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
            .map(Duration::from_secs_f64),
        env: dict_env(value),
    }
}

fn proc_cwd(_runtime: &Runtime, value: Option<&Value>) -> std::path::PathBuf {
    dict_string(value, "cwd")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(super::io::cwd_path)
}

fn process_spawn_error(prefix: &str, cwd: &Path, err: impl std::fmt::Display) -> String {
    if !cwd.is_dir() {
        return format!("could not change cwd to {}: {err}", cwd.display());
    }
    if prefix.is_empty() {
        return err.to_string();
    }
    format!("{prefix}: {err}")
}

async fn run_pipeline_async(
    specs: Vec<ProcRunSpec>,
    cancel_requested: Rc<Cell<bool>>,
) -> Result<Value> {
    if specs.is_empty() {
        return Ok(pipeline_result_from_steps(Vec::new()));
    }

    let timeout = specs.first().and_then(|spec| spec.options.timeout);
    let started = std::time::Instant::now();
    let mut children = Vec::new();
    let mut previous_stdout = None;
    let mut stdout_transfers = Vec::new();
    let mut stdin_writers = Vec::new();

    for (index, spec) in specs.iter().enumerate() {
        if cancel_requested.get() {
            return Err(ZuzuRustError::runtime("process task cancelled"));
        }

        let mut command = TokioCommand::new(&spec.command_parts[0]);
        command.args(&spec.command_parts[1..]);
        command.current_dir(&spec.cwd);
        for (name, value) in &spec.options.env {
            match value {
                Some(value) => command.env(name, value),
                None => command.env_remove(name),
            };
        }

        if index == 0 {
            if spec.options.stdin.is_empty() {
                command.stdin(Stdio::null());
            } else {
                command.stdin(Stdio::piped());
            }
        } else {
            command.stdin(Stdio::piped());
        }
        command.stdout(Stdio::piped());
        if spec.options.capture_stderr || spec.options.merge_stderr {
            command.stderr(Stdio::piped());
        } else {
            command.stderr(Stdio::null());
        }
        command.kill_on_drop(true);

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(err) => {
                abort_pipeline_tasks(&stdout_transfers, &stdin_writers);
                return Ok(pipeline_result_from_steps(vec![process_error_result(
                    &spec.command_parts,
                    process_spawn_error("Proc.pipeline failed", &spec.cwd, err),
                )]));
            }
        };

        if index == 0 && !spec.options.stdin.is_empty() {
            if let Some(child_stdin) = child.stdin.take() {
                stdin_writers.push(tokio::task::spawn_local(write_child_stdin(
                    child_stdin,
                    spec.options.stdin.clone(),
                )));
            }
        }

        if let Some(stdout) = previous_stdout.take() {
            if let Some(child_stdin) = child.stdin.take() {
                stdout_transfers.push((
                    index - 1,
                    tokio::task::spawn_local(pipe_and_capture(stdout, child_stdin)),
                ));
            }
        }

        if index + 1 < specs.len() {
            previous_stdout = child.stdout.take();
        }
        children.push(child);
    }

    let mut waiters = children
        .into_iter()
        .map(|child| Box::pin(child.wait_with_output()))
        .collect::<Vec<_>>();
    let mut outputs = Vec::new();
    let mut timed_out = false;

    for waiter in &mut waiters {
        loop {
            if cancel_requested.get() {
                abort_pipeline_tasks(&stdout_transfers, &stdin_writers);
                return Err(ZuzuRustError::runtime("process task cancelled"));
            }

            let slice = if let Some(timeout) = timeout {
                match timeout.checked_sub(started.elapsed()) {
                    Some(remaining) if !remaining.is_zero() => {
                        remaining.min(Duration::from_millis(5))
                    }
                    _ => {
                        timed_out = true;
                        break;
                    }
                }
            } else {
                Duration::from_millis(5)
            };

            match tokio::time::timeout(slice, waiter.as_mut()).await {
                Ok(Ok(output)) => {
                    outputs.push(output);
                    break;
                }
                Ok(Err(err)) => {
                    outputs.push(std::process::Output {
                        status: failure_exit_status(),
                        stdout: Vec::new(),
                        stderr: format!("Proc.pipeline failed: {err}").into_bytes(),
                    });
                    break;
                }
                Err(_) => {
                    if timeout
                        .map(|timeout| started.elapsed() >= timeout)
                        .unwrap_or(false)
                    {
                        timed_out = true;
                        break;
                    }
                }
            }
        }
        if timed_out {
            break;
        }
    }

    if timed_out {
        abort_pipeline_tasks(&stdout_transfers, &stdin_writers);
        return Ok(pipeline_timeout_result(&specs));
    }

    for writer in stdin_writers {
        if let Err(err) = writer.await {
            return Err(ZuzuRustError::runtime(format!(
                "Proc.pipeline stdin task failed: {err}"
            )));
        } else if cancel_requested.get() {
            return Err(ZuzuRustError::runtime("process task cancelled"));
        }
    }

    let mut captured_stdout = vec![None; specs.len()];
    for (index, transfer) in stdout_transfers {
        match transfer.await {
            Ok(Ok(stdout)) => captured_stdout[index] = Some(stdout),
            Ok(Err(err)) => return Err(err),
            Err(err) => {
                return Err(ZuzuRustError::runtime(format!(
                    "Proc.pipeline pipe task failed: {err}"
                )))
            }
        }
    }

    let steps = outputs
        .into_iter()
        .enumerate()
        .map(|(index, mut output)| {
            if let Some(stdout) = captured_stdout.get_mut(index).and_then(Option::take) {
                output.stdout = stdout;
            }
            process_output_result(&specs[index].command_parts, &specs[index].options, output)
        })
        .collect();

    Ok(pipeline_result_from_steps(steps))
}

async fn write_child_stdin(
    mut child_stdin: tokio::process::ChildStdin,
    input: String,
) -> Result<()> {
    child_stdin
        .write_all(input.as_bytes())
        .await
        .map_err(|err| ZuzuRustError::runtime(format!("Proc.pipeline stdin failed: {err}")))?;
    let _ = child_stdin.shutdown().await;
    Ok(())
}

async fn pipe_and_capture(
    mut stdout: tokio::process::ChildStdout,
    mut stdin: tokio::process::ChildStdin,
) -> Result<Vec<u8>> {
    let mut captured = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = stdout
            .read(&mut buffer)
            .await
            .map_err(|err| ZuzuRustError::runtime(format!("Proc.pipeline read failed: {err}")))?;
        if read == 0 {
            break;
        }
        captured.extend_from_slice(&buffer[..read]);
        if stdin.write_all(&buffer[..read]).await.is_err() {
            break;
        }
    }
    let _ = stdin.shutdown().await;
    Ok(captured)
}

fn abort_pipeline_tasks(
    transfers: &[(usize, tokio::task::JoinHandle<Result<Vec<u8>>>)],
    writers: &[tokio::task::JoinHandle<Result<()>>],
) {
    for (_, transfer) in transfers {
        transfer.abort();
    }
    for writer in writers {
        writer.abort();
    }
}

fn pipeline_result_from_steps(steps: Vec<Value>) -> Value {
    let last = steps.last();
    let ok = steps.iter().all(|step| match step {
        Value::Dict(map) => map.get("ok").map(Value::is_truthy).unwrap_or(false),
        _ => false,
    });
    Value::Dict(HashMap::from([
        ("ok".to_owned(), Value::Boolean(ok)),
        (
            "stdout".to_owned(),
            last.and_then(|step| dict_get_string(step, "stdout"))
                .map(Value::String)
                .unwrap_or_else(|| Value::String(String::new())),
        ),
        (
            "stderr".to_owned(),
            last.and_then(|step| dict_get_string(step, "stderr"))
                .map(Value::String)
                .unwrap_or_else(|| Value::String(String::new())),
        ),
        (
            "error".to_owned(),
            last.and_then(|step| match step {
                Value::Dict(map) => map.get("error").cloned(),
                _ => None,
            })
            .unwrap_or(Value::Null),
        ),
        (
            "exit_code".to_owned(),
            Value::Number(
                last.and_then(|step| dict_get_number(step, "exit_code"))
                    .unwrap_or(0.0),
            ),
        ),
        (
            "signal".to_owned(),
            Value::Number(
                last.and_then(|step| dict_get_number(step, "signal"))
                    .unwrap_or(0.0),
            ),
        ),
        (
            "core_dump".to_owned(),
            Value::Number(
                last.and_then(|step| dict_get_number(step, "core_dump"))
                    .unwrap_or(0.0),
            ),
        ),
        (
            "timed_out".to_owned(),
            Value::Boolean(
                last.and_then(|step| match step {
                    Value::Dict(map) => map.get("timed_out").map(Value::is_truthy),
                    _ => None,
                })
                .unwrap_or(false),
            ),
        ),
        ("steps".to_owned(), Value::Array(steps)),
    ]))
}

fn pipeline_timeout_result(specs: &[ProcRunSpec]) -> Value {
    let steps = specs
        .iter()
        .map(|spec| process_timeout_result(&spec.command_parts, &spec.options))
        .collect();
    pipeline_result_from_steps(steps)
}

async fn run_process_async(spec: ProcRunSpec, cancel_requested: Rc<Cell<bool>>) -> Result<Value> {
    let mut command = TokioCommand::new(&spec.command_parts[0]);
    command.args(&spec.command_parts[1..]);
    command.current_dir(&spec.cwd);
    for (name, value) in &spec.options.env {
        match value {
            Some(value) => command.env(name, value),
            None => command.env_remove(name),
        };
    }
    if !spec.options.stdin.is_empty() {
        command.stdin(Stdio::piped());
    }
    if spec.options.capture_stdout || spec.options.merge_stderr {
        command.stdout(Stdio::piped());
    }
    if spec.options.capture_stderr || spec.options.merge_stderr {
        command.stderr(Stdio::piped());
    }
    command.kill_on_drop(true);

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            return Ok(process_error_result(
                &spec.command_parts,
                process_spawn_error("Proc.run failed", &spec.cwd, err),
            ))
        }
    };
    if !spec.options.stdin.is_empty() {
        if let Some(mut child_stdin) = child.stdin.take() {
            child_stdin
                .write_all(spec.options.stdin.as_bytes())
                .await
                .map_err(|err| ZuzuRustError::runtime(format!("Proc.run stdin failed: {err}")))?;
        }
    }

    let started = std::time::Instant::now();
    let mut output = Box::pin(child.wait_with_output());
    loop {
        if cancel_requested.get() {
            drop(output);
            return Err(ZuzuRustError::runtime("process task cancelled"));
        }

        let slice = spec
            .options
            .timeout
            .map(|timeout| {
                timeout
                    .checked_sub(started.elapsed())
                    .unwrap_or(Duration::from_secs(0))
                    .min(Duration::from_millis(5))
            })
            .unwrap_or_else(|| Duration::from_millis(5));

        match tokio::time::timeout(slice, &mut output).await {
            Ok(Ok(output)) => {
                return Ok(process_output_result(
                    &spec.command_parts,
                    &spec.options,
                    output,
                ))
            }
            Ok(Err(err)) => {
                return Ok(process_error_result(
                    &spec.command_parts,
                    format!("Proc.run failed: {err}"),
                ));
            }
            Err(_) => {
                if spec
                    .options
                    .timeout
                    .map(|timeout| started.elapsed() >= timeout)
                    .unwrap_or(false)
                {
                    drop(output);
                    return Ok(process_timeout_result(&spec.command_parts, &spec.options));
                }
            }
        }
    }
}

fn process_output_result(
    command_parts: &[String],
    options: &ProcOptions,
    output: std::process::Output,
) -> Value {
    let exit_code = output.status.code().unwrap_or(1) as f64;
    let mut stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if options.merge_stderr {
        stdout.push_str(&stderr);
    }
    Value::Dict(HashMap::from([
        (
            "command".to_owned(),
            Value::Array(command_parts.iter().cloned().map(Value::String).collect()),
        ),
        ("exit_code".to_owned(), Value::Number(exit_code)),
        ("signal".to_owned(), Value::Number(0.0)),
        ("core_dump".to_owned(), Value::Number(0.0)),
        ("timed_out".to_owned(), Value::Boolean(false)),
        ("error".to_owned(), Value::Null),
        (
            "stdout".to_owned(),
            if options.capture_stdout || options.merge_stderr {
                Value::String(stdout)
            } else {
                Value::Null
            },
        ),
        (
            "stderr".to_owned(),
            if options.capture_stderr && !options.merge_stderr {
                Value::String(stderr)
            } else {
                Value::Null
            },
        ),
        ("ok".to_owned(), Value::Boolean(output.status.success())),
    ]))
}

fn process_error_result(command_parts: &[String], error: String) -> Value {
    Value::Dict(HashMap::from([
        (
            "command".to_owned(),
            Value::Array(command_parts.iter().cloned().map(Value::String).collect()),
        ),
        ("exit_code".to_owned(), Value::Number(1.0)),
        ("signal".to_owned(), Value::Number(0.0)),
        ("core_dump".to_owned(), Value::Number(0.0)),
        ("timed_out".to_owned(), Value::Boolean(false)),
        ("error".to_owned(), Value::String(error)),
        ("stdout".to_owned(), Value::String(String::new())),
        ("stderr".to_owned(), Value::String(String::new())),
        ("ok".to_owned(), Value::Boolean(false)),
    ]))
}

fn process_timeout_result(command_parts: &[String], options: &ProcOptions) -> Value {
    let seconds = options
        .timeout
        .map(|timeout| timeout.as_secs_f64())
        .unwrap_or(0.0);
    Value::Dict(HashMap::from([
        (
            "command".to_owned(),
            Value::Array(command_parts.iter().cloned().map(Value::String).collect()),
        ),
        ("exit_code".to_owned(), Value::Number(1.0)),
        ("signal".to_owned(), Value::Number(14.0)),
        ("core_dump".to_owned(), Value::Number(0.0)),
        ("timed_out".to_owned(), Value::Boolean(true)),
        (
            "error".to_owned(),
            Value::String(format!("timeout after {seconds}s")),
        ),
        ("stdout".to_owned(), Value::String(String::new())),
        ("stderr".to_owned(), Value::String(String::new())),
        ("ok".to_owned(), Value::Boolean(false)),
    ]))
}

#[cfg(unix)]
fn failure_exit_status() -> std::process::ExitStatus {
    std::process::ExitStatus::from_raw(1 << 8)
}

#[cfg(windows)]
fn failure_exit_status() -> std::process::ExitStatus {
    std::process::ExitStatus::from_raw(1)
}

fn command_parts_from_value(value: &Value) -> Vec<String> {
    match value {
        Value::Shared(value) => command_parts_from_value(&value.borrow()),
        Value::Array(items) => items.iter().map(render_string).collect(),
        other => vec![render_string(other)],
    }
}

fn sleep(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    runtime.warn_blocking_operation("std/proc sleep")?;
    let seconds = match args.first() {
        Some(Value::Number(value)) => *value,
        Some(Value::Boolean(value)) => {
            if *value {
                1.0
            } else {
                0.0
            }
        }
        Some(Value::Null) | None => 0.0,
        Some(Value::String(value)) => value.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    };
    let duration = if seconds.is_finite() && seconds > 0.0 {
        Duration::from_secs_f64(seconds)
    } else {
        Duration::from_secs(0)
    };
    std::thread::sleep(duration);
    Ok(Value::Null)
}

fn proc_onsignal(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(ZuzuRustError::runtime(
            "Proc.onsignal() expects a signal name and callback",
        ));
    }
    let signal = normalize_signal_name(&args[0]);
    runtime
        .signal_handlers
        .borrow_mut()
        .entry(signal)
        .or_default()
        .push(args[1].clone());
    Ok(Value::Number(1.0))
}

fn proc_kill(runtime: &Runtime, args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(
            "Proc.kill() expects a signal name and optional pid",
        ));
    }
    let signal = normalize_signal_name(&args[0]);
    let handlers = runtime
        .signal_handlers
        .borrow()
        .get(&signal)
        .cloned()
        .unwrap_or_default();
    for callback in handlers {
        let _ = runtime.call_value(callback, Vec::new(), Vec::new())?;
    }
    let pid = args
        .get(1)
        .map(|value| match value {
            Value::Number(value) => *value as i64,
            _ => 0,
        })
        .unwrap_or_else(|| std::process::id() as i64);
    Ok(Value::Number(if pid > 0 { 1.0 } else { 0.0 }))
}

fn proc_is_success(args: &[Value]) -> Result<Value> {
    let Some(result) = args.first() else {
        return Ok(Value::Boolean(false));
    };
    let error = dict_get_string(result, "error").unwrap_or_default();
    let signalled = dict_get_number(result, "signal").unwrap_or(0.0) != 0.0;
    let exit_code = dict_get_number(result, "exit_code").unwrap_or(1.0);
    Ok(Value::Boolean(
        error.trim().is_empty() && !signalled && exit_code == 0.0,
    ))
}

fn proc_status_text(result: Option<&Value>) -> String {
    let Some(result) = result else {
        return "exit 1".to_owned();
    };
    if let Some(error) = dict_get_string(result, "error") {
        let error = error.trim();
        if !error.is_empty() && error != "null" {
            return format!("error: {error}");
        }
    }
    if let Some(signal) = dict_get_number(result, "signal") {
        if signal != 0.0 {
            return format!("signal {}", signal as i64);
        }
    }
    format!(
        "exit {}",
        dict_get_number(result, "exit_code").unwrap_or(1.0) as i64
    )
}

fn env_get(args: &[Value]) -> Result<Value> {
    let Some(name) = args.first() else {
        return Err(ZuzuRustError::runtime("Env.get() expects a variable name"));
    };
    let name = render_string(name);
    Ok(std::env::var(name)
        .map(Value::String)
        .unwrap_or_else(|_| args.get(1).cloned().unwrap_or(Value::Null)))
}

fn env_set(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(ZuzuRustError::runtime(
            "Env.set() expects a variable name and value",
        ));
    }
    let name = render_string(&args[0]);
    let value = render_string(&args[1]);
    unsafe {
        std::env::set_var(name, &value);
    }
    Ok(Value::String(value))
}

fn env_remove(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(ZuzuRustError::runtime(
            "Env.remove() expects a variable name",
        ));
    }
    let name = render_string(&args[0]);
    unsafe {
        std::env::remove_var(name);
    }
    Ok(Value::Null)
}

fn normalize_signal_name(value: &Value) -> String {
    let name = render_string(value);
    let trimmed = name.trim();
    trimmed
        .strip_prefix("SIG")
        .or_else(|| trimmed.strip_prefix("sig"))
        .unwrap_or(trimmed)
        .to_ascii_uppercase()
}

fn dict_get_string(value: &Value, key: &str) -> Option<String> {
    match value {
        Value::Dict(map) => match map.get(key) {
            Some(Value::String(value)) => Some(value.clone()),
            Some(Value::Null) | None => None,
            Some(other) => Some(render_string(other)),
        },
        _ => None,
    }
}

fn dict_get_number(value: &Value, key: &str) -> Option<f64> {
    match value {
        Value::Dict(map) => match map.get(key) {
            Some(Value::Number(value)) => Some(*value),
            Some(Value::Boolean(value)) => Some(if *value { 1.0 } else { 0.0 }),
            Some(Value::String(value)) => value.parse::<f64>().ok(),
            _ => None,
        },
        _ => None,
    }
}

fn dict_get_number_from_option(value: Option<&Value>, key: &str) -> Option<f64> {
    let Some(Value::Dict(map)) = value else {
        return None;
    };
    match map.get(key) {
        Some(Value::Number(value)) => Some(*value),
        Some(Value::Boolean(value)) => Some(if *value { 1.0 } else { 0.0 }),
        Some(Value::String(value)) => value.parse::<f64>().ok(),
        _ => None,
    }
}

fn dict_bool(value: Option<&Value>, key: &str) -> Option<bool> {
    let Some(Value::Dict(map)) = value else {
        return None;
    };
    match map.get(key) {
        Some(Value::Boolean(value)) => Some(*value),
        Some(Value::Number(value)) => Some(*value != 0.0),
        Some(Value::String(value)) => Some(!value.is_empty()),
        Some(Value::Null) | None => None,
        Some(other) => Some(other.is_truthy()),
    }
}

fn dict_string(value: Option<&Value>, key: &str) -> Option<String> {
    let Some(Value::Dict(map)) = value else {
        return None;
    };
    map.get(key).map(render_string)
}

fn dict_env(value: Option<&Value>) -> Vec<(String, Option<String>)> {
    let Some(Value::Dict(map)) = value else {
        return Vec::new();
    };
    let Some(Value::Dict(env)) = map.get("env") else {
        return Vec::new();
    };
    env.iter()
        .map(|(name, value)| {
            let value = match value {
                Value::Null => None,
                other => Some(render_string(other)),
            };
            (name.clone(), value)
        })
        .collect()
}

fn render_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::BinaryString(bytes) => String::from_utf8_lossy(bytes).to_string(),
        Value::Number(value) => value.to_string(),
        Value::Boolean(value) => {
            if *value {
                "true".to_owned()
            } else {
                "false".to_owned()
            }
        }
        Value::Null => String::new(),
        other => other.render(),
    }
}
