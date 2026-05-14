use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Output};

use zuzu_rust::{module_search_roots, Runtime, RuntimePolicy};

const RUN_ONE_ARG: &str = "--__zuzu-rust-run-one";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verbosity {
    Normal,
    Quiet,
    UltraQuiet,
}

#[derive(Clone, Debug)]
struct TestRunnerOptions {
    targets: Vec<PathBuf>,
    include_dirs: Vec<PathBuf>,
    denied_capabilities: Vec<String>,
    denied_modules: Vec<String>,
    debug_level: u32,
    show_help: bool,
    verbosity: Verbosity,
}

impl Default for TestRunnerOptions {
    fn default() -> Self {
        Self {
            targets: Vec::new(),
            include_dirs: Vec::new(),
            denied_capabilities: Vec::new(),
            denied_modules: Vec::new(),
            debug_level: 1,
            show_help: false,
            verbosity: Verbosity::Normal,
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(all_ok) => {
            if all_ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        Err(message) => {
            eprintln!("{message}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<bool, String> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some(RUN_ONE_ARG) {
        let options = parse_args(&args[1..])?;
        if options.show_help {
            print_help();
            return Ok(true);
        }
        let file = match options.targets.as_slice() {
            [file] => file,
            _ => {
                return Err(format!(
                    "usage: zuzu-rust-run-tests {RUN_ONE_ARG} [options] <test-file>"
                ));
            }
        };
        return run_one(file, &options);
    }

    let options = parse_args(&args)?;
    if options.show_help {
        print_help();
        return Ok(true);
    }
    if options.targets.is_empty() {
        return Err(
            "usage: zuzu-rust-run-tests [options] <test-file-or-directory> [...]".to_owned(),
        );
    }

    let mut files = Vec::new();
    for target in &options.targets {
        collect_targets(target, &mut files)?;
    }
    files.sort();
    files.dedup();

    let mut all_ok = true;
    let mut passed = 0usize;
    let mut failed = 0usize;
    let zuzu = zuzu_binary_path()?;

    for file in files {
        let display = file.display().to_string();
        let output = run_test_child(&file, &zuzu, &options)?;
        if options.verbosity == Verbosity::Normal {
            print!("{}", String::from_utf8_lossy(&output.stdout));
            eprint!("{}", String::from_utf8_lossy(&output.stderr));
        }

        let (file_ok, failure_reason) = test_output_passed(&output);
        if file_ok {
            if options.verbosity != Verbosity::UltraQuiet {
                print_file_result(true, &display, None);
            }
            passed += 1;
        } else {
            if options.verbosity != Verbosity::UltraQuiet {
                print_file_result(false, &display, failure_reason.as_deref());
            }
            failed += 1;
            all_ok = false;
        }
    }

    if options.verbosity == Verbosity::UltraQuiet
        || (options.verbosity == Verbosity::Normal && passed + failed > 1)
    {
        print_total_result(passed, failed);
    }

    Ok(all_ok)
}

fn print_help() {
    println!(
        "\
Usage: zuzu-rust-run-tests [options] <test-file-or-directory> [...]
Options:
  -d[=N]                 set debug level (defaults to -d1)
  -I/path/to/lib         add module include directory
  -q                     quiet: print one result line per test file
  -Q                     ultra-quiet: print one overall result line
  --deny=CAP             deny runtime capability (repeatable or comma-separated)
  --denymodule=MODULE    deny a specific module (repeatable or comma-separated)
  -h, --help             show this help"
    );
}

fn parse_args(args: &[String]) -> Result<TestRunnerOptions, String> {
    let mut options = TestRunnerOptions::default();
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        match arg.as_str() {
            "-h" | "--help" => {
                options.show_help = true;
            }
            "-q" => {
                options.verbosity = Verbosity::Quiet;
            }
            "-Q" => {
                options.verbosity = Verbosity::UltraQuiet;
            }
            "--" => {
                options
                    .targets
                    .extend(args[index + 1..].iter().map(PathBuf::from));
                break;
            }
            "-I" => {
                index += 1;
                options
                    .include_dirs
                    .push(PathBuf::from(expect_arg(args, index, "-I")?));
            }
            "--deny" => {
                index += 1;
                options
                    .denied_capabilities
                    .extend(split_csv(expect_arg(args, index, "--deny")?));
            }
            "--denymodule" => {
                index += 1;
                options
                    .denied_modules
                    .extend(split_csv(expect_arg(args, index, "--denymodule")?));
            }
            "-d" => {
                options.debug_level = 1;
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
            _ if arg.starts_with('-') => {
                return Err(format!("unsupported option: {arg}"));
            }
            _ => options.targets.push(PathBuf::from(arg)),
        }
        index += 1;
    }
    Ok(options)
}

fn expect_arg<'a>(args: &'a [String], index: usize, option: &str) -> Result<&'a str, String> {
    args.get(index)
        .map(String::as_str)
        .ok_or_else(|| format!("expected value after {option}"))
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect()
}

fn is_debug_short_option(arg: &str) -> bool {
    let Some(value) = arg.strip_prefix("-d") else {
        return false;
    };
    !value.is_empty() && value.chars().all(|ch| ch.is_ascii_digit())
}

fn parse_debug_level(value: &str) -> Result<u32, String> {
    if value.is_empty() {
        return Ok(1);
    }
    value
        .parse::<u32>()
        .map_err(|_| "debug level must be a non-negative integer".to_owned())
}

fn run_one(file: &Path, options: &TestRunnerOptions) -> Result<bool, String> {
    let runtime = Runtime::with_policy(
        module_search_roots(options.include_dirs.clone()),
        runtime_policy(
            options.denied_capabilities.clone(),
            options.denied_modules.clone(),
        )
        .debug_level(options.debug_level),
    );
    match runtime.run_script_file(file) {
        Ok(output) => Ok(tap_passed(&output.stdout)),
        Err(err) => {
            eprintln!("{err}");
            Ok(false)
        }
    }
}

fn run_test_child(file: &Path, zuzu: &Path, options: &TestRunnerOptions) -> Result<Output, String> {
    let current_exe = env::current_exe().map_err(|err| err.to_string())?;
    let mut command = Command::new(current_exe);
    command.arg(RUN_ONE_ARG);
    for arg in child_runtime_args(options) {
        command.arg(arg);
    }
    command
        .arg(file)
        .env("ZUZU", zuzu)
        .output()
        .map_err(|err| format!("failed to run {}: {err}", file.display()))
}

fn child_runtime_args(options: &TestRunnerOptions) -> Vec<String> {
    let mut args = Vec::new();
    for path in &options.include_dirs {
        args.push("-I".to_owned());
        args.push(path.display().to_string());
    }
    for capability in &options.denied_capabilities {
        args.push("--deny".to_owned());
        args.push(capability.clone());
    }
    for module in &options.denied_modules {
        args.push("--denymodule".to_owned());
        args.push(module.clone());
    }
    args.push(format!("-d{}", options.debug_level));
    args
}

fn zuzu_binary_path() -> Result<PathBuf, String> {
    let current_exe = env::current_exe().map_err(|err| err.to_string())?;
    let mut path = current_exe.to_path_buf();
    path.set_file_name(format!("zuzu-rust{}", env::consts::EXE_SUFFIX));
    if path.is_file() {
        Ok(path)
    } else {
        Err(format!(
            "could not locate zuzu-rust binary next to {}",
            current_exe.display()
        ))
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct TapAnalysis {
    passed: bool,
    failure_reason: Option<String>,
}

fn test_output_passed(output: &Output) -> (bool, Option<String>) {
    let analysis = analyse_tap(&String::from_utf8_lossy(&output.stdout));
    (
        output.status.success() && analysis.passed,
        analysis.failure_reason,
    )
}

fn print_file_result(passed: bool, display: &str, failure_reason: Option<&str>) {
    if passed {
        println!("✅  {display}");
    } else if let Some(reason) = failure_reason {
        println!("❌  {display} ({reason})");
    } else {
        println!("❌  {display}");
    }
}

fn print_total_result(passed: usize, failed: usize) {
    if failed == 0 {
        println!("✅  total: {passed} passed, {failed} failed");
    } else {
        println!("❌  total: {passed} passed, {failed} failed");
    }
}

fn tap_passed(stdout: &str) -> bool {
    analyse_tap(stdout).passed
}

fn analyse_tap(stdout: &str) -> TapAnalysis {
    let mut plan = None;
    let mut top_level_tests = 0usize;
    let mut has_top_level_not_ok = false;
    let mut plan_error = None;

    for line in stdout.lines().filter(|line| is_top_level_tap_line(line)) {
        if line.starts_with("1..") {
            match parse_plan_count(line) {
                Some(count) if plan.is_none() => {
                    plan = Some(count);
                }
                Some(_) => {
                    plan_error = Some("bad TAP plan: multiple top-level plans".to_owned());
                }
                None => {
                    plan_error = Some("bad TAP plan: malformed top-level plan".to_owned());
                }
            }
        }

        if is_top_level_ok_line(line) {
            top_level_tests += 1;
        } else if is_top_level_not_ok_line(line) {
            top_level_tests += 1;
            has_top_level_not_ok = true;
        }
    }

    let failure_reason = if let Some(reason) = plan_error {
        Some(reason)
    } else {
        match plan {
            Some(planned) if planned != top_level_tests => Some(format!(
                "bad TAP plan: planned {planned} top-level tests, saw {top_level_tests}"
            )),
            None => Some("bad TAP plan: missing top-level plan".to_owned()),
            _ if has_top_level_not_ok => None,
            _ => None,
        }
    };

    TapAnalysis {
        passed: failure_reason.is_none() && !has_top_level_not_ok,
        failure_reason,
    }
}

fn is_top_level_tap_line(line: &str) -> bool {
    !line.starts_with(|ch: char| ch.is_ascii_whitespace())
}

fn parse_plan_count(line: &str) -> Option<usize> {
    let rest = line.strip_prefix("1..")?;
    let digits_len = rest
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digits_len == 0 {
        return None;
    }
    let (digits, trailing) = rest.split_at(digits_len);
    if !trailing.is_empty()
        && !trailing
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_whitespace() || ch == '#')
    {
        return None;
    }
    digits.parse::<usize>().ok()
}

fn is_top_level_ok_line(line: &str) -> bool {
    has_tap_keyword(line, "ok")
}

fn is_top_level_not_ok_line(line: &str) -> bool {
    has_tap_keyword(line, "not ok")
}

fn has_tap_keyword(line: &str, keyword: &str) -> bool {
    let Some(rest) = line.strip_prefix(keyword) else {
        return false;
    };
    rest.is_empty()
        || rest
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_whitespace())
}

fn collect_targets(path: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let metadata =
        fs::metadata(path).map_err(|err| format!("failed to stat {}: {err}", path.display()))?;

    if metadata.is_file() {
        if is_test_script(path) {
            out.push(path.to_path_buf());
        }
        return Ok(());
    }

    if metadata.is_dir() {
        collect_dir(path, out)?;
        return Ok(());
    }

    Ok(())
}

fn collect_dir(path: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = fs::read_dir(path)
        .map_err(|err| format!("failed to read directory {}: {err}", path.display()))?;

    for entry in entries {
        let entry = entry.map_err(|err| {
            format!(
                "failed to read directory entry in {}: {err}",
                path.display()
            )
        })?;
        let child = entry.path();
        let metadata = entry
            .metadata()
            .map_err(|err| format!("failed to stat {}: {err}", child.display()))?;

        if metadata.is_dir() {
            collect_dir(&child, out)?;
        } else if metadata.is_file() && is_test_script(&child) {
            out.push(child);
        }
    }

    Ok(())
}

fn is_test_script(path: &Path) -> bool {
    matches!(path.extension().and_then(|ext| ext.to_str()), Some("zzs"))
}
