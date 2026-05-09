use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::process::ExitCode;
use std::time::{Duration, Instant};

const DEFAULT_ITERATIONS: usize = 3;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<(), String> {
    let config = Config::from_args(env::args().skip(1))?;
    let repo_root = find_repo_root(&env::current_dir().map_err(|err| err.to_string())?)?;
    let workloads = collect_workloads(&repo_root, &config.paths)?;

    println!("workload\tlevel\titerations\ttotal_ms\tavg_ms");
    for workload in workloads {
        benchmark_workload(&repo_root, &workload, config.iterations)?;
    }

    Ok(())
}

struct Config {
    iterations: usize,
    paths: Vec<PathBuf>,
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut iterations = DEFAULT_ITERATIONS;
        let mut paths = Vec::new();
        let mut pending_iterations = false;

        for arg in args {
            if pending_iterations {
                iterations = parse_iterations(&arg)?;
                pending_iterations = false;
                continue;
            }
            if arg == "--iterations" {
                pending_iterations = true;
                continue;
            }
            if let Some(value) = arg.strip_prefix("--iterations=") {
                iterations = parse_iterations(value)?;
                continue;
            }
            if arg == "--help" || arg == "-h" {
                return Err(usage());
            }
            paths.push(PathBuf::from(arg));
        }

        if pending_iterations {
            return Err("--iterations requires a value".to_owned());
        }

        Ok(Self { iterations, paths })
    }
}

fn parse_iterations(value: &str) -> Result<usize, String> {
    let iterations = value
        .parse::<usize>()
        .map_err(|_| format!("invalid iteration count: {value}"))?;
    if iterations == 0 {
        return Err("--iterations must be greater than zero".to_owned());
    }
    Ok(iterations)
}

fn usage() -> String {
    concat!(
        "usage: zuzu-rust-benchmark-optimizations ",
        "[--iterations N] [test-file-or-directory ...]"
    )
    .to_owned()
}

fn collect_workloads(repo_root: &Path, paths: &[PathBuf]) -> Result<Vec<PathBuf>, String> {
    let mut workloads = Vec::new();
    if paths.is_empty() {
        workloads.extend(default_workloads(repo_root));
    } else {
        for path in paths {
            collect_target(path, &mut workloads)?;
        }
    }
    workloads.sort();
    workloads.dedup();
    Ok(workloads)
}

fn default_workloads(repo_root: &Path) -> Vec<PathBuf> {
    [
        "t/ztests/std/path/z.zzs",
        "t/ztests/std/data/cbor/_loaddump.zzs",
        "t/ztests/std/data/kdl/_loaddump.zzs",
    ]
    .into_iter()
    .map(|path| repo_root.join(path))
    .collect()
}

fn collect_target(path: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let metadata =
        fs::metadata(path).map_err(|err| format!("failed to stat {}: {err}", path.display()))?;

    if metadata.is_file() {
        if is_zuzu_test(path) {
            out.push(
                fs::canonicalize(path)
                    .map_err(|err| format!("failed to canonicalize {}: {err}", path.display()))?,
            );
        }
        return Ok(());
    }
    if metadata.is_dir() {
        collect_dir(path, out)?;
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
        } else if metadata.is_file() && is_zuzu_test(&child) {
            out.push(
                fs::canonicalize(&child)
                    .map_err(|err| format!("failed to canonicalize {}: {err}", child.display()))?,
            );
        }
    }

    Ok(())
}

fn is_zuzu_test(path: &Path) -> bool {
    matches!(path.extension().and_then(|ext| ext.to_str()), Some("zzs"))
}

fn benchmark_workload(repo_root: &Path, path: &Path, iterations: usize) -> Result<(), String> {
    for label in ["o0", "o1", "o2", "o3"] {
        let elapsed = run_iterations(repo_root, path, label, iterations)?;
        let total_ms = elapsed.as_secs_f64() * 1000.0;
        let avg_ms = total_ms / iterations as f64;
        println!(
            "{}\t{}\t{}\t{:.3}\t{:.3}",
            path.display(),
            label,
            iterations,
            total_ms,
            avg_ms
        );
    }

    Ok(())
}

fn run_iterations(
    repo_root: &Path,
    path: &Path,
    level: &str,
    iterations: usize,
) -> Result<Duration, String> {
    let subject = benchmark_subject_exe()?;
    let opt_arg = format!("-{level}");
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        let started = Instant::now();
        let output = Command::new(&subject)
            .arg(&opt_arg)
            .arg(path)
            .current_dir(repo_root)
            .output()
            .map_err(|err| format!("failed to run {}: {err}", subject.display()))?;
        elapsed += started.elapsed();

        if !output.stderr.is_empty() {
            return Err(format!(
                "{} wrote to stderr at {level}: {}",
                path.display(),
                String::from_utf8_lossy(&output.stderr).trim_end()
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !tap_passed(&stdout) {
            return Err(format!(
                "{} produced failing TAP at {level}",
                path.display()
            ));
        }
        if !output.status.success() {
            return Err(format!(
                "{} exited with {} at {level}; stdout was: {}",
                path.display(),
                output.status,
                stdout.trim_end()
            ));
        }
    }
    Ok(elapsed)
}

fn benchmark_subject_exe() -> Result<PathBuf, String> {
    let current = env::current_exe().map_err(|err| err.to_string())?;
    let Some(dir) = current.parent() else {
        return Err("could not locate benchmark executable directory".to_owned());
    };
    let exe_name = if cfg!(windows) {
        "zuzu-rust.exe"
    } else {
        "zuzu-rust"
    };
    Ok(dir.join(exe_name))
}

fn tap_passed(stdout: &str) -> bool {
    let has_plan = stdout
        .lines()
        .any(|line| line.trim_start().starts_with("1.."));
    let has_not_ok = stdout
        .lines()
        .any(|line| line.trim_start().starts_with("not ok"));
    has_plan && !has_not_ok
}

fn find_repo_root(start: &Path) -> Result<PathBuf, String> {
    let mut current = start.to_path_buf();
    loop {
        if current.join("modules").is_dir() && current.join("t").is_dir() {
            return Ok(current);
        }
        if !current.pop() {
            break;
        }
    }
    Err("could not locate repository root containing modules/ and t/".to_owned())
}
