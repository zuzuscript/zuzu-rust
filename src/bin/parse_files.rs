use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use zuzu_rust::parse_program;

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
    if args
        .iter()
        .any(|arg| matches!(arg.as_str(), "-h" | "--help"))
    {
        print_help();
        return Ok(true);
    }

    let targets = parse_args(&args)?;
    if targets.is_empty() {
        return Err("usage: zuzu-rust-parse-files [options] <file-or-directory> [...]".to_owned());
    }

    let mut files = Vec::new();
    for target in &targets {
        collect_targets(target, &mut files)?;
    }
    files.sort();
    files.dedup();

    let mut all_ok = true;
    let mut passed = 0usize;
    let mut failed = 0usize;
    for file in files {
        let display = file.display().to_string();
        match fs::read_to_string(&file) {
            Ok(source) => match parse_program(&source) {
                Ok(_) => {
                    println!("✅  {display}");
                    passed += 1;
                }
                Err(_) => {
                    println!("❌  {display}");
                    failed += 1;
                    all_ok = false;
                }
            },
            Err(_) => {
                println!("❌  {display}");
                failed += 1;
                all_ok = false;
            }
        }
    }

    if passed + failed > 1 {
        if failed == 0 {
            println!("✅  total: {passed} passed, {failed} failed");
        } else {
            println!("❌  total: {passed} passed, {failed} failed");
        }
    }

    Ok(all_ok)
}

fn print_help() {
    println!(
        "\
Usage: zuzu-rust-parse-files [options] <file-or-directory> [...]
Options:
  -d[=N]                 set debug level (accepted for CLI parity)
  -I/path/to/lib         add module include directory (accepted for CLI parity)
  --deny=CAP             deny runtime capability (accepted for CLI parity)
  --denymodule=MODULE    deny a specific module (accepted for CLI parity)
  -h, --help             show this help"
    );
}

fn parse_args(args: &[String]) -> Result<Vec<PathBuf>, String> {
    let mut targets = Vec::new();
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        match arg.as_str() {
            "--" => {
                targets.extend(args[index + 1..].iter().map(PathBuf::from));
                break;
            }
            "-I" => {
                index += 1;
                expect_arg(args, index, "-I")?;
            }
            "--deny" => {
                index += 1;
                let value = expect_arg(args, index, "--deny")?;
                let _ = split_csv(value);
            }
            "--denymodule" => {
                index += 1;
                let value = expect_arg(args, index, "--denymodule")?;
                let _ = split_csv(value);
            }
            "-d" => {
                let _ = parse_debug_level("")?;
            }
            _ if arg.starts_with("-I") && arg.len() > 2 => {}
            _ if arg.starts_with("-d=") => {
                let _ = parse_debug_level(&arg[3..])?;
            }
            _ if is_debug_short_option(arg) => {
                let _ = parse_debug_level(&arg[2..])?;
            }
            _ if arg.starts_with("--deny=") => {
                let _ = split_csv(&arg["--deny=".len()..]);
            }
            _ if arg.starts_with("--denymodule=") => {
                let _ = split_csv(&arg["--denymodule=".len()..]);
            }
            _ if arg.starts_with('-') => {
                return Err(format!("unsupported option: {arg}"));
            }
            _ => targets.push(PathBuf::from(arg)),
        }
        index += 1;
    }
    Ok(targets)
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

fn collect_targets(path: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let metadata =
        fs::metadata(path).map_err(|err| format!("failed to stat {}: {err}", path.display()))?;

    if metadata.is_file() {
        if is_zuzu_file(path) {
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
        } else if metadata.is_file() && is_zuzu_file(&child) {
            out.push(child);
        }
    }

    Ok(())
}

fn is_zuzu_file(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("zzs") | Some("zzm")
    )
}
