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
    if args.is_empty() {
        return Err("usage: zuzu-rust-parse-files <file-or-directory> [...]".to_owned());
    }

    let mut files = Vec::new();
    for arg in &args {
        collect_targets(Path::new(arg), &mut files)?;
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
