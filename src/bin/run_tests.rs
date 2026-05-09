use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use zuzu_rust::Runtime;

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
        return Err("usage: zuzu-rust-run-tests <test-file-or-directory> [...]".to_owned());
    }

    let repo_root = find_repo_root(&env::current_dir().map_err(|err| err.to_string())?)?;
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
        let runtime = Runtime::new(test_module_roots(&repo_root));
        match runtime.run_script_file(&file) {
            Ok(output) if tap_passed(&output.stdout) => {
                println!("✅  {display}");
                passed += 1;
            }
            Ok(_) | Err(_) => {
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

fn test_module_roots(repo_root: &Path) -> Vec<PathBuf> {
    vec![repo_root.join("t/modules"), repo_root.join("modules")]
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
        if current.join("modules").is_dir() {
            return Ok(current);
        }
        if !current.pop() {
            break;
        }
    }
    Err("could not locate repository root containing modules/".to_owned())
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
