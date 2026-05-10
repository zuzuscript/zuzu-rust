use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use zuzu_rust::Runtime;

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(script) = args.next() else {
        eprintln!("usage: dump_test_output path/to/test.zzs");
        return ExitCode::from(2);
    };

    let cwd = env::current_dir().expect("cwd");
    let repo_root = find_repo_root(&cwd).expect("repo root");
    let runtime = Runtime::new(vec![
        repo_root.join("t/modules"),
        repo_root.join("stdlib").join("test-modules"),
        repo_root.join("modules"),
        repo_root.join("stdlib").join("modules"),
    ]);
    if let Err(err) = runtime.run_script_file(&PathBuf::from(&script)) {
        eprintln!("{err}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

fn find_repo_root(start: &PathBuf) -> Result<PathBuf, String> {
    let mut current = start.clone();
    loop {
        if current.join("modules").is_dir() || current.join("stdlib").join("modules").is_dir() {
            return Ok(current);
        }
        if !current.pop() {
            break;
        }
    }
    Err("could not locate repository root containing modules/ or stdlib/modules/".to_owned())
}
