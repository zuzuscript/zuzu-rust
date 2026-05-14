use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn temp_dir(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("zuzu-rust-helper-{}-{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("temp dir should be created");
    dir
}

fn run_parse_files(args: &[&str], cwd: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_zuzu-rust-parse-files"))
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("zuzu-rust-parse-files should run")
}

fn run_tests(args: &[&str], cwd: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_zuzu-rust-run-tests"))
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("zuzu-rust-run-tests should run")
}

#[test]
fn helper_binaries_print_help() {
    let parse_files = run_parse_files(&["--help"], &repo_root());
    assert!(parse_files.status.success());
    assert_eq!(String::from_utf8_lossy(&parse_files.stderr), "");
    assert!(String::from_utf8_lossy(&parse_files.stdout).contains("Usage: zuzu-rust-parse-files"));

    let run_tests = run_tests(&["--help"], &repo_root());
    assert!(run_tests.status.success());
    assert_eq!(String::from_utf8_lossy(&run_tests.stderr), "");
    let run_tests_stdout = String::from_utf8_lossy(&run_tests.stdout);
    assert!(run_tests_stdout.contains("Usage: zuzu-rust-run-tests"));
    assert!(run_tests_stdout.contains("-q"));
    assert!(run_tests_stdout.contains("-Q"));
}

#[test]
fn parse_files_accepts_runtime_parity_options() {
    let dir = temp_dir("parse-files-options");
    let source = dir.join("sample.zzs");
    fs::write(&source, "say 1;\n").expect("sample script should be written");

    let include_arg = format!("-I{}", dir.display());
    let source_arg = source.display().to_string();
    let output = run_parse_files(
        &[
            &include_arg,
            "--deny=fs,net",
            "--denymodule",
            "std/gui",
            "-d0",
            &source_arg,
        ],
        &repo_root(),
    );

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
}

#[test]
fn run_tests_defaults_debug_to_one() {
    let dir = temp_dir("run-tests-debug-default");
    let source = dir.join("debug-default.zzs");
    fs::write(
        &source,
        r#"
        say "1..1";
        debug 1, "debug-one";
        say "ok 1";
        "#,
    )
    .expect("test script should be written");

    let source_arg = source.display().to_string();
    let output = run_tests(&[&source_arg], &repo_root());

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("debug-one"));
    assert!(String::from_utf8_lossy(&output.stdout).contains("ok 1"));
}

#[test]
fn run_tests_allows_explicit_debug_zero_and_runtime_policy_options() {
    let dir = temp_dir("run-tests-debug-zero");
    let source = dir.join("debug-zero.zzs");
    fs::write(
        &source,
        r#"
        say "1..1";
        debug 1, "hidden-debug";
        if ( __system__{deny_fs} ) {
            say "ok 1";
        }
        else {
            say "not ok 1";
        }
        "#,
    )
    .expect("test script should be written");

    let include_arg = format!("-I{}", dir.display());
    let source_arg = source.display().to_string();
    let output = run_tests(
        &[
            "-d0",
            "--deny",
            "fs",
            "--denymodule=std/gui",
            &include_arg,
            &source_arg,
        ],
        &repo_root(),
    );

    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("hidden-debug"));
    assert!(String::from_utf8_lossy(&output.stdout).contains("ok 1"));
}

#[test]
fn run_tests_quiet_prints_one_line_per_file() {
    let dir = temp_dir("run-tests-quiet");
    let passing = dir.join("passing.zzs");
    let failing = dir.join("failing.zzs");
    fs::write(
        &passing,
        r#"
        say "1..1";
        say "ok 1 - hidden tap";
        "#,
    )
    .expect("passing test script should be written");
    fs::write(
        &failing,
        r#"
        say "1..1";
        say "not ok 1 - hidden tap";
        "#,
    )
    .expect("failing test script should be written");

    let dir_arg = dir.display().to_string();
    let output = run_tests(&["-q", &dir_arg], &repo_root());

    assert!(!output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    assert!(lines
        .iter()
        .any(|line| line.contains("✅") && line.contains("passing.zzs")));
    assert!(lines
        .iter()
        .any(|line| line.contains("❌") && line.contains("failing.zzs")));
    assert!(!stdout.contains("hidden tap"));
    assert!(!stdout.contains("total:"));
}

#[test]
fn run_tests_fails_bad_plan_with_reason_in_verbose_mode() {
    let dir = temp_dir("run-tests-bad-plan-verbose");
    let source = dir.join("bad-plan.zzs");
    fs::write(
        &source,
        r#"
        say "1..2";
        say "ok 1 - only test";
        "#,
    )
    .expect("test script should be written");

    let source_arg = source.display().to_string();
    let output = run_tests(&[&source_arg], &repo_root());

    assert!(!output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("bad TAP plan: planned 2 top-level tests, saw 1"));
}

#[test]
fn run_tests_fails_bad_plan_with_reason_in_quiet_mode() {
    let dir = temp_dir("run-tests-bad-plan-quiet");
    let source = dir.join("bad-plan.zzs");
    fs::write(
        &source,
        r#"
        say "1..1";
        say "ok 1 - first";
        say "ok 2 - extra";
        "#,
    )
    .expect("test script should be written");

    let source_arg = source.display().to_string();
    let output = run_tests(&["-q", &source_arg], &repo_root());

    assert!(!output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("bad TAP plan: planned 1 top-level tests, saw 2"));
    assert!(!stdout.contains("ok 1 - first"));
}

#[test]
fn run_tests_fails_top_level_not_ok() {
    let dir = temp_dir("run-tests-top-not-ok");
    let source = dir.join("top-not-ok.zzs");
    fs::write(
        &source,
        r#"
        say "1..1";
        say "not ok 1 - top failure";
        "#,
    )
    .expect("test script should be written");

    let source_arg = source.display().to_string();
    let output = run_tests(&["-q", &source_arg], &repo_root());

    assert!(!output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
    assert!(!String::from_utf8_lossy(&output.stdout).contains("bad TAP plan"));
}

#[test]
fn run_tests_ignores_indented_subtest_output() {
    let dir = temp_dir("run-tests-indented-subtest");
    let source = dir.join("indented-subtest.zzs");
    fs::write(
        &source,
        r#"
        say "    1..2";
        say "    ok 1 - nested pass";
        say "    not ok 2 - nested fail";
        say "1..1";
        say "ok 1 - parent";
        "#,
    )
    .expect("test script should be written");

    let source_arg = source.display().to_string();
    let output = run_tests(&["-q", &source_arg], &repo_root());

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
    assert!(String::from_utf8_lossy(&output.stdout).contains("✅"));
}

#[test]
fn run_tests_ultra_quiet_prints_single_overall_line() {
    let dir = temp_dir("run-tests-ultra-quiet");
    let passing = dir.join("passing.zzs");
    let failing = dir.join("failing.zzs");
    fs::write(
        &passing,
        r#"
        say "1..1";
        say "ok 1 - hidden tap";
        "#,
    )
    .expect("passing test script should be written");
    fs::write(
        &failing,
        r#"
        say "1..1";
        say "not ok 1 - hidden tap";
        "#,
    )
    .expect("failing test script should be written");

    let dir_arg = dir.display().to_string();
    let output = run_tests(&["-Q", &dir_arg], &repo_root());

    assert!(!output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines, vec!["❌  total: 1 passed, 1 failed"]);
}
