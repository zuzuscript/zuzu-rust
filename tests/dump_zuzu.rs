use std::path::{Path, PathBuf};
use std::process::Command;

use zuzu_rust::{codegen, parse_program_with_compile_options, OptimizationOptions, ParseOptions};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("repo root should exist")
        .to_path_buf()
}

fn run_zuzu(args: &[&str], cwd: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_zuzu-rust"))
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("zuzu-rust should run")
}

fn parse_loose(source: &str) -> zuzu_rust::Program {
    let options = ParseOptions::new(false, true, OptimizationOptions::disabled());
    parse_program_with_compile_options(source, &options).expect("source should parse")
}

#[test]
fn dump_zuzu_prints_optimized_source() {
    let source = "let a := 2; let b := 3; say a * b;";
    let output = run_zuzu(&["--dump-zuzu", "-o3", "-e", source], &repo_root());

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "let a := 2;\nlet b := 3;\nsay 6;\n"
    );
}

#[test]
fn dump_zuzu_honours_no_sema_and_remains_parseable() {
    let output = run_zuzu(
        &[
            "--dump-zuzu",
            "--no-sema",
            "-e",
            "let foo := 1; let foo := 2; say foo;",
        ],
        &repo_root(),
    );

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
    let dumped = String::from_utf8(output.stdout).expect("stdout should be UTF-8");
    assert!(dumped.contains("say 2;"));

    let reparsed = run_zuzu(
        &["--dump-ast", "--no-sema", "-e", dumped.as_str()],
        &repo_root(),
    );
    assert!(reparsed.status.success());
    assert_eq!(String::from_utf8_lossy(&reparsed.stderr), "");
}

#[test]
fn dump_modes_are_mutually_exclusive() {
    let output = run_zuzu(&["--dump-ast", "--dump-zuzu", "-e", "say 1;"], &repo_root());

    assert!(!output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "");
    assert!(String::from_utf8_lossy(&output.stderr).contains("expected at most one dump mode"));
}

#[test]
fn codegen_round_trips_representative_program() {
    let source = r#"
    function add ( x, y := 1 ) -> Number {
        return x + y;
    }

    trait Named {
        method name () {
            return "anon";
        }
    }

    class Counter with Named {
        let value with get, set := 0;

        method inc () {
            self.value++;
            return self.value;
        }
    }

    let xs := [ 1, 2, 3 ];
    if ( add(1) > 1 ) {
        say `ok ${ add(1) }`;
    }
    else {
        say "no";
    }

    for ( let x in xs ) {
        say x;
    }
    else {
        say "empty";
    }

    switch ( xs.length : == ) {
        case 3:
            say "three";
        default:
            say "other";
    }

    try {
        throw "err";
    }
    catch ( e ) {
        warn e;
    }

    say fn ( n ) -> n * 2;
    "#;

    let program = parse_loose(source);
    let rendered = codegen::render_program(&program);
    parse_loose(&rendered);

    assert!(rendered.contains("let value with get, set := 0;"));
    assert!(rendered.contains("switch ( xs.length : =="));
    assert!(rendered.contains("say fn ( n ) -> n * 2;"));
}
