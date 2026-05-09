use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("repo root should exist")
        .to_path_buf()
}

fn temp_dir(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("zuzu-rust-phase6-{}-{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("temp dir should be created");
    dir
}

fn run_zuzu(args: &[&str], cwd: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_zuzu-rust"))
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("zuzu-rust should run")
}

fn run_zuzu_with_stdin(args: &[&str], cwd: &Path, stdin: &str) -> std::process::Output {
    run_zuzu_with_stdin_and_env(args, cwd, stdin, &[])
}

fn run_zuzu_with_stdin_and_env(
    args: &[&str],
    cwd: &Path,
    stdin: &str,
    envs: &[(&str, &str)],
) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_zuzu-rust"));
    command
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, value) in envs {
        command.env(key, value);
    }
    let mut child = command.spawn().expect("zuzu-rust should run");
    child
        .stdin
        .as_mut()
        .expect("stdin should be piped")
        .write_all(stdin.as_bytes())
        .expect("stdin should be written");
    child.wait_with_output().expect("zuzu-rust should finish")
}

#[test]
fn cli_no_sema_skips_validation_during_execution() {
    let output = run_zuzu(
        &["--no-sema", "-e", "let foo := 1; let foo := 2; say foo;"],
        &repo_root(),
    );

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "2\n");
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");

    let checked = run_zuzu(
        &["-e", "let foo := 1; let foo := 2; say foo;"],
        &repo_root(),
    );
    assert!(!checked.status.success());
    assert!(String::from_utf8_lossy(&checked.stderr)
        .contains("Redeclaration of 'foo' in the same scope"));
}

#[test]
fn cli_allows_inline_source_attached_to_e_option() {
    let output = run_zuzu(&["-esay 1;"], &repo_root());

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "1\n");
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
}

#[test]
fn cli_no_sema_applies_to_imported_pure_modules() {
    let dir = temp_dir("no-sema-module");
    let lib = dir.join("lib");
    let module_dir = lib.join("acme");
    fs::create_dir_all(&module_dir).expect("module dir should be created");
    fs::write(
        module_dir.join("loose.zzm"),
        r#"
        let answer := 1;
        let answer := 42;
        "#,
    )
    .expect("module should be written");

    let include_arg = format!("-I{}", lib.display());
    let output = run_zuzu(
        &[
            "--no-sema",
            &include_arg,
            "-e",
            "from acme/loose import answer; say answer;",
        ],
        &repo_root(),
    );

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "42\n");
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
}

#[test]
fn cli_optimization_flags_control_dump_ast() {
    let source = "let a := 2; let b := 3; say a * b;";

    let o0 = run_zuzu(&["--dump-ast", "-o0", "-e", source], &repo_root());
    assert!(o0.status.success());
    let o0_stdout = String::from_utf8_lossy(&o0.stdout);
    assert!(o0_stdout.contains("\"type\": \"BinaryExpression\""));
    assert!(o0_stdout.contains("\"operator\": \"*\""));

    let o3 = run_zuzu(&["--dump-ast", "-o3", "-e", source], &repo_root());
    assert!(o3.status.success());
    let o3_stdout = String::from_utf8_lossy(&o3.stdout);
    assert!(o3_stdout.contains("\"type\": \"NumberLiteral\""));
    assert!(o3_stdout.contains("\"value\": \"6\""));
    assert!(!o3_stdout.contains("\"type\": \"BinaryExpression\""));

    let no_folding = run_zuzu(
        &[
            "--dump-ast",
            "-o3",
            "--no-opt=constant-folding",
            "-e",
            source,
        ],
        &repo_root(),
    );
    assert!(no_folding.status.success());
    let no_folding_stdout = String::from_utf8_lossy(&no_folding.stdout);
    assert!(no_folding_stdout.contains("\"type\": \"BinaryExpression\""));
}

#[test]
fn cli_rejects_unknown_optimization_passes() {
    let output = run_zuzu(
        &["--dump-ast", "--opt=no-such-pass", "-e", "say 1;"],
        &repo_root(),
    );

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknown optimization pass"));
}

#[test]
fn cli_opt_help_lists_passes_and_level_defaults() {
    let output = run_zuzu(&["--opt-help"], &repo_root());

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(stdout.contains("Available optimization passes:"));
    assert!(stdout.contains("  block-scope-elision"));
    assert!(stdout.contains("  constant-condition-pruning"));
    assert!(stdout.contains("  regex-cache"));
    assert!(stdout.contains("  collection-presize"));
    assert!(stdout.contains("  range-array-loop-lowering"));

    assert!(stdout.contains("  -o0: (none)"));
    assert!(stdout.contains(
        "  -o1: block-scope-elision, constant-folding, \
         unreachable-pruning, regex-cache, typecheck-skip, collection-presize"
    ));
    assert!(stdout.contains(
        "  -o2: block-scope-elision, constant-folding, \
         unreachable-pruning, regex-cache, identifier-resolution, \
         typecheck-skip, operator-enum, collection-presize"
    ));
    assert!(stdout.contains(
        "  -o3: block-scope-elision, constant-folding, \
         unreachable-pruning, regex-cache, identifier-resolution, \
         typecheck-skip, operator-enum, collection-presize, switch-indexing, \
         range-array-loop-lowering"
    ));
}

#[test]
fn cli_help_describes_dump_zuzu_as_parsed_ast_source() {
    let output = run_zuzu(&["--help"], &repo_root());

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--opt-help"));
    assert!(stdout.contains("--dump-zuzu            print parsed AST as ZuzuScript source"));
}

#[test]
fn cli_forwards_script_args_to_main() {
    let dir = temp_dir("argv");
    let script = dir.join("main.zzs");
    fs::write(
        &script,
        r#"
        function __main__ ( argv ) {
            say argv.length;
            say argv[0];
            say argv[1];
        }
        "#,
    )
    .expect("script should be written");

    let script_arg = script.to_string_lossy().into_owned();
    let output = run_zuzu(&[&script_arg, "alpha", "beta"], &repo_root());

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "2\nalpha\nbeta\n");
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
}

#[test]
fn cli_language_skeleton_debug_warn_assert() {
    let assert_disabled = run_zuzu(&["-d0", "-e", "assert false; say \"after\";"], &repo_root());
    assert!(assert_disabled.status.success());
    assert_eq!(String::from_utf8_lossy(&assert_disabled.stdout), "after\n");
    assert_eq!(String::from_utf8_lossy(&assert_disabled.stderr), "");

    let assert_enabled = run_zuzu(&["-d1", "-e", "assert false; say \"after\";"], &repo_root());
    assert!(!assert_enabled.status.success());
    assert_eq!(String::from_utf8_lossy(&assert_enabled.stdout), "");
    assert!(String::from_utf8_lossy(&assert_enabled.stderr).contains("Assertion failed"));

    let debug_visible = run_zuzu(
        &["-d3", "-e", "debug 3, \"shown\"; say \"visible\";"],
        &repo_root(),
    );
    assert!(debug_visible.status.success());
    assert_eq!(String::from_utf8_lossy(&debug_visible.stdout), "visible\n");
    assert_eq!(String::from_utf8_lossy(&debug_visible.stderr), "shown\n");

    let warning = run_zuzu(&["-e", "warn \"careful\"; say \"visible\";"], &repo_root());
    assert!(warning.status.success());
    assert_eq!(String::from_utf8_lossy(&warning.stdout), "visible\n");
    assert_eq!(String::from_utf8_lossy(&warning.stderr), "careful\n");

    let debug_suppressed = run_zuzu(
        &[
            "-d2",
            "-e",
            concat!(
                "function explode () { die \"debug should not evaluate\"; } ",
                "debug 3, explode(); ",
                "say \"visible\";"
            ),
        ],
        &repo_root(),
    );
    assert!(debug_suppressed.status.success());
    assert_eq!(
        String::from_utf8_lossy(&debug_suppressed.stdout),
        "visible\n"
    );
    assert_eq!(String::from_utf8_lossy(&debug_suppressed.stderr), "");
}

#[test]
fn cli_uses_include_dirs_for_module_search() {
    let dir = temp_dir("include");
    let lib = dir.join("lib");
    let module_dir = lib.join("acme");
    fs::create_dir_all(&module_dir).expect("module dir should be created");
    fs::write(
        module_dir.join("tool.zzm"),
        r#"
        function greet ( name ) {
            return "hello " _ name;
        }
        "#,
    )
    .expect("module should be written");
    let script = dir.join("main.zzs");
    fs::write(
        &script,
        r#"
        from acme/tool import greet;
        say greet("Ada");
        "#,
    )
    .expect("script should be written");

    let include_arg = format!("-I{}", lib.display());
    let script_arg = script.to_string_lossy().into_owned();
    let output = run_zuzu(&[&include_arg, &script_arg], &repo_root());

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "hello Ada\n");
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
}

#[test]
fn cli_include_dir_does_not_move_repo_relative_paths() {
    let output = run_zuzu(
        &[
            "-It/modules",
            "-e",
            r#"
            from std/io import Path;
            let files := Path.glob("t/fixtures/toon/decode/*.json");
            say files.length() > 0 ? "found" : "missing";
            "#,
        ],
        &repo_root(),
    );

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "found\n");
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
}

#[test]
fn cli_deny_policy_updates_system_flags_and_try_imports() {
    let dir = temp_dir("deny");
    let script = dir.join("main.zzs");
    fs::write(
        &script,
        r#"
        from std/io try import File;
        say File == null;
        say __system__{deny_fs};
        "#,
    )
    .expect("script should be written");

    let script_arg = script.to_string_lossy().into_owned();
    let output = run_zuzu(&["--deny=fs", &script_arg], &repo_root());

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "1\n1\n");
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
}

#[test]
fn cli_deny_fs_makes_tui_path_completions_empty() {
    let output = run_zuzu(
        &[
            "--deny=fs",
            "-e",
            concat!(
                "from std/tui import filename_completions, directory_completions; ",
                "say(filename_completions(\"modules/std/tu\").length()); ",
                "say(directory_completions(\"modules/st\").length());"
            ),
        ],
        &repo_root(),
    );

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "0\n0\n");
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
}

#[test]
fn cli_deny_gui_updates_system_flag_and_rejects_gui_objects() {
    let dir = temp_dir("deny-gui");
    let flag_script = dir.join("flag.zzs");
    fs::write(
        &flag_script,
        r#"
        say __system__{deny_gui};
        "#,
    )
    .expect("flag script should be written");

    let flag_arg = flag_script.to_string_lossy().into_owned();
    let default_output = run_zuzu(&[&flag_arg], &repo_root());
    assert!(default_output.status.success());
    assert_eq!(String::from_utf8_lossy(&default_output.stdout), "0\n");
    assert_eq!(String::from_utf8_lossy(&default_output.stderr), "");

    let flag_output = run_zuzu(&["--deny=gui", &flag_arg], &repo_root());

    assert!(flag_output.status.success());
    assert_eq!(String::from_utf8_lossy(&flag_output.stdout), "1\n");
    assert_eq!(String::from_utf8_lossy(&flag_output.stderr), "");

    let import_script = dir.join("import.zzs");
    fs::write(
        &import_script,
        r#"
        from std/gui/objects import Widget;
        "#,
    )
    .expect("import script should be written");

    let import_arg = import_script.to_string_lossy().into_owned();
    let import_output = run_zuzu(&["--deny=gui", &import_arg], &repo_root());

    assert!(!import_output.status.success());
    assert!(String::from_utf8_lossy(&import_output.stderr)
        .contains("std/gui/objects is denied by runtime policy"));
}

#[test]
fn cli_js_capability_is_always_denied_and_hides_javascript_module() {
    let dir = temp_dir("deny-js");
    let flag_script = dir.join("flag.zzs");
    fs::write(
        &flag_script,
        r#"
        say __system__{deny_js};
        "#,
    )
    .expect("flag script should be written");

    let flag_arg = flag_script.to_string_lossy().into_owned();
    let flag_output = run_zuzu(&[&flag_arg], &repo_root());
    assert!(flag_output.status.success());
    assert_eq!(String::from_utf8_lossy(&flag_output.stdout), "1\n");
    assert_eq!(String::from_utf8_lossy(&flag_output.stderr), "");

    let import_script = dir.join("import.zzs");
    fs::write(
        &import_script,
        r#"
        from javascript import JS;
        "#,
    )
    .expect("import script should be written");

    let import_arg = import_script.to_string_lossy().into_owned();
    let import_output = run_zuzu(&[&import_arg], &repo_root());

    assert!(!import_output.status.success());
    assert!(
        String::from_utf8_lossy(&import_output.stderr).contains("module 'javascript' not found")
    );
}

#[test]
fn cli_deny_gui_allows_dialogue_tui_fallbacks() {
    let output = run_zuzu(
        &[
            "--deny=gui",
            "-e",
            concat!(
                "from std/gui/dialogue import alert, confirm, prompt, file_open; ",
                "alert(\"Hi\"); ",
                "say(confirm(\"Q\", auto_result: true)); ",
                "say(prompt(\"Name:\", auto_result: \"Ada\")); ",
                "say(file_open(auto_result: \"x.txt\")); ",
                "from std/tui import filename_completions; ",
                "say(filename_completions(\"modules/std/tu\").length() > 0);"
            ),
        ],
        &repo_root(),
    );

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "Hi\n1\nAda\nx.txt\n1\n"
    );
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
}

#[test]
fn cli_repl_evaluates_incrementally_and_uses_continuation_prompt() {
    let output = run_zuzu_with_stdin(
        &["-R"],
        &repo_root(),
        concat!(
            "let n1 := 40\n",
            "function get_n1 () {\n",
            "return n1;\n",
            "}\n",
            "get_n1\n",
            "get_n1()\n",
        ),
    );

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("zuzu (^_^)> "));
    assert!(stdout.contains("zuzu (...)> "));
    assert!(stdout.contains("\x1b["));
    assert!(stderr.is_empty());

    let first_value = stdout.find("40").expect("let result should be printed");
    let first_function = stdout[first_value..]
        .find("Function")
        .map(|offset| first_value + offset)
        .expect("function declaration should be rendered");
    let second_function = stdout[first_function + "Function".len()..]
        .find("Function")
        .map(|offset| first_function + "Function".len() + offset)
        .expect("function value should be rendered");
    let final_value = stdout[second_function..]
        .find("40")
        .map(|offset| second_function + offset)
        .expect("function call result should be printed");
    assert!(first_value < first_function);
    assert!(first_function < second_function);
    assert!(second_function < final_value);
}

#[test]
fn cli_repl_uses_emoji_prompts_when_enabled() {
    let output = run_zuzu_with_stdin_and_env(
        &["-R"],
        &repo_root(),
        concat!("function f () {\n", "return 1;\n", "}\n"),
        &[("ZUZU_EMOJI", "1")],
    );

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("zuzu 🦝 💤 > "));
    assert!(stdout.contains("zuzu 🦝 ⏳ > "));
    assert!(!stdout.contains("zuzu (^_^)> "));
    assert!(!stdout.contains("zuzu (...)> "));
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
}

#[test]
fn cli_repl_continues_triple_quoted_and_triple_backtick_literals() {
    let output = run_zuzu_with_stdin(
        &["-R"],
        &repo_root(),
        concat!("\"\"\"\n", "alpha\n", "\"\"\"\n", "```\n", "beta\n", "```\n",),
    );

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("zuzu (...)> "));
    assert!(stdout.contains("alpha"));
    assert!(stdout.contains("beta"));
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");
}
