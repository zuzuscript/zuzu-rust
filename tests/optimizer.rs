use std::path::PathBuf;
use std::process::Command;

use zuzu_rust::{
    parse_program_with_compile_options, OptimizationLevel, OptimizationOptions, OptimizationPass,
    ParseOptions, Runtime,
};

fn compile_json(source: &str, optimizations: OptimizationOptions) -> String {
    let options = ParseOptions::new(false, true, optimizations);
    parse_program_with_compile_options(source, &options)
        .expect("source should compile")
        .to_json_pretty()
}

fn optimization_options(level: OptimizationLevel) -> OptimizationOptions {
    OptimizationOptions::for_level(level)
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn optimization_levels_and_overrides_control_constant_folding() {
    let source = "let a := 2; let b := 3; say a * b;";

    let o0_json = compile_json(source, optimization_options(OptimizationLevel::O0));
    assert!(o0_json.contains("\"type\": \"BinaryExpression\""));
    assert!(o0_json.contains("\"operator\": \"*\""));

    let o3_json = compile_json(source, optimization_options(OptimizationLevel::O3));
    assert!(o3_json.contains("\"type\": \"NumberLiteral\""));
    assert!(o3_json.contains("\"value\": \"6\""));
    assert!(!o3_json.contains("\"type\": \"BinaryExpression\""));

    let mut disabled = optimization_options(OptimizationLevel::O3);
    disabled
        .disable("constant-folding")
        .expect("known pass should disable");
    let disabled_json = compile_json(source, disabled);
    assert!(disabled_json.contains("\"type\": \"BinaryExpression\""));

    let mut enabled = optimization_options(OptimizationLevel::O0);
    enabled
        .enable("constant-folding")
        .expect("known pass should enable");
    let enabled_json = compile_json(source, enabled);
    assert!(enabled_json.contains("\"value\": \"6\""));
}

#[test]
fn unknown_optimization_names_are_errors() {
    let mut options = OptimizationOptions::default();
    let err = options
        .enable("no-such-pass")
        .expect_err("unknown pass should be rejected");

    assert!(err.to_string().contains("unknown optimization pass"));
}

#[test]
fn ast_annotations_include_scope_and_identifier_metadata() {
    let json = compile_json(
        r#"
        let a := get_value();
        {
            say a;
        }
        "#,
        optimization_options(OptimizationLevel::O2),
    );

    assert!(json.contains("\"needs_lexical_scope\": false"));
    assert!(json.contains("\"binding_depth\": 0"));
}

#[test]
fn operator_aliases_are_normalized_to_preferred_spellings() {
    let json = compile_json(
        r#"
        say left == right;
        say left * right;
        say left <= right;
        "#,
        optimization_options(OptimizationLevel::O2),
    );

    assert!(json.contains("\"operator\": \"≡\""));
    assert!(json.contains("\"operator\": \"×\""));
    assert!(json.contains("\"operator\": \"≤\""));
}

#[test]
fn regex_cache_metadata_is_controlled_by_pass() {
    let source = r#"say "abc" ~ /a/i;"#;

    let o0_json = compile_json(source, optimization_options(OptimizationLevel::O0));
    assert!(o0_json.contains("\"regex_cache_key\": null"));

    let o1_json = compile_json(source, optimization_options(OptimizationLevel::O1));
    assert!(o1_json.contains("\"regex_cache_key\": \"1:a:i\""));

    let mut disabled = optimization_options(OptimizationLevel::O1);
    disabled
        .disable("regex-cache")
        .expect("known pass should disable");
    let disabled_json = compile_json(source, disabled);
    assert!(disabled_json.contains("\"regex_cache_key\": null"));
}

#[test]
fn collection_presize_metadata_counts_expanded_ranges() {
    let source = r#"
        say [ 1 ... 5, 10 ].length();
        say << 1 ... 3, 3 >>.length();
        say { alpha: 1, beta: 2 }{alpha};
        say {{ alpha: 1, alpha: 2 }}.length();
    "#;

    let o0_json = compile_json(source, optimization_options(OptimizationLevel::O0));
    assert!(o0_json.contains("\"capacity_hint\": null"));

    let o1_json = compile_json(source, optimization_options(OptimizationLevel::O1));
    assert!(o1_json.contains("\"capacity_hint\": 6"));
    assert!(o1_json.contains("\"capacity_hint\": 4"));
    assert!(o1_json.contains("\"capacity_hint\": 2"));

    let root = repo_root();
    for level in [OptimizationLevel::O0, OptimizationLevel::O1] {
        let runtime =
            Runtime::from_repo_root(&root).with_optimization_options(optimization_options(level));
        let output = runtime
            .run_script_source(source)
            .expect("collection source should execute");

        assert_eq!(output.stdout, "6\n3\n1\n2\n");
        assert_eq!(output.stderr, "");
    }
}

#[test]
fn constant_propagation_preserves_mutating_runtime_semantics() {
    let source = r#"
        let sum := 0;
        for ( let i in [ 1, 2, 3 ] ) {
            sum := sum + i;
        }
        say sum;

        let declared_value := 0;
        function set_declared_value () {
            declared_value := 7;
        }
        set_declared_value();
        say declared_value;

        function run_callback ( callback ) {
            callback();
        }
        let callback_value := null;
        run_callback( function () {
            callback_value := "set";
        } );
        say callback_value;

        let mutable := "Hello World";
        mutable[1:3] := "app";
        say mutable;

        from std/eval import eval;
        let eval_value := 9;
        eval("eval_value += 2;");
        say eval_value;
    "#;
    let root = repo_root();

    for level in [OptimizationLevel::O0, OptimizationLevel::O2] {
        let runtime =
            Runtime::from_repo_root(&root).with_optimization_options(optimization_options(level));
        let output = runtime
            .run_script_source(source)
            .expect("mutating source should execute");

        assert_eq!(output.stdout, "6\n7\nset\nHappo World\n11\n");
        assert_eq!(output.stderr, "");
    }
}

#[test]
fn pruning_removes_dead_branches_and_postfix_conditionals() {
    let mut options = optimization_options(OptimizationLevel::O0);
    options
        .enable("constant-condition-pruning")
        .expect("known pass should enable");
    let json = compile_json(
        r#"
        if ( false ) {
            say "dead";
        }
        else {
            say "live";
        }
        say "also dead" unless true;
        "#,
        options,
    );

    assert!(!json.contains("\"value\": \"dead\""));
    assert!(json.contains("\"value\": \"live\""));
    assert!(!json.contains("\"type\": \"PostfixConditionalStatement\""));
}

#[test]
fn switch_index_metadata_is_only_emitted_at_o3() {
    let source = r#"
        switch ( mode ) {
            case "alpha":
                say 1;
            case "beta":
                say 2;
            default:
                say 3;
        }
    "#;

    let o2_json = compile_json(source, optimization_options(OptimizationLevel::O2));
    assert!(o2_json.contains("\"index\": null"));

    let o3_json = compile_json(source, optimization_options(OptimizationLevel::O3));
    assert!(o3_json.contains("\"key\": \"s:alpha\""));
    assert!(o3_json.contains("\"case_index\": 0"));
    assert!(o3_json.contains("\"key\": \"s:beta\""));
    assert!(o3_json.contains("\"case_index\": 1"));
}

#[test]
fn range_array_loop_lowering_is_o3_only() {
    let source = r#"
        for ( let i in [ 1 ... 3 ] ) {
            say i;
        }
    "#;

    let o2_json = compile_json(source, optimization_options(OptimizationLevel::O2));
    assert!(o2_json.contains("\"type\": \"ForStatement\""));
    assert!(!o2_json.contains("__zuzu_range_"));

    let o3_json = compile_json(source, optimization_options(OptimizationLevel::O3));
    assert!(o3_json.contains("\"type\": \"WhileStatement\""));
    assert!(o3_json.contains("__zuzu_range_"));
    assert!(!o3_json.contains("\"type\": \"ForStatement\""));

    let mut disabled = optimization_options(OptimizationLevel::O3);
    disabled
        .disable("range-array-loop-lowering")
        .expect("known pass should disable");
    let disabled_json = compile_json(source, disabled);
    assert!(disabled_json.contains("\"type\": \"ForStatement\""));
}

#[test]
fn lowered_range_array_loops_match_runtime_semantics() {
    let source = r#"
        let descending := [ ];
        for ( let i in [ 3 ... 1 ] ) {
            descending.push(i);
        }
        say descending[0];
        say descending[1];
        say descending[2];

        let total := 0;
        for ( let i in [ 1 ... 5 ] ) {
            next if i == 2;
            last if i == 4;
            total += i;
        }
        say total;

        let else_marker := 0;
        for ( let i in [ 1 ... 2 ] ) {
            else_marker := i;
            last;
        }
        else {
            else_marker := 99;
        }
        say else_marker;
    "#;
    let root = repo_root();

    for level in [OptimizationLevel::O0, OptimizationLevel::O3] {
        let runtime =
            Runtime::from_repo_root(&root).with_optimization_options(optimization_options(level));
        let output = runtime
            .run_script_source(source)
            .expect("range loop source should execute");

        assert_eq!(output.stdout, "3\n2\n1\n4\n1\n");
        assert_eq!(output.stderr, "");
    }
}

#[test]
fn runtime_output_matches_across_optimization_levels() {
    let source = r#"
        let a := 2;
        let b := 3;
        if ( true ) {
            say a * b;
        }
    "#;
    let root = repo_root();

    for level in [
        OptimizationLevel::O0,
        OptimizationLevel::O1,
        OptimizationLevel::O2,
        OptimizationLevel::O3,
    ] {
        let runtime =
            Runtime::from_repo_root(&root).with_optimization_options(optimization_options(level));
        let output = runtime
            .run_script_source(source)
            .expect("source should execute");

        assert_eq!(output.stdout, "6\n");
        assert_eq!(output.stderr, "");
    }
}

#[test]
fn constant_propagation_keeps_loop_mutations_live() {
    let source = r#"
        let i := 0;
        while ( i < 3 ) {
            i += 1;
        }
        say i;
    "#;
    let root = repo_root();

    let runtime = Runtime::from_repo_root(&root)
        .with_optimization_options(optimization_options(OptimizationLevel::O1));
    let output = runtime
        .run_script_source(source)
        .expect("loop should execute");

    assert_eq!(output.stdout, "3\n");
    assert_eq!(output.stderr, "");
}

#[test]
fn constant_propagation_respects_regex_replacement_scope() {
    let source = r#"
        let mixed := "Foo fOO";
        let m := "outer";
        mixed ~= /foo/i -> ( m[0] _ "!" );
        say mixed;
        say m;
    "#;
    let root = repo_root();

    let runtime = Runtime::from_repo_root(&root)
        .with_optimization_options(optimization_options(OptimizationLevel::O1));
    let output = runtime
        .run_script_source(source)
        .expect("regex replacement should execute");

    assert_eq!(output.stdout, "Foo! fOO\nouter\n");
    assert_eq!(output.stderr, "");
}

#[test]
fn identifier_resolution_falls_back_when_runtime_method_frames_differ() {
    let source = r#"
        class Person {
            let String name;

            method get_self_name () {
                return self{name};
            }
        }

        let person := new Person( name: "Alice" );
        say person.get_self_name();
    "#;
    let root = repo_root();

    let runtime = Runtime::from_repo_root(&root)
        .with_optimization_options(optimization_options(OptimizationLevel::O2));
    let output = runtime
        .run_script_source(source)
        .expect("method self lookup should execute");

    assert_eq!(output.stdout, "Alice\n");
    assert_eq!(output.stderr, "");
}

#[test]
fn optimizer_sensitive_stdlib_round_trips_pass_at_default_level() {
    let root = repo_root();

    for relative in [
        "stdlib/tests/std/path/z.zzs",
        "stdlib/tests/std/data/cbor/_loaddump.zzs",
        "stdlib/tests/std/data/kdl/_loaddump.zzs",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_zuzu-rust"))
            .arg(root.join(relative))
            .current_dir(&root)
            .output()
            .unwrap_or_else(|err| panic!("{relative} should execute: {err}"));
        let stdout = String::from_utf8_lossy(&output.stdout);

        assert!(
            output.status.success(),
            "{relative} should exit successfully: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            stdout.lines().any(|line| line.starts_with("1..")),
            "{relative} should emit a TAP plan"
        );
        assert!(
            !stdout
                .lines()
                .any(|line| line.trim_start().starts_with("not ok")),
            "{relative} should not emit failing TAP"
        );
        assert_eq!(
            String::from_utf8_lossy(&output.stderr),
            "",
            "{relative} should not write stderr"
        );
    }
}

#[test]
fn optimization_level_membership_matches_documented_defaults() {
    let o0 = optimization_options(OptimizationLevel::O0);
    assert!(!o0.enables(OptimizationPass::ConstantFolding));

    let o1 = optimization_options(OptimizationLevel::O1);
    assert!(o1.enables(OptimizationPass::ConstantFolding));
    assert!(o1.enables(OptimizationPass::BlockScopeElision));
    assert!(!o1.enables(OptimizationPass::IdentifierResolution));

    let o2 = optimization_options(OptimizationLevel::O2);
    assert!(o2.enables(OptimizationPass::IdentifierResolution));
    assert!(o2.enables(OptimizationPass::OperatorEnum));
    assert!(!o2.enables(OptimizationPass::SwitchIndexing));

    let o3 = optimization_options(OptimizationLevel::O3);
    assert!(o3.enables(OptimizationPass::SwitchIndexing));
    assert!(o3.enables(OptimizationPass::RangeArrayLoopLowering));
}
