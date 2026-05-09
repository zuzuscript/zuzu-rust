use zuzu_rust::{parse_program, parse_program_with_options, ZuzuRustError};

fn parse_syntax_only(source: &str) -> Result<zuzu_rust::Program, ZuzuRustError> {
    parse_program_with_options(source, false, true)
}

#[test]
fn dumps_expected_ast_for_task1_subset() {
    let source = r#"
        let foo := 1;
        ++foo;

        class Speaker {
            method speak ( String str ) {
                say str;
            }
        }

        Speaker.speak( foo );
    "#;

    let program = parse_program(source).expect("parser should accept task1 sample");
    let json = program.to_json_pretty();

    assert!(json.contains("\"type\": \"Program\""));
    assert!(json.contains("\"type\": \"VariableDeclaration\""));
    assert!(json.contains("\"kind\": \"let\""));
    assert!(json.contains("\"name\": \"foo\""));
    assert!(json.contains("\"type\": \"UnaryExpression\""));
    assert!(json.contains("\"operator\": \"++\""));
    assert!(json.contains("\"type\": \"ClassDeclaration\""));
    assert!(json.contains("\"name\": \"Speaker\""));
    assert!(json.contains("\"type\": \"MethodDeclaration\""));
    assert!(json.contains("\"name\": \"speak\""));
    assert!(json.contains("\"declared_type\": \"String\""));
    assert!(json.contains("\"type\": \"KeywordStatement\""));
    assert!(json.contains("\"keyword\": \"say\""));
    assert!(json.contains("\"type\": \"CallExpression\""));
}

#[test]
fn dumps_exact_ast_shape_for_simple_typed_declaration() {
    let program = parse_program(r#"let String greeting := "Hello";"#)
        .expect("simple typed declaration should parse");
    let json = program.to_json_pretty();

    let expected = r#"{
  "type": "Program",
  "line": 1,
  "statements": [
    {
      "type": "VariableDeclaration",
      "line": 1,
      "kind": "let",
      "declared_type": "String",
      "name": "greeting",
      "is_weak_storage": false,
      "runtime_typecheck_required": false,
      "init": {
        "type": "StringLiteral",
        "line": 1,
        "inferred_type": "String",
        "value": "Hello"
      }
    }
  ]
}"#;

    assert_eq!(json, expected);
}

#[test]
fn dumps_weak_parser_metadata() {
    let source = r#"
        let owner := null;
        let parent := owner but weak;
        parent := owner but weak;
        class Node {
            let parent with get, set but weak;
        }
    "#;

    let program = parse_syntax_only(source).expect("weak metadata source should parse");
    let json = program.to_json_pretty();

    assert!(json.contains("\"name\": \"parent\",\n      \"is_weak_storage\": true"));
    assert!(json.contains("\"is_weak_write\": true"));
    assert!(json.contains("\"type\": \"FieldDeclaration\""));
    assert!(json.contains("\"is_weak_storage\": true"));
}

#[test]
fn rejects_invalid_weak_syntax() {
    for source in [
        "let myarray := [];\nlet x := null;\nmyarray.push(x but weak);",
        "let y := null;\nlet x := (y but weak);",
        "let x := 1;\nlet y := 2;\nx += y but weak;",
        "let x := \"\";\nlet y := \"suffix\";\nx _= y but weak;",
        "let x;\nlet y := 1;\nx ?:= y but weak;",
        "let owner := null;\nlet data := {};\ndata @? \"/parent\" := owner but weak;",
        "let owner but strong;",
    ] {
        let err = parse_syntax_only(source).expect_err("invalid weak syntax should reject");
        let message = err.to_string();
        assert!(
            message.contains("but")
                || message.contains("weak")
                || message.contains("Unexpected")
                || message.contains("Expected"),
            "unexpected error message: {message}"
        );
    }
}

#[test]
fn parses_core_phase_a_statement_forms() {
    let source = r#"
        from std/io import File as Reader if enabled;

        function add ( Number a, Number b ) -> Number {
            return a + b;
        }

        trait Loud {
            method shout ( String msg ) {
                warn msg;
            }
        }

        class Speaker extends Base with Loud {
            let String name with get, set := "z";

            static method build ( String name ) {
                return Speaker( name: name );
            }
        }

        if ( enabled ) {
            debug foo;
        }
        else {
            die "bad";
        }

        while ( count < 10 ) {
            count += 1;
            next if count = 5;
        }

        for ( let item in items ) {
            print item;
        }
        else {
            assert false;
        }

        switch ( mode : eq ) {
            case "a", "b":
                say mode;
            default:
                throw mode;
        }

        try {
            risky();
        }
        catch ( Error err ) {
            warn err;
        }
    "#;

    let program = parse_syntax_only(source).expect("phase A source should parse");
    let json = program.to_json_pretty();

    for expected in [
        "\"type\": \"ImportDeclaration\"",
        "\"type\": \"FunctionDeclaration\"",
        "\"type\": \"TraitDeclaration\"",
        "\"type\": \"ClassDeclaration\"",
        "\"type\": \"IfStatement\"",
        "\"type\": \"WhileStatement\"",
        "\"type\": \"ForStatement\"",
        "\"type\": \"SwitchStatement\"",
        "\"type\": \"TryStatement\"",
        "\"type\": \"ReturnStatement\"",
        "\"type\": \"PostfixConditionalStatement\"",
        "\"type\": \"AssignmentExpression\"",
    ] {
        assert!(json.contains(expected), "missing {expected} in AST dump");
    }
}

#[test]
fn parses_phase_a_expression_forms() {
    let source = r#"
        let arr := [ 1, 2, 3 ];
        let cfg := { foo: 1, "bar": arr[1] };
        let picked := arr[0:2];
        let refx := foo.bar( x: 1, y: 2 )[0]{key};
        let expr := not a and b or c ? d : e ?: f;
        let action := fn ( Number x ) -> x * 2;
        let maybe := try { risky(); } catch ( err ) { err; };
        let other := do { foo(); };
        let made := super( x: 1 );
    "#;

    let program = parse_syntax_only(source).expect("expression source should parse");
    let json = program.to_json_pretty();

    for expected in [
        "\"type\": \"ArrayLiteral\"",
        "\"type\": \"DictLiteral\"",
        "\"type\": \"SliceExpression\"",
        "\"type\": \"CallExpression\"",
        "\"type\": \"DictAccessExpression\"",
        "\"type\": \"TernaryExpression\"",
        "\"type\": \"DefinedOrExpression\"",
        "\"type\": \"LambdaExpression\"",
        "\"type\": \"TryExpression\"",
        "\"type\": \"DoExpression\"",
        "\"type\": \"SuperCallExpression\"",
    ] {
        assert!(json.contains(expected), "missing {expected} in AST dump");
    }
}

#[test]
fn skips_inline_pod_blocks() {
    let source = r#"
=encoding utf8

=head1 NAME

example - parser should skip this pod

=cut

let foo := 1;

=head1 DESCRIPTION

=over

=item Something

=back

=cut

function greet ( String name ) {
    say name;
}
    "#;

    let program = parse_program(source).expect("source with pod should parse");
    let json = program.to_json_pretty();

    assert!(json.contains("\"type\": \"VariableDeclaration\""));
    assert!(json.contains("\"type\": \"FunctionDeclaration\""));
    assert!(json.contains("\"name\": \"foo\""));
    assert!(json.contains("\"name\": \"greet\""));
}

#[test]
fn skips_single_line_comments() {
    let source = r#"
// top comment
let foo := 1; // trailing comment
// another comment
function greet ( String name ) {
    say name; // inline
}
    "#;

    let program = parse_program(source).expect("source with // comments should parse");
    let json = program.to_json_pretty();

    assert!(json.contains("\"type\": \"VariableDeclaration\""));
    assert!(json.contains("\"type\": \"FunctionDeclaration\""));
    assert!(json.contains("\"name\": \"foo\""));
    assert!(json.contains("\"name\": \"greet\""));
}

#[test]
fn skips_leading_shebang_line() {
    let source = r#"#!/usr/bin/env zuzu-rust
let foo := 1;
"#;

    let program = parse_program(source).expect("source with shebang should parse");
    let json = program.to_json_pretty();

    assert!(json.contains("\"type\": \"VariableDeclaration\""));
    assert!(json.contains("\"line\": 2"));
    assert!(json.contains("\"name\": \"foo\""));
}

#[test]
fn skips_multi_line_comments() {
    let source = r#"
/* leading block
   comment */
let foo := 1;

/* comment between declarations */
function greet ( String name ) {
    /* comment inside block */
    say name;
}
    "#;

    let program = parse_program(source).expect("source with /* */ comments should parse");
    let json = program.to_json_pretty();

    assert!(json.contains("\"type\": \"VariableDeclaration\""));
    assert!(json.contains("\"type\": \"FunctionDeclaration\""));
    assert!(json.contains("\"name\": \"foo\""));
    assert!(json.contains("\"name\": \"greet\""));
}

#[test]
fn includes_line_numbers_on_ast_nodes() {
    let source = "let foo := 1;\n\nfunction greet ( String name ) {\n    say name;\n}\n";

    let program = parse_program(source).expect("source should parse");
    let json = program.to_json_pretty();

    assert!(json.contains("\"line\": 1"));
    assert!(json.contains("\"type\": \"VariableDeclaration\""));
    assert!(json.contains("\"line\": 3"));
    assert!(json.contains("\"type\": \"FunctionDeclaration\""));
    assert!(json.contains("\"line\": 4"));
    assert!(json.contains("\"type\": \"KeywordStatement\""));
}

#[test]
fn parses_phase_b_unicode_operator_forms() {
    let source = r#"
        let math := 2 × 3 + 8 ÷ 4;
        let compare := a ≤ b and c ≥ d or e ≠ f;
        let strict := a ≡ b xor c ≢ d;
        let sets := left ⋃ right ⋂ other ∖ missing;
        let rel := item ∈ items and item ∉ blocked;
        let types := obj instanceof Thing and obj does Role and obj can run;
        let order := a ≶ b or a ≷ b;
        let pathy := data @ "/foo" and data @@ "/bar" or data @? "/baz";
        let unary := ¬flag or √value > 2 or \target;
        let bools := ⊤ ⋀ false ⋁ ⊥ ⊻ maybe ⊼ other;
        let nested := a ⊂ b and c ⊃ d and e ⊂⊃ f;
    "#;

    let program = parse_syntax_only(source).expect("unicode operator source should parse");
    let json = program.to_json_pretty();

    for expected in [
        "\"operator\": \"×\"",
        "\"operator\": \"÷\"",
        "\"operator\": \"≤\"",
        "\"operator\": \"≥\"",
        "\"operator\": \"≠\"",
        "\"operator\": \"≡\"",
        "\"operator\": \"≢\"",
        "\"operator\": \"⋃\"",
        "\"operator\": \"⋂\"",
        "\"operator\": \"∖\"",
        "\"operator\": \"∈\"",
        "\"operator\": \"∉\"",
        "\"operator\": \"≶\"",
        "\"operator\": \"≷\"",
        "\"operator\": \"⋀\"",
        "\"operator\": \"⋁\"",
        "\"operator\": \"⊻\"",
        "\"operator\": \"⊼\"",
        "\"operator\": \"⊂\"",
        "\"operator\": \"⊃\"",
        "\"operator\": \"⊂⊃\"",
        "\"operator\": \"@\"",
        "\"operator\": \"@@\"",
        "\"operator\": \"@?\"",
        "\"operator\": \"¬\"",
        "\"operator\": \"√\"",
        "\"operator\": \"\\\\\"",
    ] {
        assert!(json.contains(expected), "missing {expected} in AST dump");
    }
}

#[test]
fn parses_template_literals_with_interpolation() {
    let source = r#"
        let who := "world";
        let msg := `hello ${ who }, ${ 1 + 2 }`;
    "#;

    let program = parse_program(source).expect("template literal source should parse");
    let json = program.to_json_pretty();

    for expected in [
        "\"type\": \"TemplateLiteral\"",
        "\"type\": \"TemplateText\"",
        "\"line\": 3",
        "\"value\": \"hello \"",
        "\"value\": \", \"",
        "\"type\": \"TemplateExpression\"",
        "\"name\": \"who\"",
        "\"operator\": \"+\"",
    ] {
        assert!(json.contains(expected), "missing {expected} in AST dump");
    }
}

#[test]
fn parses_triple_backtick_template_literals() {
    let source = "let msg := ```alpha\nbeta\n```;\n";

    let program = parse_program(source).expect("triple-backtick template should parse");
    let json = program.to_json_pretty();

    assert!(json.contains("\"type\": \"TemplateLiteral\""));
    assert!(json.contains("\"type\": \"TemplateText\""));
    assert!(json.contains("\"value\": \"alpha\\nbeta\\n\""));
}

#[test]
fn parses_regex_collection_and_function_expression_forms() {
    let source = r#"
        let re := /ab([0-9]+)/gi;
        let setv := << 1, 2 >>;
        let setu := « 3, 4 »;
        let bag := <<< 1, 2, 2 >>>;
        let pairs := {{ foo: 1, bar: 2 }};
        let anon := function( Number x, String label?, ... PairList rest ) {
            return x;
        };
    "#;

    let program = parse_program(source).expect("extended syntax source should parse");
    let json = program.to_json_pretty();

    for expected in [
        "\"type\": \"RegexLiteral\"",
        "\"pattern\": \"ab([0-9]+)\"",
        "\"flags\": \"gi\"",
        "\"type\": \"SetLiteral\"",
        "\"type\": \"BagLiteral\"",
        "\"type\": \"PairListLiteral\"",
        "\"type\": \"FunctionExpression\"",
        "\"optional\": true",
        "\"variadic\": true",
    ] {
        assert!(json.contains(expected), "missing {expected} in AST dump");
    }
}

#[test]
fn includes_inferred_types_and_runtime_typecheck_hints() {
    let source = r#"
        let String greeting := "Hello world";

        function get_string () -> String {
            return "Hello world";
        }

        let String mystr := get_string();
        let Number count := 1;
        count += 2;
        let alpha := "a";
        let beta := "b";
        let Number cmp_order := alpha cmp beta;
        let Number cmpi_order := alpha cmpi beta;
        let Number spaceship := count <=> 2;
        let Number lower := count ≶ 2;
        let Number upper := count ≷ 2;
    "#;

    let program = parse_program(source).expect("inference sample should parse");
    let json = program.to_json_pretty();

    for expected in [
        "\"inferred_type\": \"String\"",
        "\"inferred_type\": \"Number\"",
        "\"runtime_typecheck_required\": false",
        "\"name\": \"greeting\"",
        "\"name\": \"mystr\"",
        "\"name\": \"cmp_order\"",
        "\"name\": \"cmpi_order\"",
        "\"name\": \"spaceship\"",
        "\"name\": \"lower\"",
        "\"name\": \"upper\"",
        "\"type\": \"ReturnStatement\"",
        "\"operator\": \"+=\"",
        "\"operator\": \"cmp\"",
        "\"operator\": \"cmpi\"",
        "\"operator\": \"<=>\"",
        "\"operator\": \"≶\"",
        "\"operator\": \"≷\"",
    ] {
        assert!(json.contains(expected), "missing {expected} in AST dump");
    }
}

#[test]
fn can_skip_type_inference() {
    let source = r#"
        let String greeting := "Hello world";
        let Number count := 1;
        count += 2;
    "#;

    let program = parse_program_with_options(source, true, false)
        .expect("source should parse without inference");
    let json = program.to_json_pretty();

    assert!(
        !json.contains("\"inferred_type\": \""),
        "AST dump should not include concrete inferred types when inference is disabled",
    );
    assert!(
        !json.contains("\"runtime_typecheck_required\": true")
            && !json.contains("\"runtime_typecheck_required\": false"),
        "AST dump should not include concrete runtime typecheck hints when inference is disabled",
    );
}

#[test]
fn dumps_exact_ast_shape_without_inference() {
    let program = parse_program_with_options(r#"let String greeting := "Hello";"#, true, false)
        .expect("simple typed declaration should parse without inference");
    let json = program.to_json_pretty();

    let expected = r#"{
  "type": "Program",
  "line": 1,
  "statements": [
    {
      "type": "VariableDeclaration",
      "line": 1,
      "kind": "let",
      "declared_type": "String",
      "name": "greeting",
      "is_weak_storage": false,
      "runtime_typecheck_required": null,
      "init": {
        "type": "StringLiteral",
        "line": 1,
        "inferred_type": null,
        "value": "Hello"
      }
    }
  ]
}"#;

    assert_eq!(json, expected);
}

#[test]
fn diagnostics_are_stable_for_representative_failures() {
    let parse_err = parse_program("let foo := ;").expect_err("missing expression should fail");
    assert_eq!(
        parse_err.to_string(),
        "parse error at 1:12: Expected expression",
    );

    let name_err = parse_program("function f ( { return 1; }")
        .expect_err("missing parameter name should fail");
    assert_eq!(name_err.to_string(), "parse error at 1:14: Expected name");

    let redeclare_err = parse_program("let foo := 1; let foo := 2;")
        .expect_err("same-scope redeclaration should fail");
    assert_eq!(
        redeclare_err.to_string(),
        "semantic error at 1:1: Redeclaration of 'foo' in the same scope",
    );
}

#[test]
fn incomplete_input_returns_distinct_error_type() {
    let block_err = parse_program("if ( true ) {")
        .expect_err("unterminated block should fail as incomplete input");
    match block_err {
        ZuzuRustError::IncompleteParse { .. } => {}
        other => panic!("expected IncompleteParse, got {other}"),
    }

    let params_err = parse_program("function f (")
        .expect_err("unterminated parameter list should fail as incomplete input");
    match params_err {
        ZuzuRustError::IncompleteParse { .. } => {}
        other => panic!("expected IncompleteParse, got {other}"),
    }
}

#[test]
fn can_skip_semantic_validation() {
    let program = parse_program_with_options("return 1;", false, false)
        .expect("source should parse without semantic validation");
    let json = program.to_json_pretty();

    assert!(json.contains("\"type\": \"ReturnStatement\""));
}

#[test]
fn rejects_return_outside_function() {
    let err = parse_program("return 1;").expect_err("return outside function should fail");
    let text = err.to_string();
    assert!(text.contains("semantic error"));
    assert!(text.contains("return is not valid outside function scope"));
}

#[test]
fn rejects_loop_control_outside_loop() {
    let err = parse_program("next;").expect_err("next outside loop should fail");
    let text = err.to_string();
    assert!(text.contains("semantic error"));
    assert!(text.contains("next is not valid outside loop scope"));
}

#[test]
fn rejects_invalid_assignment_target() {
    let err = parse_program("( a + b ) := 1;").expect_err("invalid assignment target should fail");
    let text = err.to_string();
    assert!(text.contains("semantic error"));
    assert!(text.contains("invalid assignment target"));
}

#[test]
fn rejects_assignment_to_const_binding() {
    let err = parse_program("const foo := 1; foo := 2;")
        .expect_err("assignment to const binding should fail");
    let text = err.to_string();
    assert!(text.contains("semantic error"));
    assert!(text.contains("cannot modify const binding 'foo'"));

    for (source, name) in [
        ("const foo := 1; foo += 2;", "foo"),
        (r#"const bar := "x"; bar _= "y";"#, "bar"),
        (r#"const baz := "x"; baz ~= /x/;"#, "baz"),
    ] {
        let err =
            parse_program(source).expect_err("compound assignment to const binding should fail");
        let text = err.to_string();
        assert!(text.contains("semantic error"));
        assert!(text.contains(&format!("cannot modify const binding '{name}'")));
    }
}

#[test]
fn rejects_invalid_update_target() {
    let err = parse_program("++( a + b );").expect_err("invalid update target should fail");
    let text = err.to_string();
    assert!(text.contains("semantic error"));
    assert!(text.contains("invalid target for unary operator '++'"));
}

#[test]
fn rejects_update_of_const_binding() {
    let err = parse_program("const foo := 1; ++foo;")
        .expect_err("prefix update of const binding should fail");
    let text = err.to_string();
    assert!(text.contains("semantic error"));
    assert!(text.contains("cannot modify const binding 'foo'"));

    let err = parse_program("const bar := 1; bar++;")
        .expect_err("postfix update of const binding should fail");
    let text = err.to_string();
    assert!(text.contains("semantic error"));
    assert!(text.contains("cannot modify const binding 'bar'"));
}

#[test]
fn rejects_same_scope_redeclaration() {
    let err = parse_program("let foo := 1; let foo := 2;")
        .expect_err("same-scope redeclaration should fail");
    let text = err.to_string();
    assert!(text.contains("semantic error"));
    assert!(text.contains("Redeclaration of 'foo' in the same scope"));
}

#[test]
fn rejects_use_of_out_of_scope_identifier() {
    let err = parse_program(
        r#"
        function f () {
            {
                let y := 1;
            }
            let z := 2;
            return y;
        }
        "#,
    )
    .expect_err("out-of-scope identifier should fail during sema");
    let text = err.to_string();
    assert!(text.contains("semantic error"));
    assert!(text.contains("Use of undeclared identifier 'y' (compile-time)"));
}
