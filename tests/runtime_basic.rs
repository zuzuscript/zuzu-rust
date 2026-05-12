use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use zuzu_rust::{parse_program, parse_program_with_options, HostValue, Runtime, RuntimePolicy};

fn spawn_delayed_http_server(
    delay: Duration,
    expected_requests: usize,
) -> Option<(String, thread::JoinHandle<()>)> {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
        Err(err) => panic!("test HTTP server should bind: {err}"),
    };
    let url = format!("http://{}", listener.local_addr().expect("local addr"));
    let handle = thread::spawn(move || {
        let mut handlers = Vec::new();
        for stream in listener.incoming().take(expected_requests) {
            let delay = delay;
            let mut stream = stream.expect("test HTTP connection should accept");
            handlers.push(thread::spawn(move || {
                let mut buffer = [0_u8; 1024];
                let _ = stream.read(&mut buffer);
                thread::sleep(delay);
                let response = concat!(
                    "HTTP/1.1 200 OK\r\n",
                    "Content-Type: text/plain\r\n",
                    "Content-Length: 2\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                    "ok"
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("test HTTP response should write");
            }));
        }
        for handler in handlers {
            handler.join().expect("test HTTP handler should finish");
        }
    });
    Some((url, handle))
}

fn host_dict(items: Vec<(&str, HostValue)>) -> HostValue {
    HostValue::Dict(
        items
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect::<HashMap<_, _>>(),
    )
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn file_global_tracks_source_files_and_fs_denial() {
    let runtime = Runtime::new(Vec::new());
    let output = runtime
        .run_script_source_with_args_and_source_file(
            r#"say(__file__.to_String());"#,
            &[],
            Some("relative-main.zzs"),
        )
        .expect("__file__ should be available for a main file");
    assert_eq!(output.stdout, "relative-main.zzs\n");

    let denied = Runtime::with_policy(Vec::new(), RuntimePolicy::new().deny_capability("fs"));
    let output = denied
        .run_script_source_with_args_and_source_file(
            r#"say(typeof __file__);"#,
            &[],
            Some("relative-main.zzs"),
        )
        .expect("__file__ should be null when fs is denied");
    assert_eq!(output.stdout, "Null\n");

    let temp_root =
        std::env::temp_dir().join(format!("zuzu-rust-file-global-{}", std::process::id()));
    let module_root = temp_root.join("modules");
    fs::create_dir_all(&module_root).expect("module dir should be created");
    let module_path = module_root.join("file_probe.zzm");
    fs::write(
        &module_path,
        r#"const module_file := __file__.to_String();"#,
    )
    .expect("module should be written");

    let runtime = Runtime::new(vec![module_root]);
    let output = runtime
        .run_script_source_with_args_and_source_file(
            r#"
            from file_probe import module_file;
            say(module_file);
            "#,
            &[],
            Some("main.zzs"),
        )
        .expect("module __file__ should be available");
    assert_eq!(output.stdout, format!("{}\n", module_path.display()));

    let _ = fs::remove_dir_all(temp_root);
}

#[test]
fn loads_program_without_main_and_exposes_request_function() {
    let runtime = Runtime::new(Vec::new());
    let program = parse_program(
        r#"
        function __request__ ( env ) {
            return [ 200, {{}}, [] ];
        }
        "#,
    )
    .expect("program should parse");

    let loaded = runtime
        .load_program_without_main(&program, Some("app.zzs"))
        .expect("program should load");

    assert!(loaded.has_function("__request__"));
    assert!(!loaded.has_function("__main__"));
}

#[test]
fn load_program_without_main_does_not_call_main() {
    let runtime = Runtime::new(Vec::new());
    let program = parse_program(
        r#"
        function __main__ ( argv ) {
            say "called";
        }

        function __request__ ( env ) {
            return [ 200, {{}}, [] ];
        }
        "#,
    )
    .expect("program should parse");

    let loaded = runtime
        .load_program_without_main(&program, Some("app.zzs"))
        .expect("program should load");

    assert!(loaded.has_function("__request__"));
    assert!(loaded.has_function("__main__"));
    assert_eq!(runtime.last_output().stdout, "");
}

#[test]
fn load_program_without_main_allows_top_level_imports() {
    let repo_root = repo_root();
    let runtime = Runtime::from_repo_root(&repo_root);
    let program = parse_program(
        r#"
        from std/io import Path;

        let asset := new Path("README.md");

        function __request__ ( env ) {
            return [ 200, {{}}, [ asset.to_String() ] ];
        }
        "#,
    )
    .expect("program should parse");

    let loaded = runtime
        .load_program_without_main(&program, Some("app.zzs"))
        .expect("program with import should load");

    assert!(loaded.has_function("__request__"));
}

#[test]
fn load_program_without_main_rejects_top_level_return() {
    let runtime = Runtime::new(Vec::new());
    let program =
        parse_program_with_options("return 1;", false, true).expect("program should parse");

    let err = match runtime.load_program_without_main(&program, Some("app.zzs")) {
        Ok(_) => panic!("top-level return should fail"),
        Err(err) => err,
    };

    assert!(err
        .to_string()
        .contains("return is not valid at top-level scope"));
}

#[test]
fn host_value_call_round_trips_supported_boundary_values() {
    let runtime = Runtime::new(Vec::new());
    let program = parse_program(
        r#"
        from std/io import Path;

        function __request__ ( env ) {
            return [
                null,
                true,
                42,
                "hello",
                to_binary("raw"),
                [ env{method}, env{body} ],
                { path: env{path} },
                env{headers},
                new Path("asset.txt"),
            ];
        }
        "#,
    )
    .expect("program should parse");
    let loaded = runtime
        .load_program_without_main(&program, Some("app.zzs"))
        .expect("program should load");
    let env = host_dict(vec![
        ("method", HostValue::String("POST".to_owned())),
        ("path", HostValue::String("/upload".to_owned())),
        ("body", HostValue::Binary(vec![1, 2, 3])),
        (
            "headers",
            HostValue::PairList(vec![
                ("Set-Cookie".to_owned(), HostValue::String("a=1".to_owned())),
                ("Set-Cookie".to_owned(), HostValue::String("b=2".to_owned())),
            ]),
        ),
    ]);

    let result = loaded
        .call_request(env)
        .expect("__request__ should return host values");

    let mut expected_dict = HashMap::new();
    expected_dict.insert("path".to_owned(), HostValue::String("/upload".to_owned()));
    assert_eq!(
        result,
        HostValue::Array(vec![
            HostValue::Null,
            HostValue::Bool(true),
            HostValue::Number(42.0),
            HostValue::String("hello".to_owned()),
            HostValue::Binary(b"raw".to_vec()),
            HostValue::Array(vec![
                HostValue::String("POST".to_owned()),
                HostValue::Binary(vec![1, 2, 3]),
            ]),
            HostValue::Dict(expected_dict),
            HostValue::PairList(vec![
                ("Set-Cookie".to_owned(), HostValue::String("a=1".to_owned())),
                ("Set-Cookie".to_owned(), HostValue::String("b=2".to_owned())),
            ]),
            HostValue::Path(PathBuf::from("asset.txt")),
        ]),
    );
}

#[test]
fn host_value_call_awaits_async_request_function() {
    let runtime = Runtime::new(Vec::new());
    let program = parse_program(
        r#"
        from std/task import sleep;

        async function __request__ ( env ) {
            await { sleep(0.001); };
            return [ env{method}, env{path} ];
        }
        "#,
    )
    .expect("program should parse");
    let loaded = runtime
        .load_program_without_main(&program, Some("app.zzs"))
        .expect("program should load");
    let env = host_dict(vec![
        ("method", HostValue::String("GET".to_owned())),
        ("path", HostValue::String("/async".to_owned())),
    ]);

    let result = loaded
        .call_request(env)
        .expect("async __request__ should be awaited");

    assert_eq!(
        result,
        HostValue::Array(vec![
            HostValue::String("GET".to_owned()),
            HostValue::String("/async".to_owned()),
        ]),
    );
}

#[test]
fn host_value_call_reports_missing_callable() {
    let runtime = Runtime::new(Vec::new());
    let program = parse_program("function __request__ ( env ) { return null; }")
        .expect("program should parse");
    let loaded = runtime
        .load_program_without_main(&program, Some("app.zzs"))
        .expect("program should load");

    let err = loaded
        .call("missing", Vec::new())
        .expect_err("missing callable should fail");

    assert!(err
        .to_string()
        .contains("use of undeclared identifier 'missing'"));
}

#[test]
fn host_value_call_rejects_unsupported_return_value() {
    let runtime = Runtime::new(Vec::new());
    let program = parse_program(
        r#"
        function helper () {
            return 1;
        }

        function __request__ ( env ) {
            return helper;
        }
        "#,
    )
    .expect("program should parse");
    let loaded = runtime
        .load_program_without_main(&program, Some("app.zzs"))
        .expect("program should load");

    let err = loaded
        .call_request(HostValue::Null)
        .expect_err("function return value should not convert");

    assert!(err
        .to_string()
        .contains("cannot convert Function to HostValue"));
}

#[test]
fn runs_basic_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/basic.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("basic.zzs should execute successfully");

    assert_eq!(
        output.stdout,
        concat!(
            "ok 1\n",
            "ok 2 - Math is Math\n",
            "ok 3 - returning an internal function works\n",
            "    # Subtest: truthiness\n",
            "    ok 1 - yes\n",
            "    ok 2 - yep\n",
            "    ok 3 - yeh\n",
            "    ok 4 - yee\n",
            "    1..4\n",
            "ok 4 - truthiness\n",
            "ok 5 - after subtest\n",
            "1..5\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_collection_operator_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/operators/collection-operators.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("collection-operators.zzs should execute successfully");

    assert_eq!(
        output.stdout,
        concat!(
            "ok 1 - in works for arrays\n",
            "ok 2 - ∈ works for sets\n",
            "ok 3 - ∉ works\n",
            "ok 4 - ascending range builds arrays\n",
            "ok 5 - range includes both endpoints\n",
            "ok 6 - descending range is supported\n",
            "ok 7 - range works in sets\n",
            "ok 8 - range works in bags\n",
            "ok 9 - union works\n",
            "ok 10 - ⋃ works\n",
            "ok 11 - intersection works\n",
            "ok 12 - ⋂ works\n",
            "ok 13 - difference operator works\n",
            "ok 14 - difference operator removes RHS values\n",
            "ok 15 - ∖ difference works\n",
            "ok 16 - subsetof works\n",
            "ok 17 - ⊂ works\n",
            "ok 18 - supersetof works\n",
            "ok 19 - ⊃ works\n",
            "ok 20 - equivalentof works\n",
            "ok 21 - ⊂⊃ works\n",
            "1..21\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn system_inc_exposes_module_roots_as_array() {
    let cwd = std::env::current_dir().unwrap();
    let root_a = cwd.join("alpha/modules");
    let root_b = cwd.join("beta/modules");
    let expected = format!("[{}, {}]", root_a.display(), root_b.display());
    let runtime = Runtime::new(vec![root_a, root_b]);

    let output = runtime
        .run_script_source(
            r#"
            say __system__{inc};
            "#,
        )
        .expect("__system__.inc should be readable");

    assert_eq!(output.stdout, format!("{expected}\n"));
    assert_eq!(output.stderr, "");
}

#[test]
fn system_inc_rejects_mutation() {
    let runtime = Runtime::new(vec![PathBuf::from("/opt/zuzu/modules")]);

    let err = runtime
        .run_script_source(
            r#"
            __system__{inc}.append("/tmp/other");
            "#,
        )
        .err()
        .expect("__system__.inc should reject mutation");

    assert!(err.to_string().contains("Cannot modify __system__"));
}

#[test]
fn system_global_rejects_dict_mutation() {
    let runtime = Runtime::new(vec![PathBuf::from("/opt/zuzu/modules")]);

    let err = runtime
        .run_script_source(
            r#"
            __system__.set("runtime", "other");
            "#,
        )
        .err()
        .expect("__system__ should reject mutation");

    assert!(err.to_string().contains("Cannot modify __system__"));
}

#[test]
fn outer_execution_continues_after_inner_block_scope_ends() {
    let repo_root = repo_root();
    let runtime = Runtime::from_repo_root(&repo_root);

    let output = runtime
        .run_script_source(
            r#"
            function f () {
                let x := 0;
                {
                    let y := 1;
                    x := y;
                }
                let z := x;
                say z;
            }

            f();
            "#,
        )
        .expect("inner-block scope teardown should not affect later outer bindings");

    assert_eq!(output.stdout, "1\n");
    assert_eq!(output.stderr, "");
}

#[test]
fn zero_arg_dot_syntax_invokes_method_instead_of_reading_property() {
    let repo_root = repo_root();
    let runtime = Runtime::from_repo_root(&repo_root);

    let output = runtime
        .run_script_source(
            r#"
            say [ 1, 2, 3 ].length;
            say [].empty;
            say [ 1 ].empty;
            "#,
        )
        .expect("zero-arg member syntax should call the method");

    assert_eq!(output.stdout, "3\n1\n0\n");
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_string_more_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/operators/string-more.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("string-more.zzs should execute successfully");

    assert_eq!(
        output.stdout,
        concat!(
            "ok 1 - _ concatenates strings\n",
            "ok 2 - _ casts non-string lhs\n",
            "ok 3 - _ casts non-string rhs\n",
            "ok 4 - _ casts null to empty string\n",
            "ok 5 - eq\n",
            "ok 6 - ne\n",
            "ok 7 - gt\n",
            "ok 8 - ge\n",
            "ok 9 - lt\n",
            "ok 10 - le\n",
            "ok 11 - cmp\n",
            "ok 12 - eqi\n",
            "ok 13 - nei\n",
            "ok 14 - gti\n",
            "ok 15 - gei\n",
            "ok 16 - lti\n",
            "ok 17 - lei\n",
            "ok 18 - cmpi\n",
            "ok 19 - eq casts number to string\n",
            "ok 20 - gt compares stringified values\n",
            "ok 21 - true stringifies to true\n",
            "ok 22 - false stringifies to false\n",
            "1..22\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_function_signatures_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/functions/signatures.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("signatures.zzs should execute successfully");

    assert_eq!(
        output.stdout,
        concat!(
            "ok 1 - fixed-arity functions evaluate\n",
            "ok 2 - function calls allow loose and trailing commas\n",
            "ok 3 - optional/default/varargs can be combined\n",
            "ok 4 - optional/default/varargs bind in order\n",
            "ok 5 - typed parameters accept matching args\n",
            "ok 6 - __argc__ tracks omitted optional args\n",
            "ok 7 - __argc__ tracks explicit null args\n",
            "ok 8 - fixed-arity functions reject missing args\n",
            "1..8\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_lambda_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/functions/lambdas.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("lambdas.zzs should execute successfully");

    assert_eq!(
        output.stdout,
        concat!(
            "ok 1 - fn lambdas close over lexical scope\n",
            "ok 2 - typed fn shorthand lambdas execute\n",
            "ok 3 - typed fn shorthand supports parenthesized multi-params\n",
            "ok 4 - typed fn shorthand parenthesized multi-params branch\n",
            "ok 5 - anonymous function expressions are callable\n",
            "ok 6 - typed fn shorthand enforces argument type\n",
            "1..6\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn std_path_z_query_helpers_keep_full_nodesets() {
    let repo_root = repo_root();
    let runtime = Runtime::from_repo_root(&repo_root);

    let output = runtime
        .run_script_source(
            r#"
            from std/path/z import ZPath;
            from std/string import join;

            function query ( data, path ) {
                return new ZPath( path: path ).query(data);
            }

            let data := {
                users: [
                    { name: "Ada", age: 32 },
                    { name: "Bob", age: 27 },
                    { name: "Cara", age: 40 },
                ],
            };

            say query( data, "/users/*/name" ).length();
            say query( data, "/users/*/name" )[2];
            say new ZPath( path: "/users/*/name" ).evaluate( data, { level: 9 } ).length();
            say join( ", ", query( data, "/users/*/name" ) );
            "#,
        )
        .expect("std/path/z query helpers should preserve full nodesets");

    assert_eq!(output.stdout, "3\nCara\n3\nAda, Bob, Cara\n");
    assert_eq!(output.stderr, "");
}

#[test]
fn std_string_chr_ord_roundtrip_unicode_scalars() {
    let repo_root = repo_root();
    let runtime = Runtime::from_repo_root(&repo_root);

    let output = runtime
        .run_script_source(
            r#"
            from std/string import chr, ord;
            say chr(9731);
            say ord("a☃😀", 1);
            say chr(ord("a☃😀", 2));
            "#,
        )
        .expect("std/string chr and ord should run");

    assert_eq!(output.stdout, "☃\n9731\n😀\n");
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_switch_ztest_scripts() {
    let repo_root = repo_root();
    let switch_output = Runtime::from_repo_root(&repo_root)
        .run_script_file(&repo_root.join("languagetests/lang/control/switch.zzs"))
        .expect("switch.zzs should execute successfully");
    assert_eq!(
        switch_output.stdout,
        concat!(
            "ok 1 - switch :eq comparator and continue work\n",
            "ok 2 - switch :~ comparator supports regexp cases\n",
            "ok 3 - switch supports numeric case matching\n",
            "1..3\n",
        ),
    );

    let more_output = Runtime::from_repo_root(&repo_root)
        .run_script_file(&repo_root.join("languagetests/lang/control/switch-more.zzs"))
        .expect("switch-more.zzs should execute successfully");
    assert_eq!(
        more_output.stdout,
        concat!(
            "ok 1 - switch case with continue fallthrough\n",
            "ok 2 - switch does not fall through without continue\n",
            "ok 3 - switch accepts explicit blocks in case bodies\n",
            "ok 4 - switch supports non-equality comparator operators\n",
            "1..4\n",
        ),
    );
}

#[test]
fn runs_loops_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/control/loops.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("loops.zzs should execute successfully");

    assert_eq!(
        output.stdout,
        concat!(
            "ok 1 - for-else runs else for empty arrays\n",
            "ok 2 - for loops iterate non-empty sets without else\n",
            "ok 3 - for-else runs else for empty bags\n",
            "ok 4 - for loops iterate dict enumerations\n",
            "ok 5 - for loop const variables iterate correctly\n",
            "ok 6 - for loops can iterate function iterators\n",
            "ok 7 - for loop still iterates called function result as array\n",
            "ok 8 - for loop uses object to_Array conversion\n",
            "ok 9 - for loop uses object to_Iterator conversion\n",
            "ok 10 - for loop prefers to_Iterator over to_Array\n",
            "ok 11 - for-else runs when iterator is immediately exhausted\n",
            "1..11\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_phase_a_concurrency_contract_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/concurrency/phase-a.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("phase-a.zzs should execute successfully");

    assert!(output
        .stdout
        .contains("ok 1 - async function call returns Task\n"));
    assert!(output.stdout.contains(" - race cancels losing task\n"));
    assert!(output.stdout.contains(" - timeout cancels wrapped task\n"));
    assert!(output
        .stdout
        .contains(" - scheduled all observes async functions before direct await\n"));
    assert!(output
        .stdout
        .contains(" - scheduled race cancels loser before direct await\n"));
    assert!(output
        .stdout
        .contains(" - spawned task failure is observed when awaited\n"));
    assert!(output.stdout.contains("1..54\n"));
    assert_eq!(output.stderr, "");
}

#[test]
fn timeout_task_expires_while_executor_runs_unrelated_sleep() {
    let repo_root = repo_root();

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_source(
            r#"
            from test/more import *;
            from std/task import sleep, timeout, Channel;

            async function __main__ ( argv ) {
                let timeout_target := new Channel().recv();
                let timed := timeout( 0.01, timeout_target );
                await {
                    sleep(0.05);
                };
                is(
                    timeout_target.status(),
                    "cancelled",
                    "scheduled timeout cancels wrapped task before direct await",
                );
                ok( timed.done(), "scheduled timeout finishes before direct await" );
                let thrown := false;
                try {
                    await {
                        timed;
                    };
                }
                catch ( TimeoutException e ) {
                    thrown := true;
                }
                ok( thrown, "scheduled timeout throws on await" );
                done_testing();
            }
            "#,
        )
        .expect("scheduled timeout should execute successfully");

    assert!(output
        .stdout
        .contains("ok 1 - scheduled timeout cancels wrapped task before direct await\n"));
    assert!(output
        .stdout
        .contains("ok 2 - scheduled timeout finishes before direct await\n"));
    assert!(output
        .stdout
        .contains("ok 3 - scheduled timeout throws on await\n"));
    assert!(output.stdout.contains("1..3\n"));
    assert_eq!(output.stderr, "");
}

#[test]
fn await_rejects_non_task_block_result() {
    let repo_root = repo_root();
    let runtime = Runtime::from_repo_root(&repo_root);

    let err = match runtime.run_script_source(
        r#"
        async function main () {
            await {
                123;
            };
        }

        await {
            main();
        };
        "#,
    ) {
        Ok(_) => panic!("await with non-task block result should fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("await block must return a Task, got Number"),
        "unexpected error: {err}"
    );
}

#[test]
fn sleep_tasks_overlap_under_task_all() {
    let repo_root = repo_root();

    let single_runtime = Runtime::from_repo_root(&repo_root);
    let single_start = Instant::now();
    let single_output = single_runtime
        .run_script_source(
            r#"
            from std/task import sleep;

            async function __main__ ( argv ) {
                await {
                    sleep(0.12);
                };
                say "single";
            }
            "#,
        )
        .expect("single sleep should run");
    let single_elapsed = single_start.elapsed();
    assert_eq!(single_output.stdout, "single\n");

    let pair_runtime = Runtime::from_repo_root(&repo_root);
    let pair_start = Instant::now();
    let pair_output = pair_runtime
        .run_script_source(
            r#"
            from std/proc import sleep_async;
            from std/task import all, sleep;

            async function __main__ ( argv ) {
                await {
                    all( [ sleep(0.12), sleep_async(0.12) ] );
                };
                say "pair";
            }
            "#,
        )
        .expect("paired sleeps should run");
    let pair_elapsed = pair_start.elapsed();
    assert_eq!(pair_output.stdout, "pair\n");

    assert!(
        pair_elapsed.as_secs_f64() < single_elapsed.as_secs_f64() * 1.6 + 0.05,
        "paired sleeps should overlap; single={single_elapsed:?}, pair={pair_elapsed:?}"
    );
}

#[test]
fn run_async_processes_overlap_under_task_all() {
    let repo_root = repo_root();

    let single_runtime = Runtime::from_repo_root(&repo_root);
    let single_start = Instant::now();
    let single_output = single_runtime
        .run_script_source(
            r#"
            from std/proc import Proc;

            async function __main__ ( argv ) {
                let run := await {
                    Proc.run_async(
                        "perl",
                        [ "-e", "select undef, undef, undef, 0.12; print qq<single>" ],
                    );
                };
                say run{stdout};
            }
            "#,
        )
        .expect("single async process should run");
    let single_elapsed = single_start.elapsed();
    assert_eq!(single_output.stdout, "single\n");

    let pair_runtime = Runtime::from_repo_root(&repo_root);
    let pair_start = Instant::now();
    let pair_output = pair_runtime
        .run_script_source(
            r#"
            from std/proc import Proc;
            from std/task import all;

            async function __main__ ( argv ) {
                let both := await {
                    all( [
                        Proc.run_async(
                            "perl",
                            [ "-e", "select undef, undef, undef, 0.12; print qq<a>" ],
                        ),
                        Proc.run_async(
                            "perl",
                            [ "-e", "select undef, undef, undef, 0.12; print qq<b>" ],
                        ),
                    ] );
                };
                say both[0]{stdout} _ both[1]{stdout};
            }
            "#,
        )
        .expect("paired async processes should run");
    let pair_elapsed = pair_start.elapsed();
    assert_eq!(pair_output.stdout, "ab\n");

    assert!(
        pair_elapsed.as_secs_f64() < single_elapsed.as_secs_f64() * 1.6 + 0.08,
        "paired run_async calls should overlap; single={single_elapsed:?}, pair={pair_elapsed:?}"
    );
}

#[test]
fn pipeline_async_streams_between_running_processes() {
    let repo_root = repo_root();

    let runtime = Runtime::from_repo_root(&repo_root);
    let start = Instant::now();
    let output = runtime
        .run_script_source(
            r#"
            from std/proc import Proc;

            async function __main__ ( argv ) {
                let pipeline := await {
                    Proc.pipeline_async(
                        [
                            [
                                "perl",
                                "-e",
                                "$|=1; for my $i (1..4) { print qq<$i\n>; select undef, undef, undef, 0.10; }",
                            ],
                            [
                                "perl",
                                "-e",
                                "while (<STDIN>) { select undef, undef, undef, 0.10; print uc($_); }",
                            ],
                        ],
                    );
                };
                say pipeline{stdout};
            }
            "#,
        )
        .expect("streaming async pipeline should run");
    let elapsed = start.elapsed();

    assert_eq!(output.stdout, "1\n2\n3\n4\n\n");
    assert!(
        elapsed.as_secs_f64() < 0.65,
        "pipeline_async should stream between concurrently running processes; elapsed={elapsed:?}"
    );
}

#[test]
fn run_async_timeout_and_combinator_cancellation() {
    let repo_root = repo_root();

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_source(
            r#"
            from std/proc import Proc;
            from std/task import sleep, timeout;
            from test/more import *;

            async function __main__ ( argv ) {
                let run := await {
                    Proc.run_async(
                        "perl",
                        [ "-e", "select undef, undef, undef, 0.20; print qq<late>" ],
                        { timeout: 0.03 },
                    );
                };
                ok( not run{ok}, "run_async timeout reports failure" );
                ok( run{timed_out}, "run_async timeout marks timed_out" );
                is( run{stdout}, "", "run_async timeout suppresses late stdout" );

                let child := Proc.run_async(
                    "perl",
                    [ "-e", "select undef, undef, undef, 0.20; print qq<late>" ],
                );
                let threw := false;
                try {
                    await {
                        timeout( 0.03, child );
                    };
                }
                catch ( TimeoutException e ) {
                    threw := true;
                }
                ok( threw, "timeout combinator cancels run_async task" );
                is( child.status(), "cancelled", "cancelled run_async task reports cancelled" );
                await {
                    sleep(0.03);
                };
                done_testing();
            }
            "#,
        )
        .expect("async process timeout should run");

    assert!(output
        .stdout
        .contains("ok 1 - run_async timeout reports failure\n"));
    assert!(output
        .stdout
        .contains("ok 2 - run_async timeout marks timed_out\n"));
    assert!(output
        .stdout
        .contains("ok 5 - cancelled run_async task reports cancelled\n"));
    assert!(output.stdout.contains("1..5\n"));
    assert_eq!(output.stderr, "");
}

#[test]
fn get_async_requests_overlap_under_task_all() {
    let repo_root = repo_root();
    let Some((base_url, server)) = spawn_delayed_http_server(Duration::from_millis(120), 3) else {
        eprintln!("skipping get_async overlap test: local socket bind is not permitted");
        return;
    };

    let single_runtime = Runtime::from_repo_root(&repo_root);
    let single_start = Instant::now();
    let single_output = single_runtime
        .run_script_source(&format!(
            r#"
            from std/net/http import UserAgent;

            async function __main__ ( argv ) {{
                let ua := new UserAgent( timeout: 1 );
                let response := await {{
                    ua.get_async( "{base_url}/single" );
                }};
                say response.status();
            }}
            "#
        ))
        .expect("single async HTTP request should run");
    let single_elapsed = single_start.elapsed();
    assert_eq!(single_output.stdout, "200\n");

    let pair_runtime = Runtime::from_repo_root(&repo_root);
    let pair_start = Instant::now();
    let pair_output = pair_runtime
        .run_script_source(&format!(
            r#"
            from std/net/http import UserAgent;
            from std/task import all;

            async function __main__ ( argv ) {{
                let ua := new UserAgent( timeout: 1 );
                let both := await {{
                    all( [
                        ua.get_async( "{base_url}/a" ),
                        ua.get_async( "{base_url}/b" ),
                    ] );
                }};
                say both[0].content() _ both[1].content();
            }}
            "#
        ))
        .expect("paired async HTTP requests should run");
    let pair_elapsed = pair_start.elapsed();
    assert_eq!(pair_output.stdout, "okok\n");
    server.join().expect("test HTTP server should finish");

    assert!(
        pair_elapsed.as_secs_f64() < single_elapsed.as_secs_f64() * 1.6 + 0.08,
        "paired get_async calls should overlap; single={single_elapsed:?}, pair={pair_elapsed:?}"
    );
}

#[test]
fn path_async_methods_return_pending_tasks() {
    let repo_root = repo_root();

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_source(
            r#"
            from std/io import Path;
            from test/more import *;

            async function __main__ ( argv ) {
                let text_file := Path.tempfile();
                let write := text_file.spew_utf8_async("alpha\nbeta\n");
                is( write.status(), "pending", "spew_utf8_async returns pending task" );
                await {
                    write;
                };

                let read := text_file.slurp_utf8_async();
                is( read.status(), "pending", "slurp_utf8_async returns pending task" );
                is( await { read; }, "alpha\nbeta\n", "slurp_utf8_async reads text" );

                let lines := text_file.lines_utf8_async();
                is( lines.status(), "pending", "lines_utf8_async returns pending task" );
                is( ( await { lines; } )[1], "beta\n", "lines_utf8_async returns text lines" );

                let raw_file := Path.tempfile();
                let raw_write := raw_file.spew_async( ~to_binary("abc") );
                is( raw_write.status(), "pending", "spew_async returns pending task" );
                await {
                    raw_write;
                };

                let raw_read := raw_file.slurp_async();
                is( raw_read.status(), "pending", "slurp_async returns pending task" );
                let raw_value := await {
                    raw_read;
                };
                is( typeof raw_value, "BinaryString", "slurp_async reads binary data" );
                done_testing();
            }
            "#,
        )
        .expect("async Path methods should run");

    assert!(output
        .stdout
        .contains("ok 1 - spew_utf8_async returns pending task\n"));
    assert!(output
        .stdout
        .contains("ok 2 - slurp_utf8_async returns pending task\n"));
    assert!(output
        .stdout
        .contains("ok 4 - lines_utf8_async returns pending task\n"));
    assert!(output
        .stdout
        .contains("ok 6 - spew_async returns pending task\n"));
    assert!(output
        .stdout
        .contains("ok 7 - slurp_async returns pending task\n"));
    assert!(output.stdout.contains("1..8\n"));
    assert_eq!(output.stderr, "");
}

#[test]
fn native_async_tasks_cancel_with_cancelled_exception() {
    let repo_root = repo_root();

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_source(
            r#"
            from std/io import Path;
            from std/net/http import UserAgent;
            from std/proc import Proc;
            from std/task import all, sleep;
            from test/more import *;

            async function cancelled_with ( task, reason ) {
                task.cancel(reason);
                try {
                    await {
                        task;
                    };
                }
                catch ( CancelledException e ) {
                    return e.message = reason;
                }
                return false;
            }

            async function __main__ ( argv ) {
                let file := Path.tempfile();
                ok(
                    await {
                        cancelled_with(
                            file.spew_utf8_async("cancelled"),
                            "file stop",
                        );
                    },
                    "async file task cancellation throws CancelledException",
                );

                ok(
                    await {
                        cancelled_with(
                            Proc.run_async("sh", [ "-c", "sleep 1" ]),
                            "proc stop",
                        );
                    },
                    "async process task cancellation throws CancelledException",
                );

                let ua := new UserAgent( timeout: 1 );
                ok(
                    await {
                        cancelled_with(
                            ua.get_async("http://127.0.0.1:1/"),
                            "http stop",
                        );
                    },
                    "async HTTP task cancellation throws CancelledException",
                );

                let sleeper := sleep(1);
                let combo := all( [ sleeper ] );
                combo.cancel("combo stop");
                is(
                    sleeper.status(),
                    "cancelled",
                    "combinator cancellation reaches child task",
                );

                done_testing();
            }
            "#,
        )
        .expect("native async cancellation script should run");

    assert!(output.stdout.contains(concat!(
        "ok 1 - async file task cancellation throws CancelledException\n",
        "ok 2 - async process task cancellation throws CancelledException\n",
        "ok 3 - async HTTP task cancellation throws CancelledException\n",
        "ok 4 - combinator cancellation reaches child task\n",
        "1..4\n",
    )));
    assert_eq!(output.stderr, "");
}

#[test]
fn debug_mode_warns_for_blocking_sync_apis_inside_async_tasks() {
    let repo_root = repo_root();

    let runtime =
        Runtime::from_repo_root_with_policy(&repo_root, RuntimePolicy::new().debug_level(1));
    let output = runtime
        .run_script_source(
            r#"
            from std/io import Path;
            from std/proc import sleep;
            from test/more import *;

            let top_level_file := Path.tempfile();
            top_level_file.spew_utf8("top");

            async function __main__ ( argv ) {
                let file := Path.tempfile();
                file.spew_utf8("alpha");
                is( file.slurp_utf8(), "alpha", "sync file API still works" );
                sleep(0);
                done_testing();
            }
            "#,
        )
        .expect("blocking diagnostics script should run");

    assert!(output.stdout.contains("ok 1 - sync file API still works\n"));
    assert!(output.stdout.contains("1..1\n"));
    assert_eq!(
        output
            .stderr
            .matches("BlockingOperation: std/io Path.spew_utf8")
            .count(),
        1,
        "top-level sync file operation should not warn"
    );
    assert!(output
        .stderr
        .contains("BlockingOperation: std/io Path.slurp_utf8"));
    assert!(output.stderr.contains("BlockingOperation: std/proc sleep"));
}

#[test]
fn runs_named_args_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/functions/named-args.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("named-args.zzs should execute successfully");

    assert_eq!(output.stdout.lines().count(), 47);
    assert!(output
        .stdout
        .contains("ok 1 - named arguments became a pairlist\n"));
    assert!(output
        .stdout
        .contains("ok 38 - can't pass named arguments to a function which doesn't accept them\n",));
    assert!(output
        .stdout
        .contains("ok 46 - can't pass named arguments to a function which doesn't accept them\n",));
    assert!(output.stdout.ends_with("1..46\n"));
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_field_accessors_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/oop/field-accessors.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("field-accessors.zzs should execute successfully");

    assert_eq!(
        output.stdout,
        concat!(
            "ok 1 - get accessor returns default value\n",
            "ok 2 - has accessor reports non-null value\n",
            "ok 3 - set accessor stores provided value\n",
            "ok 4 - clear accessor marks slot as empty\n",
            "ok 5 - slot can be set again after clear\n",
            "ok 6 - typed set accessor preserves field type checks\n",
            "1..6\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_assignment_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/operators/assignment.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("assignment.zzs should execute successfully");

    assert_eq!(
        output.stdout,
        concat!(
            "ok 1 - += works\n",
            "ok 2 - -= works\n",
            "ok 3 - *= works\n",
            "ok 4 - ×= works\n",
            "ok 5 - /= works\n",
            "ok 6 - ÷= works\n",
            "ok 7 - **= works\n",
            "ok 8 - _= works\n",
            "ok 9 - ?:= works when undef\n",
            "ok 10 - ?:= preserves defined zero\n",
            "ok 11 - ~= supports /g and captures\n",
            "ok 12 - ~= supports unicode arrow and expression replacement\n",
            "ok 13 - replacement scope variable m does not leak\n",
            "ok 14 - ~= supports do{...} replacement expressions\n",
            "ok 15 - ~= do{...} replacement can branch and return\n",
            "ok 16 - postfix ++ mutates\n",
            "ok 17 - prefix ++ returns new value\n",
            "ok 18 - postfix -- mutates\n",
            "ok 19 - prefix -- returns new value\n",
            "ok 20 - \\x getter reads lvalue\n",
            "ok 21 - \\x setter returns assigned value\n",
            "ok 22 - \\x setter mutates target lvalue\n",
            "ok 23 - \\dict{key} getter works\n",
            "ok 24 - \\dict{key} setter works\n",
            "ok 25 - \\slice getter returns current slice value\n",
            "ok 26 - \\slice setter replaces slice\n",
            "1..26\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn weak_storage_declarations_assignments_and_fields_work() {
    let repo_root = repo_root();
    let runtime = Runtime::from_repo_root(&repo_root);

    let output = runtime
        .run_script_source(
            r#"
            from test/more import *;

            class Node {
                let parent with get, set, clear, has but weak;
            }

            let owner := [];
            let lexical := owner but weak;
            is( lexical.length(), 0, "weak lexical reads live referent" );
            owner := null;
            is( lexical, null, "weak lexical reads null after referent drops" );

            let strong_owner := [];
            let slot := null;
            slot := strong_owner but weak;
            is( slot.length(), 0, "one-off weak assignment reads live referent" );
            strong_owner := null;
            is( slot, null, "one-off weak assignment does not keep referent alive" );

            let later_owner := [];
            slot := later_owner;
            later_owner := null;
            is( typeof slot, "Array", "ordinary assignment after one-off weak is strong" );

            let parent := new Node();
            let child := new Node( parent: parent );
            is( child.get_parent(), parent, "weak field constructor arg reads live referent" );
            parent := null;
            is( child.get_parent(), null, "weak field constructor arg drops referent" );

            let parent2 := new Node();
            child.set_parent(parent2);
            ok( child.has_parent(), "weak field setter stores live referent" );
            parent2 := null;
            is( child.get_parent(), null, "weak field setter drops referent" );
            child.clear_parent();
            ok( !child.has_parent(), "weak field clearer stores null" );

            let direct_owner := [];
            let arr := [];
            arr[0] := direct_owner but weak;
            is( arr[0], direct_owner, "weak index assignment reads live referent" );
            direct_owner := null;
            is( arr[0], null, "weak index assignment drops referent" );

            let dict_owner := [];
            let dict := {};
            dict{item} := dict_owner but weak;
            is( dict{item}, dict_owner, "weak dict assignment reads live referent" );
            dict_owner := null;
            is( dict{item}, null, "weak dict assignment drops referent" );

            let path_owner := [];
            let path_data := { item: null };
            path_data @ "/item" := path_owner but weak;
            is( path_data{item}, path_owner, "weak path assignment reads live referent" );
            path_owner := null;
            is( path_data{item}, null, "weak path assignment drops referent" );

            let Number scalar but weak := 7;
            is( scalar, 7, "weak scalar declaration stores scalar normally" );

            done_testing();
            "#,
        )
        .expect("weak storage script should execute");

    assert!(output.stdout.contains("1..17"), "got {}", output.stdout);
    assert!(
        !output.stdout.contains("not ok"),
        "got failing TAP:\n{}",
        output.stdout
    );
    assert!(
        output
            .stderr
            .contains("'but weak' on Number binding 'scalar' has no effect"),
        "got stderr:\n{}",
        output.stderr
    );
}

#[test]
fn runs_weak_collection_methods_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/weak/collection-methods.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("collection-methods.zzs should execute successfully");

    assert!(output.stdout.contains("1..24"), "got {}", output.stdout);
    assert!(
        !output.stdout.contains("not ok"),
        "got failing TAP:\n{}",
        output.stdout
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn weak_collection_methods_drop_and_overwrite_entries() {
    let repo_root = repo_root();
    let runtime = Runtime::from_repo_root(&repo_root);

    let output = runtime
        .run_script_source(
            r#"
            from test/more import *;

            class Owner {
                let label with get;
            }

            function make_owner ( String label ) {
                return new Owner( label: label );
            }

            let arr_owner := make_owner("arr");
            let arr := [];
            arr.push_weak(arr_owner);
            is( arr[0].get_label(), "arr", "Array weak entry reads live referent" );
            arr_owner := null;
            is( arr[0], null, "Array weak entry reads null after owner drops" );

            let arr_strong := make_owner("strong");
            arr.set( 0, arr_strong );
            arr_strong := null;
            is( arr[0].get_label(), "strong", "Array.set overwrites weak entry strongly" );
            arr.pop();
            is( arr.length(), 0, "Array.pop removes weak slot" );

            let dict_owner := make_owner("dict");
            let dict := {};
            dict.set_weak( "owner", dict_owner );
            dict_owner := null;
            is( dict{"owner"}, null, "Dict weak entry drops referent" );
            ok( !dict.defined("owner"), "Dict.defined treats dead weak entry as null" );
            let dict_strong := make_owner("dict-strong");
            dict.set( "owner", dict_strong );
            dict_strong := null;
            is(
                dict{"owner"}.get_label(),
                "dict-strong",
                "Dict.set overwrites weak entry strongly",
            );
            dict.remove("owner");
            ok( !dict.exists("owner"), "Dict.remove removes weak key" );

            let pairs_owner := make_owner("pairlist");
            let pairs := {{ }};
            pairs.add_weak( "owner", pairs_owner );
            pairs_owner := null;
            is( pairs{"owner"}, null, "PairList weak entry drops referent" );
            ok( !pairs.defined("owner"), "PairList.defined treats dead weak entry as null" );
            pairs.remove("owner");
            ok( !pairs.exists("owner"), "PairList.remove removes weak key" );
            let pairs_strong := make_owner("pairlist-strong");
            pairs.set( "owner", pairs_strong );
            pairs_strong := null;
            is(
                pairs{"owner"}.get_label(),
                "pairlist-strong",
                "PairList.set stores strongly after weak entry removal",
            );
            pairs.clear();
            is( pairs.length(), 0, "PairList.clear removes weak entries" );

            let set_owner := make_owner("set");
            let set := << >>;
            set.add_weak(set_owner);
            ok( set.contains(set_owner), "Set weak entry reads live referent" );
            set_owner := null;
            ok( set.contains(null), "Set weak entry drops referent" );
            set.clear();
            is( set.length(), 0, "Set.clear removes weak entries" );

            let bag_owner := make_owner("bag");
            let bag := <<< >>>;
            bag.add_weak(bag_owner);
            ok( bag.contains(bag_owner), "Bag weak entry reads live referent" );
            bag_owner := null;
            ok( bag.contains(null), "Bag weak entry drops referent" );
            bag.clear();
            is( bag.length(), 0, "Bag.clear removes weak entries" );

            done_testing();
            "#,
        )
        .expect("weak collection method script should execute");

    assert!(output.stdout.contains("1..19"), "got {}", output.stdout);
    assert!(
        !output.stdout.contains("not ok"),
        "got failing TAP:\n{}",
        output.stdout
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_binary_string_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/operators/binary-string.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("binary-string.zzs should execute successfully");

    assert!(
        output.stdout.contains("1..28\n"),
        "got TAP output:\n{}",
        output.stdout
    );
    assert!(
        !output.stdout.contains("not ok"),
        "got failing TAP:\n{}",
        output.stdout
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_type_instanceof_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/types/instanceof.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("types/instanceof.zzs should execute successfully");

    assert_eq!(output.stdout.lines().count(), 62);
    assert!(output.stdout.contains("ok 1\n"));
    assert!(output.stdout.contains("ok 61\n"));
    assert!(output.stdout.ends_with("1..61\n"));
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_type_return_types_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/types/return-types.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("types/return-types.zzs should execute successfully");

    assert_eq!(
        output.stdout,
        concat!(
            "ok 1 - typed function returns Number\n",
            "ok 2 - Any return type skips checks\n",
            "ok 3 - typed method returns Number\n",
            "ok 4 - function return type mismatch throws TypeException\n",
            "ok 5 - method return type mismatch throws TypeException\n",
            "1..5\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_type_tostring_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/types/tostring.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("types/tostring.zzs should execute successfully");

    assert_eq!(output.stdout.lines().count(), 18);
    assert!(output.stdout.contains("ok 1 - Null => String\n"));
    assert!(output.stdout.contains("ok 14 - Regexp => non-null\n"));
    assert!(output
        .stdout
        .contains("ok 17 - Exception uses to_String method\n"));
    assert!(output.stdout.ends_with("1..17\n"));
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_oop_dynamic_member_call_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/oop/dynamic-member-call.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("dynamic-member-call.zzs should execute successfully");

    assert_eq!(
        output.stdout,
        concat!(
            "ok 1 - dynamic member call syntax .(expr)(...) works\n",
            "1..1\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_oop_inheritance_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/oop/inheritance.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("inheritance.zzs should execute successfully");

    assert_eq!(
        output.stdout,
        concat!(
            "ok 1 - extends supports inherited fields and methods\n",
            "ok 2 - builtin subclasses preserve catch(Exception) behavior\n",
            "1..2\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_oop_traits_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/oop/traits.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("traits.zzs should execute successfully");

    assert_eq!(
        output.stdout,
        concat!(
            "ok 1 - with composes trait methods\n",
            "ok 2 - but alias composes trait methods\n",
            "ok 3 - does recognizes composed traits\n",
            "ok 4 - super() can dispatch to trait method in overrides\n",
            "ok 5 - trait can call class get accessor shortcut\n",
            "ok 6 - trait can call class set accessor shortcut\n",
            "ok 7 - set accessor in trait updates consumed class field\n",
            "1..7\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_oop_ambiguous_classes_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/oop/ambiguous-classes.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("ambiguous-classes.zzs should execute successfully");

    assert_eq!(
        output.stdout,
        concat!(
            "ok 1\n", "ok 2\n", "ok 3\n", "ok 4\n", "ok 5\n", "ok 6\n", "ok 7\n", "ok 8\n",
            "1..8\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_oop_super_and_static_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("languagetests/lang/oop/super-and-static.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("super-and-static.zzs should execute successfully");

    assert_eq!(
        output.stdout,
        concat!(
            "ok 1 - instance overrides can call super()\n",
            "ok 2 - static overrides can call super()\n",
            "ok 3 - static self dispatch stays late-bound on subclass\n",
            "1..3\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn runs_std_clib_ztest_script() {
    let repo_root = repo_root();
    let script = repo_root.join("stdlib/tests/std/clib.zzs");

    let runtime = Runtime::from_repo_root(&repo_root);
    let output = runtime
        .run_script_file(&script)
        .expect("std/clib.zzs should execute successfully");

    assert!(
        output
            .stdout
            .starts_with("ok 1 - example C library exists\n")
            || output
                .stdout
                .starts_with("ok 1 - built example C library with gcc\n"),
        "unexpected first TAP line: {:?}",
        output.stdout.lines().next()
    );
    assert!(
        output.stdout.ends_with(concat!(
            "ok 2 - greet returns bytes\n",
            "ok 3 - greet_person accepts BinaryString\n",
            "ok 4 - greet_person accepts nullable binary parameter\n",
            "ok 5 - int64 parameters and return\n",
            "ok 6 - float64 parameters and return\n",
            "ok 7 - bool parameter and return\n",
            "ok 8 - bool false parameter and return\n",
            "ok 9 - void return maps to null\n",
            "ok 10 - null pointer binary return maps to null\n",
            "ok 11 - binary bytes round trip\n",
            "ok 12 - binary parameter with explicit length\n",
            "ok 13 - nullable binary parameter accepts null\n",
            "ok 14 - configured free function runs for owned returns\n",
            "ok 15 - has_symbol detects exported symbol\n",
            "ok 16 - close returns null\n",
            "ok 17 - double close returns null\n",
            "1..17\n",
        )),
        "unexpected std/clib TAP output:\n{}",
        output.stdout
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn std_clib_close_invalidates_bound_functions() {
    assert_std_clib_source_error(
        r#"
            from std/clib import CLib;

            let lib := CLib.open("stdlib/test-fixtures/example_clib/libgreet.so");
            let greet := lib.func(
                "greet",
                [],
                {
                    type: "binary",
                    terminated_by: "nul",
                    free: "greet_free"
                },
            );
            lib.close();
            greet.call();
            "#,
        "closed CLibrary",
    );
}

#[test]
fn std_clib_close_invalidates_symbol_lookup() {
    assert_std_clib_source_error(
        r#"
            from std/clib import CLib;

            let lib := CLib.open("stdlib/test-fixtures/example_clib/libgreet.so");
            lib.close();
            lib.has_symbol("greet");
            "#,
        "CLibrary is closed",
    );
}

#[test]
fn std_clib_reports_binding_descriptor_and_call_errors() {
    assert_std_clib_source_error(
        r#"
            from std/clib import CLib;

            let lib := CLib.open("stdlib/test-fixtures/example_clib/libgreet.so");
            lib.func("not_a_symbol", [], "null");
            "#,
        "Could not bind C symbol 'not_a_symbol'",
    );
    assert_std_clib_source_error(
        r#"
            from std/clib import CLib;

            let lib := CLib.open("stdlib/test-fixtures/example_clib/libgreet.so");
            lib.func("greet_add_i64", [ { type: "int", bits: 32 } ], "null");
            "#,
        "parameter 0 int descriptor only supports bits=64",
    );
    assert_std_clib_source_error(
        r#"
            from std/clib import CLib;

            let lib := CLib.open("stdlib/test-fixtures/example_clib/libgreet.so");
            let greet := lib.func(
                "greet",
                [],
                {
                    type: "binary",
                    terminated_by: "nul",
                    free: "greet_free"
                },
            );
            greet.call(1);
            "#,
        "Function 'greet' expects 0 arguments, got 1",
    );
    assert_std_clib_source_error(
        r#"
            from std/clib import CLib;

            let lib := CLib.open("stdlib/test-fixtures/example_clib/libgreet.so");
            let bool_not := lib.func("greet_not", [ "bool" ], "bool");
            bool_not.call(1);
            "#,
        "argument 0 must be Boolean, got Number",
    );
}

fn assert_std_clib_source_error(source: &str, needle: &str) {
    let repo_root = repo_root();
    let runtime = Runtime::from_repo_root(&repo_root);

    let err = match runtime.run_script_source(source) {
        Ok(_) => panic!("expected std/clib source to fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains(needle),
        "expected error to contain {needle:?}, got {err}"
    );
}

#[test]
fn std_marshal_exports_public_api_and_exceptions() {
    let repo_root = repo_root();
    let runtime = Runtime::from_repo_root(&repo_root);

    let output = runtime
        .run_script_source(
            r#"
            from std/marshal import
                dump,
                load,
                safe_to_dump,
                MarshallingException,
                UnmarshallingException;

            say typeof dump;
            say typeof load;
            say typeof safe_to_dump;
            say typeof MarshallingException;
            say typeof UnmarshallingException;

            try {
                dump(dump);
            }
            catch ( MarshallingException e ) {
                say e{message};
            }

            try {
                load(to_binary("abc"));
            }
            catch ( UnmarshallingException e ) {
                say e{message};
            }

            say safe_to_dump(42);
            say safe_to_dump(dump);

            try {
                load("abc");
            }
            catch ( TypeException e ) {
                say e{message};
            }

            try {
                safe_to_dump(1, 2);
            }
            catch ( TypeException e ) {
                say e{message};
            }
            "#,
        )
        .expect("std/marshal stubs should be importable and catchable");

    assert_eq!(
        output.stdout,
        concat!(
            "Function\n",
            "Function\n",
            "Function\n",
            "Class\n",
            "Class\n",
            "std/marshal.dump failed: Value of type Function is not marshalable in this phase\n",
            "std/marshal.load failed: Invalid Zuzu Marshal CBOR: trailing bytes after item\n",
            "1\n",
            "0\n",
            "load expects BinaryString, got String\n",
            "safe_to_dump expects 1 argument, got 2\n",
        ),
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn std_marshal_round_trips_phase25_data_graphs() {
    let repo_root = repo_root();
    let runtime = Runtime::from_repo_root(&repo_root);

    let output = runtime
        .run_script_source(
            r#"
            from std/io import Path;
            from std/marshal import dump, load, safe_to_dump;
            from std/time import Time;
            from test/more import *;

            is( load( dump(null) ), null, "null round trip" );
            is( load( dump(true) ), true, "true round trip" );
            is( load( dump(42) ), 42, "number round trip" );
            is( load( dump("hello") ), "hello", "String round trip" );

            let raw := to_binary("ABC");
            let raw_roundtrip := load( dump(raw) );
            is( typeof raw_roundtrip, "BinaryString", "BinaryString type" );
            ok( raw_roundtrip == raw, "BinaryString payload" );

            let cycle := [];
            cycle.push(cycle);
            let loaded_cycle := load( dump(cycle) );
            is( typeof loaded_cycle, "Array", "cyclic Array type" );

            let dict := load( dump({ alpha: 1, beta: [ 2 ] }) );
            is( typeof dict, "Dict", "Dict type" );
            is( dict{alpha}, 1, "Dict scalar value" );
            is( dict{beta}[0], 2, "Dict nested Array value" );

            let pairlist := load( dump({{ foo: 1, foo: 2 }}) );
            is( typeof pairlist, "PairList", "PairList type" );
            is( pairlist[0]{pair}[1], 1, "PairList first duplicate value" );
            is( pairlist[1]{pair}[1], 2, "PairList second duplicate value" );

            let set := load( dump( << 1, 2, 2 >> ) );
            is( typeof set, "Set", "Set type" );
            is( set.length(), 2, "Set keeps unique values" );

            let bag := load( dump( <<< 1, 2, 2 >>> ) );
            is( typeof bag, "Bag", "Bag type" );
            is( bag.length(), 3, "Bag keeps duplicate values" );

            let pair := load( dump( new Pair( pair: [ "key", "value" ] ) ) );
            is( typeof pair, "Pair", "Pair type" );
            is( pair{pair}[0], "key", "Pair key" );
            is( pair{pair}[1], "value", "Pair value" );

            let time := load( dump( new Time(12345) ) );
            is( typeof time, "Time", "Time type" );
            is( time.epoch(), 12345, "Time epoch" );

            let path := load( dump( new Path("tmp/../file.txt") ) );
            is( typeof path, "Path", "Path type" );
            is( path.to_String(), "tmp/../file.txt", "Path string" );

            is( safe_to_dump([ 1, [ 2 ] ]), true, "safe_to_dump accepts data graph" );
            done_testing();
            "#,
        )
        .expect("std/marshal phase 25 data graph should round trip");

    assert!(output.stdout.contains("1..25"), "got {}", output.stdout);
    assert!(
        !output.stdout.contains("not ok"),
        "got failing TAP:\n{}",
        output.stdout
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn std_marshal_preserves_weak_storage_records() {
    let repo_root = repo_root();
    let runtime = Runtime::from_repo_root(&repo_root);

    let output = runtime
        .run_script_source(
            r#"
            from std/marshal import dump, load;
            from test/more import *;

            class Node {
                let parent with get, set but weak;
            }

            let parent := new Node();
            let child := new Node( parent: parent );
            let loaded_nodes := load( dump( [ parent, child ] ) );
            is(
                loaded_nodes[1].get_parent(),
                loaded_nodes[0],
                "weak object field round trips while target is strong",
            );
            loaded_nodes[0] := null;
            is(
                loaded_nodes[1].get_parent(),
                null,
                "weak object field remains weak after load",
            );

            let owner := [];
            let arr := [ owner ];
            arr[1] := owner but weak;
            let loaded_arr := load( dump(arr) );
            is( loaded_arr[1], loaded_arr[0], "weak array entry round trips live target" );
            loaded_arr[0] := null;
            is( loaded_arr[1], null, "weak array entry remains weak after load" );

            done_testing();
            "#,
        )
        .expect("weak marshal script should execute");

    assert!(output.stdout.contains("1..4"), "got {}", output.stdout);
    assert!(
        !output.stdout.contains("not ok"),
        "got failing TAP:\n{}",
        output.stdout
    );
    assert_eq!(output.stderr, "");
}

#[test]
fn std_marshal_round_trips_user_objects_and_lifecycle_hooks() {
    let repo_root = repo_root();
    let runtime = Runtime::from_repo_root(&repo_root);

    let output = runtime
        .run_script_source(
            r#"
            from std/marshal import dump, load;
            from test/more import *;

            __global__{builds} := 0;

            class MarshalPhase26Box {
                let name with get, set := "unset";
                let status := "new";

                method __build__ () {
                    __global__{builds} := __global__{builds} + 1;
                }

                method __on_dump__ () {
                    status := "dumped";
                }

                method __on_load__ () {
                    status := status _ ":loaded";
                }

                method label () {
                    return name _ ":" _ status;
                }
            }

            let box := new MarshalPhase26Box( name: "Ada" );
            let copy := load( dump(box) );

            is( typeof copy, "MarshalPhase26Box", "object class round trip" );
            is( copy.get_name(), "Ada", "field accessor survives" );
            is( copy.label(), "Ada:dumped:loaded", "lifecycle hooks run" );
            is( __global__{builds}, 1, "load suppresses __build__" );
            done_testing();
            "#,
        )
        .expect("std/marshal user object lifecycle should round trip");

    assert!(output.stdout.contains("1..4"), "got {}", output.stdout);
    assert!(
        !output.stdout.contains("not ok"),
        "got failing TAP:\n{}",
        output.stdout
    );
    assert_eq!(output.stderr, "");
}
