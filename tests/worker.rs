use std::path::PathBuf;
use std::time::{Duration, Instant};

use zuzu_rust::Runtime;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("repo root should exist")
        .to_path_buf()
}

fn test_runtime() -> Runtime {
    let repo_root = repo_root();
    Runtime::new(vec![repo_root.join("t/modules"), repo_root.join("modules")])
}

#[test]
fn worker_transports_payloads_and_results() {
    let output = test_runtime()
        .run_script_source(
            r#"
            from test/more import *;
            from std/worker import Worker;

            async function main () {
                is(
                    await {
                        Worker.spawn(
                            function ( x ) {
                                return x * 2;
                            },
                            [ 21 ],
                        );
                    },
                    42,
                    "scalar transport",
                );

                is(
                    await {
                        Worker.spawn(
                            function () {
                                return { items: [ 1, 2, 3 ], labels: << "a", "b" >> };
                            },
                            [],
                        );
                    },
                    { items: [ 1, 2, 3 ], labels: << "a", "b" >> },
                    "collection transport",
                );
            }

            await {
                main();
            };

            done_testing();
            "#,
        )
        .expect("worker transport script should run");

    assert!(output.stdout.contains("ok 1 - scalar transport\n"));
    assert!(output.stdout.contains("ok 2 - collection transport\n"));
    assert!(output.stdout.contains("1..2\n"));
    assert_eq!(output.stderr, "");
}

#[test]
fn worker_keeps_object_copies_isolated() {
    let output = test_runtime()
        .run_script_source(
            r#"
            from test/more import *;
            from std/worker import Worker;

            class UnitWorkerBox {
                let Number value with get, set := 0;

                method bump () {
                    value := value + 1;
                    return value;
                }
            }

            async function main () {
                let box := new UnitWorkerBox( value: 41 );
                is(
                    await {
                        Worker.spawn(
                            function ( copy ) {
                                return copy.bump();
                            },
                            [ box ],
                        );
                    },
                    42,
                    "worker mutates copied object",
                );
                is( box.get_value(), 41, "parent object is unchanged" );
            }

            await {
                main();
            };

            done_testing();
            "#,
        )
        .expect("worker object isolation script should run");

    assert!(output
        .stdout
        .contains("ok 1 - worker mutates copied object\n"));
    assert!(output
        .stdout
        .contains("ok 2 - parent object is unchanged\n"));
    assert!(output.stdout.contains("1..2\n"));
    assert_eq!(output.stderr, "");
}

#[test]
fn worker_failure_does_not_poison_later_spawns() {
    let output = test_runtime()
        .run_script_source(
            r#"
            from test/more import *;
            from std/worker import Worker;

            async function main () {
                let message := "";
                try {
                    await {
                        Worker.spawn(
                            function () {
                                throw new Exception( message: "unit worker failed" );
                            },
                            [],
                        );
                    };
                }
                catch ( Exception e ) {
                    message := e.to_String();
                }

                like( message, /unit worker failed/, "worker failure is observed" );
                is(
                    await {
                        Worker.spawn(
                            function () {
                                return 42;
                            },
                            [],
                        );
                    },
                    42,
                    "later worker still succeeds",
                );
            }

            await {
                main();
            };

            done_testing();
            "#,
        )
        .expect("worker failure recovery script should run");

    assert!(output
        .stdout
        .contains("ok 1 - worker failure is observed\n"));
    assert!(output
        .stdout
        .contains("ok 2 - later worker still succeeds\n"));
    assert!(output.stdout.contains("1..2\n"));
    assert_eq!(output.stderr, "");
}

#[test]
fn worker_cancellation_rejects_sleeping_worker_task() {
    let output = test_runtime()
        .run_script_source(
            r#"
            from test/more import *;
            from std/task import sleep;
            from std/worker import Worker;

            async function main () {
                let task := Worker.spawn(
                    function () {
                        from std/task import sleep;
                        return sleep(1);
                    },
                    [],
                );

                await {
                    sleep(0.02);
                };
                task.cancel("unit-stop");

                let message := "";
                try {
                    await {
                        task;
                    };
                }
                catch ( CancelledException e ) {
                    message := e.to_String();
                }

                like( message, /unit-stop/, "worker cancellation reason is observed" );
                is( task.status(), "cancelled", "cancelled worker task status" );
            }

            await {
                main();
            };

            done_testing();
            "#,
        )
        .expect("worker cancellation script should run");

    assert!(output
        .stdout
        .contains("ok 1 - worker cancellation reason is observed\n"));
    assert!(output
        .stdout
        .contains("ok 2 - cancelled worker task status\n"));
    assert!(output.stdout.contains("1..2\n"));
    assert_eq!(output.stderr, "");
}

#[test]
fn detached_worker_does_not_block_script_completion() {
    let started = Instant::now();
    let output = test_runtime()
        .run_script_source(
            r#"
            from test/more import *;
            from std/worker import Worker;

            Worker.spawn(
                function () {
                    from std/task import sleep;
                    return sleep(0.2);
                },
                [],
            );

            pass( "detached worker returned control" );
            done_testing();
            "#,
        )
        .expect("detached worker script should run");

    assert!(
        started.elapsed() < Duration::from_millis(150),
        "detached worker should not block script completion"
    );
    assert!(output
        .stdout
        .contains("ok 1 - detached worker returned control\n"));
    assert!(output.stdout.contains("1..1\n"));
    assert_eq!(output.stderr, "");
}
