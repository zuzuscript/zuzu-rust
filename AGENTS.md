# ZuzuScript Rust Runtime

This repository contains the Rust implementation of ZuzuScript. The frontend
has a real lexer, a recursive-descent parser with explicit expression
precedence, and a true AST. The `zuzu-rust` CLI should continue to support
`--dump-ast` with a stable, machine-readable AST format.

Use Oxford English in documentation: mostly standard British English, with
`-ize` word endings.

## Relationship To Other Projects

`zuzu-rust` is one of the three main runtimes, alongside `zuzu-perl` and
`zuzu-js`. It consumes shared resources through submodules:

- `stdlib` for shared modules, stdlib tests, fixtures, and test helpers.
- `languagetests` for language conformance tests.
- `docs/examples` and `docs/userguide` for examples and language reference.

The matrix project runs this runtime against the shared tests.
`zuzu-designer` embeds this crate for GUI XML previews. Do not refer to
sibling repositories with `..`; use the local submodules.

## Project Shape

- `src/lexer.rs`, `src/parser.rs`, `src/ast.rs`, and `src/token.rs` hold the
  frontend.
- `src/runtime.rs` and related modules hold evaluation and native module
  support.
- `src/codegen.rs`, `src/optimizer.rs`, `src/sema.rs`, and `src/infer.rs`
  support analysis and generated output.
- `src/web.rs` and the `zuzu-rust-server` binary support web execution.
- `tests/` contains Rust tests for CLI, AST dumping, runtime, optimizer,
  worker, and server behaviour.

## Runtime Rules

The BNF, Perl parser/runtime, and JavaScript parser are the main guides for
behaviour. If a Pure Zuzu Module exists, the Rust runtime must load, parse,
and evaluate it through normal ZuzuScript semantics. Do not add Rust-side
shortcuts or native replacements for `std/path/*`, including `std/path/z`
and `std/path/zz`.

Implement runtime-supported modules in Rust when required. The
runtime-supported `perl.zzm` module is out of scope for `zuzu-rust`.

## Tests

Use `nice` for compile-heavy checks:

```bash
nice -n 10 cargo check
nice -n 10 cargo test
nice -n 10 cargo run --bin zuzu-rust-run-tests -- languagetests stdlib/tests
```

Ztests emit TAP. A passing ztest should emit a valid plan, no `not ok`
lines, and exit with status zero. When fixing tests, prefer fixing
parser/runtime behaviour. Do not modify `.zzs` test scripts or fixture data
unless the test itself is the requested target.

Keep `--dump-ast` stable and structured. If AST output changes, update
focused tests and treat the output shape as a public interface for future
tooling.

## Style

Use `cargo fmt` for Rust code you touch, but avoid broad formatting churn in
unrelated files. For ZuzuScript code, use tabs for indentation, spaces for
alignment, One True Brace Style, uncuddled `else`, and semicolons as
terminators.
