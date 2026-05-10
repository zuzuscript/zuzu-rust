# ZuzuScript Rust Runtime

This repository contains the Rust implementation of ZuzuScript. The frontend
has a real lexer, a recursive-descent parser with explicit expression
precedence, and a true AST. The `zuzu-rust` CLI should continue to support
`--dump-ast` with a stable, machine-readable AST format.

Use Oxford English in documentation. Prefer standard British English with
`-ize` word endings.

## Split Repository Layout

Shared ZuzuScript resources live in submodules:

- `stdlib/modules` contains Pure Zuzu Modules and POD stubs for
  runtime-supported modules.
- `stdlib/tests` contains standard-library ztests.
- `stdlib/test-modules` contains test helper modules.
- `stdlib/test-fixtures` contains standard-library fixtures.
- `languagetests` contains language-level ztests.
- `docs/examples` and `docs/userguide` are documentation submodules.

Do not refer to sibling repositories with `..`. If this repository needs
shared files from another repository, add them as a git submodule.

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

    nice -n 10 cargo check
    nice -n 10 cargo test
    nice -n 10 cargo run --bin zuzu-rust-run-tests -- languagetests stdlib/tests

Ztests emit TAP. A passing ztest should emit a valid plan, no `not ok` lines,
and exit with status zero. When fixing tests, prefer fixing parser/runtime
behaviour. Do not modify `.zzs` test scripts or fixture data.

## Style

Use `cargo fmt` for Rust code you touch, but avoid broad formatting churn in
unrelated files. For ZuzuScript code, use tabs for indentation, spaces for
alignment, One True Brace Style, uncuddled `else`, and semicolons as
terminators.
