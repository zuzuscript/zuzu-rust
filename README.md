# ZuzuScript Rust Runtime

`zuzu-rust` is the Rust implementation of ZuzuScript, a scripting language for
automation, command-line tools, text processing, data wrangling, integrations,
and small web applications.

This runtime can run `.zzs` scripts, load ZuzuScript modules, start an
interactive REPL, serve ZuzuScript web applications, and print a stable JSON AST
for tooling.

## Installation

### Debian Package

Pre-built Debian packages are available from:

<https://zuzulang.org/downloads/>

This is the simplest installation route on supported Linux systems. The package
installs the Rust runtime, the shared standard library, packaged helper scripts,
manual pages, and a `zuzu` command wrapper.

### Cargo Installation

From a checkout of this repository:

```sh
git submodule update --init stdlib
cargo install --path .
./post-install.sh
```

The extra `post-install.sh` step installs the shared standard library and
creates the `zuzu` wrapper in the same layout used by the packaged install.

For custom installation locations, see [INSTALL.md](INSTALL.md).

## Quick Start

Create `hello.zzs`:

```zzs
say "Hello, world!";
```

Run it:

```sh
zuzu hello.zzs
```

If you are using the runtime binary directly:

```sh
zuzu-rust hello.zzs
```

You can also run inline code:

```sh
zuzu -e 'say "Hello, world!";'
```

Start the REPL:

```sh
zuzu --repl
```

## Main Commands

- `zuzu` runs ZuzuScript using the installed wrapper.
- `zuzu-rust` runs scripts directly with the Rust runtime.
- `zuzu-rust-server` serves ZuzuScript web applications.
- `zuzu-rust-parse-files` checks that `.zzs` and `.zzm` files parse.
- `zuzu-rust-run-tests` runs ZuzuScript TAP tests.

Run any command with `--help` for its options.

## Useful Runtime Options

```sh
zuzu-rust [options] path/to/script.zzs [arg ...]
zuzu-rust [options] -e 'code' [arg ...]
```

Common options:

- `-I/path/to/lib` adds a module include directory.
- `-Mmodule` preloads a module with wildcard import.
- `-e 'code'` evaluates inline code.
- `--repl` starts the interactive shell.
- `--dump-ast` prints the parsed AST as stable JSON.
- `--dump-zuzu` prints parsed code back as ZuzuScript source.
- `--deny=CAP` denies a runtime capability.
- `--denymodule=MODULE` denies a specific module.
- `-v` prints the version.
- `-V` prints verbose version details.

## Standard Library

The Rust runtime uses the shared ZuzuScript standard library from the `stdlib`
submodule. A normal package install configures this automatically.

If imports fail after a Cargo install, make sure you have run:

```sh
git submodule update --init stdlib
./post-install.sh
```

Useful environment variables:

- `ZUZULIB` adds module search paths.
- `ZUZU_STDLIB` overrides the installed standard-library module path.

## Examples And Documentation

The user guide starts at:

- [docs/userguide/zuzuscript-guide/01-hello-world-and-everything-after.md](docs/userguide/zuzuscript-guide/01-hello-world-and-everything-after.md)

Example scripts are in:

- [docs/examples](docs/examples)

Run an example from this repository with:

```sh
zuzu-rust docs/examples/06_text_processing_zia_digest.zzs
```

Serve the Rust web example:

```sh
zuzu-rust-server --listen 127.0.0.1:3000 docs/examples/12_rust_web_server.zzs
```

Then open:

```text
http://127.0.0.1:3000/
```

## Checking A Local Build

For a quick parser check:

```sh
cargo run --bin zuzu-rust-parse-files -- docs/examples stdlib/modules
```

For the Rust test suite:

```sh
nice -n 10 cargo test
```

For shared ZuzuScript standard-library tests:

```sh
nice -n 10 cargo run --bin zuzu-rust-run-tests -- languagetests stdlib/tests
```

## Project Status

`zuzu-rust` is released as version 0.2.x and is under active development. The
`--dump-ast` JSON format is treated as a stable, machine-readable interface for
tooling.

## Licence

`zuzu-rust` is copyright Toby Inkster.

It is free software; you may redistribute it and/or modify it under the terms of
either the Artistic License 1.0 or the GNU General Public License version 2 or
later.
