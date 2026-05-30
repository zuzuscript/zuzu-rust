# Installing zuzu-rust with cargo

The Debian package installs more than the Rust binaries. It also copies the
shared ZuzuScript standard-library modules, installs the packaged scripts, and
creates a `zuzu` command wrapper. Cargo only installs the binaries from this
crate, so a cargo installation needs one extra step.

## Quick install

From a checkout of this repository:

```sh
git submodule update --init stdlib
cargo install --path .
./post-install.sh
```

The helper script uses the same layout that the runtime expects for a cargo
install. For a normal user installation this means:

- binaries and command links in `~/.cargo/bin`;
- modules in `~/.cargo/share/zuzu-rust/modules`;
- packaged scripts in `~/.cargo/share/zuzu-rust/scripts`.

The `zuzu-rust` runtime looks for installed modules below the prefix that
contains the running binary. For example, if the executable is
`~/.cargo/bin/zuzu-rust`, the installed standard library is expected at
`~/.cargo/share/zuzu-rust/modules`.

## What the helper installs

`post-install.sh` does the following:

1. copies `stdlib/modules` to `PREFIX/share/zuzu-rust/modules`;
2. copies executable files from `stdlib/scripts` to
   `PREFIX/share/zuzu-rust/scripts`;
3. creates a `zuzu` wrapper in the cargo binary directory;
4. creates command links in the cargo binary directory for the packaged
   standard-library scripts.

The wrapper honours `ZUZU` when it is set. Otherwise it searches `PATH` for
`zuzu-rust`, `zuzu.pl`, and `zuzu-js`, in that order.

## Custom locations

Use `--bindir` when cargo installed the binaries somewhere other than the
usual cargo binary directory:

```sh
./post-install.sh --bindir /opt/zuzu/bin
```

Use `--prefix` to keep the conventional `bin` and `share` layout under a
specific installation prefix:

```sh
./post-install.sh --prefix /opt/zuzu --bindir /opt/zuzu/bin
```

Use `--share-dir` if you need a custom share directory:

```sh
./post-install.sh \
  --bindir /opt/zuzu/bin \
  --share-dir /opt/zuzu/share/zuzu-rust
```

If you use a non-standard share directory that is not below the executable's
prefix, set `ZUZU_STDLIB` when running ZuzuScript programs:

```sh
export ZUZU_STDLIB=/opt/zuzu/share/zuzu-rust/modules
```

## Existing commands

The helper does not replace an existing `zuzu` command or existing packaged
script commands by default. To replace them, use:

```sh
./post-install.sh --force
```

If you want the packaged scripts copied but do not want command links for them,
use:

```sh
./post-install.sh --no-script-links
```

## Updating after reinstalling with cargo

Run the helper again after updating the source checkout or reinstalling the
crate with cargo:

```sh
git submodule update --init stdlib
cargo install --path . --force
./post-install.sh --force
```

## Troubleshooting

If imports from the standard library fail, check that the module directory
exists:

```sh
test -d "$(dirname "$(dirname "$(command -v zuzu-rust)")")/share/zuzu-rust/modules"
```

If the directory exists but you intentionally installed the standard library in
a custom location, set `ZUZU_STDLIB` to the directory containing the standard
library modules.
