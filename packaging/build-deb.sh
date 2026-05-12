#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PACKAGE=zuzu-rust
TARGET_DIR="$ROOT/target/debian"
WORK_DIR="$TARGET_DIR/work"
STAGE_DIR="$WORK_DIR/${PACKAGE}"
DEBIAN_DIR="$STAGE_DIR/DEBIAN"
VERSION=${DEB_VERSION:-$(awk '
  /^\[package\]/ { in_package = 1; next }
  /^\[/ { in_package = 0 }
  in_package && /^version[[:space:]]*=/ {
    gsub(/"/, "", $3);
    print $3;
    exit
  }
' "$ROOT/Cargo.toml")}
ARCH=${DEB_ARCH:-$(dpkg --print-architecture)}
MAINTAINER=${DEB_MAINTAINER:-Toby Inkster <zuzu@toby.ink>}
BUILD_NICE=${BUILD_NICE:-10}
BINS=(
  zuzu-rust
  zuzu-rust-parse-files
  zuzu-rust-run-tests
  zuzu-rust-server
)

require_tool() {
  local tool=$1
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: required tool not found: $tool" >&2
    exit 2
  fi
}

copy_tree_contents() {
  local source=$1
  local dest=$2
  mkdir -p "$dest"
  if [ -d "$source" ]; then
    cp -a "$source"/. "$dest"/
  fi
}

install_binary() {
  local name=$1
  install -D -m 0755 "$ROOT/target/release/$name" "$STAGE_DIR/usr/bin/$name"
}

generate_manpage() {
  local source=$1
  local name=$2
  local out_dir="$STAGE_DIR/usr/share/man/man1"
  mkdir -p "$out_dir"
  pod2man \
    --section=1 \
    --center="ZuzuScript" \
    --release="zuzu-rust $VERSION" \
    --name="$name" \
    "$source" \
    "$out_dir/$name.1"
  gzip -n -f "$out_dir/$name.1"
}

script_has_pod() {
  local source=$1
  grep -Eq '^=(pod|head[0-9]|over|item|begin|for|encoding)\b' "$source"
}

shared_library_depends() {
  local deps_file="$WORK_DIR/shlibdeps"
  mkdir -p "$WORK_DIR/debian"
  cat >"$WORK_DIR/debian/control" <<CONTROL
Source: $PACKAGE
Section: interpreters
Priority: optional
Maintainer: $MAINTAINER
Standards-Version: 4.6.2

Package: $PACKAGE
Architecture: $ARCH
Description: Rust implementation of ZuzuScript
CONTROL

  (
    cd "$WORK_DIR"
    dpkg-shlibdeps -O"${deps_file}" "${BINS[@]/#/$STAGE_DIR/usr/bin/}"
  )

  if [ -f "$deps_file" ]; then
    sed -n 's/^shlibs:Depends=//p' "$deps_file"
  fi
}

write_control_file() {
  local depends=$1
  local installed_size
  installed_size=$(du -sk "$STAGE_DIR" | awk '{ print $1 }')

  mkdir -p "$DEBIAN_DIR"
  {
    echo "Package: $PACKAGE"
    echo "Version: $VERSION"
    echo "Section: interpreters"
    echo "Priority: optional"
    echo "Architecture: $ARCH"
    echo "Maintainer: $MAINTAINER"
    if [ -n "$depends" ]; then
      echo "Depends: $depends"
    fi
    echo "Installed-Size: $installed_size"
    echo "Description: Rust implementation of ZuzuScript"
    echo " ZuzuScript is a scripting language with a shared standard library."
    echo " This package provides the Rust runtime, developer helper commands,"
    echo " standard modules, and packaged ZuzuScript command-line tools."
  } >"$DEBIAN_DIR/control"
}

main() {
  require_tool cargo
  require_tool dpkg
  require_tool dpkg-deb
  require_tool dpkg-shlibdeps
  require_tool gzip
  require_tool install
  require_tool pod2man

  rm -rf "$WORK_DIR"
  mkdir -p "$DEBIAN_DIR"

  if command -v nice >/dev/null 2>&1; then
    nice -n "$BUILD_NICE" cargo build --release \
      --bin zuzu-rust \
      --bin zuzu-rust-parse-files \
      --bin zuzu-rust-run-tests \
      --bin zuzu-rust-server
  else
    cargo build --release \
      --bin zuzu-rust \
      --bin zuzu-rust-parse-files \
      --bin zuzu-rust-run-tests \
      --bin zuzu-rust-server
  fi

  for bin in "${BINS[@]}"; do
    install_binary "$bin"
  done

  copy_tree_contents "$ROOT/stdlib/modules" "$STAGE_DIR/usr/share/zuzu-rust/modules"

  mkdir -p "$STAGE_DIR/usr/share/zuzu-rust/scripts"
  if [ -d "$ROOT/stdlib/scripts" ]; then
    while IFS= read -r -d '' script; do
      install -m 0755 "$script" "$STAGE_DIR/usr/share/zuzu-rust/scripts/$(basename "$script")"
    done < <(find "$ROOT/stdlib/scripts" -maxdepth 1 -type f -print0 | sort -z)
  fi

  install -D -m 0755 "$ROOT/packaging/zuzu-wrapper" \
    "$STAGE_DIR/usr/lib/zuzu-rust/zuzu-wrapper"
  install -D -m 0755 "$ROOT/packaging/debian/postinst" "$DEBIAN_DIR/postinst"
  install -D -m 0755 "$ROOT/packaging/debian/prerm" "$DEBIAN_DIR/prerm"

  for pod in "$ROOT"/packaging/man/*.pod; do
    [ -e "$pod" ] || continue
    generate_manpage "$pod" "$(basename "$pod" .pod)"
  done

  if [ -d "$ROOT/stdlib/scripts" ]; then
    while IFS= read -r -d '' script; do
      if script_has_pod "$script"; then
        generate_manpage "$script" "$(basename "$script")"
      fi
    done < <(find "$ROOT/stdlib/scripts" -maxdepth 1 -type f -print0 | sort -z)
  fi

  local depends
  depends=$(shared_library_depends)
  write_control_file "$depends"

  mkdir -p "$TARGET_DIR"
  local deb="$TARGET_DIR/${PACKAGE}_${VERSION}_${ARCH}.deb"
  rm -f "$deb"
  dpkg-deb --build --root-owner-group "$STAGE_DIR" "$deb"
  echo "$deb"
}

main "$@"
