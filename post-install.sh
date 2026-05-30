#!/bin/sh
set -eu

usage() {
	cat <<'USAGE'
Usage: ./post-install.sh [options]

Install ZuzuScript standard-library files after installing zuzu-rust with
cargo. Run this script from the zuzu-rust source checkout which contains the
stdlib submodule.

Options:
  --source-root DIR    Source checkout root (default: directory of this script)
  --bindir DIR         Directory containing zuzu-rust (default: cargo bin dir)
  --prefix DIR         Installation prefix (default: parent of --bindir)
  --share-dir DIR      Destination for share files (default: PREFIX/share/zuzu-rust)
  --force              Replace an existing zuzu wrapper and script links
  --no-script-links    Copy stdlib scripts, but do not link them into --bindir
  -h, --help           Show this help

Environment:
  CARGO_INSTALL_ROOT   Used to find the cargo bin directory when set
  CARGO_HOME           Used to find the cargo bin directory when set
USAGE
}

script_dir() {
	case $0 in
		*/*)
			cd "$(dirname "$0")" && pwd -P
			;;
		*)
			pwd -P
			;;
	esac
}

find_cargo_bindir() {
	if [ "${CARGO_INSTALL_ROOT+x}" = x ] && [ -n "$CARGO_INSTALL_ROOT" ]; then
		printf '%s\n' "$CARGO_INSTALL_ROOT/bin"
		return
	fi

	if [ "${CARGO_HOME+x}" = x ] && [ -n "$CARGO_HOME" ]; then
		printf '%s\n' "$CARGO_HOME/bin"
		return
	fi

	if command -v zuzu-rust >/dev/null 2>&1; then
		dir=$(dirname "$(command -v zuzu-rust)")
		cd "$dir" && pwd -P
		return
	fi

	printf '%s\n' "$HOME/.cargo/bin"
}

copy_tree_contents() {
	source_dir=$1
	dest_dir=$2

	mkdir -p "$dest_dir"
	if [ -d "$source_dir" ]; then
		cp -R "$source_dir"/. "$dest_dir"/
	fi
}

install_wrapper() {
	dest=$1
	tmp=$dest.tmp.$$

	cat >"$tmp" <<'WRAPPER'
#!/bin/sh
set -eu

if [ "${ZUZU+x}" = x ] && [ -n "$ZUZU" ]; then
	export ZUZU
	exec "$ZUZU" "$@"
fi

for ZUZU in zuzu-rust zuzu.pl zuzu-js; do
	if command -v "$ZUZU" >/dev/null 2>&1; then
		export ZUZU
		exec "$ZUZU" "$@"
	fi
done

echo "zuzu: no ZuzuScript implementation found in PATH" >&2
exit 127

# installed via zuzu-rust post-install.sh
WRAPPER
	chmod 0755 "$tmp"
	mv "$tmp" "$dest"
}

source_root=$(script_dir)
bindir=
prefix=
share_dir=
force=0
script_links=1

while [ "$#" -gt 0 ]; do
	case $1 in
		--source-root)
			shift
			[ "$#" -gt 0 ] || { echo "post-install.sh: --source-root requires DIR" >&2; exit 2; }
			source_root=$1
			;;
		--bindir)
			shift
			[ "$#" -gt 0 ] || { echo "post-install.sh: --bindir requires DIR" >&2; exit 2; }
			bindir=$1
			;;
		--prefix)
			shift
			[ "$#" -gt 0 ] || { echo "post-install.sh: --prefix requires DIR" >&2; exit 2; }
			prefix=$1
			;;
		--share-dir)
			shift
			[ "$#" -gt 0 ] || { echo "post-install.sh: --share-dir requires DIR" >&2; exit 2; }
			share_dir=$1
			;;
		--force)
			force=1
			;;
		--no-script-links)
			script_links=0
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			echo "post-install.sh: unknown option: $1" >&2
			usage >&2
			exit 2
			;;
	esac
	shift
done

source_root=$(cd "$source_root" && pwd -P)

if [ -z "$bindir" ]; then
	bindir=$(find_cargo_bindir)
fi
mkdir -p "$bindir"
bindir=$(cd "$bindir" && pwd -P)

if [ -z "$prefix" ]; then
	if [ "$(basename "$bindir")" = bin ]; then
		prefix=$(dirname "$bindir")
	else
		prefix=$bindir
	fi
fi
mkdir -p "$prefix"
prefix=$(cd "$prefix" && pwd -P)

if [ -z "$share_dir" ]; then
	share_dir=$prefix/share/zuzu-rust
fi

modules_source=$source_root/stdlib/modules
scripts_source=$source_root/stdlib/scripts
modules_dest=$share_dir/modules
scripts_dest=$share_dir/scripts

if [ ! -d "$modules_source" ]; then
	cat >&2 <<EOF_ERROR
post-install.sh: cannot find $modules_source
Initialise or update the stdlib submodule, then run this script again:
  git submodule update --init stdlib
EOF_ERROR
	exit 1
fi

mkdir -p "$share_dir"
rm -rf "$modules_dest"
copy_tree_contents "$modules_source" "$modules_dest"

rm -rf "$scripts_dest"
mkdir -p "$scripts_dest"
if [ -d "$scripts_source" ]; then
	find "$scripts_source" -maxdepth 1 -type f | sort | while IFS= read -r script; do
		name=${script##*/}
		install -m 0755 "$script" "$scripts_dest/$name"
	done
fi

zuzu_dest=$bindir/zuzu
if [ ! -e "$zuzu_dest" ] || [ "$force" -eq 1 ]; then
	install_wrapper "$zuzu_dest"
else
	printf '%s\n' "Leaving existing $zuzu_dest in place; use --force to replace it."
fi

if [ "$script_links" -eq 1 ] && [ -d "$scripts_dest" ]; then
	find "$scripts_dest" -maxdepth 1 -type f | sort | while IFS= read -r script; do
		name=${script##*/}
		link=$bindir/$name
		if [ -e "$link" ] || [ -L "$link" ]; then
			if [ "$force" -eq 1 ] || { [ -L "$link" ] && [ "$(readlink "$link")" = "$script" ]; }; then
				rm -f "$link"
			else
				printf '%s\n' "Leaving existing $link in place; use --force to replace it."
				continue
			fi
		fi
		ln -s "$script" "$link"
	done
fi

cat <<EOF_DONE
Installed ZuzuScript support files:
  modules: $modules_dest
  scripts: $scripts_dest
  wrapper: $zuzu_dest

Make sure this directory is on PATH:
  $bindir
EOF_DONE
