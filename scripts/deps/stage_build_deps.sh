#!/usr/bin/env bash
# Stage the build-time dependency reserve (OPSAI-6696).
#
# Downloads every [[file]] entry of config/build_deps.lock.toml once, verifies
# its sha256 against the committed pin, and publishes the files into --dest only
# after every entry verifies. A transport error, a divergent digest or a
# malformed pin aborts with a non-zero status and leaves neither a published
# file nor a temporary download behind.
#
# The Cloud Build config deploy/cloudbuild/agentenv-build-deps.cloudbuild.yaml
# runs this script; it is the only place in the repository allowed to download
# from GitHub releases.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
lock_file="${repo_root}/config/build_deps.lock.toml"
dest_dir="${repo_root}/deploy/cloudbuild/build-deps"

usage() {
    cat <<'EOF'
usage: stage_build_deps.sh [--lock FILE] [--dest DIR]

  --lock FILE  lock file to read (default: config/build_deps.lock.toml)
  --dest DIR   directory that receives the verified files
               (default: deploy/cloudbuild/build-deps)
EOF
}

while (( $# > 0 )); do
    case "$1" in
        --lock)
            lock_file="${2:?--lock needs a path}"
            shift 2
            ;;
        --dest)
            dest_dir="${2:?--dest needs a path}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ ! -f "$lock_file" ]]; then
    echo "error: lock file not found: $lock_file" >&2
    exit 1
fi

sha256_hex() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | cut -d' ' -f1
    else
        echo "error: neither sha256sum nor shasum is available" >&2
        return 1
    fi
}

# Parse the [[file]] blocks into name<TAB>url<TAB>sha256<TAB>dest records.
entries="$(
    awk '
        function trim(value) {
            sub(/^[ \t]+/, "", value)
            sub(/[ \t]+$/, "", value)
            return value
        }
        function unquote(value) {
            value = trim(value)
            if (value ~ /^".*"$/) {
                value = substr(value, 2, length(value) - 2)
            }
            return value
        }
        function emit() {
            if (in_file) {
                printf "%s\t%s\t%s\t%s\n", name, url, sha256, dest
            }
        }
        /^[ \t]*\[\[file\]\][ \t]*$/ {
            emit()
            in_file = 1
            name = ""
            url = ""
            sha256 = ""
            dest = ""
            next
        }
        in_file && /^[ \t]*[A-Za-z_][A-Za-z0-9_]*[ \t]*=/ {
            key = $0
            sub(/[ \t]*=.*/, "", key)
            key = trim(key)
            value = $0
            sub(/^[^=]*=/, "", value)
            value = unquote(value)
            if (key == "name") {
                name = value
            } else if (key == "url") {
                url = value
            } else if (key == "sha256") {
                sha256 = value
            } else if (key == "dest") {
                dest = value
            }
            next
        }
        END {
            emit()
        }
    ' "$lock_file"
)"

if [[ -z "$entries" ]]; then
    echo "error: no [[file]] entries in $lock_file" >&2
    exit 1
fi

# Validate every entry before any request: a malformed pin must never be able
# to download.
seen_dests=$'\n'
while IFS=$'\t' read -r name url sha256 dest; do
    [[ -n "$name" ]] || continue
    if [[ -z "$url" || -z "$sha256" || -z "$dest" ]]; then
        echo "error: entry '${name}' needs name, url, sha256 and dest" >&2
        exit 1
    fi
    if [[ ! "$sha256" =~ ^[0-9a-fA-F]{64}$ ]]; then
        echo "error: invalid sha256 pin for '${name}': ${sha256} (expected 64 hexadecimal characters)" >&2
        exit 1
    fi
    if [[ ! "$dest" =~ ^[A-Za-z0-9._-]+$ || "$dest" == "." || "$dest" == ".." ]]; then
        echo "error: invalid dest for '${name}': ${dest} (expected a plain file name)" >&2
        exit 1
    fi
    if [[ "$seen_dests" == *$'\n'"${dest}"$'\n'* ]]; then
        echo "error: dest '${dest}' is declared twice" >&2
        exit 1
    fi
    seen_dests+="${dest}"$'\n'
done <<< "$entries"

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

# Download and verify everything before publishing anything.
staged="${work_dir}/staged"
: > "$staged"
while IFS=$'\t' read -r name url sha256 dest; do
    [[ -n "$name" ]] || continue
    tmp="${work_dir}/${dest}.tmp"
    echo "staging ${name} from ${url}"
    if ! curl --fail --location --silent --show-error \
        --retry 5 --retry-all-errors --output "$tmp" "$url"; then
        echo "error: failed to download '${name}' from ${url}" >&2
        exit 1
    fi
    actual="$(sha256_hex "$tmp")"
    actual_lower="$(printf '%s' "$actual" | tr '[:upper:]' '[:lower:]')"
    expected_lower="$(printf '%s' "$sha256" | tr '[:upper:]' '[:lower:]')"
    if [[ "$actual_lower" != "$expected_lower" ]]; then
        echo "error: sha256 mismatch for '${name}': expected ${expected_lower}, got ${actual_lower}" >&2
        exit 1
    fi
    printf '%s\t%s\n' "$dest" "$tmp" >> "$staged"
done <<< "$entries"

mkdir -p "$dest_dir"
while IFS=$'\t' read -r dest tmp; do
    install -m 0755 "$tmp" "${dest_dir}/${dest}"
    echo "published ${dest_dir}/${dest}"
done < "$staged"

echo "staged $(wc -l < "$staged") files into ${dest_dir}"
