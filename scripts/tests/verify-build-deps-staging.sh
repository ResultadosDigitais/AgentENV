#!/usr/bin/env bash
# Tests for scripts/deps/stage_build_deps.sh (OPSAI-6696).
#
# Fixtures are served over file:// so the suite needs no network. It covers:
#   1. every file installs only after its pinned sha256 matches;
#   2. a divergent digest publishes nothing and leaves no temporary file;
#   3. a malformed pin fails before any download;
#   4. the lock's firecracker/kernel digests equal the x86_64 pins of
#      config/deps_manifest.toml, so the reserve ships the same bytes the
#      runtime installer verifies today;
#   5. the lock declares only linux/amd64 entries.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
stage_script="${repo_root}/scripts/deps/stage_build_deps.sh"
lock_file="${repo_root}/config/build_deps.lock.toml"
manifest="${repo_root}/config/deps_manifest.toml"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

failures=0
fail() {
    echo "FAIL: $*" >&2
    failures=1
}
pass() {
    echo "ok: $*"
}

if [[ ! -f "$stage_script" ]]; then
    echo "error: missing $stage_script" >&2
    exit 1
fi
if [[ ! -f "$lock_file" ]]; then
    echo "error: missing $lock_file" >&2
    exit 1
fi
if [[ ! -f "$manifest" ]]; then
    echo "error: missing $manifest" >&2
    exit 1
fi
bash -n "$stage_script"

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

fixtures="${tmp_dir}/fixtures"
mkdir -p "$fixtures"
printf 'fixture grpc health probe\n' > "${fixtures}/grpc_health_probe"
printf 'fixture firecracker tarball\n' > "${fixtures}/firecracker"
printf 'fixture kernel image\n' > "${fixtures}/vmlinux"
grpc_sha="$(sha256_of "${fixtures}/grpc_health_probe")"
firecracker_sha="$(sha256_of "${fixtures}/firecracker")"
kernel_sha="$(sha256_of "${fixtures}/vmlinux")"

write_lock() {
    local path="$1" grpc="$2" firecracker="$3" kernel="$4" grpc_url="$5"
    cat > "$path" <<EOF
image_repository = "us-central1-docker.pkg.dev/rd-sre-stg-01/agentenv/agentenv-build-deps"
image_tag = "v0.4.46-1.15.1-patch-v1"
image_digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

[[file]]
name = "grpc_health_probe"
dest = "grpc_health_probe"
platform = "linux/amd64"
url = "${grpc_url}"
sha256 = "${grpc}"

[[file]]
name = "firecracker"
dest = "firecracker"
platform = "linux/amd64"
url = "file://${fixtures}/firecracker"
sha256 = "${firecracker}"

[[file]]
name = "kernel"
dest = "vmlinux"
platform = "linux/amd64"
url = "file://${fixtures}/vmlinux"
sha256 = "${kernel}"
EOF
}

# 1. Matching digests install every file and leave no temporary behind.
out="${tmp_dir}/out-ok"
write_lock "${tmp_dir}/lock-ok.toml" "$grpc_sha" "$firecracker_sha" "$kernel_sha" "file://${fixtures}/grpc_health_probe"
if bash "$stage_script" --lock "${tmp_dir}/lock-ok.toml" --dest "$out" >"${tmp_dir}/ok.log" 2>&1; then
    pass "staging succeeds with matching digests"
else
    cat "${tmp_dir}/ok.log" >&2
    fail "staging failed with matching digests"
fi
for name in grpc_health_probe firecracker vmlinux; do
    if [[ ! -f "${out}/${name}" ]]; then
        fail "missing ${name} after a successful staging run"
    elif [[ "$(cat "${out}/${name}")" != "$(cat "${fixtures}/${name}")" ]]; then
        fail "${name} content differs from the fixture"
    elif [[ ! -x "${out}/${name}" ]]; then
        fail "${name} is not executable"
    fi
done
if [[ -n "$(find "$tmp_dir" -name '*.tmp' -print -quit)" ]]; then
    fail "staging left a .tmp file after a successful run"
fi

# 2. A divergent digest aborts and publishes nothing.
out="${tmp_dir}/out-bad"
write_lock "${tmp_dir}/lock-bad.toml" "$(printf '0%.0s' {1..64})" "$firecracker_sha" "$kernel_sha" "file://${fixtures}/grpc_health_probe"
if bash "$stage_script" --lock "${tmp_dir}/lock-bad.toml" --dest "$out" >"${tmp_dir}/bad.log" 2>&1; then
    fail "staging accepted a divergent digest"
else
    pass "staging rejects a divergent digest"
fi
grep -q 'sha256 mismatch' "${tmp_dir}/bad.log" || fail "divergent digest error does not mention sha256 mismatch"
if [[ -d "$out" ]] && [[ -n "$(ls -A "$out")" ]]; then
    fail "staging published files after a divergent digest"
fi
if [[ -n "$(find "$tmp_dir" -name '*.tmp' -print -quit)" ]]; then
    fail "staging left a .tmp file after a divergent digest"
fi

# 3. A malformed pin fails before any request.
out="${tmp_dir}/out-pin"
write_lock "${tmp_dir}/lock-pin.toml" "not-a-sha256" "$firecracker_sha" "$kernel_sha" "http://127.0.0.1:1/never"
if bash "$stage_script" --lock "${tmp_dir}/lock-pin.toml" --dest "$out" >"${tmp_dir}/pin.log" 2>&1; then
    fail "staging accepted a malformed pin"
else
    pass "staging rejects a malformed pin"
fi
grep -q 'invalid sha256' "${tmp_dir}/pin.log" || fail "malformed pin error does not mention invalid sha256"
if grep -q '127.0.0.1' "${tmp_dir}/pin.log"; then
    fail "malformed pin was detected only after a request"
fi
if [[ -d "$out" ]] && [[ -n "$(ls -A "$out")" ]]; then
    fail "staging published files after a malformed pin"
fi

# 4. The reserve ships the same bytes the runtime installer verifies today.
lock_sha() {
    awk -v want="$1" '
        function reset() { name = ""; sha = "" }
        /^\[\[file\]\]/ {
            if (in_file && name == want) { print sha; found = 1; exit }
            reset()
            in_file = 1
            next
        }
        in_file && /^name[[:space:]]*=/ {
            name = $0
            sub(/^name[[:space:]]*=[[:space:]]*"/, "", name)
            sub(/".*/, "", name)
            next
        }
        in_file && /^sha256[[:space:]]*=/ {
            sha = $0
            sub(/^sha256[[:space:]]*=[[:space:]]*"/, "", sha)
            sub(/".*/, "", sha)
            next
        }
        in_file && /^\[/ {
            if (name == want) { print sha; found = 1; exit }
            in_file = 0
        }
        END {
            if (!found && in_file && name == want) print sha
        }
    ' "$lock_file"
}

manifest_sha() {
    awk -v section="$1" '
        $0 == "[" section "]" { inside = 1; next }
        inside && /^\[/ { inside = 0 }
        inside && /x86_64[[:space:]]*=/ {
            line = $0
            sub(/.*x86_64[[:space:]]*=[[:space:]]*"/, "", line)
            sub(/".*/, "", line)
            print line
            exit
        }
    ' "$manifest"
}

for name in firecracker kernel; do
    lock_digest="$(lock_sha "$name")"
    manifest_digest="$(manifest_sha "${name}.kvm")"
    if [[ -z "$lock_digest" ]]; then
        fail "lock has no sha256 for ${name}"
    elif [[ "$lock_digest" != "$manifest_digest" ]]; then
        fail "${name} pin in the lock (${lock_digest}) differs from the x86_64 pin in config/deps_manifest.toml (${manifest_digest})"
        echo "  the reserve must ship the same bytes; re-measure both pins together after an upstream republish" >&2
    else
        pass "${name} lock pin matches the manifest x86_64 pin"
    fi
done

# 5. The reserve is linux/amd64 only.
if grep -Eqi 'arm64|aarch64' "$lock_file"; then
    fail "the lock declares a non-amd64 entry"
else
    pass "the lock has no arm64/aarch64 entry"
fi
entries="$(grep -c '^\[\[file\]\]$' "$lock_file")"
amd64="$(grep -c '^platform = "linux/amd64"$' "$lock_file")"
if [[ "$entries" != "$amd64" ]]; then
    fail "${entries} file entries but only ${amd64} linux/amd64 platform lines"
else
    pass "all ${entries} lock entries are linux/amd64"
fi
for name in grpc_health_probe firecracker kernel; do
    if ! grep -Fqx "name = \"${name}\"" "$lock_file"; then
        fail "lock has no entry for ${name}"
    fi
done

if (( failures != 0 )); then
    echo "verify-build-deps-staging: FAILED" >&2
    exit 1
fi
echo "verify-build-deps-staging: OK"
