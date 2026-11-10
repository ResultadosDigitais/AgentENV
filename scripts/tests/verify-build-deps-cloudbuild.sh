#!/usr/bin/env bash
# Static check for the build-time dependency reserve (OPSAI-6696).
#
# The Cloud Build config is only executed when the release owner submits it, so
# this suite asserts its structure without running anything: the destination
# repository, the linux/amd64 platform, the three staged files, the lock
# reference and the absence of any multi-architecture switch.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
config="${repo_root}/deploy/cloudbuild/agentenv-build-deps.cloudbuild.yaml"
dockerfile="${repo_root}/deploy/cloudbuild/Dockerfile.build-deps"

failures=0
fail() {
    echo "FAIL: $*" >&2
    failures=1
}

if [[ ! -f "$config" ]]; then
    echo "error: missing $config" >&2
    exit 1
fi
if [[ ! -f "$dockerfile" ]]; then
    echo "error: missing $dockerfile" >&2
    exit 1
fi

# Structural YAML sanity: no tabs in indentation, expected top-level keys.
if grep -n $'\t' "$config" >/dev/null; then
    fail "$config contains tabs; Cloud Build YAML needs spaces"
fi
for key in substitutions steps; do
    if ! grep -Eq "^${key}:$" "$config"; then
        fail "$config has no top-level ${key}: key"
    fi
done

# Destination repository and tag are pinned.
grep -Eq '^[[:space:]]+_IMAGE:[[:space:]]+us-central1-docker\.pkg\.dev/rd-sre-stg-01/agentenv/agentenv-build-deps$' "$config" ||
    fail "_IMAGE does not name the rd-sre-stg-01/agentenv reserve repository"
grep -Eq '^[[:space:]]+_TAG:[[:space:]]+v0\.4\.46-1\.15\.1-patch-v1$' "$config" ||
    fail "_TAG does not match the pinned release"

# The config is the one authorized downloader, and it runs the staging script.
grep -Fq 'scripts/deps/stage_build_deps.sh' "$config" ||
    fail "the staging script is not invoked"
grep -Fq 'config/build_deps.lock.toml' "$config" ||
    fail "the lock file is not referenced"

# Single-architecture build: linux/amd64, no multi-architecture switch.
grep -Fq -- '--platform linux/amd64' "$config" ||
    fail "the image build does not pin --platform linux/amd64"
grep -Fq 'deploy/cloudbuild/Dockerfile.build-deps' "$config" ||
    fail "the image build does not use deploy/cloudbuild/Dockerfile.build-deps"
if grep -Eqi 'arm64|aarch64|TARGETARCH|buildx|--manifest|multi-arch' "$config"; then
    fail "the config has a multi-architecture switch; the reserve is amd64 only"
fi

# The run publishes the tag and logs the digest plus every file digest.
grep -Fq '${_IMAGE}:${_TAG}' "$config" ||
    fail "the config does not publish \${_IMAGE}:\${_TAG}"
grep -Fq 'gcloud artifacts docker images describe' "$config" ||
    fail "the config does not log the published digest"
grep -Fq 'sha256sum' "$config" ||
    fail "the config does not log the sha256 of each staged file"
grep -Fq 'gcloud builds submit --project rd-sre-stg-01 --config deploy/cloudbuild/agentenv-build-deps.cloudbuild.yaml --substitutions _TAG=v0.4.46-1.15.1-patch-v1 .' "$config" ||
    fail "the header does not carry the exact owner command on one line"

# The reserve image holds exactly the three staged artifacts.
grep -Fqx 'FROM scratch' "$dockerfile" ||
    fail "the reserve Dockerfile is not FROM scratch"
for name in grpc_health_probe firecracker vmlinux; do
    if ! grep -Fqx "COPY ${name} /${name}" "$dockerfile"; then
        fail "the reserve Dockerfile does not copy ${name} to /${name}"
    fi
done
if grep -Eqi 'arm64|aarch64|TARGETARCH' "$dockerfile"; then
    fail "the reserve Dockerfile has an arm64 switch"
fi

if (( failures != 0 )); then
    echo "verify-build-deps-cloudbuild: FAILED" >&2
    exit 1
fi
echo "verify-build-deps-cloudbuild: OK"
