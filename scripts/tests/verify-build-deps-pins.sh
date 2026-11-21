#!/usr/bin/env bash
# Drift check for the build-time dependency reserve (OPSAI-6696).
#
# deploy/docker/Dockerfile.scheduler, config/build_deps.lock.toml and
# config/deps_manifest.toml must agree, and the image digest must not be the
# placeholder. The check fails on the placeholder on purpose: until the release
# owner publishes the reserve and commits the digest it prints, the scheduler
# image build cannot pull the reserve and fails closed instead of falling back
# to GitHub releases.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
lock="${repo_root}/config/build_deps.lock.toml"
dockerfile="${repo_root}/deploy/docker/Dockerfile.scheduler"
manifest="${repo_root}/config/deps_manifest.toml"
cloudbuild="${repo_root}/deploy/cloudbuild/agentenv-build-deps.cloudbuild.yaml"

for file in "$lock" "$dockerfile" "$manifest" "$cloudbuild"; do
    if [[ ! -f "$file" ]]; then
        echo "error: missing $file" >&2
        exit 1
    fi
done

zero_digest="sha256:$(printf '0%.0s' {1..64})"
failures=0
fail() {
    echo "FAIL: $*" >&2
    failures=1
}

lock_value() {
    awk -v key="$1" '
        index($0, key " =") == 1 {
            value = $0
            sub(/^[^=]*=[[:space:]]*"/, "", value)
            sub(/".*/, "", value)
            print value
            exit
        }
    ' "$lock"
}

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
    ' "$lock"
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

dockerfile_value() {
    sed -n "s/^ARG $1=//p" "$dockerfile"
}

image_repository="$(lock_value image_repository)"
image_tag="$(lock_value image_tag)"
image_digest="$(lock_value image_digest)"

dockerfile_repository="$(dockerfile_value BUILD_DEPS_IMAGE)"
dockerfile_digest="$(dockerfile_value BUILD_DEPS_DIGEST)"
dockerfile_probe_sha="$(dockerfile_value GRPC_HEALTH_PROBE_SHA256)"

if [[ -z "$image_repository" ]]; then
    fail "config/build_deps.lock.toml has no image_repository"
elif [[ "$dockerfile_repository" != "$image_repository" ]]; then
    fail "Dockerfile BUILD_DEPS_IMAGE (${dockerfile_repository}) differs from the lock (${image_repository})"
else
    echo "ok: Dockerfile and lock name the same reserve repository"
fi

if [[ -z "$image_digest" ]]; then
    fail "config/build_deps.lock.toml has no image_digest"
elif [[ ! "$image_digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    fail "image_digest is not a sha256 digest: ${image_digest}"
elif [[ "$dockerfile_digest" != "$image_digest" ]]; then
    fail "Dockerfile BUILD_DEPS_DIGEST (${dockerfile_digest}) differs from the lock (${image_digest})"
else
    echo "ok: Dockerfile and lock pin the same reserve digest"
fi

grpc_sha="$(lock_sha grpc_health_probe)"
if [[ -z "$grpc_sha" ]]; then
    fail "the lock has no sha256 for grpc_health_probe"
elif [[ "$dockerfile_probe_sha" != "$grpc_sha" ]]; then
    fail "Dockerfile GRPC_HEALTH_PROBE_SHA256 (${dockerfile_probe_sha}) differs from the lock (${grpc_sha})"
else
    echo "ok: Dockerfile and lock pin the same grpc_health_probe digest"
fi

for name in firecracker kernel; do
    lock_digest="$(lock_sha "$name")"
    manifest_digest="$(manifest_sha "${name}.kvm")"
    if [[ -z "$lock_digest" ]]; then
        fail "the lock has no sha256 for ${name}"
    elif [[ "$lock_digest" != "$manifest_digest" ]]; then
        fail "${name} pin in the lock (${lock_digest}) differs from config/deps_manifest.toml (${manifest_digest})"
    else
        echo "ok: ${name} lock pin matches the manifest x86_64 pin"
    fi
done

if ! grep -Fq "_TAG: ${image_tag}" "$cloudbuild"; then
    fail "the Cloud Build _TAG does not match the lock image_tag (${image_tag})"
else
    echo "ok: the Cloud Build tag matches the lock image_tag"
fi

if ! grep -Fq 'COPY --from=reserve /grpc_health_probe' "$dockerfile"; then
    fail "the scheduler Dockerfile does not copy the probe from the reserve stage"
fi
if ! grep -Fq 'COPY --from=reserve-verify /grpc_health_probe' "$dockerfile"; then
    fail "the scheduler Dockerfile does not ship the verified probe"
fi

if [[ "$image_digest" == "$zero_digest" ]]; then
    cat >&2 <<EOF
error: image_digest is still the placeholder in config/build_deps.lock.toml.
Publish the reserve once and commit the digest it prints:
  gcloud builds submit --project rd-sre-stg-01 --config deploy/cloudbuild/agentenv-build-deps.cloudbuild.yaml --substitutions _TAG=${image_tag} .
  gcloud artifacts docker images describe ${image_repository}:${image_tag} --format='get(image_summary.digest)'
Then set image_digest here and BUILD_DEPS_DIGEST in deploy/docker/Dockerfile.scheduler.
EOF
    failures=1
fi

if (( failures != 0 )); then
    echo "verify-build-deps-pins: FAILED" >&2
    exit 1
fi
echo "verify-build-deps-pins: OK"
