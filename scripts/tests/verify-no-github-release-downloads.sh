#!/usr/bin/env bash
# Guard: build-path Dockerfiles must not download from GitHub releases.
#
# The GitHub incident of 2026-11-09 returned HTTP 504 from
# github.com/<owner>/<repo>/releases for anonymous and authenticated requests
# alike and broke the scheduler/gateway image build. Build-time dependencies now
# come from the OCI reserve image published by
# deploy/cloudbuild/agentenv-build-deps.cloudbuild.yaml, which is the only place
# in this repository allowed to download from GitHub releases.
#
# The critical list is explicit on purpose: a new deploy/docker/Dockerfile.*
# must be classified here as critical or excluded with a reason, or this guard
# fails. deploy/cloudbuild/ is out of the scan by design, because the reserve
# builder is the one place that downloads.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

critical=(
    deploy/docker/Dockerfile.scheduler
    deploy/docker/Dockerfile.gateway
)

# Excluded on purpose: the download happens at setup time (`server --setup-only`
# reads config/deps_manifest.toml), not as a URL in the Dockerfile, so a text
# scan cannot see it.
#
# Follow-up (not covered by this PR): move the docker-compose e2e to the reserve
# image. Prerequisites: a Workload Identity pool and provider for GitHub in
# rd-sre-stg-01 with read access to the agentenv repository (infrastructure
# change outside this PR), a keyless google-github-actions/auth step, the e2e
# consuming the reserve by digest (or a url override pointing to it), and only
# then .github/workflows/integration-tests.yml and
# deploy/docker/Dockerfile.agentenv join the critical list above.
excluded=(
    deploy/docker/Dockerfile.agentenv
)

release_pattern='github\.com/[^/[:space:]]+/[^/[:space:]]+/releases'

failures=0

# Completeness: every deploy/docker/Dockerfile.* is classified exactly once.
shopt -s nullglob
dockerfiles=(deploy/docker/Dockerfile.*)
shopt -u nullglob
if (( ${#dockerfiles[@]} == 0 )); then
    echo "error: no deploy/docker/Dockerfile.* found; run from the repository root" >&2
    exit 1
fi
for dockerfile in "${dockerfiles[@]}"; do
    classified=0
    for known in "${critical[@]}" "${excluded[@]}"; do
        if [[ "$dockerfile" == "$known" ]]; then
            classified=1
            break
        fi
    done
    if (( classified == 0 )); then
        echo "error: $dockerfile is neither critical nor explicitly excluded" >&2
        echo "  add it to critical[] or excluded[] in $0 with a reason" >&2
        failures=1
    fi
done

# Every critical file must exist.
for dockerfile in "${critical[@]}"; do
    if [[ ! -f "$dockerfile" ]]; then
        echo "error: critical file is missing: $dockerfile" >&2
        failures=1
    fi
done

# The assertion: no GitHub releases URL in an instruction of a critical file.
# Comment lines are ignored: they never download anything.
for dockerfile in "${critical[@]}"; do
    [[ -f "$dockerfile" ]] || continue
    raw_matches="$(grep -nHE "$release_pattern" "$dockerfile" || true)"
    matches="$(printf '%s\n' "$raw_matches" | grep -vE '^[^:]+:[0-9]+:[[:space:]]*#' || true)"
    if [[ -n "$matches" ]]; then
        echo "error: $dockerfile downloads from GitHub releases:" >&2
        printf '%s\n' "$matches" >&2
        failures=1
    fi
done

if (( failures != 0 )); then
    echo "verify-no-github-release-downloads: FAILED" >&2
    exit 1
fi
echo "verify-no-github-release-downloads: OK (${#critical[@]} critical, ${#excluded[@]} excluded)"
