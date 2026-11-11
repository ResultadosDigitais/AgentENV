#!/usr/bin/env bash
# Static check for the fork CI authentication of the build-time dependency
# reserve (OPSAI-6696).
#
# The scheduler image resolves build-deps from a private Artifact Registry
# repository, so the workflows that pull it must federate through Workload
# Identity instead of using a key or a secret. This suite asserts the wiring
# without running anything: the provider and service account, the action pinned
# by commit SHA, the id-token permission, the Docker credential configuration
# before the first pull, and the absence of the insecure escape hatches.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
services_ci="${repo_root}/.github/workflows/services-ci.yml"
integration="${repo_root}/.github/workflows/integration-tests.yml"

provider="projects/627931310875/locations/global/workloadIdentityPools/pf-gha-agentenv-fork/providers/services-ci"
service_account="gha-agentenv-fork-ci@rd-sre-stg-01.iam.gserviceaccount.com"
registry="us-central1-docker.pkg.dev"

failures=0
fail() {
    echo "FAIL: $*" >&2
    failures=1
}

for file in "$services_ci" "$integration"; do
    if [[ ! -f "$file" ]]; then
        echo "error: missing $file" >&2
        exit 1
    fi
done

for file in "$services_ci" "$integration"; do
    name="$(basename "$file")"

    # Federated identity: the exact provider and service account, no indirection.
    grep -Fq "workload_identity_provider: ${provider}" "$file" ||
        fail "${name} does not use the pf-gha-agentenv-fork workload identity provider"
    grep -Fq "service_account: ${service_account}" "$file" ||
        fail "${name} does not use the gha-agentenv-fork-ci service account"

    # The action is pinned by commit SHA, never by a moving tag.
    grep -Eq 'google-github-actions/auth@[0-9a-f]{40}([[:space:]]|$)' "$file" ||
        fail "${name} does not pin google-github-actions/auth by commit SHA"
    if grep -Eq 'google-github-actions/auth@v?[0-9]' "$file"; then
        fail "${name} pins google-github-actions/auth by a tag instead of a SHA"
    fi

    # The job may mint an OIDC token and still only read the repository.
    grep -Eq '^[[:space:]]+id-token: write$' "$file" ||
        fail "${name} does not grant id-token: write"
    grep -Eq '^[[:space:]]+contents: read$' "$file" ||
        fail "${name} does not keep contents: read"

    # Docker is pointed at the registry before any image is pulled.
    grep -Fq "gcloud auth configure-docker ${registry} --quiet" "$file" ||
        fail "${name} does not configure Docker for ${registry}"

    # No long-lived key, no secret and no pull_request_target.
    if grep -Fq 'credentials_json' "$file"; then
        fail "${name} carries a JSON credential"
    fi
    if grep -Eq '^[[:space:]]+pull_request_target:' "$file"; then
        fail "${name} uses pull_request_target, which runs with a write token"
    fi
done

# Ordering: the auth step must come before the first pull in each workflow.
auth_line_in() {
    grep -n 'google-github-actions/auth@' "$1" | head -n 1 | cut -d: -f1
}
first_line_in() {
    grep -n -m 1 -E "$2" "$1" | head -n 1 | cut -d: -f1
}

encoded_line="$(auth_line_in "$services_ci")"
pull_line="$(first_line_in "$services_ci" 'docker pull')"
if [[ -z "$encoded_line" || -z "$pull_line" || "$encoded_line" -ge "$pull_line" ]]; then
    fail "services-ci.yml does not authenticate before the reserve image pull"
fi

encoded_line="$(auth_line_in "$integration")"
build_line="$(first_line_in "$integration" 'docker/build-push-action')"
if [[ -z "$encoded_line" || -z "$build_line" || "$encoded_line" -ge "$build_line" ]]; then
    fail "integration-tests.yml does not authenticate before the first image build"
fi

if (( failures != 0 )); then
    echo "verify-build-deps-ci-auth: FAILED" >&2
    exit 1
fi
echo "verify-build-deps-ci-auth: OK"
