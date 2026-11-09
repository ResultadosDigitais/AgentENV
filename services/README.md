# services

Go implementation of a distributed Gateway and pluggable Scheduler for AgentENV.

## Features

- Gateway routes control-plane requests by real-time scheduling.
- Gateway aggregates `GET /sandboxes` and `GET /v2/sandboxes` across all scheduler nodes.
- Gateway aggregates `GET /nodes` across all observed nodes in the scheduler.
- Gateway resolves `GET /nodes/{id}` via scheduler and proxies to the target node.
- Gateway routes sandbox requests by existing sandbox-to-node binding.
- Scheduler exposes gRPC API and supports pluggable strategy providers.
- Built-in strategies: round_robin, random and least_loaded.
- Scheduler supports both static node configuration and Kubernetes EndpointSlice discovery.
- Scheduler sandbox binding store can be in-memory or Redis-backed.
- Scheduler can run as a primary read/write service or as query-only replicas that serve only `LookupNode` from Redis.
- Scheduler observes node health and sandbox roster from heartbeats, and drops expired sandbox-to-node bindings on heartbeat, node unregistration, or lookup.
- HTTP and WebSocket forwarding.

## Header compatibility

Gateway treats these headers as sandbox-routing markers:

- x-agentenv-sandbox-id
- e2b-sandbox-id

If one of them exists, gateway resolves node from scheduler binding and forwards request there.

When `gateway.sandbox_proxy_domains` is configured, gateway also accepts host-based sandbox
data-plane URLs in the form `{port}-{sandboxID}.{proxy_domain}`. The host-derived
sandbox ID and port take precedence over conflicting routing headers; the gateway logs
that conflict at debug level and forwards the request to the backend node's `/proxy`
endpoint. Host-based routing requires the sandbox ID to be RFC 952/1123 DNS-label compatible
(`[a-z0-9]([a-z0-9-]*[a-z0-9])?`), and the full `{port}-{sandboxID}` label must fit
within the 63-character DNS label limit.

Sandbox data-plane routing is host- or header-based. Path-derived sandbox IDs are
only used for sandbox control-plane APIs such as `/sandboxes/{id}/pause`; clients
that proxy sandbox traffic through the gateway must use a sandbox proxy host or a
sandbox routing header.

## Build

Prerequisites:

- Go 1.21+

Commands (from `services/`):

```bash
make tidy
make proto
make build      # builds both gateway and scheduler
make test       # tests both services
```

Per-service (from `services/gateway/` or `services/scheduler/`):

```bash
make build
make test
```

## Run locally

Start scheduler:

```bash
make run-scheduler
```

Start gateway with the same API key configured on every AgentENV runtime node:

```bash
export AENV_API_KEY="e2b_$(openssl rand -hex 32)"
make run-gateway
```

The default local config uses `127.0.0.1:9090` for the scheduler.

The gateway and runtime nodes require the same API key for control-plane APIs.
The gateway reads `AENV_API_KEY` or `/run/secrets/api-key`; it does not generate
a key. The gateway routes data-plane requests without authenticating them
because only the owning runtime has the sandbox policy needed to distinguish
public ingress, private ingress, and secure envd. Private application proxy
requests use the sandbox response's `trafficAccessToken` in the
`e2b-traffic-access-token` header; secure envd requests use `X-Access-Token`.

## Scheduler configuration

Scheduler discovery modes:

- `static` (default): use `scheduler.nodes` from config.
- `kubernetes`: watch EndpointSlices for a headless Service and build the node list from serving Pod endpoints. Terminating endpoints, or Pods matching `no_schedule_pod_selector`, are kept as lingering/no-schedule nodes; Pods matching `ignore_pod_selector` are excluded.

General config notes:

- `scheduler.report_ttl` must be a duration string such as `"30s"` in JSON config files.
- `scheduler.binding_ttl` must be a duration string such as `"30s"` in JSON config files.
- `scheduler.report_ttl` controls how long an observed node heartbeat stays healthy.
- `scheduler.binding_ttl` controls how long sandbox-to-node bindings survive without a fresh `RecordAssignment` or heartbeat roster refresh.
- `scheduler.node_resource_limit.admission_reservation` (`"on"`/`"off"`, default `"on"`) reserves the shape of every admitted create and resume until the gateway settles it, so simultaneous decisions cannot both see the same free capacity. A failed settle releases it immediately; a confirmed one is held until the node reports a snapshot produced after the confirmation, so the gap before the next heartbeat cannot admit another create into the same space.
- `scheduler.node_resource_limit.admission_reservation_ttl` (duration, default `"90s"`) is the backstop that bounds how long a reservation keeps occupying node capacity. Set it to `gateway.request_timeout` plus 60s: the shipped configs use `"150s"` over the 90s gateway timeout, which covers the settlement RPC and a cold create with an image pull.
- `scheduler.redis_addr` selects Redis-backed sandbox binding storage when set; when empty, the scheduler uses the in-memory binding store. It accepts either `host:port` or a Redis URL such as `redis://[:password@]host:6379/db`.
- `--query-only` starts a read-only scheduler that supports only `LookupNode`; it requires `scheduler.redis_addr` and does not need node discovery config.
- `scheduler.artifact_store_capacity` controls how many distinct P2P artifact keys the in-memory artifact index keeps before LRU eviction; defaults to `1000000`.
- `scheduler.artifact_lookup_node_limit` controls how many node IDs a P2P artifact lookup returns; values `<= 0` return all matching nodes.
- `SCHEDULER_BINDING_TTL=<duration>` overrides `scheduler.binding_ttl` from the environment.
- `SCHEDULER_REDIS_ADDR=<addr>` overrides `scheduler.redis_addr` from the environment.
- `SCHEDULER_ARTIFACT_STORE_CAPACITY=<count>` overrides `scheduler.artifact_store_capacity` from the environment.
- `SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT=<count>` overrides `scheduler.artifact_lookup_node_limit` from the environment.

### Scheduling strategy

`scheduler.strategy` selects the algorithm used to pick a node from the eligible candidate list. Built-in strategies:

| Strategy | Behaviour |
|---|---|
| `round_robin` (default) | Cycles through eligible nodes in stable order |
| `random` | Picks a uniformly random eligible node |
| `least_loaded` | Samples two distinct eligible nodes and keeps the one with the lower heartbeat load: the higher of CPU and memory pressure, plus 15 points per sandbox still starting; a node without a heartbeat loses to one with it |

The strategy interface receives `RichNode` values that carry the node identity (ID + endpoint) together with the latest heartbeat `NodeSnapshot` (sandbox counts, CPU, memory, disk metrics). `round_robin` and `random` ignore the snapshot; `least_loaded` reads it.

### Node resource limit

`scheduler.node_resource_limit` defines per-node resource thresholds that are evaluated **before** the strategy runs. Any node whose heartbeat snapshot exceeds a configured limit is removed from the candidate list, regardless of which strategy is in use. This is a generic guard-rail that sits above the strategy layer — strategies only see nodes that already passed the resource filter.

Nodes that have not yet sent a heartbeat (no snapshot available) are always kept in the candidate list, since there are no metrics to evaluate.

All fields are optional. Omitting a field (or setting the whole block to `null`) disables that particular check.

| Field | Type | Description |
|---|---|---|
| `max_sandbox_count` | uint32 | Maximum total sandbox count |
| `max_sandbox_starting_count` | uint32 | Maximum concurrently starting sandboxes |
| `max_cpu_used_percent` | uint32 | Maximum observed CPU usage (0–100) |
| `max_cpu_allocated_percent` | uint32 | Maximum allocated-CPU-to-physical-CPU ratio; can exceed 100 when overcommit is allowed |
| `max_memory_used_percent` | uint32 | Maximum observed memory usage (0–100) |
| `max_memory_allocated_percent` | uint32 | Maximum allocated-memory-to-physical-memory ratio; can exceed 100 when overcommit is allowed |
| `admission_mode` | string | `nominal` (default), `request` or `shadow`; see "Memory admission modes" below |
| `max_memory_limit_overcommit_ratio` | float | In `request`/`shadow` mode: cap on (sum of nominal VM sizes) / node memory. Default `1.5`; ignored in `nominal` |
| `max_disk_used_percent` | uint32 | Maximum used percentage of the filesystem at `disk_mount_point` (0–100). Always effective: unlike the memory limits it does not take part in `admission_mode` |
| `disk_mount_point` | string | Mount point of the heartbeat disk metric the disk limit reads, matched exactly. Default `/workspace`; only used when `max_disk_used_percent` is set |
| `memory_capacity_source` | string | `allocatable` (default) computes the admission ceiling over the Kubernetes node's `status.allocatable.memory` minus the runtime overhead; `total` is the kill switch that restores the `memory_total` math |
| `shape_admission` | string | `on` (default) decides create and resume admission by the shape rule and publishes the per-layer verdict; `shadow` evaluates the rule and publishes the verdict and the shadow outcomes while deciding like `off`; `off` is the kill switch that restores the pre-OPSAI-6200 routing and publishes nothing |
| `cross_node_resume` | bool | Allow a paused sandbox rejected on its owner to resume on another admitted node. Off by default: only enable it once the paused artifacts are readable from another node |
| `admission_layers` | array | VM shapes published per node: `{"memory_gb": 8, "cpu_count": 4}`. Default the 8, 16, 32 and 64 GiB layers; `cpu_count` is optional |

Example:

```json
"node_resource_limit": {
  "max_sandbox_count": 50,
  "max_sandbox_starting_count": 10,
  "max_cpu_used_percent": 90,
  "max_cpu_allocated_percent": 150,
  "max_memory_used_percent": 85,
  "max_memory_allocated_percent": 150,
  "max_disk_used_percent": 75,
  "disk_mount_point": "/workspace",
  "memory_capacity_source": "allocatable",
  "shape_admission": "on",
  "cross_node_resume": false,
  "admission_layers": [
    {"memory_gb": 8},
    {"memory_gb": 16},
    {"memory_gb": 32},
    {"memory_gb": 64}
  ]
}
```

The disk limit reads the real used/total ratio of one mount point and rejects the node when it is strictly above the limit. A heartbeat that does not carry that mount (an older agent) is admitted — fail-open — and counted in `agentenv_scheduler_disk_admission_total{outcome="mount_missing"}`, so an inert limit is visible instead of silent. Pausing a sandbox writes a chain of roughly one VM's worth of state to this filesystem, so the limit protects the next pause, not only the current usage.

### Capacity rule and published admission verdict

The admission ceiling is one rule. With `memory_capacity_source: allocatable` the scheduler reads the Kubernetes node's `status.allocatable.memory` (through the pod-to-node mapping of the EndpointSlice) and subtracts the runtime overhead, estimated conservatively as `memory_used - allocated`. Nodes without that data fall back to `memory_total`, exactly as before. Every memory check (allocated and used, in `nominal`, `request` and `shadow` modes) reads this base; the rule is identical in all three modes.

`GET /nodes` publishes, per node, the capacity audit fields and a verdict for each configured layer:

```json
"admission": {
  "allocatableMemoryBytes": 125148200960,
  "runtimeOverheadBytes": 4194304000,
  "capacityMemoryBytes": 120953896960,
  "capacitySource": "allocatable",
  "layers": [
    {"memoryGB": 8, "admitted": true},
    {"memoryGB": 16, "admitted": false, "reason": "memory_allocated"}
  ]
}
```

`reason` is a closed set — `memory_allocated`, `memory_used`, `cpu`, `starting`, `disk` — and is omitted when the layer is admitted. The same capacity check gates a paused-sandbox resume: `POST /sandboxes/{id}/resume` asks the scheduler whether the paused shape fits its owner. When it does not and `cross_node_resume` is off, the gateway returns `503` with `Retry-After: 5` and `{"code":"sandbox_capacity_deferred","reason":"..."}`; with cross-node resume on it moves the sandbox to an admitted node and records the new binding before proxying. When neither the heartbeat nor the gateway's recorded template shape is known, the resume is deferred without a reason instead of over-admitting the node.

### Shape admission and template resolution

`shape_admission` has three modes:

- `on` (default): every create and resume is projected with the same capacity rule and the per-layer verdict is published. A create whose body carries no `memoryMB` uses the template shape; a template the gateway cannot read is refused with `refusal_reason: UNKNOWN_TEMPLATE_SHAPE` when `max_memory_allocated_percent` is configured, never admitted blind.
- `shadow`: evaluates the rule for create and resume and records each verdict without changing the decision, so the rule can be measured for 24 h before it rejects. Creates keep today's reservation (body shape or slot); resumes return the owner with no reservation and no binding change.
- `off`: ignores the template shape and restores the pre-OPSAI-6200 routing.

Before scheduling a create that carries only `templateID`, the gateway picks a READY node from `ListObservedNodes` and reads `GET /templates/{id}` with its API key (`builds[0].cpuCount` and `builds[0].memoryMB`). The result travels in `NewSandboxHint` (`resolved_memory_mb`, `resolved_cpu_count`, `template_shape_status`, `template_id`); the forwarded create body is untouched. The shape is cached in memory by the exact `templateID` for 5 minutes and the lookup times out after 1 s; a failed read never blocks the create and is reported as `lookup_failed`, `not_found` or `unavailable`. `refusal_reason: UNKNOWN_TEMPLATE_SHAPE` becomes `503` with `Retry-After: 5` and the same reason.

Shape decisions are counted in `agentenv_scheduler_shape_admission_total{kind,outcome,reason}`: `kind` is `create` or `resume`; `outcome` is one of `admit`, `would_reject`, `would_reject_all`, `would_defer`, `would_move`, `unknown_shape`; `reason` is the capacity reason (`memory_allocated`, `memory_used`, `cpu`, `starting`, `disk`), `none`, `unknown_shape` or `unknown_template_shape`. The gateway reports the template read in `agentenv_gateway_template_shape_total{outcome}` (`resolved`, `cache_hit`, `not_found`, `lookup_failed`, `unavailable`, `no_node`) and its latency in `agentenv_gateway_template_shape_lookup_seconds{outcome}`.

Deploy the scheduler with `shape_admission: shadow` first and the gateway after it, so the gateway only sends the new hint fields once the scheduler can read them. Roll back the gateway first, then the scheduler; `off` is the final kill switch and needs no gateway change.

### Memory admission modes

A sandbox is booted with a fixed VM size (`memoryMB`, the *limit*; no memory hotplug). Sandbox creation also accepts an optional `memoryRequestMB` (> 0, <= `memoryMB`, default = `memoryMB`) which only affects scheduling; it never changes the VM size. Nodes report the sum of active requests as `requested_memory_bytes` next to `allocated_memory_bytes` (sum of limits).

- `nominal` (default): `max_memory_allocated_percent` compares the sum of limits. Identical to previous releases; `memoryRequestMB`, `requested_memory_bytes` and `max_memory_limit_overcommit_ratio` are ignored.
- `request`: `max_memory_allocated_percent` compares the sum of requests, and the sum of limits must stay <= `max_memory_limit_overcommit_ratio` x node memory. `max_memory_used_percent` still applies and is the only protection against real host exhaustion.
- `shadow`: decides exactly like `nominal`, evaluates `request` in parallel, logs `admission shadow divergence` and increments `agentenv_scheduler_admission_shadow_total{outcome=agree|request_would_reject|request_would_admit}`. Use it to size the rollout before enabling `request`.

Nodes running an older agent report no `requested_memory_bytes`; they are evaluated with their nominal figure in `request` mode. Unknown `admission_mode` values or a ratio <= 0 are rejected at startup.

Risk: in `request` mode, sandboxes can grow to their limit at the same time and exhaust host memory (OOM kills of Firecracker processes). Do not enable `request` before a pressure-based pause/eviction mechanism exists on the node; paused sandboxes are not counted in the active sums.

When all nodes are filtered out, the scheduler returns `Unavailable` to the caller.

## Gateway configuration

- `gateway.scheduler_addr` points to the primary scheduler. The gateway uses it for scheduling, assignment writes, node listing, node detail resolution, and P2P scheduler APIs.
- `gateway.query_only_scheduler_addr` optionally points to a query-only scheduler. When set, sandbox `LookupNode` routing uses this client; when unset, gateway falls back to `gateway.scheduler_addr`.
- `gateway.request_timeout` must be a duration string such as `"30s"` in JSON config files.
- `gateway.request_timeout` bounds scheduler route resolution and regular control-plane requests. Data-plane requests routed by sandbox headers or host-based sandbox domains, streaming requests, and WebSocket connections reuse the client context after routing and are not cut off by this timeout.
- `gateway.forward_response_size` only limits how much of a successful `POST /sandboxes` response the gateway buffers while extracting a sandbox ID for `RecordAssignment`; it is not a global response-size cap for all proxied traffic.
- Cluster list requests (`GET /sandboxes`, `GET /v2/sandboxes`) fan out to every scheduler node and merge results in the gateway. Direct requests to a backend node remain node-scoped.
- Cluster list requests are strict all-or-nothing: if any node times out, returns a non-2xx response, or cannot be reached, the gateway fails the whole list request rather than returning partial data.
- `GET /nodes` returns scheduler-observed node snapshots (including runtime/resource counters), with optional `clusterID` filtering.
- `GET /nodes/{id}` resolves node endpoint via scheduler and then proxies to the runtime node's admin endpoint.
- `GATEWAY_REQUEST_TIMEOUT=<duration>` overrides `gateway.request_timeout` from the environment (for example, `1m30s`).
- `GATEWAY_QUERY_ONLY_SCHEDULER_ADDR=<addr>` overrides `gateway.query_only_scheduler_addr` from the environment.
- `gateway.sandbox_proxy_domains` enables host-based sandbox data-plane routing for `{port}-{sandboxID}.{domain}` URLs. Domains are normalized to lowercase, deduplicated, and must be valid DNS names. Sandbox IDs used in host routes must be lowercase RFC 952/1123 DNS labels, and the full `{port}-{sandboxID}` label must be at most 63 characters.
- `GATEWAY_SANDBOX_PROXY_DOMAINS=<domain>[,<domain>...]` overrides `gateway.sandbox_proxy_domains` from the environment.

Logging format defaults to `auto`:

- `auto`: console when stdout looks like an interactive terminal, otherwise JSON
- `console`: force human-readable terminal logs
- `json`: force structured logs for containers and log pipelines

Examples:

```bash
LOG_FORMAT=console make run-scheduler
LOG_FORMAT=json make run-gateway
```

## Deploy with Docker Compose

From **repository root**, start gateway + scheduler + two backend nodes:

```bash
# Run scripts/docker-setup.sh first for host prerequisites.
make deploy-up
```

Optional host-based sandbox data-plane routing:

```bash
SANDBOX_PROXY_DOMAINS=sandbox.example.com \
make deploy-up
```

Repository deployment helpers also accept `SANDBOX_PROXY_DOMAINS=<domain>[,<domain>...]`
and pass it to both gateway and runtime node processes.

Check status / logs / teardown:

```bash
make deploy-ps
make deploy-logs
make deploy-down
```

Container deployments use `deploy/docker/config/default.json`, where scheduler service-discovery and backend node endpoints are set for the Docker network.

The compose stack also wires each runtime node for scheduler heartbeat reporting:

- `AENV_NODE_ID` is set per runtime container (`node-a`, `node-b`).
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true` enables heartbeat reporting.
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` points runtime nodes at `http://scheduler:9090`.
- `SANDBOX_PROXY_DOMAINS`, when set, is passed through as both `GATEWAY_SANDBOX_PROXY_DOMAINS` and `AENV_SANDBOX_PROXY_DOMAINS`.

## Deploy on Kubernetes

From the repository root:

```bash
make k8s-render
make k8s-apply
```

Optional host-based sandbox data-plane routing:

```bash
SANDBOX_PROXY_DOMAINS=sandbox.example.com make k8s-apply
```

The default overlay is `deploy/k8s/overlays/default`.
The make targets materialize a temporary Kustomize build context so Kubernetes runtime nodes always consume the repository's single AgentENV runtime config source: `config/default.toml`.

The DaemonSet injects scheduler-report identity and endpoint wiring for runtime nodes:

- `AENV_NODE_ID` comes from Pod metadata name.
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true` enables heartbeat reporting.
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` is set to `http://agentenv-scheduler:9090`.
- `AENV_SANDBOX_PROXY_DOMAINS` comes from the shared sandbox proxy ConfigMap.

Shared Kubernetes helpers:

```bash
make k8s-build
make k8s-redeploy
```

For single-machine development, `make k8s-apply-dev` uses the `local-dev`
overlay and mounts the repository `env/` directory directly into the AgentENV
DaemonSet at `/workspace/env`. This avoids copying runtime assets into `/var/lib/agentenv/env`.

For local k3s-style development, use:

```bash
make k8s-load-dev
make k8s-refresh-dev
```

`k8s-load-dev` imports the locally built images into k3s/containerd, while
`k8s-refresh-dev` runs build, load, and rollout restart together.

Deployment model:

- `gateway`: Deployment + ClusterIP Service
- `scheduler`: single-replica Deployment + ClusterIP Service
- `agentenv-node`: privileged DaemonSet with `/dev/kvm` and hostPath `/var/lib/agentenv`
- `agentenv-nodes`: headless Service used by scheduler EndpointSlice discovery

Kubernetes config keys:

- `scheduler.discovery.mode`
- `scheduler.discovery.kubernetes.namespace`
- `scheduler.discovery.kubernetes.service_name`
- `scheduler.discovery.kubernetes.port`
- `scheduler.discovery.kubernetes.scheme` (defaults to `http`)
- `scheduler.discovery.kubernetes.ignore_pod_selector` (optional Kubernetes label selector; matching Pods are excluded from discovery)
- `scheduler.discovery.kubernetes.no_schedule_pod_selector` (optional Kubernetes label selector; matching Pods are kept as lingering/no-schedule nodes)

Kubernetes endpoint address handling:

- Scheduler only accepts EndpointSlice addresses that parse as valid IPs.
- Both IPv4 and IPv6 endpoint addresses are supported.
- IPv6 endpoints are emitted using bracketed host:port form (for example, `http://[2001:db8::10]:8000`).

Operational notes:

- The scheduler uses in-cluster Kubernetes config and watches EndpointSlices plus Pods for service discovery.
- Only serving, non-terminating DaemonSet Pods are schedulable. Use `no_schedule_pod_selector` for drain/no-new-work labels and `ignore_pod_selector` for Pods that should be completely hidden from discovery.
- For the default `memory` binding store, `scheduler` should stay single-replica because sandbox bindings are process-local.
- For high availability, run one primary scheduler with `scheduler.redis_addr` set and multiple query-only scheduler replicas started with `--query-only` against the same Redis. Point gateways at the primary with `gateway.scheduler_addr` and at the query-only service with `gateway.query_only_scheduler_addr`. The primary writes sandbox bindings to Redis while query-only replicas continue to serve data-plane `LookupNode` during primary restarts or upgrades. This HA mode is intentionally data-plane only: requests that proxy to existing sandboxes can keep routing, but control-plane operations that need the primary scheduler, such as creating new sandboxes, scheduling, assignment writes, node listing, node detail resolution, and P2P scheduler APIs, still fail while the primary scheduler is unavailable. Artifact store state is still in-memory and is not covered by this HA mode.
- The gateway is intentionally left as ClusterIP by default; attach an Ingress or LoadBalancer based on your environment.

## gRPC API

Proto contract: api/proto/scheduler.proto

Methods:

- Schedule
- ListNodes
- LookupNode
- RecordAssignment
- Heartbeat
- ListObservedNodes
- ReportSandboxEvent
- GetNode
- UnregisterNode
