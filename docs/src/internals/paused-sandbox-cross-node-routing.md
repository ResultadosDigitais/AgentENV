# Paused Sandbox Cross-Node Routing

Moving paused sandbox artifacts from node-local SSD to the regional bucket is
necessary but not sufficient for a paused sandbox to survive a pod swap. The
gateway resolves where to send a `/sandboxes/{id}/resume` or
`/sandboxes/{id}/connect` request through the scheduler, and that binding —
not the artifact location — decides whether the request reaches a node at all.

This document records the two premises that had to be settled before the storage
switch was designed, with the evidence for each.

## Premise 1: a recreated node becomes the owner of a paused sandbox

**Result: it does, with no scheduler or gateway change.**

The chain, end to end:

1. A node lists every sandbox its metadata store tracks —
   `Orchestrator::list_sandbox_ids` (`src/orchestrator/service.rs`) — which
   includes the paused records restored from the persister at startup
   (`new_inner_with_volumes` adds every persisted record to the store before the
   orchestrator is built).
2. That list is the `sandbox_ids` field of the node snapshot
   (`ObservabilityService::node_snapshot`, `src/observability/service.rs`), sent
   in the heartbeat (`ObservabilityReporter`, default interval 5 s).
3. `Service.Heartbeat` (`services/scheduler/internal/service.go`) passes
   `req.GetSandboxIds()` to `store.ReconcileNode`.
4. `RedisBindingStore.ReconcileNode` (`services/scheduler/internal/redis_store.go`)
   writes the node's binding record for every desired ID and drops IDs the node
   no longer reports. The Lua script sets the binding for each desired sandbox
   with the binding TTL (default 30 s) and steals the sandbox from another node's
   reverse index when that node reported it before.
5. The recreated pod is a known node before it can heartbeat: the scheduler's
   node list comes from Kubernetes discovery over the StatefulSet pods
   (`services/scheduler/internal/kubernetes_discovery.go`), not from the
   heartbeat itself, so `Heartbeat`'s `ErrNodeNotInRegistry` applies only to a
   node the discovery never produced.

So once a recreated node loads a paused record into its store, its next
heartbeat makes it the sandbox owner, and the gateway's `LookupNode` resolves to
it. Ownership is not the blocker.

**What this premise does not cover, and why it still matters.** Ownership follows
*metadata*, so it only works if the recreated node can load the paused record at
all. That is exactly the load path this card fixes: today a record whose runtime
state does not decode on the loading node (the case where the artifacts are
elsewhere) is deleted, so the recreated node reports nothing and no binding is
ever written. The storage and load fix is what makes premise 1 reachable; the
routing itself needs no change.

## Premise 2: no path other than `LookupNode` refreshes ownership

**Result: none exists that a paused sandbox can reach.**

- `RecordAssignment` (`services/scheduler/internal/service.go`) is the only other
  writer of a binding. Nothing in this repository calls it for a paused sandbox:
  there is no `RecordAssignment` call site in `src/`, and its RPC name is only
  reachable by an explicit gRPC client.
- `ReportSandboxEvent` is explicitly a no-op — it logs and returns an empty
  response, and the scheduler contract states lifecycle events are best-effort
  and do not establish bindings.
- The gateway routes every owner-scoped request through
  `s.queryOnlyScheduler.LookupNode` (`services/gateway/internal/server.go`), and
  `LookupNode` is a read of the binding store. A miss is `NotFound`.

Ownership therefore refreshes only through the heartbeat reconcile described in
premise 1, which is what a recreated node does on its own.

## Consequence for the card

No gateway or scheduler change is required, and none is added to the scope. The
acceptance criterion "pause, delete the pod, resume on another node, reach
READY" is reachable through the storage and load path alone, provided:

- the paused record is still listed by the loading node when its runtime state is
  not decodable there (the retention rule added to `load_all`), and
- the artifacts travel with the sandbox (the bucket-backed paused store).

The one assumption this leaves open is node identity stability across a pod
swap: `node_id` comes from `node_identity.node_id` in configuration
(`src/identity.rs`), so a recreated pod keeps its identity only while the
deployment keeps that value stable per replica. This is already the deployment's
contract for `RecordAssignment` and the existing paused-resume flow and is not
changed here.
