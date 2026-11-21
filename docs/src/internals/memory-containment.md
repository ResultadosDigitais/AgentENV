# Memory Containment

Two independent mechanisms keep node memory pressure from evicting the runtime
pod:

1. **Snapshot I/O leaves the working set** (`[memory_snapshot]
   drop_page_cache_after_write`). After a memory-snapshot layer is sealed, the
   node syncs it and drops its page cache, so the bytes the pause just wrote
   stop counting toward the pod's working set. Off by default: it adds an fsync
   to the pause path, and whether that is worth it has to be measured first.
2. **One cgroup per VM** (`[memory_containment]`). Every VMM lives in its own
   cgroup v2 domain with `memory.max` equal to the VM shape and `memory.high`
   below it, so node pressure throttles or OOM-kills a single VM and never the
   pod that holds all of them.

Both are disabled by default; a node that cannot prepare the cgroup subtree
logs the reason and keeps today's behavior.

## Layout

```text
<container cgroup>        domain, no processes, +memory +cpu
├── agentenv-server/      domain leaf, the runtime's own processes
└── agentenv-vms/         domain, +cpu (for a nested CPU quota)
    └── <sandbox id>/     domain, memory.max = shape, memory.high < shape
        └── cpu/          threaded, cpu.max (when [resize] caps the VMM)
```

The memory controller distributes only to domain children, and a cgroup that
holds processes cannot delegate a domain controller. Startup therefore moves
the runtime's processes into `agentenv-server/` before enabling `memory`; if the
move or the delegation fails, the processes are put back and the node keeps the
old layout. A cgroup cannot have both domain and threaded children, so when
containment is active the CPU quota nests `cpu/` under each VM's memory domain
instead of using the shared `agentenv-sandboxes` root.

`memory.oom.group` is set to 1 on the per-VM cgroup: an OOM inside one VM kills
that VM's subtree as a unit. The container's own `memory.oom.group` is left at
0, so the pod is never killed with it.

## Measurement (OPSAI-6266)

The numbers below decide the option and are **not measured yet**: this change
was prepared from a sandbox with no cluster access and no `/dev/kvm`, so every
value is `UNKNOWN` with the command that failed. The card owner runs the
protocol on a drained canary node and fills the table before the feature is
enabled anywhere.

Never run `drop_caches` on a node with live VMs, not even to measure: on
2026-11-05 a `drop_caches` on a node with restored VMs forced them to re-read
guest memory from disk.

### p-1 · cgroup delegation

```sh
CGROUP=$(awk -F: '$1=="0"{print $3}' /proc/self/cgroup)
cd "/sys/fs/cgroup${CGROUP}"
cat cgroup.controllers cgroup.subtree_control
mkdir -p /tmp/aenv-cgroup-probe && rmdir /tmp/aenv-cgroup-probe
mkdir probe-vm && echo "+memory +cpu" > cgroup.subtree_control
echo "+memory" > probe-vm/cgroup.subtree_control
echo $((1024*1024*1024)) > probe-vm/memory.max
echo $((900*1024*1024)) > probe-vm/memory.high
cat probe-vm/memory.max probe-vm/memory.high probe-vm/memory.oom.group
rmdir probe-vm
```

A failure means the runtime cannot create per-VM memory cgroups on this node:
the card returns to the owner instead of shipping a fallback that cannot meet
"one VM dies, the others stay".

### p-2 · VMM process topology

```sh
ps -eo pid,ppid,comm | grep -E 'agentenv|firecracker'
pstree -p $(pgrep -f 'agentenv.*server' | head -1)
```

Confirms the VMM of each VM is a direct child of the runtime and can be moved
alone. A shared VMM process would need a different containment axis.

### p-4 · what the working set is made of

With one VM, sample the runtime pod's cgroup before and after pause, restore and
snapshot:

```sh
CGROUP=$(awk -F: '$1=="0"{print $3}' /proc/self/cgroup)
grep -E '^(anon|file|active_file|inactive_file|slab) ' /sys/fs/cgroup${CGROUP}/memory.stat
cat /sys/fs/cgroup${CGROUP}/memory.current
```

Then repeat the same pause/restore cycle with
`[memory_snapshot] drop_page_cache_after_write = true` and record the pause and
resume duration for each variant. The split between snapshot I/O cache and
guest memory of restored VMs (Firecracker maps the memory file, so read-only
guest pages count as file cache) is what decides between `fadvise(DONTNEED)`,
`O_DIRECT`, `memory.high`, and copying restored memory into anonymous memory.
Only after that split is measured does `[memory_containment]
memory_high_ratio` get a value, and `memory.high` must not stall a VM past the
runner grace (90 s).

| Field | Value | Source |
|-------|-------|--------|
| pod `active_file` before/after pause | UNKNOWN | `kubectl get ns` failed: `dial tcp [::1]:8080: connect: connection refused` (no cluster credential in the run) |
| pod `active_file` before/after restore | UNKNOWN | same failing command |
| share of the working set that is snapshot I/O cache | UNKNOWN | same failing command |
| share that is guest memory mapped from the memory file | UNKNOWN | same failing command |
| pause time, cache kept / dropped after write | UNKNOWN | same failing command |
| resume time, cache kept / dropped after write | UNKNOWN | same failing command |
| `posix_fadvise(DONTNEED)` resident pages after write | UNKNOWN | same failing command |
| `O_DIRECT` effect on pause and resume | UNKNOWN | not implemented: it changes alignment and the GCS backend semantics, so it is only pursued if p-4 shows `fadvise` is insufficient |

### p-8 · canary

With the pin applied node by node through the drain, induce memory pressure on
the canary and verify: the VM under pressure dies or is throttled alone, the
other VMs and the paused sandboxes stay, and the runtime pod is not evicted.
Record the date, the commands, and the outcome. A single node does not prove the
pool.

## Rollout and rollback

- Roll out by `[memory_containment] enabled = true` with the measured
  `memory_high_ratio`, and `[memory_snapshot] drop_page_cache_after_write =
  true` once p-4 prices its fsync. One node at a time through the drain; never
  a bulk restart.
- Roll back by setting both back to their defaults and restarting the server.
  The per-VM cgroups and the server leaf are removed by the next startup; a
  stale non-empty cgroup is kept and logged, never deleted under a live VMM.
- A node where the subtree cannot be prepared logs `per-VM memory containment
  unavailable` at error level and keeps running uncontained; treat that line as
  a failed canary, not as success.
