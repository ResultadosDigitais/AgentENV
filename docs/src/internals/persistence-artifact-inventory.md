# Persistence Artifact Inventory

This document lists AgentENV artifacts that can remain on disk or in object storage beyond a single function call. It is organized by the module that owns each artifact's lifecycle.

## Path Roots

| Root | Default | Owner | Notes |
| --- | --- | --- | --- |
| `home_path` | `/var/lib/aenv` | `src/cfg.rs` | Base for paths containing the literal `$AENV_HOME` placeholder. `AENV_HOME_PATH` overrides it before placeholder expansion. |
| `runtime_path` | `/run/aenv` | `src/cfg.rs`, `src/sandbox/network/*` | Base for transient namespace mount points and daemon sockets. `AENV_RUNTIME_PATH` overrides it. |
| `deps_path` | `$AENV_HOME/deps` | `src/cfg.rs`, `src/setup/*` | Base for downloaded runtime dependencies. `AENV_DEPS_PATH` can place these rebuildable assets outside `home_path`. |
| Managed sandbox access-token seed | `$AENV_HOME/secrets/sandbox-access-token-hash-seed` | `src/sandbox/access.rs` | Node-local secret used to derive envd and traffic tokens when `[sandbox].access_token_hash_seed` is unset. It must be preserved with persisted secure or private-ingress sandboxes. |
| Firecracker sandbox work dirs | `$AENV_HOME/firecracker-work` with `agentenv-fc-` children | `src/sandbox/firecracker/*` | Per-sandbox runtime directories for sockets, symlinks, ublk runtime dirs, local logs, and writable OverlayBD upper layer data (`overlaybd/upper.data`, `overlaybd/upper.index`). An explicit `[firecracker].work_dir` overrides the root. |
| `firecracker.serial_dir` | `$AENV_HOME/logs/serial` | `src/sandbox/firecracker/*` | Durable Firecracker stdout/stderr root, grouped by sandbox ID. An explicit `[firecracker].serial_dir` overrides the root. |
| `managed_snapshot_root` | `<firecracker-work-base>/managed-snapshots` | `src/sandbox/firecracker/*` | In-process live snapshot artifact root used to keep captured snapshots alive until publish or drop. |
| `persisted_sandbox_store_path` | `$AENV_HOME/persisted-sandboxes` | `src/orchestrator/persistence/*` | Durable paused sandbox records and artifacts. |
| `snapshot_store` | `$AENV_HOME/snapshot-store` | `src/repository/*` | Durable committed snapshot repository root. The configured backend uses `<snapshot_store>/repository`. Relative explicit paths are resolved against the config file directory. |
| `snapshot.local_cache_path` | `$AENV_HOME/snapshot-local-cache` | `src/snapshot/artifact_cache.rs`, runtime resolvers | Node-local cache for materialized runtime artifacts. Relative explicit paths are resolved against the config file directory. |
| `image.cache.root_dir` | `$AENV_HOME/image-cache` | `src/image/*`, overlaybd runtime | Node-local image cache root. Contains `configs/`, `indexes/`, `commits/`, and `remote-blocks/`. The offline C++ tools own isolated sibling cache roots: `convert-blocks/` (`overlaybd-apply`) and `resize-blocks/` (`overlaybd-resize`). |
| `p2p.store_dir` | `$AENV_HOME/p2p/store` | `src/p2p/*`, `src/cfg.rs` | Local store for P2P artifact transport backends. Relative explicit paths are resolved against the config file directory. |
| `image.cache.remote_blocks` | `<image.cache.root_dir>/remote-blocks` | overlaybd runtime config | Remote block cache root. Overlaybd also stores `premerged-index/` under this cache dir. Its size limit comes from `image.cache.remote_blocks.max_size_gb`. |
| `ublk.daemon_socket_path` | `$AENV_RUNTIME/ublk-daemon.sock` | `src/sandbox/ublk/*`, `storage/ublk-daemon/*` | Unix socket used for server-to-daemon IPC. |
| `ublk.daemon_log_path` | `$AENV_HOME/logs/ublk-daemon.log` | `storage/ublk-daemon/*` | Daemon log file supplied during config normalization. An explicit path overrides the default. |

## Setup And Config

Owned by `src/setup/*` and `src/cfg.rs`.

| Artifact | Location | Contents | Purpose | Lifecycle |
| --- | --- | --- | --- | --- |
| Firecracker binary | `<deps_path>/firecracker/{version}/firecracker` | Firecracker executable | VM process runtime | Created during setup when missing. Old versions are retained until manually removed. |
| CPU template helper | `<deps_path>/firecracker/{version}/cpu-template-helper` | Optional Firecracker helper executable | Detects host CPU config for cluster-wide CPU intersection | Extracted from Firecracker package when present. |
| Kernel image | `<deps_path>/kernel/{version}/vmlinux.bin` | Guest kernel | VM boot source | Downloaded during setup. Old versions are retained until manually removed. |
| Tools drive | `<deps_path>/tools/{version}/tools.ext4` | Read-only ext4 image with envd/tools | Firecracker root drive shared by sandboxes and pinned by snapshots through its immutable release version | Extracted from OCI or imported from `tools.drive_path` during setup. Old versions are retained until manually removed. |
| Overlaybd tools | `<deps_path>/overlaybd/bin/*` | Statically linked `overlaybd-create`, `overlaybd-apply`, `overlaybd-commit`, and `overlaybd-resize` | OCI-to-overlaybd conversion and packaging | Installed during setup when release metadata does not match. |
| Overlaybd release metadata | `<deps_path>/overlaybd/tools-release.json` | Installed overlaybd release identifier | Detects whether tools need reinstalling | Rewritten on setup when release changes. |
| Overlaybd package downloads | `<deps_path>/overlaybd/downloads/*` | Temporary downloaded package archives | Setup staging for overlaybd release packages | Removed after a successful install. |
| Generated overlaybd config | `$AENV_HOME/overlaybd/overlaybd-global.json`, `$AENV_HOME/overlaybd/mem-overlaybd-global.json`, `$AENV_HOME/overlaybd/convert-overlaybd-global.json`, `$AENV_HOME/overlaybd/resize-overlaybd-global.json` | Runtime global config, cache path, credentials config | Configures overlaybd runtime, memory snapshot overlaybd access, and the offline C++ tools (`overlaybd-apply`, `overlaybd-resize`), which get dedicated configs with isolated cacheDirs (`convert-blocks`, `resize-blocks`) and download disabled | Rewritten during setup/startup. |
| Overlaybd runtime log | `$AENV_HOME/overlaybd/overlaybd.log` | Overlaybd runtime logs | Debugging | Appended by overlaybd runtime; no automatic GC. |
| Managed sandbox access-token seed | `$AENV_HOME/secrets/sandbox-access-token-hash-seed` | 32 random bytes encoded as lowercase hexadecimal | Derives stable per-sandbox envd and traffic access tokens when no explicit seed is configured | Atomically created with mode `0600` during normal startup and reused thereafter. Must not be deleted while secure or private-ingress sandboxes are persisted. |

## Firecracker Sandbox

### Runtime

Owned by `src/sandbox/firecracker/*`.

| Artifact                     | Location                                                     | Contents                                                  | Purpose                                                      | Lifecycle                                                    | Rebuildable                                                  |
| ---------------------------- | ------------------------------------------------------------ | --------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Managed live snapshot root   | `<firecracker-work-base>/managed-snapshots/{sandbox_id}/{uuid}/...` | Firecracker snapshot artifacts kept alive in-process      | Holds captured running-sandbox snapshots until publish, and supports in-process pause state | Created by `FirecrackerSandbox::pause()` or `snapshot()`. Removed when `PersistentSnapshotRootGuard` drops. | No.                                                          |
| Snapshot VM state            | snapshot artifact dir `vm_state.bin`                         | Firecracker VM state                                      | Pause/resume and snapshot publish input                      | Created by Firecracker's state-only diff snapshot. Owner depends on caller: persister, managed root, or repository publish input. | No.                                                          |
| Memory overlaybd layer       | snapshot artifact dir `mem_overlaybd/overlaybd.commit`       | Sealed OverlayBD layer for memory pages                   | Runtime memory restore and publish input                     | Created directly from Firecracker dirty/present memory ranges read with `process_vm_readv`. Imported into repository managed layers during publish. | No.                                                          |
| Memory image config          | snapshot artifact dir `mem_image.json`                       | Overlaybd image config stacking memory layers             | Resume paused sandbox and publish memory layers              | Written after memory conversion. Later repository resolvers regenerate runtime memory configs from committed layers. | Yes after publish; no for paused sandbox unless layers are known. |
| Rootfs snapshot config       | snapshot artifact dir `rootfs/image.json`                    | Overlaybd image config for captured rootfs state          | Resume paused sandbox and publish rootfs layers              | Staged from live runtime config after restack/seal.          | Yes only from associated layers and metadata.                |
| Rootfs snapshot layer        | snapshot artifact dir `rootfs/snapshot.commit`               | Sealed writable upper from live rootfs                    | Captures disk writes since previous lower stack              | Created by ublk daemon restack for writable overlaybd rootfs. | No.                                                          |
| Inherited runtime layers     | snapshot artifact dir `rootfs/inherited-layers/{index}/{source-file}` | Snapshot-owned hard links or copies of inherited runtime-created lower suffixes | Removes dependence on previous managed snapshot or persisted sandbox artifact roots | Created during pause when inherited lowers come from the managed snapshot root or another sandbox/generation under the same persisted `artifacts` root. | No.                                                          |
| Attached-drive snapshot dirs | snapshot artifact dir `drives/{drive_id}/...`                | Per-drive image config and snapshot layer                 | Captures writable attached-drive state                       | Created alongside rootfs snapshot for each drive.            | No.                                                          |
| Firecracker work dir         | configured work root, or consumed pool tempfile dir       | API socket, symlinks, runtime dirs, local logs            | Firecracker CWD for a sandbox                                | Created when sandbox handle is built, or moved from a warm pool entry; removed by owning `TempDir`. | Yes, except live state.                                      |
| Firecracker serial logs      | `firecracker.serial_dir/{sandbox_id}/*`, or warm pool work dir logs | Firecracker stdout/stderr                                 | Debugging                                                    | Opened on spawn and appended only when `firecracker.log_level` is non-empty or an explicit Rust capture destination is set. Enabled warm logs are relocated when a warm process is consumed. Configured serial output is not automatically GC'd. | No, but disposable.                                          |
| Firecracker logger output    | `firecracker.log` in the same per-sandbox log directory as the serial logs | Firecracker internal logger (`PUT /logger`)               | Debugging                                                    | Only created when `firecracker.log_level` is set to a non-empty level. Written by Firecracker itself; not automatically GC'd. | No, but disposable.                                          |

### Firecracker Pool

Owned by `src/sandbox/firecracker/pool.rs`.

| Artifact           | Location                                                     | Contents                                               | Purpose                             | Lifecycle                                                    |
| ------------------ | ------------------------------------------------------------ | ------------------------------------------------------ | ----------------------------------- | ------------------------------------------------------------ |
| Warm pool work dir | system tempfile dir | Firecracker socket and warm process logs               | Pre-spawned Firecracker process CWD | Created by pool maintenance as a `TempDir`. Removed when warm entry is cleaned up, or moved into the consuming sandbox and later dropped there. |
| Warm pool logs     | warm pool work dir `firecracker-stdout.log`, `firecracker-stderr.log` | Firecracker output before the warm process is consumed | Debugging warm startup              | Created on warm spawn only when `firecracker.log_level` is non-empty. Relocated into sandbox log path when consumed if no explicit stdout/stderr override. |

## Extra Drives

Owned by `src/sandbox/extra_drive.rs` and Firecracker snapshot code.

| Artifact                      | Location                                           | Contents                                         | Purpose                                                      | Lifecycle                                                    | Rebuildable                               |
| ----------------------------- | -------------------------------------------------- | ------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ----------------------------------------- |
| Extra-drive runtime dir       | sandbox work dir `extra-drive-runtime-{drive_id}/` | Runtime `image.json`, upper files, result file   | ublk runtime for attached drive                              | Created when preparing extra drives. Released on rollback/stop; work dir cleanup removes files. | Not while running; otherwise unnecessary. |
| Extra-drive symlink           | sandbox work dir `extra-drive-{drive_id}`          | Symlink to `/dev/ublkbN` device path             | Firecracker drive attachment path                            | Created after ublk runtime device creation. Removed on rollback or work dir cleanup. | Yes.                                      |
| Extra-drive snapshot artifact | snapshot artifact dir `drives/{drive_id}/...`      | Captured drive overlaybd config and commit layer | Preserve attached-drive writable state across pause/resume/publish | Created during sandbox snapshot/pause. Later owned by persister, managed root, or repository publish flow. | No.                                       |

## Snapshot Storage Model

Snapshot data passes through three storage layers with different ownership and
lifetime rules:

1. **Builder staging** is a manager-owned temporary workspace under
   `<snapshot.local_cache_path>/snapshots/<id>/`. It holds local rootfs,
   memory, VM-state, and attached-drive artifacts while a build or capture is
   in progress. It is not the durable snapshot record.
2. **The committed snapshot repository** stores the snapshot catalog,
   aliases, `snapshot.json`, `firecracker-manifest.json`, `vm_state.bin`, and
   referenced managed layers. This is the durable source of truth exposed by
   the template and snapshot APIs.
3. **The node-local runtime cache** materializes runnable rootfs, memory, and
   drive `image.json` files under
   `<snapshot.local_cache_path>/runtime/<id>/` before launch. These files are
   derived runtime inputs and can be rebuilt from committed state.

The committed `snapshot.json` records the captured runtime context, startup
configuration, and rootfs, drive, and memory layer references. The Firecracker
manifest records launch metadata such as virtual sizes and attached-drive
configuration. Temporary upper files and generated `image.json` files are not
committed snapshot truth.

During runtime resolution, AgentENV converts the committed layer references
into node-local rootfs, memory, and attached-drive configs, hydrates the
Firecracker manifest with node-local paths, and resolves the committed
`vm_state.bin` into a runnable path.

## Image Resolver

Owned by `src/image/*`.

| Artifact | Location | Contents | Purpose | Lifecycle |
| --- | --- | --- | --- | --- |
| Image config cache | `<image.cache.root_dir>/configs/*-image.json` | Digest-qualified overlaybd image configs for resolved user images | Avoids repeating OCI manifest classification and config generation | Written on image resolve. Regenerated if invalid or if referenced local lowers are missing. No unified GC today. |
| Image metadata sidecar | `<image.cache.root_dir>/configs/*.metadata.json` | Base env/workdir metadata from OCI image config | Preserves image launch context beside cached image config | Written after image config. Rebuilt if missing while image config is usable. |
| Overlaybd commit cache | `<image.cache.root_dir>/commits/{digest-slug}/overlaybd.commit` | Content-addressed overlaybd commit layers from standard OCI conversion, and target dirs for remote overlaybd-native layers | Node-local reusable layer store for user image layers | Standard OCI conversion writes commits. Remote overlaybd-native configs point `dir` here for runtime population. No unified GC today. |
| OCI conversion index | `<image.cache.root_dir>/indexes/{source-digest}/...json` | Mapping from OCI source layer/context to overlaybd commit digest and size | Skips repeated layer conversion when converted commits exist | Written after successful standard OCI conversion. No unified GC today. |
| Converted OCI layer P2P artifact | P2P catalog key `oci-layer/v1/{context-hash}` | A completed OverlayBD commit plus its conversion context and output digest/size | Lets another node reuse a compatible standard-OCI conversion | Published as a Reference after the local commit is durable; ownership is persisted on the hard-commit record and removed by image-cache GC. |
| Temporary OCI pull/conversion work | process temp dir | OCI layout and per-layer conversion workspace | Intermediate input for standard OCI conversion | Owned by `TempDir`; removed after conversion scope exits. |

## Snapshot Repository

Owned by `src/repository/*` and `src/snapshot/types/*`.

| Artifact | Location | Contents | Purpose | Lifecycle | Rebuildable |
| --- | --- | --- | --- | --- | --- |
| Snapshot records | POSIX: `<snapshot_store>/repository/catalog/records/{id}.json`; OSS: `catalog/records/{id}.json` | Snapshot ID, alias, source, resources, build status, committed logical metadata | Durable user-visible snapshot/template metadata | Shared documents written by `src/snapshot/catalog.rs`. Created before template build or at publish. Updated when committed or errored. Snapshot delete marks it `deleting` first, so no startup manifest is attached afterwards, and deletes it last. | No. |
| Snapshot aliases | POSIX: `<snapshot_store>/repository/catalog/aliases/{alias}`; OSS: `catalog/aliases/{alias}.json` | Alias-to-snapshot binding | Name lookup for templates/snapshots | Shared documents. Claimed after the record is written; released before the record is deleted. A binding whose record is gone is stale and is removed by the next lookup or claim. | Partially, from records if aliases are still recorded. |
| Shared documents (snapshot records and aliases, volume records and names, build logs, build-cache state) | Paths named in `src/repository/layout.rs`. POSIX: one file each, updated under a lock file. OSS: one `{name}.json` object each. S3 stores replace it only if its ETag is unchanged. Alibaba OSS cannot, so every write holds a lease on the appendable lock object `locks/{path}` (see `src/repository/backends/oss/lease.rs`). | Volume state, reservations, and name bindings; template build logs; the shared build-cache seed | Updated atomically by the snapshot and volume catalogs and the template builder on every node | A released lease deletes its lock object. A holder that dies leaves it until a contender takes it over 30 seconds later. | No. |
| Firecracker manifest | POSIX: `<snapshot_store>/repository/snapshots/{id}/firecracker-manifest.json`; OSS: `artifacts/{id}/firecracker-manifest.json` | Firecracker snapshot shape, virtual sizes, attached-drive metadata; path fields are hydrated at runtime | Runtime manifest template for launching committed snapshots | Persisted during publish. Removed with per-snapshot artifacts. | Partially, but kept as durable artifact. |
| VM state | POSIX: `<snapshot_store>/repository/snapshots/{id}/vm_state.bin`; OSS: `artifacts/{id}/vm_state.bin` | Firecracker VM state snapshot | Required to resume committed snapshots | Copied/uploaded during publish. Removed with per-snapshot artifacts. | No. |
| Managed snapshot layers | POSIX: `<snapshot_store>/repository/managed-layers/{digest}.overlaybd.commit`; OSS: `managed-layers/{digest}` | Content-addressed rootfs, attached-drive, and memory overlaybd commit layers | Shared immutable layer storage for committed snapshots, volume backing, and published images | Imported during publish by descriptor or by hashing local descriptor-less layers. Not removed with a single snapshot, because other records may cite the layer. A layer no record cites is collected by `POST /admin/managed-layers/collect` once it is older than `[snapshot.managed_layer_collect].grace_secs`; see "Managed Layer Collection" below. | Not from metadata alone; can be re-fetched only if source still exists, and a collected layer is gone for good. |
| Managed-layer collection lock | POSIX: `<snapshot_store>/repository/maintenance/locks/managed-layer-collect`; OSS: `maintenance/locks/managed-layer-collect` | The holder token of the running collection pass | Serializes collection passes so two never overlap | Created by `maintenance::collect_managed_layers`. POSIX holds an `flock` the kernel drops with the process; OSS deletes the object on release, and a later pass takes over an object older than 30 minutes. Never under `managed-layers/`, so a pass never lists its own lock as a layer. | Yes. |
| Staging files | POSIX: `<snapshot_store>/repository/staging/*`; OSS: process temp dir | Dense-exported and recontainerized layers prepared for storage | On POSIX, the same filesystem as `managed-layers/`, so a prepared layer is hard-linked in without a copy | Temporary files, removed once the publish that made them is done. A process that crashes mid-publish leaves its files behind. | Yes. |
| Source-registry publications | `SnapshotRecord.committed.disk_publications` plus remote OCI registry objects | Published rootfs/attached-drive image refs and manifest digests | Lets compatible snapshot deltas live in their source registry | Created during OSS publish when image publishing is enabled. Rolled back on failure or delete when possible. | Registry-owned; record is required to locate. |

Repository records are the logical source of truth for committed snapshots. Build-time and runtime `image.json` files should not become committed truth unless they contain data that cannot be derived from committed metadata.

## Managed Layer Collection

Owned by `src/maintenance/managed_layers.rs`.

A managed layer is content addressed and shared, so deleting a snapshot removes only that snapshot's own files and leaves every layer behind. Nothing used to notice a layer that no record cited any more, so deleting a record stranded its layers in the bucket for good. `POST /admin/managed-layers/collect` is that collector.

The citation set is closed and comes from four places:

- **snapshot records** (`catalog/records/`): committed rootfs, attached-drive, memory and volume-snapshot layers;
- **volume records** (`volumes/records/`): backing layers, which keep a volume's layers after the snapshots that used it are gone;
- **published image descriptions** (`images/descriptions/`): an image owns its layers;
- **live sandboxes**: the source snapshot of every sandbox in the cluster, running or paused. The pass unions the node's own list with the gateway's aggregated `GET /v2/sandboxes`, which fans out to every node, because a sandbox paused on *another* node still reads its source snapshot's layers on resume. If a live sandbox's source is no longer in the catalog, the pass is downgraded to a report for the whole bucket, because the substrate can no longer prove which layers that sandbox may read.

The cluster list is fail-closed. When `[snapshot.managed_layer_collect].gateway_url` is unset, when any node of the gateway's fan-out fails (the gateway answers 502 for the whole page), when a page does not arrive, or when a page token repeats, the pass treats the citation set as unknowable and deletes nothing: `enforce` is refused with 409 and only `report` runs. A partial list is never read as a complete one — an enforce pass built from one node's list deleted 123 layers on staging, one of them a lower layer of a sandbox paused on another node.

`BlobKey::Layer` is written only by snapshot publication (`src/snapshot/store.rs`) and volume publication (`src/volume/mod.rs`); image builds reuse the same two stores, and image descriptions only reference layers those stores wrote.

The pass runs in two modes. `report` lists and counts without deleting anything. `enforce` deletes only the uncited layers older than `[snapshot.managed_layer_collect].grace_secs`, and only after a report has run in the same server process. The grace period exists because a publish uploads its layers *before* it writes the record that cites them: a layer uploaded moments ago and not cited yet belongs to a publish in flight.

Two things the pass never does:

- it never deletes an object under `managed-layers/` whose name does not translate back to a `sha256:<hex>` digest — the substrate cannot prove what such an object is, so it is counted as unreadable and left alone;
- it never deletes while another pass holds the maintenance lock. The lock serializes passes only; it does not exclude publishers, and the grace period covers that race.

Every pass records cited layers and bytes, orphan layers and bytes, unexpired and unreadable layers, deleted layers and reclaimed bytes, and the objects it could not handle. A single failure never stops the pass.

### Rolling it out

Report first, in this order, on a node whose repository is the environment you are cleaning:

1. Start one node with `AENV_SNAPSHOT_MANAGED_LAYER_COLLECT_ENABLED=true`, and with `AENV_SNAPSHOT_MANAGED_LAYER_COLLECT_GATEWAY_URL` pointing at the cluster gateway. Without the gateway URL the pass sees only its own node, so a sandbox paused elsewhere looks gone and its layers look uncited: enforce is refused, and report still runs but its `orphanLayers` is not the cluster's.
2. Run `POST /admin/managed-layers/collect` with `{"mode": "report"}` and compare `orphanLayers`/`orphanBytes` with what you measured independently. A difference larger than the layers a known publication is uploading means the citation set is missing a source; **stop** and investigate, do not enforce. A report whose `downgradedReason` is set proves nothing and must not be followed by an enforce.
3. Only then run `{"mode": "enforce"}`. It applies the same grace period, so the layers a pass in flight is uploading stay.
4. The pass never removes an object whose name is not a digest. A non-zero `unreadableLayers` is expected from older releases and stays reported every pass.

## Snapshot Runtime Resolution

Owned by `src/snapshot/artifact_cache.rs`, `src/snapshot/runtime_support.rs`, and backend runtime resolvers.

| Artifact | Location | Contents | Purpose | Lifecycle | Rebuildable |
| --- | --- | --- | --- | --- | --- |
| Runtime rootfs config | `<snapshot_local_cache_path>/runtime/{snapshot_id}/rootfs/image.json` | Node-local overlaybd config for committed rootfs layers | Launch committed snapshot on the current node | Materialized during snapshot resolve and pinned by `RunnableSnapshot` lease. LRU-evictable after unpinned. | Yes. |
| Runtime memory config | `<snapshot_local_cache_path>/runtime/{snapshot_id}/memory/image.json` | Node-local overlaybd config for committed memory layers | Provides memory backend image to Firecracker resume | Materialized during snapshot resolve and pinned by lease. LRU-evictable after unpinned. | Yes. |
| Runtime attached-drive configs | `<snapshot_local_cache_path>/runtime/{snapshot_id}/drives/{drive_id}/image.json` | Node-local overlaybd configs for attached drives | Launch committed snapshot with attached drives | Materialized during snapshot resolve and pinned by lease. LRU-evictable after unpinned. | Yes. |
| OSS cached VM state | `<snapshot_local_cache_path>/artifacts/{id}/vm_state.bin` | Downloaded VM state object | Avoids repeated object-store download while pinned/cached | Downloaded by OSS resolver through `LocalArtifactCache`. LRU-evictable after unpinned. | Yes, by downloading again. |
| POSIX VM state reference | Repository `vm_state.bin` path | Direct path into POSIX repository | Avoids copying VM state into local runtime cache | Checked during resolve. Lifetime follows repository artifact. | No; repository artifact is source. |

`LocalArtifactCache` owns pinning, in-flight materialization deduplication, and LRU eviction for files it manages. It does not own the durable snapshot repository.

## Orchestrator Persistence

Owned by `src/orchestrator/persistence/*`.

| Artifact | Location | Contents | Purpose | Lifecycle |
| --- | --- | --- | --- | --- |
| Paused sandbox record DB | `<persisted_sandbox_store_path>/records.db` | RocksDB keyed by sandbox ID; values are compact JSON records containing version, lifecycle, sandbox metadata, artifact root, and backend state | Restores paused sandboxes after server restart | Record written after pause succeeds. Marked `resuming` before resume. Rolled back on resume failure. Deleted on resume/delete. A record whose runtime state cannot be decoded on the loading node is **retained**, not deleted; see "Load outcome". |
| Paused sandbox artifact generation | `<persisted_sandbox_store_path>/artifacts/{sandbox_id}/{uuid}/...` | Firecracker snapshot artifacts generated by `pause_to_dir` | Durable artifacts for one paused sandbox generation | Allocated before pause, populated by backend, referenced by paused record. Each pause adds a generation whose unchanged files are hard links of the previous ones. Removed only by an explicit delete (a control-plane delete removes `artifacts/{sandbox_id}` and any quarantined copy); boot never deletes one. A generation whose record the loading node does not have is moved to `artifacts/.quarantine/{epoch}/{sandbox_id}` at boot, preserving its bytes; once the new record is durable, the persister also sweeps stale sibling generations of regular files whose link count is 1. A periodic sweep in `src/maintenance/paused_artifacts.rs` reclaims the whole `{sandbox_id}` directory of a sandbox no record and no cluster list names any more, moving it to `artifacts/.trash/{epoch}/` first and deleting it only after a second grace; see "Paused Artifact Reaping". |
| Paused sandbox artifact quarantine | `<persisted_sandbox_store_path>/artifacts/.quarantine/{epoch}/{sandbox_id}/...` | A generation boot found with no record on this node | Keeps a paused sandbox's bytes when the record is on another node, was lost in an eviction, or was never written | Written only by `load_all`, as a rename: boot never copies and never deletes. Expired only by the reaper in enforce mode, once the epoch is older than `QUARANTINE_TTL` (7 days), or removed by an explicit control-plane delete of that sandbox id. Quarantine preserves bytes, not resumability: the cluster does not list the sandbox until a record exists, and `FirecrackerPausedState::decode` rebuilds the snapshot config from the record's state, not from the artifact path. |

Paused sandbox artifacts are not committed snapshot repository artifacts. They are owned by the sandbox persister and should not be shared as snapshot truth. Only an explicit delete removes an `artifacts/{sandbox_id}/` directory; a destroy that answered `404`, a record lost between `allocate_artifact_root` and `persist_paused`, and the last generation of a sandbox that has since resumed all strand it, and boot quarantines rather than deletes it; `src/maintenance/paused_artifacts.rs` sweeps those directories periodically, reclaiming one only when no keep condition holds — no local id (running, paused, restoring or template build), no restore open, the id absent from the gateway's aggregated `GET /v2/sandboxes`, and an mtime older than `ARTIFACT_GRACE` — while a symlink, a non-directory, a non-canonical name or a path outside `artifacts/` is skipped and never followed. The removal is two-phase: a reclaimed directory is renamed to `artifacts/.trash/{epoch}/{sandbox_id}`, which frees nothing, and deleted by a later pass once the epoch is older than `TRASH_GRACE`, so a mistaken verdict can be renamed back while its record still resolves; bytes are counted only on the deletion. A third phase empties the boot quarantine, `artifacts/.quarantine/{epoch}/{sandbox_id}`, under the same rules and only once the epoch is older than `QUARANTINE_TTL`; report mode counts it and deletes nothing. Neither the boot path nor the reaper deletes without one of those two explicit decisions. An unconfigured gateway, a failed node in the fan-out or a missing page degrades the whole pass to a report, which moves and deletes nothing, quarantine included. `AENV_ORCHESTRATOR_PAUSED_ARTIFACT_REAP` selects `off`, `report` or `enforce` and defaults to `off`; each verdict has a named counter (`agentenv_paused_sandbox_artifact_reap_{kept,skipped}_total` with a `reason`, `..._{moved,deleted,reclaimed_bytes}_total`, and `agentenv_paused_sandbox_quarantine_{expired,reclaimed_bytes}_total`), the sandbox id is a log field and never a label, and the `trashed`/`removed`/quarantine lines carry the id and the bytes.

A sandbox's chain of generations is bounded by two independent limits. The absolute `orchestrator.chain_bytes_ceiling` bounds the reclaimable part of a chain — the runtime-owned overlaybd lowers plus the layer the pause appends, allocated bytes deduplicated by device and inode so hard links count once — because that is the part a sandbox's own pauses grow. The immutable stable prefix is reported but never charged there: it is materialized once from the image and every sandbox of that image points at the same node-local files, so charging it per sandbox would count one node's bytes once per sandbox. The second, relative limit is `orchestrator.rootfs_chain_disk_multiple`: it judges the whole deduplicated rootfs chain, stable prefix included, against a small measured multiple of the guest rootfs disk (default `2`). Only the rootfs chain carries it; memory, drive and volume keep the absolute ceiling alone. When a pause would push either bound past its limit, the runtime-owned suffix is compacted into one complete generation. For disks of 48 GiB and above the absolute reclaimable ceiling usually fires first; for smaller disks the relative bound is the tighter one. The stale-generation sweep treats a regular file as unreferenced only when every hard link it has lives in the swept (stale) set, so an inode shared solely by two stale generations is freed and both directories can empty, while a file the newest generation, a fork child or a live runtime still reaches has a larger link count and is kept; non-regular files are never deleted.

A pause that still exceeds a limit after compaction fails with the corresponding stable code: `CHAIN_BYTES_CEILING_EXCEEDED` for the absolute bound, or `ROOTFS_CHAIN_DISK_MULTIPLE_EXCEEDED` when the whole rootfs chain is still above `multiple × guest disk`. Both failures are **recoverable on every device** — rootfs, drive and volume alike. Pause is a capacity optimization: losing the VM loses the sandbox's work. The rootfs and drive restacks mutate the live runtime before the size is known, so a limit refusal is the one post-restack error that is deliberately not terminal; the pause path resumes the paused-in-memory VM in place, reinserts the handle, restores the proxy route and leaves the sandbox `Running`. Every other post-restack error (a lost restack reply, a descriptor size mismatch) stays terminal, because the runtime there is genuinely unknown. Before the restack, a pre-flight measures the live chain plus the writable upper's allocated bytes against both enabled bounds; when that already fits, the post-compaction rejection is provably moot and is skipped, and the pre-flight never refuses on its own (its number is an upper bound, not a floor). A rejected pause cleans up the artifacts it wrote, but never frees a layer a live runtime config references: the sealed layer is copied into the sandbox's snapshot tree and the config repointed before the generation directory is deleted, and a failure to retain it keeps the directory instead. Setting `chain_bytes_ceiling` to `0` disables the absolute bound, the pre-flight and the stale-generation sweep; `rootfs_chain_disk_multiple = 0` disables only the relative bound, restoring the unbounded behavior without a redeploy.

Each refusal increments `agentenv_paused_sandbox_chain_ceiling_exceeded_total{kind}`, emits one backend log line with the stable code, the observed bytes, the limit, the appended-upper bytes and the artifact root's free/total bytes, and one orchestrator line with the node's paused-sandbox count; the seven-day measurement reads both through the named code. Every pause also records `agentenv_paused_sandbox_chain_limit_ratio{kind,limit}`, the measured bytes divided by the limit, so the seven-day check can assert that no post-rollout rootfs pause ran above its bound without re-deriving the accounting. The sandbox id is a log field, never a metric label.

### Load outcome

`load_all` decides one outcome per paused record and records it in
`agentenv_paused_sandbox_load_total{outcome}`. The label values are a closed set,
so a sandbox ID never becomes a label:

| `outcome` | Meaning | Record kept | Artifacts kept |
| --- | --- | --- | --- |
| `loaded_from_local` | Runtime state decoded and its artifacts are on this node. | Yes | Yes |
| `retained_unreachable` | The record is readable but this node holds no resumable runtime state for it — the artifacts are on another node, or its virtualization mode differs. It stays listed as `Paused` with no runtime state. | Yes | Yes |
| `quarantined_no_record` | An `artifacts/{sandbox_id}` directory that no record on this node names. It is renamed to `artifacts/.quarantine/{epoch}/{sandbox_id}` and kept. | No | Yes, moved to the quarantine |
| `discarded` | The record is not a paused record at all, or a resume was abandoned in `resuming`. This is the only branch that deletes a record. | No | No for the `resuming` branch, which removes both; an undecodable record is removed alone, so its directory is then quarantined by name |

The distinction matters because a node that cannot decode a paused record is
exactly what a recreated pod looks like when the artifacts live elsewhere.
Deleting that record is what made a pod swap permanent loss. The same reasoning
applies to the artifacts: a generation with no record can be a pause whose
record never reached the store, or one whose record sits on another node, so the
boot path moves it to the quarantine instead of deleting it. The boot summary
line reports `quarantined` and `quarantine_failed` beside the outcome tallies; a
quarantine step that cannot rename logs the sandbox id and both paths and leaves
the directory in place, so a failed move is never a loss.

**Acceptance query.** Zero paused sandboxes may be lost across a node
replacement, so the discarded branch is the one to watch:

```promql
sum by (node_id) (increase(agentenv_paused_sandbox_load_total{outcome="discarded"}[7d]))
```

It must stay at `0` for the observation window. The alert fires above zero, on the
reasoning that a single discarded record is already a lost sandbox and the
discarded branch should only ever see genuinely corrupt bytes. A run that
discards one record is a bug report, not a threshold to tune.

The boot must also stop deleting artifact generations, which is a different
signal: a non-zero `quarantined_no_record` is the expected outcome of a pod swap
on a node that still holds the SSD, not an incident. The pair to watch together
is the quarantine growth (it is inert until the reaper runs in enforce mode) and
the absence of any new `discarded`:

```promql
sum by (node_id) (increase(agentenv_paused_sandbox_load_total{outcome="quarantined_no_record"}[24h]))
```

A window with `discarded` still at `0` and a non-zero
`quarantined_no_record` is the card's scenario handled: the generations survived
the recreated pod. If `quarantined_no_record` stays at zero across a pod swap
whose boot log reports no loss, the artifacts were not on the node at all and
the quarantine path was never exercised.

The complementary signal, that the bucket path is actually being used, is:

```promql
sum by (node_id) (increase(agentenv_paused_sandbox_load_total{outcome="loaded_from_local"}[1h]))
```

A node that keeps reporting only `loaded_from_local` across pod swaps is still
serving from the SSD, so its bucket path is not exercised.

### Where a paused record and its artifacts live

Both live in the same directory, and that is what makes a recreated pod on the
same node find one with the other. The store root is
`persisted_sandbox_store_path`, default `$AENV_HOME/persisted-sandboxes`
(`src/cfg.rs`); `$AENV_HOME` is `home_path`, which `AENV_HOME_PATH` overrides
because the environment is the first source the config builder adds
(`src/cfg.rs`, `Config::builder().env().file(path)`; see
[Configuration reference](../configuration/reference.md)). In the runtime image
`AENV_HOME_PATH=/workspace/env` (`deploy/docker/Dockerfile.agentenv`), and
`/workspace` is the DaemonSet's `hostPath` `/var/lib/aenv`
(`deploy/k8s/base/agentenv-daemonset.yaml`) — the node's local SSD. So on a
cluster node:

- the record DB is `/workspace/env/persisted-sandboxes/records.db`;
- the generations are
  `/workspace/env/persisted-sandboxes/artifacts/{sandbox_id}/{uuid}/...`;
- both are on the local SSD and survive a pod recreation on the same node; no
  paused record is ever written to an `emptyDir` or to the container overlay.

The record therefore cannot disappear in an eviction while its generation
stays: a generation found without a record was either never given one or had it
removed. Two paths produce that state, and both run against the same SSD. The
first is resume, which deletes the record and keeps the generation
(`launch_sandbox` calls `delete_record`, not `delete_record_and_artifacts`);
nothing reclaims the directory while `paused_artifact_reap` is at its default
`off`. The second is an eviction inside the window between
`allocate_artifact_root` and `persist_paused`. The log line that separates them
for one sandbox id is `persisting paused sandbox`: present in the evicted pod's
log means its pause reached the record write, absent means it did not. The lv72
measurement of 05/11/2026 found five generations and three records: the replica
that replaced the evicted pod read the three and reported `loaded=3 retained=3`,
so the record DB was intact on the SSD and no record was lost with the pod —
and a record write is fsynced before it is acknowledged, so a record cannot be
torn. The five were generations of sandboxes that had already resumed.
Whichever of the two produced them, the boot path now quarantines them instead
of deleting them.

## P2P Artifact Transport

Owned by `src/p2p/*`.

| Artifact | Location | Contents | Purpose | Lifecycle |
| --- | --- | --- | --- | --- |
| Iroh blob store | `<p2p.store_dir>/iroh` | `iroh-blobs` content-addressed data and internal metadata | Local store for published artifacts and cached fetches | Created on P2P transport init, grows on publish and successful remote fetch. These blobs are collected by GC after unpublish removes those tags. |
| P2P catalog DB | `<p2p.store_dir>/iroh/catalog.db` | RocksDB keyed by artifact key; values are compact JSON artifact descriptors | Lets peers resolve stable keys into descriptors for this node | Loaded on startup. Individual entries are upserted on publish and after successful remote fetch, and deleted on unpublish. |

Snapshot publication writes best-effort P2P catalog entries after repository commit:

- `snapshot/v1/artifacts/{snapshot_id}/vm_state.bin`
- `snapshot/v1/artifacts/{snapshot_id}/firecracker-manifest.json`
- `overlaybd-layer/v1/sha256:<digest>` for referenced rootfs, memory, and attached-drive overlaybd commit layers

Those P2P entries are optional copies, not committed snapshot truth. Clearing `<p2p.store_dir>` can reduce peer-to-peer availability and force peers back to OSS or origin-registry reads, but it must not make a committed snapshot invalid by itself.

## Ublk Daemon Runtime

Owned by `storage/ublk-daemon/*` and `src/sandbox/ublk/*`.

| Artifact | Location | Contents | Purpose | Lifecycle | Rebuildable |
| --- | --- | --- | --- | --- | --- |
| Overlaybd runtime image config | caller-provided runtime dir `image.json` | Rewritten overlaybd config with runtime-relative lower and upper paths | Opens ublk overlaybd target | Materialized during `CreateOverlaybdRuntimeDevice`. Removed during rollback or work dir cleanup. | Yes from source image config while not running. |
| Runtime upper data | runtime dir `upper.data` | Writable overlaybd upper data file | Stores live writes for writable devices | Created for writable runtime if source config has no existing upper. Restacked into snapshot layer on pause/snapshot. | No while running. |
| Runtime upper index | runtime dir `upper.index` | Log-structured upper index | Resolves writes in `upper.data` | Created with log-structured writable upper. Restacked into sealed snapshot layer. | No while running. |
| Runtime result file | runtime dir `result.txt` | Overlaybd result file path | Overlaybd runtime convention | Created/used by overlaybd runtime. Removed during cleanup. | Yes. |
| Ublk daemon socket | default `$AENV_RUNTIME/ublk-daemon.sock` | Unix socket | IPC between AgentENV server and ublk daemon | Created when daemon starts; removed/replaced by process lifecycle. | Yes. |
| Ublk daemon log | configured `ublk.daemon_log_path`, default `$AENV_HOME/logs/ublk-daemon.log` | Daemon logs | Debugging | Appended by the daemon; the deployment owns rotation and retention. | No, but disposable. |

## Overlaybd Storage

Owned by `storage/overlaybd/*`.

| Artifact | Location | Contents | Purpose | Lifecycle | Rebuildable |
| --- | --- | --- | --- | --- | --- |
| Remote block cache | configured `cacheConfig.cacheDir`, derived from `<image.cache.root_dir>/remote-blocks` | Cached registryfs_v2 block ranges | Speeds remote overlaybd-native layer reads | Managed by overlaybd cache settings. | Yes. |
| Offline C++ tool block caches | `<image.cache.root_dir>/convert-blocks` (`overlaybd-apply`), `<image.cache.root_dir>/resize-blocks` (`overlaybd-resize`) | C++ file-cache entries for the offline tools | Keeps C++ cache eviction (truncate+unlink of flat cacheDir files) away from the Rust runtime cache's per-entry directories | Owned and evicted by the C++ tools via their dedicated generated global configs; download is disabled in those configs. Never shared with the Rust runtime `remote-blocks` cache. | Yes. |
| Premerged index cache | `cacheConfig.cacheDir/premerged-index/*.pmidx` | Serialized merged read-only lower index | Speeds opening repeated lower stacks | Written asynchronously on read-only open. Pruned by size limit derived from cache size. | Yes. |
| Sealed overlaybd commit files | Various owner paths: image cache, snapshot dirs, repository managed layers | Overlaybd layer data and index trailer | Immutable lower layers for block devices and memory images | Lifecycle is owned by the module that stores the file. Overlaybd only defines the format and open/merge behavior. | Depends on owner. |

## Ownership Rules

- Snapshot repository artifacts are durable user-visible state. Do not delete them from node-local GC code.
- Paused sandbox artifacts are durable only for the paused sandbox that owns them. Do not treat them as committed snapshots.
- Runtime `image.json` files under snapshot local cache or ublk runtime dirs are derived artifacts. They should be rebuildable from committed metadata or source image configs.
- Node-local `image-cache` artifacts are disposable cache, but content-addressed commits may be expensive to regenerate. The metadata-backed GC only reclaims commits no longer rooted by on-disk source configs, held by image-cache leases, or referenced by the in-process running set. Committed snapshots are durable repository state and do not pin ImageCache commits. See `[image.cache.gc]` in the configuration reference for scheduling.
- P2P store contents are node-local and optional. Unpublish removes the local catalog entry and related blobs; clearing the store only affects peer-to-peer sharing and may require republishing or refetching artifacts.
- Logs and dependency downloads are operational artifacts. They are not part of sandbox or snapshot correctness, but may require separate retention policy outside image-cache GC.
