//! Deferring paused-chain compaction outside the pause call.
//!
//! A pause that would cross `orchestrator.chain_bytes_ceiling` (or the rootfs
//! `rootfs_chain_disk_multiple`) used to compact the runtime-owned overlaybd
//! suffix synchronously, paying the whole rewrite before returning. The measured
//! cost of that rewrite is tens of seconds; the pause only has to write what is
//! dirty. This module is the leaf that lets the pause record the work and lets
//! the maintenance worker do it afterwards:
//!
//! - [`DeferredCompactionDescriptor`] is the durable work order, stored as a
//!   sibling file inside the paused generation. It is an *optional sibling*: the
//!   paused record keeps its version and fields unchanged, and a runtime that
//!   does not know the descriptor ignores it, which is what keeps a generation
//!   written by the new version resumable by the previous one.
//! - [`DeferredCompactionGate`] is the node-wide admission state shared by the
//!   pause that registers work and the worker that runs it: how many compactions
//!   are in flight and how many transient bytes are already pending. The pause
//!   falls back to the inline path whenever admission cannot prove it is safe.
//!
//! `overlaybd` depends on neither `sandbox` nor `maintenance`, so both can import
//! this module without creating a top-level module cycle.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Context, Result};
use overlaybd::config::LayerConfig;
use serde::{Deserialize, Serialize};

/// File name of the durable work order inside one paused generation.
pub(crate) const DEFERRED_COMPACTION_FILE: &str = "deferred-compaction.json";

/// Counter of compactions performed outside the pause.
///
/// Deliberately separate from `agentenv_paused_sandbox_chain_compactions_total`:
/// reusing the inline series would erase the distinction the card measures —
/// how much compaction the pause itself paid for and how much the maintenance
/// worker absorbed. Labels stay bounded: `kind` is the output name and
/// `outcome` is one of the fixed strings below, never a sandbox id or a path.
pub(crate) const DEFERRED_COMPACTIONS: &str =
    "agentenv_paused_sandbox_deferred_chain_compactions_total";

/// Record one deferred-compaction decision or result.
pub(crate) fn record_deferred_compaction(kind: &str, outcome: &'static str) {
    metrics::counter!(
        DEFERRED_COMPACTIONS,
        "kind" => kind.to_string(),
        "outcome" => outcome
    )
    .increment(1);
}

/// Version of [`DeferredCompactionDescriptor`]. A reader refuses anything else
/// instead of guessing, so a future format cannot be executed by this worker.
pub(crate) const DESCRIPTOR_VERSION: u32 = 1;

/// Where one descriptor is in the two-pass lifecycle.
///
/// `Pending` means the compacted layer has not replaced its inputs yet. Once the
/// worker has swapped the config atomically, it marks the descriptor `Swapped`;
/// the inputs may only be unlinked by a *later* pass, while the record is still
/// `Paused`, so a resume that read the old config never loses a layer it named.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeferredCompactionState {
    Pending,
    Swapped,
}

/// The durable work order a pause leaves behind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeferredCompactionDescriptor {
    pub(crate) version: u32,
    /// Compaction output name the pause would have used (`kind` in metrics too).
    pub(crate) kind: String,
    /// Image config file, relative to the descriptor's own directory, that the
    /// worker swaps atomically.
    pub(crate) config_file: String,
    /// Compacted layer file name, relative to the descriptor's own directory.
    pub(crate) output_name: String,
    /// Layer files folded into the output, in chain order. Absolute paths so a
    /// descriptor written in a volume directory still names them unambiguously.
    pub(crate) compactable: Vec<String>,
    /// Virtual size frozen at the pause instant, the merged commit's bound.
    pub(crate) view_bytes: u64,
    /// The two bounds frozen at the pause instant. They are recorded so the
    /// worker logs the same numbers the pause measured, never a later default.
    pub(crate) reclaimable_ceiling: u64,
    pub(crate) whole_chain_ceiling: u64,
    /// Generation directory name, a bounded log field (never a metric label).
    pub(crate) generation: String,
    pub(crate) state: DeferredCompactionState,
}

impl DeferredCompactionDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kind: impl Into<String>,
        config_file: impl Into<String>,
        output_name: impl Into<String>,
        compactable: Vec<String>,
        view_bytes: u64,
        limits: (u64, u64),
        generation: impl Into<String>,
    ) -> Self {
        Self {
            version: DESCRIPTOR_VERSION,
            kind: kind.into(),
            config_file: config_file.into(),
            output_name: output_name.into(),
            compactable,
            view_bytes,
            reclaimable_ceiling: limits.0,
            whole_chain_ceiling: limits.1,
            generation: generation.into(),
            state: DeferredCompactionState::Pending,
        }
    }

    /// The descriptor path inside the directory that holds the image config.
    pub(crate) fn path_in(directory: &Path) -> PathBuf {
        directory.join(DEFERRED_COMPACTION_FILE)
    }

    pub(crate) fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("read deferred compaction descriptor {}", path.display()))?;
        let descriptor: Self = serde_json::from_slice(&bytes).with_context(|| {
            format!(
                "deserialize deferred compaction descriptor {}",
                path.display()
            )
        })?;
        anyhow::ensure!(
            descriptor.version == DESCRIPTOR_VERSION,
            "unsupported deferred compaction descriptor version {} at {}",
            descriptor.version,
            path.display()
        );
        Ok(descriptor)
    }

    /// Write the descriptor durably: temp file in the same directory, fsync, then
    /// rename. A reader sees the old descriptor or the new one, never a mix.
    pub(crate) fn write(&self, path: &Path) -> Result<()> {
        let bytes =
            serde_json::to_vec_pretty(self).context("serialize deferred compaction descriptor")?;
        write_file_atomically(path, &bytes, "deferred compaction descriptor")
    }

    /// The layers folded by the worker, in chain order.
    pub(crate) fn compactable_layers(&self) -> Vec<LayerConfig> {
        self.compactable
            .iter()
            .map(|file| LayerConfig {
                file: file.clone(),
                ..Default::default()
            })
            .collect()
    }
}

/// The swap point exposed to a caller: `(target, complete temp file)`.
pub(crate) type AtomicWriteHook = dyn Fn(&Path, &Path) + Send + Sync;

/// Write `bytes` to `path` through a temp file in the same directory, fsync it,
/// then rename it into place.
///
/// Used by the descriptor and by the worker's config swap. The rename is what
/// makes the swap atomic for a concurrent resume: a reader sees the whole old
/// file or the whole new one, and the layers the old one named only disappear in
/// a later pass, so a resume that read the old file keeps a reachable chain.
pub(crate) fn write_file_atomically(path: &Path, bytes: &[u8], description: &str) -> Result<()> {
    write_file_atomically_with_hook(path, bytes, description, None)
}

/// [`write_file_atomically`] with the swap point exposed to a caller.
///
/// The hook runs with the complete temp file already fsynced and the target
/// still holding its previous content, which is exactly the instant a
/// concurrent reader could see a mix if the swap were not a rename. Production
/// passes `None`; the deterministic swap test uses it to read both files at the
/// swap point instead of racing a reader.
pub(crate) fn write_file_atomically_with_hook(
    path: &Path,
    bytes: &[u8],
    description: &str,
    hook: Option<&AtomicWriteHook>,
) -> Result<()> {
    let parent = path.parent().with_context(|| {
        format!(
            "{description} path has no parent directory: {}",
            path.display()
        )
    })?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temp {description} in {}", parent.display()))?;
    temp.write_all(bytes)
        .with_context(|| format!("write temp {description} {}", temp.path().display()))?;
    temp.as_file()
        .sync_all()
        .with_context(|| format!("sync temp {description} {}", temp.path().display()))?;
    if let Some(hook) = hook {
        hook(path, temp.path());
    }
    let temp_path = temp.path().to_path_buf();
    temp.persist(path).map_err(|error| {
        anyhow::Error::new(error.error).context(format!(
            "persist {description} {} -> {}",
            temp_path.display(),
            path.display()
        ))
    })?;
    Ok(())
}

/// Node-wide admission state for deferred compactions.
///
/// One instance is shared by every pause on the node and by the worker that
/// drains the queue. The pause asks [`Self::admit`]; a refused admission is not
/// an error, it is the signal to take the inline path the pause took before this
/// mechanism existed, so no new refusal code is introduced.
#[derive(Debug, Default)]
pub struct DeferredCompactionGate {
    inner: Mutex<GateCounters>,
}

#[derive(Debug, Default)]
struct GateCounters {
    in_flight: usize,
    /// Descriptor path -> transient bytes the compaction of that descriptor
    /// needs. Registration is the durable queue's in-memory mirror; a restart
    /// rebuilds it from the descriptors as the worker scans them.
    queued: BTreeMap<PathBuf, u64>,
}

impl DeferredCompactionGate {
    /// The process-wide gate the pause and the maintenance task both use.
    pub(crate) fn global() -> Arc<Self> {
        static GLOBAL: OnceLock<Arc<DeferredCompactionGate>> = OnceLock::new();
        Arc::clone(GLOBAL.get_or_init(|| Arc::new(DeferredCompactionGate::default())))
    }

    #[cfg(test)]
    pub(crate) fn new_for_test() -> Arc<Self> {
        Arc::new(DeferredCompactionGate::default())
    }

    /// Whether the pause may leave the work to the worker.
    ///
    /// Fails closed on every input it cannot prove: a full concurrency budget, a
    /// pending-bytes budget that the new work would cross, or a filesystem probe
    /// that is absent. A refusal sends the pause down the inline path, where the
    /// pre-rewrite headroom check decides with the same numbers.
    pub(crate) fn admit(
        &self,
        descriptor_path: &Path,
        transient_bytes: u64,
        max_concurrent: usize,
        pending_budget: u64,
        free_bytes: Option<u64>,
    ) -> bool {
        let mut counters = self
            .inner
            .lock()
            .expect("deferred compaction gate poisoned");
        if counters.in_flight >= max_concurrent {
            return false;
        }
        let pending: u64 = counters.queued.values().copied().sum();
        if pending.saturating_add(transient_bytes) > pending_budget {
            return false;
        }
        if !free_bytes.is_some_and(|free| free >= transient_bytes) {
            return false;
        }
        counters
            .queued
            .insert(descriptor_path.to_path_buf(), transient_bytes);
        true
    }

    /// Record a descriptor the worker discovered on disk (boot or a later pass).
    pub(crate) fn register(&self, descriptor_path: &Path, transient_bytes: u64) {
        let mut counters = self
            .inner
            .lock()
            .expect("deferred compaction gate poisoned");
        counters
            .queued
            .insert(descriptor_path.to_path_buf(), transient_bytes);
    }

    /// Drop a descriptor the worker finished, failed to keep, or unlinked.
    pub(crate) fn complete(&self, descriptor_path: &Path) {
        let mut counters = self
            .inner
            .lock()
            .expect("deferred compaction gate poisoned");
        counters.queued.remove(descriptor_path);
    }

    pub(crate) fn begin_in_flight(&self) {
        let mut counters = self
            .inner
            .lock()
            .expect("deferred compaction gate poisoned");
        counters.in_flight += 1;
    }

    pub(crate) fn end_in_flight(&self) {
        let mut counters = self
            .inner
            .lock()
            .expect("deferred compaction gate poisoned");
        counters.in_flight = counters.in_flight.saturating_sub(1);
    }

    /// Transient bytes of every descriptor waiting in the queue.
    pub(crate) fn pending_transient_bytes(&self) -> u64 {
        let counters = self
            .inner
            .lock()
            .expect("deferred compaction gate poisoned");
        counters.queued.values().copied().sum()
    }

    #[cfg(test)]
    pub(crate) fn in_flight(&self) -> usize {
        let counters = self
            .inner
            .lock()
            .expect("deferred compaction gate poisoned");
        counters.in_flight
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(dir: &Path, kind: &str, compactable: &[PathBuf]) -> DeferredCompactionDescriptor {
        DeferredCompactionDescriptor::new(
            kind,
            "image.json",
            "managed-base.commit",
            compactable
                .iter()
                .map(|path| path.display().to_string())
                .collect(),
            4096,
            (0, 0),
            dir.file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default(),
        )
    }

    #[test]
    fn the_metric_name_is_the_one_the_card_requires() {
        assert_eq!(
            DEFERRED_COMPACTIONS,
            "agentenv_paused_sandbox_deferred_chain_compactions_total"
        );
    }

    #[test]
    fn descriptor_round_trips_through_the_durable_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let descriptor = descriptor(temp.path(), "rootfs", &[temp.path().join("a.commit")]);
        let path = DeferredCompactionDescriptor::path_in(temp.path());
        descriptor.write(&path).expect("write descriptor");

        let loaded = DeferredCompactionDescriptor::load(&path).expect("load descriptor");
        assert_eq!(loaded, descriptor);
        assert_eq!(loaded.state, DeferredCompactionState::Pending);
        assert_eq!(loaded.version, DESCRIPTOR_VERSION);
    }

    #[test]
    fn admission_refuses_a_full_concurrency_budget() {
        let gate = DeferredCompactionGate::new_for_test();
        let path = Path::new("/tmp/a/deferred-compaction.json");
        gate.begin_in_flight();
        assert!(
            !gate.admit(path, 0, 1, u64::MAX, Some(u64::MAX)),
            "a pause must not queue work while the concurrency budget is full"
        );
        gate.end_in_flight();
        assert!(gate.admit(path, 0, 1, u64::MAX, Some(u64::MAX)));
    }

    #[test]
    fn admission_refuses_when_the_pending_budget_would_be_crossed() {
        let gate = DeferredCompactionGate::new_for_test();
        let first = Path::new("/tmp/a/deferred-compaction.json");
        let second = Path::new("/tmp/b/deferred-compaction.json");
        assert!(gate.admit(first, 60, 4, 100, Some(u64::MAX)));
        assert_eq!(gate.pending_transient_bytes(), 60);
        assert!(
            !gate.admit(second, 60, 4, 100, Some(u64::MAX)),
            "60 + 60 transient bytes must not fit a 100 byte budget"
        );
        assert_eq!(gate.pending_transient_bytes(), 60);
        gate.complete(first);
        assert!(gate.admit(second, 60, 4, 100, Some(u64::MAX)));
    }

    #[test]
    fn admission_fails_closed_without_a_filesystem_probe_or_headroom() {
        let gate = DeferredCompactionGate::new_for_test();
        let path = Path::new("/tmp/a/deferred-compaction.json");
        assert!(!gate.admit(path, 10, 4, u64::MAX, None));
        assert!(!gate.admit(path, 10, 4, u64::MAX, Some(9)));
        assert!(gate.admit(path, 10, 4, u64::MAX, Some(10)));
    }
}
