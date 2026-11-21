//! Overlaybd-specific snapshot helpers for Firecracker sandboxes.
//!
//! During pause, [`restack_snapshot_overlaybd_rootfs`] either stages a
//! read-only runtime config as-is or asks the ublk daemon to
//! `close_seal + restack` the live upper before writing the persisted
//! snapshot config.
//!
//! Sealed layers always stay raw locally: compression, when enabled via
//! `[snapshot.publish_compression]`, happens once at publish time when the
//! snapshot repository uploads layers to OSS/ACR.
//!
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use firecracker_client::models::DirtyMemoryRanges;
use nix::unistd::Pid;
use overlaybd::backend::local::LocalFile;
use overlaybd::config::{ImageConfig, LayerConfig};
use overlaybd::index::{Segment, SegmentMapping};
use overlaybd::index_file::{compact_to_with_options, CompactOptions, CompactStats};
use overlaybd::virtual_file::VirtualFile;
use tracing::{debug, warn};
use uuid::Uuid;

use super::process_vm_reader::ProcessVmReader;
use crate::cfg::ConfigManager;
use crate::overlaybd::{
    compact_layers, compactable_allocated_bytes, compaction_transient_bytes, create_commit_args,
    DeferredCompactionDescriptor, DeferredCompactionGate, OverlaybdCompactOutput,
};
use crate::sandbox::ublk::{UblkDevice, UblkDeviceManager};
use crate::sandbox::SandboxCaptureError;

/// Base budget of runtime-owned overlaybd snapshot lowers before compaction
/// triggers; the effective budget shrinks as the stable prefix grows. Well
/// below the hard limit of 255 (`MAX_STACK_LAYERS`) in overlaybd.
const DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS: usize = 32;
const INHERITED_LAYERS_DIR: &str = "inherited-layers";
const MANAGED_BASE_LAYER_FILE: &str = "managed-base.commit";
const FIRECRACKER_DIRTY_PAGE_SIZE: u64 = 4096;
const OVERLAYBD_ALIGNMENT: u64 = 512;
const DIRECT_MEMORY_SNAPSHOT_COMPACTION_CONCURRENCY: usize = 32;
/// Allocation unit reported by `st_blocks` on every supported filesystem.
const ALLOCATION_BLOCK_SIZE: u64 = 512;
/// Named, recoverable failure code: the sandbox chain cannot be brought under
/// `orchestrator.chain_bytes_ceiling` because the part compaction can reclaim
/// (the runtime-owned lowers plus the layer this pause appends) is still above
/// it. The pause is rejected instead of being allowed to grow the chain, and
/// the sandbox keeps its runtime state.
pub(crate) const CHAIN_BYTES_CEILING_EXCEEDED: &str = "CHAIN_BYTES_CEILING_EXCEEDED";

/// Named, recoverable failure code: the whole deduplicated rootfs chain of one
/// sandbox (its stable prefix plus its runtime-owned generations) is still above
/// `orchestrator.rootfs_chain_disk_multiple` times the guest rootfs disk after
/// compaction. The pause is rejected and the sandbox resumed in place, exactly
/// like the absolute ceiling above; the two differ only in what they bound.
pub(crate) const ROOTFS_CHAIN_DISK_MULTIPLE_EXCEEDED: &str = "ROOTFS_CHAIN_DISK_MULTIPLE_EXCEEDED";

/// Which paused-chain bound a pause breached.
///
/// The two bounds answer different questions and are recoverable in exactly the
/// same way, so the concrete error carries the variant and every consumer reads
/// the stable code from [`PausedChainLimit::as_str`] instead of matching text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PausedChainLimit {
    /// The absolute cap on the reclaimable part (`chain_bytes_ceiling`).
    ChainBytesCeiling,
    /// The relative cap on the whole rootfs chain (`rootfs_chain_disk_multiple`).
    RootfsDiskMultiple,
}

impl PausedChainLimit {
    /// The stable, grep-able code carried by logs, metrics and the error.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ChainBytesCeiling => CHAIN_BYTES_CEILING_EXCEEDED,
            Self::RootfsDiskMultiple => ROOTFS_CHAIN_DISK_MULTIPLE_EXCEEDED,
        }
    }
}

/// The bounds a paused-chain rewrite enforces, each `0` meaning disabled.
///
/// `reclaimable_ceiling` is the absolute cap that existed before this type:
/// the part this sandbox's pauses add (runtime-owned lowers plus the appended
/// layer). `whole_chain_ceiling` is the relative cap the card adds: the whole
/// deduplicated chain, the shared stable prefix included, against a small
/// multiple of the guest rootfs disk. Only the rootfs chain sets the second one.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct ChainLimits {
    pub(super) reclaimable_ceiling: u64,
    pub(super) whole_chain_ceiling: u64,
}

impl ChainLimits {
    #[cfg(test)]
    pub(super) fn disabled() -> Self {
        Self::default()
    }

    /// Whether either bound is enforced.
    pub(super) fn enabled(self) -> bool {
        self.reclaimable_ceiling != 0 || self.whole_chain_ceiling != 0
    }
}

/// The first bound a measured chain breaches, with the numbers that name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ChainLimitBreach {
    pub(super) limit: PausedChainLimit,
    pub(super) observed_bytes: u64,
    pub(super) limit_bytes: u64,
}

/// What the byte ceiling bounds: the bytes the sandbox's own pauses add.
///
/// The ceiling exists so one sandbox's pause churn cannot fill a node's disk,
/// so it has to bound the part that grows pause by pause. The stable prefix is
/// the opposite of that part. It is materialized once from the image and every
/// sandbox of that image points at the same node-local files: the lowers reach
/// an image config through `OverlaybdLayerStore::layer_location(digest, ..)`,
/// which answers with a digest-addressed path under the image cache, and the
/// commit index hardlinks the cache seed from that same store, so two sandboxes
/// of one image share the inode, not just the path. Compaction cannot reclaim a
/// byte of it either. Charging it per sandbox therefore counts one node's bytes
/// once per sandbox, and at the default ceiling rejects a chain whose own growth
/// is far below the ceiling. `chain_allocated_bytes` deduplicates by inode
/// inside a single chain, so it could not correct that double count across
/// sandboxes; not charging the prefix at all does. What the ceiling judges is
/// the reclaimable part — the runtime-owned lowers plus the layer the pause
/// appends — and nothing else, so a reclaimable part that alone exceeds the
/// ceiling still fails by name.
///
/// The prefix is never a breach, however large it is. It is a function of the
/// image, not of this sandbox's pauses, so refusing a pause because of it would
/// fail a sandbox whose own growth is under the ceiling and never let it pause
/// again: the rootfs chain is restacked under the same ceiling on every pause,
/// and its image prefix alone is hundreds of megabytes, so any ceiling below
/// that prefix would terminally reject the pause of every sandbox of that image.
/// A node whose images outgrow the ceiling needs a different image, not a
/// sandbox that cannot stop.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct ChainByteAccounting {
    /// Bounded by the absolute ceiling: runtime-owned lowers plus the appended
    /// layer.
    pub(super) reclaimable_bytes: u64,
    /// Reported and only judged by the relative bound: immutable lowers shared
    /// across sandboxes.
    pub(super) stable_prefix_bytes: u64,
    /// The whole deduplicated chain, prefix included. The two parts are disjoint
    /// through the shared inode set, so this is exactly the `du` figure the card
    /// measures per generation.
    pub(super) total_bytes: u64,
}

impl ChainByteAccounting {
    /// Measure a chain plus the layer this pause appends, splitting it with the
    /// predicate compaction itself uses.
    ///
    /// Position cannot decide this: a runtime-owned lower can follow another
    /// runtime-owned lower, so the positional split only bounds the classified
    /// one from below. Classifying here keeps the ceiling, the compaction it
    /// triggers and the diagnostic report answering the same question.
    pub(super) fn measure(
        chain: impl IntoIterator<Item = LayerConfig>,
        appended_layer: Option<&LayerConfig>,
        runtime_owned_roots: &[PathBuf],
    ) -> Self {
        let (stable_prefix, runtime_owned) =
            split_chain_by_classification(chain, runtime_owned_roots);
        // One inode set for both parts: a layer hardlinked into the prefix and
        // the suffix belongs to the chain once, not once per part.
        let mut seen = std::collections::HashSet::new();
        let stable_prefix_bytes = chain_allocated_bytes_deduped(
            stable_prefix.iter().map(|lower| Path::new(&lower.file)),
            &mut seen,
        );
        let reclaimable_bytes = chain_allocated_bytes_deduped(
            runtime_owned
                .iter()
                .chain(appended_layer)
                .map(|lower| Path::new(&lower.file)),
            &mut seen,
        );
        Self {
            reclaimable_bytes,
            stable_prefix_bytes,
            total_bytes: stable_prefix_bytes.saturating_add(reclaimable_bytes),
        }
    }

    /// The first bound this chain breaches, or `None` when it fits.
    ///
    /// The absolute bound keeps judging only the reclaimable part: it is the
    /// part this sandbox's pauses add and the part compaction can shrink, and
    /// the shared prefix must never reject a pause on its own. The relative
    /// bound is the card's invariant and judges the whole deduplicated chain,
    /// prefix included; it is only enabled for the rootfs.
    pub(super) fn breach(self, limits: ChainLimits) -> Option<ChainLimitBreach> {
        if limits.reclaimable_ceiling != 0 && self.reclaimable_bytes > limits.reclaimable_ceiling {
            return Some(ChainLimitBreach {
                limit: PausedChainLimit::ChainBytesCeiling,
                observed_bytes: self.reclaimable_bytes,
                limit_bytes: limits.reclaimable_ceiling,
            });
        }
        if limits.whole_chain_ceiling != 0 && self.total_bytes > limits.whole_chain_ceiling {
            return Some(ChainLimitBreach {
                limit: PausedChainLimit::RootfsDiskMultiple,
                observed_bytes: self.total_bytes,
                limit_bytes: limits.whole_chain_ceiling,
            });
        }
        None
    }
}

/// Measured shape of a chain that the byte ceiling rejected.
///
/// The reclaimable total alone cannot tell a ceiling that was set too low from
/// a compaction that did not shrink the chain: both leave a generation above the
/// ceiling. Carrying the parts names the layers and the appended bytes the
/// rewrite folded, so the operator reads the shape that was refused instead of
/// only the number. The stable prefix travels with it because it explains how
/// much of the node's disk this sandbox does not own.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ChainCeilingBreakdown {
    /// Lowers left untouched by compaction, and their allocated bytes.
    pub stable_prefix_lowers: usize,
    pub stable_prefix_bytes: u64,
    /// Lowers compaction rewrote, and the allocated bytes of the layer it
    /// produced from them (0 when compaction produced nothing).
    pub compacted_lowers: usize,
    pub compacted_bytes: u64,
    /// Allocated bytes of the layer the pause itself appended, before it was
    /// folded into the compacted layer.
    pub appended_bytes: u64,
}

/// Ceiling rejection for one sandbox's paused artifact chain.
///
/// Kept as a concrete error so the pause path can classify it as recoverable
/// through `context` layers, instead of matching on message text. `limit` says
/// which bound refused, so the absolute ceiling and the rootfs disk multiple
/// stay distinguishable by type and code after every wrapper.
///
/// `observed_bytes` is the measured part the refusing bound judges: the
/// reclaimable part for [`PausedChainLimit::ChainBytesCeiling`], the whole
/// deduplicated chain for [`PausedChainLimit::RootfsDiskMultiple`].
#[derive(Debug)]
pub(crate) struct PausedChainLimitExceeded {
    pub limit: PausedChainLimit,
    pub observed_bytes: u64,
    pub limit_bytes: u64,
    pub breakdown: ChainCeilingBreakdown,
}

impl std::fmt::Display for PausedChainLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let breakdown = &self.breakdown;
        write!(
            f,
            "{}: paused sandbox chain needs {} bytes against the {} byte limit (compacted {} \
             lowers into {} bytes; appended layer: {} bytes; immutable stable prefix not charged: \
             {} lowers, {} bytes)",
            self.limit.as_str(),
            self.observed_bytes,
            self.limit_bytes,
            breakdown.compacted_lowers,
            breakdown.compacted_bytes,
            breakdown.appended_bytes,
            breakdown.stable_prefix_lowers,
            breakdown.stable_prefix_bytes
        )
    }
}

impl std::error::Error for PausedChainLimitExceeded {}

fn paused_chain_limit_exceeded(
    limit: PausedChainLimit,
    observed_bytes: u64,
    limit_bytes: u64,
    breakdown: ChainCeilingBreakdown,
) -> anyhow::Error {
    anyhow::Error::new(PausedChainLimitExceeded {
        limit,
        observed_bytes,
        limit_bytes,
        breakdown,
    })
}

/// A [`PausedChainLimitExceeded`] with no breakdown detail.
///
/// Test-only: the orchestrator's recoverable-pause branch classifies by error
/// type, and the mock backend needs to produce that exact type (never a message
/// that merely spells the code) for the branch to be exercised for real.
#[cfg(test)]
pub(crate) fn paused_chain_limit_error(
    limit: PausedChainLimit,
    observed_bytes: u64,
    limit_bytes: u64,
) -> anyhow::Error {
    paused_chain_limit_exceeded(
        limit,
        observed_bytes,
        limit_bytes,
        ChainCeilingBreakdown::default(),
    )
}

/// Whether `error` carries a [`PausedChainLimitExceeded`] anywhere in its
/// `context` chain.
///
/// The pause path wraps the rejection with context at several layers (the
/// restack of one device, the staging of its snapshot config, the pause
/// itself), so matching the concrete error type through the chain is the only
/// classification that survives that nesting. Matching on message text would
/// also reclassify an unrelated error that merely mentions the code.
pub(crate) fn is_paused_chain_limit_exceeded(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.downcast_ref::<PausedChainLimitExceeded>().is_some())
}

/// Named, recoverable failure code: compacting the runtime-owned suffix would
/// need more free bytes at the artifact root than the node has. The pause is
/// refused **before** the compaction starts, so the node never runs a rewrite
/// that would fill the disk half-way; the sandbox is resumed in place like the
/// chain-limit refusals.
pub(crate) const COMPACTION_HEADROOM_INSUFFICIENT: &str = "COMPACTION_HEADROOM_INSUFFICIENT";

/// Transient disk the compaction of one device needs, refused pre-emptively.
///
/// The merged commit is written beside its inputs and only renamed into place
/// when complete, so while the compaction runs both the inputs and the output
/// exist. The output is the union of the inputs' blocks, which is bounded by the
/// device's virtual size and by the sum of the inputs' allocated bytes, so the
/// smaller of the two is already an upper bound. `free_bytes` is `None` when the
/// filesystem probe failed — the refusal is then fail-closed, because starting
/// a rewrite without being able to prove headroom is exactly the half-way
/// failure this check exists to prevent.
#[derive(Debug)]
pub(crate) struct CompactionHeadroomInsufficient {
    pub required_bytes: u64,
    pub free_bytes: Option<u64>,
}

impl std::fmt::Display for CompactionHeadroomInsufficient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.free_bytes {
            Some(free_bytes) => write!(
                f,
                "{COMPACTION_HEADROOM_INSUFFICIENT}: compaction needs {required} bytes of free \
                 space at the artifact root but the filesystem reports {free} bytes; the pause is \
                 refused before the rewrite starts",
                required = self.required_bytes,
                free = free_bytes,
            ),
            None => write!(
                f,
                "{COMPACTION_HEADROOM_INSUFFICIENT}: compaction needs {required} bytes of free \
                 space at the artifact root but the filesystem probe failed; the pause is refused \
                 before the rewrite starts",
                required = self.required_bytes,
            ),
        }
    }
}

impl std::error::Error for CompactionHeadroomInsufficient {}

fn compaction_headroom_insufficient(required_bytes: u64, free_bytes: Option<u64>) -> anyhow::Error {
    anyhow::Error::new(CompactionHeadroomInsufficient {
        required_bytes,
        free_bytes,
    })
}

/// Whether `error` is a pause refusal the pause path can recover from by
/// resuming the VM in place.
///
/// Both refusals are capacity decisions taken after the device restack only
/// because their numbers are known then, and both leave the device usable, so
/// neither may become a terminal capture failure.
pub(crate) fn is_recoverable_pause_refusal(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<PausedChainLimitExceeded>().is_some()
            || cause
                .downcast_ref::<CompactionHeadroomInsufficient>()
                .is_some()
    })
}

/// The stable code carried by a recoverable pause refusal, when the error is
/// one.
///
/// The API layer reads the code from here so the node's HTTP response and the
/// backend log line name the refusal identically, without either of them
/// matching on message text.
pub(crate) fn pause_refusal_code(error: &anyhow::Error) -> Option<&'static str> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<PausedChainLimitExceeded>()
            .map(|exceeded| exceeded.limit.as_str())
            .or_else(|| {
                cause
                    .downcast_ref::<CompactionHeadroomInsufficient>()
                    .map(|_| COMPACTION_HEADROOM_INSUFFICIENT)
            })
    })
}

/// The chain measured **before** the pause mutates the live runtime.
///
/// A restack is the point of no return: it seals the writable upper and reopens
/// a fresh one, so the pause can only learn the size compaction produced after
/// the device has already been mutated. This pre-flight answers the cheaper
/// question first — does the chain plus the writable upper's allocated bytes
/// already fit the ceiling? Compaction only ever drops blocks, so when that
/// already fits, the post-compaction rejection cannot be the reason to refuse
/// and is skipped; the pause then never pays for a refusal it provably does not
/// need.
///
/// It is deliberately *not* a refusal. `measured_reclaimable_bytes` is an upper bound, not a
/// floor: the appended bytes are the writable upper's allocated blocks, and
/// compaction folds those same blocks into the generation it writes. Refusing on
/// an upper bound would reject pauses that would have fit, which is worse than
/// the recoverable refusal it replaces, so an over-ceiling pre-flight only logs
/// and lets the post-compaction check decide.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct ChainPreflight {
    /// Reclaimable chain bytes plus the writable upper's allocated bytes.
    pub(super) measured_reclaimable_bytes: u64,
    /// Whole deduplicated chain bytes plus the writable upper's allocated bytes.
    pub(super) measured_total_bytes: u64,
    /// The bounds the post-compaction check will enforce; disabled when neither
    /// is set.
    pub(super) limits: ChainLimits,
}

impl ChainPreflight {
    /// Ceiling disabled: nothing to pre-flight and nothing to skip.
    pub(super) fn disabled() -> Self {
        Self::default()
    }

    /// Measure the live chain plus its writable upper against both bounds.
    pub(super) fn measure(
        chain: impl IntoIterator<Item = LayerConfig>,
        upper_paths: &[PathBuf],
        runtime_owned_roots: &[PathBuf],
        limits: ChainLimits,
    ) -> Self {
        if !limits.enabled() {
            return Self::disabled();
        }
        let accounting = ChainByteAccounting::measure(chain, None, runtime_owned_roots);
        let upper_bytes = chain_allocated_bytes(upper_paths.iter().map(PathBuf::as_path));
        Self {
            measured_reclaimable_bytes: accounting.reclaimable_bytes.saturating_add(upper_bytes),
            measured_total_bytes: accounting.total_bytes.saturating_add(upper_bytes),
            limits,
        }
    }

    /// Whether the measured chain already fits every enabled bound, so
    /// compaction cannot push it past one and the post-compaction rejection is
    /// provably moot.
    pub(super) fn guaranteed_fit(&self) -> bool {
        if !self.limits.enabled() {
            return false;
        }
        let reclaimable_fits = self.limits.reclaimable_ceiling == 0
            || self.measured_reclaimable_bytes <= self.limits.reclaimable_ceiling;
        let total_fits = self.limits.whole_chain_ceiling == 0
            || self.measured_total_bytes <= self.limits.whole_chain_ceiling;
        reclaimable_fits && total_fits
    }
}

/// Measure a live runtime's chain plus its writable upper pair.
///
/// Reads the same config the restack is about to mutate, so the number belongs
/// to the chain the pause will actually append to. A config without a writable
/// upper answers [`ChainPreflight::disabled`]: there is nothing to append there
/// and the post-compaction check keeps deciding alone.
fn preflight_chain_before_restack(
    live_runtime_image_config_path: &Path,
    runtime_owned_roots: &[PathBuf],
    limits: ChainLimits,
) -> Result<ChainPreflight> {
    if !limits.enabled() {
        return Ok(ChainPreflight::disabled());
    }
    let image_config = overlaybd::config::load_image_config(live_runtime_image_config_path)
        .with_context(|| {
            format!(
                "load live overlaybd runtime config before restack {}",
                live_runtime_image_config_path.display()
            )
        })?;
    let mut upper_paths = Vec::new();
    for path in [&image_config.upper.data, &image_config.upper.index] {
        if !path.is_empty() {
            upper_paths.push(PathBuf::from(path));
        }
    }
    Ok(ChainPreflight::measure(
        image_config.lowers,
        &upper_paths,
        runtime_owned_roots,
        limits,
    ))
}

/// Byte ceiling observability for the paused artifact chain.
///
/// Labels stay bounded: `kind` is one of `rootfs` / `memory`, never a sandbox
/// id, so a node with thousands of sandboxes cannot explode cardinality.
mod chain_metrics {
    use super::{ChainByteAccounting, ChainLimits, PausedChainLimit};

    pub(super) const CHAIN_BYTES: &str = "agentenv_paused_sandbox_chain_bytes";
    /// How close every pause ran to each enabled bound. Labels stay bounded:
    /// `kind` is the compaction output name the existing chain-bytes gauge
    /// already uses (`managed-base.commit` for a rootfs/drive/volume suffix,
    /// `mem_compacted.commit` for memory) and `limit` is one of the two stable
    /// codes, never a sandbox id or a path.
    pub(super) const LIMIT_RATIO: &str = "agentenv_paused_sandbox_chain_limit_ratio";
    pub(super) const COMPACTIONS: &str = "agentenv_paused_sandbox_chain_compactions_total";
    pub(super) const CEILING_EXCEEDED: &str =
        "agentenv_paused_sandbox_chain_ceiling_exceeded_total";
    /// Refusals taken after compaction could not bring the reclaimable part
    /// under the ceiling — the ones the pause path turns into a resume.
    pub(super) const CEILING_EXCEEDED_POST_COMPACTION: &str =
        "agentenv_paused_sandbox_chain_ceiling_exceeded_post_compaction_total";
    /// Pauses whose pre-flight already fit the ceiling, so the post-compaction
    /// rejection was skipped because it provably could not fire.
    pub(super) const PREFLIGHT_GUARANTEED_FIT: &str =
        "agentenv_paused_sandbox_chain_preflight_guaranteed_fit_total";
    /// Pauses refused before a compaction because the artifact root could not
    /// hold the merged output beside its inputs.
    pub(super) const COMPACTION_HEADROOM_REFUSED: &str =
        "agentenv_paused_sandbox_compaction_headroom_refused_total";

    pub(super) fn record_chain_bytes(kind: &'static str, bytes: u64, ceiling: u64) {
        metrics::gauge!(CHAIN_BYTES, "kind" => kind).set(bytes as f64);
        metrics::gauge!(CHAIN_BYTES, "kind" => "ceiling").set(ceiling as f64);
    }

    /// Record the ratio every enabled bound judges, so the seven-day check can
    /// assert that no post-rollout rootfs pause ran above its limit without
    /// re-deriving the accounting.
    pub(super) fn record_limit_ratios(
        kind: &'static str,
        accounting: ChainByteAccounting,
        limits: ChainLimits,
    ) {
        if limits.reclaimable_ceiling != 0 {
            metrics::histogram!(
                LIMIT_RATIO,
                "kind" => kind,
                "limit" => PausedChainLimit::ChainBytesCeiling.as_str()
            )
            .record(accounting.reclaimable_bytes as f64 / limits.reclaimable_ceiling as f64);
        }
        if limits.whole_chain_ceiling != 0 {
            metrics::histogram!(
                LIMIT_RATIO,
                "kind" => kind,
                "limit" => PausedChainLimit::RootfsDiskMultiple.as_str()
            )
            .record(accounting.total_bytes as f64 / limits.whole_chain_ceiling as f64);
        }
    }

    pub(super) fn record_compaction(kind: &'static str) {
        metrics::counter!(COMPACTIONS, "kind" => kind).increment(1);
    }

    pub(super) fn record_ceiling_exceeded(kind: &'static str) {
        metrics::counter!(CEILING_EXCEEDED, "kind" => kind).increment(1);
        metrics::counter!(CEILING_EXCEEDED_POST_COMPACTION, "kind" => kind).increment(1);
    }

    pub(super) fn record_preflight_guaranteed_fit(kind: &'static str) {
        metrics::counter!(PREFLIGHT_GUARANTEED_FIT, "kind" => kind).increment(1);
    }

    pub(super) fn record_compaction_headroom_refused(kind: &'static str) {
        metrics::counter!(COMPACTION_HEADROOM_REFUSED, "kind" => kind).increment(1);
    }

    /// The pause left the rewrite to the maintenance worker. The single metric
    /// definition lives in the `overlaybd` leaf so the worker records the same
    /// series.
    pub(super) fn record_deferred_compaction(kind: &str, outcome: &'static str) {
        crate::overlaybd::record_deferred_compaction(kind, outcome);
    }
}

/// The five numbers every ceiling refusal logs, plus the artifact root's disk.
///
/// This is the single place that decides what an operator sees when a pause is
/// refused, so the seven-day measurement of the card has one query to read and
/// one shape to parse: observed chain bytes, the ceiling, the appended-upper
/// bytes, the node's free/total bytes at the artifact root, and how many
/// sandboxes the node is holding paused. The sandbox id is never a field here —
/// it would make the line unusable as a stable grep — so the caller adds it
/// where the log line is emitted if at all.
mod chain_refusal_log {
    use std::path::Path;

    /// Free and total bytes of the filesystem holding `path`, if it can be read.
    ///
    /// Missing and unreadable paths answer `None` rather than failing: a refusal
    /// must still be reported when the disk probe fails, and the message says the
    /// numbers are absent instead of inventing them.
    pub(super) fn filesystem_space(path: &Path) -> Option<(u64, u64)> {
        let stats = nix::sys::statvfs::statvfs(path).ok()?;
        let block_size = stats.fragment_size();
        let total_bytes = stats.blocks().saturating_mul(block_size);
        let free_bytes = stats.blocks_free().saturating_mul(block_size);
        Some((free_bytes, total_bytes))
    }
}

/// Allocated bytes of a layer chain, deduplicated by device and inode so a
/// hardlinked generation is counted once instead of once per path.
///
/// Remote lowers have no local file and contribute no local bytes; a local
/// layer that cannot be inspected is skipped with a warning rather than
/// failing the pause, which can only under-report (never over-report) the
/// chain and therefore never triggers a spurious compaction.
fn chain_allocated_bytes<'a>(paths: impl IntoIterator<Item = &'a Path>) -> u64 {
    let mut seen = std::collections::HashSet::new();
    chain_allocated_bytes_deduped(paths, &mut seen)
}

/// [`chain_allocated_bytes`] against a caller-owned inode set.
///
/// The two parts of a chain are measured separately, so a layer that is
/// hardlinked into both — the prefix and the runtime suffix can share an inode
/// when a pause adopts a prefix layer by link — would be counted once in each
/// part and twice in their sum. Sharing one `seen` set across the parts keeps
/// the chain-wide deduplication the single-call form already guarantees.
fn chain_allocated_bytes_deduped<'a>(
    paths: impl IntoIterator<Item = &'a Path>,
    seen: &mut std::collections::HashSet<(u64, u64)>,
) -> u64 {
    let mut total = 0u64;
    for path in paths {
        if path.as_os_str().is_empty() {
            continue;
        }
        let metadata = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) => {
                warn!(
                    path = %path.display(),
                    error = %error,
                    "skipping unreadable overlaybd layer while measuring the paused chain"
                );
                continue;
            }
        };
        if !seen.insert((metadata.dev(), metadata.ino())) {
            continue;
        }
        total = total.saturating_add(metadata.blocks().saturating_mul(ALLOCATION_BLOCK_SIZE));
    }
    total
}

/// Split a chain into the lowers compaction may rewrite and the ones it must
/// leave untouched, using the very predicate
/// `FirecrackerSandbox::describe_chain_classification` prints.
///
/// Position in the image config cannot decide this. `split_runtime_suffix` stops
/// at the first runtime-owned lower and hands the rest over wholesale, which is
/// the right unit to adopt but the wrong one to blame for bytes: on a chain whose
/// lowers the diagnostic reports as pause-owned end to end, the suffix is
/// everything and the ceiling would still be judged against a "stable prefix"
/// that does not exist. Classifying each lower keeps the ceiling, the compaction
/// it triggers and the diagnostic report all answering the same question.
fn split_chain_by_classification(
    lowers: impl IntoIterator<Item = LayerConfig>,
    runtime_owned_roots: &[PathBuf],
) -> (Vec<LayerConfig>, Vec<LayerConfig>) {
    lowers
        .into_iter()
        .partition(|lower| !is_runtime_owned(Path::new(&lower.file), runtime_owned_roots))
}

enum LiveOverlaybdSnapshotState {
    ReadOnly,
    Restacked(PathBuf),
}

impl LiveOverlaybdSnapshotState {
    fn snapshot_layer_path(&self) -> Option<&Path> {
        match self {
            Self::ReadOnly => None,
            Self::Restacked(path) => Some(path.as_path()),
        }
    }

    /// Classify a failed staging attempt after the restack, if there was one.
    ///
    /// A restack mutates the live runtime (it seals the old upper and reopens a
    /// fresh one), so every error after it is terminal by default: the caller
    /// must not keep serving a runtime whose device state is unknown. The one
    /// exception is the chain byte ceiling. It is a capacity decision taken
    /// after the restack only because the reclaimed size is only known then,
    /// and the daemon leaves both the device and the resumed VM usable — the
    /// VM is still paused in memory at that point. Returning it unwrapped makes
    /// [`crate::sandbox::SandboxCaptureError::from`] classify it as
    /// recoverable, so the pause path resumes the VM instead of tearing it
    /// down. Every other post-restack error keeps the terminal classification.
    fn finish_staging(self, staged_snapshot: Result<PathBuf>) -> Result<PathBuf> {
        match self {
            Self::ReadOnly => staged_snapshot,
            Self::Restacked(_) => staged_snapshot.map_err(|error| {
                if is_recoverable_pause_refusal(&error) {
                    debug!(
                        "keeping a post-restack pause refusal recoverable so the sandbox is \
                         resumed instead of stopped"
                    );
                    error
                } else {
                    into_terminal_snapshot_failure(error)
                }
            }),
        }
    }
}

fn into_terminal_snapshot_failure(err: anyhow::Error) -> anyhow::Error {
    SandboxCaptureError::terminal(err).into()
}

fn local_layer_config(path: &Path) -> LayerConfig {
    local_layer_config_with_descriptor(path, None)
}

fn local_layer_config_with_descriptor(
    path: &Path,
    descriptor: Option<&overlaybd::LayerDescriptor>,
) -> LayerConfig {
    LayerConfig {
        file: path.display().to_string(),
        digest: descriptor
            .map(|descriptor| descriptor.digest.clone())
            .unwrap_or_default(),
        size: descriptor.map(|descriptor| descriptor.size).unwrap_or(0),
        ..Default::default()
    }
}

/// The node-local artifact store the persister allocates paused generations from.
///
/// The only root that cannot be derived from the pause itself: it is chosen by
/// the orchestrator's persister and reaches a sandbox only through this config
/// value. The sandbox's own snapshot tree is deliberately not resolved here —
/// `FirecrackerSandbox::runtime_owned_roots` takes it from the very directory
/// the pause writes into.
pub(super) fn persisted_sandbox_store_root() -> PathBuf {
    ConfigManager::global_config()
        .orchestrator
        .persisted_sandbox_store_path
        .clone()
}

/// Whether `path` lives under one of `roots`, compared in canonical form.
///
/// A root captured before its directory exists cannot be canonicalized (the
/// directory is created on the first pause), so canonicalization has to happen
/// here, at comparison time, for the root as well. Comparing a raw root against
/// a canonicalized lower silently never matches under a symlinked or relative
/// prefix: every pause-created lower then looks like immutable stable prefix,
/// compaction only re-commits the newest generation, and the byte ceiling
/// eventually rejects the pause outright.
pub(super) fn is_runtime_owned(path: &Path, runtime_owned_roots: &[PathBuf]) -> bool {
    let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    runtime_owned_roots.iter().any(|root| {
        let root = fs::canonicalize(root).unwrap_or_else(|_| root.clone());
        canonical.starts_with(&root)
    })
}

/// Split a list of overlaybd lowers into two parts: the prefix of stable lowers and the suffix of runtime-owned lowers.
///
/// Runtime-created local lowers are appended during resume/fork/pause.
/// Published or cache-owned layers must be materialized before that
/// suffix; otherwise adopting only the tail would either leave a dangling
/// runtime artifact reference or change lower precedence.
fn split_runtime_suffix(
    mut lowers: Vec<LayerConfig>,
    runtime_owned_roots: &[PathBuf],
) -> (Vec<LayerConfig>, Vec<LayerConfig>) {
    let Some(first_runtime_owned_index) = lowers
        .iter()
        .position(|lower| is_runtime_owned(Path::new(&lower.file), runtime_owned_roots))
    else {
        return (lowers, Vec::new());
    };

    let runtime_owned_suffix = lowers.split_off(first_runtime_owned_index);

    debug_assert!(lowers
        .iter()
        .all(|lower| !is_runtime_owned(Path::new(&lower.file), runtime_owned_roots)));

    (lowers, runtime_owned_suffix)
}

fn link_or_copy_file_blocking(source: &Path, destination: &Path) -> std::io::Result<()> {
    if source == destination {
        return Ok(());
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::remove_file(destination) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    if let Err(error) = fs::hard_link(source, destination) {
        warn!(
            source = %source.display(),
            destination = %destination.display(),
            error = %error,
            "hard-link runtime overlaybd layer failed; falling back to sparse copy"
        );
    } else {
        return Ok(());
    }

    let mut cmd = std::process::Command::new("cp");
    if cfg!(target_os = "linux") {
        cmd.arg("--reflink=auto").arg("--sparse=always");
    }
    let status = cmd.arg(source).arg(destination).status();
    match status {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => {
            let _ = fs::remove_file(destination);
            Err(std::io::Error::other(format!(
                "sparse copy command failed with status {status}"
            )))
        }
        Err(error) => {
            let _ = fs::remove_file(destination);
            Err(error)
        }
    }
}

async fn link_or_copy_runtime_layer(source: &Path, destination: &Path) -> Result<()> {
    let source = source.to_path_buf();
    let destination = destination.to_path_buf();
    let description = format!(
        "link or copy runtime overlaybd layer {} -> {}",
        source.display(),
        destination.display()
    );
    tokio::task::spawn_blocking(move || link_or_copy_file_blocking(&source, &destination))
        .await
        .context("link runtime overlaybd layer task failed")?
        .with_context(|| description)
}

async fn prepare_specific_snapshot_layer_path(snapshot_layer_path: &Path) -> Result<()> {
    match tokio::fs::remove_file(&snapshot_layer_path).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "remove stale overlaybd snapshot layer {}",
                    snapshot_layer_path.display()
                )
            })
        }
    }
    Ok(())
}

fn load_existing_image_config(
    image_config_path: Option<&Path>,
    description: &str,
) -> Result<ImageConfig> {
    let Some(image_config_path) = image_config_path else {
        return Ok(ImageConfig::default());
    };

    let image_config =
        overlaybd::config::load_image_config(image_config_path).with_context(|| {
            format!(
                "load {description} image config {}",
                image_config_path.display()
            )
        })?;
    Ok(image_config)
}

fn restack_target_upper_data_path(live_runtime_image_config_path: &Path) -> Result<PathBuf> {
    let image_config = overlaybd::config::load_image_config(live_runtime_image_config_path)
        .with_context(|| {
            format!(
                "load live overlaybd runtime config {}",
                live_runtime_image_config_path.display()
            )
        })?;
    if image_config.upper.data.is_empty() {
        anyhow::bail!(
            "restack snapshot requires writable upper.data in live runtime config {}",
            live_runtime_image_config_path.display()
        );
    }
    Ok(PathBuf::from(image_config.upper.data))
}

fn on_same_filesystem(src_path: &Path, dst_dir: &Path) -> Result<bool> {
    let src_meta = fs::metadata(src_path)
        .with_context(|| format!("stat restack source path {}", src_path.display()))?;
    let dst_meta = fs::metadata(dst_dir)
        .with_context(|| format!("stat restack destination dir {}", dst_dir.display()))?;
    Ok(src_meta.dev() == dst_meta.dev())
}

fn write_bytes_atomically(path: &Path, bytes: &[u8], description: &str) -> Result<()> {
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

async fn copy_file_atomically(src: &Path, dst: &Path, description: &str) -> Result<()> {
    let parent = dst.parent().with_context(|| {
        format!(
            "{description} destination has no parent directory: {}",
            dst.display()
        )
    })?;
    let temp = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temp {description} in {}", parent.display()))?;
    let temp_path = temp.path().to_path_buf();
    tokio::fs::copy(src, &temp_path).await.with_context(|| {
        format!(
            "copy {description} {} -> {}",
            src.display(),
            temp_path.display()
        )
    })?;
    std::fs::File::open(&temp_path)
        .with_context(|| format!("open temp {description} {}", temp_path.display()))?
        .sync_all()
        .with_context(|| format!("sync temp {description} {}", temp_path.display()))?;
    temp.persist(dst).map_err(|error| {
        anyhow::Error::new(error.error).context(format!(
            "persist {description} {} -> {}",
            temp_path.display(),
            dst.display()
        ))
    })?;
    Ok(())
}

/// Move the sealed layer a live runtime config now references out of a
/// directory the pause is about to delete, and repoint the config at the new
/// path.
///
/// A rejected pause deletes its own output directory, but after the restack the
/// live runtime config references the sealed layer inside that directory. Left
/// there, the deletion frees a layer the still-running device has open — the
/// same class of dangling reference that leaves a live sandbox pointing at a
/// deleted file. The move therefore:
///
/// 1. copies the layer to the sandbox's snapshot tree, a runtime-owned root that
///    outlives this generation;
/// 2. syncs the copy before repointing, so a crash never leaves the config
///    naming a file whose bytes are still in flight;
/// 3. rewrites the live config to the new path only AFTER the copy is durable;
/// 4. only then drops the original from the doomed directory.
///
/// A copy-then-unlink (never a rename) is what makes this safe across
/// filesystems: the snapshot tree and the pause directory are not guaranteed to
/// share a device, and a rename across devices fails outright.
pub(super) async fn relocate_sealed_layer_for_rejected_pause(
    live_runtime_image_config_path: &Path,
    sealed_layer_path: &Path,
    retained_dir: &Path,
) -> Result<PathBuf> {
    // Unique per call, not just per file name: two devices of one sandbox can
    // seal layers under the same name (`snapshot.commit`), and a shared name
    // would make the second copy overwrite the first while a config already
    // points at it. The uuid prefix keeps them apart; the readable suffix keeps
    // the operator able to tell what the file is.
    let retained_path = retained_dir.join(format!(
        "{}-{}",
        Uuid::now_v7(),
        sealed_layer_path
            .file_name()
            .unwrap_or_else(|| OsStr::new("snapshot.commit"))
            .to_string_lossy()
    ));
    if retained_path != sealed_layer_path {
        tokio::fs::create_dir_all(retained_dir)
            .await
            .with_context(|| format!("create retained-layer dir {}", retained_dir.display()))?;
        copy_file_atomically(
            sealed_layer_path,
            &retained_path,
            "sealed layer retained by a rejected pause",
        )
        .await?;
        // Repoint every lower that names the doomed path, keeping its digest and
        // size: the bytes are identical, so only the location changes.
        let mut image_config = overlaybd::config::load_image_config(live_runtime_image_config_path)
            .with_context(|| {
                format!(
                    "load live overlaybd runtime config {}",
                    live_runtime_image_config_path.display()
                )
            })?;
        let doomed =
            fs::canonicalize(sealed_layer_path).unwrap_or_else(|_| sealed_layer_path.to_path_buf());
        let mut rewrote = false;
        for lower in image_config.lowers.iter_mut() {
            let current =
                fs::canonicalize(&lower.file).unwrap_or_else(|_| PathBuf::from(&lower.file));
            if current == doomed {
                lower.file = retained_path.display().to_string();
                rewrote = true;
            }
        }
        if !rewrote {
            anyhow::bail!(
                "live runtime config {} does not reference the sealed layer {} that a rejected \
                 pause must retain",
                live_runtime_image_config_path.display(),
                sealed_layer_path.display()
            );
        }
        let bytes = serde_json::to_vec_pretty(&image_config)
            .context("serialize live overlaybd runtime config after retaining the sealed layer")?;
        write_bytes_atomically(
            live_runtime_image_config_path,
            &bytes,
            "live overlaybd runtime config",
        )?;
        // The config is durable and points at the copy, so the original may go.
        if let Err(error) = tokio::fs::remove_file(sealed_layer_path).await {
            warn!(
                path = %sealed_layer_path.display(),
                error = %error,
                "failed to remove the sealed layer a rejected pause retained; the copy is \
                 authoritative and the original is wasted bytes"
            );
        }
    }
    Ok(retained_path)
}

async fn rewrite_live_runtime_config_for_restack(
    live_runtime_image_config_path: &Path,
    snapshot_layer_path: &Path,
    descriptor: Option<&overlaybd::LayerDescriptor>,
) -> Result<()> {
    let mut image_config = overlaybd::config::load_image_config(live_runtime_image_config_path)
        .with_context(|| {
            format!(
                "load live overlaybd runtime config {}",
                live_runtime_image_config_path.display()
            )
        })?;
    image_config.lowers.push(local_layer_config_with_descriptor(
        snapshot_layer_path,
        descriptor,
    ));
    let bytes = serde_json::to_vec_pretty(&image_config)
        .context("serialize rewritten live overlaybd runtime config")?;
    write_bytes_atomically(
        live_runtime_image_config_path,
        &bytes,
        "live overlaybd runtime config",
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn stage_overlaybd_snapshot_from_live_runtime(
    live_runtime_image_config_path: &Path,
    output_dir: &Path,
    snapshot_layer_path: Option<&Path>,
    limits: ChainLimits,
    runtime_owned_roots: &[PathBuf],
    preflight: ChainPreflight,
    compaction_view_bytes: u64,
    kind: &'static str,
    defer_compaction: bool,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create overlaybd snapshot dir {}", output_dir.display()))?;

    let mut image_config = overlaybd::config::load_image_config(live_runtime_image_config_path)
        .with_context(|| {
            format!(
                "load overlaybd rootfs image config {}",
                live_runtime_image_config_path.display()
            )
        })?;

    let appended_layer = if let Some(snapshot_layer_path) = snapshot_layer_path {
        let expected_snapshot_path = fs::canonicalize(snapshot_layer_path)
            .unwrap_or_else(|_| snapshot_layer_path.to_path_buf());
        let Some(latest_lower) = image_config.lowers.last() else {
            anyhow::bail!(
                "restack snapshot expected latest lower to be the newly claimed sealed upper layer"
            );
        };
        let actual_snapshot_path = fs::canonicalize(&latest_lower.file)
            .unwrap_or_else(|_| PathBuf::from(&latest_lower.file));
        anyhow::ensure!(
            actual_snapshot_path == expected_snapshot_path,
            "restack snapshot expected latest lower {} but found {}",
            expected_snapshot_path.display(),
            actual_snapshot_path.display()
        );
        Some(image_config.lowers.pop().expect("checked latest lower"))
    } else {
        None
    };

    let rewritten = rewrite_lowers_with_runtime_roots(
        image_config.lowers,
        output_dir,
        appended_layer,
        MANAGED_BASE_LAYER_FILE,
        runtime_owned_roots,
        // Sealed rootfs layers always stay raw; compression happens once at
        // publish time under `[snapshot.publish_compression]`.
        OverlaybdCompactOutput::Raw,
        ChainLimitPolicy {
            limits,
            preflight,
            compaction_view_bytes,
        },
        DeferredCompactionRequest::for_persistent_pause(
            defer_compaction,
            kind,
            "image.json",
            output_dir,
        ),
    )
    .await?;
    image_config.lowers = rewritten.lowers;
    image_config.upper = Default::default();
    image_config.result_file = "./result.txt".to_string();

    let output_path = output_dir.join("image.json");
    let bytes = serde_json::to_vec_pretty(&image_config)
        .context("serialize overlaybd rootfs image config with inherited runtime layers")?;
    write_bytes_atomically(&output_path, &bytes, "overlaybd rootfs image config")?;
    // The descriptor is written only after the config is durable: the worker
    // swaps the config it names, so a descriptor ahead of its config could
    // point at a file that does not exist yet.
    if let Some(descriptor) = rewritten.deferred.as_ref() {
        descriptor.write(&DeferredCompactionDescriptor::path_in(output_dir))?;
    }
    Ok(output_path)
}

/// Everything the pause needs to leave a byte-triggered compaction to the
/// maintenance worker.
///
/// Built only by the persistent pause path: a capture into a temporary
/// directory must keep today's inline behavior, because the worker only scans
/// generations a `Paused` record names.
#[derive(Clone)]
pub(super) struct DeferredCompactionRequest {
    pub(super) kind: &'static str,
    /// Image config the worker swaps, relative to the generation directory
    /// (`image.json` for rootfs/drive/volume, `mem_image.json` for memory).
    pub(super) config_file: &'static str,
    /// Bounded log field naming the directory the descriptor is written in.
    pub(super) generation: String,
    pub(super) gate: Arc<DeferredCompactionGate>,
    pub(super) max_concurrent: usize,
    pub(super) pending_budget: u64,
}

impl DeferredCompactionRequest {
    /// A request with an explicit gate and budgets, so the red tests can drive
    /// admission and the worker over the same instance.
    #[cfg(test)]
    pub(super) fn for_test(
        gate: Arc<DeferredCompactionGate>,
        kind: &'static str,
        config_file: &'static str,
        output_dir: &Path,
        max_concurrent: usize,
        pending_budget: u64,
    ) -> Self {
        Self {
            kind,
            config_file,
            generation: output_dir
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default(),
            gate,
            max_concurrent,
            pending_budget,
        }
    }

    /// The request a persistent pause uses, or `None` when the pause must not
    /// defer (a capture into a throwaway directory).
    pub(super) fn for_persistent_pause(
        defer: bool,
        kind: &'static str,
        config_file: &'static str,
        output_dir: &Path,
    ) -> Option<Self> {
        if !defer {
            return None;
        }
        let orchestrator = ConfigManager::global_config().orchestrator.clone();
        Some(Self {
            kind,
            config_file,
            generation: output_dir
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default(),
            gate: DeferredCompactionGate::global(),
            max_concurrent: orchestrator.deferred_compaction_max_concurrent,
            pending_budget: orchestrator.deferred_compaction_pending_bytes_budget,
        })
    }
}

/// The result of one chain rewrite.
///
/// `deferred` is the work order the caller must persist **after** the image
/// config is durable; it is `None` on the inline path, including every path that
/// compacted in the pause, and on every path that adopted without a breach.
#[derive(Debug)]
pub(super) struct RewriteLowers {
    pub(super) lowers: Vec<LayerConfig>,
    pub(super) deferred: Option<DeferredCompactionDescriptor>,
}

impl std::ops::Deref for RewriteLowers {
    type Target = Vec<LayerConfig>;

    fn deref(&self) -> &Self::Target {
        &self.lowers
    }
}

/// The bound knobs a rewrite enforces, grouped so the positional argument list
/// stays readable.
///
/// The limits and the pre-flight always travel together — one is the bound, the
/// other the pre-mutation measurement of the same bounds — and keeping them in
/// one value is what stops a caller from pairing a limit with a pre-flight taken
/// under a different one.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct ChainLimitPolicy {
    pub(super) limits: ChainLimits,
    pub(super) preflight: ChainPreflight,
    /// Virtual size, in bytes, of the device the compactable layers describe:
    /// the upper bound of the merged commit compaction writes. `0` disables the
    /// pre-compaction headroom check — right for a chain with no device size to
    /// bound it, never a way to skip the check on a device that has one.
    pub(super) compaction_view_bytes: u64,
}

impl ChainLimitPolicy {
    /// Enforcement disabled (`chain_bytes_ceiling = 0` and no disk multiple).
    ///
    /// Test-only: production always reaches a rewrite through a live limit
    /// value, so the disabled policy exists to keep the unit fixtures that do not
    /// exercise a bound readable.
    #[cfg(test)]
    pub(super) fn disabled() -> Self {
        Self::default()
    }
}

/// Refuse before a compaction that the node cannot finish.
///
/// Called immediately before the rewrite starts, while the inputs and the
/// output would coexist on the artifact root. Refusing here keeps the failure
/// clean: the alternative is an `ENOSPC` after the device was already restacked
/// and half the merged commit was written.
fn ensure_compaction_headroom(
    output_dir: &Path,
    view_bytes: u64,
    compactable: &[LayerConfig],
    kind: &'static str,
) -> Result<()> {
    ensure_compaction_headroom_with(
        output_dir,
        view_bytes,
        compactable,
        kind,
        chain_refusal_log::filesystem_space,
    )
}

fn ensure_compaction_headroom_with(
    output_dir: &Path,
    view_bytes: u64,
    compactable: &[LayerConfig],
    kind: &'static str,
    filesystem_space: impl Fn(&Path) -> Option<(u64, u64)>,
) -> Result<()> {
    if view_bytes == 0 {
        return Ok(());
    }
    let compactable_bytes =
        chain_allocated_bytes(compactable.iter().map(|lower| Path::new(&lower.file)));
    let required_bytes = compaction_transient_bytes(view_bytes, compactable_bytes);
    let free_bytes = filesystem_space(output_dir).map(|(free, _)| free);
    if free_bytes.is_some_and(|free| free >= required_bytes) {
        return Ok(());
    }
    warn!(
        kind,
        artifact_root = %output_dir.display(),
        required_bytes,
        free_bytes,
        view_bytes,
        compactable_bytes,
        compaction_inputs = compactable.len(),
        "compaction needs more transient disk than the artifact root has; \
         refusing the pause before the rewrite starts"
    );
    chain_metrics::record_compaction_headroom_refused(kind);
    Err(compaction_headroom_insufficient(required_bytes, free_bytes))
}

#[allow(clippy::too_many_arguments)]
async fn rewrite_lowers_with_runtime_roots(
    existing_lowers: Vec<LayerConfig>,
    output_dir: &Path,
    appended_layer: Option<LayerConfig>,
    compaction_output_name: &'static str,
    runtime_owned_roots: &[PathBuf],
    compaction_output: OverlaybdCompactOutput,
    policy: ChainLimitPolicy,
    deferral: Option<DeferredCompactionRequest>,
) -> Result<RewriteLowers> {
    let ChainLimitPolicy {
        limits,
        preflight,
        compaction_view_bytes,
    } = policy;
    let limits_enforced = limits.enabled();
    let (mut lowers, mut runtime_owned_lowers) =
        split_runtime_suffix(existing_lowers, runtime_owned_roots);

    // Compact the runtime-owned suffix plus the appended layer once their count
    // exceeds a budget that shrinks as the stable prefix grows: the full
    // `DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS` with an empty prefix, one less per
    // 4 prefix lowers (e.g. 24 with a 32-layer prefix), clamped to 1 so a lone
    // layer is never pointlessly rewritten into itself. This keeps the total
    // stack well below overlaybd's 255-layer hard limit without compacting on
    // every pause when the prefix is already large.
    let max_runtime_owned_layers = DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS
        .saturating_sub(lowers.len() / 4)
        .max(1);
    let compactable_count = runtime_owned_lowers.len() + usize::from(appended_layer.is_some());
    // The bounds are the stricter of the triggers: a chain can stay inside the
    // layer-count budget for many pauses and still fill the node's disk, because
    // each generation rewrites far more bytes than it shares. Both bounds `0`
    // keeps the pre-bound behavior: no byte accounting and no compaction on
    // bytes.
    //
    // The absolute bound judges the reclaimable part — the runtime-owned lowers
    // plus the layer this pause appends — never the whole chain, because the
    // stable prefix is materialized from the image once, identical for every
    // sandbox of that image, and compaction cannot reclaim it; charging it per
    // sandbox would count the same node bytes once per sandbox. The relative
    // bound is the card's invariant and judges the whole deduplicated chain,
    // stable prefix included, against a small multiple of the guest disk. It is
    // only set for the rootfs, so a prefix is never a breach of the absolute
    // bound while the rootfs chain as a whole still is.
    let accounting = if limits_enforced {
        ChainByteAccounting::measure(
            lowers
                .iter()
                .chain(runtime_owned_lowers.iter())
                .cloned()
                .collect::<Vec<_>>(),
            appended_layer.as_ref(),
            runtime_owned_roots,
        )
    } else {
        ChainByteAccounting::default()
    };
    let breach = accounting.breach(limits);
    if compactable_count > max_runtime_owned_layers || breach.is_some() {
        // A bound trigger decides what it rewrites by classification, never by
        // position: `split_runtime_suffix` hands over everything from the first
        // runtime-owned lower onward, which is the right unit to adopt but the
        // wrong one to blame for bytes. Compaction therefore rewrites exactly
        // the lowers the chain diagnostic reports as runtime-owned, including
        // the ones a positional prefix would have held as immutable — a chain of
        // pause-owned generations has no stable prefix at all, and refusing it
        // while the diagnostic reports every lower as reclaimable is the defect
        // this split closes. Under the count trigger the positional suffix is
        // kept as it was, because there the chain is not being blamed for its
        // size, only rewritten to stay below overlaybd's layer limit.
        let mut compactable = if breach.is_some() {
            let (stable_prefix, runtime_owned) = split_chain_by_classification(
                std::mem::take(&mut lowers)
                    .into_iter()
                    .chain(std::mem::take(&mut runtime_owned_lowers)),
                runtime_owned_roots,
            );
            lowers = stable_prefix;
            runtime_owned
        } else {
            std::mem::take(&mut runtime_owned_lowers)
        };
        // Measure the parts before compaction folds them into one layer: the
        // rejection has to state whether the bytes it refused live in an
        // untouchable prefix or in a compacted layer that did not shrink. The
        // disabled policy keeps the pre-bound cost too, so nothing is measured
        // when enforcement is off.
        let appended_bytes = if limits_enforced {
            chain_allocated_bytes(appended_layer.iter().map(|layer| Path::new(&layer.file)))
        } else {
            0
        };
        let compacted_lowers = compactable.len() + usize::from(appended_layer.is_some());
        if let Some(layer) = appended_layer {
            compactable.push(layer);
        }
        if compactable.iter().any(|lower| lower.file.is_empty()) {
            anyhow::bail!("cannot compact runtime-owned overlaybd suffix with remote lowers");
        }
        // The card's fix: a byte-triggered rewrite is the expensive one the pause
        // used to pay inside the call (tens of seconds on the measured node). It
        // is left to the maintenance worker whenever admission proves the node
        // can finish it later: concurrency budget, pending-bytes budget, free
        // disk at least the transient bytes, and a writable descriptor. The
        // layer-count trigger stays inline — it protects overlaybd's 255-layer
        // hard limit and is not the call the card measures — and a non-raw
        // output stays inline because the descriptor only names a raw output.
        if let Some(request) = deferral
            .filter(|_| breach.is_some() && compaction_output == OverlaybdCompactOutput::Raw)
        {
            let transient = compaction_transient_bytes(
                compaction_view_bytes,
                compactable_allocated_bytes(&compactable),
            );
            let descriptor_path = DeferredCompactionDescriptor::path_in(output_dir);
            let free_bytes = chain_refusal_log::filesystem_space(output_dir).map(|(free, _)| free);
            // Admission (d): the work order must be writable in the generation
            // directory. Proving it here, before adopting anything, is what lets
            // a read-only artifact root fall back to today's inline path instead
            // of failing the pause after the restack.
            let writable = tempfile::NamedTempFile::new_in(output_dir).is_ok();
            if writable
                && request.gate.admit(
                    &descriptor_path,
                    transient,
                    request.max_concurrent,
                    request.pending_budget,
                    free_bytes,
                )
            {
                // Adopt the suffix exactly like the no-compaction path: the pause
                // returns as soon as the dirty state is written, and the worker
                // folds these adopted layers into one generation later.
                // `compactable` already carries the appended layer, in order.
                let adopted = adopt_runtime_owned_suffix(output_dir, compactable, None).await?;
                let descriptor = DeferredCompactionDescriptor::new(
                    request.kind,
                    request.config_file,
                    compaction_output_name,
                    adopted.iter().map(|layer| layer.file.clone()).collect(),
                    compaction_view_bytes,
                    (limits.reclaimable_ceiling, limits.whole_chain_ceiling),
                    request.generation,
                );
                lowers.extend(adopted);
                chain_metrics::record_deferred_compaction(compaction_output_name, "deferred");
                return Ok(RewriteLowers {
                    lowers,
                    deferred: Some(descriptor),
                });
            }
            // Admission refused: take today's inline path. It is a path
            // decision, never a new refusal code, and its own headroom check
            // still runs before the rewrite.
        }
        // Refuse before the rewrite if the node cannot hold both the inputs
        // and the merged output at once. The check is inside this branch, not
        // on the adoption path, because only a compaction needs the transient
        // space; and it runs before `compact_layers` so a pause on a full node
        // fails cleanly instead of half-writing the output.
        ensure_compaction_headroom(
            output_dir,
            compaction_view_bytes,
            &compactable,
            compaction_output_name,
        )?;
        let compacted_path = compact_layers(
            &compactable,
            &output_dir.join(compaction_output_name),
            compaction_output,
        )
        .await?;
        if compacted_path.is_some() {
            chain_metrics::record_compaction(compaction_output_name);
        }
        let breakdown = ChainCeilingBreakdown {
            stable_prefix_lowers: lowers.len(),
            stable_prefix_bytes: accounting.stable_prefix_bytes,
            compacted_lowers,
            compacted_bytes: if limits_enforced {
                compacted_path
                    .as_deref()
                    .map(|path| chain_allocated_bytes([path]))
                    .unwrap_or(0)
            } else {
                0
            },
            appended_bytes,
        };
        if let Some(compacted_path) = compacted_path.as_ref() {
            lowers.push(local_layer_config(compacted_path));
        }
        if limits_enforced {
            // Re-measure both bounded parts on the result and refuse the pause by
            // name when compaction could not bring them under their limits.
            // After compaction the reclaimable part is exactly the generation it
            // produced — the appended layer and every rewritten lower live
            // inside it — while the whole chain adds the untouched stable prefix.
            //
            // The pre-flight is skipped here, not trusted: a measurement taken
            // before the restack that already fits cannot become a breach by
            // compaction, which only drops blocks, so re-measuring would only
            // cost a stat pass and could never change the answer.
            let reclaimable_after = match compacted_path.as_deref() {
                Some(path) => chain_allocated_bytes([path]),
                None => accounting.reclaimable_bytes,
            };
            let accounting_after = ChainByteAccounting {
                reclaimable_bytes: reclaimable_after,
                stable_prefix_bytes: breakdown.stable_prefix_bytes,
                total_bytes: breakdown
                    .stable_prefix_bytes
                    .saturating_add(reclaimable_after),
            };
            chain_metrics::record_chain_bytes(
                compaction_output_name,
                accounting_after.reclaimable_bytes,
                limits.reclaimable_ceiling,
            );
            chain_metrics::record_limit_ratios(compaction_output_name, accounting_after, limits);
            if preflight.guaranteed_fit() {
                chain_metrics::record_preflight_guaranteed_fit(compaction_output_name);
                debug!(
                    kind = compaction_output_name,
                    measured_reclaimable_bytes = preflight.measured_reclaimable_bytes,
                    measured_total_bytes = preflight.measured_total_bytes,
                    reclaimable_ceiling = limits.reclaimable_ceiling,
                    whole_chain_ceiling = limits.whole_chain_ceiling,
                    "pre-flight chain already fits; skipping the post-compaction limit check"
                );
            } else if let Some(breach_after) = accounting_after.breach(limits) {
                // The refusal carries the numbers the seven-day measurement
                // reads from this layer: the stable code of the bound that
                // refused, the observed bytes, the limit, the appended-upper
                // bytes, and the node's free and total bytes at the artifact
                // root. The paused-sandbox count is logged by the orchestrator's
                // recoverable-pause branch, the only layer with the metadata
                // store; both lines carry the named code, so one query reads the
                // whole refusal. The disk probe answers `None` rather than
                // failing the refusal, so the fields are absent instead of
                // invented when statvfs fails.
                let space = chain_refusal_log::filesystem_space(output_dir);
                warn!(
                    kind = compaction_output_name,
                    limit = breach_after.limit.as_str(),
                    limit_bytes = breach_after.limit_bytes,
                    observed_bytes = breach_after.observed_bytes,
                    whole_chain_bytes = accounting_after.total_bytes,
                    reclaimable_bytes = accounting_after.reclaimable_bytes,
                    appended_bytes = breakdown.appended_bytes,
                    measured_before_restack_reclaimable_bytes =
                        preflight.measured_reclaimable_bytes,
                    measured_before_restack_total_bytes = preflight.measured_total_bytes,
                    artifact_root = %output_dir.display(),
                    node_free_bytes = space.map(|(free, _)| free),
                    node_total_bytes = space.map(|(_, total)| total),
                    stable_prefix_lowers = breakdown.stable_prefix_lowers,
                    stable_prefix_bytes = breakdown.stable_prefix_bytes,
                    compacted_lowers = breakdown.compacted_lowers,
                    compacted_bytes = breakdown.compacted_bytes,
                    "paused sandbox chain is still above a limit after compaction; \
                     the pause is refused and the sandbox is resumed"
                );
                // The pause is rejected, so this generation must not survive:
                // leave the output directory exactly as the caller will clean
                // it, without the compacted layer the failed rewrite produced.
                if let Some(compacted_path) = compacted_path {
                    if let Err(error) = tokio::fs::remove_file(&compacted_path).await {
                        warn!(
                            path = %compacted_path.display(),
                            error = %error,
                            "failed to remove compacted layer after a limit rejection"
                        );
                    }
                }
                // Classified by the caller: the rootfs restack already mutated
                // the live runtime and stays terminal through `finish_staging`,
                // while the memory-config path is still recoverable.
                chain_metrics::record_ceiling_exceeded(compaction_output_name);
                return Err(paused_chain_limit_exceeded(
                    breach_after.limit,
                    breach_after.observed_bytes,
                    breach_after.limit_bytes,
                    breakdown,
                ));
            }
        }
        return Ok(RewriteLowers {
            lowers,
            deferred: None,
        });
    }

    // Otherwise, adopt the runtime-owned suffix into the snapshot artifact dir.
    let adopted =
        adopt_runtime_owned_suffix(output_dir, runtime_owned_lowers, appended_layer).await?;
    lowers.extend(adopted);
    if limits_enforced {
        chain_metrics::record_chain_bytes(
            compaction_output_name,
            accounting.reclaimable_bytes,
            limits.reclaimable_ceiling,
        );
        chain_metrics::record_limit_ratios(compaction_output_name, accounting, limits);
    }

    Ok(RewriteLowers {
        lowers,
        deferred: None,
    })
}

/// Adopt the runtime-owned suffix into the generation's `inherited-layers/`
/// directory, hardlinking when possible, and append the layer the pause wrote.
///
/// Shared by the no-compaction path and by the deferred path: both leave the
/// generation with every runtime layer reachable from its own directory, so the
/// paused record never names a live runtime path. The returned layers are the
/// adopted suffix in chain order plus the appended layer, which is exactly the
/// unit a later compaction folds.
async fn adopt_runtime_owned_suffix(
    output_dir: &Path,
    mut runtime_owned_lowers: Vec<LayerConfig>,
    appended_layer: Option<LayerConfig>,
) -> Result<Vec<LayerConfig>> {
    let inherited_layers_dir = output_dir.join(INHERITED_LAYERS_DIR);
    for (index, lower) in runtime_owned_lowers.iter_mut().enumerate() {
        let source = Path::new(&lower.file);
        let destination = inherited_layers_dir.join(format!("{index:04}")).join(
            source
                .file_name()
                .unwrap_or_else(|| OsStr::new("runtime-layer.commit")),
        );
        link_or_copy_runtime_layer(source, &destination).await?;
        lower.file = destination.display().to_string();
    }
    if let Some(layer) = appended_layer {
        runtime_owned_lowers.push(layer);
    }
    Ok(runtime_owned_lowers)
}

async fn capture_live_overlaybd_snapshot(
    ublk_device: &UblkDevice,
    read_only: bool,
    live_runtime_image_config_path: &Path,
    output_dir: &Path,
    snapshot_layer_file_name: &str,
    kind: &'static str,
) -> Result<LiveOverlaybdSnapshotState> {
    if read_only {
        debug!(
            output_dir = %output_dir.display(),
            "staging overlaybd snapshot from read-only runtime config"
        );
        return Ok(LiveOverlaybdSnapshotState::ReadOnly);
    }

    let live_upper_data_path = restack_target_upper_data_path(live_runtime_image_config_path)
        .context("resolve restack source upper path")?;
    let snapshot_layer_path = output_dir.join(snapshot_layer_file_name);
    prepare_specific_snapshot_layer_path(&snapshot_layer_path).await?;
    let live_snapshot_layer_path = if on_same_filesystem(&live_upper_data_path, output_dir)
        .context("validate restack snapshot filesystem precondition")?
    {
        snapshot_layer_path.clone()
    } else {
        let live_snapshot_layer_path = live_upper_data_path
            .parent()
            .context("restack source upper path has no parent directory")?
            .join(snapshot_layer_file_name);
        prepare_specific_snapshot_layer_path(&live_snapshot_layer_path)
            .await
            .with_context(|| {
                format!(
                    "prepare same-filesystem restack snapshot layer {}",
                    live_snapshot_layer_path.display()
                )
            })?;
        live_snapshot_layer_path
    };

    let descriptor = UblkDeviceManager::global()
        .restack_snapshot_device(ublk_device, &live_snapshot_layer_path, kind)
        .await
        .context("request overlaybd restack snapshot from ublk device")?;

    if live_snapshot_layer_path != snapshot_layer_path {
        // If this cross-filesystem copy fails, leave the live runtime config
        // untouched and surface a terminal pause failure. The daemon has
        // already sealed the old upper and reopened a fresh one, so callers
        // must not continue treating the live runtime as safely resumable.
        copy_file_atomically(
            &live_snapshot_layer_path,
            &snapshot_layer_path,
            "restack snapshot layer",
        )
        .await
        .context("copy restack snapshot layer into managed snapshot dir")
        .map_err(into_terminal_snapshot_failure)?;
    }

    if let Some(descriptor) = descriptor.as_ref() {
        let copied_size = tokio::fs::metadata(&snapshot_layer_path)
            .await
            .with_context(|| {
                format!(
                    "read restack snapshot layer metadata {}",
                    snapshot_layer_path.display()
                )
            })
            .map_err(into_terminal_snapshot_failure)?
            .len();
        if copied_size != descriptor.size {
            // Keep the sealed layer: the live device may still reference it,
            // and terminal deletion marks the volume failed for recovery.
            return Err(into_terminal_snapshot_failure(anyhow::anyhow!(
                "restack snapshot descriptor size mismatch for {}: descriptor says {}, file has {}",
                snapshot_layer_path.display(),
                descriptor.size,
                copied_size
            )));
        }
    }

    rewrite_live_runtime_config_for_restack(
        live_runtime_image_config_path,
        &snapshot_layer_path,
        descriptor.as_ref(),
    )
    .await
    .context("rewrite live runtime config after restack snapshot")
    .map_err(into_terminal_snapshot_failure)?;

    Ok(LiveOverlaybdSnapshotState::Restacked(snapshot_layer_path))
}

pub(super) async fn build_mem_snapshot_image_config(
    resume_mem_image_config_path: Option<&Path>,
    new_layer_path: &Path,
    output_dir: &Path,
    memory_output: OverlaybdCompactOutput,
    limits: ChainLimits,
    runtime_owned_roots: &[PathBuf],
    defer_compaction: bool,
) -> Result<(ImageConfig, Option<DeferredCompactionDescriptor>)> {
    let inherited_image_config =
        load_existing_image_config(resume_mem_image_config_path, "memory snapshot")?;
    let new_layer = local_layer_config(new_layer_path);
    let rewritten = rewrite_lowers_with_runtime_roots(
        inherited_image_config.lowers,
        output_dir,
        Some(new_layer),
        "mem_compacted.commit",
        runtime_owned_roots,
        memory_output,
        ChainLimitPolicy {
            limits,
            // The memory chain is not restacked: the pause appends a fresh layer
            // it just wrote, so there is no pre-mutation measurement to take and
            // the post-compaction check stays the only judge.
            preflight: ChainPreflight::disabled(),
            // The card's transient-disk refusal is about the rootfs chain; the
            // memory chain keeps today's behavior.
            compaction_view_bytes: 0,
        },
        DeferredCompactionRequest::for_persistent_pause(
            defer_compaction,
            "memory",
            "mem_image.json",
            output_dir,
        ),
    )
    .await?;

    Ok((
        ImageConfig {
            repo_blob_url: inherited_image_config.repo_blob_url,
            lowers: rewritten.lowers,
            ..Default::default()
        },
        rewritten.deferred,
    ))
}

/// One overlaybd device's staging inputs, as the pause path hands them over.
///
/// Grouped into a struct because every field travels together (one device, its
/// live runtime config, the snapshot directory that receives the staged config,
/// and the chain budget the rewrite enforces) and because a long positional
/// argument list of same-typed values made it easy to pair a ceiling with the
/// wrong roots at a call site without the compiler noticing.
pub(super) struct RestackSnapshotDevice<'a> {
    pub(super) ublk_device: &'a UblkDevice,
    pub(super) read_only: bool,
    pub(super) live_runtime_image_config_path: &'a Path,
    pub(super) output_dir: &'a Path,
    pub(super) snapshot_layer_file_name: &'a str,
    pub(super) kind: &'static str,
    pub(super) limits: ChainLimits,
    pub(super) runtime_owned_roots: &'a [PathBuf],
    /// Virtual size of the device, the bound of the merged commit the compaction
    /// would write. `0` disables the pre-compaction headroom check.
    pub(super) compaction_view_bytes: u64,
    /// Whether a byte-triggered compaction may be left to the maintenance
    /// worker. Only the persistent pause path sets it; a capture into a
    /// throwaway directory keeps the inline rewrite.
    pub(super) defer_compaction: bool,
}

/// Stage a persisted snapshot config from the current live overlaybd runtime.
///
/// Writable runtimes are first restacked in-place so the sealed old upper
/// becomes the newest lower. Read-only runtimes skip the restack phase and
/// only stage the persisted snapshot image config. The sealed layer always
/// stays raw; compression, when enabled, happens once at publish time.
pub(super) async fn restack_snapshot_overlaybd_device(
    request: RestackSnapshotDevice<'_>,
) -> Result<PathBuf> {
    let RestackSnapshotDevice {
        ublk_device,
        read_only,
        live_runtime_image_config_path,
        output_dir,
        snapshot_layer_file_name,
        kind,
        limits,
        runtime_owned_roots,
        compaction_view_bytes,
        defer_compaction,
    } = request;
    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create overlaybd snapshot dir {}", output_dir.display()))?;

    // Pre-flight BEFORE the restack: the restack is the pause's point of no
    // return (it seals the writable upper and reopens a fresh one), so the
    // chain the pause will produce has to be estimable from what is on disk
    // now. A measurement that already fits cannot be turned into a breach by
    // compaction, which only drops blocks, so the post-compaction rejection is
    // provably moot and is skipped there.
    //
    // This deliberately does NOT refuse: the measurement includes the writable
    // upper's allocated bytes, an upper bound rather than a floor, so refusing
    // on it would reject pauses that would have fit. That is worse than the
    // refusal it would replace, which the recoverable path already turns into a
    // resumed VM. The incident shape therefore keeps its full treatment: the
    // measurement above the ceiling is only logged, and the post-compaction
    // check refuses by name and the pause path resumes the VM.
    let preflight = if read_only {
        ChainPreflight::disabled()
    } else {
        preflight_chain_before_restack(live_runtime_image_config_path, runtime_owned_roots, limits)
            .unwrap_or_else(|error| {
                warn!(
                    kind,
                    error = %format_args!("{error:#}"),
                    "failed to measure the chain before the restack; the post-compaction check \
                     still decides"
                );
                ChainPreflight::disabled()
            })
    };

    let live_snapshot = capture_live_overlaybd_snapshot(
        ublk_device,
        read_only,
        live_runtime_image_config_path,
        output_dir,
        snapshot_layer_file_name,
        kind,
    )
    .await?;

    let staged_snapshot = stage_overlaybd_snapshot_from_live_runtime(
        live_runtime_image_config_path,
        output_dir,
        live_snapshot.snapshot_layer_path(),
        limits,
        runtime_owned_roots,
        preflight,
        compaction_view_bytes,
        kind,
        defer_compaction,
    )
    .await;

    live_snapshot.finish_staging(staged_snapshot)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn restack_snapshot_overlaybd_rootfs(
    ublk_device: &UblkDevice,
    read_only: bool,
    live_runtime_image_config_path: &Path,
    snapshot_root: &Path,
    limits: ChainLimits,
    runtime_owned_roots: &[PathBuf],
    compaction_view_bytes: u64,
    defer_compaction: bool,
) -> Result<PathBuf> {
    restack_snapshot_overlaybd_device(RestackSnapshotDevice {
        ublk_device,
        read_only,
        live_runtime_image_config_path,
        output_dir: &snapshot_root.join("rootfs"),
        snapshot_layer_file_name: "snapshot.commit",
        kind: "rootfs",
        limits,
        runtime_owned_roots,
        compaction_view_bytes,
        defer_compaction,
    })
    .await
}

/// Named, recoverable failure code: reading a dirty memory page failed, so the
/// pause cannot produce a memory layer. It is never downgraded to a zero page —
/// absence from the host reads as zeros and is detected from the bytes, while a
/// failed read aborts the pause and lets the lifecycle restore the sandbox.
pub(crate) const MEMORY_SNAPSHOT_DIRTY_RANGE_READ_FAILED: &str =
    "MEMORY_SNAPSHOT_DIRTY_RANGE_READ_FAILED";

/// Histogram of bytes a pause left out because the host already discarded the
/// pages. Bounded labels: no sandbox id.
const MEMORY_SNAPSHOT_SKIPPED_BYTES: &str = "agentenv_memory_snapshot_skipped_bytes";

async fn publish_memory_overlaybd_layer(
    src_layers: &[Arc<dyn VirtualFile>],
    mappings: &[SegmentMapping],
    virtual_size: u64,
    output_path: &Path,
    mode: OverlaybdCompactOutput,
    concurrency: usize,
    drop_page_cache: bool,
) -> Result<CompactStats> {
    let lower_tmp = output_path.with_extension("commit.tmp");
    let build_result = async {
        let output_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&lower_tmp)
                .with_context(|| format!("create temp lower failed: {}", lower_tmp.display()))?,
        );
        let commit_args = create_commit_args(output_file, mode, concurrency).await?;
        // The pause reads dirty ranges from the live Firecracker process. A
        // guest page the host already discarded reads back as zeros, so
        // recording it as an unbacked zero segment keeps the layer content
        // intact while writing none of its bytes.
        let stats = compact_to_with_options(
            src_layers,
            mappings,
            virtual_size,
            commit_args,
            CompactOptions {
                zero_detection: true,
            },
        )
        .await
        .with_context(|| {
            format!("{MEMORY_SNAPSHOT_DIRTY_RANGE_READ_FAILED}: compact memory layer")
        })?;

        tokio::fs::rename(&lower_tmp, output_path)
            .await
            .with_context(|| {
                format!(
                    "move sealed memory lower into place failed: {}",
                    output_path.display()
                )
            })?;
        if drop_page_cache {
            drop_page_cache_after_write(output_path).await?;
        }
        Ok(stats)
    }
    .await;

    if build_result.is_err() {
        let _ = tokio::fs::remove_file(&lower_tmp).await;
    }
    build_result
}

/// Sync a sealed memory layer and tell the kernel its page cache can be
/// dropped, so the bytes the snapshot just wrote stop counting toward the
/// runtime pod's working set.
///
/// `POSIX_FADV_DONTNEED` frees only clean pages, so the sync is what makes the
/// hint effective; it is also the option's cost, which the node measurement has
/// to price into pause time.
async fn drop_page_cache_after_write(path: &Path) -> Result<()> {
    let file = LocalFile::open_ro(path)
        .with_context(|| format!("open {} to drop its page cache", path.display()))?;
    file.sync()
        .await
        .with_context(|| format!("sync {}", path.display()))?;
    // `evict_all` already degrades to a no-op on a platform without
    // `posix_fadvise`; a real errno failure still propagates.
    file.evict_all()
        .await
        .with_context(|| format!("drop page cache of {}", path.display()))
}

fn checked_i64_to_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("{field} must be non-negative, got {value}"))
}

fn dirty_ranges_to_segment_mappings(
    dirty_ranges: &DirtyMemoryRanges,
) -> Result<(Vec<SegmentMapping>, u64)> {
    ensure!(
        dirty_ranges.page_size == FIRECRACKER_DIRTY_PAGE_SIZE as i32,
        "dirty memory page_size must be {FIRECRACKER_DIRTY_PAGE_SIZE}, got {}",
        dirty_ranges.page_size
    );
    let memory_size = checked_i64_to_u64(dirty_ranges.memory_size, "memory_size")?;
    ensure!(
        memory_size % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
        "dirty memory_size {memory_size} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
    );

    let mut mappings = Vec::new();
    for range in &dirty_ranges.ranges {
        let mut base_host_virt_addr =
            checked_i64_to_u64(range.base_host_virt_addr, "base_host_virt_addr")?;
        let mut image_offset = checked_i64_to_u64(range.image_offset, "image_offset")?;
        let length = checked_i64_to_u64(range.length, "length")?;
        ensure!(length > 0, "dirty memory range length must be positive");
        ensure!(
            base_host_virt_addr % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
            "dirty range base_host_virt_addr {base_host_virt_addr:#x} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
        );
        ensure!(
            image_offset % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
            "dirty range image_offset {image_offset} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
        );
        ensure!(
            length % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
            "dirty range length {length} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
        );
        ensure!(
            image_offset
                .checked_add(length)
                .is_some_and(|end| end <= memory_size),
            "dirty range [{image_offset}, {}) exceeds memory_size {memory_size}",
            image_offset.saturating_add(length)
        );

        let mut remaining_sectors = length / OVERLAYBD_ALIGNMENT;
        while remaining_sectors > 0 {
            let sector_count = remaining_sectors.min(Segment::MAX_LENGTH as u64);
            mappings.push(SegmentMapping::new(
                image_offset / OVERLAYBD_ALIGNMENT,
                sector_count as u32,
                base_host_virt_addr / OVERLAYBD_ALIGNMENT,
                false,
                0,
            ));
            let bytes = sector_count * OVERLAYBD_ALIGNMENT;
            base_host_virt_addr += bytes;
            image_offset += bytes;
            remaining_sectors -= sector_count;
        }
    }

    mappings.sort_unstable();
    // `offset` and `length` describe destination memory-image sectors, while
    // `moffset` carries the source Firecracker HVA in sector units. This check
    // intentionally rejects overlapping destination ranges, not overlapping
    // source virtual addresses.
    for pair in mappings.windows(2) {
        let previous = &pair[0];
        let next = &pair[1];
        let previous_end = previous
            .offset()
            .checked_add(u64::from(previous.length()))
            .context("dirty memory segment end overflow")?;
        ensure!(
            previous_end <= next.offset(),
            "overlapping dirty memory destination ranges detected at image sectors {} and {}",
            previous.offset(),
            next.offset()
        );
    }
    Ok((mappings, memory_size))
}

/// Convert dirty Firecracker memory ranges directly into a sealed overlaybd layer.
pub(crate) async fn convert_dirty_memory_to_overlaybd(
    firecracker_pid: Pid,
    dirty_ranges: &DirtyMemoryRanges,
    output_dir: &Path,
    mode: OverlaybdCompactOutput,
    drop_page_cache: bool,
) -> Result<(PathBuf, u64)> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create mem overlaybd dir: {}", output_dir.display()))?;

    let data_path = output_dir.join("overlaybd.commit");
    let (mappings, memory_size) = dirty_ranges_to_segment_mappings(dirty_ranges)?;
    let source_file: Arc<dyn VirtualFile> = Arc::new(ProcessVmReader::new(firecracker_pid));
    let src_layers = vec![source_file];
    let stats = publish_memory_overlaybd_layer(
        &src_layers,
        &mappings,
        memory_size,
        &data_path,
        mode,
        DIRECT_MEMORY_SNAPSHOT_COMPACTION_CONCURRENCY,
        drop_page_cache,
    )
    .await
    .context("compact dirty memory ranges as overlaybd layer")?;

    metrics::histogram!(MEMORY_SNAPSHOT_SKIPPED_BYTES).record(stats.skipped_bytes as f64);
    tracing::info!(
        dirty_bytes = stats.input_bytes,
        skipped_bytes = stats.skipped_bytes,
        written_bytes = stats.written_bytes,
        "memory snapshot skipped host-discarded pages"
    );

    Ok((data_path, memory_size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    use async_trait::async_trait;
    use bytes::Bytes;
    use firecracker_client::models::DirtyMemoryRange;
    use serde_json::json;

    #[test]
    fn dirty_ranges_to_segment_mappings_splits_large_ranges() {
        let first_chunk = Segment::MAX_LENGTH as i64 * OVERLAYBD_ALIGNMENT as i64;
        let dirty_ranges = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: first_chunk + OVERLAYBD_ALIGNMENT as i64,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: 0x1000_0000,
                image_offset: 0,
                length: first_chunk + OVERLAYBD_ALIGNMENT as i64,
            }],
        };

        let (mappings, memory_size) =
            dirty_ranges_to_segment_mappings(&dirty_ranges).expect("convert mappings");

        assert_eq!(memory_size, dirty_ranges.memory_size as u64);
        assert_eq!(mappings.len(), 2);
        assert_eq!(mappings[0].offset(), 0);
        assert_eq!(mappings[0].length(), Segment::MAX_LENGTH);
        assert_eq!(mappings[0].moffset, 0x1000_0000 / OVERLAYBD_ALIGNMENT);
        assert_eq!(mappings[1].offset(), Segment::MAX_LENGTH as u64);
        assert_eq!(mappings[1].length(), 1);
        assert_eq!(
            mappings[1].moffset,
            0x1000_0000 / OVERLAYBD_ALIGNMENT + Segment::MAX_LENGTH as u64
        );
    }

    #[test]
    fn dirty_ranges_to_segment_mappings_rejects_non_4k_page_size() {
        let dirty_ranges = DirtyMemoryRanges {
            page_size: 2048,
            memory_size: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
            ranges: Vec::new(),
        };

        let err = dirty_ranges_to_segment_mappings(&dirty_ranges).unwrap_err();
        assert!(
            err.to_string()
                .contains("dirty memory page_size must be 4096"),
            "{err:#}"
        );
    }

    #[test]
    fn dirty_ranges_to_segment_mappings_rejects_non_page_aligned_range() {
        let dirty_ranges = DirtyMemoryRanges {
            page_size: FIRECRACKER_DIRTY_PAGE_SIZE as i32,
            memory_size: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: 0x1000_0000,
                image_offset: 0,
                length: OVERLAYBD_ALIGNMENT as i64,
            }],
        };

        let err = dirty_ranges_to_segment_mappings(&dirty_ranges).unwrap_err();
        assert!(
            err.to_string()
                .contains("dirty range length 512 is not 4096-byte aligned"),
            "{err:#}"
        );
    }

    #[test]
    fn dirty_ranges_to_segment_mappings_rejects_overlaps() {
        let dirty_ranges = DirtyMemoryRanges {
            page_size: FIRECRACKER_DIRTY_PAGE_SIZE as i32,
            memory_size: (FIRECRACKER_DIRTY_PAGE_SIZE * 3) as i64,
            ranges: vec![
                DirtyMemoryRange {
                    base_host_virt_addr: 0x1000_0000,
                    image_offset: 0,
                    length: (FIRECRACKER_DIRTY_PAGE_SIZE * 2) as i64,
                },
                DirtyMemoryRange {
                    base_host_virt_addr: 0x2000_0000,
                    image_offset: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
                    length: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
                },
            ],
        };

        let err = dirty_ranges_to_segment_mappings(&dirty_ranges).unwrap_err();
        assert!(
            err.to_string()
                .contains("overlapping dirty memory destination ranges detected"),
            "{err:#}"
        );
    }

    #[test]
    fn split_runtime_suffix_preserves_stable_lowers() {
        let temp = tempfile::tempdir().expect("tempdir");
        let snapshot_store_root = temp.path().join("snapshot-store");
        let persisted_sandbox_store_root = temp.path().join("persisted-sandboxes");
        let sandbox_snapshot_tree = temp.path().join("managed-snapshots").join("sandbox-self");
        let snapshot_store_lower = snapshot_store_root
            .join("managed-layers")
            .join("sha256_base.overlaybd.commit");
        let persisted_store_lower = persisted_sandbox_store_root
            .join("artifacts")
            .join("sandbox-parent")
            .join("1700000000000")
            .join("rootfs")
            .join("snapshot.commit");
        let managed_base_lower = sandbox_snapshot_tree
            .join("1700000000001")
            .join("snapshot.commit");
        let lowers = vec![
            LayerConfig {
                file: String::new(),
                digest: "sha256:remote".to_string(),
                size: 42,
                ..Default::default()
            },
            local_layer_config(&snapshot_store_lower),
            local_layer_config(&persisted_store_lower),
            local_layer_config(&managed_base_lower),
        ];
        let runtime_owned_roots = [sandbox_snapshot_tree, persisted_sandbox_store_root];

        let (preserved, managed) = split_runtime_suffix(lowers, &runtime_owned_roots);

        assert_eq!(preserved.len(), 2);
        assert_eq!(preserved[0].file, "");
        assert_eq!(preserved[0].digest, "sha256:remote");
        assert_eq!(PathBuf::from(&preserved[1].file), snapshot_store_lower);
        assert_eq!(managed.len(), 2);
        assert_eq!(PathBuf::from(&managed[0].file), persisted_store_lower);
        assert_eq!(PathBuf::from(&managed[1].file), managed_base_lower);
    }

    /// A runtime-owned root must be matched in its canonical form even when the
    /// caller handed it over through a symlinked (or otherwise non-canonical)
    /// prefix, and even when the root did not exist when the caller resolved it.
    ///
    /// This is the shape that strandded rootfs generations in the stable prefix
    /// on the KVM suite: `split_runtime_suffix` canonicalized each lower but
    /// compared it against a root captured raw, so no lower ever matched, the
    /// whole chain looked immutable, and compaction only re-committed the newest
    /// generation until the byte ceiling rejected the pause outright.
    #[test]
    fn split_runtime_suffix_matches_a_root_through_a_symlinked_prefix() {
        let temp = tempfile::tempdir().expect("tempdir");
        let real_root = temp.path().join("real").join("persisted-sandboxes");
        let symlink = temp.path().join("link");
        std::os::unix::fs::symlink(temp.path().join("real"), &symlink).expect("create symlink");

        // The root is resolved before the directory exists, exactly like the
        // process-wide roots cache filled at startup: canonicalization must
        // degrade to the raw path instead of failing.
        let unresolved_root = symlink.join("persisted-sandboxes");
        assert!(
            !unresolved_root.exists(),
            "the root must not exist when the caller resolves it"
        );

        std::fs::create_dir_all(real_root.join("artifacts").join("sandbox-a").join("gen1"))
            .expect("create runtime root");
        let runtime_lower = real_root
            .join("artifacts")
            .join("sandbox-a")
            .join("gen1")
            .join("rootfs.commit");
        std::fs::write(&runtime_lower, b"runtime lower").expect("write runtime lower");
        let stable_lower = temp
            .path()
            .join("image-cache")
            .join("base.overlaybd.commit");
        std::fs::create_dir_all(stable_lower.parent().unwrap()).expect("create cache root");
        std::fs::write(&stable_lower, b"stable lower").expect("write stable lower");

        let lowers = vec![
            local_layer_config(&stable_lower),
            local_layer_config(&runtime_lower),
        ];
        let runtime_owned_roots = [unresolved_root];

        let (preserved, runtime_owned) = split_runtime_suffix(lowers, &runtime_owned_roots);

        assert_eq!(
            preserved.len(),
            1,
            "only the cache-owned layer is a stable lower, got {preserved:#?}"
        );
        assert_eq!(PathBuf::from(&preserved[0].file), stable_lower);
        assert_eq!(
            runtime_owned.len(),
            1,
            "the lower under the symlinked root must stay runtime-owned, got {runtime_owned:#?}"
        );
        assert_eq!(PathBuf::from(&runtime_owned[0].file), runtime_lower);
    }

    #[tokio::test]
    async fn snapshot_adopts_persisted_runtime_suffix_by_link() {
        let temp = tempfile::tempdir().expect("tempdir");
        let artifacts_root = temp.path().join("persisted-sandboxes").join("artifacts");
        let output_dir = artifacts_root.join("sandbox-c").join("gen1").join("rootfs");
        let parent_root = artifacts_root.join("sandbox-b").join("gen1").join("rootfs");
        let cache_root = temp.path().join("image-cache").join("commits");
        std::fs::create_dir_all(&parent_root).expect("create parent rootfs");
        std::fs::create_dir_all(&cache_root).expect("create cache root");

        let parent_base = parent_root.join("managed-base.commit");
        let parent_snapshot = parent_root.join("snapshot.commit");
        let cache_layer = cache_root.join("sha256-base").join("overlaybd.commit");
        std::fs::create_dir_all(cache_layer.parent().unwrap()).expect("create cache layer dir");
        std::fs::write(&parent_base, b"parent base").expect("write parent base");
        std::fs::write(&parent_snapshot, b"parent snapshot").expect("write parent snapshot");
        std::fs::write(&cache_layer, b"cache base").expect("write cache layer");

        let lowers = vec![
            LayerConfig {
                file: cache_layer.display().to_string(),
                digest: "sha256:base".to_string(),
                size: 10,
                ..Default::default()
            },
            local_layer_config(&parent_base),
            local_layer_config(&parent_snapshot),
        ];
        let runtime_owned_roots = [artifacts_root.canonicalize().unwrap()];

        let rewritten = rewrite_lowers_with_runtime_roots(
            lowers,
            &output_dir,
            None,
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            ChainLimitPolicy::disabled(),
            None,
        )
        .await
        .expect("rewrite inherited runtime layers");

        assert_eq!(rewritten.len(), 3);
        assert_eq!(PathBuf::from(&rewritten[0].file), cache_layer);
        assert_eq!(rewritten[0].digest, "sha256:base");
        assert_eq!(rewritten[0].size, 10);

        for (index, (rewritten_lower, source)) in rewritten[1..]
            .iter()
            .zip([parent_base, parent_snapshot])
            .enumerate()
        {
            let adopted = PathBuf::from(&rewritten_lower.file);
            let expected_parent = output_dir
                .join(INHERITED_LAYERS_DIR)
                .join(format!("{index:04}"));
            assert!(adopted.starts_with(expected_parent));
            assert!(!adopted.starts_with(&parent_root));
            assert_eq!(adopted.file_name(), source.file_name());
            assert!(rewritten_lower.digest.is_empty());
            assert_eq!(rewritten_lower.size, 0);

            let source_metadata = std::fs::metadata(&source).expect("source metadata");
            let adopted_metadata = std::fs::metadata(&adopted).expect("adopted metadata");
            assert_eq!(source_metadata.dev(), adopted_metadata.dev());
            assert_eq!(source_metadata.ino(), adopted_metadata.ino());
        }
    }

    /// Two paths to the same inode must count once, otherwise every hardlinked
    /// generation would inflate the measured chain and trigger a compaction the
    /// ceiling does not actually require.
    #[test]
    fn chain_allocated_bytes_counts_a_hardlinked_layer_once() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layer = temp.path().join("generation.commit");
        std::fs::write(&layer, vec![0xABu8; 8192]).expect("write layer");
        let alias = temp.path().join("generation-hardlink.commit");
        std::fs::hard_link(&layer, &alias).expect("hardlink layer");

        let single = super::chain_allocated_bytes([layer.as_path()]);
        assert!(single > 0, "a written layer must allocate bytes");
        assert_eq!(
            super::chain_allocated_bytes([layer.as_path(), alias.as_path()]),
            single,
            "a hardlink must not be counted twice"
        );
        // A remote (empty) or missing path contributes no local bytes and must
        // not abort the measurement.
        let missing = temp.path().join("missing.commit");
        assert_eq!(
            super::chain_allocated_bytes([layer.as_path(), missing.as_path(), Path::new("")]),
            single
        );
    }

    /// The ceiling code must survive `finish_staging`, every other post-restack
    /// error must stay terminal.
    ///
    /// `finish_staging` runs after the restack, which has already sealed the
    /// live upper and reopened a fresh one. Treating every post-restack error as
    /// terminal is right for a lost restack reply or a descriptor size mismatch,
    /// because the runtime there is genuinely unknown. The ceiling is the one
    /// exception: the daemon leaves the device usable and the VM is still paused
    /// in memory, so the pause path must resume it instead of stopping it. This
    /// pins that the classification is decided by the error TYPE through the
    /// context chain, not by the message, so a wrapping `context` cannot flip it.
    #[test]
    fn finish_staging_keeps_the_ceiling_recoverable_and_everything_else_terminal() {
        let breakdown = ChainCeilingBreakdown {
            stable_prefix_lowers: 0,
            stable_prefix_bytes: 0,
            compacted_lowers: 4,
            compacted_bytes: 61_167_910_912,
            appended_bytes: 46_731_390_976,
        };
        // Verbatim shape of the incident: the chain needed 61,167,910,912 bytes
        // against a 51,539,607,552 byte ceiling.
        let ceiling = paused_chain_limit_exceeded(
            PausedChainLimit::ChainBytesCeiling,
            61_167_910_912,
            51_539_607_552,
            breakdown,
        )
        // The pause path wraps the rejection with context before it reaches
        // `finish_staging`; the classification has to survive that.
        .context("snapshot overlaybd runtime state to persistent dir");

        let classified = LiveOverlaybdSnapshotState::Restacked(PathBuf::from("/tmp/unused"))
            .finish_staging(Err(ceiling))
            .expect_err("a ceiling rejection is still an error");
        let capture_error = SandboxCaptureError::from(classified);
        assert!(
            !capture_error.is_terminal(),
            "a chain ceiling breach after the restack must stay recoverable: {capture_error:#}"
        );
        // The code lives in the nested error, one context layer in; the wrapper's
        // own `Display` shows only the outermost context, so the assertion reads
        // the full chain exactly as an operator's `{:#}` log line would.
        assert!(
            format!("{:#}", capture_error.inner()).contains(CHAIN_BYTES_CEILING_EXCEEDED),
            "the recoverable refusal must keep the named code: {:#}",
            capture_error.inner()
        );

        // The control: a descriptor size mismatch through the same path is a
        // post-restack error the pause cannot recover from and must stay
        // terminal, or a genuinely broken runtime would keep serving.
        let mismatch = into_terminal_snapshot_failure(anyhow::anyhow!(
            "restack snapshot descriptor size mismatch"
        ));
        let capture_error = SandboxCaptureError::from(
            LiveOverlaybdSnapshotState::Restacked(PathBuf::from("/tmp/unused"))
                .finish_staging(Err(mismatch))
                .expect_err("a size mismatch is an error"),
        );
        assert!(
            capture_error.is_terminal(),
            "a descriptor size mismatch must remain terminal: {capture_error:#}"
        );

        // And the same non-ceiling error without the explicit terminal wrapper
        // is *not* rescued by `finish_staging`: it becomes terminal there too.
        let unclassified = LiveOverlaybdSnapshotState::Restacked(PathBuf::from("/tmp/unused"))
            .finish_staging(Err(anyhow::anyhow!("restack snapshot reply never arrived")))
            .expect_err("a lost reply is an error");
        assert!(
            SandboxCaptureError::from(unclassified).is_terminal(),
            "a lost restack reply must remain terminal"
        );

        // The pause adds an outer `.context(...)` on its way up, exactly like
        // `snapshot_to_dir` does. `From<anyhow::Error>` downcasts through the
        // context chain, so the terminal classification a nested error carries
        // must survive that wrapper too — otherwise a size mismatch would turn
        // recoverable one frame up and the sandbox would keep serving a runtime
        // whose device state is unknown.
        let wrapped_mismatch = into_terminal_snapshot_failure(anyhow::anyhow!(
            "restack snapshot descriptor size mismatch"
        ))
        .context("snapshot overlaybd runtime state to persistent dir")
        .context("pause");
        assert!(
            SandboxCaptureError::from(wrapped_mismatch).is_terminal(),
            "a terminal post-restack error must survive the pause's context wrappers"
        );
    }

    /// The classification finds the ceiling code through any depth of context.
    ///
    /// The pause wraps its errors at every layer (device restack, staging, the
    /// pause itself). A message-only match would reclassify an unrelated error
    /// that happens to mention the code, and would miss the real one once a
    /// wrapper reworded it; matching the concrete type through the chain is the
    /// only stable answer. This pins both halves.
    #[test]
    fn ceiling_classification_survives_context_layers_and_ignores_lookalikes() {
        let real = paused_chain_limit_exceeded(
            PausedChainLimit::ChainBytesCeiling,
            61_167_910_912,
            51_539_607_552,
            ChainCeilingBreakdown::default(),
        )
        .context("stage snapshot")
        .context("restack rootfs")
        .context("pause");
        assert!(
            super::is_paused_chain_limit_exceeded(&real),
            "the real rejection must be recognized through three context layers"
        );

        let lookalike =
            anyhow::anyhow!("CHAIN_BYTES_CEILING_EXCEEDED: a message that only mentions the code");
        assert!(
            !super::is_paused_chain_limit_exceeded(&lookalike),
            "a message that merely mentions the code must not be reclassified"
        );
    }

    /// A rejected pause must not free a layer its live runtime config references.
    ///
    /// The restack repoints the live config at a layer written inside the
    /// generation directory the rejected pause then deletes. The relocation moves
    /// that layer to a directory that outlives the generation and rewrites the
    /// config to the new path, so the deletion only frees bytes nothing
    /// references. The control here is the same helper on a config that does not
    /// reference the sealed layer: it must fail rather than silently leave a
    /// dangling reference.
    #[tokio::test]
    async fn relocating_a_sealed_layer_keeps_the_live_config_resolvable() {
        let temp = tempfile::tempdir().expect("tempdir");
        let doomed_dir = temp.path().join("doomed-generation");
        std::fs::create_dir_all(&doomed_dir).expect("create doomed dir");
        let retained_dir = temp.path().join("snapshot-tree").join("retained-layers");

        let sealed = doomed_dir.join("snapshot.commit");
        std::fs::write(&sealed, vec![0x7Fu8; 8192]).expect("write sealed layer");

        let config_path = temp.path().join("runtime-image.json");
        let image_config = overlaybd::config::ImageConfig {
            lowers: vec![local_layer_config(&sealed)],
            ..Default::default()
        };
        std::fs::write(
            &config_path,
            serde_json::to_vec_pretty(&image_config).expect("serialize config"),
        )
        .expect("write config");

        let retained =
            super::relocate_sealed_layer_for_rejected_pause(&config_path, &sealed, &retained_dir)
                .await
                .expect("relocate the sealed layer");

        // The copy is durable, the original is gone, and the bytes match.
        assert!(retained.exists(), "the retained copy must exist");
        assert!(
            !sealed.exists(),
            "the doomed original must be removed once the copy is durable"
        );
        assert_eq!(
            std::fs::read(&retained).expect("read retained"),
            vec![0x7Fu8; 8192]
        );

        // The live config now resolves the layer to the retained copy, so the
        // generation directory can be deleted without dangling a reference.
        let rewritten = overlaybd::config::load_image_config(&config_path).expect("reload config");
        let resolved = fs::canonicalize(&rewritten.lowers[0].file)
            .unwrap_or_else(|_| PathBuf::from(&rewritten.lowers[0].file));
        assert_eq!(
            resolved,
            fs::canonicalize(&retained).expect("canonical retained path"),
            "the live config must point at the retained copy"
        );
        tokio::fs::remove_dir_all(&doomed_dir)
            .await
            .expect("delete the doomed generation");
        assert!(
            Path::new(&rewritten.lowers[0].file).exists(),
            "deleting the generation must not break the live config's reference"
        );
    }

    /// The relocation refuses to invent a reference and fails when the config
    /// does not name the sealed layer, so a caller cannot mistake a no-op for a
    /// retained layer.
    #[tokio::test]
    async fn relocating_a_layer_the_config_never_referenced_fails_loudly() {
        let temp = tempfile::tempdir().expect("tempdir");
        let sealed = temp.path().join("snapshot.commit");
        std::fs::write(&sealed, b"sealed").expect("write sealed layer");
        let other = temp.path().join("other.commit");
        std::fs::write(&other, b"other").expect("write other layer");

        let config_path = temp.path().join("runtime-image.json");
        let image_config = overlaybd::config::ImageConfig {
            lowers: vec![local_layer_config(&other)],
            ..Default::default()
        };
        std::fs::write(
            &config_path,
            serde_json::to_vec_pretty(&image_config).expect("serialize config"),
        )
        .expect("write config");

        let retained_dir = temp.path().join("retained-layers");
        let error =
            super::relocate_sealed_layer_for_rejected_pause(&config_path, &sealed, &retained_dir)
                .await
                .expect_err("a config that does not reference the layer must fail");
        assert!(
            error
                .to_string()
                .contains("does not reference the sealed layer"),
            "the failure must name the mismatch: {error:#}"
        );
    }

    /// The pre-flight refuses nothing and only reports whether the post-restack
    /// rejection is provably moot.
    #[test]
    fn preflight_marks_a_fitting_chain_and_never_refuses() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layer = temp.path().join("generation.commit");
        std::fs::write(&layer, vec![0xA5u8; 65536]).expect("write runtime layer");
        let upper_data = temp.path().join("upper.data");
        std::fs::write(&upper_data, vec![0x5Au8; 32768]).expect("write upper data");

        let roots = [temp.path().to_path_buf()];
        let chain = [local_layer_config(&layer)];

        // A ceiling that holds the chain and the upper: fitting, so the
        // post-compaction rejection cannot fire and is skipped.
        let measured_upper = super::chain_allocated_bytes([upper_data.as_path()]);
        assert!(measured_upper > 0, "the fixture must allocate upper bytes");
        let generous = ChainPreflight::measure(
            chain.clone(),
            std::slice::from_ref(&upper_data),
            &roots,
            ChainLimits {
                reclaimable_ceiling: u64::MAX,
                whole_chain_ceiling: 0,
            },
        );
        assert!(generous.guaranteed_fit());
        let measured_layer = super::chain_allocated_bytes([layer.as_path()]);
        assert_eq!(
            generous.measured_reclaimable_bytes,
            measured_layer + measured_upper
        );
        assert_eq!(
            generous.measured_total_bytes,
            measured_layer + measured_upper,
            "with no stable prefix the whole chain is the reclaimable part"
        );

        // A ceiling below the chain alone: not a guaranteed fit, and crucially
        // not a refusal either — the pre-flight only reports. The value stays
        // above zero so it is a real bound and not the disabled value.
        let tight = ChainPreflight::measure(
            chain,
            std::slice::from_ref(&upper_data),
            &roots,
            ChainLimits {
                reclaimable_ceiling: 1,
                whole_chain_ceiling: 0,
            },
        );
        assert!(!tight.guaranteed_fit());
        assert_eq!(tight.limits.reclaimable_ceiling, 1);

        // The disabled bounds measure nothing and fit nothing.
        let disabled = ChainPreflight::measure(
            [local_layer_config(&layer)],
            std::slice::from_ref(&upper_data),
            &roots,
            ChainLimits::disabled(),
        );
        assert_eq!(disabled, super::ChainPreflight::disabled());
        assert!(!disabled.guaranteed_fit());
    }

    /// The ceiling must bound what grows, not what is shared.
    ///
    /// These are the numbers the staging run of
    /// `ten_consecutive_pauses_stay_under_the_chain_bytes_ceiling` rejected on:
    /// a chain whose runtime-owned lowers compacted into a 2,408,116,224 byte
    /// layer, against a 2,838,224,896 byte ceiling, whose chain also carried a
    /// 690,716,672 byte stable prefix. Charging the prefix as well put the total
    /// at 3,098,832,896 bytes and refused the pause, although the part that
    /// grows pause by pause was 430,108,672 bytes under the ceiling. The prefix
    /// is materialized once from the image and shared by every sandbox of it, so
    /// counting it here charged the node's disk once per sandbox and rejected a
    /// chain that had already been brought under the ceiling by compaction.
    #[test]
    fn chain_ceiling_charges_the_reclaimable_part_and_not_the_shared_prefix() {
        // Verbatim from the rejected staging pause.
        let stable_prefix_bytes = 690_716_672u64;
        let compacted_bytes = 2_408_116_224u64;
        let appended_bytes = 536_989_696u64;
        let ceiling_bytes = 2_838_224_896u64;

        // The rule before this change charged every lower after compaction, and
        // the compacted layer already contains the appended one, so it computed
        // exactly the figure the fork CI reported.
        let old_total = stable_prefix_bytes + compacted_bytes;
        assert_eq!(
            old_total, 3_098_832_896,
            "the red case must reproduce the rejected total verbatim"
        );
        assert!(
            old_total > ceiling_bytes,
            "the fixture must be the rejected shape: total above the ceiling"
        );
        assert!(
            appended_bytes < compacted_bytes,
            "the appended layer is folded into the compacted one, never added beside it"
        );

        // Compaction folds the appended layer into the generation it writes, so
        // the reclaimable part after compaction is that generation alone. The
        // absolute bound stays the only one enabled here: the relative rootfs
        // bound judges the whole chain and has its own tests.
        let absolute_only = ChainLimits {
            reclaimable_ceiling: ceiling_bytes,
            whole_chain_ceiling: 0,
        };
        let after_compaction = ChainByteAccounting {
            reclaimable_bytes: compacted_bytes,
            stable_prefix_bytes,
            total_bytes: stable_prefix_bytes + compacted_bytes,
        };
        assert_eq!(
            after_compaction.breach(absolute_only),
            None,
            "a compacted chain whose reclaimable part is {compacted_bytes} bytes must fit the \
             {ceiling_bytes} byte ceiling; only the shared {stable_prefix_bytes} byte prefix \
             pushed the total over"
        );

        // The named rejection survives for the part that really does not fit:
        // compaction left a generation above the ceiling.
        let over_absolute = ChainByteAccounting {
            reclaimable_bytes: ceiling_bytes + 1,
            stable_prefix_bytes,
            total_bytes: stable_prefix_bytes + ceiling_bytes + 1,
        }
        .breach(absolute_only)
        .expect("a compacted generation above the ceiling must still be refused");
        assert_eq!(over_absolute.limit, PausedChainLimit::ChainBytesCeiling);
        assert_eq!(over_absolute.observed_bytes, ceiling_bytes + 1);
        assert_eq!(over_absolute.limit_bytes, ceiling_bytes);

        // And a prefix above the ceiling is not a breach of the absolute bound,
        // however large it is: the prefix belongs to the image, so judging it
        // there would refuse the pause of every sandbox of that image forever.
        // The rootfs chain is restacked under this same ceiling on every pause
        // and its image prefix alone is 690,712,576 bytes, so this is the case a
        // live sandbox meets, not a corner.
        assert_eq!(
            ChainByteAccounting {
                reclaimable_bytes: 1,
                stable_prefix_bytes: ceiling_bytes + 1,
                total_bytes: ceiling_bytes + 2,
            }
            .breach(absolute_only),
            None,
            "a shared prefix above the absolute ceiling must be reported, never refused"
        );
    }

    /// A layer hardlinked into both parts of a chain must be charged once.
    ///
    /// The parts are measured separately, so a shared inode set is what stops
    /// the same layer appearing in the prefix total and in the reclaimable
    /// total. Without it the chain reports a sum larger than its own
    /// deduplicated footprint and the ceiling refuses a pause the node's disk
    /// would have tolerated.
    #[test]
    fn chain_accounting_counts_a_layer_hardlinked_into_both_parts_once() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let cache_root = temp.path().join("image-cache");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&cache_root).expect("create cache root");

        let layer = cache_root.join("sha256-shared.overlaybd.commit");
        std::fs::write(&layer, vec![0x5Au8; 65536]).expect("write shared layer");
        // The same inode reachable from the runtime-owned tree.
        let adopted = runtime_root.join("inherited-layers").join("0000");
        std::fs::create_dir_all(&adopted).expect("create adopted dir");
        let adopted = adopted.join("overlaybd.commit");
        std::fs::hard_link(&layer, &adopted).expect("hardlink the layer into the suffix");
        let runtime_only = runtime_root.join("gen-runtime.overlaybd.commit");
        std::fs::write(&runtime_only, vec![0xA5u8; 32768]).expect("write runtime layer");
        assert_ne!(
            chain_allocated_bytes(std::slice::from_ref(&runtime_only)),
            chain_allocated_bytes(std::slice::from_ref(&layer)),
            "the fixture needs two distinguishable parts"
        );

        let chain = vec![
            local_layer_config(&layer),
            local_layer_config(&adopted),
            local_layer_config(&runtime_only),
            LayerConfig {
                file: String::new(),
                digest: "sha256:remote".to_string(),
                size: 4096,
                ..Default::default()
            },
        ];
        let roots = [runtime_root.canonicalize().unwrap()];
        let accounting = super::ChainByteAccounting::measure(chain.clone(), None, &roots);

        let chain_bytes = chain_allocated_bytes(
            &chain
                .iter()
                .map(|lower| PathBuf::from(&lower.file))
                .collect::<Vec<_>>(),
        );
        let shared_bytes = chain_allocated_bytes(std::slice::from_ref(&layer));
        assert!(
            chain_bytes > shared_bytes,
            "the parts must be distinguishable"
        );
        assert_eq!(
            accounting.stable_prefix_bytes + accounting.reclaimable_bytes,
            chain_bytes,
            "the two parts must sum to the chain, so the shared inode is charged once"
        );
        assert_eq!(
            accounting.total_bytes, chain_bytes,
            "the whole-chain total must be the deduplicated chain, not a per-part sum"
        );
        // The prefix is measured first, so the shared inode lands there: those
        // bytes exist for the image already, and leaving them out of the
        // reclaimable part keeps the ceiling from charging them as pause growth.
        assert_eq!(accounting.stable_prefix_bytes, shared_bytes);
        assert_eq!(
            accounting.reclaimable_bytes,
            chain_bytes - shared_bytes,
            "the runtime-owned part must count only the layer that is not shared"
        );
        // No part alone breaches a limit above each, so the chain fits even
        // though the naive prefix-plus-suffix sum would not.
        assert_eq!(
            accounting.breach(ChainLimits {
                reclaimable_ceiling: chain_bytes,
                whole_chain_ceiling: 0,
            }),
            None,
            "a chain whose parts fit individually must fit as a whole"
        );
    }

    /// A shared prefix larger than the ceiling must not make the pause fail.
    ///
    /// This is the rule the ceiling applies, seen through the rewrite: the
    /// prefix is a function of the image, not of this sandbox's pauses, so it is
    /// reported and never judged. The fixture is the shape that would break if
    /// it were judged — a prefix above the ceiling with a runtime-owned suffix
    /// that fits on its own. The suffix is adopted as it is, which is the
    /// pre-ceiling behavior, and the pause succeeds: no compaction is triggered
    /// by bytes the sandbox does not own, and no named rejection is raised.
    ///
    /// The earlier revision of this test asserted the opposite. Under the
    /// total-chain rule the naive prefix-plus-suffix sum was what the ceiling
    /// charged, so it compacted to get under a number the sandbox could never
    /// reach. Charging only the reclaimable part leaves nothing to compact: the
    /// ceiling is satisfied before the rewrite starts.
    #[tokio::test]
    async fn rewrite_ignores_a_shared_prefix_above_the_ceiling() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let stable_root = temp.path().join("image-cache");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&stable_root).expect("create stable layer root");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        // A shared, immutable lower standing in for the image prefix. The rootfs
        // image of the staging sandbox is exactly this shape: its prefix alone is
        // 690,712,576 bytes, far above any ceiling tight enough to bound one
        // sandbox's pause churn.
        let stable = stable_root.join("sha256-base.overlaybd.commit");
        std::fs::write(&stable, vec![0x11u8; 512 * 1024]).expect("write stable lower");
        let stable_bytes = chain_allocated_bytes(std::slice::from_ref(&stable));
        assert!(stable_bytes > 0, "the prefix must allocate bytes");

        // Two runtime-owned generations rewriting the same pages.
        let mut lowers = vec![local_layer_config(&stable)];
        for (index, byte) in [0xAAu8, 0xBB].into_iter().enumerate() {
            let path = seal_raw_test_layer(
                &runtime_root,
                &format!("gen-{index}"),
                8192,
                &[(0, byte), (4096, byte)],
            )
            .await;
            lowers.push(local_layer_config(&path));
        }
        let runtime_chain: Vec<PathBuf> = lowers[1..]
            .iter()
            .map(|lower| PathBuf::from(&lower.file))
            .collect();
        let runtime_bytes = chain_allocated_bytes(&runtime_chain);
        assert!(runtime_bytes > 0, "the suffix must allocate bytes");
        // Below the prefix, above the suffix: the ceiling the rootfs chain of a
        // real sandbox meets, where the prefix is the only part over it.
        let ceiling = stable_bytes / 2;
        assert!(
            stable_bytes > ceiling,
            "the prefix must be the part above the ceiling: {stable_bytes} <= {ceiling}"
        );
        assert!(
            runtime_bytes <= ceiling,
            "the reclaimable part must fit on its own: {runtime_bytes} > {ceiling}"
        );
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        let rewritten = rewrite_lowers_with_runtime_roots(
            lowers,
            &output_dir,
            None,
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            ChainLimitPolicy {
                limits: ChainLimits {
                    reclaimable_ceiling: ceiling,
                    whole_chain_ceiling: 0,
                },
                preflight: ChainPreflight::disabled(),
                compaction_view_bytes: 0,
            },
            None,
        )
        .await
        .expect("a shared prefix above the ceiling must not refuse the pause");

        // Adopted, not compacted: the append path keeps every lower and moves
        // the runtime-owned ones into the artifact directory.
        assert_eq!(
            rewritten.len(),
            3,
            "prefix plus both adopted generations, got {rewritten:#?}"
        );
        assert_eq!(PathBuf::from(&rewritten[0].file), stable);
        for adopted in &rewritten[1..] {
            assert!(
                PathBuf::from(&adopted.file).starts_with(output_dir.join(INHERITED_LAYERS_DIR)),
                "the runtime-owned suffix must be adopted, not compacted: {adopted:#?}"
            );
        }
        assert!(
            !output_dir.join(MANAGED_BASE_LAYER_FILE).exists(),
            "bytes the sandbox does not own must not trigger a compaction"
        );
    }

    /// A reclaimable part compaction cannot bring under the ceiling must fail
    /// the rewrite by name and leave no output behind.
    #[tokio::test]
    async fn rewrite_unreclaimable_chain_above_ceiling_fails_named_and_leaves_no_output() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        let runtime_layer = seal_raw_test_layer(&runtime_root, "gen-0", 4096, &[(0, 0xAA)]).await;
        let lowers = vec![local_layer_config(&runtime_layer)];
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        // 1 byte cannot be satisfied by any chain, so the second measurement
        // after compaction rejects even the compacted output.
        let err = rewrite_lowers_with_runtime_roots(
            lowers,
            &output_dir,
            None,
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            ChainLimitPolicy {
                limits: ChainLimits {
                    reclaimable_ceiling: 1,
                    whole_chain_ceiling: 0,
                },
                preflight: ChainPreflight::disabled(),
                compaction_view_bytes: 0,
            },
            None,
        )
        .await
        .expect_err("a chain above the ceiling must fail");

        assert!(
            err.to_string().contains(CHAIN_BYTES_CEILING_EXCEEDED),
            "error must carry the named code: {err:#}"
        );
        assert!(
            err.downcast_ref::<PausedChainLimitExceeded>().is_some(),
            "the error must stay downcastable so the pause path classifies it"
        );
        assert!(
            !output_dir.join(MANAGED_BASE_LAYER_FILE).exists(),
            "a rejected pause must not leave the compacted layer behind"
        );
        assert!(
            !output_dir.join(INHERITED_LAYERS_DIR).exists(),
            "a rejected pause must not adopt the runtime suffix"
        );
        assert!(
            !std::fs::read_dir(&output_dir)
                .expect("list output dir")
                .any(|entry| entry
                    .expect("read output entry")
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".tmp")),
            "a rejected pause must not leave a temporary compaction file"
        );
    }

    #[tokio::test]
    async fn rewrite_deep_stable_prefix_adopts_without_compaction() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        // A stable prefix deeper than DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS must
        // not trigger compaction on its own: only the runtime-owned suffix counts
        // toward the budget (32 - 33/4 = 24 here, and the suffix is just 2).
        let mut lowers: Vec<LayerConfig> = (0..33)
            .map(|index| LayerConfig {
                file: String::new(),
                digest: format!("sha256:base-{index}"),
                size: 4096,
                ..Default::default()
            })
            .collect();
        let runtime_a = runtime_root.join("a.commit");
        let runtime_b = runtime_root.join("b.commit");
        std::fs::write(&runtime_a, b"runtime a").expect("write runtime layer a");
        std::fs::write(&runtime_b, b"runtime b").expect("write runtime layer b");
        lowers.push(local_layer_config(&runtime_a));
        lowers.push(local_layer_config(&runtime_b));
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        let rewritten = rewrite_lowers_with_runtime_roots(
            lowers,
            &output_dir,
            None,
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            ChainLimitPolicy::disabled(),
            None,
        )
        .await
        .expect("rewrite with deep stable prefix");

        assert_eq!(rewritten.len(), 35);
        assert!(rewritten[..33].iter().all(|lower| lower.file.is_empty()));
        assert_eq!(rewritten[0].digest, "sha256:base-0");
        for (index, rewritten_lower) in rewritten[33..].iter().enumerate() {
            let adopted = PathBuf::from(&rewritten_lower.file);
            let expected_parent = output_dir
                .join(INHERITED_LAYERS_DIR)
                .join(format!("{index:04}"));
            assert!(adopted.starts_with(expected_parent));
        }
        assert!(!output_dir.join(MANAGED_BASE_LAYER_FILE).exists());
    }

    /// Seal a tiny raw overlaybd commit for compaction tests.
    async fn seal_raw_test_layer(
        dir: &Path,
        name: &str,
        vsize: u64,
        writes: &[(u64, u8)],
    ) -> PathBuf {
        let data = Arc::new(
            LocalFile::new(dir.join(format!("{name}.data"))).expect("create layer data file"),
        );
        let index = Arc::new(
            LocalFile::new(dir.join(format!("{name}.index"))).expect("create layer index file"),
        );
        let layer = overlaybd::index_file::LSMTFile::create(data, Some(index), vsize, false)
            .await
            .expect("create layer");
        for (offset, byte) in writes {
            layer
                .write_at(*offset, &[*byte; 4096])
                .await
                .expect("write layer page");
        }
        let commit_path = dir.join(format!("{name}.commit"));
        let output: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(&commit_path).expect("create layer commit output"));
        layer
            .commit_with_args(overlaybd::index_file::CommitArgs::new(output))
            .await
            .expect("commit raw layer");
        commit_path
    }

    /// Allocated bytes of an existing layer chain, using the production helper
    /// so the test exercises the same deduplication the ceiling relies on.
    fn chain_allocated_bytes(paths: &[PathBuf]) -> u64 {
        super::chain_allocated_bytes(paths.iter().map(PathBuf::as_path))
    }

    /// A chain whose every lower is runtime-owned must be compacted, not
    /// refused, when it crosses the ceiling.
    ///
    /// The ceiling path has to classify with the very predicate the diagnostic
    /// reports. When it instead measured a "stable prefix" the diagnostic never
    /// printed (the chain is runtime-owned end to end), the projection was the
    /// whole chain plus the appended layer, no suffix was left to compact, and
    /// the pause was refused by name although compaction would have fit.
    #[tokio::test]
    async fn rewrite_all_runtime_owned_chain_above_ceiling_compacts_and_succeeds() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        // Four runtime-owned generations, each rewriting the same two pages, so
        // one compacted generation deduplicates them and lands below the chain.
        let mut lowers = Vec::new();
        for (index, byte) in [0xAAu8, 0xBB, 0xCC, 0xDD].into_iter().enumerate() {
            let path = seal_raw_test_layer(
                &runtime_root,
                &format!("gen-{index}"),
                8192,
                &[(0, byte), (4096, byte)],
            )
            .await;
            lowers.push(local_layer_config(&path));
        }
        // The layer this pause appends. Its bytes belong to the chain, so the
        // ceiling has to account for it without it being compactable on its own.
        let appended = seal_raw_test_layer(&runtime_root, "appended", 8192, &[(0, 0xEE)]).await;
        let chain: Vec<PathBuf> = lowers
            .iter()
            .map(|lower| PathBuf::from(&lower.file))
            .chain(std::iter::once(appended.clone()))
            .collect();
        let chain_bytes = chain_allocated_bytes(&chain);
        assert!(chain_bytes > 0, "sealed layers must allocate bytes");
        // Above one compacted generation, below the chain: compaction fits, the
        // raw chain does not. Every lower is runtime-owned, so there is no
        // stable prefix to blame.
        let ceiling = chain_bytes * 3 / 4;
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        let rewritten = rewrite_lowers_with_runtime_roots(
            lowers,
            &output_dir,
            Some(local_layer_config(&appended)),
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            ChainLimitPolicy {
                limits: ChainLimits {
                    reclaimable_ceiling: ceiling,
                    whole_chain_ceiling: 0,
                },
                preflight: ChainPreflight::disabled(),
                compaction_view_bytes: 0,
            },
            None,
        )
        .await
        .expect("a fully runtime-owned chain above the ceiling must compact, not fail");

        assert_eq!(
            rewritten.len(),
            1,
            "the whole runtime-owned chain must collapse into one generation, got {rewritten:#?}"
        );
        let compacted = PathBuf::from(&rewritten[0].file);
        assert_eq!(compacted, output_dir.join(MANAGED_BASE_LAYER_FILE));
        assert!(
            chain_allocated_bytes(std::slice::from_ref(&compacted)) <= ceiling,
            "the compacted generation must fit under the ceiling"
        );
    }

    /// The negative control: ten pause cycles that rewrite the same block must
    /// not grow the chain ten times.
    ///
    /// Every cycle feeds the previous rewrite back as the live chain and appends
    /// a fresh generation covering the same guest offsets. Without compaction the
    /// chain would be ten copies of the region, well above twice the disk; the
    /// relative bound must trigger a compaction and leave the final chain within
    /// the multiple. This is the shape of the incident, reduced to the unit.
    #[tokio::test]
    async fn ten_pauses_rewriting_the_same_block_do_not_grow_the_chain() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        const DISK_BYTES: u64 = 1024 * 1024;
        const REGION_BYTES: u64 = 256 * 1024;
        const CYCLES: usize = 10;
        let limit_bytes = 2 * DISK_BYTES;
        let writes = |byte: u8| -> Vec<(u64, u8)> {
            (0..REGION_BYTES / 4096)
                .map(|index| (index * 4096, byte))
                .collect()
        };

        let mut lowers: Vec<LayerConfig> = Vec::new();
        let mut compaction_seen = false;
        for cycle in 0..CYCLES {
            let appended = seal_raw_test_layer(
                &runtime_root,
                &format!("cycle-{cycle}"),
                DISK_BYTES,
                &writes(cycle as u8 + 1),
            )
            .await;
            let output_dir = runtime_root.join(format!("out-{cycle}"));
            std::fs::create_dir_all(&output_dir).expect("create output dir");
            let rewritten = rewrite_lowers_with_runtime_roots(
                std::mem::take(&mut lowers),
                &output_dir,
                Some(local_layer_config(&appended)),
                MANAGED_BASE_LAYER_FILE,
                &runtime_owned_roots,
                OverlaybdCompactOutput::Raw,
                ChainLimitPolicy {
                    limits: ChainLimits {
                        reclaimable_ceiling: 0,
                        whole_chain_ceiling: limit_bytes,
                    },
                    preflight: ChainPreflight::disabled(),
                    compaction_view_bytes: 0,
                },
                None,
            )
            .await
            .unwrap_or_else(|error| {
                panic!("cycle {cycle} must compact instead of refusing: {error:#}")
            });
            if output_dir.join(MANAGED_BASE_LAYER_FILE).exists() {
                compaction_seen = true;
            }
            lowers = rewritten.lowers;
        }

        let final_paths: Vec<PathBuf> = lowers
            .iter()
            .filter(|lower| !lower.file.is_empty())
            .map(|lower| PathBuf::from(&lower.file))
            .collect();
        let final_bytes = chain_allocated_bytes(&final_paths);
        assert!(
            compaction_seen,
            "ten rewritten cycles must compact at least once"
        );
        assert!(
            final_bytes <= limit_bytes,
            "ten pauses rewriting the same block must leave the chain at or under {limit_bytes} \
             bytes: {final_bytes} bytes across {final_paths:?}"
        );
    }

    /// The transient bound is the smaller of the device view and the inputs,
    /// plus the index overhead: a chain far larger than its disk must not demand
    /// its whole chain size in free space.
    #[test]
    fn compaction_transient_bytes_is_bounded_by_the_smaller_side() {
        let with_overhead = |bytes: u64| bytes + bytes / 8;
        assert_eq!(super::compaction_transient_bytes(0, 1_000_000), 0);
        assert_eq!(
            super::compaction_transient_bytes(4096, 1 << 20),
            with_overhead(4096),
            "the device view bounds the merged commit when the inputs are larger"
        );
        assert_eq!(
            super::compaction_transient_bytes(1 << 20, 4096),
            with_overhead(4096),
            "the inputs bound the merged commit when the device view is larger"
        );
    }

    /// The headroom refusal is typed, recoverable and fail-closed when the
    /// filesystem cannot be probed: starting a rewrite without proven headroom
    /// is the half-way failure the check exists to prevent.
    #[test]
    fn compaction_headroom_refusal_is_typed_and_fails_closed() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layer = temp.path().join("generation.commit");
        std::fs::write(&layer, vec![0xABu8; 65536]).expect("write runtime layer");
        let compactable = [local_layer_config(&layer)];
        let compactable_bytes = super::chain_allocated_bytes([layer.as_path()]);
        let required = super::compaction_transient_bytes(4096, compactable_bytes);
        assert!(required > 0, "the fixture must allocate bytes");

        // Exactly enough free space passes: the check must not be stricter than
        // the bound it documents.
        assert!(
            super::ensure_compaction_headroom_with(
                temp.path(),
                4096,
                &compactable,
                MANAGED_BASE_LAYER_FILE,
                |_| Some((required, required)),
            )
            .is_ok(),
            "free bytes equal to the requirement must pass"
        );

        let error = super::ensure_compaction_headroom_with(
            temp.path(),
            4096,
            &compactable,
            MANAGED_BASE_LAYER_FILE,
            |_| Some((required - 1, required)),
        )
        .expect_err("one byte short must refuse");
        assert_eq!(
            super::pause_refusal_code(&error),
            Some(super::COMPACTION_HEADROOM_INSUFFICIENT)
        );
        assert!(
            error.to_string().contains(&required.to_string()),
            "the refusal must carry the requirement: {error}"
        );

        let error = super::ensure_compaction_headroom_with(
            temp.path(),
            4096,
            &compactable,
            MANAGED_BASE_LAYER_FILE,
            |_| None,
        )
        .expect_err("an unreadable filesystem must fail closed");
        assert_eq!(
            super::pause_refusal_code(&error),
            Some(super::COMPACTION_HEADROOM_INSUFFICIENT)
        );

        // `0` is the documented disable switch.
        assert!(
            super::ensure_compaction_headroom_with(
                temp.path(),
                0,
                &compactable,
                MANAGED_BASE_LAYER_FILE,
                |_| None,
            )
            .is_ok(),
            "a zero view must disable the check"
        );
    }

    /// A full artifact root must refuse the pause before `compact_layers`
    /// writes anything, and the refusal must stay recoverable so the pause path
    /// resumes the VM instead of stopping it.
    #[tokio::test]
    async fn rewrite_refuses_before_compaction_when_the_artifact_root_lacks_headroom() {
        // `/proc` reports zero free bytes and is never written to; the rewrite
        // must refuse before it reaches the output path there.
        let free =
            super::chain_refusal_log::filesystem_space(Path::new("/proc")).map(|(free, _)| free);
        assert_eq!(
            free,
            Some(0),
            "this fixture relies on /proc reporting zero free bytes"
        );

        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        let mut layers = Vec::new();
        for (index, byte) in [0xAAu8, 0xBB].into_iter().enumerate() {
            let path = seal_raw_test_layer(
                &runtime_root,
                &format!("gen-{index}"),
                8192,
                &[(0, byte), (4096, byte)],
            )
            .await;
            layers.push(local_layer_config(&path));
        }
        let appended = seal_raw_test_layer(&runtime_root, "appended", 8192, &[(0, 0xCC)]).await;
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        let error = rewrite_lowers_with_runtime_roots(
            layers,
            Path::new("/proc"),
            Some(local_layer_config(&appended)),
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            ChainLimitPolicy {
                limits: ChainLimits {
                    reclaimable_ceiling: 1,
                    whole_chain_ceiling: 0,
                },
                preflight: ChainPreflight::disabled(),
                compaction_view_bytes: 8192,
            },
            None,
        )
        .await
        .expect_err("a compaction on a full artifact root must refuse");

        assert_eq!(
            super::pause_refusal_code(&error),
            Some(super::COMPACTION_HEADROOM_INSUFFICIENT)
        );
        assert!(
            !Path::new("/proc").join(MANAGED_BASE_LAYER_FILE).exists(),
            "the refusal must happen before the rewrite writes its output"
        );

        // The pause path classifies by error type: a headroom refusal after the
        // restack must be resumed in place, never torn down.
        let classified = LiveOverlaybdSnapshotState::Restacked(PathBuf::from("/tmp/unused"))
            .finish_staging(Err(error))
            .expect_err("a headroom refusal is still an error");
        let capture_error = SandboxCaptureError::from(classified);
        assert!(
            !capture_error.is_terminal(),
            "a headroom refusal must stay recoverable: {capture_error:#}"
        );
        assert_eq!(
            super::pause_refusal_code(capture_error.inner()),
            Some(super::COMPACTION_HEADROOM_INSUFFICIENT)
        );
    }

    /// A whole rootfs chain above the disk multiple must compact even when the
    /// reclaimable part alone is far below the absolute ceiling.
    ///
    /// This is the incident's accounting: the stable prefix plus the runtime
    /// generations crossed the relative bound while the part compaction can
    /// reclaim stayed under the 48 GiB absolute ceiling, so the old check never
    /// fired. The relative bound has to see the whole deduplicated chain.
    #[tokio::test]
    async fn whole_rootfs_chain_above_the_disk_multiple_compacts() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let stable_root = temp.path().join("image-cache");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&stable_root).expect("create stable root");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        const DISK_BYTES: u64 = 768 * 1024;
        const REGION_BYTES: u64 = 256 * 1024;
        let limit_bytes = 2 * DISK_BYTES;
        let stable = stable_root.join("sha256-prefix.overlaybd.commit");
        std::fs::write(&stable, vec![0x11u8; 1024 * 1024]).expect("write stable prefix");
        let writes = |byte: u8| -> Vec<(u64, u8)> {
            (0..REGION_BYTES / 4096)
                .map(|index| (index * 4096, byte))
                .collect()
        };
        let mut lowers = vec![local_layer_config(&stable)];
        for (index, byte) in [0xAAu8, 0xBB, 0xCC].into_iter().enumerate() {
            let path = seal_raw_test_layer(
                &runtime_root,
                &format!("gen-{index}"),
                DISK_BYTES,
                &writes(byte),
            )
            .await;
            lowers.push(local_layer_config(&path));
        }
        let appended =
            seal_raw_test_layer(&runtime_root, "appended", DISK_BYTES, &writes(0xDD)).await;
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        let rewritten = rewrite_lowers_with_runtime_roots(
            lowers,
            &output_dir,
            Some(local_layer_config(&appended)),
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            ChainLimitPolicy {
                limits: ChainLimits {
                    // The reclaimable part alone fits comfortably; only the
                    // whole chain, prefix included, crosses the relative bound.
                    reclaimable_ceiling: 48 * 1024 * 1024 * 1024,
                    whole_chain_ceiling: limit_bytes,
                },
                preflight: ChainPreflight::disabled(),
                compaction_view_bytes: 0,
            },
            None,
        )
        .await
        .expect("the whole chain above the relative bound must compact, not refuse");

        assert_eq!(
            rewritten.len(),
            2,
            "the stable prefix plus one compacted generation, got {rewritten:#?}"
        );
        assert_eq!(PathBuf::from(&rewritten[0].file), stable);
        let compacted = PathBuf::from(&rewritten[1].file);
        assert_eq!(compacted, output_dir.join(MANAGED_BASE_LAYER_FILE));
        let whole_chain = chain_allocated_bytes(&[stable, compacted]);
        assert!(
            whole_chain <= limit_bytes,
            "the compacted chain must fit the disk multiple: {whole_chain} > {limit_bytes}"
        );
    }

    /// A prefix near the disk plus one full generation near the disk fits the
    /// multiple exactly and must not compact or refuse.
    ///
    /// Two times the disk is the floor: an immutable prefix and one complete
    /// generation can each approach the disk size, so a tighter multiple would
    /// refuse a legitimate pause. This guards the default from being lowered.
    #[tokio::test]
    async fn prefix_and_one_full_generation_fit_the_disk_multiple() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let stable_root = temp.path().join("image-cache");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&stable_root).expect("create stable root");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        const DISK_BYTES: u64 = 1024 * 1024;
        const PART_BYTES: u64 = 768 * 1024;
        let limit_bytes = 2 * DISK_BYTES;
        let stable = stable_root.join("sha256-prefix.overlaybd.commit");
        std::fs::write(&stable, vec![0x11u8; PART_BYTES as usize]).expect("write stable prefix");
        let writes: Vec<(u64, u8)> = (0..PART_BYTES / 4096)
            .map(|index| (index * 4096, 0xAA))
            .collect();
        let generation =
            seal_raw_test_layer(&runtime_root, "generation", DISK_BYTES, &writes).await;
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        let rewritten = rewrite_lowers_with_runtime_roots(
            vec![local_layer_config(&stable), local_layer_config(&generation)],
            &output_dir,
            None,
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            ChainLimitPolicy {
                limits: ChainLimits {
                    reclaimable_ceiling: 48 * 1024 * 1024 * 1024,
                    whole_chain_ceiling: limit_bytes,
                },
                preflight: ChainPreflight::disabled(),
                compaction_view_bytes: 0,
            },
            None,
        )
        .await
        .expect("a chain that fits the multiple must not be refused");

        assert_eq!(rewritten.len(), 2);
        assert_eq!(PathBuf::from(&rewritten[0].file), stable);
        assert!(
            !output_dir.join(MANAGED_BASE_LAYER_FILE).exists(),
            "a chain that already fits must be adopted, never compacted"
        );
        let whole_chain = chain_allocated_bytes(&[stable, generation]);
        assert!(
            whole_chain <= limit_bytes,
            "the fixture must fit the multiple: {whole_chain} > {limit_bytes}"
        );
    }

    /// A chain above the byte ceiling must collapse the runtime-owned suffix
    /// into one complete generation even though the layer-count budget alone
    /// would keep adopting it layer by layer.
    #[tokio::test]
    async fn rewrite_byte_ceiling_compacts_runtime_suffix() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        // Every generation rewrites the same two pages, so a full compaction
        // deduplicates them and lands well below the chain it replaces.
        let mut layers = Vec::new();
        for (index, byte) in [0xAAu8, 0xBB, 0xCC, 0xDD].into_iter().enumerate() {
            let path = seal_raw_test_layer(
                &runtime_root,
                &format!("gen-{index}"),
                8192,
                &[(0, byte), (4096, byte)],
            )
            .await;
            layers.push(local_layer_config(&path));
        }
        let chain_paths: Vec<PathBuf> = layers
            .iter()
            .map(|lower| PathBuf::from(&lower.file))
            .collect();
        let chain_bytes = chain_allocated_bytes(&chain_paths);
        assert!(chain_bytes > 0, "sealed layers must allocate bytes");
        // Above the compacted size, below the chain: exactly the case the byte
        // trigger exists for.
        let ceiling = chain_bytes * 3 / 4;
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        let rewritten = rewrite_lowers_with_runtime_roots(
            layers,
            &output_dir,
            None,
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            ChainLimitPolicy {
                limits: ChainLimits {
                    reclaimable_ceiling: ceiling,
                    whole_chain_ceiling: 0,
                },
                preflight: ChainPreflight::disabled(),
                compaction_view_bytes: 0,
            },
            None,
        )
        .await
        .expect("rewrite above the byte ceiling");

        assert_eq!(
            rewritten.len(),
            1,
            "a chain above the ceiling must collapse into one generation"
        );
        let compacted = PathBuf::from(&rewritten[0].file);
        assert_eq!(compacted, output_dir.join(MANAGED_BASE_LAYER_FILE));
        assert!(compacted.exists());
        assert!(
            chain_allocated_bytes(std::slice::from_ref(&compacted)) <= ceiling,
            "the compacted generation must fit under the ceiling"
        );
        assert!(
            !output_dir.join(INHERITED_LAYERS_DIR).exists(),
            "compaction must not adopt the suffix before collapsing it"
        );
    }

    /// A `0` ceiling is the kill switch: it must reproduce the pre-ceiling
    /// behavior byte for byte, adopting the runtime suffix instead of
    /// compacting it, even when the chain is far above any usable ceiling.
    #[tokio::test]
    async fn rewrite_with_disabled_ceiling_adopts_the_suffix_like_before() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");

        let mut layers = Vec::new();
        for (index, byte) in [0xAAu8, 0xBB, 0xCC].into_iter().enumerate() {
            let path = seal_raw_test_layer(
                &runtime_root,
                &format!("gen-{index}"),
                8192,
                &[(0, byte), (4096, byte)],
            )
            .await;
            layers.push(local_layer_config(&path));
        }
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        let mut results = Vec::new();
        for name in ["disabled-a", "disabled-b"] {
            let output_dir = temp.path().join(name);
            std::fs::create_dir_all(&output_dir).expect("create output dir");
            let rewritten = rewrite_lowers_with_runtime_roots(
                layers.clone(),
                &output_dir,
                None,
                MANAGED_BASE_LAYER_FILE,
                &runtime_owned_roots,
                OverlaybdCompactOutput::Raw,
                ChainLimitPolicy::disabled(),
                None,
            )
            .await
            .expect("rewrite with the ceiling disabled");
            results.push(rewritten);
        }

        // Three runtime layers with a count budget of 32: adopted, one
        // inherited-layers directory per layer, no compaction.
        assert_eq!(results[0].len(), 3);
        for (index, lower) in results[0].iter().enumerate() {
            assert!(
                PathBuf::from(&lower.file).starts_with(
                    temp.path()
                        .join("disabled-a")
                        .join(INHERITED_LAYERS_DIR)
                        .join(format!("{index:04}"))
                ),
                "a disabled ceiling must adopt layer {index} instead of compacting it"
            );
        }
        let file_names = |rewritten: &[LayerConfig]| {
            rewritten
                .iter()
                .map(|lower| {
                    PathBuf::from(&lower.file)
                        .file_name()
                        .map(ToOwned::to_owned)
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            file_names(&results[0]),
            file_names(&results[1]),
            "a disabled ceiling must be deterministic: same layers in the same order"
        );
        assert!(!temp
            .path()
            .join("disabled-a")
            .join(MANAGED_BASE_LAYER_FILE)
            .exists());
    }

    #[tokio::test]
    async fn rewrite_shrinking_budget_compacts_runtime_suffix() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        // A 124-layer stable prefix shrinks the budget to max(32 - 124/4, 1) = 1,
        // so a two-layer runtime-owned suffix already triggers compaction.
        let mut lowers: Vec<LayerConfig> = (0..124)
            .map(|index| LayerConfig {
                file: String::new(),
                digest: format!("sha256:base-{index}"),
                size: 4096,
                ..Default::default()
            })
            .collect();
        let layer_a = seal_raw_test_layer(&runtime_root, "a", 4096, &[(0, 0xAA)]).await;
        let layer_b = seal_raw_test_layer(&runtime_root, "b", 4096, &[(0, 0xBB)]).await;
        lowers.push(local_layer_config(&layer_a));
        lowers.push(local_layer_config(&layer_b));
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        let rewritten = rewrite_lowers_with_runtime_roots(
            lowers,
            &output_dir,
            None,
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            ChainLimitPolicy::disabled(),
            None,
        )
        .await
        .expect("rewrite with shrinking budget");

        assert_eq!(rewritten.len(), 125);
        assert!(rewritten[..124].iter().all(|lower| lower.file.is_empty()));
        let compacted = PathBuf::from(&rewritten[124].file);
        assert_eq!(compacted, output_dir.join(MANAGED_BASE_LAYER_FILE));
        assert!(compacted.exists());
        assert!(!compacted.with_extension("commit.tmp").exists());
    }

    #[tokio::test]
    async fn stage_live_runtime_preserves_restacked_layer_descriptor() {
        let temp = tempfile::tempdir().expect("tempdir");
        let live_dir = temp.path().join("live");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&live_dir).expect("create live dir");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        let snapshot_lower = output_dir.join("snapshot.commit");
        std::fs::write(&snapshot_lower, b"snapshot").expect("write snapshot lower");
        let live_runtime_image_config_path = live_dir.join("image.json");
        std::fs::write(
            &live_runtime_image_config_path,
            serde_json::to_vec_pretty(&json!({
                "lowers": [
                    {
                        "file": snapshot_lower,
                        "digest": "sha256:descriptor",
                        "size": 8
                    }
                ],
                "upper": {},
                "resultFile": live_dir.join("result.txt"),
                "download": {}
            }))
            .expect("serialize live image config"),
        )
        .expect("write live image config");

        let output_path = stage_overlaybd_snapshot_from_live_runtime(
            &live_runtime_image_config_path,
            &output_dir,
            Some(&snapshot_lower),
            ChainLimits::disabled(),
            &[],
            ChainPreflight::disabled(),
            0,
            "rootfs",
            false,
        )
        .await
        .expect("stage snapshot");

        let staged =
            overlaybd::config::load_image_config(&output_path).expect("load staged image config");
        let latest = staged.lowers.last().expect("latest lower");
        assert_eq!(PathBuf::from(&latest.file), snapshot_lower);
        assert_eq!(latest.digest, "sha256:descriptor");
        assert_eq!(latest.size, 8);
    }

    #[tokio::test]
    async fn build_mem_snapshot_image_config_skips_compaction_for_remote_lowers() {
        let temp = tempfile::tempdir().expect("tempdir");
        let parent_config_path = temp.path().join("parent-mem-image.json");
        let repo_blob_url = "s3://agentenv-oss-validation/validation/managed-layers";
        let remote_lowers: Vec<_> = (0..DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS)
            .map(|index| {
                json!({
                    "digest": format!("sha256:parent-{index}"),
                    "size": 4096
                })
            })
            .collect();
        std::fs::write(
            &parent_config_path,
            serde_json::to_vec_pretty(&json!({
                "repoBlobUrl": repo_blob_url,
                "lowers": remote_lowers,
                "upper": {},
                "resultFile": "",
                "download": {}
            }))
            .expect("serialize parent image config"),
        )
        .expect("write parent image config");

        let new_layer = temp.path().join("mem_overlaybd").join("overlaybd.commit");
        std::fs::create_dir_all(new_layer.parent().unwrap()).expect("create mem layer dir");
        std::fs::write(&new_layer, b"memory delta").expect("write mem layer");
        let (image_config, deferred) = build_mem_snapshot_image_config(
            Some(&parent_config_path),
            &new_layer,
            temp.path(),
            OverlaybdCompactOutput::Raw,
            ChainLimits::disabled(),
            &[],
            false,
        )
        .await
        .expect("build memory image config");
        assert!(
            deferred.is_none(),
            "a chain without a byte breach has nothing to defer"
        );

        assert_eq!(image_config.repo_blob_url, repo_blob_url);
        assert_eq!(
            image_config.lowers.len(),
            DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS + 1
        );
        assert_eq!(image_config.lowers[0].digest, "sha256:parent-0");
        assert!(image_config.lowers[..DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS]
            .iter()
            .all(|lower| lower.file.is_empty()));
        let latest = image_config.lowers.last().expect("latest mem lower");
        assert_eq!(PathBuf::from(&latest.file), new_layer);
        assert!(latest.digest.is_empty());
        assert_eq!(latest.size, 0);
        assert!(image_config.download_override.is_none());
    }

    struct PatternSource {
        data: Vec<u8>,
    }

    #[async_trait]
    impl VirtualFile for PatternSource {
        async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
            let offset = offset as usize;
            anyhow::ensure!(
                offset + len <= self.data.len(),
                "read past pattern source end"
            );
            Ok(Bytes::copy_from_slice(&self.data[offset..offset + len]))
        }

        async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
            let offset = offset as usize;
            anyhow::ensure!(
                offset + dst.len() <= self.data.len(),
                "read past pattern source end"
            );
            dst.copy_from_slice(&self.data[offset..offset + dst.len()]);
            Ok(dst.len())
        }

        async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
            anyhow::bail!("pattern source is read-only")
        }

        async fn size(&self) -> Result<u64> {
            Ok(self.data.len() as u64)
        }
    }

    struct FailingSource;

    #[async_trait]
    impl VirtualFile for FailingSource {
        async fn read_at(&self, _offset: u64, _len: usize) -> Result<Bytes> {
            anyhow::bail!("injected memory source read failure")
        }

        async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
            anyhow::bail!("injected memory source write failure")
        }

        async fn size(&self) -> Result<u64> {
            Ok(OVERLAYBD_ALIGNMENT)
        }
    }

    fn one_page_mapping() -> Vec<SegmentMapping> {
        vec![SegmentMapping::new(0, 1, 0, false, 0)]
    }

    #[tokio::test]
    async fn publish_memory_layer_failure_removes_temp_and_preserves_final() {
        let temp = tempfile::tempdir().expect("tempdir");
        let output = temp.path().join("memory.commit");
        let lower_tmp = output.with_extension("commit.tmp");
        let existing = b"existing final";
        tokio::fs::write(&output, existing)
            .await
            .expect("write existing final");
        let source: Arc<dyn VirtualFile> = Arc::new(FailingSource);
        let err = publish_memory_overlaybd_layer(
            &[source],
            &one_page_mapping(),
            OVERLAYBD_ALIGNMENT,
            &output,
            OverlaybdCompactOutput::Raw,
            1,
            false,
        )
        .await
        .expect_err("copy failure should be returned");
        assert!(
            err.to_string()
                .contains(MEMORY_SNAPSHOT_DIRTY_RANGE_READ_FAILED),
            "the read failure must carry its named code: {err:#}"
        );

        assert!(!lower_tmp.exists());
        assert_eq!(
            tokio::fs::read(&output).await.expect("read final"),
            existing
        );
    }

    /// R1: the pause that crosses the memory ceiling returns without calling
    /// `compact_layers`; the durable descriptor is the only compaction work it
    /// leaves, and the worker folds the chain under the ceiling afterwards.
    #[tokio::test]
    async fn deferred_pause_returns_without_compacting_and_the_worker_finishes_the_memory_chain() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let generation = temp.path().join("artifacts").join("sandbox").join("gen");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&generation).expect("create generation dir");

        let mut lowers = Vec::new();
        for (index, byte) in [0xAAu8, 0xBB, 0xCC].into_iter().enumerate() {
            let path = seal_raw_test_layer(
                &runtime_root,
                &format!("mem-{index}"),
                8192,
                &[(0, byte), (4096, byte)],
            )
            .await;
            lowers.push(local_layer_config(&path));
        }
        let appended = seal_raw_test_layer(
            &runtime_root,
            "mem-appended",
            8192,
            &[(0, 0xEE), (4096, 0xEE)],
        )
        .await;
        let chain_paths: Vec<PathBuf> = lowers
            .iter()
            .map(|lower| PathBuf::from(&lower.file))
            .chain(std::iter::once(appended.clone()))
            .collect();
        let chain_bytes = chain_allocated_bytes(&chain_paths);
        let ceiling = chain_bytes * 3 / 4;
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];

        let gate = DeferredCompactionGate::new_for_test();
        let request = DeferredCompactionRequest::for_test(
            gate.clone(),
            "memory",
            "mem_image.json",
            &generation,
            1,
            96 << 30,
        );

        let rewritten = rewrite_lowers_with_runtime_roots(
            lowers,
            &generation,
            Some(local_layer_config(&appended)),
            "mem_compacted.commit",
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            ChainLimitPolicy {
                limits: ChainLimits {
                    reclaimable_ceiling: ceiling,
                    whole_chain_ceiling: 0,
                },
                preflight: ChainPreflight::disabled(),
                compaction_view_bytes: 0,
            },
            Some(request),
        )
        .await
        .expect("a byte-triggered memory pause must defer the compaction");

        assert!(
            !generation.join("mem_compacted.commit").exists(),
            "the deferred pause must return without compacting"
        );
        let descriptor = rewritten
            .deferred
            .clone()
            .expect("the deferred pause must leave durable work");
        assert_eq!(
            descriptor.state,
            crate::overlaybd::DeferredCompactionState::Pending
        );
        assert!(!descriptor.compactable.is_empty());
        let descriptor_path = DeferredCompactionDescriptor::path_in(&generation);
        descriptor
            .write(&descriptor_path)
            .expect("write descriptor");

        // The caller writes the config after the rewrite, exactly like the
        // pause path does; the descriptor names it.
        let image_config = ImageConfig {
            lowers: rewritten.lowers.clone(),
            ..Default::default()
        };
        std::fs::write(
            generation.join("mem_image.json"),
            serde_json::to_vec_pretty(&image_config).expect("serialize mem image config"),
        )
        .expect("write mem image config");
        assert!(
            descriptor_path.exists(),
            "the pause must leave its work order next to the config"
        );
        // The pause returns as soon as it adopted the suffix and wrote the
        // config: every adopted input is reachable from the generation and the
        // compacted output does not exist yet. The worker's own suite proves the
        // rewrite and the unlink that follow (`maintenance::deferred_compaction`);
        // this test pins the pause's side of the contract.
        let staged = overlaybd::config::load_image_config(generation.join("mem_image.json"))
            .expect("load staged memory image config");
        let staged_files: Vec<String> = staged
            .lowers
            .iter()
            .map(|lower| lower.file.clone())
            .collect();
        assert_eq!(
            staged_files.len(),
            descriptor.compactable.len(),
            "every layer the descriptor folds must be named by the staged config"
        );
        assert!(
            staged_files
                .iter()
                .all(|file| Path::new(file).starts_with(&generation)),
            "the pause must leave the runtime layers inside the generation"
        );
    }

    /// R2: the same mechanism on the disk chain. The whole rootfs chain above
    /// `rootfs_chain_disk_multiple` is not compacted inside the pause; the worker
    /// compacts it and the deduplicated chain comes back under the multiple.
    #[tokio::test]
    async fn deferred_pause_returns_without_compacting_and_the_worker_finishes_the_rootfs_chain() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let stable_root = temp.path().join("image-cache");
        let generation = temp
            .path()
            .join("artifacts")
            .join("sandbox")
            .join("gen")
            .join("rootfs");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&stable_root).expect("create stable root");
        std::fs::create_dir_all(&generation).expect("create generation dir");

        const DISK_BYTES: u64 = 768 * 1024;
        const REGION_BYTES: u64 = 256 * 1024;
        let limit_bytes = 2 * DISK_BYTES;
        let stable = stable_root.join("sha256-prefix.overlaybd.commit");
        std::fs::write(&stable, vec![0x11u8; 1024 * 1024]).expect("write stable prefix");
        let writes = |byte: u8| -> Vec<(u64, u8)> {
            (0..REGION_BYTES / 4096)
                .map(|index| (index * 4096, byte))
                .collect()
        };
        let mut lowers = vec![local_layer_config(&stable)];
        for (index, byte) in [0xAAu8, 0xBB, 0xCC].into_iter().enumerate() {
            let path = seal_raw_test_layer(
                &runtime_root,
                &format!("rootfs-{index}"),
                DISK_BYTES,
                &writes(byte),
            )
            .await;
            lowers.push(local_layer_config(&path));
        }
        let appended =
            seal_raw_test_layer(&runtime_root, "rootfs-appended", DISK_BYTES, &writes(0xDD)).await;
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];
        let gate = DeferredCompactionGate::new_for_test();
        let request = DeferredCompactionRequest::for_test(
            gate.clone(),
            "rootfs",
            "image.json",
            &generation,
            1,
            96 << 30,
        );

        let rewritten = rewrite_lowers_with_runtime_roots(
            lowers,
            &generation,
            Some(local_layer_config(&appended)),
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            ChainLimitPolicy {
                limits: ChainLimits {
                    reclaimable_ceiling: 0,
                    whole_chain_ceiling: limit_bytes,
                },
                preflight: ChainPreflight::disabled(),
                compaction_view_bytes: DISK_BYTES,
            },
            Some(request),
        )
        .await
        .expect("a rootfs chain over the disk multiple must defer");

        assert!(
            !generation.join(MANAGED_BASE_LAYER_FILE).exists(),
            "the pause must not compact the rootfs inline"
        );
        let descriptor = rewritten.deferred.clone().expect("durable work");
        let descriptor_path = DeferredCompactionDescriptor::path_in(&generation);
        descriptor
            .write(&descriptor_path)
            .expect("write descriptor");
        std::fs::write(
            generation.join("image.json"),
            serde_json::to_vec_pretty(&ImageConfig {
                lowers: rewritten.lowers.clone(),
                ..Default::default()
            })
            .expect("serialize rootfs image config"),
        )
        .expect("write rootfs image config");

        // The pause's side of the contract only: the whole chain is still above
        // the multiple because the rewrite was deferred, not refused, and every
        // adopted layer is reachable from the generation. The worker's suite
        // proves the rewrite that brings the deduplicated chain back under it.
        assert!(
            !generation.join(MANAGED_BASE_LAYER_FILE).exists(),
            "the deferred pause must not write the compacted generation"
        );
        let staged = overlaybd::config::load_image_config(generation.join("image.json"))
            .expect("load staged rootfs image config");
        assert_eq!(
            staged
                .lowers
                .iter()
                .filter(|lower| Path::new(&lower.file).starts_with(&generation))
                .count(),
            descriptor.compactable.len(),
            "the staged config must name every layer the worker will fold"
        );
        let total_now = chain_allocated_bytes(
            &staged
                .lowers
                .iter()
                .filter(|lower| !lower.file.is_empty())
                .map(|lower| PathBuf::from(&lower.file))
                .collect::<Vec<_>>(),
        );
        assert!(
            total_now > limit_bytes,
            "the deferred generation is still above the multiple until the worker runs: \
             {total_now} vs {limit_bytes}"
        );
    }

    /// R3: admission that cannot prove the node can finish the work takes
    /// today's inline path, and that path keeps its three named codes.
    #[tokio::test]
    async fn a_refused_admission_keeps_the_inline_compaction_and_its_codes() {
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_root = temp.path().join("runtime");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&runtime_root).expect("create runtime root");
        std::fs::create_dir_all(&output_dir).expect("create output dir");
        let mut layers = Vec::new();
        for (index, byte) in [0xAAu8, 0xBB].into_iter().enumerate() {
            let path = seal_raw_test_layer(
                &runtime_root,
                &format!("gen-{index}"),
                8192,
                &[(0, byte), (4096, byte)],
            )
            .await;
            layers.push(local_layer_config(&path));
        }
        let appended = seal_raw_test_layer(&runtime_root, "appended", 8192, &[(0, 0xCC)]).await;
        // Above one compacted generation but below the chain, so the inline path
        // compacts and succeeds instead of refusing for a reason of its own.
        let chain_paths: Vec<PathBuf> = layers
            .iter()
            .map(|lower| PathBuf::from(&lower.file))
            .chain(std::iter::once(appended.clone()))
            .collect();
        let reclaimable_ceiling = chain_allocated_bytes(&chain_paths) * 3 / 4;
        let runtime_owned_roots = [runtime_root.canonicalize().unwrap()];
        let gate = DeferredCompactionGate::new_for_test();
        // The node already has its one in-flight compaction, so the pause must
        // not queue more work.
        gate.begin_in_flight();
        let request = DeferredCompactionRequest::for_test(
            gate.clone(),
            "rootfs",
            "image.json",
            &output_dir,
            1,
            u64::MAX,
        );

        let rewritten = rewrite_lowers_with_runtime_roots(
            layers,
            &output_dir,
            Some(local_layer_config(&appended)),
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
            ChainLimitPolicy {
                limits: ChainLimits {
                    reclaimable_ceiling,
                    whole_chain_ceiling: 0,
                },
                preflight: ChainPreflight::disabled(),
                compaction_view_bytes: 0,
            },
            Some(request),
        )
        .await
        .expect("the inline path still compacts");
        gate.end_in_flight();

        assert!(
            output_dir.join(MANAGED_BASE_LAYER_FILE).exists(),
            "a refused admission must fall back to today's inline compaction"
        );
        assert!(
            rewritten.deferred.is_none(),
            "an inline compaction leaves no deferred work"
        );
        assert_eq!(gate.pending_transient_bytes(), 0);

        // The refusal catalog the pause exposes keeps exactly the three codes the
        // card requires; admission is a path decision, never an error.
        assert_eq!(
            pause_refusal_code(&paused_chain_limit_error(
                PausedChainLimit::ChainBytesCeiling,
                10,
                1
            )),
            Some(CHAIN_BYTES_CEILING_EXCEEDED)
        );
        assert_eq!(
            pause_refusal_code(&paused_chain_limit_error(
                PausedChainLimit::RootfsDiskMultiple,
                10,
                1
            )),
            Some(ROOTFS_CHAIN_DISK_MULTIPLE_EXCEEDED)
        );
        assert_eq!(
            pause_refusal_code(&compaction_headroom_insufficient(10, Some(0))),
            Some(COMPACTION_HEADROOM_INSUFFICIENT)
        );
    }

    #[tokio::test]
    async fn publish_memory_layer_records_discarded_pages_as_zeroed_segments() {
        use overlaybd::index_file::open_file_ro;

        let temp = tempfile::tempdir().expect("tempdir");
        let output = temp.path().join("memory.commit");
        let mut data = vec![0x77u8; 4096];
        data.extend(std::iter::repeat_n(0u8, 4096));
        let source: Arc<dyn VirtualFile> = Arc::new(PatternSource { data });

        let stats = publish_memory_overlaybd_layer(
            &[source],
            &[SegmentMapping::new(0, 16, 0, false, 0)],
            16 * OVERLAYBD_ALIGNMENT,
            &output,
            OverlaybdCompactOutput::Raw,
            1,
            false,
        )
        .await
        .expect("publish memory layer");

        assert_eq!(stats.skipped_bytes, 4096);
        assert_eq!(stats.written_bytes, 4096);

        let layer_file = Arc::new(LocalFile::open_ro(&output).expect("open published layer"));
        let ro = open_file_ro(layer_file)
            .await
            .expect("read published layer");
        assert_eq!(
            ro.read_at(0, 4096).await.expect("read data page").as_ref(),
            &[0x77u8; 4096]
        );
        assert_eq!(
            ro.read_at(4096, 4096)
                .await
                .expect("read zero page")
                .as_ref(),
            &[0u8; 4096],
            "a discarded page must read back as zeros from the sealed layer"
        );
        assert!(
            ro.index_view()
                .expect("index view")
                .mappings()
                .iter()
                .any(|m| m.zeroed),
            "the published memory layer must carry a zeroed segment"
        );
    }

    #[tokio::test]
    async fn drop_page_cache_after_write_keeps_the_layer_readable() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("memory.commit");
        let payload = vec![7u8; 64 * 1024];
        tokio::fs::write(&path, &payload)
            .await
            .expect("write layer");

        drop_page_cache_after_write(&path)
            .await
            .expect("drop page cache");

        assert_eq!(
            tokio::fs::read(&path).await.expect("read layer"),
            payload,
            "the eviction hint must never change what a read returns"
        );
    }
}
