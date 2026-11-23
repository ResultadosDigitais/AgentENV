//! Compacting a paused sandbox's chain outside the pause call.
//!
//! A pause whose byte bound is crossed leaves a [`DeferredCompactionDescriptor`]
//! beside its image config and returns; this pass is what pays the rewrite. It
//! is deliberately *not* part of the pause: the 13/11/2026 measurement showed a
//! 12 GiB pause taking 46.4 s when it compacted and about 12 s when it only wrote
//! what was dirty, and the card's goal is to take that difference off the call.
//!
//! # Two passes, in this order
//!
//! 1. A descriptor still `Pending` is executed: the compacted output is written,
//!    then the image config that names the inputs is replaced atomically
//!    (temp + fsync + rename), then the descriptor is marked `Swapped`. Marking
//!    comes last, so a crash before it leaves the input layers untouched and the
//!    next pass retries from the durable config.
//! 2. A descriptor already `Swapped` only unlinks the inputs the *current*
//!    config no longer references, and then removes itself. It never unlinks in
//!    the same pass that swapped, which is what protects a resume that read the
//!    old config: its layers stay reachable for the rest of that pass. The
//!    pause's inline fallback writes its descriptor already `Swapped` — it
//!    compacted inside the call and left only the unlink — so the copies a
//!    compaction replaced do not stay on the node beside the compacted one.
//!
//! # What this pass never does
//!
//! It only reads generations a `Paused` record names, and the list is rebuilt
//! every pass ([`crate::orchestrator::PausedArtifactCandidate`]); a resume
//! clears the record's `Paused` state before it decodes anything, so a
//! generation being resumed is not handed to this pass. A generation whose
//! record disappeared is the existing quarantine/trash sweep's job, not this
//! one's: this pass never deletes a generation it was not handed.
//!
//! # Failure
//!
//! A failed compaction removes only the output it was writing and keeps the
//! descriptor and every input layer. The generation a paused sandbox resumes
//! from is untouched, so the failure costs a retry, never a sandbox.

use std::collections::HashSet;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::orchestrator::PausedArtifactCandidate;
use crate::overlaybd::{
    compact_layers, compactable_allocated_bytes, compaction_transient_bytes, filesystem_free_bytes,
    record_deferred_compaction, write_file_atomically_with_hook, DeferredCompactionDescriptor,
    DeferredCompactionGate, DeferredCompactionState, OverlaybdCompactOutput,
    DEFERRED_COMPACTION_FILE,
};

/// How often the node runs one pass.
///
/// The pause no longer waits for the rewrite, so this only bounds how long a
/// generation stays above its bound. It is shorter than the paused-artifact
/// sweep because it drains the queue the pauses build, not a rare orphan.
pub const DEFERRED_COMPACTION_INTERVAL: Duration = Duration::from_secs(30);

/// Named log line of one successful deferred compaction.
pub const DEFERRED_COMPACTION_LOG: &str = "paused sandbox chain compacted outside the pause";

/// Named log line of the pass that frees the copies a compaction replaced.
///
/// A compacted chain that keeps its inputs costs the node the compacted copy
/// *and* the delta it folded — the 21/11 measurement found 146 GiB in one
/// generation where about 79 were needed. This line is what an operator reads
/// to see the bytes actually come back.
pub const DEFERRED_RECLAIM_LOG: &str =
    "paused sandbox chain removed the copies its compaction replaced";

/// What one pass did, aggregated across the node.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DeferredCompactionReport {
    /// Descriptors executed in this pass.
    pub compacted: usize,
    /// Descriptors whose rewrite failed; they stay queued for the next pass.
    pub failed: usize,
    /// Descriptors refused before the rewrite because the node lacks headroom.
    pub headroom_refused: usize,
    /// Descriptors left for a later pass because this one was at its
    /// concurrency budget (or the pending-bytes budget).
    pub queued: usize,
    /// Descriptors whose swapped-in inputs were unlinked.
    pub unlinked: usize,
    /// Bytes the node actually got back in this pass: only an inode whose last
    /// link was removed counts, so an adopted hardlink of a layer an older
    /// generation still owns adds nothing (the sweep frees it later).
    pub reclaimed_bytes: u64,
}

/// The filesystem probe the worker uses before a rewrite. Production reads the
/// real node; a test injects the answer it needs to exercise the refusal.
type FreeBytesProbe = dyn Fn(&Path) -> Option<u64> + Send + Sync;

/// Called with `(config path, complete temp file)` at the swap point.
type SwapHook = dyn Fn(&Path, &Path) + Send + Sync;

/// Drains deferred compaction work for this node.
pub struct DeferredCompactionWorker {
    gate: Arc<DeferredCompactionGate>,
    max_concurrent: usize,
    pending_budget: u64,
    free_bytes: Arc<FreeBytesProbe>,
    #[cfg(test)]
    swap_hook: Option<Arc<SwapHook>>,
}

impl DeferredCompactionWorker {
    pub fn new(
        gate: Arc<DeferredCompactionGate>,
        max_concurrent: usize,
        pending_budget: u64,
    ) -> Self {
        Self {
            gate,
            max_concurrent: max_concurrent.max(1),
            pending_budget,
            free_bytes: Arc::new(filesystem_free_bytes),
            #[cfg(test)]
            swap_hook: None,
        }
    }

    /// The worker a node runs, bound to the process-wide gate the pauses consult
    /// before they leave work behind.
    ///
    /// Used by the server's periodic task and by the integration suite, which
    /// has to execute the same worker against a real guest to measure the bytes
    /// a pause leaves on disk.
    pub fn for_node(max_concurrent: usize, pending_budget: u64) -> Self {
        Self::new(
            DeferredCompactionGate::global(),
            max_concurrent,
            pending_budget,
        )
    }

    /// Replace the free-bytes probe. Test-only: production always reads the
    /// filesystem that holds the generation.
    #[cfg(test)]
    pub(crate) fn with_free_bytes_probe(
        mut self,
        probe: impl Fn(&Path) -> Option<u64> + Send + Sync + 'static,
    ) -> Self {
        self.free_bytes = Arc::new(probe);
        self
    }

    /// Observe the swap point. Test-only; the deterministic swap test reads both
    /// the old config and the complete new temp file there.
    #[cfg(test)]
    pub(crate) fn with_swap_hook(
        mut self,
        hook: impl Fn(&Path, &Path) + Send + Sync + 'static,
    ) -> Self {
        self.swap_hook = Some(Arc::new(hook));
        self
    }

    /// Run one pass over the records this node can act on.
    ///
    /// The pass is deterministic in the order it works (descriptor paths sorted)
    /// and never starts more than `max_concurrent` rewrites: the remainder stays
    /// queued and is counted as such, so the bounded-concurrency behavior is
    /// observable instead of silent.
    pub async fn run_once(
        &self,
        candidates: &[PausedArtifactCandidate],
    ) -> DeferredCompactionReport {
        let mut report = DeferredCompactionReport::default();
        let mut work = Vec::new();
        for candidate in candidates {
            work.extend(descriptors_for(candidate));
        }
        work.sort_by(|left, right| left.0.cmp(&right.0));
        // One descriptor can be named by two paths of the same candidate (the
        // config directory and the generation directory); it must run once.
        work.dedup_by(|left, right| left.0 == right.0);

        // Pass one: finish every swap whose inputs are already unreferenced.
        for (path, descriptor) in &work {
            if descriptor.state != DeferredCompactionState::Swapped {
                continue;
            }
            match unlink_swapped_inputs(path, descriptor).await {
                Ok(reclaimed_bytes) => {
                    if let Err(error) = std::fs::remove_file(path) {
                        warn!(
                            path = %path.display(),
                            error = %error,
                            "failed to remove a finished deferred compaction descriptor"
                        );
                        continue;
                    }
                    self.gate.complete(path);
                    report.unlinked += 1;
                    report.reclaimed_bytes += reclaimed_bytes;
                    record_deferred_compaction(descriptor.kind.as_str(), "reclaimed");
                    info!(
                        kind = descriptor.kind.as_str(),
                        generation = descriptor.generation.as_str(),
                        reclaimed_bytes,
                        inputs = descriptor.compactable.len(),
                        "{}",
                        DEFERRED_RECLAIM_LOG
                    );
                }
                Err(error) => warn!(
                    path = %path.display(),
                    error = %format_args!("{error:#}"),
                    "failed to unlink swapped deferred compaction inputs; will retry"
                ),
            }
        }

        // Pass two: execute pending work, bounded by the concurrency and
        // pending-bytes budgets.
        let mut started = 0usize;
        for (path, descriptor) in work
            .iter()
            .filter(|(_, descriptor)| descriptor.state == DeferredCompactionState::Pending)
        {
            let transient = compaction_transient_bytes(
                descriptor.view_bytes,
                compactable_allocated_bytes(&descriptor.compactable_layers()),
            );
            // Rebuild the in-memory queue after a restart: the descriptors on
            // disk are the durable truth.
            self.gate.register(path, transient);
            let over_budget = self.gate.pending_transient_bytes() > self.pending_budget;
            if started >= self.max_concurrent || over_budget {
                record_deferred_compaction(descriptor.kind.as_str(), "queued");
                report.queued += 1;
                continue;
            }
            started += 1;
            self.gate.begin_in_flight();
            let result = self.compact_one(path, descriptor).await;
            self.gate.end_in_flight();
            match result {
                Ok(CompactionOutcome::Compacted) => {
                    self.gate.complete(path);
                    report.compacted += 1;
                }
                Ok(CompactionOutcome::NothingToCompact) => {
                    self.gate.complete(path);
                }
                Ok(CompactionOutcome::HeadroomRefused) => {
                    report.headroom_refused += 1;
                }
                Err(error) => {
                    warn!(
                        path = %path.display(),
                        kind = descriptor.kind.as_str(),
                        error = %format_args!("{error:#}"),
                        "deferred chain compaction failed; keeping the descriptor for the next pass"
                    );
                    record_deferred_compaction(descriptor.kind.as_str(), "failed");
                    report.failed += 1;
                }
            }
        }
        report
    }

    async fn compact_one(
        &self,
        descriptor_path: &Path,
        descriptor: &DeferredCompactionDescriptor,
    ) -> Result<CompactionOutcome> {
        let directory = descriptor_directory(descriptor_path)?;
        let config_path = directory.join(&descriptor.config_file);
        let output_path = directory.join(&descriptor.output_name);
        let layers = descriptor.compactable_layers();
        if layers.is_empty() {
            return Ok(CompactionOutcome::NothingToCompact);
        }
        // The crash window between the atomic rename and marking the descriptor
        // `Swapped`, and a descriptor an inline fallback superseded: the config
        // already names the output and no longer names the inputs. Rewriting the
        // output from stale inputs here would regress a newer generation, so the
        // pass only records that the swap is done.
        if config_already_swapped(&config_path, &descriptor.compactable, &output_path)? {
            let mut swapped = descriptor.clone();
            swapped.state = DeferredCompactionState::Swapped;
            swapped.write(descriptor_path)?;
            record_deferred_compaction(descriptor.kind.as_str(), "already_swapped");
            return Ok(CompactionOutcome::Compacted);
        }

        // The pause's pre-rewrite refusal, repeated here because the worker can
        // run long after the pause and on a node that has filled up since. It
        // runs before `compact_layers`, so a full node never half-writes an
        // output.
        let required =
            compaction_transient_bytes(descriptor.view_bytes, compactable_allocated_bytes(&layers));
        let free_bytes = (self.free_bytes)(directory);
        if required > 0 && !free_bytes.is_some_and(|free| free >= required) {
            record_deferred_compaction(descriptor.kind.as_str(), "headroom_refused");
            warn!(
                kind = descriptor.kind.as_str(),
                generation = descriptor.generation.as_str(),
                artifact_root = %directory.display(),
                required_bytes = required,
                free_bytes,
                "deferred compaction needs more transient disk than the node has; \
                 keeping the descriptor for a later pass"
            );
            return Ok(CompactionOutcome::HeadroomRefused);
        }

        let before_bytes = compactable_allocated_bytes(&layers);
        let started = Instant::now();
        let compacted = compact_layers(&layers, &output_path, OverlaybdCompactOutput::Raw).await?;
        if compacted.is_none() {
            return Ok(CompactionOutcome::NothingToCompact);
        }
        let after_bytes = compactable_allocated_bytes(&[overlaybd::config::LayerConfig {
            file: output_path.display().to_string(),
            ..Default::default()
        }]);

        #[cfg(test)]
        let hook = self.swap_hook.as_deref();
        #[cfg(not(test))]
        let hook: Option<&SwapHook> = None;
        swap_config_atomically(&config_path, &descriptor.compactable, &output_path, hook)
            .with_context(|| {
                format!(
                    "swap image config after deferred compaction {}",
                    config_path.display()
                )
            })?;
        let mut swapped = descriptor.clone();
        swapped.state = DeferredCompactionState::Swapped;
        swapped.write(descriptor_path)?;

        record_deferred_compaction(descriptor.kind.as_str(), "compacted");
        info!(
            kind = descriptor.kind.as_str(),
            generation = descriptor.generation.as_str(),
            bytes_before = before_bytes,
            bytes_after = after_bytes,
            duration_ms = started.elapsed().as_millis() as u64,
            "{}",
            DEFERRED_COMPACTION_LOG
        );
        if exceeds_a_bound(after_bytes, descriptor) {
            // The pause promised the chain would come down; if it did not, the
            // operator must see the numbers. The generation stays usable and is
            // never deleted here — the next pause folds another generation in
            // and the descriptor is retried only while it is `Pending`.
            record_deferred_compaction(descriptor.kind.as_str(), "still_over_limit");
            warn!(
                kind = descriptor.kind.as_str(),
                generation = descriptor.generation.as_str(),
                bytes_after = after_bytes,
                reclaimable_ceiling = descriptor.reclaimable_ceiling,
                whole_chain_ceiling = descriptor.whole_chain_ceiling,
                "deferred chain compaction finished but the generation is still above a bound"
            );
        }
        Ok(CompactionOutcome::Compacted)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactionOutcome {
    Compacted,
    NothingToCompact,
    HeadroomRefused,
}

/// The descriptor a config directory holds, when it holds one.
fn descriptors_for(
    candidate: &PausedArtifactCandidate,
) -> Vec<(PathBuf, DeferredCompactionDescriptor)> {
    let mut directories: Vec<PathBuf> = candidate
        .config_paths
        .iter()
        .filter_map(|path| path.parent().map(Path::to_path_buf))
        .collect();
    // The memory config may be named by the state; the generation directory is
    // the fallback for a record whose encoded state did not name it (and for the
    // rootfs, whose config is directly below it).
    directories.push(candidate.artifact_root.clone());
    directories.sort();
    directories.dedup();

    let mut found = Vec::new();
    for directory in directories {
        let path = directory.join(DEFERRED_COMPACTION_FILE);
        if !path.is_file() {
            continue;
        }
        match DeferredCompactionDescriptor::load(&path) {
            Ok(descriptor) => found.push((path, descriptor)),
            Err(error) => warn!(
                sandbox_id = %candidate.sandbox_id,
                path = %path.display(),
                error = %format_args!("{error:#}"),
                "ignoring an unreadable deferred compaction descriptor"
            ),
        }
    }
    found
}

fn descriptor_directory(descriptor_path: &Path) -> Result<&Path> {
    descriptor_path.parent().with_context(|| {
        format!(
            "deferred compaction descriptor has no parent: {}",
            descriptor_path.display()
        )
    })
}

/// Whether the config already names the output and none of the inputs.
fn config_already_swapped(
    config_path: &Path,
    compactable: &[String],
    output_path: &Path,
) -> Result<bool> {
    let bytes = std::fs::read(config_path)
        .with_context(|| format!("read image config {}", config_path.display()))?;
    let config: Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("deserialize image config {}", config_path.display()))?;
    let files: HashSet<&str> = config
        .get("lowers")
        .and_then(Value::as_array)
        .map(|lowers| {
            lowers
                .iter()
                .filter_map(|lower| lower.get("file").and_then(Value::as_str))
                .collect()
        })
        .unwrap_or_default();
    let output = output_path.display().to_string();
    Ok(files.contains(output.as_str())
        && !compactable
            .iter()
            .any(|input| files.contains(input.as_str())))
}

/// Replace the inputs the descriptor names with the compacted layer, preserving
/// every other field of the config file.
///
/// The edit goes through `serde_json::Value` on purpose: a typed round-trip
/// would drop fields this version does not know, and the previous version's
/// resume must still read everything it wrote.
fn swap_config_atomically(
    config_path: &Path,
    compactable: &[String],
    output_path: &Path,
    hook: Option<&SwapHook>,
) -> Result<()> {
    let bytes = std::fs::read(config_path)
        .with_context(|| format!("read image config {}", config_path.display()))?;
    let mut config: Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("deserialize image config {}", config_path.display()))?;
    let lowers = config
        .get_mut("lowers")
        .and_then(Value::as_array_mut)
        .with_context(|| format!("image config {} has no lowers array", config_path.display()))?;
    let compactable: HashSet<&str> = compactable.iter().map(String::as_str).collect();
    let mut replaced = false;
    let mut rewritten = Vec::with_capacity(lowers.len());
    for lower in lowers.drain(..) {
        let file = lower
            .get("file")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !compactable.contains(file) {
            rewritten.push(lower);
            continue;
        }
        if !replaced {
            rewritten.push(serde_json::json!({
                "file": output_path.display().to_string(),
            }));
            replaced = true;
        }
    }
    if !replaced {
        // The crash window between the rename and marking the descriptor
        // `Swapped`: the config already names the output and no longer names
        // the inputs. That is the completed state, so the retry must mark the
        // descriptor instead of failing forever.
        let output = output_path.display().to_string();
        let already_swapped = rewritten
            .iter()
            .any(|lower| lower.get("file").and_then(Value::as_str) == Some(output.as_str()));
        anyhow::ensure!(
            already_swapped,
            "deferred compaction descriptor names layers the image config {} no longer references",
            config_path.display()
        );
        return Ok(());
    }
    config["lowers"] = Value::Array(rewritten);
    let bytes = serde_json::to_vec_pretty(&config).context("serialize swapped image config")?;
    write_file_atomically_with_hook(config_path, &bytes, "swapped image config", hook)
}

/// Remove the inputs of a swapped descriptor that the current config no longer
/// references.
///
/// Only regular files directly inside the descriptor's generation directory are
/// candidates; a path outside it is refused with a warning, because a
/// descriptor can only ever name the layers the pause adopted into that
/// directory and never gets to delete anything else.
async fn unlink_swapped_inputs(
    descriptor_path: &Path,
    descriptor: &DeferredCompactionDescriptor,
) -> Result<u64> {
    let directory = descriptor_directory(descriptor_path)?;
    let config_path = directory.join(&descriptor.config_file);
    let bytes = std::fs::read(&config_path)
        .with_context(|| format!("read image config {}", config_path.display()))?;
    let config: Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("deserialize image config {}", config_path.display()))?;
    let referenced: HashSet<&str> = config
        .get("lowers")
        .and_then(Value::as_array)
        .map(|lowers| {
            lowers
                .iter()
                .filter_map(|lower| lower.get("file").and_then(Value::as_str))
                .collect()
        })
        .unwrap_or_default();

    let mut reclaimed_bytes = 0u64;
    for layer in &descriptor.compactable {
        if referenced.contains(layer.as_str()) {
            continue;
        }
        let layer_path = Path::new(layer);
        if !layer_path.starts_with(directory) {
            warn!(
                kind = descriptor.kind.as_str(),
                layer = %layer_path.display(),
                generation = descriptor.generation.as_str(),
                "refusing to unlink a deferred compaction input outside its generation"
            );
            continue;
        }
        // Only the last link frees bytes; an adopted hardlink of a layer an
        // older generation still owns costs nothing to remove now and shows up
        // in the reclaimed total only when the stale-generation sweep takes the
        // rest. `blocks()` is the allocation actually charged to the node.
        let freed = std::fs::symlink_metadata(layer_path)
            .ok()
            .filter(|metadata| metadata.nlink() == 1)
            .map(|metadata| metadata.blocks().saturating_mul(512))
            .unwrap_or(0);
        match tokio::fs::remove_file(layer_path).await {
            Ok(()) => {
                reclaimed_bytes = reclaimed_bytes.saturating_add(freed);
                debug!(
                    kind = descriptor.kind.as_str(),
                    layer = %layer_path.display(),
                    "unlinked a swapped deferred compaction input"
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "unlink swapped deferred compaction input {}",
                        layer_path.display()
                    )
                });
            }
        }
    }
    Ok(reclaimed_bytes)
}

/// Whether the compacted generation still breaches one of the frozen bounds.
///
/// Only the reclaimable part is known here (the compacted output itself), so the
/// absolute bound can be judged exactly and the relative one conservatively:
/// comparing the output alone against `whole_chain_ceiling` under-reports, never
/// over-reports, which keeps the warning honest without re-measuring the stable
/// prefix on every pass.
fn exceeds_a_bound(bytes: u64, descriptor: &DeferredCompactionDescriptor) -> bool {
    (descriptor.reclaimable_ceiling != 0 && bytes > descriptor.reclaimable_ceiling)
        || (descriptor.whole_chain_ceiling != 0 && bytes > descriptor.whole_chain_ceiling)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use overlaybd::backend::local::LocalFile;
    use overlaybd::index_file::{CommitArgs, LSMTFile};
    use overlaybd::virtual_file::VirtualFile;
    use serde_json::json;

    use super::*;
    use crate::types::SandboxId;

    /// Seal a tiny raw overlaybd commit, the same fixture the chain tests use.
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
        let layer = LSMTFile::create(data, Some(index), vsize, false)
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
            .commit_with_args(CommitArgs::new(output))
            .await
            .expect("commit raw layer");
        commit_path
    }

    fn image_config(layers: &[PathBuf]) -> Value {
        json!({
            "lowers": layers
                .iter()
                .map(|path| json!({ "file": path.display().to_string() }))
                .collect::<Vec<_>>(),
            "upper": {},
            "resultFile": "",
            "download": {},
        })
    }

    /// A generation directory with an image config naming `layers` and a
    /// `Pending` descriptor, exactly what a deferred pause leaves behind.
    fn pending_fixture(
        dir: &Path,
        kind: &str,
        config_file: &str,
        output_name: &str,
        layers: &[PathBuf],
        view_bytes: u64,
        limits: (u64, u64),
    ) -> (PathBuf, DeferredCompactionDescriptor) {
        std::fs::create_dir_all(dir).expect("create generation dir");
        std::fs::write(
            dir.join(config_file),
            serde_json::to_vec_pretty(&image_config(layers)).expect("serialize config"),
        )
        .expect("write config");
        let descriptor = DeferredCompactionDescriptor::new(
            kind,
            config_file,
            output_name,
            layers
                .iter()
                .map(|path| path.display().to_string())
                .collect(),
            view_bytes,
            limits,
            dir.file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default(),
        );
        let path = DeferredCompactionDescriptor::path_in(dir);
        descriptor.write(&path).expect("write descriptor");
        (path, descriptor)
    }

    fn candidate(dir: &Path, config_file: &str) -> PausedArtifactCandidate {
        PausedArtifactCandidate {
            sandbox_id: SandboxId::new(),
            artifact_root: dir.to_path_buf(),
            config_paths: vec![dir.join(config_file)],
        }
    }

    fn layers_in(config_path: &Path) -> Vec<String> {
        let config: Value =
            serde_json::from_slice(&std::fs::read(config_path).expect("read config"))
                .expect("parse config");
        config["lowers"]
            .as_array()
            .expect("lowers array")
            .iter()
            .map(|lower| lower["file"].as_str().expect("lower file").to_string())
            .collect()
    }

    /// R5: with a one-compaction budget two pending descriptors run one per
    /// pass; the second stays queued (and is counted) until the first finishes.
    #[tokio::test]
    async fn two_pending_descriptors_run_one_at_a_time_and_the_second_waits() {
        let temp = tempfile::tempdir().expect("tempdir");
        let inputs = temp.path().join("inputs");
        std::fs::create_dir_all(&inputs).expect("create inputs");
        let first_dir = temp.path().join("gen-a");
        let second_dir = temp.path().join("gen-b");
        let first_layer = seal_raw_test_layer(&inputs, "a", 8192, &[(0, 0xAA)]).await;
        let second_layer = seal_raw_test_layer(&inputs, "b", 8192, &[(0, 0xBB)]).await;
        pending_fixture(
            &first_dir,
            "rootfs",
            "image.json",
            "managed-base.commit",
            std::slice::from_ref(&first_layer),
            8192,
            (0, 0),
        );
        pending_fixture(
            &second_dir,
            "rootfs",
            "image.json",
            "managed-base.commit",
            std::slice::from_ref(&second_layer),
            8192,
            (0, 0),
        );

        let gate = DeferredCompactionGate::new_for_test();
        let worker = DeferredCompactionWorker::new(gate.clone(), 1, u64::MAX);
        let candidates = [
            candidate(&first_dir, "image.json"),
            candidate(&second_dir, "image.json"),
        ];

        let first = worker.run_once(&candidates).await;
        assert_eq!(first.compacted, 1, "{first:?}");
        assert_eq!(first.queued, 1, "{first:?}");
        assert!(first_dir.join("managed-base.commit").exists());
        assert!(
            !second_dir.join("managed-base.commit").exists(),
            "the second descriptor must wait for the concurrency budget"
        );
        assert_eq!(gate.in_flight(), 0);

        let second = worker.run_once(&candidates).await;
        assert_eq!(second.compacted, 1, "{second:?}");
        assert_eq!(second.queued, 0, "{second:?}");
        assert!(second_dir.join("managed-base.commit").exists());
    }

    /// R3 (worker side): the pre-rewrite headroom refusal happens before any
    /// output is written and keeps the descriptor for a later pass.
    #[tokio::test]
    async fn headroom_refusal_writes_nothing_and_keeps_the_descriptor() {
        let temp = tempfile::tempdir().expect("tempdir");
        let generation = temp.path().join("gen");
        let layer = temp.path().join("input.commit");
        std::fs::write(&layer, vec![0xAAu8; 8192]).expect("write input");
        let (descriptor_path, _) = pending_fixture(
            &generation,
            "memory",
            "mem_image.json",
            "mem_compacted.commit",
            std::slice::from_ref(&layer),
            8192,
            (0, 0),
        );

        let gate = DeferredCompactionGate::new_for_test();
        let worker = DeferredCompactionWorker::new(gate.clone(), 1, u64::MAX)
            .with_free_bytes_probe(|_| Some(0));
        let report = worker
            .run_once(&[candidate(&generation, "mem_image.json")])
            .await;

        assert_eq!(report.headroom_refused, 1, "{report:?}");
        assert_eq!(report.compacted, 0, "{report:?}");
        assert!(!generation.join("mem_compacted.commit").exists());
        assert!(
            descriptor_path.exists(),
            "a refused rewrite must keep its durable work order"
        );
        assert_eq!(
            DeferredCompactionDescriptor::load(&descriptor_path)
                .expect("load descriptor")
                .state,
            DeferredCompactionState::Pending
        );
        assert!(
            layers_in(&generation.join("mem_image.json")).len() == 1,
            "the config must be untouched"
        );
    }

    /// R7 (i): the process died before the swap. The descriptor is durable, so a
    /// fresh worker (new in-memory gate) finishes the work without a new pause.
    #[tokio::test]
    async fn a_crash_before_the_swap_is_finished_from_the_durable_descriptor() {
        let temp = tempfile::tempdir().expect("tempdir");
        let generation = temp.path().join("gen");
        std::fs::create_dir_all(generation.join("inherited-layers/0000"))
            .expect("create adopted layer dir");
        let layer = seal_raw_test_layer(
            &generation.join("inherited-layers/0000"),
            "input",
            4096,
            &[(0, 0x11)],
        )
        .await;
        let (descriptor_path, _) = pending_fixture(
            &generation,
            "memory",
            "mem_image.json",
            "mem_compacted.commit",
            std::slice::from_ref(&layer),
            4096,
            (0, 0),
        );

        // A fresh process: the gate has no memory of the queue, only the
        // descriptor on disk.
        let gate = DeferredCompactionGate::new_for_test();
        let worker = DeferredCompactionWorker::new(gate, 1, u64::MAX);
        let report = worker
            .run_once(&[candidate(&generation, "mem_image.json")])
            .await;

        assert_eq!(report.compacted, 1, "{report:?}");
        assert_eq!(
            DeferredCompactionDescriptor::load(&descriptor_path)
                .expect("load descriptor")
                .state,
            DeferredCompactionState::Swapped
        );
        let lowers = layers_in(&generation.join("mem_image.json"));
        assert_eq!(lowers.len(), 1, "the inputs must fold into one generation");
        assert!(Path::new(&lowers[0]).ends_with("mem_compacted.commit"));
    }

    /// R7 (ii): the process died after the swap and before the unlink. The next
    /// pass unlinks only the inputs the current config no longer names.
    #[tokio::test]
    async fn a_crash_after_the_swap_unlinks_in_the_next_pass() {
        let temp = tempfile::tempdir().expect("tempdir");
        let generation = temp.path().join("gen");
        let input = generation
            .join("inherited-layers")
            .join("0000")
            .join("old.commit");
        std::fs::create_dir_all(input.parent().unwrap()).expect("create input dir");
        std::fs::write(&input, vec![0x22u8; 4096]).expect("write input");
        let output = generation.join("mem_compacted.commit");
        std::fs::create_dir_all(&generation).expect("create generation");
        std::fs::write(&output, vec![0x33u8; 4096]).expect("write output");
        // The swapped state: the config names only the output, the input is still
        // on disk and the descriptor says `Swapped`.
        std::fs::write(
            generation.join("mem_image.json"),
            serde_json::to_vec_pretty(&image_config(std::slice::from_ref(&output)))
                .expect("serialize config"),
        )
        .expect("write config");
        let mut descriptor = DeferredCompactionDescriptor::new(
            "memory",
            "mem_image.json",
            "mem_compacted.commit",
            vec![input.display().to_string()],
            4096,
            (0, 0),
            "gen",
        );
        descriptor.state = DeferredCompactionState::Swapped;
        let descriptor_path = DeferredCompactionDescriptor::path_in(&generation);
        descriptor
            .write(&descriptor_path)
            .expect("write descriptor");

        let gate = DeferredCompactionGate::new_for_test();
        let worker = DeferredCompactionWorker::new(gate, 1, u64::MAX);
        let report = worker
            .run_once(&[candidate(&generation, "mem_image.json")])
            .await;

        assert_eq!(report.unlinked, 1, "{report:?}");
        assert!(!input.exists(), "the swapped-in input must be unlinked");
        assert!(output.exists(), "the swapped-in output must stay");
        assert!(!descriptor_path.exists(), "the finished work order must go");
    }

    /// R7 (iii): a generation no `Paused` record names is not this pass's to
    /// touch; the quarantine/trash sweep keeps owning it.
    #[tokio::test]
    async fn a_generation_no_record_names_is_never_touched() {
        let temp = tempfile::tempdir().expect("tempdir");
        let generation = temp.path().join("orphan");
        let layer = temp.path().join("input.commit");
        std::fs::write(&layer, vec![0x44u8; 4096]).expect("write input");
        let (descriptor_path, _) = pending_fixture(
            &generation,
            "memory",
            "mem_image.json",
            "mem_compacted.commit",
            std::slice::from_ref(&layer),
            4096,
            (0, 0),
        );

        let gate = DeferredCompactionGate::new_for_test();
        let worker = DeferredCompactionWorker::new(gate, 1, u64::MAX);
        let report = worker.run_once(&[]).await;

        assert_eq!(report, DeferredCompactionReport::default());
        assert!(descriptor_path.exists());
        assert!(layer.exists());
        assert!(!generation.join("mem_compacted.commit").exists());
    }

    /// R1/R2 (worker side): a paused chain that crossed its byte bound comes
    /// back under it, and the config names the compacted generation, not the
    /// inputs.
    #[tokio::test]
    async fn the_worker_brings_a_paused_chain_under_its_ceiling() {
        let temp = tempfile::tempdir().expect("tempdir");
        let generation = temp.path().join("gen");
        std::fs::create_dir_all(generation.join("inherited-layers/0000"))
            .expect("create adopted layer dir");
        let mut layers = Vec::new();
        for (index, byte) in [0xAAu8, 0xBB, 0xCC].into_iter().enumerate() {
            let layer = seal_raw_test_layer(
                &generation.join("inherited-layers/0000"),
                &format!("gen-{index}"),
                8192,
                &[(0, byte), (4096, byte)],
            )
            .await;
            layers.push(layer);
        }
        let appended =
            seal_raw_test_layer(&generation, "appended", 8192, &[(0, 0xDD), (4096, 0xDD)]).await;
        layers.push(appended);
        let chain_bytes = compactable_allocated_bytes(
            &layers
                .iter()
                .map(|path| overlaybd::config::LayerConfig {
                    file: path.display().to_string(),
                    ..Default::default()
                })
                .collect::<Vec<_>>(),
        );
        let ceiling = chain_bytes * 3 / 4;
        assert!(ceiling > 0 && ceiling < chain_bytes);
        pending_fixture(
            &generation,
            "memory",
            "mem_image.json",
            "mem_compacted.commit",
            &layers,
            0,
            (ceiling, 0),
        );

        let gate = DeferredCompactionGate::new_for_test();
        let worker = DeferredCompactionWorker::new(gate, 1, u64::MAX);
        let report = worker
            .run_once(&[candidate(&generation, "mem_image.json")])
            .await;
        assert_eq!(report.compacted, 1, "{report:?}");

        let compacted = generation.join("mem_compacted.commit");
        assert!(compacted.exists());
        let after = compactable_allocated_bytes(&[overlaybd::config::LayerConfig {
            file: compacted.display().to_string(),
            ..Default::default()
        }]);
        assert!(
            after <= ceiling,
            "the compacted generation must come back under the ceiling: {after} > {ceiling}"
        );
        assert_eq!(
            layers_in(&generation.join("mem_image.json")),
            vec![compacted.display().to_string()],
            "the config must name the compacted generation"
        );
    }

    /// The crash window between the rename and marking the descriptor must not
    /// rewrite the output from stale inputs: a retry marks the descriptor and
    /// leaves the newer generation alone.
    #[tokio::test]
    async fn an_already_swapped_config_is_not_rewritten_from_stale_inputs() {
        let temp = tempfile::tempdir().expect("tempdir");
        let generation = temp.path().join("gen");
        std::fs::create_dir_all(generation.join("inherited-layers/0000"))
            .expect("create adopted layer dir");
        let input = seal_raw_test_layer(
            &generation.join("inherited-layers/0000"),
            "input",
            4096,
            &[(0, 0x11)],
        )
        .await;
        let output = generation.join("mem_compacted.commit");
        std::fs::write(&output, vec![0x99u8; 4096]).expect("write newer output");
        let newer = std::fs::read(&output).expect("read newer output");
        // The config already names the output; the input is still on disk.
        std::fs::write(
            generation.join("mem_image.json"),
            serde_json::to_vec_pretty(&image_config(std::slice::from_ref(&output)))
                .expect("serialize config"),
        )
        .expect("write config");
        let descriptor = DeferredCompactionDescriptor::new(
            "memory",
            "mem_image.json",
            "mem_compacted.commit",
            vec![input.display().to_string()],
            4096,
            (0, 0),
            "gen",
        );
        let descriptor_path = DeferredCompactionDescriptor::path_in(&generation);
        descriptor
            .write(&descriptor_path)
            .expect("write descriptor");

        let gate = DeferredCompactionGate::new_for_test();
        let worker = DeferredCompactionWorker::new(gate, 1, u64::MAX);
        let report = worker
            .run_once(&[candidate(&generation, "mem_image.json")])
            .await;

        assert_eq!(report.compacted, 1, "{report:?}");
        assert_eq!(
            std::fs::read(&output).expect("read output"),
            newer,
            "the retry must not overwrite the newer compacted generation"
        );
        assert_eq!(
            DeferredCompactionDescriptor::load(&descriptor_path)
                .expect("load descriptor")
                .state,
            DeferredCompactionState::Swapped
        );
    }

    /// R6: at the swap point the target still holds the complete old config and
    /// the temp file holds the complete new one. A reader there never sees a
    /// mix, and the old layers stay reachable until a later pass unlinks them.
    #[tokio::test]
    async fn the_swap_point_never_exposes_a_mixed_config() {
        let temp = tempfile::tempdir().expect("tempdir");
        let generation = temp.path().join("gen");
        std::fs::create_dir_all(generation.join("inherited-layers/0000"))
            .expect("create adopted layer dir");
        std::fs::create_dir_all(generation.join("inherited-layers/0001"))
            .expect("create adopted layer dir");
        let first = seal_raw_test_layer(
            &generation.join("inherited-layers/0000"),
            "first",
            4096,
            &[(0, 0x55)],
        )
        .await;
        let second = seal_raw_test_layer(
            &generation.join("inherited-layers/0001"),
            "second",
            4096,
            &[(0, 0x66)],
        )
        .await;
        pending_fixture(
            &generation,
            "memory",
            "mem_image.json",
            "mem_compacted.commit",
            &[first.clone(), second.clone()],
            8192,
            (0, 0),
        );

        type ObservedSwaps = Arc<Mutex<Vec<(Vec<String>, Vec<String>)>>>;
        let observed: ObservedSwaps = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&observed);
        let gate = DeferredCompactionGate::new_for_test();
        let worker = DeferredCompactionWorker::new(gate, 1, u64::MAX).with_swap_hook(
            move |target: &Path, temp_file: &Path| {
                // No reader runs between these two reads that the rename could
                // interleave: both files are complete on their own.
                seen.lock()
                    .expect("observed lock")
                    .push((layers_in(target), layers_in(temp_file)));
            },
        );
        let report = worker
            .run_once(&[candidate(&generation, "mem_image.json")])
            .await;
        assert_eq!(report.compacted, 1, "{report:?}");

        let observed = observed.lock().expect("observed lock");
        assert_eq!(observed.len(), 1, "the swap point must be observed once");
        let (old, new) = &observed[0];
        assert_eq!(
            old.len(),
            2,
            "the target must still be the whole old config"
        );
        assert!(old
            .iter()
            .all(|lower| !lower.ends_with("mem_compacted.commit")));
        assert_eq!(new.len(), 1, "the temp file must be the whole new config");
        assert!(new[0].ends_with("mem_compacted.commit"));
        // The unlink only runs in a later pass, while the record is still
        // `Paused`; here the inputs are still reachable.
        assert!(first.exists() && second.exists());
    }

    #[test]
    fn the_log_name_is_the_one_the_card_requires() {
        assert_eq!(
            DEFERRED_COMPACTION_LOG,
            "paused sandbox chain compacted outside the pause"
        );
        assert_eq!(
            DEFERRED_RECLAIM_LOG,
            "paused sandbox chain removed the copies its compaction replaced"
        );
    }

    /// Allocated bytes of a set of layers, deduplicated by inode exactly like
    /// the node's disk accounting.
    fn allocated_bytes(layers: &[PathBuf]) -> u64 {
        compactable_allocated_bytes(
            &layers
                .iter()
                .map(|path| overlaybd::config::LayerConfig {
                    file: path.display().to_string(),
                    ..Default::default()
                })
                .collect::<Vec<_>>(),
        )
    }

    /// The layers an image config names, as paths.
    fn lowers_of(config_path: &Path) -> Vec<PathBuf> {
        layers_in(config_path)
            .iter()
            .map(PathBuf::from)
            .collect::<Vec<_>>()
    }

    /// The card's disk-bytes acceptance, on the worker's side: two consecutive
    /// pauses of the same sandbox, the second one crossing the bound, and the
    /// maintenance pass afterwards. The generation the sandbox resumes from
    /// holds the shared base plus the bytes the pauses really wrote — never the
    /// compacted copy *and* the delta it replaced, which is what the 21/11
    /// measurement found (146 GiB in one generation where about 79 were
    /// needed).
    ///
    /// The fixture has the shape the pause leaves: each pause's delta at its own
    /// path inside its generation, the inherited layer adopted as a hard link,
    /// and a work order naming exactly the layers that generation holds.
    #[tokio::test]
    async fn two_pauses_leave_the_base_plus_the_real_dirty_bytes() {
        let temp = tempfile::tempdir().expect("tempdir");
        let staging = temp.path().join("staging");
        let image_cache = temp.path().join("image-cache");
        let sandbox_root = temp.path().join("artifacts").join("sandbox");
        let first = sandbox_root.join("gen-1").join("rootfs");
        let second = sandbox_root.join("gen-2").join("rootfs");
        for dir in [&staging, &image_cache, &first, &second] {
            std::fs::create_dir_all(dir).expect("create fixture dir");
        }

        const DISK_BYTES: u64 = 1024 * 1024;
        const PART_BYTES: u64 = 256 * 1024;
        // A base every sandbox of the image shares: it lives in the image cache,
        // outside the paused artifacts, so the node pays it once.
        let base = image_cache.join("sha256-base.overlaybd.commit");
        std::fs::write(&base, vec![0x11u8; 512 * 1024]).expect("write base layer");
        // Two pauses writing disjoint regions, so a perfect compaction holds the
        // base plus both deltas and nothing else.
        let dirty = |first_page: u64, byte: u8| -> Vec<(u64, u8)> {
            (0..PART_BYTES / 4096)
                .map(|index| ((first_page + index) * 4096, byte))
                .collect()
        };

        // Pause one adopted the delta it wrote, exactly like the no-breach path.
        let sealed = seal_raw_test_layer(&staging, "first", DISK_BYTES, &dirty(0, 0xAA)).await;
        let first_delta = first.join("snapshot.commit");
        std::fs::rename(&sealed, &first_delta).expect("move the first delta");
        std::fs::write(
            first.join("image.json"),
            serde_json::to_vec_pretty(&image_config(&[base.clone(), first_delta.clone()]))
                .expect("serialize first config"),
        )
        .expect("write first config");

        // Pause two: the delta it wrote stays at its own path, the inherited one
        // is adopted as a hard link beside it, and the order names both.
        let sealed = seal_raw_test_layer(
            &staging,
            "second",
            DISK_BYTES,
            &dirty(PART_BYTES / 4096, 0xBB),
        )
        .await;
        let second_delta = second.join("snapshot.commit");
        std::fs::rename(&sealed, &second_delta).expect("move the second delta");
        let inherited = second
            .join("inherited-layers")
            .join("0000")
            .join("snapshot.commit");
        std::fs::create_dir_all(inherited.parent().expect("inherited parent"))
            .expect("create inherited layer dir");
        std::fs::hard_link(&first_delta, &inherited).expect("adopt the inherited layer by link");
        std::fs::write(
            second.join("image.json"),
            serde_json::to_vec_pretty(&image_config(&[
                base.clone(),
                inherited.clone(),
                second_delta.clone(),
            ]))
            .expect("serialize second config"),
        )
        .expect("write second config");
        let descriptor = DeferredCompactionDescriptor::new(
            "rootfs",
            "image.json",
            "managed-base.commit",
            vec![
                inherited.display().to_string(),
                second_delta.display().to_string(),
            ],
            DISK_BYTES,
            (0, 0),
            second
                .parent()
                .and_then(Path::file_name)
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default(),
        );
        descriptor
            .write(&DeferredCompactionDescriptor::path_in(&second))
            .expect("write descriptor");

        // The model, measured while the deltas are still on disk.
        let base_bytes = allocated_bytes(std::slice::from_ref(&base));
        let dirty_bytes = allocated_bytes(&[first_delta.clone(), second_delta.clone()]);
        let model = base_bytes + dirty_bytes;

        let gate = DeferredCompactionGate::new_for_test();
        let worker = DeferredCompactionWorker::new(gate.clone(), 1, u64::MAX)
            .with_free_bytes_probe(|_| Some(u64::MAX));
        let candidate = candidate(&second, "image.json");
        // Pass one compacts and swaps; pass two unlinks the copies it replaced.
        worker.run_once(std::slice::from_ref(&candidate)).await;
        worker.run_once(std::slice::from_ref(&candidate)).await;

        assert!(
            second.join("managed-base.commit").exists(),
            "the worker must compact the chain"
        );
        assert!(
            !second_delta.exists(),
            "the delta the pause wrote must be freed once its copy replaced it"
        );
        assert!(
            !inherited.exists(),
            "the adopted hard link the merge folded must be unlinked"
        );
        assert!(
            !DeferredCompactionDescriptor::path_in(&second).exists(),
            "the finished order must be removed"
        );
        assert!(
            first_delta.exists(),
            "a layer outside the generation belongs to the stale-generation sweep"
        );

        let reachable = allocated_bytes(&lowers_of(&second.join("image.json")));
        assert!(
            reachable <= model + model / 20,
            "the sandbox must hold the base plus the real dirty bytes ({model}) within 5%, \
             not the compacted copy and the delta it replaced: {reachable}"
        );

        let compacted = allocated_bytes(&[second.join("managed-base.commit")]);
        assert!(
            compacted <= dirty_bytes + dirty_bytes / 20,
            "the generation must hold the delta over the base, not a second copy of the image: \
             {compacted} vs {dirty_bytes}"
        );
    }
}
