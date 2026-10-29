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
use overlaybd::index_file::compact_to;
use overlaybd::virtual_file::VirtualFile;
use tracing::{debug, warn};

use super::process_vm_reader::ProcessVmReader;
use super::sandbox::managed_snapshot_base;
use crate::cfg::ConfigManager;
use crate::overlaybd::{compact_layers, create_commit_args, OverlaybdCompactOutput};
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
/// `orchestrator.chain_bytes_ceiling` because its immutable stable prefix alone
/// already exceeds it. The pause is rejected instead of being allowed to grow
/// the chain, and the sandbox keeps its runtime state.
pub(crate) const CHAIN_BYTES_CEILING_EXCEEDED: &str = "CHAIN_BYTES_CEILING_EXCEEDED";

/// Ceiling rejection for one sandbox's paused artifact chain.
///
/// Kept as a concrete error so the pause path can classify it as recoverable
/// through `context` layers, instead of matching on message text.
#[derive(Debug)]
pub(crate) struct ChainBytesCeilingExceeded {
    pub observed_bytes: u64,
    pub ceiling_bytes: u64,
}

impl std::fmt::Display for ChainBytesCeilingExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{CHAIN_BYTES_CEILING_EXCEEDED}: paused sandbox chain needs {} bytes with a stable \
             prefix that already exceeds the {} byte ceiling",
            self.observed_bytes, self.ceiling_bytes
        )
    }
}

impl std::error::Error for ChainBytesCeilingExceeded {}

fn chain_ceiling_exceeded(observed_bytes: u64, ceiling_bytes: u64) -> anyhow::Error {
    anyhow::Error::new(ChainBytesCeilingExceeded {
        observed_bytes,
        ceiling_bytes,
    })
}

/// Byte ceiling observability for the paused artifact chain.
///
/// Labels stay bounded: `kind` is one of `rootfs` / `memory`, never a sandbox
/// id, so a node with thousands of sandboxes cannot explode cardinality.
mod chain_metrics {
    pub(super) const CHAIN_BYTES: &str = "agentenv_paused_sandbox_chain_bytes";
    pub(super) const COMPACTIONS: &str = "agentenv_paused_sandbox_chain_compactions_total";
    pub(super) const CEILING_EXCEEDED: &str =
        "agentenv_paused_sandbox_chain_ceiling_exceeded_total";

    pub(super) fn record_chain_bytes(kind: &'static str, bytes: u64, ceiling: u64) {
        metrics::gauge!(CHAIN_BYTES, "kind" => kind).set(bytes as f64);
        metrics::gauge!(CHAIN_BYTES, "kind" => "ceiling").set(ceiling as f64);
    }

    pub(super) fn record_compaction(kind: &'static str) {
        metrics::counter!(COMPACTIONS, "kind" => kind).increment(1);
    }

    pub(super) fn record_ceiling_exceeded(kind: &'static str) {
        metrics::counter!(CEILING_EXCEEDED, "kind" => kind).increment(1);
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

    fn finish_staging(self, staged_snapshot: Result<PathBuf>) -> Result<PathBuf> {
        match self {
            Self::ReadOnly => staged_snapshot,
            Self::Restacked(_) => staged_snapshot.map_err(into_terminal_snapshot_failure),
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

fn canonicalized_runtime_owned_roots() -> &'static [PathBuf] {
    static RUNTIME_OWNED_ROOTS: std::sync::OnceLock<Vec<PathBuf>> = std::sync::OnceLock::new();
    RUNTIME_OWNED_ROOTS
        .get_or_init(|| {
            let config = ConfigManager::global_config();
            [
                managed_snapshot_base(),
                config.orchestrator.persisted_sandbox_store_path.clone(),
            ]
            .into_iter()
            .map(|root| fs::canonicalize(&root).unwrap_or(root))
            .collect::<Vec<_>>()
        })
        .as_slice()
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
    let Some(first_runtime_owned_index) = lowers.iter().position(|lower| {
        let lower_path =
            fs::canonicalize(&lower.file).unwrap_or_else(|_| PathBuf::from(&lower.file));
        runtime_owned_roots
            .iter()
            .any(|root| lower_path.starts_with(root))
    }) else {
        return (lowers, Vec::new());
    };

    let runtime_owned_suffix = lowers.split_off(first_runtime_owned_index);

    debug_assert!(lowers.iter().all(|lower| {
        let lower_path =
            fs::canonicalize(&lower.file).unwrap_or_else(|_| PathBuf::from(&lower.file));
        runtime_owned_roots
            .iter()
            .all(|root| !lower_path.starts_with(root))
    }));

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

pub(super) async fn stage_overlaybd_snapshot_from_live_runtime(
    live_runtime_image_config_path: &Path,
    output_dir: &Path,
    snapshot_layer_path: Option<&Path>,
    chain_bytes_ceiling: u64,
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

    let rewritten_lowers = rewrite_lowers_with_owned_runtime_suffix(
        image_config.lowers,
        output_dir,
        appended_layer,
        MANAGED_BASE_LAYER_FILE,
        // Sealed rootfs layers always stay raw; compression happens once at
        // publish time under `[snapshot.publish_compression]`.
        OverlaybdCompactOutput::Raw,
        chain_bytes_ceiling,
    )
    .await?;
    image_config.lowers = rewritten_lowers;
    image_config.upper = Default::default();
    image_config.result_file = "./result.txt".to_string();

    let output_path = output_dir.join("image.json");
    let bytes = serde_json::to_vec_pretty(&image_config)
        .context("serialize overlaybd rootfs image config with inherited runtime layers")?;
    write_bytes_atomically(&output_path, &bytes, "overlaybd rootfs image config")?;
    Ok(output_path)
}

async fn rewrite_lowers_with_owned_runtime_suffix(
    existing_lowers: Vec<LayerConfig>,
    output_dir: &Path,
    appended_layer: Option<LayerConfig>,
    compaction_output_name: &'static str,
    compaction_output: OverlaybdCompactOutput,
    chain_bytes_ceiling: u64,
) -> Result<Vec<LayerConfig>> {
    rewrite_lowers_with_runtime_roots(
        existing_lowers,
        output_dir,
        appended_layer,
        compaction_output_name,
        canonicalized_runtime_owned_roots(),
        compaction_output,
        chain_bytes_ceiling,
    )
    .await
}

async fn rewrite_lowers_with_runtime_roots(
    existing_lowers: Vec<LayerConfig>,
    output_dir: &Path,
    appended_layer: Option<LayerConfig>,
    compaction_output_name: &'static str,
    runtime_owned_roots: &[PathBuf],
    compaction_output: OverlaybdCompactOutput,
    chain_bytes_ceiling: u64,
) -> Result<Vec<LayerConfig>> {
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
    // The byte ceiling is the stricter of the two triggers: a chain can stay
    // inside the layer-count budget for many pauses and still fill the node's
    // disk, because each generation rewrites far more bytes than it shares.
    // Projected bytes are the stable prefix plus the runtime suffix plus the
    // layer this pause appends (hardlinks deduplicated). A ceiling of 0 keeps
    // the pre-ceiling behavior: no byte accounting, no compaction on bytes.
    let projected_chain_bytes = if chain_bytes_ceiling == 0 {
        0
    } else {
        let stable_prefix: Vec<&Path> = lowers.iter().map(|lower| Path::new(&lower.file)).collect();
        let runtime_suffix: Vec<&Path> = runtime_owned_lowers
            .iter()
            .map(|lower| Path::new(&lower.file))
            .chain(appended_layer.iter().map(|layer| Path::new(&layer.file)))
            .collect();
        chain_allocated_bytes(stable_prefix).saturating_add(chain_allocated_bytes(runtime_suffix))
    };
    let over_ceiling = chain_bytes_ceiling != 0 && projected_chain_bytes > chain_bytes_ceiling;
    if compactable_count > max_runtime_owned_layers || over_ceiling {
        let mut runtime_suffix = runtime_owned_lowers;
        if let Some(layer) = appended_layer {
            runtime_suffix.push(layer);
        }
        if runtime_suffix.iter().any(|lower| lower.file.is_empty()) {
            anyhow::bail!("cannot compact runtime-owned overlaybd suffix with remote lowers");
        }
        let compacted_path = compact_layers(
            &runtime_suffix,
            &output_dir.join(compaction_output_name),
            compaction_output,
        )
        .await?;
        if compacted_path.is_some() {
            chain_metrics::record_compaction(compaction_output_name);
        }
        if let Some(compacted_path) = compacted_path.as_ref() {
            lowers.push(local_layer_config(compacted_path));
        }
        if chain_bytes_ceiling != 0 {
            // The stable prefix is immutable, so compaction cannot reclaim it.
            // Re-measure the result and refuse the pause by name when even the
            // compacted chain is still over the ceiling.
            let chain_bytes =
                chain_allocated_bytes(lowers.iter().map(|lower| Path::new(&lower.file)));
            if chain_bytes > chain_bytes_ceiling {
                // The pause is rejected, so this generation must not survive:
                // leave the output directory exactly as the caller will clean
                // it, without the compacted layer the failed rewrite produced.
                if let Some(compacted_path) = compacted_path {
                    if let Err(error) = tokio::fs::remove_file(&compacted_path).await {
                        warn!(
                            path = %compacted_path.display(),
                            error = %error,
                            "failed to remove compacted layer after a ceiling rejection"
                        );
                    }
                }
                // Classified by the caller: the rootfs restack already mutated
                // the live runtime and stays terminal through `finish_staging`,
                // while the memory-config path is still recoverable.
                chain_metrics::record_chain_bytes(
                    compaction_output_name,
                    chain_bytes,
                    chain_bytes_ceiling,
                );
                chain_metrics::record_ceiling_exceeded(compaction_output_name);
                return Err(chain_ceiling_exceeded(chain_bytes, chain_bytes_ceiling));
            }
        }
        return Ok(lowers);
    }

    // Otherwise, adopt the runtime-owned suffix into the snapshot artifact dir.
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
    lowers.extend(runtime_owned_lowers);

    if let Some(layer) = appended_layer {
        lowers.push(layer);
    }
    if chain_bytes_ceiling != 0 {
        let chain_bytes = chain_allocated_bytes(lowers.iter().map(|lower| Path::new(&lower.file)));
        chain_metrics::record_chain_bytes(compaction_output_name, chain_bytes, chain_bytes_ceiling);
    }

    Ok(lowers)
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
    chain_bytes_ceiling: u64,
) -> Result<ImageConfig> {
    let inherited_image_config =
        load_existing_image_config(resume_mem_image_config_path, "memory snapshot")?;
    let new_layer = local_layer_config(new_layer_path);
    let lowers = rewrite_lowers_with_owned_runtime_suffix(
        inherited_image_config.lowers,
        output_dir,
        Some(new_layer),
        "mem_compacted.commit",
        memory_output,
        chain_bytes_ceiling,
    )
    .await?;

    Ok(ImageConfig {
        repo_blob_url: inherited_image_config.repo_blob_url,
        lowers,
        ..Default::default()
    })
}

/// Stage a persisted snapshot config from the current live overlaybd runtime.
///
/// Writable runtimes are first restacked in-place so the sealed old upper
/// becomes the newest lower. Read-only runtimes skip the restack phase and
/// only stage the persisted snapshot image config. The sealed layer always
/// stays raw; compression, when enabled, happens once at publish time.
pub(super) async fn restack_snapshot_overlaybd_device(
    ublk_device: &UblkDevice,
    read_only: bool,
    live_runtime_image_config_path: &Path,
    output_dir: &Path,
    snapshot_layer_file_name: &str,
    kind: &'static str,
    chain_bytes_ceiling: u64,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create overlaybd snapshot dir {}", output_dir.display()))?;

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
        chain_bytes_ceiling,
    )
    .await;

    live_snapshot.finish_staging(staged_snapshot)
}

pub(super) async fn restack_snapshot_overlaybd_rootfs(
    ublk_device: &UblkDevice,
    read_only: bool,
    live_runtime_image_config_path: &Path,
    snapshot_root: &Path,
    chain_bytes_ceiling: u64,
) -> Result<PathBuf> {
    restack_snapshot_overlaybd_device(
        ublk_device,
        read_only,
        live_runtime_image_config_path,
        &snapshot_root.join("rootfs"),
        "snapshot.commit",
        "rootfs",
        chain_bytes_ceiling,
    )
    .await
}

async fn publish_memory_overlaybd_layer(
    src_layers: &[Arc<dyn VirtualFile>],
    mappings: &[SegmentMapping],
    virtual_size: u64,
    output_path: &Path,
    mode: OverlaybdCompactOutput,
    concurrency: usize,
) -> Result<()> {
    let lower_tmp = output_path.with_extension("commit.tmp");
    let build_result = async {
        let output_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&lower_tmp)
                .with_context(|| format!("create temp lower failed: {}", lower_tmp.display()))?,
        );
        let commit_args = create_commit_args(output_file, mode, concurrency).await?;
        compact_to(src_layers, mappings, virtual_size, commit_args)
            .await
            .context("compact memory layer")?;
        tokio::fs::rename(&lower_tmp, output_path)
            .await
            .with_context(|| {
                format!(
                    "move sealed memory lower into place failed: {}",
                    output_path.display()
                )
            })?;
        Ok(())
    }
    .await;

    if build_result.is_err() {
        let _ = tokio::fs::remove_file(&lower_tmp).await;
    }
    build_result
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
) -> Result<(PathBuf, u64)> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create mem overlaybd dir: {}", output_dir.display()))?;

    let data_path = output_dir.join("overlaybd.commit");
    let (mappings, memory_size) = dirty_ranges_to_segment_mappings(dirty_ranges)?;
    let source_file: Arc<dyn VirtualFile> = Arc::new(ProcessVmReader::new(firecracker_pid));
    let src_layers = vec![source_file];
    publish_memory_overlaybd_layer(
        &src_layers,
        &mappings,
        memory_size,
        &data_path,
        mode,
        DIRECT_MEMORY_SNAPSHOT_COMPACTION_CONCURRENCY,
    )
    .await
    .context("compact dirty memory ranges as overlaybd layer")?;

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
        let config = ConfigManager::global_config();
        let snapshot_store_root = config
            .backend
            .posix_fs
            .as_ref()
            .map(|posix_fs| posix_fs.snapshot_store.clone())
            .unwrap_or_else(|| std::env::temp_dir().join("test_snapshot_store"));
        let persisted_sandbox_store_root = config.orchestrator.persisted_sandbox_store_path.clone();
        let snapshot_store_lower = snapshot_store_root
            .join("managed-layers")
            .join("sha256_base.overlaybd.commit");
        let persisted_store_lower = persisted_sandbox_store_root
            .join("artifacts")
            .join("sandbox-parent")
            .join("1700000000000")
            .join("rootfs")
            .join("snapshot.commit");
        let managed_base_lower = managed_snapshot_base().join("sandbox/1700000000001/mem.commit");
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

        let (preserved, managed) =
            split_runtime_suffix(lowers, canonicalized_runtime_owned_roots());

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
        let stable_lower = temp.path().join("image-cache").join("base.overlaybd.commit");
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
            0,
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

    /// A stable prefix that cannot be reclaimed must fail the rewrite by name.
    #[tokio::test]
    async fn rewrite_stable_prefix_above_ceiling_fails_named_and_leaves_no_output() {
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
            1,
        )
        .await
        .expect_err("a chain above the ceiling must fail");

        assert!(
            err.to_string().contains(CHAIN_BYTES_CEILING_EXCEEDED),
            "error must carry the named code: {err:#}"
        );
        assert!(
            err.downcast_ref::<ChainBytesCeilingExceeded>().is_some(),
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
            0,
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
            ceiling,
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
                0,
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
            0,
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
            0,
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
        let image_config = build_mem_snapshot_image_config(
            Some(&parent_config_path),
            &new_layer,
            temp.path(),
            OverlaybdCompactOutput::Raw,
            0,
        )
        .await
        .expect("build memory image config");

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
        publish_memory_overlaybd_layer(
            &[source],
            &one_page_mapping(),
            OVERLAYBD_ALIGNMENT,
            &output,
            OverlaybdCompactOutput::Raw,
            1,
        )
        .await
        .expect_err("copy failure should be returned");

        assert!(!lower_tmp.exists());
        assert_eq!(
            tokio::fs::read(&output).await.expect("read final"),
            existing
        );
    }
}
