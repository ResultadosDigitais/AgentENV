//! Reclaiming paused-sandbox artifact directories that no sandbox still owns.
//!
//! A paused sandbox stores its Firecracker artifacts under
//! `<persisted_sandbox_store_path>/artifacts/{sandbox_id}/{generation}/`. Every
//! path that removes that directory requires the runtime to still know the
//! sandbox: the explicit delete removes it, and `load_all` removes it for a
//! record it discards at boot. Two cases leave the directory behind for good:
//!
//! - the delete answered `404` because the runtime had already forgotten the
//!   sandbox, and the caller treated the `404` as a completed destroy;
//! - the record disappeared (an invalid record discarded by `load_all`, or a
//!   crash between `allocate_artifact_root` and `persist_paused`) and the
//!   directory stayed.
//!
//! [`sweep`] is the periodic pass that reclaims them. It lives in `maintenance`
//! rather than next to the persister because it needs the cluster's sandbox
//! list, and `orchestrator` may not import `maintenance`.
//!
//! # What keeps a directory
//!
//! A directory is reclaimed only when *every* keep condition is false:
//!
//! - no **local** sandbox has that id, and no **restore** is open for it;
//! - the id is not in the **cluster** list, which covers a sandbox paused on
//!   another node;
//! - the entry is a directory that is not a symlink; the pass never follows one;
//! - the name is a **canonical** UUID, so the entry is a sandbox generation at
//!   all, and the path is directly under the artifacts root;
//! - the directory's mtime is older than [`ARTIFACT_GRACE`], which is what
//!   proves a pause that allocated the root is no longer writing to it.
//!
//! # Fail closed
//!
//! The cluster list is the only view of every node. When it is unconfigured,
//! when a node in the gateway's fan-out failed, or when a page did not arrive,
//! the pass cannot prove any directory orphaned, so the whole pass degrades to a
//! report and neither moves nor deletes anything. This is the same rule, and the
//! same gateway reader, as the managed-layer collector.
//!
//! # Two phases
//!
//! A reclaimed directory is first *moved* to `artifacts/.trash/{epoch}/` and
//! deleted only by a later pass, once the epoch in that path is older than
//! [`TRASH_GRACE`]. The window makes a mistaken verdict recoverable: a directory
//! can be renamed back to `artifacts/{sandbox_id}` while its paused record still
//! resolves. Bytes are counted only on the deletion, because a rename frees none.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::Context;
use async_trait::async_trait;
use tokio::fs;
use tracing::{debug, info, warn};

use super::ClusterSandboxIds;
use crate::types::SandboxId;

/// The directory, inside the artifacts root, that holds a moved generation
/// until its second grace elapses.
pub const TRASH_DIR: &str = ".trash";

/// How often the runtime runs a pass.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(3600);

/// How old an orphan artifact directory must be before the pass moves it to the
/// trash.
///
/// It must exceed the longest window between a pause allocating its artifact
/// root and the record that references it becoming durable: a crash inside that
/// window leaves a directory whose record never existed, and a pass that ran
/// earlier would move a directory a sandbox is still writing.
pub const ARTIFACT_GRACE: Duration = Duration::from_secs(2 * 3600);

/// How long a moved directory stays in the trash before the pass deletes it.
///
/// It must exceed the longest restore that could still be reading a directory
/// the pass moved by mistake: the record of a paused sandbox is marked
/// `resuming` before its state is decoded, so a directory moved mid-restore is
/// still recoverable by hand for this long.
pub const TRASH_GRACE: Duration = Duration::from_secs(24 * 3600);

/// Allocation unit reported by `st_blocks`, matching the chain measurement in
/// `orchestrator::persistence::file_backed`.
const ALLOCATION_BLOCK_SIZE: u64 = 512;

/// Whether a pass may act, or must only report.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReapMode {
    /// List and count every verdict without moving or deleting anything.
    #[default]
    Report,
    /// Move orphans to the trash and delete trash entries past
    /// [`TRASH_GRACE`].
    Enforce,
}

impl ReapMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Report => "report",
            Self::Enforce => "enforce",
        }
    }
}

/// One entry the pass looked at, and what it decided.
///
/// Every variant that refers to a sandbox carries its id, so an operator can
/// cross one verdict with the node's `artifacts/` listing and with the node SSD
/// usage series.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArtifactVerdict {
    /// A directory nobody owns, older than the grace: it was moved to the
    /// trash. `bytes` is what the deletion will free later, not what the move
    /// freed, which is nothing.
    Trashed { sandbox_id: SandboxId, bytes: u64 },
    /// A trash entry older than the second grace: it was deleted.
    Removed { sandbox_id: SandboxId, bytes: u64 },
    /// A directory the pass kept, and why.
    Kept {
        sandbox_id: SandboxId,
        reason: KeepReason,
    },
    /// An entry the pass refused to interpret, and why.
    Skipped { name: String, reason: SkipReason },
    /// An orphan the pass would have moved, reported because the pass could
    /// not enforce.
    Reapable { sandbox_id: SandboxId },
    /// A trash entry the pass would have deleted, reported because the pass
    /// could not enforce.
    Expired { sandbox_id: SandboxId, bytes: u64 },
}

/// Why a directory was kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeepReason {
    /// The id belongs to a sandbox this node still holds, or to a template
    /// build in flight.
    LocalId,
    /// The id belongs to a sandbox this node is restoring right now: its
    /// record is marked `resuming`, so its artifacts are being read.
    RestoreOpen,
    /// The id is in the cluster's sandbox list.
    ClusterListed,
    /// The directory is younger than [`ARTIFACT_GRACE`].
    WithinGrace,
}

impl KeepReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalId => "local_id",
            Self::RestoreOpen => "restore_open",
            Self::ClusterListed => "cluster_listed",
            Self::WithinGrace => "within_grace",
        }
    }
}

/// Why an entry was not interpreted at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkipReason {
    /// The name is not a canonical UUID.
    InvalidName,
    /// The entry is a symlink; the pass never follows one.
    Symlink,
    /// The entry is not a directory.
    NotADirectory,
    /// The path is not directly under the artifacts root.
    OutsideRoot,
}

impl SkipReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidName => "invalid_name",
            Self::Symlink => "symlink",
            Self::NotADirectory => "not_a_directory",
            Self::OutsideRoot => "outside_root",
        }
    }
}

/// What one pass found and did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ArtifactSweepReport {
    pub mode: ReapMode,
    /// Set when the pass listed but neither moved nor deleted anything, even in
    /// enforce mode. A cluster list that could not be proved complete sets it.
    pub downgraded_reason: Option<String>,
    pub verdicts: Vec<ArtifactVerdict>,
    /// Directories moved to the trash by this pass.
    pub trashed: u64,
    /// Trash entries deleted by this pass.
    pub removed: u64,
    /// Orphans the pass did not move because it could not enforce.
    pub reapable: u64,
    /// Trash entries the pass did not delete because it could not enforce.
    pub expired: u64,
    /// Bytes a deletion actually freed. Always zero until the deletion phase,
    /// because a rename frees nothing.
    pub reclaimed_bytes: u64,
    /// Entries one error kept the pass from acting on. The rest of the root is
    /// still swept, and every failure is reported so an operator sees what was
    /// left behind.
    pub failures: Vec<ArtifactSweepFailure>,
}

/// One entry the pass could not classify or act on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactSweepFailure {
    pub name: String,
    pub message: String,
}

/// One sandbox this node holds, as the pass needs to see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalSandbox {
    pub id: SandboxId,
    /// Whether a restore of this sandbox is in flight. Its artifacts are being
    /// read, so they must stay even before the record settles.
    pub restoring: bool,
}

/// What a pass needs from the node it runs on.
///
/// It is a trait so `maintenance` never learns the persister's layout or reads
/// the orchestrator, which imports this module.
#[async_trait]
pub trait PausedArtifactSubject: Send + Sync {
    /// The directory holding one entry per sandbox id, or `None` when paused
    /// persistence is disabled on this node.
    fn artifacts_root(&self) -> Option<PathBuf>;

    /// Every sandbox id this node currently holds: running, paused, restoring,
    /// and every template build in flight. Taken fresh on every pass, because a
    /// startup snapshot would miss every sandbox created since.
    async fn local_sandboxes(&self) -> anyhow::Result<Vec<LocalSandbox>>;
}

/// Runs one pass over `subject`'s artifacts root.
///
/// `cluster` answers which sandbox ids the cluster lists. A cluster read that
/// fails, or no cluster at all, downgrades the pass to a report: without it the
/// pass cannot prove any directory orphaned.
///
/// `now` is a parameter rather than a clock read so the aging rules are
/// testable without sleeping.
pub async fn sweep(
    subject: &dyn PausedArtifactSubject,
    cluster: Option<&dyn ClusterSandboxIds>,
    mode: ReapMode,
    now: SystemTime,
) -> anyhow::Result<ArtifactSweepReport> {
    let mut report = ArtifactSweepReport {
        mode,
        ..ArtifactSweepReport::default()
    };
    let Some(root) = subject.artifacts_root() else {
        // Paused persistence is disabled: every artifact is the backend's, and
        // there is nothing this pass may touch.
        return Ok(report);
    };
    let local = subject
        .local_sandboxes()
        .await
        .context("this node's sandbox list could not be read")?;
    let local_ids: BTreeSet<SandboxId> = local.iter().map(|sandbox| sandbox.id).collect();
    let restoring: BTreeSet<SandboxId> = local
        .iter()
        .filter(|sandbox| sandbox.restoring)
        .map(|sandbox| sandbox.id)
        .collect();

    let cluster_ids = match cluster {
        Some(cluster) => match cluster.list_sandbox_ids().await {
            Ok(ids) => ids,
            Err(error) => {
                report.downgraded_reason = Some(format!(
                    "the cluster sandbox list is incomplete, so nothing can be proved orphaned: {error:#}"
                ));
                BTreeSet::new()
            }
        },
        None => {
            report.downgraded_reason = Some(
                "the cluster gateway is not configured, so sandboxes on other nodes cannot be listed"
                    .into(),
            );
            BTreeSet::new()
        }
    };
    let enforce = mode == ReapMode::Enforce && report.downgraded_reason.is_none();

    let mut entries = match fs::read_dir(&root).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(report),
        Err(source) => {
            return Err(anyhow::Error::new(source).context(format!(
                "read paused sandbox artifacts at {}",
                root.display()
            )));
        }
    };
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("scan paused sandbox artifacts at {}", root.display()))?
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == TRASH_DIR {
            // The trash has its own phase below; it is never a sandbox dir, and
            // its name is never judged as an invalid one.
            continue;
        }
        let outcome = classify(
            &root,
            &name,
            &local_ids,
            &restoring,
            &cluster_ids,
            enforce,
            now,
        )
        .await;
        // One entry that cannot be read or acted on must not stop the pass: the
        // rest of the root is still swept, and the failure is never swallowed.
        let verdict = match outcome {
            Ok(verdict) => verdict,
            Err(error) => {
                warn!(
                    name = %name,
                    error = %format_args!("{error:#}"),
                    "failed to sweep a paused sandbox artifact entry"
                );
                report.failures.push(ArtifactSweepFailure {
                    name,
                    message: format!("{error:#}"),
                });
                continue;
            }
        };
        if let Some(verdict) = verdict {
            record(&mut report, verdict);
        }
    }

    reap_trash(&root, enforce, now, &mut report).await?;
    record_pass_metrics(&report);
    info!(
        mode = report.mode.as_str(),
        trashed = report.trashed,
        removed = report.removed,
        reclaimable_bytes = report.reclaimed_bytes,
        reapable = report.reapable,
        expired = report.expired,
        kept = report
            .verdicts
            .iter()
            .filter(|verdict| matches!(verdict, ArtifactVerdict::Kept { .. }))
            .count(),
        skipped = report
            .verdicts
            .iter()
            .filter(|verdict| matches!(verdict, ArtifactVerdict::Skipped { .. }))
            .count(),
        downgraded = report.downgraded_reason.is_some(),
        failures = report.failures.len(),
        "paused sandbox artifact sweep finished"
    );
    Ok(report)
}

/// Decides one entry directly under the artifacts root, and acts on it when
/// `enforce` is set.
async fn classify(
    root: &Path,
    name: &str,
    local_ids: &BTreeSet<SandboxId>,
    restoring: &BTreeSet<SandboxId>,
    cluster_ids: &BTreeSet<SandboxId>,
    enforce: bool,
    now: SystemTime,
) -> anyhow::Result<Option<ArtifactVerdict>> {
    let path = root.join(name);
    let skipped = |reason| {
        Some(ArtifactVerdict::Skipped {
            name: name.to_owned(),
            reason,
        })
    };
    let metadata = match fs::symlink_metadata(&path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(anyhow::Error::new(source)),
    };
    // `symlink_metadata` never follows the link, so a symlink is recognised as
    // one and its target is never inspected, moved or deleted.
    if metadata.file_type().is_symlink() {
        return Ok(skipped(SkipReason::Symlink));
    }
    if !metadata.is_dir() {
        return Ok(skipped(SkipReason::NotADirectory));
    }
    let Some(sandbox_id) = canonical_sandbox_id(name) else {
        return Ok(skipped(SkipReason::InvalidName));
    };
    if path.parent() != Some(root) {
        return Ok(skipped(SkipReason::OutsideRoot));
    }

    let keep = if restoring.contains(&sandbox_id) {
        Some(KeepReason::RestoreOpen)
    } else if local_ids.contains(&sandbox_id) {
        Some(KeepReason::LocalId)
    } else if cluster_ids.contains(&sandbox_id) {
        Some(KeepReason::ClusterListed)
    } else {
        let age = metadata
            .modified()
            .ok()
            .and_then(|modified| now.duration_since(modified).ok());
        // An entry whose mtime cannot be read is treated as too young: without
        // it the pass cannot prove a pause finished writing.
        (age.is_none_or(|age| age < ARTIFACT_GRACE)).then_some(KeepReason::WithinGrace)
    };
    if let Some(reason) = keep {
        return Ok(Some(ArtifactVerdict::Kept { sandbox_id, reason }));
    }
    if !enforce {
        return Ok(Some(ArtifactVerdict::Reapable { sandbox_id }));
    }

    let bytes = directory_bytes(&path).await?;
    let destination = root
        .join(TRASH_DIR)
        .join(epoch_seconds(now).to_string())
        .join(name);
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create the paused artifact trash at {}", parent.display()))?;
    }
    fs::rename(&path, &destination).await.with_context(|| {
        format!(
            "move orphan paused sandbox artifact {} to {}",
            path.display(),
            destination.display()
        )
    })?;
    // The id and the bytes are both carried, so the move can be crossed with
    // the node SSD usage series; the freed bytes are zero until the deletion
    // phase.
    info!(
        sandbox_id = %sandbox_id,
        bytes_freed = 0u64,
        pending_bytes = bytes,
        from = %path.display(),
        to = %destination.display(),
        "moved orphan paused sandbox artifact to the trash"
    );
    Ok(Some(ArtifactVerdict::Trashed { sandbox_id, bytes }))
}

/// Deletes the trash entries whose epoch is older than [`TRASH_GRACE`].
///
/// The trash is laid out as `.trash/{epoch}/{sandbox_id}`, so the epoch records
/// when the move happened and no mtime is needed.
async fn reap_trash(
    root: &Path,
    enforce: bool,
    now: SystemTime,
    report: &mut ArtifactSweepReport,
) -> anyhow::Result<()> {
    let trash_root = root.join(TRASH_DIR);
    let mut epochs = match fs::read_dir(&trash_root).await {
        Ok(epochs) => epochs,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(anyhow::Error::new(source).context(format!(
                "read the paused artifact trash at {}",
                trash_root.display()
            )));
        }
    };
    while let Some(epoch) = epochs
        .next_entry()
        .await
        .with_context(|| format!("scan the paused artifact trash at {}", trash_root.display()))?
    {
        let epoch_name = epoch.file_name().to_string_lossy().into_owned();
        let mut verdict = |reason| {
            report.verdicts.push(ArtifactVerdict::Skipped {
                name: epoch_name.clone(),
                reason,
            });
        };
        let metadata = match fs::symlink_metadata(epoch.path()).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(anyhow::Error::new(source).context(format!(
                    "inspect the paused artifact trash entry {}",
                    epoch.path().display()
                )));
            }
        };
        if metadata.file_type().is_symlink() {
            verdict(SkipReason::Symlink);
            continue;
        }
        if !metadata.is_dir() {
            verdict(SkipReason::NotADirectory);
            continue;
        }
        let Ok(moved_at) = epoch_name.parse::<u64>() else {
            verdict(SkipReason::InvalidName);
            continue;
        };
        if now
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .and_then(|since| since.as_secs().checked_sub(moved_at))
            .is_none_or(|age| age < TRASH_GRACE.as_secs())
        {
            continue;
        }
        reap_trash_epoch(&epoch.path(), enforce, report).await?;
    }
    Ok(())
}

/// Deletes, or reports, every sandbox generation inside one expired trash
/// epoch.
async fn reap_trash_epoch(
    epoch: &Path,
    enforce: bool,
    report: &mut ArtifactSweepReport,
) -> anyhow::Result<()> {
    let mut entries = fs::read_dir(epoch)
        .await
        .with_context(|| format!("read the paused artifact trash epoch {}", epoch.display()))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("scan the paused artifact trash epoch {}", epoch.display()))?
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        let verdict = |report: &mut ArtifactSweepReport, reason| {
            report.verdicts.push(ArtifactVerdict::Skipped {
                name: name.clone(),
                reason,
            });
        };
        let metadata = match fs::symlink_metadata(entry.path()).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(anyhow::Error::new(source).context(format!(
                    "inspect the trashed paused sandbox artifact {}",
                    entry.path().display()
                )));
            }
        };
        if metadata.file_type().is_symlink() {
            verdict(report, SkipReason::Symlink);
            continue;
        }
        if !metadata.is_dir() {
            verdict(report, SkipReason::NotADirectory);
            continue;
        }
        let Some(sandbox_id) = canonical_sandbox_id(&name) else {
            verdict(report, SkipReason::InvalidName);
            continue;
        };
        let bytes = directory_bytes(&entry.path()).await?;
        if !enforce {
            report
                .verdicts
                .push(ArtifactVerdict::Expired { sandbox_id, bytes });
            report.expired += 1;
            continue;
        }
        if let Err(error) = fs::remove_dir_all(entry.path()).await {
            // One directory that cannot be removed must not stop the pass; it
            // stays in the trash and the next pass retries it.
            warn!(
                sandbox_id = %sandbox_id,
                error = %error,
                path = %entry.path().display(),
                "failed to delete a trashed paused sandbox artifact"
            );
            continue;
        }
        info!(
            sandbox_id = %sandbox_id,
            bytes_freed = bytes,
            path = %entry.path().display(),
            "deleted a trashed paused sandbox artifact"
        );
        report
            .verdicts
            .push(ArtifactVerdict::Removed { sandbox_id, bytes });
        report.removed += 1;
        report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(bytes);
    }
    // The epoch directory is only removed once it is empty; a failed deletion
    // above leaves it for the next pass. Report mode never removes anything,
    // not even an empty epoch.
    if enforce {
        let _ = fs::remove_dir(epoch).await;
    }
    Ok(())
}

/// Records one verdict in the report and its metric.
fn record(report: &mut ArtifactSweepReport, verdict: ArtifactVerdict) {
    match &verdict {
        ArtifactVerdict::Trashed { .. } => report.trashed += 1,
        ArtifactVerdict::Removed { bytes, .. } => {
            report.removed += 1;
            report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(*bytes);
        }
        ArtifactVerdict::Reapable { .. } => report.reapable += 1,
        ArtifactVerdict::Expired { .. } => report.expired += 1,
        ArtifactVerdict::Kept { .. } | ArtifactVerdict::Skipped { .. } => {}
    }
    report.verdicts.push(verdict);
}

/// Records one pass's verdicts as metrics and log lines, one per verdict and
/// reason.
///
/// The sandbox id is deliberately not a label: it is unbounded, and the
/// structured log line of each verdict carries it instead. The two byte
/// counters are separate on purpose: `moved` counts a rename, which frees no
/// bytes, while `deleted` and `reclaimed_bytes` count the deletion. An operator
/// crossing these with the node SSD usage series must never read a move as
/// freed space.
fn record_pass_metrics(report: &ArtifactSweepReport) {
    let mode = report.mode.as_str();
    for reason in [
        KeepReason::LocalId,
        KeepReason::RestoreOpen,
        KeepReason::ClusterListed,
        KeepReason::WithinGrace,
    ] {
        let kept = report
            .verdicts
            .iter()
            .filter(|verdict| {
                matches!(verdict, ArtifactVerdict::Kept { reason: kept, .. } if *kept == reason)
            })
            .count();
        if kept > 0 {
            metrics::counter!(
                "agentenv_paused_sandbox_artifact_reap_kept_total",
                "mode" => mode,
                "reason" => reason.as_str(),
            )
            .increment(kept as u64);
        }
    }
    for reason in [
        SkipReason::InvalidName,
        SkipReason::Symlink,
        SkipReason::NotADirectory,
        SkipReason::OutsideRoot,
    ] {
        let skipped = report
            .verdicts
            .iter()
            .filter(|verdict| {
                matches!(
                    verdict,
                    ArtifactVerdict::Skipped { reason: skipped, .. } if *skipped == reason
                )
            })
            .count();
        if skipped > 0 {
            metrics::counter!(
                "agentenv_paused_sandbox_artifact_reap_skipped_total",
                "mode" => mode,
                "reason" => reason.as_str(),
            )
            .increment(skipped as u64);
        }
    }
    // Each action is counted once, with the id in the structured log line and
    // the bytes in the counter, so the receipt can be crossed with the node SSD
    // usage series.
    for verdict in &report.verdicts {
        match verdict {
            // Both actions log once, at the site of the rename or the deletion,
            // where the sandbox id and the bytes are known; this only counts
            // them, so a move is never reported as freed bytes.
            ArtifactVerdict::Trashed { bytes, .. } => {
                metrics::counter!(
                    "agentenv_paused_sandbox_artifact_reap_moved_total",
                    "mode" => mode,
                )
                .increment(1);
                // Named `pending`, not `reclaimed`: a rename frees nothing, and
                // this is what the deletion will free later.
                metrics::counter!(
                    "agentenv_paused_sandbox_artifact_reap_pending_bytes_total",
                    "mode" => mode,
                )
                .increment(*bytes);
            }
            ArtifactVerdict::Removed { bytes, .. } => {
                metrics::counter!(
                    "agentenv_paused_sandbox_artifact_reap_deleted_total",
                    "mode" => mode,
                )
                .increment(1);
                metrics::counter!(
                    "agentenv_paused_sandbox_artifact_reap_reclaimed_bytes_total",
                    "mode" => mode,
                )
                .increment(*bytes);
            }
            ArtifactVerdict::Kept { sandbox_id, reason } => debug!(
                mode,
                sandbox_id = %sandbox_id,
                reason = reason.as_str(),
                "kept a paused sandbox artifact"
            ),
            ArtifactVerdict::Skipped { name, reason } => debug!(
                mode,
                name = %name,
                reason = reason.as_str(),
                "skipped a paused sandbox artifact entry"
            ),
            ArtifactVerdict::Reapable { sandbox_id } => warn!(
                mode,
                sandbox_id = %sandbox_id,
                "a paused sandbox artifact is orphaned, but this pass may not enforce"
            ),
            ArtifactVerdict::Expired { sandbox_id, bytes } => warn!(
                mode,
                sandbox_id = %sandbox_id,
                pending_bytes = *bytes,
                "a trashed paused sandbox artifact is expired, but this pass may not enforce"
            ),
        }
    }
}

/// Allocated bytes of every regular file under `directory`.
///
/// Allocated blocks, not apparent size: a pause writes sparse files, and what
/// the deletion frees on the node SSD is the allocation.
async fn directory_bytes(directory: &Path) -> anyhow::Result<u64> {
    use std::os::unix::fs::MetadataExt;

    let mut bytes = 0u64;
    let mut pending = vec![directory.to_path_buf()];
    while let Some(current) = pending.pop() {
        let mut entries = match fs::read_dir(&current).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(
                    anyhow::Error::new(source).context(format!("measure {}", current.display()))
                );
            }
        };
        while let Some(entry) = entries
            .next_entry()
            .await
            .with_context(|| format!("measure {}", current.display()))?
        {
            let metadata = match fs::symlink_metadata(entry.path()).await {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => {
                    return Err(anyhow::Error::new(source)
                        .context(format!("measure {}", entry.path().display())));
                }
            };
            if metadata.file_type().is_symlink() {
                continue;
            }
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                bytes =
                    bytes.saturating_add(metadata.blocks().saturating_mul(ALLOCATION_BLOCK_SIZE));
            }
        }
    }
    Ok(bytes)
}

/// `name` as a sandbox id, only when it is the canonical spelling.
///
/// [`SandboxId::parse_str`] accepts every UUID spelling, uppercase, braced and
/// urn forms included, while the persister always writes the lowercase
/// hyphenated form. Accepting a looser spelling would let a directory the
/// persister never creates be treated as a sandbox generation.
fn canonical_sandbox_id(name: &str) -> Option<SandboxId> {
    let sandbox_id = SandboxId::parse_str(name).ok()?;
    (sandbox_id.to_string() == name).then_some(sandbox_id)
}

fn epoch_seconds(now: SystemTime) -> u64 {
    now.duration_since(SystemTime::UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
