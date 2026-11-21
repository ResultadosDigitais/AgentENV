//! One named test per verdict of the paused artifact sweep.
//!
//! Each test builds its own artifacts root, its own local sandbox list and its
//! own cluster list, so a passing row cannot be explained by another row's
//! setup. Every test asserts the *verdict and its reason*, not only whether the
//! directory still exists: a table that only checked existence would pass while
//! the wrong condition made the decision.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use tempfile::TempDir;

use super::*;
use crate::maintenance::behavior_table::filetime_set;
use crate::maintenance::ClusterSandboxIds;

/// The subject a test feeds the pass: one artifacts root and one fixed local
/// sandbox list.
struct Subject {
    root: Option<PathBuf>,
    local: Vec<LocalSandbox>,
}

#[async_trait]
impl PausedArtifactSubject for Subject {
    fn artifacts_root(&self) -> Option<PathBuf> {
        self.root.clone()
    }

    async fn local_sandboxes(&self) -> anyhow::Result<Vec<LocalSandbox>> {
        Ok(self.local.clone())
    }
}

/// A cluster that answers with a fixed id set.
struct Cluster(BTreeSet<SandboxId>);

#[async_trait]
impl ClusterSandboxIds for Cluster {
    async fn list_sandbox_ids(&self) -> anyhow::Result<BTreeSet<SandboxId>> {
        Ok(self.0.clone())
    }
}

/// A cluster whose pass must fail, standing in for a node that dropped out of
/// the gateway's fan-out.
struct FailingCluster;

#[async_trait]
impl ClusterSandboxIds for FailingCluster {
    async fn list_sandbox_ids(&self) -> anyhow::Result<BTreeSet<SandboxId>> {
        anyhow::bail!("node list failed")
    }
}

fn cluster(ids: &[SandboxId]) -> Cluster {
    Cluster(ids.iter().copied().collect())
}

fn subject(root: &Path, local: Vec<LocalSandbox>) -> Subject {
    Subject {
        root: Some(root.to_path_buf()),
        local,
    }
}

/// Creates `artifacts/{id}`, as a pause does, and ages it far past both graces.
fn sandbox_dir(root: &Path, id: SandboxId) -> PathBuf {
    let path = root.join(id.to_string());
    std::fs::create_dir_all(&path).expect("create sandbox artifact dir");
    age(&path, ARTIFACT_GRACE + Duration::from_secs(3600));
    path
}

fn age(path: &Path, by: Duration) {
    filetime_set(path, SystemTime::now() - by);
}

fn verdicts(report: &ArtifactSweepReport) -> Vec<&ArtifactVerdict> {
    report.verdicts.iter().collect()
}

fn kept_reason(report: &ArtifactSweepReport, id: SandboxId) -> Option<KeepReason> {
    report.verdicts.iter().find_map(|verdict| match verdict {
        ArtifactVerdict::Kept { sandbox_id, reason } if *sandbox_id == id => Some(*reason),
        _ => None,
    })
}

// ---------------------------------------------------------------------------
// Reap: the directory leaves, in two phases.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_orphan_older_than_the_grace_moves_to_the_trash() -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let path = temp.path().join(id.to_string());
    std::fs::create_dir_all(&path)?;
    std::fs::write(path.join("vm_state.bin"), vec![0u8; 4096])?;
    // Aged after the write: creating a file inside the directory refreshes the
    // directory's own mtime, and the grace is measured on that mtime.
    age(&path, ARTIFACT_GRACE + Duration::from_secs(3600));
    let now = SystemTime::now();

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        now,
    )
    .await?;

    assert_eq!(
        verdicts(&report),
        vec![&ArtifactVerdict::Trashed {
            sandbox_id: id,
            bytes: 4096
        }],
        "the only verdict is the move, carrying the id and the bytes the deletion will free"
    );
    assert_eq!(report.trashed, 1);
    assert_eq!(
        report.reclaimed_bytes, 0,
        "a rename frees nothing, so the move must not report freed bytes"
    );
    assert!(!path.exists(), "the directory must be gone from artifacts/");
    let trashed = temp
        .path()
        .join(TRASH_DIR)
        .join(epoch_seconds(now).to_string())
        .join(id.to_string());
    assert!(
        trashed.is_dir(),
        "the directory must be under .trash/{{epoch}}/"
    );
    assert!(
        trashed.join("vm_state.bin").is_file(),
        "the move must carry the artifacts, not only the directory"
    );
    Ok(())
}

#[tokio::test]
async fn a_trashed_directory_is_deleted_after_the_second_grace_and_counts_its_bytes() -> Result<()>
{
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let moved_at = SystemTime::now() - TRASH_GRACE - Duration::from_secs(60);
    let trashed = temp
        .path()
        .join(TRASH_DIR)
        .join(epoch_seconds(moved_at).to_string())
        .join(id.to_string());
    std::fs::create_dir_all(&trashed)?;
    std::fs::write(trashed.join("memory.bin"), vec![0u8; 8192])?;

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(
        verdicts(&report),
        vec![&ArtifactVerdict::Removed {
            sandbox_id: id,
            bytes: 8192
        }],
        "the deletion is the only verdict that frees bytes, and it carries both id and bytes"
    );
    assert_eq!(report.removed, 1);
    assert_eq!(report.reclaimed_bytes, 8192);
    assert!(!trashed.exists(), "the trash entry must be gone");
    Ok(())
}

#[tokio::test]
async fn a_trashed_directory_inside_the_second_grace_stays() -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let trashed = temp
        .path()
        .join(TRASH_DIR)
        .join(epoch_seconds(SystemTime::now()).to_string())
        .join(id.to_string());
    std::fs::create_dir_all(&trashed)?;

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(report.removed, 0, "the second grace has not elapsed yet");
    assert!(trashed.is_dir(), "the soft-deleted directory must survive");
    Ok(())
}

// ---------------------------------------------------------------------------
// Keep: every condition on its own.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_id_in_the_local_list_is_kept_even_with_an_ancient_mtime() -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let path = sandbox_dir(temp.path(), id);

    let report = sweep(
        &subject(
            temp.path(),
            vec![LocalSandbox {
                id,
                restoring: false,
            }],
        ),
        // The cluster list omits it: the local list alone must keep it.
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(kept_reason(&report, id), Some(KeepReason::LocalId));
    assert!(path.is_dir(), "a locally held sandbox must never be moved");
    Ok(())
}

#[tokio::test]
async fn an_id_in_the_cluster_list_is_kept_even_with_an_ancient_mtime() -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let path = sandbox_dir(temp.path(), id);

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[id])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(kept_reason(&report, id), Some(KeepReason::ClusterListed));
    assert!(
        path.is_dir(),
        "a sandbox on another node must keep its artifacts"
    );
    Ok(())
}

#[tokio::test]
async fn an_open_restore_is_kept() -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let path = sandbox_dir(temp.path(), id);

    let report = sweep(
        &subject(
            temp.path(),
            vec![LocalSandbox {
                id,
                restoring: true,
            }],
        ),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(
        kept_reason(&report, id),
        Some(KeepReason::RestoreOpen),
        "the restore reason must win over the plain local id, or the log would hide it"
    );
    assert!(
        path.is_dir(),
        "the artifacts of a restore in flight must stay"
    );
    Ok(())
}

#[tokio::test]
async fn a_directory_inside_the_grace_is_kept() -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let path = temp.path().join(id.to_string());
    std::fs::create_dir_all(&path)?;
    // Freshly created: the pause that allocated it may still be writing.
    age(&path, Duration::from_secs(60));

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(kept_reason(&report, id), Some(KeepReason::WithinGrace));
    assert!(path.is_dir());
    Ok(())
}

// ---------------------------------------------------------------------------
// Skip: entries the pass refuses to interpret.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_symlink_is_skipped_and_its_target_survives() -> Result<()> {
    let temp = TempDir::new()?;
    let outside = TempDir::new()?;
    let id = SandboxId::new();
    let target = outside.path().join("precious");
    std::fs::create_dir_all(&target)?;
    std::fs::write(target.join("data"), b"keep me")?;
    let link = temp.path().join(id.to_string());
    std::os::unix::fs::symlink(&target, &link)?;
    age(&link, ARTIFACT_GRACE + Duration::from_secs(3600));

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(
        verdicts(&report),
        vec![&ArtifactVerdict::Skipped {
            name: id.to_string(),
            reason: SkipReason::Symlink
        }]
    );
    assert!(
        target.join("data").is_file(),
        "the pass must never follow a symlink and delete its target"
    );
    assert!(
        std::fs::symlink_metadata(&link).is_ok(),
        "the link itself is left for an operator"
    );
    Ok(())
}

#[tokio::test]
async fn a_name_that_is_not_a_canonical_uuid_is_skipped() -> Result<()> {
    let temp = TempDir::new()?;
    let uppercase = SandboxId::new().to_string().to_uppercase();
    for name in [uppercase.as_str(), "not-a-uuid"] {
        let path = temp.path().join(name);
        std::fs::create_dir_all(&path)?;
        age(&path, ARTIFACT_GRACE + Duration::from_secs(3600));
    }

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(
        report.trashed, 0,
        "no entry with a non-canonical name may move"
    );
    let skipped: Vec<_> = report
        .verdicts
        .iter()
        .filter_map(|verdict| match verdict {
            ArtifactVerdict::Skipped { name, reason } => Some((name.as_str(), *reason)),
            _ => None,
        })
        .collect();
    assert!(
        skipped
            .iter()
            .any(|(name, reason)| *name == uppercase && *reason == SkipReason::InvalidName),
        "a valid UUID in uppercase is not the spelling the persister writes: {skipped:?}"
    );
    assert!(
        skipped
            .iter()
            .any(|(name, reason)| *name == "not-a-uuid" && *reason == SkipReason::InvalidName),
        "a non-UUID name is never a sandbox id: {skipped:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_canonical_uuid_under_a_subdirectory_of_the_root_is_never_reached() -> Result<()> {
    // `read_dir` yields only direct entries, so an id nested one level down is
    // invisible to the pass: the persister's layout is `artifacts/{id}`, and a
    // deeper path is not one of its directories. This pins the `parent == root`
    // rule the classification enforces.
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let nested = temp.path().join("nest").join(id.to_string());
    std::fs::create_dir_all(&nested)?;
    age(
        &temp.path().join("nest"),
        ARTIFACT_GRACE + Duration::from_secs(3600),
    );
    age(&nested, ARTIFACT_GRACE + Duration::from_secs(3600));

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(
        verdicts(&report),
        vec![&ArtifactVerdict::Skipped {
            name: "nest".to_owned(),
            reason: SkipReason::InvalidName
        }],
        "only a direct child of the root is a candidate at all"
    );
    assert!(
        nested.is_dir(),
        "a directory the pass never classified must stay untouched"
    );
    Ok(())
}

#[tokio::test]
async fn a_regular_file_is_skipped_and_left_in_place() -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let path = temp.path().join(id.to_string());
    std::fs::write(&path, b"not a directory")?;
    age(&path, ARTIFACT_GRACE + Duration::from_secs(3600));

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(
        verdicts(&report),
        vec![&ArtifactVerdict::Skipped {
            name: id.to_string(),
            reason: SkipReason::NotADirectory
        }]
    );
    assert!(
        path.is_file(),
        "only a directory is ever a sandbox generation"
    );
    Ok(())
}

#[tokio::test]
async fn the_trash_directory_is_never_a_sandbox_dir_and_never_reaped_as_an_invalid_name(
) -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let trashed = temp
        .path()
        .join(TRASH_DIR)
        .join(epoch_seconds(SystemTime::now() - TRASH_GRACE - Duration::from_secs(60)).to_string())
        .join(id.to_string());
    std::fs::create_dir_all(&trashed)?;
    std::fs::write(trashed.join("state"), vec![0u8; 1024])?;
    age(
        &temp.path().join(TRASH_DIR),
        ARTIFACT_GRACE + Duration::from_secs(3600),
    );

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert!(
        !report.verdicts.iter().any(|verdict| matches!(
            verdict,
            ArtifactVerdict::Skipped { name, .. } if name == TRASH_DIR
        )),
        "the trash is the pass's own directory, not an invalid sandbox name: {:?}",
        report.verdicts
    );
    assert_eq!(
        report.removed, 1,
        "the trash phase still owns the entry: {:?}",
        report.verdicts
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Quarantine: where boot puts a generation no record names, emptied only by an
// explicit TTL.
// ---------------------------------------------------------------------------

/// Creates `artifacts/.quarantine/{moved_at}/{id}`, as boot leaves it, with one
/// file so the deletion has bytes to count.
fn quarantined(root: &Path, id: SandboxId, moved_at: SystemTime) -> PathBuf {
    let path = root
        .join(QUARANTINE_DIR)
        .join(epoch_seconds(moved_at).to_string())
        .join(id.to_string());
    std::fs::create_dir_all(&path).expect("create quarantined generation");
    // One allocation block of 4096 bytes: the reaper counts allocated blocks,
    // not apparent size, so the assertion below can name the exact number.
    std::fs::write(path.join("vm_state.bin"), vec![0u8; 4096]).expect("write quarantined artifact");
    path
}

#[tokio::test]
async fn a_quarantined_generation_is_deleted_after_the_ttl_and_counts_its_bytes() -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let path = quarantined(
        temp.path(),
        id,
        SystemTime::now() - QUARANTINE_TTL - Duration::from_secs(60),
    );

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(
        verdicts(&report),
        vec![&ArtifactVerdict::QuarantineRemoved {
            sandbox_id: id,
            bytes: 4096
        }],
        "the only verdict is the quarantine deletion, carrying the id and the bytes"
    );
    assert_eq!(report.quarantine_expired, 0);
    assert_eq!(report.quarantine_removed, 1);
    assert_eq!(report.reclaimed_bytes, 4096);
    assert!(!path.exists(), "an expired quarantined generation is gone");
    Ok(())
}

#[tokio::test]
async fn a_quarantined_generation_inside_the_ttl_stays() -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let path = quarantined(
        temp.path(),
        id,
        SystemTime::now() - QUARANTINE_TTL + Duration::from_secs(60),
    );

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert!(report.verdicts.is_empty(), "the TTL has not elapsed yet");
    assert_eq!(report.quarantine_removed, 0);
    assert!(
        path.join("vm_state.bin").is_file(),
        "the quarantined bytes must survive until the TTL"
    );
    Ok(())
}

#[tokio::test]
async fn report_mode_counts_quarantined_expiry_and_deletes_nothing() -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let path = quarantined(
        temp.path(),
        id,
        SystemTime::now() - QUARANTINE_TTL - Duration::from_secs(60),
    );

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Report,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(
        verdicts(&report),
        vec![&ArtifactVerdict::QuarantineExpired {
            sandbox_id: id,
            bytes: 4096
        }],
        "report mode still names what it would delete"
    );
    assert_eq!(report.quarantine_expired, 1);
    assert_eq!(report.quarantine_removed, 0);
    assert_eq!(report.reclaimed_bytes, 0);
    assert!(path.is_dir(), "report mode must delete nothing");
    Ok(())
}

#[tokio::test]
async fn a_cluster_error_deletes_no_quarantined_generation() -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let path = quarantined(
        temp.path(),
        id,
        SystemTime::now() - QUARANTINE_TTL - Duration::from_secs(60),
    );

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&FailingCluster),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(
        (report.quarantine_expired, report.quarantine_removed),
        (1, 0),
        "a downgraded pass reports the expired generation and deletes nothing"
    );
    assert!(path.is_dir(), "the quarantined bytes must survive");
    Ok(())
}

#[tokio::test]
async fn a_quarantine_epoch_with_a_malformed_name_is_skipped() -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let epoch = temp.path().join(QUARANTINE_DIR).join("not-an-epoch");
    std::fs::create_dir_all(epoch.join(id.to_string()))?;

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(
        verdicts(&report),
        vec![&ArtifactVerdict::Skipped {
            name: "not-an-epoch".to_string(),
            reason: SkipReason::InvalidName
        }],
        "an epoch name that does not parse is reported, never interpreted"
    );
    assert!(epoch.is_dir(), "nothing is deleted on a skipped epoch");
    Ok(())
}

#[tokio::test]
async fn a_quarantined_symlink_is_skipped_and_its_target_survives() -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    // The target lives outside the artifacts root: if the pass ever followed
    // the link it would find a name that is not a sandbox id.
    let target = TempDir::new()?;
    std::fs::write(target.path().join("keep"), b"keep")?;
    let epoch = temp.path().join(QUARANTINE_DIR).join(
        epoch_seconds(SystemTime::now() - QUARANTINE_TTL - Duration::from_secs(60)).to_string(),
    );
    std::fs::create_dir_all(&epoch)?;
    std::os::unix::fs::symlink(target.path(), epoch.join(id.to_string()))?;

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(
        verdicts(&report),
        vec![&ArtifactVerdict::Skipped {
            name: id.to_string(),
            reason: SkipReason::Symlink
        }],
        "a symlink is never followed, so it is never deleted through"
    );
    assert!(
        target.path().join("keep").is_file(),
        "the target must survive"
    );
    assert!(epoch.join(id.to_string()).is_symlink());
    Ok(())
}

#[tokio::test]
async fn the_quarantine_directory_is_never_a_sandbox_dir_in_the_entry_phase() -> Result<()> {
    let temp = TempDir::new()?;
    std::fs::create_dir_all(temp.path().join(QUARANTINE_DIR))?;
    age(
        &temp.path().join(QUARANTINE_DIR),
        ARTIFACT_GRACE + Duration::from_secs(3600),
    );

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert!(
        !report.verdicts.iter().any(|verdict| matches!(
            verdict,
            ArtifactVerdict::Skipped { name, .. } if name == QUARANTINE_DIR
        )),
        "the quarantine is the pass's own directory, not an invalid sandbox name: {:?}",
        report.verdicts
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Report mode and fail-closed.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn report_mode_never_moves_or_deletes() -> Result<()> {
    let temp = TempDir::new()?;
    let orphan = SandboxId::new();
    let orphan_path = sandbox_dir(temp.path(), orphan);
    let trashed_id = SandboxId::new();
    let trashed = temp
        .path()
        .join(TRASH_DIR)
        .join(epoch_seconds(SystemTime::now() - TRASH_GRACE - Duration::from_secs(60)).to_string())
        .join(trashed_id.to_string());
    std::fs::create_dir_all(&trashed)?;

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&cluster(&[])),
        ReapMode::Report,
        SystemTime::now(),
    )
    .await?;

    assert_eq!(
        verdicts(&report),
        vec![
            &ArtifactVerdict::Reapable { sandbox_id: orphan },
            &ArtifactVerdict::Expired {
                sandbox_id: trashed_id,
                bytes: 0
            },
        ],
        "report mode still names what it would do"
    );
    assert_eq!((report.trashed, report.removed), (0, 0));
    assert_eq!(report.reclaimed_bytes, 0);
    assert!(orphan_path.is_dir(), "report mode must move nothing");
    assert!(trashed.is_dir(), "report mode must delete nothing");
    Ok(())
}

#[tokio::test]
async fn a_cluster_error_moves_and_deletes_nothing_in_enforce_mode() -> Result<()> {
    let temp = TempDir::new()?;
    let orphan = SandboxId::new();
    let orphan_path = sandbox_dir(temp.path(), orphan);
    let trashed_id = SandboxId::new();
    let trashed = temp
        .path()
        .join(TRASH_DIR)
        .join(epoch_seconds(SystemTime::now() - TRASH_GRACE - Duration::from_secs(60)).to_string())
        .join(trashed_id.to_string());
    std::fs::create_dir_all(&trashed)?;

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        Some(&FailingCluster),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert!(
        report
            .downgraded_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("incomplete")),
        "the report must say why it could not enforce: {:?}",
        report.downgraded_reason
    );
    assert_eq!(
        (report.trashed, report.removed, report.reclaimed_bytes),
        (0, 0, 0),
        "a partial list deletes nothing, in either phase"
    );
    assert!(
        orphan_path.is_dir(),
        "the orphan must survive a failed cluster read"
    );
    assert!(
        trashed.is_dir(),
        "the trash must survive a failed cluster read"
    );
    Ok(())
}

#[tokio::test]
async fn an_unconfigured_cluster_moves_and_deletes_nothing() -> Result<()> {
    let temp = TempDir::new()?;
    let id = SandboxId::new();
    let path = sandbox_dir(temp.path(), id);

    let report = sweep(
        &subject(temp.path(), Vec::new()),
        None,
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert!(
        report
            .downgraded_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("not configured")),
        "an unconfigured gateway must be named as the reason: {:?}",
        report.downgraded_reason
    );
    assert_eq!(report.trashed, 0);
    assert!(
        path.is_dir(),
        "without the cluster the pass cannot prove an orphan"
    );
    Ok(())
}

#[tokio::test]
async fn a_disabled_persister_sweeps_nothing() -> Result<()> {
    let report = sweep(
        &Subject {
            root: None,
            local: Vec::new(),
        },
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    assert!(report.verdicts.is_empty());
    assert_eq!((report.trashed, report.removed), (0, 0));
    Ok(())
}

#[tokio::test]
async fn an_unreadable_root_fails_the_pass_instead_of_reporting_an_empty_one() -> Result<()> {
    // A root that exists but cannot be listed must not read as "nothing to
    // reclaim": an operator would then believe the SSD is clean.
    let temp = TempDir::new()?;
    let missing = temp.path().join("not-a-directory");
    std::fs::write(&missing, b"file, not a directory")?;

    let error = sweep(
        &Subject {
            root: Some(missing),
            local: Vec::new(),
        },
        Some(&cluster(&[])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await
    .expect_err("an unlistable root is an error, not an empty result");

    assert!(
        format!("{error:#}").contains("read paused sandbox artifacts"),
        "{error:#}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The report's own invariants.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_kept_verdict_carries_the_id_and_a_relabelled_reason() -> Result<()> {
    let temp = TempDir::new()?;
    let local = SandboxId::new();
    let listed = SandboxId::new();
    let fresh = SandboxId::new();
    sandbox_dir(temp.path(), local);
    sandbox_dir(temp.path(), listed);
    let fresh_path = temp.path().join(fresh.to_string());
    std::fs::create_dir_all(&fresh_path)?;
    age(&fresh_path, Duration::from_secs(10));
    let subject = subject(
        temp.path(),
        vec![LocalSandbox {
            id: local,
            restoring: false,
        }],
    );

    let report = sweep(
        &subject,
        Some(&cluster(&[listed])),
        ReapMode::Enforce,
        SystemTime::now(),
    )
    .await?;

    let mut reasons: Vec<&str> = report
        .verdicts
        .iter()
        .filter_map(|verdict| match verdict {
            ArtifactVerdict::Kept { reason, .. } => Some(reason.as_str()),
            _ => None,
        })
        .collect();
    reasons.sort_unstable();
    assert_eq!(reasons, vec!["cluster_listed", "local_id", "within_grace"]);
    Ok(())
}

/// Records the paths a subject was asked about, so a test can prove the pass
/// reads the root it was given rather than a startup snapshot.
#[derive(Default)]
struct RecordingSubject {
    root: Option<PathBuf>,
    calls: Arc<Mutex<u32>>,
}

#[async_trait]
impl PausedArtifactSubject for RecordingSubject {
    fn artifacts_root(&self) -> Option<PathBuf> {
        self.root.clone()
    }

    async fn local_sandboxes(&self) -> anyhow::Result<Vec<LocalSandbox>> {
        *self.calls.lock().expect("calls") += 1;
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn every_pass_takes_a_fresh_local_list() -> Result<()> {
    let temp = TempDir::new()?;
    let subject = RecordingSubject {
        root: Some(temp.path().to_path_buf()),
        calls: Arc::default(),
    };

    for _ in 0..2 {
        sweep(
            &subject,
            Some(&cluster(&[])),
            ReapMode::Report,
            SystemTime::now(),
        )
        .await?;
    }

    assert_eq!(
        *subject.calls.lock().expect("calls"),
        2,
        "a pass must read the node's current sandboxes, not a startup snapshot"
    );
    Ok(())
}
