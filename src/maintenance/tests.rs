//! The managed-layer collector's behavior table.
//!
//! Every scenario is written once here and run against every backend, so a
//! backend cannot encode its own version of the rule. The OSS backend runs the
//! same table from `crates/e2e-tests`, which is where MinIO lives.

use std::collections::BTreeSet;
use std::time::Duration;

use async_trait::async_trait;

use super::{
    collect_managed_layers, LiveSandboxes, ManagedLayerCollectMode, ManagedLayerCollectRequest,
    ManagedLayerCollectorError,
};
use crate::image::{ImageCatalog, PublishedImage};
use crate::repository::{
    BlobBody, BlobKey, DocumentKey, LayerRole, ManagedLayerObject, Repository, RepositoryResult,
};
use crate::repository::backends::PosixFsRepository;
use crate::snapshot::{CommittedSnapshot, SnapshotRecord};
use crate::types::{OverlaybdLayerRef, SandboxResources};
use crate::volume::{VolumeRecord, VolumeStatus};

/// A layer directory with no records at all: everything in it is a candidate.
///
/// The grace of one hour is far longer than any test takes, so a layer written
/// now is always inside it unless a scenario says otherwise.
const GRACE: Duration = Duration::from_secs(3600);

fn digest(seed: &str) -> String {
    crate::digest::sha256_digest(seed.as_bytes())
}

fn layer(digest: &str) -> OverlaybdLayerRef {
    OverlaybdLayerRef::Managed(crate::types::ManagedLayer {
        digest: digest.to_owned(),
        size: 4096,
        uuid: None,
    })
}

fn request(mode: ManagedLayerCollectMode) -> ManagedLayerCollectRequest {
    ManagedLayerCollectRequest {
        mode,
        grace: GRACE,
    }
}

/// A node with no live sandbox.
struct NoLiveSandboxesUnderTest;

#[async_trait]
impl LiveSandboxes for NoLiveSandboxesUnderTest {
    async fn live_snapshot_origins(&self) -> anyhow::Result<Vec<String>> {
        Ok(Vec::new())
    }
}

/// A node whose live sandboxes all read the given snapshot IDs.
struct LiveSandboxesUnderTest(Vec<String>);

#[async_trait]
impl LiveSandboxes for LiveSandboxesUnderTest {
    async fn live_snapshot_origins(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.0.clone())
    }
}

/// Stores one managed layer of `bytes` bytes straight through the blob
/// primitives, which is exactly what a publication does before it writes its
/// record.
async fn store_layer<R: Repository + ?Sized>(
    repository: &R,
    digest: &str,
    bytes: u64,
) -> RepositoryResult<()> {
    repository
        .put_blob(
            &BlobKey::Layer {
                digest: digest.to_owned(),
                role: LayerRole::Rootfs,
            },
            BlobBody::Bytes(vec![0xAB; bytes as usize]),
        )
        .await
}

async fn stored_layer_keys<R: Repository + ?Sized>(repository: &R) -> BTreeSet<String> {
    repository
        .list_managed_layers()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|object| object.layer_digest)
        .collect()
}

/// Writes a snapshot record whose committed rootfs cites `layers`.
async fn store_snapshot_record<R: Repository + ?Sized>(repository: &R, layers: &[&str]) {
    let record = SnapshotRecord {
        id: crate::types::SnapshotId::generate(),
        alias: None,
        source: crate::snapshot::SnapshotSource::Template {
            build: crate::snapshot::TemplateBuildInfo {
                status: crate::snapshot::TemplateBuildStatus::Ready,
                started_at_unix_ms: None,
                finished_at_unix_ms: Some(0),
                error_reason: None,
            },
        },
        resources: SandboxResources::default(),
        created_at_unix_ms: 0,
        updated_at_unix_ms: 0,
        committed: Some(CommittedSnapshot {
            rootfs_layers: layers.iter().map(|digest| layer(digest)).collect(),
            ..CommittedSnapshot::mock()
        }),
        deleting: false,
    };
    let bytes = serde_json::to_vec(&record).expect("serialize snapshot record");
    repository
        .put_document(DocumentKey::SnapshotRecord(record.id), bytes)
        .await
        .expect("store snapshot record");
}

/// Writes a volume record whose backing cites `layers`.
async fn store_volume_record<R: Repository + ?Sized>(repository: &R, layers: &[&str]) {
    let record = VolumeRecord {
        id: "vol_collect".to_owned(),
        name: "collect".to_owned(),
        mode: crate::volume::VolumeMode::Exclusive,
        size_mb: 16,
        status: VolumeStatus::Ready,
        reserved_by_sandbox_id: None,
        backing_image_config: None,
        backing_layers: layers.iter().map(|digest| layer(digest)).collect(),
        read_only_mounts: Vec::new(),
        deleting: false,
    };
    let bytes = serde_json::to_vec(&record).expect("serialize volume record");
    repository
        .put_document(DocumentKey::VolumeRecord(record.id), bytes)
        .await
        .expect("store volume record");
}

/// Publishes an image description that owns `layers`.
async fn store_image<R: Repository + ?Sized>(repository: &R, layers: &[&str]) {
    let image = PublishedImage::new(
        "amd64".to_owned(),
        layers.iter().map(|digest| layer(digest)).collect(),
        serde_json::json!({ "architecture": "amd64", "os": "linux" }),
    );
    repository.put_image(&image).await.expect("publish image");
}

/// Makes an object look older than any grace, without changing the backend's
/// own clock: the pass reads `last_modified`, and only the POSIX backend can be
/// aged through the filesystem.
fn age_layer(root: &std::path::Path, digest: &str) {
    let path = root.join(
        BlobKey::Layer {
            digest: digest.to_owned(),
            role: LayerRole::Rootfs,
        }
        .posix_path(),
    );
    let old = std::time::SystemTime::now() - Duration::from_secs(24 * 3600);
    filetime_set(&path, old);
}

/// Stores one object under `managed-layers/` whose name does not translate back
/// to a digest, straight into the backend's storage.
///
/// It cannot go through `put_blob`, which only writes digest-shaped names: such
/// an object is exactly what an older release, or a hand edit, leaves behind.
async fn write_unreadable_layer<R: Repository + ?Sized>(
    repository: &R,
    root: &std::path::Path,
) {
    let key = format!("{}/not-a-digest.overlaybd.commit", BlobKey::LAYERS_DIR);
    let path = root.join(&key);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("mkdir managed layers");
    }
    std::fs::write(&path, [0u8; 32]).expect("write unreadable layer");
    let old = std::time::SystemTime::now() - Duration::from_secs(24 * 3600);
    filetime_set(&path, old);
    let _ = repository;
}

fn filetime_set(path: &std::path::Path, at: std::time::SystemTime) {
    // `filetime` is not a dependency, and the standard library cannot set a
    // file's mtime; `utimensat` is one call. Keeping it here avoids adding a
    // crate for a test-only helper.
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).expect("path");
    let times = [
        libc::timespec {
            tv_sec: at
                .duration_since(std::time::UNIX_EPOCH)
                .expect("after epoch")
                .as_secs() as libc::time_t,
            tv_nsec: 0,
        },
        libc::timespec {
            tv_sec: at
                .duration_since(std::time::UNIX_EPOCH)
                .expect("after epoch")
                .as_secs() as libc::time_t,
            tv_nsec: 0,
        },
    ];
    let result = unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0) };
    assert_eq!(
        result,
        0,
        "set mtime of '{}': {}",
        path.display(),
        std::io::Error::last_os_error()
    );
}

/// Runs the whole table against one repository backend.
///
/// `root` is where a POSIX backend stores its files, so a scenario can age a
/// layer; `age` says whether the backend exposes file timestamps this way.
macro_rules! behavior_table {
    ($name:ident, $repository:expr, $root:expr, $age:expr) => {
        mod $name {
            use super::*;

            async fn repository() -> (Box<dyn Repository>, tempfile::TempDir) {
                let temp = tempfile::TempDir::new().expect("tempdir");
                let repository: Box<dyn Repository> = $repository(temp.path());
                (repository, temp)
            }

            /// Makes the layer `digest` old enough to be collected.
            ///
            /// A backend whose storage this test cannot reach through the
            /// filesystem ages nothing and asks for a zero grace instead, so
            /// "older than the grace" still holds for anything written before
            /// the pass started.
            fn age_or_shrink_grace(
                temp: &tempfile::TempDir,
                digest: &str,
                mode: ManagedLayerCollectMode,
            ) -> ManagedLayerCollectRequest {
                if $age {
                    age_layer(&$root(temp), digest);
                    request(mode)
                } else {
                    ManagedLayerCollectRequest {
                        mode,
                        grace: Duration::ZERO,
                    }
                }
            }

            #[tokio::test]
            async fn a_layer_cited_by_any_record_never_leaves() {
                let (repository, temp) = repository().await;
                let cited = digest("cited");
                let orphan = digest("orphan");
                store_layer(repository.as_ref(), &cited, 64)
                    .await
                    .expect("store cited layer");
                store_layer(repository.as_ref(), &orphan, 128)
                    .await
                    .expect("store orphan layer");
                store_snapshot_record(repository.as_ref(), &[&cited]).await;
                // Past the grace, so the uncited layer counts as an orphan
                // rather than as a layer a publish in flight may still cite.
                let pass = age_or_shrink_grace(&temp, &orphan, ManagedLayerCollectMode::Report);

                let report = collect_managed_layers(
                    repository.as_ref(),
                    &NoLiveSandboxesUnderTest,
                    &pass,
                )
                .await
                .expect("collect");

                assert_eq!(report.cited_layers, 1);
                assert_eq!(report.cited_bytes, 64);
                assert_eq!(report.orphan_layers, 1);
                assert_eq!(report.orphan_bytes, 128);
                assert_eq!(report.deleted_layers, 0);
                assert_eq!(report.reclaimed_bytes, 0);
                assert!(stored_layer_keys(repository.as_ref())
                    .await
                    .contains(&cited));
            }

            #[tokio::test]
            async fn an_orphan_older_than_the_grace_leaves_in_enforce() {
                let (repository, temp) = repository().await;
                let orphan = digest("stale-orphan");
                store_layer(repository.as_ref(), &orphan, 256)
                    .await
                    .expect("store orphan");
                let pass = age_or_shrink_grace(&temp, &orphan, ManagedLayerCollectMode::Enforce);

                let report = collect_managed_layers(
                    repository.as_ref(),
                    &NoLiveSandboxesUnderTest,
                    &pass,
                )
                .await
                .expect("collect");

                assert_eq!(report.deleted_layers, 1);
                assert_eq!(report.reclaimed_bytes, 256);
                assert!(stored_layer_keys(repository.as_ref()).await.is_empty());
            }

            #[tokio::test]
            async fn an_orphan_inside_the_grace_stays() {
                let (repository, _temp) = repository().await;
                let young = digest("young-orphan");
                store_layer(repository.as_ref(), &young, 512)
                    .await
                    .expect("store orphan");

                let report = collect_managed_layers(
                    repository.as_ref(),
                    &NoLiveSandboxesUnderTest,
                    &request(ManagedLayerCollectMode::Enforce),
                )
                .await
                .expect("collect");

                assert_eq!(report.unexpired_layers, 1);
                assert_eq!(report.orphan_layers, 0);
                assert_eq!(report.deleted_layers, 0);
                assert!(stored_layer_keys(repository.as_ref())
                    .await
                    .contains(&young));
            }

            #[tokio::test]
            async fn report_mode_deletes_nothing() {
                let (repository, temp) = repository().await;
                let stale = digest("report-stale");
                store_layer(repository.as_ref(), &stale, 1024)
                    .await
                    .expect("store orphan");
                let pass = age_or_shrink_grace(&temp, &stale, ManagedLayerCollectMode::Report);

                let report = collect_managed_layers(
                    repository.as_ref(),
                    &NoLiveSandboxesUnderTest,
                    &pass,
                )
                .await
                .expect("collect");

                assert_eq!(report.orphan_layers, 1);
                assert_eq!(report.orphan_bytes, 1024);
                assert_eq!(report.deleted_layers, 0);
                assert_eq!(report.reclaimed_bytes, 0);
                // The backend still holds it: the report must not have deleted.
                assert!(stored_layer_keys(repository.as_ref()).await.contains(&stale));
            }

            #[tokio::test]
            async fn a_layer_cited_only_by_a_published_image_stays() {
                let (repository, temp) = repository().await;
                let image_layer = digest("image-layer");
                store_layer(repository.as_ref(), &image_layer, 2048)
                    .await
                    .expect("store image layer");
                store_image(repository.as_ref(), &[&image_layer]).await;
                let pass = age_or_shrink_grace(&temp, &image_layer, ManagedLayerCollectMode::Enforce);

                let report = collect_managed_layers(
                    repository.as_ref(),
                    &NoLiveSandboxesUnderTest,
                    &pass,
                )
                .await
                .expect("collect");

                assert_eq!(report.cited_layers, 1);
                assert_eq!(report.deleted_layers, 0);
                assert!(stored_layer_keys(repository.as_ref())
                    .await
                    .contains(&image_layer));
            }

            #[tokio::test]
            async fn a_layer_cited_by_a_volume_record_stays() {
                let (repository, temp) = repository().await;
                let volume_layer = digest("volume-layer");
                store_layer(repository.as_ref(), &volume_layer, 4096)
                    .await
                    .expect("store volume layer");
                store_volume_record(repository.as_ref(), &[&volume_layer]).await;
                let pass =
                    age_or_shrink_grace(&temp, &volume_layer, ManagedLayerCollectMode::Enforce);

                let report = collect_managed_layers(
                    repository.as_ref(),
                    &NoLiveSandboxesUnderTest,
                    &pass,
                )
                .await
                .expect("collect");

                assert_eq!(report.cited_layers, 1);
                assert_eq!(report.deleted_layers, 0);
                assert!(stored_layer_keys(repository.as_ref())
                    .await
                    .contains(&volume_layer));
            }

            #[tokio::test]
            async fn an_object_whose_name_is_not_a_digest_never_leaves() {
                let (repository, temp) = repository().await;
                write_unreadable_layer(repository.as_ref(), &$root(&temp)).await;

                let listed = repository.list_managed_layers().await.expect("list");
                let unreadable = listed
                    .iter()
                    .filter(|object: &&ManagedLayerObject| object.layer_digest.is_none())
                    .count();
                assert_eq!(
                    unreadable, 1,
                    "an object whose name is not a digest must be listed as unreadable: {listed:?}"
                );

                let report = collect_managed_layers(
                    repository.as_ref(),
                    &NoLiveSandboxesUnderTest,
                    &ManagedLayerCollectRequest {
                        mode: ManagedLayerCollectMode::Enforce,
                        grace: Duration::ZERO,
                    },
                )
                .await
                .expect("collect");

                assert_eq!(report.unreadable_layers, 1);
                assert_eq!(report.deleted_layers, 0);
                assert_eq!(
                    repository
                        .list_managed_layers()
                        .await
                        .expect("list")
                        .iter()
                        .filter(|object| object.layer_digest.is_none())
                        .count(),
                    1
                );
            }

            #[tokio::test]
            async fn a_second_pass_does_not_start_while_the_first_holds_the_lock() {
                let (repository, _temp) = repository().await;
                let lock = repository
                    .acquire_maintenance_lock("managed-layer-collect")
                    .await
                    .expect("acquire")
                    .expect("free lock");
                let blocked = collect_managed_layers(
                    repository.as_ref(),
                    &NoLiveSandboxesUnderTest,
                    &request(ManagedLayerCollectMode::Report),
                )
                .await;
                assert!(
                    matches!(blocked, Err(ManagedLayerCollectorError::Locked)),
                    "a pass must not run while another holds the lock: {blocked:?}"
                );
                repository
                    .release_maintenance_lock(lock)
                    .await
                    .expect("release");
                let after = collect_managed_layers(
                    repository.as_ref(),
                    &NoLiveSandboxesUnderTest,
                    &request(ManagedLayerCollectMode::Report),
                )
                .await;
                assert!(after.is_ok(), "the lock must be free again: {after:?}");
            }

            #[tokio::test]
            async fn a_live_sandbox_source_that_is_gone_downgrades_the_pass_to_a_report() {
                let (repository, temp) = repository().await;
                let orphan = digest("downgraded-orphan");
                store_layer(repository.as_ref(), &orphan, 8192)
                    .await
                    .expect("store orphan");
                let pass = age_or_shrink_grace(&temp, &orphan, ManagedLayerCollectMode::Enforce);
                let missing = crate::types::SnapshotId::generate().to_string();

                let report = collect_managed_layers(
                    repository.as_ref(),
                    &LiveSandboxesUnderTest(vec![missing]),
                    &pass,
                )
                .await
                .expect("collect");

                assert!(
                    report.downgraded_reason.is_some(),
                    "a live sandbox whose source is gone must downgrade the pass"
                );
                assert_eq!(report.deleted_layers, 0);
                assert!(stored_layer_keys(repository.as_ref()).await.contains(&orphan));
            }

            #[tokio::test]
            async fn a_live_sandbox_keeps_its_source_layers() {
                let (repository, temp) = repository().await;
                let live_layer = digest("live-layer");
                store_layer(repository.as_ref(), &live_layer, 16384)
                    .await
                    .expect("store live layer");
                store_snapshot_record(repository.as_ref(), &[&live_layer]).await;
                let pass = age_or_shrink_grace(&temp, &live_layer, ManagedLayerCollectMode::Enforce);
                let records = repository
                    .read_documents(crate::repository::DocumentCollection::SnapshotRecords)
                    .await
                    .expect("read records");
                let origin = records.first().expect("one record").0.clone();

                let report = collect_managed_layers(
                    repository.as_ref(),
                    &LiveSandboxesUnderTest(vec![origin]),
                    &pass,
                )
                .await
                .expect("collect");

                assert!(report.downgraded_reason.is_none());
                assert_eq!(report.deleted_layers, 0);
                assert!(stored_layer_keys(repository.as_ref())
                    .await
                    .contains(&live_layer));
            }

            #[tokio::test]
            async fn the_pass_releases_its_lock_so_the_next_one_runs() {
                let (repository, _temp) = repository().await;
                collect_managed_layers(
                    repository.as_ref(),
                    &NoLiveSandboxesUnderTest,
                    &request(ManagedLayerCollectMode::Report),
                )
                .await
                .expect("first pass");
                collect_managed_layers(
                    repository.as_ref(),
                    &NoLiveSandboxesUnderTest,
                    &request(ManagedLayerCollectMode::Report),
                )
                .await
                .expect("a finished pass must not keep the lock");
            }
        }
    };
}

behavior_table!(
    posix,
    |root: &std::path::Path| -> Box<dyn Repository> {
        Box::new(PosixFsRepository::new(root.join("repository")))
    },
    |temp: &tempfile::TempDir| temp.path().join("repository"),
    true
);
