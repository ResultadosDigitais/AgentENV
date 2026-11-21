//! The managed-layer collector's behavior table.
//!
//! Every scenario is written once here and run against every backend, so a
//! backend cannot encode its own version of the rule. POSIX runs it at the
//! bottom of this file; the OSS backend runs the same [`behavior_table!`] macro
//! from `crates/e2e-tests`, which is where MinIO lives.
//!
//! This module is compiled outside `cfg(test)` on purpose, the same way
//! `snapshot::mock` is: an integration test in another crate can only see it
//! then.

use std::time::Duration;

use async_trait::async_trait;

use super::{LiveSandboxes, ManagedLayerCollectMode, ManagedLayerCollectRequest};
use crate::image::{ImageCatalog, PublishedImage};
use crate::repository::{BlobBody, BlobKey, DocumentKey, LayerRole, Repository, RepositoryResult};
use crate::snapshot::{CommandContext, SnapshotRecord, SnapshotRuntimeVersions};
use crate::types::{ImageConfigs, OverlaybdLayerRef, SandboxResources};
use crate::volume::{VolumeRecord, VolumeStatus};

/// A grace far longer than any test takes, so a layer written now is always
/// inside it unless a scenario says otherwise.
pub const GRACE: Duration = Duration::from_secs(3600);

pub fn digest(seed: &str) -> String {
    crate::digest::sha256_digest(seed.as_bytes())
}

pub fn layer(digest: &str) -> OverlaybdLayerRef {
    OverlaybdLayerRef::Managed(crate::types::ManagedLayer {
        digest: digest.to_owned(),
        size: 4096,
        uuid: None,
    })
}

pub fn request(mode: ManagedLayerCollectMode) -> ManagedLayerCollectRequest {
    ManagedLayerCollectRequest {
        mode,
        grace: GRACE,
        report_token: None,
    }
}

/// Stores one layer and the snapshot record that cites it.
///
/// A clean report cites at least one layer, so every case that wants a receipt
/// able to authorize an enforce starts here. Returns the cited digest.
pub async fn store_cited_layer<R: Repository + ?Sized>(repository: &R) -> String {
    let cited = digest("cited");
    store_layer(repository, &cited, 64)
        .await
        .expect("store cited layer");
    store_snapshot_record(repository, &[&cited]).await;
    cited
}

/// A token shaped exactly like the one a report pass mints.
pub fn fresh_report_token() -> String {
    uuid::Uuid::now_v7().hyphenated().to_string()
}

/// Writes a report receipt as a report pass would, dated by the caller.
///
/// Test scaffolding: a case needs a receipt that expired, and only the server
/// issues real ones. The token is fresh on every call.
pub async fn write_report_receipt_at(
    repository: &dyn Repository,
    issued_at_unix_ms: u64,
) -> String {
    let token = fresh_report_token();
    super::managed_layers::persist_receipt_for_test(repository, &token, issued_at_unix_ms)
        .await
        .expect("persist report receipt");
    token
}

/// A node with no live sandbox.
pub struct NoLiveSandboxesUnderTest;

#[async_trait]
impl LiveSandboxes for NoLiveSandboxesUnderTest {
    async fn live_snapshot_origins(&self) -> anyhow::Result<Vec<String>> {
        Ok(Vec::new())
    }
}

/// A node whose live sandboxes all read the given snapshot IDs.
pub struct LiveSandboxesUnderTest(pub Vec<String>);

#[async_trait]
impl LiveSandboxes for LiveSandboxesUnderTest {
    async fn live_snapshot_origins(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.0.clone())
    }
}

/// The gateway's aggregated list, as the cluster reader consumes it.
///
/// `Ok` is a complete fan-out over every node; `Err` stands for any node that
/// failed, could not be reached, or a page that did not arrive.
pub struct GatewaySandboxListUnderTest(pub Result<Vec<String>, String>);

#[async_trait]
impl super::ClusterSandboxList for GatewaySandboxListUnderTest {
    async fn list_snapshot_origins(&self) -> anyhow::Result<Vec<String>> {
        match &self.0 {
            Ok(origins) => Ok(origins.clone()),
            Err(message) => Err(anyhow::anyhow!("{message}")),
        }
    }
}

/// The cluster reader the node runs: `gateway: None` is the unconfigured
/// deployment, where only the local list exists.
pub fn cluster_live(
    gateway: Option<Result<Vec<String>, String>>,
    local: Vec<String>,
) -> super::ClusterLiveSandboxes {
    super::ClusterLiveSandboxes::new(
        std::sync::Arc::new(LiveSandboxesUnderTest(local)) as std::sync::Arc<dyn LiveSandboxes>,
        gateway.map(|gateway| {
            std::sync::Arc::new(GatewaySandboxListUnderTest(gateway))
                as std::sync::Arc<dyn super::ClusterSandboxList>
        }),
    )
}

/// Stores one managed layer of `bytes` bytes straight through the blob
/// primitives, which is exactly what a publication does before it writes its
/// record.
pub async fn store_layer<R: Repository + ?Sized>(
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

/// The digests of every managed layer the backend currently stores.
pub async fn stored_layer_keys<R: Repository + ?Sized>(
    repository: &R,
) -> std::collections::BTreeSet<String> {
    repository
        .list_managed_layers()
        .await
        .expect("list managed layers")
        .into_iter()
        .filter_map(|object| object.layer_digest)
        .collect()
}

/// Writes a committed snapshot record whose rootfs cites `layers`.
pub async fn store_snapshot_record<R: Repository + ?Sized>(repository: &R, layers: &[&str]) {
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
        committed: Some(crate::snapshot::CommittedSnapshot {
            context: CommandContext::default(),
            startup: None,
            runtime_versions: SnapshotRuntimeVersions::new(
                "kernel".to_owned(),
                "firecracker".to_owned(),
                "envd".to_owned(),
                "0.1.0".to_owned(),
            ),
            virtualization_mode: Default::default(),
            image_configs: ImageConfigs::new(),
            rootfs_layers: layers.iter().map(|digest| layer(digest)).collect(),
            attached_drives: Vec::new(),
            volume_snapshots: Vec::new(),
            memory_layers: Vec::new(),
            disk_publications: Vec::new(),
            custom_extension_params: None,
            memory_startup: None,
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
pub async fn store_volume_record<R: Repository + ?Sized>(repository: &R, layers: &[&str]) {
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
pub async fn store_image<R: Repository + ?Sized>(repository: &R, layers: &[&str]) {
    let image = PublishedImage::new(
        "amd64".to_owned(),
        layers.iter().map(|digest| layer(digest)).collect(),
        serde_json::json!({ "architecture": "amd64", "os": "linux" }),
    );
    repository.put_image(&image).await.expect("publish image");
}

/// Makes a POSIX-stored layer look older than any grace.
///
/// The pass reads the backend's own `last_modified`, so this reaches the file
/// the backend stored. A backend whose storage is not on this filesystem cannot
/// be aged this way and asks for a zero grace instead.
pub fn age_layer(root: &std::path::Path, digest: &str) {
    let path = root.join(
        BlobKey::Layer {
            digest: digest.to_owned(),
            role: LayerRole::Rootfs,
        }
        .posix_path(),
    );
    filetime_set(
        &path,
        std::time::SystemTime::now() - Duration::from_secs(24 * 3600),
    );
}

/// Writes one object under `managed-layers/` whose name does not translate back
/// to a digest.
///
/// It cannot go through `put_blob`, which only writes digest-shaped names: such
/// an object is exactly what an older release, or a hand edit, leaves behind.
///
/// It goes through the backend, not the filesystem, so the case means the same
/// thing on every backend; aging is the caller's, and only POSIX storage can be
/// aged.
pub async fn write_unreadable_layer(repository: &dyn Repository) {
    let key = format!("{}/not-a-digest.overlaybd.commit", BlobKey::LAYERS_DIR);
    repository
        .put_managed_layer_object(&key, &[0u8; 32])
        .await
        .expect("write unreadable layer");
}

pub fn filetime_set(path: &std::path::Path, at: std::time::SystemTime) {
    // `filetime` is not a dependency, and the standard library cannot set a
    // file's mtime; `utimensat` is one call. Keeping it here avoids adding a
    // crate for a test-only helper.
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).expect("path");
    let seconds = at
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after epoch")
        .as_secs() as libc::time_t;
    let times = [
        libc::timespec {
            tv_sec: seconds,
            tv_nsec: 0,
        },
        libc::timespec {
            tv_sec: seconds,
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
/// `repository` is an `async` expression (usually a call to a helper the caller
/// defines) that yields `Arc<dyn Repository>` over the temporary directory it
/// is given; `root` says where that backend stores its files; `age` says
/// whether a scenario can age a layer there. With `age` false the table asks
/// for a zero grace, so "older than the grace" still holds for anything written
/// before the pass.
///
/// The expansion needs the caller crate to have `tokio` (with `macros`) and
/// `tempfile` available, which every test crate here does.
#[macro_export]
macro_rules! behavior_table {
    ($name:ident, $repository:expr, $root:expr, $age:expr) => {
        mod $name {
            // The backend builder this invocation names is defined next to the
            // invocation, one level up.
            use super::*;
            use $crate::maintenance::behavior_table::{
                age_layer, cluster_live, digest, filetime_set, fresh_report_token, request,
                store_cited_layer, store_image, store_layer, store_snapshot_record,
                store_volume_record, stored_layer_keys, write_report_receipt_at,
                write_unreadable_layer, LiveSandboxesUnderTest, NoLiveSandboxesUnderTest,
            };
            use $crate::maintenance::{
                collect_managed_layers, LiveSandboxes, ManagedLayerCollectMode,
                ManagedLayerCollectRequest, ManagedLayerCollectorError,
            };
            use $crate::repository::{
                DocumentCollection, DocumentKey, ManagedLayerObject, Repository,
            };

            /// Builds this table's backend. It lives inside the generated
            /// module so one module can hold several tables.
            async fn __behavior_table_constructor(
                root: &std::path::Path,
            ) -> std::sync::Arc<dyn Repository> {
                $repository(root).await
            }

            async fn repository() -> (std::sync::Arc<dyn Repository>, tempfile::TempDir) {
                let temp = tempfile::TempDir::new().expect("tempdir");
                let repository = __behavior_table_constructor(temp.path()).await;
                (repository, temp)
            }

            /// Makes the layer `digest` old enough to be collected, or asks for
            /// a zero grace when this backend's storage cannot be aged.
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
                        grace: std::time::Duration::ZERO,
                        report_token: None,
                    }
                }
            }

            /// Runs a report pass and returns the receipt token it issued.
            ///
            /// Every enforce case starts here: the receipt, not this process, is
            /// what authorizes a deletion.
            async fn report_and_token(
                repository: &std::sync::Arc<dyn Repository>,
                live: &dyn LiveSandboxes,
                pass: ManagedLayerCollectRequest,
            ) -> String {
                let report = collect_managed_layers(repository.as_ref(), live, &pass)
                    .await
                    .expect("report pass");
                report
                    .report_token
                    .expect("a report pass must return its receipt token")
            }

            /// The same request, carrying the token of the report it follows.
            fn with_receipt(
                mut pass: ManagedLayerCollectRequest,
                token: &str,
            ) -> ManagedLayerCollectRequest {
                pass.report_token = Some(token.to_owned());
                pass
            }

            /// A report pass over a clean bucket that cites one layer.
            async fn clean_report_token(repository: &std::sync::Arc<dyn Repository>) -> String {
                store_cited_layer(repository.as_ref()).await;
                report_and_token(
                    repository,
                    &NoLiveSandboxesUnderTest,
                    request(ManagedLayerCollectMode::Report),
                )
                .await
            }

            fn assert_receipt_rejected<T: std::fmt::Debug>(
                result: Result<T, ManagedLayerCollectorError>,
                case: &str,
            ) {
                assert!(
                    matches!(
                        &result,
                        Err(ManagedLayerCollectorError::ReportReceiptRejected(_))
                    ),
                    "{case}: an enforce without a valid receipt must be refused, got {result:?}"
                );
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

                let report =
                    collect_managed_layers(repository.as_ref(), &NoLiveSandboxesUnderTest, &pass)
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
                let token = clean_report_token(&repository).await;
                let orphan = digest("stale-orphan");
                store_layer(repository.as_ref(), &orphan, 256)
                    .await
                    .expect("store orphan");
                let pass = with_receipt(
                    age_or_shrink_grace(&temp, &orphan, ManagedLayerCollectMode::Enforce),
                    &token,
                );

                let report =
                    collect_managed_layers(repository.as_ref(), &NoLiveSandboxesUnderTest, &pass)
                        .await
                        .expect("collect");

                assert_eq!(report.deleted_layers, 1);
                assert_eq!(report.reclaimed_bytes, 256);
                let remaining = stored_layer_keys(repository.as_ref()).await;
                assert!(!remaining.contains(&orphan), "the stale orphan must leave");
                assert!(
                    remaining.contains(&digest("cited")),
                    "the cited layer must stay: {remaining:?}"
                );
            }

            #[tokio::test]
            async fn an_orphan_inside_the_grace_stays() {
                let (repository, _temp) = repository().await;
                let token = clean_report_token(&repository).await;
                let young = digest("young-orphan");
                store_layer(repository.as_ref(), &young, 512)
                    .await
                    .expect("store orphan");
                let pass = with_receipt(request(ManagedLayerCollectMode::Enforce), &token);

                let report =
                    collect_managed_layers(repository.as_ref(), &NoLiveSandboxesUnderTest, &pass)
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

                let report =
                    collect_managed_layers(repository.as_ref(), &NoLiveSandboxesUnderTest, &pass)
                        .await
                        .expect("collect");

                assert_eq!(report.orphan_layers, 1);
                assert_eq!(report.orphan_bytes, 1024);
                assert_eq!(report.deleted_layers, 0);
                assert_eq!(report.reclaimed_bytes, 0);
                // The backend still holds it: the report must not have deleted.
                assert!(stored_layer_keys(repository.as_ref())
                    .await
                    .contains(&stale));
            }

            #[tokio::test]
            async fn a_layer_cited_only_by_a_published_image_stays() {
                let (repository, temp) = repository().await;
                let image_layer = digest("image-layer");
                store_layer(repository.as_ref(), &image_layer, 2048)
                    .await
                    .expect("store image layer");
                store_image(repository.as_ref(), &[&image_layer]).await;
                let token = report_and_token(
                    &repository,
                    &NoLiveSandboxesUnderTest,
                    request(ManagedLayerCollectMode::Report),
                )
                .await;
                let pass = with_receipt(
                    age_or_shrink_grace(&temp, &image_layer, ManagedLayerCollectMode::Enforce),
                    &token,
                );

                let report =
                    collect_managed_layers(repository.as_ref(), &NoLiveSandboxesUnderTest, &pass)
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
                let token = report_and_token(
                    &repository,
                    &NoLiveSandboxesUnderTest,
                    request(ManagedLayerCollectMode::Report),
                )
                .await;
                let pass = with_receipt(
                    age_or_shrink_grace(&temp, &volume_layer, ManagedLayerCollectMode::Enforce),
                    &token,
                );

                let report =
                    collect_managed_layers(repository.as_ref(), &NoLiveSandboxesUnderTest, &pass)
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
                // A stray object that appears after a clean report must still be
                // left alone by the enforce the report authorized.
                let token = clean_report_token(&repository).await;
                write_unreadable_layer(repository.as_ref()).await;
                // POSIX storage can be aged; an object store reports its own
                // clock, so these cases ask for a zero grace instead.
                if $age {
                    filetime_set(
                        &$root(&temp).join(format!(
                            "{}/not-a-digest.overlaybd.commit",
                            $crate::repository::BlobKey::LAYERS_DIR
                        )),
                        std::time::SystemTime::now() - std::time::Duration::from_secs(24 * 3600),
                    );
                }

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
                    &with_receipt(
                        ManagedLayerCollectRequest {
                            mode: ManagedLayerCollectMode::Enforce,
                            grace: std::time::Duration::ZERO,
                            report_token: None,
                        },
                        &token,
                    ),
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
            async fn a_report_pass_leaves_one_receipt_outside_the_layers_and_an_enforce_spends_it()
            {
                let (repository, temp) = repository().await;
                store_cited_layer(repository.as_ref()).await;
                let orphan = digest("receipt-orphan");
                store_layer(repository.as_ref(), &orphan, 128)
                    .await
                    .expect("store orphan");
                let pass = age_or_shrink_grace(&temp, &orphan, ManagedLayerCollectMode::Report);
                let before = stored_layer_keys(repository.as_ref()).await;

                let report =
                    collect_managed_layers(repository.as_ref(), &NoLiveSandboxesUnderTest, &pass)
                        .await
                        .expect("report");
                let token = report.report_token.expect("a report must return its token");
                assert_eq!(report.deleted_layers, 0, "a report never deletes");
                assert_eq!(
                    stored_layer_keys(repository.as_ref()).await,
                    before,
                    "the receipt must not be stored under managed-layers/"
                );

                // The enforce lands after the report and still spends the
                // receipt; nothing about the process that ran the report matters.
                let enforce = with_receipt(
                    age_or_shrink_grace(&temp, &orphan, ManagedLayerCollectMode::Enforce),
                    &token,
                );
                let swept = collect_managed_layers(
                    repository.as_ref(),
                    &NoLiveSandboxesUnderTest,
                    &enforce,
                )
                .await
                .expect("enforce");
                assert_eq!(swept.deleted_layers, 1);
                assert!(!stored_layer_keys(repository.as_ref())
                    .await
                    .contains(&orphan));

                // One use only: the same token is refused the second time. The
                // orphan is already gone, so the request needs no aging.
                let again = collect_managed_layers(
                    repository.as_ref(),
                    &NoLiveSandboxesUnderTest,
                    &with_receipt(request(ManagedLayerCollectMode::Enforce), &token),
                )
                .await;
                assert_receipt_rejected(again, "a consumed receipt");
            }

            #[tokio::test]
            async fn an_enforce_without_a_report_token_is_refused() {
                let (repository, temp) = repository().await;
                let orphan = digest("no-receipt-orphan");
                store_layer(repository.as_ref(), &orphan, 256)
                    .await
                    .expect("store orphan");
                let pass = age_or_shrink_grace(&temp, &orphan, ManagedLayerCollectMode::Enforce);

                let refused =
                    collect_managed_layers(repository.as_ref(), &NoLiveSandboxesUnderTest, &pass)
                        .await;

                assert_receipt_rejected(refused, "no report token");
                assert!(
                    stored_layer_keys(repository.as_ref())
                        .await
                        .contains(&orphan),
                    "a refused enforce must delete nothing"
                );
            }

            #[tokio::test]
            async fn an_enforce_with_a_token_this_server_never_issued_is_refused() {
                let (repository, temp) = repository().await;
                let orphan = digest("unknown-receipt-orphan");
                store_layer(repository.as_ref(), &orphan, 256)
                    .await
                    .expect("store orphan");
                let unknown = fresh_report_token();
                let pass = with_receipt(
                    age_or_shrink_grace(&temp, &orphan, ManagedLayerCollectMode::Enforce),
                    &unknown,
                );

                let refused =
                    collect_managed_layers(repository.as_ref(), &NoLiveSandboxesUnderTest, &pass)
                        .await;

                assert_receipt_rejected(refused, "an unknown token");
                assert!(stored_layer_keys(repository.as_ref())
                    .await
                    .contains(&orphan));
            }

            #[tokio::test]
            async fn an_enforce_with_an_expired_receipt_is_refused() {
                let (repository, temp) = repository().await;
                let orphan = digest("expired-receipt-orphan");
                store_layer(repository.as_ref(), &orphan, 256)
                    .await
                    .expect("store orphan");
                // Issued at the Unix epoch: far past any short TTL.
                let expired = write_report_receipt_at(repository.as_ref(), 0).await;
                let pass = with_receipt(
                    age_or_shrink_grace(&temp, &orphan, ManagedLayerCollectMode::Enforce),
                    &expired,
                );

                let refused =
                    collect_managed_layers(repository.as_ref(), &NoLiveSandboxesUnderTest, &pass)
                        .await;

                assert_receipt_rejected(refused, "an expired receipt");
                assert!(stored_layer_keys(repository.as_ref())
                    .await
                    .contains(&orphan));
            }

            #[tokio::test]
            async fn a_report_that_was_not_clean_does_not_authorize_an_enforce() {
                let (repository, temp) = repository().await;
                let orphan = digest("unclean-report-orphan");
                store_layer(repository.as_ref(), &orphan, 256)
                    .await
                    .expect("store orphan");
                write_unreadable_layer(repository.as_ref()).await;
                let report_pass =
                    age_or_shrink_grace(&temp, &orphan, ManagedLayerCollectMode::Report);

                let report = collect_managed_layers(
                    repository.as_ref(),
                    &NoLiveSandboxesUnderTest,
                    &report_pass,
                )
                .await
                .expect("report");
                assert_eq!(report.unreadable_layers, 1);
                let token = report.report_token.expect("a report returns a token");
                let pass = with_receipt(
                    age_or_shrink_grace(&temp, &orphan, ManagedLayerCollectMode::Enforce),
                    &token,
                );

                let refused =
                    collect_managed_layers(repository.as_ref(), &NoLiveSandboxesUnderTest, &pass)
                        .await;

                assert_receipt_rejected(refused, "an unclean report");
                assert!(stored_layer_keys(repository.as_ref())
                    .await
                    .contains(&orphan));
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
                // A clean report authorizes the enforce; the downgrade happens in
                // the enforce itself, when a source it must keep cited is gone.
                let token = clean_report_token(&repository).await;
                let orphan = digest("downgraded-orphan");
                store_layer(repository.as_ref(), &orphan, 8192)
                    .await
                    .expect("store orphan");
                let pass = with_receipt(
                    age_or_shrink_grace(&temp, &orphan, ManagedLayerCollectMode::Enforce),
                    &token,
                );
                let missing = $crate::types::SnapshotId::generate().to_string();

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
                assert!(stored_layer_keys(repository.as_ref())
                    .await
                    .contains(&orphan));
            }

            #[tokio::test]
            async fn a_layer_cited_only_by_a_paused_sandbox_on_another_node_never_leaves() {
                let (repository, temp) = repository().await;
                let live_layer = digest("cross-node-layer");
                store_layer(repository.as_ref(), &live_layer, 32768)
                    .await
                    .expect("store live layer");
                store_snapshot_record(repository.as_ref(), &[&live_layer]).await;
                // The report runs while the golden's record still exists, so it is
                // clean; the deletion that strands the layer happens afterwards.
                let token = report_and_token(
                    &repository,
                    &NoLiveSandboxesUnderTest,
                    request(ManagedLayerCollectMode::Report),
                )
                .await;
                let pass = with_receipt(
                    age_or_shrink_grace(&temp, &live_layer, ManagedLayerCollectMode::Enforce),
                    &token,
                );
                let records = repository
                    .read_documents(DocumentCollection::SnapshotRecords)
                    .await
                    .expect("read records");
                let (origin, _) = records.first().expect("one record").clone();
                // This is the staging shape: retention already removed the
                // golden's record, so nothing in the catalog cites the layer —
                // yet a sandbox paused on another node still resumes from it.
                repository
                    .delete_document(DocumentKey::SnapshotRecord(
                        $crate::types::SnapshotId::parse(&origin).expect("snapshot id"),
                    ))
                    .await
                    .expect("drop the record");

                // This node holds nothing: the sandbox is paused elsewhere, so
                // only the gateway's aggregated list names it.
                let live = cluster_live(Some(Ok(vec![origin])), Vec::new());
                let report = collect_managed_layers(repository.as_ref(), &live, &pass)
                    .await
                    .expect("collect");

                assert!(
                    report.downgraded_reason.is_some(),
                    "a live sandbox whose source is gone must downgrade the pass, \
                     whichever node holds it: {:?}",
                    report.downgraded_reason
                );
                assert_eq!(report.deleted_layers, 0);
                assert!(
                    stored_layer_keys(repository.as_ref())
                        .await
                        .contains(&live_layer),
                    "the layer a paused sandbox on another node reads must stay"
                );
            }

            #[tokio::test]
            async fn a_fan_out_with_one_node_erroring_downgrades_to_a_report() {
                let (repository, temp) = repository().await;
                let token = clean_report_token(&repository).await;
                let orphan = digest("partial-fan-out-orphan");
                store_layer(repository.as_ref(), &orphan, 4096)
                    .await
                    .expect("store orphan");
                let pass = with_receipt(
                    age_or_shrink_grace(&temp, &orphan, ManagedLayerCollectMode::Enforce),
                    &token,
                );
                // One node of the fan-out failed: the merged list is complete
                // for every other node and silently missing this one, so it
                // cannot be treated as the cluster.
                let live = cluster_live(
                    Some(Err(
                        "node agentenv-node-local-ssd-xb9bd list failed".to_owned()
                    )),
                    Vec::new(),
                );

                let report = collect_managed_layers(repository.as_ref(), &live, &pass)
                    .await
                    .expect("collect");

                assert!(
                    report.downgraded_reason.is_some(),
                    "a partial fan-out must downgrade the pass to a report"
                );
                assert_eq!(report.deleted_layers, 0);
                assert!(stored_layer_keys(repository.as_ref())
                    .await
                    .contains(&orphan));
            }

            #[tokio::test]
            async fn an_unconfigured_gateway_downgrades_to_a_report() {
                let (repository, temp) = repository().await;
                let token = clean_report_token(&repository).await;
                let orphan = digest("no-gateway-orphan");
                store_layer(repository.as_ref(), &orphan, 2048)
                    .await
                    .expect("store orphan");
                let pass = with_receipt(
                    age_or_shrink_grace(&temp, &orphan, ManagedLayerCollectMode::Enforce),
                    &token,
                );
                // Without a gateway this node cannot see the sandboxes other
                // nodes hold, so it cannot prove any layer uncited.
                let live = cluster_live(None, Vec::new());

                let report = collect_managed_layers(repository.as_ref(), &live, &pass)
                    .await
                    .expect("collect");

                assert!(
                    report.downgraded_reason.is_some(),
                    "an unconfigured gateway must downgrade the pass to a report"
                );
                assert_eq!(report.deleted_layers, 0);
                assert!(stored_layer_keys(repository.as_ref())
                    .await
                    .contains(&orphan));
            }

            #[tokio::test]
            async fn a_live_sandbox_keeps_its_source_layers() {
                let (repository, temp) = repository().await;
                let live_layer = digest("live-layer");
                store_layer(repository.as_ref(), &live_layer, 16384)
                    .await
                    .expect("store live layer");
                store_snapshot_record(repository.as_ref(), &[&live_layer]).await;
                let records = repository
                    .read_documents(DocumentCollection::SnapshotRecords)
                    .await
                    .expect("read records");
                let origin = records.first().expect("one record").0.clone();
                let token = report_and_token(
                    &repository,
                    &LiveSandboxesUnderTest(vec![origin.clone()]),
                    request(ManagedLayerCollectMode::Report),
                )
                .await;
                let pass = with_receipt(
                    age_or_shrink_grace(&temp, &live_layer, ManagedLayerCollectMode::Enforce),
                    &token,
                );

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

/// Builds a POSIX repository under `root`.
#[cfg(test)]
pub async fn posix_repository(root: &std::path::Path) -> std::sync::Arc<dyn Repository> {
    std::sync::Arc::new(crate::repository::backends::PosixFsRepository::new(
        root.join("repository"),
    ))
}

#[cfg(test)]
behavior_table!(
    posix,
    posix_repository,
    |temp: &tempfile::TempDir| temp.path().join("repository"),
    true
);

/// Builds an object-store repository over the GCS wire protocol, backed by the
/// in-process fake rather than a bucket.
///
/// The fake's store outlives each case and its listing is keyed by object name,
/// so every case works under its own prefix, the way the MinIO suite does.
#[cfg(test)]
pub async fn gcs_repository(root: &std::path::Path) -> std::sync::Arc<dyn Repository> {
    use crate::repository::backends::oss::fake_gcs::FakeGcs;

    let fake = FakeGcs::start().await;
    let client = crate::repository::backends::oss::OssClient::new_gcs(
        "snapshots".to_owned(),
        fake.endpoint.clone(),
        gcs_prefix(root),
        object_store_operator::GcsAuth::Anonymous,
    );
    std::sync::Arc::new(crate::repository::backends::OssRepository::new(
        std::sync::Arc::new(client),
        crate::cfg::SnapshotImageStoragePolicy::ObjectStorage,
        &crate::cfg::SnapshotPublishCompressionConfig::default(),
    ))
}

#[cfg(test)]
fn gcs_prefix(root: &std::path::Path) -> String {
    // The tempdir name is unique per case, which makes the prefix unique.
    let name = root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("case");
    format!("managed-layer-collect/{name}")
}

// GCS reports `last_modified` for every object, so the cases can age nothing
// but can still ask for a zero grace.
#[cfg(test)]
behavior_table!(
    gcs,
    gcs_repository,
    |temp: &tempfile::TempDir| temp.path().join("never-aged"),
    false
);
