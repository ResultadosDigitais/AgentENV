//! Collecting managed layers that no catalog record cites.
//!
//! A managed layer is content addressed and shared, so deleting a snapshot
//! removes only that snapshot's own files: every layer stays for whoever else
//! may use it. Nothing used to notice a layer that *nobody* used any more, so a
//! deleted record left its layers in the bucket for good. This pass is that
//! collector.
//!
//! # What cites a layer
//!
//! A layer must be kept when any of these cites it:
//!
//! - a **snapshot record** under `catalog/records/`: its committed rootfs,
//!   attached-drive, memory and volume-snapshot layers;
//! - a **volume record** under `volumes/records/`: its backing layers, which is
//!   why a volume outlives the snapshots that used it;
//! - a **published image description** under `images/descriptions/`: an image
//!   owns its layers, so collecting one would corrupt the image;
//! - a **live sandbox** the node knows about: its source snapshot's layers stay,
//!   and a source that is already gone from the catalog downgrades the whole
//!   pass to a report, because the substrate cannot then prove which layers the
//!   sandbox may still read.
//!
//! That set is closed. `BlobKey::Layer` is written only by snapshot publication
//! (`src/snapshot/store.rs`) and volume publication
//! (`src/volume/mod.rs`); image builds reuse the same two stores, and image
//! descriptions only ever reference layers those stores wrote.
//!
//! # The grace period
//!
//! Publication uploads a record's layers *before* it writes the record. Between
//! the two a layer is already stored and no record cites it yet, so a pass that
//! collected it would break the publish in flight. An uncited layer is therefore
//! collected only once it is older than the grace period, which must exceed how
//! long one publication takes from its first layer upload to its record commit.
//!
//! # What the pass does not do
//!
//! It never deletes an object in `managed-layers/` whose name does not translate
//! back to a `sha256:<hex>` digest: the substrate cannot prove what such an
//! object is, so it is reported and left alone.

use std::collections::BTreeSet;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tracing::{info, warn};

use crate::image::{ImageCatalog, PublishedImage};
use crate::repository::{
    DocumentCollection, DocumentKey, MaintenanceLock, ManagedLayerObject, Repository,
    RepositoryError, RepositoryResult,
};

/// The lock one collection pass holds, so two passes never overlap.
const PASS_LOCK: &str = "managed-layer-collect";

/// What one pass does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedLayerCollectMode {
    /// List and count the orphans without deleting anything.
    Report,
    /// Delete the orphans that are older than the grace period.
    Enforce,
}

impl ManagedLayerCollectMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Report => "report",
            Self::Enforce => "enforce",
        }
    }
}

/// One pass's inputs.
#[derive(Clone, Debug)]
pub struct ManagedLayerCollectRequest {
    pub mode: ManagedLayerCollectMode,
    /// How old an uncited layer must be before [`ManagedLayerCollectMode::Enforce`]
    /// may delete it. Must exceed one publication's layer-upload-to-record-commit
    /// time; see the module documentation.
    pub grace: Duration,
}

/// One object the pass could not classify or act on. A single error never stops
/// the pass: the rest of the bucket is still swept, and every failure is
/// reported so an operator sees what was left behind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedLayerCollectFailure {
    pub key: String,
    pub message: String,
}

/// What one pass found and did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ManagedLayerCollectReport {
    pub mode: Option<ManagedLayerCollectMode>,
    /// Distinct layer digests any record or live sandbox cites.
    pub cited_layers: u64,
    /// Bytes of the managed layers those digests name.
    pub cited_bytes: u64,
    /// Uncited layers past the grace period.
    pub orphan_layers: u64,
    /// Bytes of those layers.
    pub orphan_bytes: u64,
    /// Uncited layers still inside the grace period, so they stay.
    pub unexpired_layers: u64,
    /// Objects whose name is not a digest, so they are never deleted.
    pub unreadable_layers: u64,
    /// Layers actually removed. Always zero in report mode.
    pub deleted_layers: u64,
    /// Bytes actually removed. Always zero in report mode.
    pub reclaimed_bytes: u64,
    /// Set when the pass listed and counted but deleted nothing, even in enforce
    /// mode. A live sandbox whose source is gone sets it, and so does a request
    /// that asked for enforce without a preceding report.
    pub downgraded_reason: Option<String>,
    pub failures: Vec<ManagedLayerCollectFailure>,
}

/// Why a pass could not run.
#[derive(Debug, thiserror::Error)]
pub enum ManagedLayerCollectorError {
    #[error("another managed-layer collection pass holds the repository lock")]
    Locked,
    #[error("enforce requires a report pass in this server process first")]
    ReportRequired,
    #[error(transparent)]
    Repository(#[from] RepositoryError),
}

/// What the collector needs from the node it runs on.
///
/// It is a trait so the collector does not depend on `orchestrator`, which
/// depends on every catalog: the node supplies the live sandboxes, and the
/// collector only reads which snapshot each one came from.
#[async_trait]
pub trait LiveSandboxes: Send + Sync {
    /// The snapshot ID of every sandbox the node currently holds, running or
    /// paused. A sandbox resumed from a snapshot keeps reading that snapshot's
    /// layers on demand, so its source must stay cited.
    async fn live_snapshot_origins(&self) -> anyhow::Result<Vec<String>>;
}

/// Runs one collection pass over `repository`.
///
/// `live` answers which snapshots live sandboxes still read. A source that is no
/// longer in the catalog downgrades the pass to a report, because the layers
/// that source named may still be in use and the pass can no longer prove they
/// are not.
pub async fn collect_managed_layers<R, L>(
    repository: &R,
    live: &L,
    request: &ManagedLayerCollectRequest,
) -> Result<ManagedLayerCollectReport, ManagedLayerCollectorError>
where
    R: Repository + ?Sized,
    L: LiveSandboxes + ?Sized,
{
    let lock = repository.acquire_maintenance_lock(PASS_LOCK).await?;
    let Some(lock) = lock else {
        return Err(ManagedLayerCollectorError::Locked);
    };
    let outcome = run_pass(repository, live, request).await;
    release(repository, lock).await;
    outcome.map_err(ManagedLayerCollectorError::from)
}

async fn run_pass<R, L>(
    repository: &R,
    live: &L,
    request: &ManagedLayerCollectRequest,
) -> RepositoryResult<ManagedLayerCollectReport>
where
    R: Repository + ?Sized,
    L: LiveSandboxes + ?Sized,
{
    let started = std::time::Instant::now();
    let mut report = ManagedLayerCollectReport {
        mode: Some(request.mode),
        ..ManagedLayerCollectReport::default()
    };
    let mut cited = cited_layer_digests(repository, live, &mut report).await;

    // A live sandbox reading a source that is gone makes the citation set
    // unknowable: the source's layers may still be read on demand.
    if report.downgraded_reason.is_some() {
        cited.clear();
    }

    let objects = repository.list_managed_layers().await?;
    // Enforce only when the citation set is trustworthy and the request asked
    // for it.
    let enforce =
        request.mode == ManagedLayerCollectMode::Enforce && report.downgraded_reason.is_none();

    let mut to_delete = Vec::new();
    for object in objects {
        let Some(digest) = object.layer_digest.as_deref() else {
            // The name does not translate to a digest, so no record could ever
            // cite it and the substrate cannot prove what it is. Never delete.
            report.unreadable_layers += 1;
            continue;
        };
        if cited.contains(digest) {
            report.cited_layers += 1;
            report.cited_bytes += object.bytes;
            continue;
        }
        if !expired(&object, request.grace) {
            report.unexpired_layers += 1;
            continue;
        }
        report.orphan_layers += 1;
        report.orphan_bytes += object.bytes;
        if enforce {
            to_delete.push(object);
        }
    }

    if enforce {
        for object in to_delete {
            match repository.delete_managed_layer(&object.key).await {
                Ok(()) => {
                    report.deleted_layers += 1;
                    report.reclaimed_bytes += object.bytes;
                }
                // One object that cannot be removed must not stop the sweep.
                Err(error) => report.failures.push(ManagedLayerCollectFailure {
                    key: object.key,
                    message: error.to_string(),
                }),
            }
        }
    }

    let elapsed = started.elapsed();
    record_pass_metrics(&report, elapsed);
    info!(
        mode = request.mode.as_str(),
        elapsed_ms = elapsed.as_millis(),
        cited_layers = report.cited_layers,
        cited_bytes = report.cited_bytes,
        orphan_layers = report.orphan_layers,
        orphan_bytes = report.orphan_bytes,
        unexpired_layers = report.unexpired_layers,
        unreadable_layers = report.unreadable_layers,
        deleted_layers = report.deleted_layers,
        reclaimed_bytes = report.reclaimed_bytes,
        failures = report.failures.len(),
        downgraded = report.downgraded_reason.is_some(),
        "managed layer collection pass finished"
    );
    Ok(report)
}

/// Records one pass, so an operator can see the bucket shrinking without
/// reading the log.
///
/// The gauges carry the pass's own numbers, so the last pass is always what
/// `/metrics` shows. The histograms keep the trend.
fn record_pass_metrics(report: &ManagedLayerCollectReport, elapsed: Duration) {
    let mode = report
        .mode
        .unwrap_or(ManagedLayerCollectMode::Report)
        .as_str();
    metrics::gauge!("agentenv_managed_layer_cited_layers", "mode" => mode)
        .set(report.cited_layers as f64);
    metrics::gauge!("agentenv_managed_layer_cited_bytes", "mode" => mode)
        .set(report.cited_bytes as f64);
    metrics::gauge!("agentenv_managed_layer_orphan_layers", "mode" => mode)
        .set(report.orphan_layers as f64);
    metrics::gauge!("agentenv_managed_layer_orphan_bytes", "mode" => mode)
        .set(report.orphan_bytes as f64);
    metrics::gauge!("agentenv_managed_layer_unexpired_layers", "mode" => mode)
        .set(report.unexpired_layers as f64);
    metrics::gauge!("agentenv_managed_layer_unreadable_layers", "mode" => mode)
        .set(report.unreadable_layers as f64);
    metrics::counter!("agentenv_managed_layer_collect_reclaimed_bytes_total", "mode" => mode)
        .increment(report.reclaimed_bytes);
    metrics::counter!("agentenv_managed_layer_collect_deleted_layers_total", "mode" => mode)
        .increment(report.deleted_layers);
    metrics::histogram!("agentenv_managed_layer_collect_duration_seconds", "mode" => mode)
        .record(elapsed.as_secs_f64());
}

async fn release<R: Repository + ?Sized>(repository: &R, lock: MaintenanceLock) {
    if let Err(error) = repository.release_maintenance_lock(lock).await {
        // The pass's work is done either way; a lock left behind is taken over
        // by the next pass once it is stale.
        warn!(error = %error, "failed to release the managed layer collection lock");
    }
}

/// Whether `object` is old enough to be collected.
///
/// An object whose age the backend does not report is treated as *not* expired:
/// without a timestamp the pass cannot prove it is not a layer a publication in
/// flight just uploaded.
fn expired(object: &ManagedLayerObject, grace: Duration) -> bool {
    object
        .last_modified
        .and_then(|modified| modified.elapsed().ok())
        .is_some_and(|age| age >= grace)
}

/// The digests of every layer any record cites, plus every layer a live
/// sandbox's source cites.
async fn cited_layer_digests<R, L>(
    repository: &R,
    live: &L,
    report: &mut ManagedLayerCollectReport,
) -> BTreeSet<String>
where
    R: Repository + ?Sized,
    L: LiveSandboxes + ?Sized,
{
    let mut cited = BTreeSet::new();
    // A record that cannot be read leaves its layers uncited, so the pass must
    // not delete anything at all: a report is the only safe answer.
    let mut unreadable = Vec::new();

    // A catalog the pass cannot read at all leaves every record it holds
    // uncited, which would make live layers look like orphans. It is recorded
    // as a failure, and the failure downgrades the pass to a report below.
    match repository
        .read_documents(DocumentCollection::SnapshotRecords)
        .await
    {
        Ok(records) => {
            for (id, bytes) in records {
                match SnapshotCitation::decode(&bytes) {
                    Ok(citation) => citation.collect_into(&mut cited),
                    Err(message) => unreadable.push(ManagedLayerCollectFailure {
                        key: format!("{}{id}.json", DocumentCollection::SnapshotRecords.prefix()),
                        message,
                    }),
                }
            }
        }
        Err(error) => unreadable.push(ManagedLayerCollectFailure {
            key: DocumentCollection::SnapshotRecords.prefix().to_owned(),
            message: error.to_string(),
        }),
    }

    match repository
        .read_documents(DocumentCollection::VolumeRecords)
        .await
    {
        Ok(records) => {
            for (id, bytes) in records {
                match VolumeCitation::decode(&bytes) {
                    Ok(citation) => citation.collect_into(&mut cited),
                    Err(message) => unreadable.push(ManagedLayerCollectFailure {
                        key: format!("{}{id}.json", DocumentCollection::VolumeRecords.prefix()),
                        message,
                    }),
                }
            }
        }
        Err(error) => unreadable.push(ManagedLayerCollectFailure {
            key: DocumentCollection::VolumeRecords.prefix().to_owned(),
            message: error.to_string(),
        }),
    }

    match repository.list_images().await {
        Ok(images) => {
            for image in &images {
                collect_image_layers(image, &mut cited);
            }
        }
        Err(error) => unreadable.push(ManagedLayerCollectFailure {
            key: DocumentCollection::ImageDescriptions.prefix().to_owned(),
            message: error.to_string(),
        }),
    }

    if !unreadable.is_empty() {
        // A record the pass cannot read may cite any layer, so nothing can be
        // proven an orphan.
        report.downgraded_reason =
            Some("catalog records could not be read, so no layer can be proven uncited".to_owned());
        report.failures.extend(unreadable);
    }

    match live.live_snapshot_origins().await {
        Ok(origins) => {
            for origin in origins {
                if let Some(layers) = snapshot_layers(repository, &origin, report).await {
                    cited.extend(layers);
                }
            }
        }
        Err(error) => {
            report.downgraded_reason = Some(format!(
                "live sandboxes could not be listed, so a layer in use cannot be ruled out: {error}"
            ));
        }
    }

    cited
}

/// The layers of one live sandbox's source snapshot, or `None` when the source
/// is gone from the catalog.
async fn snapshot_layers<R: Repository + ?Sized>(
    repository: &R,
    origin: &str,
    report: &mut ManagedLayerCollectReport,
) -> Option<BTreeSet<String>> {
    let Some(id) = crate::types::SnapshotId::parse(origin).ok() else {
        // An image-launched sandbox names its image, not a snapshot; it reads
        // layers from a registry or an image description, both of which are
        // cited elsewhere.
        return None;
    };
    let record = match repository
        .get_document(DocumentKey::SnapshotRecord(id.clone()))
        .await
    {
        Ok(record) => record,
        Err(error) => {
            report.downgraded_reason = Some(format!(
                "the source of live sandbox is unreadable ({error}), so its layers cannot be proven unused"
            ));
            return None;
        }
    };
    let Some(bytes) = record else {
        // The PF may have unregistered a golden a paused sandbox still resumes
        // from. Its layers must stay, and the pass cannot know which they are.
        warn!(snapshot_id = %id, "live sandbox source is no longer in the catalog; downgrading the pass to a report");
        report.downgraded_reason = Some(format!(
            "live sandbox reads snapshot '{id}', which is no longer in the catalog"
        ));
        return None;
    };
    match SnapshotCitation::decode(&bytes) {
        Ok(citation) => {
            let mut layers = BTreeSet::new();
            citation.collect_into(&mut layers);
            Some(layers)
        }
        Err(message) => {
            report.downgraded_reason = Some(format!(
                "the source of live sandbox could not be read ({message}), so its layers cannot be proven unused"
            ));
            None
        }
    }
}

fn collect_image_layers(image: &PublishedImage, cited: &mut BTreeSet<String>) {
    for layer in image.layers() {
        if let crate::types::OverlaybdLayerRef::Managed(layer) = layer {
            cited.insert(layer.digest.clone());
        }
    }
}

/// The layer citations of one snapshot record.
///
/// The decode borrows only the fields that name layers, so it keeps working when
/// the rest of the record grows. It mirrors the shape `src/snapshot/types`
/// writes; that module owns the format.
#[derive(Deserialize)]
struct SnapshotCitation {
    #[serde(default)]
    committed: Option<CommittedCitation>,
}

#[derive(Deserialize)]
struct CommittedCitation {
    #[serde(default)]
    rootfs_layers: Vec<LayerCitation>,
    #[serde(default)]
    attached_drives: Vec<AttachedDriveCitation>,
    #[serde(default)]
    memory_layers: Vec<ManagedLayerCitation>,
    #[serde(default)]
    volume_snapshots: Vec<VolumeSnapshotCitation>,
}

#[derive(Deserialize)]
struct AttachedDriveCitation {
    #[serde(default)]
    layers: Vec<LayerCitation>,
}

#[derive(Deserialize)]
struct VolumeSnapshotCitation {
    #[serde(default)]
    layers: Vec<LayerCitation>,
}

/// An `OverlaybdLayerRef`: either a layer this repository stores or one an
/// external registry holds.
#[derive(Deserialize)]
enum LayerCitation {
    Managed(ManagedLayerCitation),
    External(serde::de::IgnoredAny),
}

#[derive(Deserialize)]
struct ManagedLayerCitation {
    digest: String,
}

impl SnapshotCitation {
    fn decode(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|error| format!("parse snapshot record: {error}"))
    }

    fn collect_into(self, cited: &mut BTreeSet<String>) {
        let Some(committed) = self.committed else {
            return;
        };
        collect_refs(committed.rootfs_layers, cited);
        for drive in committed.attached_drives {
            collect_refs(drive.layers, cited);
        }
        for layer in committed.memory_layers {
            cited.insert(layer.digest);
        }
        for volume in committed.volume_snapshots {
            collect_refs(volume.layers, cited);
        }
    }
}

fn collect_refs(layers: Vec<LayerCitation>, cited: &mut BTreeSet<String>) {
    for layer in layers {
        if let LayerCitation::Managed(layer) = layer {
            cited.insert(layer.digest);
        }
    }
}

/// The layer citations of one volume record.
///
/// This mirrors `src/volume/mod.rs`'s record shape for reading only; that module
/// owns the format, and the collector cannot import it because `volume` builds
/// on `snapshot` and the collector must sit above both.
#[derive(Deserialize)]
struct VolumeCitation {
    #[serde(default, rename = "backingLayers")]
    backing_layers: Vec<LayerCitation>,
}

impl VolumeCitation {
    fn decode(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|error| format!("parse volume record: {error}"))
    }

    fn collect_into(self, cited: &mut BTreeSet<String>) {
        collect_refs(self.backing_layers, cited);
    }
}
