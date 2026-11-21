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
//!
//! # The report receipt
//!
//! Enforce is irreversible, so it only runs after an operator has seen a clean
//! report. The two calls of one pass need not reach the same server process —
//! the caller goes through the gateway, and the gateway may spread them over
//! different nodes — so the report leaves a **receipt** in the repository and
//! the enforce consumes it:
//!
//! - the report writes `{ token, issuedAtUnixMs, counts }` as a repository
//!   document under `maintenance/managed-layer-collect/receipts/`, **outside**
//!   `managed-layers/`, and returns the token;
//! - the enforce requires `reportToken`, reads the receipt under the same lock
//!   the report held, and accepts it only when it exists, has not expired
//!   ([`REPORT_RECEIPT_TTL`]), is clean (no downgrade, no unreadable or unknown
//!   layer, at least one cited layer) and was not consumed;
//! - the enforce then reclassifies the whole bucket under the lock and deletes
//!   only an uncited layer older than the grace period. It never trusts the
//!   counts in the receipt;
//! - any deviation refuses the pass; nothing is deleted. A receipt authorizes
//!   exactly one enforce, and each report prunes the receipts that expired.
//!
//! The token is a secret: it is never logged, and every backend message that
//! names the receipt's document is redacted before it reaches a log line.

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::image::{ImageCatalog, PublishedImage};
use crate::repository::{
    DocumentCollection, DocumentKey, MaintenanceLock, ManagedLayerObject, Repository,
    RepositoryError, RepositoryResult,
};

/// The lock one collection pass holds, so two passes never overlap.
const PASS_LOCK: &str = "managed-layer-collect";

/// How many report receipts one prune page lists.
const RECEIPT_PRUNE_PAGE: usize = 256;

/// How long a report receipt stays valid after the report pass finished.
///
/// A caller runs the report and the enforce of the same pass seconds apart, so
/// this window only has to cover the gap between the two calls. It is short on
/// purpose: the receipt is the proof that an operator saw a clean report, and an
/// old receipt no longer describes the bucket.
pub const REPORT_RECEIPT_TTL: Duration = Duration::from_secs(15 * 60);

/// What one pass does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedLayerCollectMode {
    /// List and count the orphans without deleting anything, and leave the
    /// receipt one enforce pass can use.
    Report,
    /// Delete the orphans that are older than the grace period, after consuming
    /// the receipt a report pass left.
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
    /// The token a report pass returned. An enforce pass carries the token of
    /// the report pass it follows; a report pass ignores it.
    ///
    /// This is what replaces the old requirement that both calls reach the same
    /// server process: the report writes a receipt under the repository lock and
    /// the enforce consumes it, so the two calls can land on different nodes.
    /// The token is a secret and is never logged.
    pub report_token: Option<String>,
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
    /// Layers the pass could not classify because the citation set was
    /// unknowable. They are neither cited nor orphaned, and never deleted.
    pub unknown_layers: u64,
    /// Layers actually removed. Always zero in report mode.
    pub deleted_layers: u64,
    /// Bytes actually removed. Always zero in report mode.
    pub reclaimed_bytes: u64,
    /// Set when the pass listed and counted but deleted nothing, even in enforce
    /// mode. A live sandbox whose source is gone sets it.
    pub downgraded_reason: Option<String>,
    pub failures: Vec<ManagedLayerCollectFailure>,
    /// The opaque token a report pass leaves for exactly one enforce pass.
    ///
    /// Present only on a report pass that persisted its receipt. It is a
    /// secret: never log it, never persist it anywhere but its own receipt.
    pub report_token: Option<String>,
}

/// Why a pass could not run.
#[derive(Debug, thiserror::Error)]
pub enum ManagedLayerCollectorError {
    #[error("another managed-layer collection pass holds the repository lock")]
    Locked,
    /// Enforce was requested with a token that names no usable report receipt.
    ///
    /// The reason never carries the token itself: it is a secret.
    #[error("enforce requires a valid report receipt: {0}")]
    ReportReceiptRejected(&'static str),
    /// The repository could not read, write or consume the receipt.
    ///
    /// The message is the backend's error with the token redacted.
    #[error("the report receipt is unavailable: {0}")]
    ReceiptUnavailable(String),
    #[error(transparent)]
    Repository(#[from] RepositoryError),
}

/// The proof one report pass leaves behind for exactly one enforce pass.
///
/// It is a repository document under
/// `maintenance/managed-layer-collect/receipts/`, never under `managed-layers/`:
/// a collection pass that listed its own receipt as a layer would count it
/// unreadable and could never run a clean enforce again.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ManagedLayerCollectReceipt {
    /// The opaque token the report returned.
    token: String,
    /// When the report pass finished, in milliseconds since the Unix epoch.
    #[serde(rename = "issuedAtUnixMs")]
    issued_at_unix_ms: u64,
    #[serde(rename = "citedLayers")]
    cited_layers: u64,
    #[serde(rename = "citedBytes")]
    cited_bytes: u64,
    #[serde(rename = "orphanLayers")]
    orphan_layers: u64,
    #[serde(rename = "orphanBytes")]
    orphan_bytes: u64,
    #[serde(rename = "unexpiredLayers")]
    unexpired_layers: u64,
    #[serde(rename = "unreadableLayers")]
    unreadable_layers: u64,
    #[serde(rename = "unknownLayers")]
    unknown_layers: u64,
    #[serde(rename = "downgradedReason", default)]
    downgraded_reason: Option<String>,
}

impl ManagedLayerCollectReceipt {
    fn from_report(token: String, report: &ManagedLayerCollectReport) -> Self {
        Self {
            token,
            issued_at_unix_ms: now_unix_ms(),
            cited_layers: report.cited_layers,
            cited_bytes: report.cited_bytes,
            orphan_layers: report.orphan_layers,
            orphan_bytes: report.orphan_bytes,
            unexpired_layers: report.unexpired_layers,
            unreadable_layers: report.unreadable_layers,
            unknown_layers: report.unknown_layers,
            downgraded_reason: report.downgraded_reason.clone(),
        }
    }

    /// Whether the report this receipt names proved the bucket safe to sweep.
    ///
    /// Only a pass whose citation set was knowable and whose bucket held at
    /// least one cited layer can authorize a deletion. An unreadable object is
    /// left alone by the pass itself, but it still means an operator must look
    /// at what it is before anything is deleted.
    fn is_clean(&self) -> bool {
        self.unknown_layers == 0
            && self.unreadable_layers == 0
            && self.cited_layers > 0
            && self.downgraded_reason.is_none()
    }

    fn expired(&self, now_unix_ms: u64) -> bool {
        let ttl_ms = u64::try_from(REPORT_RECEIPT_TTL.as_millis()).unwrap_or(u64::MAX);
        now_unix_ms.saturating_sub(self.issued_at_unix_ms) > ttl_ms
    }
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
    let outcome = run_locked_pass(repository, live, request).await;
    release(repository, lock).await;
    outcome
}

/// One pass, run with the repository lock already held.
///
/// The receipt is read and consumed here, under the same lock that serializes
/// passes, so two enforce calls can never both use one receipt. The enforce
/// reclassifies the whole bucket under this lock; it never trusts the counts the
/// receipt carries, which are recorded for the operator only.
async fn run_locked_pass<R, L>(
    repository: &R,
    live: &L,
    request: &ManagedLayerCollectRequest,
) -> Result<ManagedLayerCollectReport, ManagedLayerCollectorError>
where
    R: Repository + ?Sized,
    L: LiveSandboxes + ?Sized,
{
    if request.mode == ManagedLayerCollectMode::Enforce {
        consume_report_receipt(repository, request.report_token.as_deref()).await?;
    }
    let mut report = run_pass(repository, live, request).await?;
    if request.mode == ManagedLayerCollectMode::Report {
        prune_expired_report_receipts(repository).await;
        report.report_token = Some(issue_report_receipt(repository, &report).await?);
    }
    Ok(report)
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
    let cited = cited_layer_digests(repository, live, &mut report).await;

    // When the citation set is unknowable, no layer can be proven uncited, so
    // nothing is classified as an orphan and nothing is deleted. The pass still
    // lists the bucket, so an operator sees what it covers.
    let knowable = report.downgraded_reason.is_none();

    let objects = repository.list_managed_layers().await?;
    // Enforce only when the citation set is trustworthy and the request asked
    // for it.
    let enforce = request.mode == ManagedLayerCollectMode::Enforce && knowable;

    let mut to_delete = Vec::new();
    for object in objects {
        let Some(digest) = object.layer_digest.as_deref() else {
            // The name does not translate to a digest, so no record could ever
            // cite it and the substrate cannot prove what it is. Never delete.
            report.unreadable_layers += 1;
            continue;
        };
        if !knowable {
            // Not an orphan and not cited either: the pass cannot tell.
            report.unknown_layers += 1;
            continue;
        }
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
        unknown_layers = report.unknown_layers,
        deleted_layers = report.deleted_layers,
        reclaimed_bytes = report.reclaimed_bytes,
        failures = report.failures.len(),
        downgraded = report.downgraded_reason.is_some(),
        "managed layer collection pass finished"
    );
    Ok(report)
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(0)
}

/// The canonical form of a token this server issued, or `None` when `token`
/// cannot be one.
///
/// Only a UUID can be a token, so a caller-supplied string can never name a
/// document outside the receipt prefix.
fn canonical_report_token(token: &str) -> Option<String> {
    uuid::Uuid::parse_str(token)
        .ok()
        .map(|token| token.hyphenated().to_string())
}

fn receipt_key(token: &str) -> DocumentKey {
    DocumentKey::ManagedLayerCollectReceipt(token.to_owned())
}

/// Removes the token from a backend message before it can reach a log line or
/// an operator. A backend names the document it failed on, and the token is
/// that document's name.
fn redact(token: &str, message: &str) -> String {
    message.replace(token, "<report-token>")
}

/// Persists the receipt a report pass leaves for one enforce pass.
async fn issue_report_receipt<R: Repository + ?Sized>(
    repository: &R,
    report: &ManagedLayerCollectReport,
) -> Result<String, ManagedLayerCollectorError> {
    let token = uuid::Uuid::now_v7().hyphenated().to_string();
    let receipt = ManagedLayerCollectReceipt::from_report(token.clone(), report);
    let bytes = serde_json::to_vec(&receipt).map_err(|error| {
        ManagedLayerCollectorError::ReceiptUnavailable(redact(&token, &error.to_string()))
    })?;
    repository
        .put_document(receipt_key(&token), bytes)
        .await
        .map_err(|error| {
            ManagedLayerCollectorError::ReceiptUnavailable(redact(&token, &error.to_string()))
        })?;
    Ok(token)
}

/// Persists a clean receipt as a report pass would, dated by the caller.
///
/// Test scaffolding for the behavior table's expired case: only a real report
/// pass mints a receipt in production, and this is not reachable from an
/// external crate.
pub(crate) async fn persist_receipt_for_test<R: Repository + ?Sized>(
    repository: &R,
    token: &str,
    issued_at_unix_ms: u64,
) -> RepositoryResult<()> {
    let receipt = ManagedLayerCollectReceipt {
        token: token.to_owned(),
        issued_at_unix_ms,
        cited_layers: 1,
        cited_bytes: 64,
        orphan_layers: 0,
        orphan_bytes: 0,
        unexpired_layers: 0,
        unreadable_layers: 0,
        unknown_layers: 0,
        downgraded_reason: None,
    };
    let bytes = serde_json::to_vec(&receipt).map_err(|error| RepositoryError::InvalidRequest {
        reason: format!("serialize report receipt: {error}"),
    })?;
    repository.put_document(receipt_key(token), bytes).await
}

/// Validates the receipt an enforce pass names and consumes it.
///
/// Called under the pass lock, before the bucket is classified: one report
/// authorizes one enforce, and the enforce that gets here is the one that spends
/// it. Every failure is a refusal to delete anything.
async fn consume_report_receipt<R: Repository + ?Sized>(
    repository: &R,
    token: Option<&str>,
) -> Result<(), ManagedLayerCollectorError> {
    let Some(token) = token else {
        return Err(ManagedLayerCollectorError::ReportReceiptRejected(
            "enforce requires the reportToken a report pass returned",
        ));
    };
    let Some(token) = canonical_report_token(token) else {
        return Err(ManagedLayerCollectorError::ReportReceiptRejected(
            "the reportToken is not a token this server hands out",
        ));
    };
    let key = receipt_key(&token);
    let bytes = repository
        .get_document(key.clone())
        .await
        .map_err(|error| {
            ManagedLayerCollectorError::ReceiptUnavailable(redact(&token, &error.to_string()))
        })?;
    let Some(bytes) = bytes else {
        return Err(ManagedLayerCollectorError::ReportReceiptRejected(
            "no report receipt matches this token: it was never issued, was already used, \
             or expired",
        ));
    };
    let receipt: ManagedLayerCollectReceipt = serde_json::from_slice(&bytes).map_err(|_| {
        ManagedLayerCollectorError::ReportReceiptRejected(
            "the report receipt this token names is unreadable",
        )
    })?;
    if receipt.token != token {
        return Err(ManagedLayerCollectorError::ReportReceiptRejected(
            "the report receipt this token names does not belong to it",
        ));
    }
    if receipt.expired(now_unix_ms()) {
        return Err(ManagedLayerCollectorError::ReportReceiptRejected(
            "the report receipt expired; run a new report pass",
        ));
    }
    if !receipt.is_clean() {
        return Err(ManagedLayerCollectorError::ReportReceiptRejected(
            "the report that issued this receipt did not prove the bucket safe: it was \
             downgraded, found unreadable or unknown layers, or cited none",
        ));
    }
    // The receipt is spent before the sweep starts, still under the lock: a call
    // that dies mid-sweep never leaves a receipt another enforce could reuse.
    repository.delete_document(key).await.map_err(|error| {
        ManagedLayerCollectorError::ReceiptUnavailable(redact(&token, &error.to_string()))
    })?;
    Ok(())
}

/// Deletes the receipts that can no longer authorize an enforce.
///
/// A receipt the caller never used would otherwise stay in the bucket for good.
/// Pruning is best-effort: a failure leaves the receipt to the next report, and
/// never fails the report that is running.
async fn prune_expired_report_receipts<R: Repository + ?Sized>(repository: &R) {
    let collection = DocumentCollection::ManagedLayerCollectReceipts;
    let now = now_unix_ms();
    // Paged, because an object-store backend materializes the page it is asked
    // for and cannot be handed an unbounded one.
    let mut after: Option<String> = None;
    loop {
        let page = match repository
            .list_documents(collection, after.as_deref(), RECEIPT_PRUNE_PAGE)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                // The listing names only the shared prefix, never a token.
                warn!(error = %error, "failed to list report receipts; expired receipts stay until the next report");
                return;
            }
        };
        let full_page = page.len() == RECEIPT_PRUNE_PAGE;
        for token in page.iter() {
            let Some(token) = canonical_report_token(token) else {
                // Nothing but a report writes here, so this is not a receipt
                // this server can judge. Leaving it is safer than deleting a
                // foreign object.
                continue;
            };
            let key = receipt_key(&token);
            let expired = match repository.get_document(key.clone()).await {
                // A receipt this server cannot parse can never authorize an
                // enforce, so it is collected with the expired ones.
                Ok(Some(bytes)) => {
                    match serde_json::from_slice::<ManagedLayerCollectReceipt>(&bytes) {
                        Ok(receipt) => receipt.expired(now),
                        Err(_) => true,
                    }
                }
                // Consumed by an enforce since the listing.
                Ok(None) => continue,
                Err(error) => {
                    warn!(error = %redact(&token, &error.to_string()), "failed to read a report receipt while pruning it");
                    continue;
                }
            };
            if !expired {
                continue;
            }
            if let Err(error) = repository.delete_document(key).await {
                warn!(error = %redact(&token, &error.to_string()), "failed to prune an expired report receipt");
            }
        }
        if !full_page {
            return;
        }
        after = page.last().cloned();
    }
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
    metrics::gauge!("agentenv_managed_layer_unknown_layers", "mode" => mode)
        .set(report.unknown_layers as f64);
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
