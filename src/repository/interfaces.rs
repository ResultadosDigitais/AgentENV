use std::path::{Path, PathBuf};
use std::time::SystemTime;

use async_trait::async_trait;
use tempfile::NamedTempFile;

use super::errors::{RepositoryError, RepositoryResult};
use super::{BlobKey, DocumentCollection, DocumentKey};
use crate::cfg::{SnapshotImageStoragePolicy, SnapshotPublishCompressionConfig};
use crate::types::SnapshotId;

/// How snapshots and volumes are published into one repository.
#[derive(Clone, Debug)]
pub struct PublishPolicy {
    /// Recontainerizes raw local layers as zfile before they are stored.
    pub compression: SnapshotPublishCompressionConfig,
    /// Whether disk images go to their source registry or to managed layers.
    pub image_storage: SnapshotImageStoragePolicy,
}

impl Default for PublishPolicy {
    /// Raw layers, stored as managed layers.
    fn default() -> Self {
        Self {
            compression: SnapshotPublishCompressionConfig {
                enabled: false,
                ..SnapshotPublishCompressionConfig::default()
            },
            image_storage: SnapshotImageStoragePolicy::default(),
        }
    }
}

/// What a stored layer is used for. It labels metrics and logs only; layers
/// are stored by digest alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LayerRole {
    Rootfs,
    AttachedDrive,
    Memory,
}

/// The contents [`Repository::put_blob`] stores.
#[derive(Debug)]
pub enum BlobBody<'a> {
    File(&'a Path),
    Bytes(Vec<u8>),
}

/// Where a stored blob is read from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobLocation {
    /// A path on this node.
    Local(PathBuf),
    /// The blob's URL.
    Remote(String),
}

/// What an update does to a document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DocumentChange {
    Keep,
    Put(Vec<u8>),
    Delete,
}

/// Decides how to change a document from its current bytes, if any. Backends
/// may call it again when a concurrent writer wins, so it must be a pure
/// function of its input.
pub type DocumentUpdate =
    Box<dyn Fn(Option<&[u8]>) -> RepositoryResult<DocumentChange> + Send + Sync>;

/// One object found under `managed-layers/`.
///
/// [`layer_digest`] is `None` when the object name does not translate back to
/// a `sha256:<hex>` digest, which makes the object unreachable by any record:
/// it must never be deleted, because the substrate cannot prove what it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedLayerObject {
    /// The repository-relative key the backend stores the object under.
    pub key: String,
    /// The digest the key translates back to, or `None` when it does not.
    pub layer_digest: Option<String>,
    /// The object's size in bytes.
    pub bytes: u64,
    /// When the object was last written, when the backend reports it.
    pub last_modified: Option<SystemTime>,
}

/// How long a maintenance lock object may sit unreleased before a later pass
/// takes it over. A pass that dies without releasing (a crash, a node lost
/// mid-sweep) leaves its lock object behind on backends that cannot drop a lock
/// with the process; this bound keeps the next scheduled pass from being
/// blocked forever. It is far longer than a pass takes, so a live pass is never
/// overtaken.
pub const MAINTENANCE_LOCK_STALE_AFTER: std::time::Duration =
    std::time::Duration::from_secs(30 * 60);

/// A held maintenance lock. The token identifies this holder, so a release
/// never removes a lock another pass has since taken over.
///
/// Dropping the lock releases whatever the backend holds: a POSIX backend holds
/// an `flock` that the kernel drops with the process, so a pass that dies leaves
/// no stale lock. Dropping is enough for those backends; call
/// [`Repository::release_maintenance_lock`] so a backend that must remove a lock
/// object can do it.
#[derive(Debug)]
pub struct MaintenanceLock {
    /// The lock's name in the repository.
    pub name: String,
    /// This holder's token.
    pub token: String,
    /// Backend state that keeps the lock held for as long as it lives. It is
    /// never read: holding it *is* the point, and dropping it releases the lock.
    _keeper: Option<Box<dyn std::any::Any + Send + Sync>>,
}

impl MaintenanceLock {
    pub(crate) fn new(name: String, token: String) -> Self {
        Self {
            name,
            token,
            _keeper: None,
        }
    }

    pub(crate) fn with_keeper(
        name: String,
        token: String,
        keeper: Box<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        Self {
            name,
            token,
            _keeper: Some(keeper),
        }
    }
}

fn unsupported<T>(feature: &str) -> RepositoryResult<T> {
    Err(RepositoryError::Unsupported {
        feature: feature.to_owned(),
    })
}

#[async_trait]
/// Durable storage shared by every node: small shared documents, snapshot
/// files and layers, and volume backing layers.
///
/// A repository stores; it does not decide. The catalogs built on its
/// documents (the snapshot and volume catalogs, and
/// [`super::BuildCacheStore`]) own the records, aliases, and their rules, the
/// same on every backend. It depends on no module that builds on it.
///
/// The repository boundary excludes node-local derived state: runtime-ready
/// `image.json` files are materialized by the snapshot runtime resolver over
/// every backend, and node-local cache directories never leak into durable
/// records. Shared
/// layer imports must use atomic protocols so concurrent writers never expose
/// half-written layers.
pub trait Repository: Send + Sync {
    /// How snapshots and volumes are published into this repository.
    fn publish_policy(&self) -> PublishPolicy {
        PublishPolicy::default()
    }

    /// The overlaybd `repoBlobUrl` that reads layers stored here, if remote
    /// readers address them by URL.
    fn managed_layers_repo_blob_url(&self) -> Option<String> {
        None
    }

    /// Stores one blob. A layer that is already stored is kept, since its
    /// digest names its contents; a snapshot file is replaced.
    async fn put_blob(&self, _key: &BlobKey, _body: BlobBody<'_>) -> RepositoryResult<()> {
        unsupported("blob storage")
    }

    /// Where readers on this node find a blob: in place on a shared filesystem,
    /// or by URL.
    fn locate_blob(&self, _key: &BlobKey) -> RepositoryResult<BlobLocation> {
        unsupported("blob storage")
    }

    /// Whether a blob is stored.
    async fn has_blob(&self, _key: &BlobKey) -> RepositoryResult<bool> {
        unsupported("blob storage")
    }

    /// Downloads one blob to `destination`, returning its size.
    async fn fetch_blob(&self, _key: &BlobKey, _destination: &Path) -> RepositoryResult<u64> {
        unsupported("blob storage")
    }

    /// Reads one small blob; `ArtifactNotFound` when it is absent.
    async fn read_blob(&self, _key: &BlobKey) -> RepositoryResult<Vec<u8>> {
        unsupported("blob storage")
    }

    /// Removes every file of one snapshot. Shared layers stay. Missing files
    /// are considered success.
    async fn delete_snapshot_blobs(&self, _id: &SnapshotId) -> RepositoryResult<()> {
        unsupported("blob storage")
    }

    /// A new temporary file to prepare a blob in. It is placed where putting
    /// it costs least: a POSIX repository links it in without copying.
    fn staging_file(&self) -> RepositoryResult<NamedTempFile> {
        NamedTempFile::new().map_err(|error| RepositoryError::backend("create staging file", error))
    }

    /// Reads one shared document; `None` when absent.
    async fn get_document(&self, _key: DocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
        unsupported("repository documents")
    }

    /// Atomically replaces one shared document.
    async fn put_document(&self, _key: DocumentKey, _bytes: Vec<u8>) -> RepositoryResult<()> {
        unsupported("repository documents")
    }

    /// Atomically applies `update` against concurrent writers on every node,
    /// returning the document it was applied to.
    async fn update_document(
        &self,
        _key: DocumentKey,
        _update: DocumentUpdate,
    ) -> RepositoryResult<Option<Vec<u8>>> {
        unsupported("repository documents")
    }

    /// Removes one shared document. Missing documents are considered success.
    async fn delete_document(&self, _key: DocumentKey) -> RepositoryResult<()> {
        unsupported("repository documents")
    }

    /// Lists up to `limit` document IDs after `after`, in the backend's stable order.
    async fn list_documents(
        &self,
        _collection: DocumentCollection,
        _after: Option<&str>,
        _limit: usize,
    ) -> RepositoryResult<Vec<String>> {
        unsupported("repository documents")
    }

    /// Reads every live document in `collection`, in the backend's stable ID order.
    async fn read_documents(
        &self,
        _collection: DocumentCollection,
    ) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
        unsupported("repository documents")
    }

    /// Lists every object stored under `managed-layers/`, without following
    /// any catalog. A record that cites a layer is the only thing that keeps
    /// it alive, so this is what a managed-layer collection pass compares the
    /// citation set against.
    async fn list_managed_layers(&self) -> RepositoryResult<Vec<ManagedLayerObject>> {
        unsupported("managed layer enumeration")
    }

    /// Removes one object listed by [`Self::list_managed_layers`], named by its
    /// [`ManagedLayerObject::key`]. A missing object is considered success, so
    /// a retried pass converges.
    async fn delete_managed_layer(&self, _key: &str) -> RepositoryResult<()> {
        unsupported("managed layer collection")
    }

    /// Stores one object under `managed-layers/` under a name that need not be a
    /// digest.
    ///
    /// [`Self::put_blob`] only accepts a digest-shaped layer, which is all a
    /// publisher writes. An older release, or a hand edit, can leave an object
    /// there whose name is not a digest, and that object must be listed as
    /// unreadable and never deleted. A test needs to place such an object next
    /// to the ones a pass collects, and it can only do that through the backend
    /// that stores it.
    async fn put_managed_layer_object(&self, _key: &str, _body: &[u8]) -> RepositoryResult<()> {
        unsupported("managed layer object write")
    }

    /// Takes the named maintenance lock for the lifetime of one pass, so two
    /// passes never overlap. `Ok(None)` reports that another pass holds it.
    ///
    /// The lock excludes other maintenance passes only. It does not exclude
    /// publishers: a publish uploads its layers before it writes the record
    /// that cites them, so a pass may see a layer no record cites yet. The
    /// collection grace period, not this lock, covers that race.
    async fn acquire_maintenance_lock(
        &self,
        _name: &str,
    ) -> RepositoryResult<Option<MaintenanceLock>> {
        unsupported("maintenance lock")
    }

    /// Releases a lock returned by [`Self::acquire_maintenance_lock`]. A lock
    /// left behind by a process that died is taken over by a later pass.
    async fn release_maintenance_lock(&self, _lock: MaintenanceLock) -> RepositoryResult<()> {
        unsupported("maintenance lock")
    }
}
