use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

use anyhow::Result;
use tracing::warn;

use super::client::{OssClient, OssUploadArtifact};
use super::config::{NormalizedGcsConfig, NormalizedOssConfig};
use super::documents;
use crate::cfg::{
    GcsBackendConfig, OssBackendConfig, SnapshotImageStoragePolicy,
    SnapshotPublishCompressionConfig,
};
use crate::repository::interfaces::{LayerRole, PublishPolicy, Repository};
use crate::repository::{
    BlobBody, BlobKey, BlobLocation, DocumentCollection, DocumentKey, DocumentUpdate,
    MaintenanceLock, ManagedLayerObject, RepositoryError, RepositoryResult, SnapshotFile,
    MAINTENANCE_LOCK_STALE_AFTER,
};
use crate::types::SnapshotId;

/// Manages the committed‐state layer of the OSS snapshot repository.
///
/// Objects sit under the configured prefix at the paths
/// [`DocumentKey::path`] and [`BlobKey::path`] name.
pub(crate) struct OssRepository {
    client: Arc<OssClient>,
    policy: PublishPolicy,
}

impl OssRepository {
    /// Connects to the bucket and prefix that `config` names.
    pub(crate) fn from_config(
        config: &OssBackendConfig,
        snapshot_image_storage: SnapshotImageStoragePolicy,
        publish_compression: &SnapshotPublishCompressionConfig,
    ) -> Result<Self> {
        let config = NormalizedOssConfig::new(config, snapshot_image_storage)?;
        let client = Arc::new(OssClient::new(
            config.bucket().to_string(),
            config.endpoint().to_string(),
            config.region().to_string(),
            config.prefix().to_string(),
            config.credential_source(),
            config.addressing_style(),
        )?);
        Ok(Self::new(
            client,
            config.snapshot_image_storage(),
            publish_compression,
        ))
    }

    /// Connects to the Google Cloud Storage bucket and prefix that `config`
    /// names. No request is made until the first operation, so credentials
    /// are first resolved there.
    pub(crate) fn from_gcs_config(
        config: &GcsBackendConfig,
        snapshot_image_storage: SnapshotImageStoragePolicy,
        publish_compression: &SnapshotPublishCompressionConfig,
    ) -> Result<Self> {
        let client = Arc::new(NormalizedGcsConfig::new(config)?.client());
        Ok(Self::new(
            client,
            snapshot_image_storage,
            publish_compression,
        ))
    }

    pub(crate) fn new(
        client: Arc<OssClient>,
        snapshot_image_storage: SnapshotImageStoragePolicy,
        publish_compression: &SnapshotPublishCompressionConfig,
    ) -> Self {
        Self {
            client,
            policy: PublishPolicy {
                compression: publish_compression.clone(),
                image_storage: snapshot_image_storage,
            },
        }
    }
}

fn upload_artifact(key: &BlobKey) -> OssUploadArtifact {
    match key {
        BlobKey::Layer { role, .. } => match role {
            LayerRole::Rootfs => OssUploadArtifact::RootfsLayer,
            LayerRole::AttachedDrive => OssUploadArtifact::AttachedDriveLayer,
            LayerRole::Memory => OssUploadArtifact::MemoryLayer,
        },
        BlobKey::Snapshot(_, SnapshotFile::VmState) => OssUploadArtifact::VmState,
        BlobKey::Snapshot(_, SnapshotFile::FirecrackerManifest) => {
            OssUploadArtifact::FirecrackerManifest
        }
        BlobKey::Snapshot(_, SnapshotFile::StartupManifest) => OssUploadArtifact::StartupPack,
    }
}

/// Takes over a lock object whose holder is gone, or reports the contention.
///
/// The object is rewritten with `this` token only when it is older than
/// [`MAINTENANCE_LOCK_STALE_AFTER`]. Two passes that see the same stale object
/// may both rewrite it; the last write wins, and the loser sees a token that is
/// not its own on release and leaves the object alone.
async fn take_over_stale_lock(
    client: &OssClient,
    name: &str,
    key: &str,
    token: String,
) -> RepositoryResult<Option<MaintenanceLock>> {
    let metadata = match client.stat(key).await {
        Ok(metadata) => metadata,
        Err(error) if OssClient::is_not_found_error(&error) => return Ok(None),
        Err(error) => {
            return Err(RepositoryError::backend(
                format!("stat maintenance lock '{key}'"),
                error,
            ))
        }
    };
    let stale = metadata.last_modified.is_some_and(|modified| {
        modified
            .elapsed()
            .is_ok_and(|age| age >= MAINTENANCE_LOCK_STALE_AFTER)
    });
    if !stale {
        return Ok(None);
    }
    warn!(key, "taking over a maintenance lock its holder left behind");
    client
        .put_bytes(
            key,
            token.clone().into_bytes(),
            OssUploadArtifact::CatalogRecord,
        )
        .await
        .map_err(|error| {
            RepositoryError::backend(format!("replace maintenance lock '{key}'"), error)
        })?;
    Ok(Some(MaintenanceLock::new(name.to_owned(), token)))
}

// ── Repository impl ────────────────────────────────────────────

#[async_trait]
impl Repository for OssRepository {
    fn publish_policy(&self) -> PublishPolicy {
        self.policy.clone()
    }

    fn managed_layers_repo_blob_url(&self) -> Option<String> {
        Some(self.client.object_url(BlobKey::LAYERS_DIR))
    }

    /// Layers are keyed by digest, so concurrent writers of one layer write
    /// identical bytes and the `exists → put` race is benign.
    ///
    /// The put is unconditional because Alibaba OSS rejects
    /// `x-oss-forbid-overwrite` on multipart uploads, which opendal uses for
    /// large files.
    async fn put_blob(&self, key: &BlobKey, body: BlobBody<'_>) -> RepositoryResult<()> {
        let object_key = key.path();
        if matches!(key, BlobKey::Layer { .. }) {
            let exists = self.client.exists(&object_key).await.map_err(|error| {
                RepositoryError::backend("check managed layer existence", error)
            })?;
            if exists {
                return Ok(());
            }
        }
        let artifact = upload_artifact(key);
        match body {
            BlobBody::File(source) => self.client.put_file(&object_key, source, artifact).await,
            BlobBody::Bytes(bytes) => self.client.put_bytes(&object_key, bytes, artifact).await,
        }
        .map_err(|error| RepositoryError::backend(format!("upload '{object_key}'"), error))
    }

    fn locate_blob(&self, key: &BlobKey) -> RepositoryResult<BlobLocation> {
        Ok(BlobLocation::Remote(self.client.object_url(&key.path())))
    }

    async fn has_blob(&self, key: &BlobKey) -> RepositoryResult<bool> {
        let object_key = key.path();
        self.client
            .exists(&object_key)
            .await
            .map_err(|error| RepositoryError::backend(format!("check '{object_key}'"), error))
    }

    async fn fetch_blob(&self, key: &BlobKey, destination: &Path) -> RepositoryResult<u64> {
        let object_key = key.path();
        self.client
            .get_to_file(&object_key, destination)
            .await
            .map_err(|error| RepositoryError::backend(format!("download '{object_key}'"), error))
    }

    async fn read_blob(&self, key: &BlobKey) -> RepositoryResult<Vec<u8>> {
        let object_key = key.path();
        match self.client.get_bytes(&object_key).await {
            Ok(bytes) => Ok(bytes.to_vec()),
            Err(error) if OssClient::is_not_found_error(&error) => {
                Err(RepositoryError::ArtifactNotFound {
                    artifact: format!("'{object_key}'"),
                })
            }
            Err(error) => Err(RepositoryError::backend(
                format!("read '{object_key}'"),
                error,
            )),
        }
    }

    async fn delete_snapshot_blobs(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let prefix = format!("{}/", BlobKey::snapshot_dir(id));
        self.client
            .delete_prefix(&prefix)
            .await
            .map_err(|error| RepositoryError::backend(format!("delete '{prefix}'"), error))
    }

    async fn get_document(&self, key: DocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
        documents::get(&self.client, &key).await
    }

    async fn put_document(&self, key: DocumentKey, bytes: Vec<u8>) -> RepositoryResult<()> {
        documents::put(&self.client, &key, bytes).await
    }

    async fn update_document(
        &self,
        key: DocumentKey,
        update: DocumentUpdate,
    ) -> RepositoryResult<Option<Vec<u8>>> {
        documents::update(&self.client, &key, update).await
    }

    async fn delete_document(&self, key: DocumentKey) -> RepositoryResult<()> {
        documents::delete(&self.client, &key).await
    }

    async fn list_documents(
        &self,
        collection: DocumentCollection,
        after: Option<&str>,
        limit: usize,
    ) -> RepositoryResult<Vec<String>> {
        documents::list(&self.client, collection, after, limit).await
    }

    async fn read_documents(
        &self,
        collection: DocumentCollection,
    ) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
        documents::read_all(&self.client, collection).await
    }

    async fn list_managed_layers(&self) -> RepositoryResult<Vec<ManagedLayerObject>> {
        let entries = self
            .client
            .list_entries_recursive(BlobKey::LAYERS_PREFIX)
            .await
            .map_err(|error| {
                RepositoryError::backend(format!("list '{}'", BlobKey::LAYERS_PREFIX), error)
            })?;
        let mut layers: Vec<ManagedLayerObject> = entries
            .into_iter()
            .map(|entry| ManagedLayerObject {
                layer_digest: BlobKey::digest_from_layer_key(&entry.key),
                bytes: entry.bytes,
                last_modified: entry.last_modified,
                key: entry.key,
            })
            .collect();
        layers.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(layers)
    }

    async fn delete_managed_layer(&self, key: &str) -> RepositoryResult<()> {
        self.client
            .delete(key)
            .await
            .map_err(|error| RepositoryError::backend(format!("delete '{key}'"), error))
    }

    /// Takes the pass lock by creating the lock object only when it is absent.
    ///
    /// This is a create-only write, not a [`lease`](super::lease): a lease needs
    /// Alibaba OSS's append, which other S3 stores do not support, while every
    /// store this backend talks to honours a conditional create. A holder that
    /// dies without releasing leaves the object behind, so a later pass takes it
    /// over once it is older than
    /// [`MAINTENANCE_LOCK_STALE_AFTER`](crate::repository::MAINTENANCE_LOCK_STALE_AFTER).
    async fn acquire_maintenance_lock(
        &self,
        name: &str,
    ) -> RepositoryResult<Option<MaintenanceLock>> {
        let key = crate::repository::maintenance_lock_key(name);
        let token = uuid::Uuid::now_v7().to_string();
        let created = self
            .client
            .put_bytes_conditionally(&key, token.clone().into_bytes(), None)
            .await
            .map_err(|error| {
                RepositoryError::backend(format!("create maintenance lock '{key}'"), error)
            })?;
        if created {
            return Ok(Some(MaintenanceLock::new(name.to_owned(), token)));
        }
        take_over_stale_lock(&self.client, name, &key, token).await
    }

    async fn release_maintenance_lock(&self, lock: MaintenanceLock) -> RepositoryResult<()> {
        let key = crate::repository::maintenance_lock_key(&lock.name);
        // Only the holder removes the object: a lock another pass took over
        // already carries that pass's token.
        let current = match self.client.get_bytes(&key).await {
            Ok(bytes) => Some(bytes.to_vec()),
            Err(error) if super::client::OssClient::is_not_found_error(&error) => None,
            Err(error) => {
                return Err(RepositoryError::backend(
                    format!("read maintenance lock '{key}'"),
                    error,
                ))
            }
        };
        if current.as_deref() != Some(lock.token.as_bytes()) {
            return Ok(());
        }
        self.client
            .delete(&key)
            .await
            .map_err(|error| RepositoryError::backend(format!("delete '{key}'"), error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store_operator::CredentialSource;

    #[test]
    fn publish_policy_carries_the_configured_compression_and_image_storage() {
        let client = OssClient::new(
            "bucket".to_string(),
            "https://oss.example.com".to_string(),
            "region".to_string(),
            "prefix".to_string(),
            CredentialSource::Anonymous,
            None,
        )
        .expect("oss client");
        let repository = OssRepository::new(
            Arc::new(client),
            SnapshotImageStoragePolicy::SourceRegistry,
            &SnapshotPublishCompressionConfig {
                enabled: true,
                algorithm: crate::cfg::OverlaybdCompressionAlgorithm::Zstd,
                workers: 4,
            },
        );
        let policy = repository.publish_policy();
        assert_eq!(
            policy.image_storage,
            SnapshotImageStoragePolicy::SourceRegistry
        );
        assert!(policy.compression.enabled);
        assert_eq!(policy.compression.workers, 4);
        assert_eq!(
            repository.managed_layers_repo_blob_url().as_deref(),
            Some("s3://bucket/prefix/managed-layers")
        );
    }
}
