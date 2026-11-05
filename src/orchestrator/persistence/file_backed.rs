use std::collections::HashSet;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs;
use tokio::sync::OnceCell;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::{PersistenceResult, SandboxPersistenceError, SandboxPersister};
use crate::local_store::{LocalKvStore, LocalStoreDurability};
use crate::orchestrator::{store::SandboxMetadata, SandboxState};
use crate::sandbox::{PausedSandboxState, SandboxBackendFactory};
use crate::types::SandboxId;
use crate::virtualization::VirtualizationMode;

const RECORD_VERSION: u32 = 1;
const RECORD_DB_DIR: &str = "records.db";

/// What `load_all` decided about one paused record. Every branch of the load
/// path ends in exactly one of these, so the "nothing was lost" acceptance is a
/// query over `agentenv_paused_sandbox_load_total` instead of a log read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LoadOutcome {
    /// The runtime state decoded here and its artifacts are on this node.
    LoadedFromLocal,
    /// The record was readable but this node holds no resumable runtime state
    /// for it (incompatible virtualization mode). It stays listed as `Paused`
    /// and its artifacts are kept.
    RetainedWithoutRuntimeState,
    /// The record or its artifacts cannot be used by this node. It is deleted.
    Discarded,
}

impl LoadOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::LoadedFromLocal => "loaded_from_local",
            Self::RetainedWithoutRuntimeState => "retained_unreachable",
            Self::Discarded => "discarded",
        }
    }

    /// Records the outcome. Label values come from this closed set; a sandbox ID
    /// is never a label value, because its cardinality is unbounded.
    fn record(self) {
        metrics::counter!(
            "agentenv_paused_sandbox_load_total",
            "outcome" => self.as_str(),
        )
        .increment(1);
    }
}

/// The per-outcome tally of one `load_all` pass.
///
/// One call per decided record keeps the metric and the summary log line from
/// drifting apart: a branch cannot count without also emitting.
#[derive(Default)]
struct LoadOutcomes {
    loaded_from_local: usize,
    retained_unreachable: usize,
    discarded: usize,
}

impl LoadOutcomes {
    fn observe(&mut self, outcome: LoadOutcome) {
        outcome.record();
        match outcome {
            LoadOutcome::LoadedFromLocal => self.loaded_from_local += 1,
            LoadOutcome::RetainedWithoutRuntimeState => self.retained_unreachable += 1,
            LoadOutcome::Discarded => self.discarded += 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PersistedPausedLifecycle {
    Paused,
    Resuming,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedPausedRecord {
    version: u32,
    lifecycle: PersistedPausedLifecycle,
    metadata: SandboxMetadata,
    artifact_root: PathBuf,
    state: Value,
}

impl PersistedPausedRecord {
    /// Metadata for this record with its runtime state decoded by `factory`.
    ///
    /// Borrows the record so a decode failure can still fall back to
    /// [`Self::into_metadata_without_runtime_state`]: the failure may mean the
    /// artifacts live on another node, not that the record is junk.
    fn decode_runtime_metadata<F>(&self, factory: &F) -> PersistenceResult<SandboxMetadata>
    where
        F: SandboxBackendFactory,
    {
        ensure_supported_version(self.version)?;

        let paused_state = factory
            .decode_paused_state(self.artifact_root.clone(), self.state.clone())
            .map_err(|source| SandboxPersistenceError::InvalidRecord {
                reason: "failed to decode paused sandbox state".to_string(),
                source: Some(source),
            })?;
        let mut metadata = self.metadata.clone();
        metadata.state = SandboxState::Paused;
        metadata.paused_state = Some(paused_state);

        Ok(metadata)
    }

    fn into_metadata_without_runtime_state(mut self) -> SandboxMetadata {
        self.metadata.state = SandboxState::Paused;
        self.metadata.paused_state = None;
        self.metadata
    }
}

fn decode_record(bytes: &[u8]) -> PersistenceResult<PersistedPausedRecord> {
    let record: PersistedPausedRecord =
        serde_json::from_slice(bytes).map_err(|source| SandboxPersistenceError::InvalidRecord {
            reason: "failed to deserialize record".to_string(),
            source: Some(source.into()),
        })?;
    ensure_supported_version(record.version)?;
    Ok(record)
}

fn ensure_supported_version(version: u32) -> PersistenceResult<()> {
    if version == RECORD_VERSION {
        Ok(())
    } else {
        Err(SandboxPersistenceError::InvalidRecord {
            reason: format!("unsupported record version {version}"),
            source: None,
        })
    }
}

pub struct FileBackedSandboxPersister {
    root: PathBuf,
    virtualization_mode: VirtualizationMode,
    durability: LocalStoreDurability,
    db: OnceCell<LocalKvStore>,
}

impl FileBackedSandboxPersister {
    pub fn new(root: PathBuf, virtualization_mode: VirtualizationMode) -> Self {
        Self {
            root,
            virtualization_mode,
            durability: LocalStoreDurability::Sync,
            db: OnceCell::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(root: PathBuf) -> Self {
        Self::new(root, VirtualizationMode::Kvm)
    }

    pub fn with_durability(mut self, durability: LocalStoreDurability) -> Self {
        self.durability = durability;
        self
    }

    fn records_db_path(&self) -> PathBuf {
        self.root.join(RECORD_DB_DIR)
    }

    fn artifacts_root(&self) -> PathBuf {
        self.root.join("artifacts")
    }

    fn sandbox_artifact_root(&self, sandbox_id: &SandboxId) -> PathBuf {
        self.artifacts_root().join(sandbox_id.to_string())
    }

    async fn db(&self) -> PersistenceResult<LocalKvStore> {
        self.db
            .get_or_try_init(|| async {
                LocalKvStore::open(self.records_db_path(), self.durability)
                    .await
                    .map_err(|source| SandboxPersistenceError::store("open RocksDB", source))
            })
            .await
            .cloned()
    }

    async fn get_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<PersistedPausedRecord> {
        let bytes = self
            .db()
            .await?
            .get(sandbox_id.to_string())
            .await
            .map_err(|source| SandboxPersistenceError::store("read paused sandbox record", source))?
            .ok_or_else(|| SandboxPersistenceError::InvalidRecord {
                reason: format!("paused sandbox record {sandbox_id} not found"),
                source: None,
            })?;
        decode_record(&bytes)
    }

    async fn put_record(&self, record: &PersistedPausedRecord) -> PersistenceResult<()> {
        let bytes = serde_json::to_vec(record).map_err(|source| {
            SandboxPersistenceError::InvalidRecord {
                reason: "failed to serialize record".to_string(),
                source: Some(source.into()),
            }
        })?;

        self.db()
            .await?
            .put(record.metadata.id.to_string(), bytes)
            .await
            .map_err(|source| {
                SandboxPersistenceError::store("persist paused sandbox record", source)
            })
    }

    async fn remove_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        self.db()
            .await?
            .delete(sandbox_id.to_string())
            .await
            .map_err(|source| {
                SandboxPersistenceError::store("remove paused sandbox record", source)
            })
    }

    async fn remove_artifact_root(path: &Path) -> PersistenceResult<()> {
        match fs::remove_dir_all(path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(SandboxPersistenceError::io(
                "remove paused sandbox artifacts",
                path,
                source,
            )),
        }
    }

    /// Drops a record this node can never use: the bytes are not a paused
    /// record at all, or a resumed record was abandoned mid-flight.
    ///
    /// Deliberately *not* used for a record whose runtime state fails to decode:
    /// that failure is also what a node with missing artifacts looks like, and
    /// deleting it is the step that turns a pod swap into permanent loss. Such a
    /// record is retained by [`Self::retain_unusable_record`] instead.
    async fn cleanup_invalid_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "cleaning up invalid paused sandbox record");
        self.remove_record(sandbox_id).await?;
        Self::remove_artifact_root(&self.sandbox_artifact_root(sandbox_id)).await?;
        Ok(())
    }

    /// Keeps a record whose runtime state could not be decoded here, so a retry
    /// — on a node that does hold its artifacts, or here once they arrive — can
    /// still resume it. The record is reported, never silently dropped.
    ///
    /// Retention is not unbounded, and the sweep that bounds it is deliberately
    /// *not* the orphan sweep: that one only reclaims a sandbox whose record is
    /// gone, so counting on it would let a retained record keep its artifacts
    /// forever. Retention is bounded instead by two things this branch can see:
    /// the record still being replaced on the next successful pause of the same
    /// sandbox, and the operator-facing counter plus log line below, which is what
    /// the seven-day acceptance watches. Reclaiming a retained record on a timer
    /// is a separate decision, because a timer that is too short destroys exactly
    /// the record this branch exists to save.
    fn retain_unusable_record(&self, sandbox_id: &SandboxId, error: &SandboxPersistenceError) {
        warn!(
            sandbox_id = %sandbox_id,
            artifacts = %self.sandbox_artifact_root(sandbox_id).display(),
            error = %error,
            "retaining paused sandbox record whose runtime state is unavailable on this node"
        );
    }

    async fn cleanup_orphan_artifacts(
        &self,
        retained_sandbox_ids: &HashSet<SandboxId>,
    ) -> PersistenceResult<()> {
        let artifacts_root = self.artifacts_root();
        let mut entries = match fs::read_dir(&artifacts_root).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(SandboxPersistenceError::io(
                    "read paused sandbox artifacts",
                    &artifacts_root,
                    source,
                ));
            }
        };

        while let Some(entry) = entries.next_entry().await.map_err(|source| {
            SandboxPersistenceError::io("scan paused sandbox artifacts", &artifacts_root, source)
        })? {
            let file_type = entry.file_type().await.map_err(|source| {
                SandboxPersistenceError::io(
                    "inspect paused sandbox artifacts",
                    entry.path(),
                    source,
                )
            })?;
            if !file_type.is_dir() {
                continue;
            }

            let Some(sandbox_id) = entry
                .file_name()
                .to_str()
                .and_then(|name| SandboxId::parse_str(name).ok())
            else {
                continue;
            };

            if !retained_sandbox_ids.contains(&sandbox_id) {
                info!(
                    sandbox_id = %sandbox_id,
                    artifacts = %entry.path().display(),
                    "removing orphaned paused sandbox artifacts"
                );
                Self::remove_artifact_root(&entry.path()).await?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl SandboxPersister for FileBackedSandboxPersister {
    async fn load_all<F>(&self, factory: &F) -> PersistenceResult<Vec<SandboxMetadata>>
    where
        F: SandboxBackendFactory,
    {
        info!(store = %self.root.display(), "loading paused sandbox records");
        let records = self.db().await?.entries().await.map_err(|source| {
            SandboxPersistenceError::store("scan paused sandbox records", source)
        })?;
        let mut sandboxes = Vec::new();
        let mut retained_artifacts = HashSet::new();
        let mut outcomes = LoadOutcomes::default();

        for (key, bytes) in records {
            let sandbox_id_from_key = std::str::from_utf8(&key)
                .ok()
                .and_then(|value| SandboxId::parse_str(value).ok());
            let record = match decode_record(&bytes) {
                Ok(record) => record,
                Err(err) => {
                    warn!(record_key = %String::from_utf8_lossy(&key), error = %err, "discarding invalid paused sandbox record");
                    if let Some(sandbox_id) = sandbox_id_from_key {
                        let _ = self.remove_record(&sandbox_id).await;
                    }
                    outcomes.observe(LoadOutcome::Discarded);
                    continue;
                }
            };
            let sandbox_id = record.metadata.id;

            if record.lifecycle == PersistedPausedLifecycle::Resuming {
                warn!(sandbox_id = %sandbox_id, "discarding paused sandbox record left in resuming state");
                self.cleanup_invalid_record(&sandbox_id).await?;
                outcomes.observe(LoadOutcome::Discarded);
                continue;
            }

            if record.metadata.virtualization_mode != self.virtualization_mode {
                warn!(
                    sandbox_id = %sandbox_id,
                    record_mode = %record.metadata.virtualization_mode,
                    node_mode = %self.virtualization_mode,
                    "loading paused sandbox metadata without resumable runtime state because its virtualization mode is incompatible"
                );
                retained_artifacts.insert(sandbox_id);
                sandboxes.push(record.into_metadata_without_runtime_state());
                outcomes.observe(LoadOutcome::RetainedWithoutRuntimeState);
                continue;
            }

            match record.decode_runtime_metadata(factory) {
                Ok(metadata) => {
                    retained_artifacts.insert(sandbox_id);
                    sandboxes.push(metadata);
                    outcomes.observe(LoadOutcome::LoadedFromLocal);
                }
                Err(err) => {
                    self.retain_unusable_record(&sandbox_id, &err);
                    retained_artifacts.insert(sandbox_id);
                    sandboxes.push(record.into_metadata_without_runtime_state());
                    outcomes.observe(LoadOutcome::RetainedWithoutRuntimeState);
                }
            }
        }

        self.cleanup_orphan_artifacts(&retained_artifacts).await?;

        info!(
            loaded = sandboxes.len(),
            retained = retained_artifacts.len(),
            loaded_from_local = outcomes.loaded_from_local,
            retained_unreachable = outcomes.retained_unreachable,
            discarded = outcomes.discarded,
            "loaded paused sandbox records"
        );

        Ok(sandboxes)
    }

    async fn allocate_artifact_root(
        &self,
        sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>> {
        let artifact_root = self
            .sandbox_artifact_root(sandbox_id)
            .join(Uuid::now_v7().to_string());
        fs::create_dir_all(&artifact_root).await.map_err(|source| {
            SandboxPersistenceError::io(
                "allocate paused sandbox artifact root",
                &artifact_root,
                source,
            )
        })?;
        Ok(Some(artifact_root))
    }

    async fn persist_paused(
        &self,
        metadata: &SandboxMetadata,
        artifact_root: Option<&Path>,
        paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()> {
        let artifact_root = artifact_root.ok_or_else(|| SandboxPersistenceError::RuntimeState {
            reason: "file-backed persister requires an allocated artifact root",
        })?;
        debug!(
            sandbox_id = %metadata.id,
            artifact_root = %artifact_root.display(),
            "persisting paused sandbox"
        );
        let state = match paused_state.encode() {
            Ok(state) => state,
            Err(source) => {
                let _ = Self::remove_artifact_root(artifact_root).await;
                return Err(SandboxPersistenceError::InvalidRecord {
                    reason: "failed to encode paused sandbox state".to_string(),
                    source: Some(source),
                });
            }
        };
        let record = PersistedPausedRecord {
            version: RECORD_VERSION,
            lifecycle: PersistedPausedLifecycle::Paused,
            metadata: metadata.clone(),
            artifact_root: artifact_root.to_path_buf(),
            state,
        };
        let result = self.put_record(&record).await;
        if result.is_err() {
            let _ = Self::remove_artifact_root(artifact_root).await;
        }
        result
    }

    async fn mark_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "marking paused sandbox as resuming");
        let mut record = self.get_record(sandbox_id).await?;
        record.lifecycle = PersistedPausedLifecycle::Resuming;
        self.put_record(&record).await
    }

    async fn rollback_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "rolling back paused sandbox to paused");
        let mut record = self.get_record(sandbox_id).await?;
        record.lifecycle = PersistedPausedLifecycle::Paused;
        self.put_record(&record).await
    }

    async fn delete_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "deleting paused sandbox record");
        self.remove_record(sandbox_id).await
    }

    async fn delete_record_and_artifacts(&self, sandbox_id: &SandboxId) -> PersistenceResult<()> {
        debug!(sandbox_id = %sandbox_id, "deleting paused sandbox record and artifacts");
        self.remove_record(sandbox_id).await?;
        Self::remove_artifact_root(&self.sandbox_artifact_root(sandbox_id)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::{
        mock::{MockBackendFactory, MockSnapshot},
        FreshSandboxBuildSpec, PausedSandboxState, RuntimeArtifactSet, SandboxBackend,
        SandboxLaunchConfig,
    };
    use crate::snapshot::RunnableSnapshot;
    use anyhow::Result;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    #[derive(Debug)]
    struct FailingEncodeState;

    impl PausedSandboxState for FailingEncodeState {
        fn encode(&self) -> Result<Value> {
            anyhow::bail!("forced encode failure")
        }

        fn runtime_artifacts(&self) -> RuntimeArtifactSet {
            RuntimeArtifactSet::empty()
        }
    }

    #[derive(Default)]
    struct RejectingFactory;

    impl SandboxBackendFactory for RejectingFactory {
        fn build(
            &self,
            _build_spec: FreshSandboxBuildSpec,
            _launch_config: SandboxLaunchConfig,
        ) -> Result<Box<dyn SandboxBackend>> {
            unreachable!("persister tests only decode state")
        }

        fn build_from_snapshot(
            &self,
            _snapshot: &RunnableSnapshot,
            _launch_config: SandboxLaunchConfig,
        ) -> Result<Box<dyn SandboxBackend>> {
            unreachable!("persister tests only decode state")
        }

        fn build_from_paused_state(
            &self,
            _sandbox_id: SandboxId,
            _state: &dyn PausedSandboxState,
            _envd_access_token: Option<crate::sandbox::EnvdAccessToken>,
        ) -> Result<Box<dyn SandboxBackend>> {
            unreachable!("persister tests only decode state")
        }

        fn decode_paused_state(
            &self,
            _artifact_root: PathBuf,
            _state: Value,
        ) -> Result<Arc<dyn PausedSandboxState>> {
            anyhow::bail!("forced decode failure")
        }
    }

    fn paused_state(root: &Path) -> Arc<dyn PausedSandboxState> {
        std::fs::create_dir_all(root).expect("create test artifact root");
        Arc::new(MockSnapshot)
    }

    fn test_persister(root: &Path) -> FileBackedSandboxPersister {
        FileBackedSandboxPersister::new_for_test(root.to_path_buf())
            .with_durability(LocalStoreDurability::Memory)
    }

    async fn persist_test_record(
        persister: &FileBackedSandboxPersister,
        snapshot_root: &Path,
    ) -> anyhow::Result<(SandboxId, Arc<dyn PausedSandboxState>)> {
        let paused_state = paused_state(snapshot_root);
        let metadata = SandboxMetadata {
            id: SandboxId::new(),
            virtualization_mode: persister.virtualization_mode,
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };
        let sandbox_id = metadata.id;
        persister
            .persist_paused(&metadata, Some(snapshot_root), paused_state.as_ref())
            .await?;
        Ok((sandbox_id, paused_state))
    }

    async fn has_record(
        persister: &FileBackedSandboxPersister,
        sandbox_id: &SandboxId,
    ) -> anyhow::Result<bool> {
        Ok(persister
            .db()
            .await?
            .get(sandbox_id.to_string())
            .await?
            .is_some())
    }

    #[tokio::test]
    async fn file_persister_round_trips_paused_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata {
            timeout: Some(Duration::from_secs(5)),
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };

        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;
        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, metadata.id);
        assert!(loaded[0]
            .paused_state
            .as_ref()
            .expect("paused state should be restored")
            .downcast_ref::<MockSnapshot>()
            .is_some());
        Ok(())
    }

    #[tokio::test]
    async fn paused_record_from_other_mode_is_visible_but_not_resumable() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let kvm_persister = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        let (sandbox_id, _paused_state) =
            persist_test_record(&kvm_persister, &snapshot_root).await?;
        drop(kvm_persister);
        let pvm_persister =
            FileBackedSandboxPersister::new(temp.path().to_path_buf(), VirtualizationMode::Pvm)
                .with_durability(LocalStoreDurability::Memory);

        let loaded = pvm_persister.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, sandbox_id);
        assert_eq!(loaded[0].state, SandboxState::Paused);
        assert_eq!(loaded[0].virtualization_mode, VirtualizationMode::Kvm);
        assert!(loaded[0].paused_state.is_none());
        assert!(has_record(&pvm_persister, &sandbox_id).await?);
        assert!(snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn mixed_mode_records_are_both_visible_and_retained() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let kvm_persister = test_persister(temp.path());
        let kvm_root = temp.path().join("kvm-artifacts");
        let (kvm_id, _kvm_state) = persist_test_record(&kvm_persister, &kvm_root).await?;
        drop(kvm_persister);

        let pvm_persister =
            FileBackedSandboxPersister::new(temp.path().to_path_buf(), VirtualizationMode::Pvm)
                .with_durability(LocalStoreDurability::Memory);
        let pvm_root = temp.path().join("pvm-artifacts");
        let (pvm_id, _pvm_state) = persist_test_record(&pvm_persister, &pvm_root).await?;

        let mut loaded = pvm_persister.load_all(&MockBackendFactory::new()).await?;
        loaded.sort_by_key(|metadata| metadata.id);

        let kvm_metadata = loaded
            .iter()
            .find(|metadata| metadata.id == kvm_id)
            .expect("KVM metadata should remain visible");
        assert_eq!(kvm_metadata.virtualization_mode, VirtualizationMode::Kvm);
        assert!(kvm_metadata.paused_state.is_none());

        let pvm_metadata = loaded
            .iter()
            .find(|metadata| metadata.id == pvm_id)
            .expect("PVM metadata should load");
        assert_eq!(pvm_metadata.virtualization_mode, VirtualizationMode::Pvm);
        assert!(pvm_metadata.paused_state.is_some());

        assert!(has_record(&pvm_persister, &kvm_id).await?);
        assert!(has_record(&pvm_persister, &pvm_id).await?);
        assert!(kvm_root.exists());
        assert!(pvm_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn allocate_artifact_root_creates_unique_snapshot_roots() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();

        let first_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("file-backed persister should allocate artifact root");
        let second_root = persister
            .allocate_artifact_root(&sandbox_id)
            .await?
            .expect("file-backed persister should allocate artifact root");
        let sandbox_id_dir = sandbox_id.to_string();

        assert_ne!(first_root, second_root);
        assert!(first_root.is_dir());
        assert!(second_root.is_dir());
        assert_eq!(
            first_root.parent().and_then(Path::file_name),
            Some(std::ffi::OsStr::new(&sandbox_id_dir))
        );
        assert_eq!(
            first_root
                .parent()
                .and_then(Path::parent)
                .and_then(Path::file_name),
            Some(std::ffi::OsStr::new("artifacts"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn resuming_records_are_cleaned_on_load() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let snapshot_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join("snapshot");
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };

        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;
        persister.mark_resuming(&metadata.id).await?;

        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert!(loaded.is_empty());
        assert!(!has_record(&persister, &metadata.id).await?);
        assert!(!persister.sandbox_artifact_root(&metadata.id).exists());
        Ok(())
    }

    #[tokio::test]
    async fn persist_paused_accepts_backend_agnostic_state() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata::default();

        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;
        drop(paused_state);

        assert!(snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn persist_paused_cleans_artifacts_when_encode_fails() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        tokio::fs::create_dir_all(&snapshot_root).await?;
        let paused_state: Arc<dyn PausedSandboxState> = Arc::new(FailingEncodeState);
        let err = persister
            .persist_paused(
                &SandboxMetadata::default(),
                Some(&snapshot_root),
                paused_state.as_ref(),
            )
            .await
            .expect_err("encode failure should reject paused state");

        assert!(matches!(err, SandboxPersistenceError::InvalidRecord { .. }));
        assert!(!snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn mark_resuming_and_rollback_preserve_loadability() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        let (sandbox_id, _paused_state) = persist_test_record(&persister, &snapshot_root).await?;

        persister.mark_resuming(&sandbox_id).await?;
        assert_eq!(
            persister.get_record(&sandbox_id).await?.lifecycle,
            PersistedPausedLifecycle::Resuming
        );

        persister.rollback_resuming(&sandbox_id).await?;
        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, sandbox_id);
        assert!(snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn delete_record_removes_record_but_keeps_artifacts() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let snapshot_root = temp.path().join("artifacts");
        let (sandbox_id, _paused_state) = persist_test_record(&persister, &snapshot_root).await?;

        persister.delete_record(&sandbox_id).await?;

        assert!(!has_record(&persister, &sandbox_id).await?);
        assert!(snapshot_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn load_all_removes_orphan_artifacts_without_records() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let artifact_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join("resumed-generation");
        tokio::fs::create_dir_all(&artifact_root).await?;

        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert!(loaded.is_empty());
        assert!(!persister.sandbox_artifact_root(&sandbox_id).exists());
        Ok(())
    }

    #[tokio::test]
    async fn load_all_keeps_artifacts_for_valid_paused_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let snapshot_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join("paused-generation");
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };
        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;

        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert_eq!(loaded.len(), 1);
        assert!(persister.sandbox_artifact_root(&sandbox_id).exists());
        Ok(())
    }

    #[tokio::test]
    async fn delete_record_and_artifacts_removes_both() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let snapshot_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join("snapshot");
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };
        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;

        persister.delete_record_and_artifacts(&sandbox_id).await?;

        assert!(!has_record(&persister, &sandbox_id).await?);
        assert!(!persister.sandbox_artifact_root(&sandbox_id).exists());
        Ok(())
    }

    #[tokio::test]
    async fn delete_record_and_artifacts_removes_artifacts_without_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let sandbox_artifact_root = persister.sandbox_artifact_root(&sandbox_id);
        tokio::fs::create_dir_all(sandbox_artifact_root.join("stale-generation")).await?;

        persister.delete_record_and_artifacts(&sandbox_id).await?;

        assert!(!sandbox_artifact_root.exists());
        Ok(())
    }

    #[tokio::test]
    async fn delete_record_and_artifacts_removes_invalid_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        persister
            .db()
            .await?
            .put(sandbox_id.to_string(), b"not-json")
            .await?;

        persister.delete_record_and_artifacts(&sandbox_id).await?;

        assert!(!has_record(&persister, &sandbox_id).await?);
        Ok(())
    }

    #[tokio::test]
    async fn load_all_discards_invalid_record() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        persister
            .db()
            .await?
            .put(sandbox_id.to_string(), b"not-json")
            .await?;

        let loaded = persister.load_all(&MockBackendFactory::new()).await?;

        assert!(loaded.is_empty());
        assert!(!has_record(&persister, &sandbox_id).await?);
        Ok(())
    }

    /// A record this node cannot decode is not junk: its artifacts may be on
    /// another node, and only a record kept is a record a pod swap can resume.
    /// Deleting here is the step that turned a node replacement into permanent
    /// loss, so the record and its artifacts must both survive the load.
    ///
    /// This replaces the previous assertion that such a record was discarded
    /// and its artifacts removed.
    #[tokio::test]
    async fn load_all_retains_record_whose_runtime_state_is_unavailable() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let sandbox_id = SandboxId::new();
        let snapshot_root = persister
            .sandbox_artifact_root(&sandbox_id)
            .join("snapshot");
        let paused_state = paused_state(&snapshot_root);
        let metadata = SandboxMetadata {
            id: sandbox_id,
            paused_state: Some(Arc::clone(&paused_state)),
            ..Default::default()
        };
        persister
            .persist_paused(&metadata, Some(&snapshot_root), paused_state.as_ref())
            .await?;

        let loaded = persister.load_all(&RejectingFactory).await?;

        let retained = loaded
            .iter()
            .find(|loaded| loaded.id == sandbox_id)
            .expect("a record whose runtime state is unavailable must stay listed");
        assert_eq!(retained.state, SandboxState::Paused);
        assert!(
            retained.paused_state.is_none(),
            "a record without decodable runtime state must not advertise one"
        );
        assert!(has_record(&persister, &sandbox_id).await?);
        assert!(snapshot_root.exists());
        Ok(())
    }

    /// The three outcomes must stay distinguishable: one that loaded, one whose
    /// runtime state is unavailable here, and one that is genuinely corrupt.
    /// The corrupt one keeps its cleanup, so this test also pins that the
    /// retention above did not simply disable the cleanup path.
    #[tokio::test]
    async fn load_all_distinguishes_loaded_retained_and_discarded_records() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let persister = test_persister(temp.path());
        let loaded_root = temp.path().join("loaded-artifacts");
        let (loaded_id, _loaded_state) = persist_test_record(&persister, &loaded_root).await?;

        let unavailable_id = SandboxId::new();
        let unavailable_root = persister
            .sandbox_artifact_root(&unavailable_id)
            .join("snapshot");
        let unavailable_state = paused_state(&unavailable_root);
        let unavailable_metadata = SandboxMetadata {
            id: unavailable_id,
            paused_state: Some(Arc::clone(&unavailable_state)),
            ..Default::default()
        };
        persister
            .persist_paused(
                &unavailable_metadata,
                Some(&unavailable_root),
                unavailable_state.as_ref(),
            )
            .await?;

        let corrupt_id = SandboxId::new();
        persister
            .db()
            .await?
            .put(corrupt_id.to_string(), b"not-json")
            .await?;

        let mut loaded = persister.load_all(&MockBackendFactory::new()).await?;
        loaded.sort_by_key(|metadata| metadata.id);

        assert_eq!(loaded.len(), 2, "loaded and retained records stay listed");
        assert!(loaded.iter().any(|metadata| metadata.id == loaded_id));

        let unavailable = loaded
            .iter()
            .find(|metadata| metadata.id == unavailable_id)
            .expect("the unavailable record stays listed");
        assert!(unavailable.paused_state.is_none());

        assert!(!has_record(&persister, &corrupt_id).await?);
        assert!(has_record(&persister, &unavailable_id).await?);
        assert!(unavailable_root.exists());
        Ok(())
    }

    #[test]
    fn load_outcome_labels_are_a_closed_set() {
        let labels = [
            LoadOutcome::LoadedFromLocal.as_str(),
            LoadOutcome::RetainedWithoutRuntimeState.as_str(),
            LoadOutcome::Discarded.as_str(),
        ];

        let unique: HashSet<_> = labels.iter().copied().collect();
        assert_eq!(unique.len(), labels.len(), "labels must not collide");
        assert!(
            labels.iter().all(|label| label.is_ascii()),
            "label values stay greppable in metrics output"
        );
    }
}
