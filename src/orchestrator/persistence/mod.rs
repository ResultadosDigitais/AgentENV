mod file_backed;
#[cfg(test)]
mod mock;
pub mod paused_store_cost;

use async_trait::async_trait;
use std::path::{Path, PathBuf};

use crate::orchestrator::store::SandboxMetadata;
use crate::sandbox::{PausedSandboxState, SandboxBackendFactory};
use crate::types::SandboxId;

pub(crate) use file_backed::begin_graceful_shutdown;
pub use file_backed::FileBackedSandboxPersister;
pub(crate) use file_backed::{QUARANTINE_DIR, QUARANTINE_TTL};
#[cfg(test)]
pub(crate) use mock::{RecordingCall, RecordingPersister};

pub type PersistenceResult<T> = std::result::Result<T, SandboxPersistenceError>;

/// One `Paused` record the deferred-compaction worker may act on.
///
/// Carries no runtime state: the worker only needs the generation directory and
/// the image config paths the encoded state names, and decoding the whole
/// backend state per tick would be far more expensive than the pass itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PausedArtifactCandidate {
    pub sandbox_id: SandboxId,
    pub artifact_root: PathBuf,
    /// Every image config the paused state names (`mem_image.json`, `rootfs/image.json`,
    /// a drive's or volume's `image.json`). A descriptor lives beside its config.
    pub config_paths: Vec<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub enum SandboxPersistenceError {
    #[error("failed to {operation} {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid sandbox record: {reason}")]
    InvalidRecord {
        reason: String,
        #[source]
        source: Option<anyhow::Error>,
    },
    #[error("invalid paused sandbox runtime state: {reason}")]
    RuntimeState { reason: &'static str },
    #[error("paused sandbox store operation failed: {operation}: {source}")]
    Store {
        operation: &'static str,
        #[source]
        source: anyhow::Error,
    },
}

impl SandboxPersistenceError {
    pub(super) fn io(
        operation: &'static str,
        path: impl Into<PathBuf>,
        source: std::io::Error,
    ) -> Self {
        Self::Io {
            operation,
            path: path.into(),
            source,
        }
    }

    pub(super) fn store(operation: &'static str, source: anyhow::Error) -> Self {
        Self::Store { operation, source }
    }
}

impl<S, F, P> crate::orchestrator::Orchestrator<S, F, P>
where
    S: crate::orchestrator::MetadataStore,
    F: SandboxBackendFactory,
    P: SandboxPersister,
{
    /// The paused artifacts root this node's persister owns, when it owns one.
    ///
    /// The paused-artifact sweep runs over it; exposing it here keeps the
    /// persister's layout out of every caller that only needs the directory,
    /// and out of `service.rs`, which is already past the file-size ceiling.
    pub fn artifacts_root(&self) -> Option<PathBuf> {
        self.persister.artifacts_root()
    }

    /// The `Paused` records the deferred-compaction worker may act on, with the
    /// image config paths each one names.
    ///
    /// A fresh read per pass is what keeps the worker from touching a generation
    /// a resume already moved: a record that is no longer `Paused` is simply not
    /// returned.
    pub async fn paused_artifact_candidates(
        &self,
    ) -> PersistenceResult<Vec<PausedArtifactCandidate>> {
        self.persister.paused_artifact_candidates().await
    }
}

#[async_trait]
/// Persistence interface for sandbox records and artifacts.
pub trait SandboxPersister: Send + Sync {
    /// Load all persisted sandbox metadata.
    async fn load_all<F>(&self, factory: &F) -> PersistenceResult<Vec<SandboxMetadata>>
    where
        F: SandboxBackendFactory;

    /// Allocate an UNIQUE directory for sandbox artifacts.
    ///
    /// `None` means persistence is disabled and the sandbox backend should manage
    /// the lifecycle of its temporary artifacts.
    async fn allocate_artifact_root(
        &self,
        sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>>;

    /// Persist metadata and runtime state for a paused sandbox.
    async fn persist_paused(
        &self,
        metadata: &SandboxMetadata,
        artifact_root: Option<&Path>,
        paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()>;

    /// Mark a paused sandbox as resuming.
    async fn mark_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Roll back a resuming mark after a failed resume attempt.
    async fn rollback_resuming(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Delete the persistence record for a sandbox.
    async fn delete_record(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// Delete the persistence record and all associated artifacts.
    async fn delete_record_and_artifacts(&self, sandbox_id: &SandboxId) -> PersistenceResult<()>;

    /// The directory holding one entry per sandbox id, or `None` when this
    /// persister keeps no artifacts of its own.
    ///
    /// The paused-artifact sweep needs the root to reclaim a directory no
    /// sandbox owns any more, and it must not learn the layout to get it: it
    /// asks the persister, which is the only owner of the arrangement.
    fn artifacts_root(&self) -> Option<PathBuf> {
        None
    }

    /// The `Paused` records the deferred-compaction worker may act on.
    ///
    /// The default answers empty: a persister that keeps no records of its own
    /// has nothing to defer, and the worker then has nothing to do.
    async fn paused_artifact_candidates(&self) -> PersistenceResult<Vec<PausedArtifactCandidate>> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
pub struct DisabledSandboxPersister;

#[async_trait]
impl SandboxPersister for DisabledSandboxPersister {
    async fn load_all<F>(&self, _factory: &F) -> PersistenceResult<Vec<SandboxMetadata>>
    where
        F: SandboxBackendFactory,
    {
        Ok(Vec::new())
    }

    async fn allocate_artifact_root(
        &self,
        _sandbox_id: &SandboxId,
    ) -> PersistenceResult<Option<PathBuf>> {
        Ok(None)
    }

    async fn persist_paused(
        &self,
        _metadata: &SandboxMetadata,
        _artifact_root: Option<&Path>,
        _paused_state: &dyn PausedSandboxState,
    ) -> PersistenceResult<()> {
        Ok(())
    }

    async fn mark_resuming(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }

    async fn rollback_resuming(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }

    async fn delete_record(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }

    async fn delete_record_and_artifacts(&self, _sandbox_id: &SandboxId) -> PersistenceResult<()> {
        Ok(())
    }
}
