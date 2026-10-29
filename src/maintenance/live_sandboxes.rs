//! The node's live sandboxes, as the managed-layer collector needs them.
//!
//! A sandbox resumed from a snapshot keeps reading that snapshot's layers on
//! demand, so the collector must keep those layers cited even when no record
//! says the sandbox exists. `maintenance` cannot read the orchestrator itself
//! (`orchestrator` depends on every catalog, so importing it back would cycle),
//! so the API layer supplies this reader.

use std::sync::Arc;

use async_trait::async_trait;

use super::LiveSandboxes;
use crate::orchestrator::Orchestrator;

/// Reads the source snapshot of every sandbox this node currently holds.
///
/// Both running and paused sandboxes count: a paused sandbox resumes from its
/// snapshot's layers, so collecting them would break the resume.
pub struct OrchestratorLiveSandboxes {
    orchestrator: Arc<Orchestrator>,
}

impl OrchestratorLiveSandboxes {
    pub fn new(orchestrator: Arc<Orchestrator>) -> Self {
        Self { orchestrator }
    }
}

#[async_trait]
impl LiveSandboxes for OrchestratorLiveSandboxes {
    async fn live_snapshot_origins(&self) -> anyhow::Result<Vec<String>> {
        Ok(self
            .orchestrator
            .list_sandboxes()
            .await?
            .into_iter()
            .map(|metadata| metadata.snapshot_id)
            .collect())
    }
}
