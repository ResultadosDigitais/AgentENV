//! Live sandboxes, as the managed-layer collector needs them.
//!
//! A sandbox resumed from a snapshot keeps reading that snapshot's layers on
//! demand, so the collector must keep those layers cited even when no record
//! says the sandbox exists. `maintenance` cannot read the orchestrator itself
//! (`orchestrator` depends on every catalog, so importing it back would cycle),
//! so the API layer supplies this reader.
//!
//! One node's list is not enough. A sandbox paused on another node still reads
//! its source snapshot's layers, and the collector runs on a single node: a
//! citation set built from this node alone deletes exactly those layers, which
//! is what happened on staging. [`ClusterLiveSandboxes`] therefore reads the
//! gateway's aggregated list, which fans out to every node, and unions it with
//! this node's own list.

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{bail, Context};
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

/// The cluster's sandboxes, as the gateway aggregates them.
///
/// `Ok` means the list covers every node. `Err` means it does not: a node in the
/// fan-out failed, a page did not arrive, or the gateway is not configured.
/// Callers must never read a short list as a complete one — that is the mistake
/// that collected live layers.
#[async_trait]
pub trait ClusterSandboxList: Send + Sync {
    /// The source snapshot ID of every sandbox in the cluster, running or
    /// paused, in no particular order.
    async fn list_snapshot_origins(&self) -> anyhow::Result<Vec<String>>;
}

/// The collector's live-sandbox reader: the whole cluster, or an error.
///
/// The union of the gateway's aggregated list and this node's own list. The
/// local list is redundant when the gateway answers, and it is what still
/// protects this node if the gateway omits one; it is never a substitute for the
/// cluster, because a pass that cannot see the other nodes cannot prove any
/// layer uncited.
pub struct ClusterLiveSandboxes {
    cluster: Option<Arc<dyn ClusterSandboxList>>,
    local: Arc<dyn LiveSandboxes>,
}

impl ClusterLiveSandboxes {
    /// A reader whose `local` half is always present and whose `cluster` half is
    /// `None` when no gateway base URL is configured.
    pub fn new(local: Arc<dyn LiveSandboxes>, cluster: Option<Arc<dyn ClusterSandboxList>>) -> Self {
        Self { cluster, local }
    }
}

#[async_trait]
impl LiveSandboxes for ClusterLiveSandboxes {
    async fn live_snapshot_origins(&self) -> anyhow::Result<Vec<String>> {
        let Some(cluster) = &self.cluster else {
            bail!(
                "the cluster gateway is not configured, so sandboxes on other nodes cannot be listed"
            );
        };
        let cluster = cluster
            .list_snapshot_origins()
            .await
            .context("the cluster sandbox list is incomplete")?;
        let local = self
            .local
            .live_snapshot_origins()
            .await
            .context("this node's sandbox list could not be read")?;
        let union: BTreeSet<String> = cluster.into_iter().chain(local).collect();
        Ok(union.into_iter().collect())
    }
}
