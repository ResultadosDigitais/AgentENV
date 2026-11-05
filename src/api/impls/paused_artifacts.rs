//! The node's periodic paused-artifact sweep.
//!
//! A sandbox that ended without its record being removed leaves its artifact
//! directory on the node SSD for good. The sweep reclaims it, but only when the
//! cluster list proves no node holds the sandbox any more; see
//! [`crate::maintenance::paused_artifacts`] for the verdicts.
//!
//! The task lives here, next to the server's other periodic work, because it
//! needs the API's gateway URL and API key and the orchestrator's live sandbox
//! list. The pass itself stays in `maintenance`, which knows neither.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use tracing::{info, warn};

use super::ApiImpl;
use crate::cfg::PausedArtifactReapMode;
use crate::maintenance::{
    sweep, ClusterSandboxIds, GatewaySandboxList, LocalSandbox, PausedArtifactSubject, ReapMode,
    SWEEP_INTERVAL,
};
use crate::orchestrator::{SandboxListFilter, SandboxState};
use crate::types::SandboxId;

/// The node as the sweep sees it: the persister's artifacts root plus this
/// node's sandboxes, read fresh on every pass.
pub(crate) struct NodePausedArtifacts {
    api: Arc<ApiImpl>,
}

#[async_trait]
impl PausedArtifactSubject for NodePausedArtifacts {
    fn artifacts_root(&self) -> Option<std::path::PathBuf> {
        self.api.orchestrator().artifacts_root()
    }

    async fn local_sandboxes(&self) -> anyhow::Result<Vec<LocalSandbox>> {
        let orchestrator = self.api.orchestrator();
        // `list_sandbox_ids` covers running, paused, restoring and
        // template-build ids; the filter says which of them is restoring its
        // artifacts from this node right now.
        let restoring: HashSet<SandboxId> = orchestrator
            .list_sandboxes_filtered(SandboxListFilter {
                states: Some(vec![SandboxState::Resuming]),
                ..SandboxListFilter::matches_all()
            })
            .await?
            .into_iter()
            .map(|metadata| metadata.id)
            .collect();
        Ok(orchestrator
            .list_sandbox_ids()
            .await?
            .into_iter()
            .map(|id| LocalSandbox {
                id,
                restoring: restoring.contains(&id),
            })
            .collect())
    }
}

impl ApiImpl {
    /// Starts the paused-artifact sweep, or returns `None` when the kill switch
    /// is off.
    ///
    /// The task holds a `Weak` handle so a shutting-down server is not kept
    /// alive by it, and it waits one interval before the first pass: the boot
    /// sweep inside `load_all` has already reclaimed the records it could see.
    pub fn start_paused_artifact_reap(
        self: &Arc<Self>,
        mode: PausedArtifactReapMode,
        gateway_url: Option<&str>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let reap_mode = reap_mode(mode, gateway_url)?;
        let cluster: Option<Arc<dyn ClusterSandboxIds>> = match gateway_url {
            Some(url) => match GatewaySandboxList::new(url, self.api_key().clone()) {
                Ok(gateway) => Some(Arc::new(gateway)),
                Err(error) => {
                    // A malformed URL must not take the whole node down; the
                    // pass then runs without the cluster, which already
                    // downgrades it to report, and the reason is never lost.
                    warn!(
                        error = %format_args!("{error:#}"),
                        "the paused-artifact sweep has an invalid cluster gateway URL, so it runs without the cluster"
                    );
                    None
                }
            },
            None => None,
        };

        let api = Arc::downgrade(self);
        info!(
            mode = reap_mode.as_str(),
            interval_secs = SWEEP_INTERVAL.as_secs(),
            cluster = cluster.is_some(),
            "paused-artifact sweep task started"
        );
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(SWEEP_INTERVAL).await;
                let Some(api) = api.upgrade() else {
                    break;
                };
                let subject = NodePausedArtifacts { api };
                match sweep(&subject, cluster.as_deref(), reap_mode, SystemTime::now()).await {
                    Ok(report) => {
                        if let Some(reason) = &report.downgraded_reason {
                            warn!(mode = reap_mode.as_str(), reason = %reason, "paused-artifact sweep could not enforce");
                        }
                    }
                    // One failed pass must not stop the task: the next tick
                    // retries, and the failure is never swallowed.
                    Err(error) => {
                        warn!(error = %format_args!("{error:#}"), "paused-artifact sweep failed; will retry");
                    }
                }
            }
        }))
    }
}

/// The mode a task started with these inputs actually runs in, or `None` when
/// it must not start at all.
///
/// A pass without the cluster can prove no directory orphaned, so enforce would
/// move nothing on every tick and log the same reason forever. It is downgraded
/// once, at startup, and said out loud.
pub(crate) fn reap_mode(
    mode: PausedArtifactReapMode,
    gateway_url: Option<&str>,
) -> Option<ReapMode> {
    match mode {
        PausedArtifactReapMode::Off => None,
        PausedArtifactReapMode::Report => Some(ReapMode::Report),
        PausedArtifactReapMode::Enforce if gateway_url.is_none() => {
            warn!(
                "the paused-artifact sweep is configured to enforce, but no cluster gateway URL \
                 is set, so sandboxes on other nodes cannot be listed; running in report mode \
                 instead"
            );
            Some(ReapMode::Report)
        }
        PausedArtifactReapMode::Enforce => Some(ReapMode::Enforce),
    }
}

#[cfg(test)]
mod tests;
