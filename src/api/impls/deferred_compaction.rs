//! The node's periodic deferred-compaction task.
//!
//! A pause that crosses a chain bound no longer compacts inline; it leaves a
//! descriptor and returns (OPSAI-7467). This task is what executes those
//! descriptors afterwards. It lives next to the server's other periodic work
//! because it needs the orchestrator's paused records; the pass itself stays in
//! [`crate::maintenance::deferred_compaction`], which knows neither the API nor
//! the record store.

use std::sync::Arc;

use tracing::{info, warn};

use super::ApiImpl;
use crate::maintenance::{DeferredCompactionWorker, DEFERRED_COMPACTION_INTERVAL};
use crate::overlaybd::DeferredCompactionGate;

impl ApiImpl {
    /// Starts the deferred-compaction task.
    ///
    /// Returns `None` when the persister keeps no artifacts of its own: without
    /// a generation directory there is nothing a pause could have deferred.
    /// The task holds a `Weak` handle so a shutting-down server is not kept
    /// alive by it.
    pub fn start_deferred_compaction(
        self: &Arc<Self>,
        max_concurrent: usize,
        pending_bytes_budget: u64,
    ) -> Option<tokio::task::JoinHandle<()>> {
        self.orchestrator().artifacts_root()?;
        let worker = Arc::new(DeferredCompactionWorker::new(
            DeferredCompactionGate::global(),
            max_concurrent,
            pending_bytes_budget,
        ));
        let api = Arc::downgrade(self);
        info!(
            interval_secs = DEFERRED_COMPACTION_INTERVAL.as_secs(),
            max_concurrent, pending_bytes_budget, "deferred compaction task started"
        );
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(DEFERRED_COMPACTION_INTERVAL).await;
                let Some(api) = api.upgrade() else {
                    break;
                };
                let candidates = match api.orchestrator().paused_artifact_candidates().await {
                    Ok(candidates) => candidates,
                    Err(error) => {
                        // One failed read must not stop the task: the next tick
                        // retries and the failure is never swallowed.
                        warn!(
                            error = %format_args!("{error:#}"),
                            "deferred compaction could not read the paused records; will retry"
                        );
                        continue;
                    }
                };
                let report = worker.run_once(&candidates).await;
                if report != Default::default() {
                    info!(
                        compacted = report.compacted,
                        failed = report.failed,
                        headroom_refused = report.headroom_refused,
                        queued = report.queued,
                        unlinked = report.unlinked,
                        "deferred compaction pass finished"
                    );
                }
            }
        }))
    }
}
