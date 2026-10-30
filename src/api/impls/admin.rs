use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use crate::maintenance::{
    collect_managed_layers, ManagedLayerCollectMode, ManagedLayerCollectReport,
    ManagedLayerCollectRequest, ManagedLayerCollectorError, OrchestratorLiveSandboxes,
};
use crate::observability::{DiskMetric, MachineInfo, NodeMetricsSnapshot, NodeSnapshot};
use agentenv_http_server::{apis::admin::*, models};

use super::ApiImpl;

/// Whether a report pass has run in this process, which enforce requires.
///
/// Enforce is irreversible: the bucket has no trash, and a layer cannot be
/// rebuilt without its origin. Requiring one report first makes an operator see
/// what a pass would remove before it removes anything.
static REPORT_RAN: AtomicBool = AtomicBool::new(false);

impl From<MachineInfo> for models::MachineInfo {
    fn from(machine_info: MachineInfo) -> Self {
        models::MachineInfo::new(
            machine_info.cpu_family,
            machine_info.cpu_model,
            machine_info.cpu_model_name,
            machine_info.cpu_architecture,
        )
    }
}

impl From<DiskMetric> for models::DiskMetrics {
    fn from(disk: DiskMetric) -> Self {
        models::DiskMetrics::new(
            disk.mount_point,
            disk.device,
            disk.filesystem_type,
            disk.used_bytes,
            disk.total_bytes,
        )
    }
}

impl From<NodeMetricsSnapshot> for models::NodeMetrics {
    fn from(metrics: NodeMetricsSnapshot) -> Self {
        models::NodeMetrics::new(
            metrics.allocated_cpu,
            metrics.cpu_percent,
            metrics.cpu_count,
            metrics.allocated_memory_bytes,
            metrics.memory_used_bytes,
            metrics.memory_total_bytes,
            metrics
                .disks
                .into_iter()
                .map(models::DiskMetrics::from)
                .collect(),
            metrics.paused_allocated_cpu,
            metrics.paused_allocated_memory_bytes,
        )
    }
}

impl From<NodeSnapshot> for models::Node {
    fn from(node: NodeSnapshot) -> Self {
        models::Node::new(
            node.version,
            node.commit,
            node.node_id,
            node.service_instance_id,
            node.cluster_id.to_string(),
            node.machine_info.into(),
            models::NodeStatus::NodeStatusReady,
            node.sandbox_count,
            node.metrics.into(),
            node.create_successes,
            node.create_fails,
            node.sandbox_starting_count,
            node.paused_sandbox_count,
        )
    }
}

#[async_trait]
impl Admin<()> for ApiImpl {
    type Claims = super::Claims;

    async fn admin_managed_layers_collect_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &models::ManagedLayerCollectRequest,
    ) -> Result<AdminManagedLayersCollectPostResponse, ()> {
        match self.run_managed_layer_collect(body).await {
            Ok(report) => Ok(
                AdminManagedLayersCollectPostResponse::Status200_SuccessfullyRanOneCollectionPass(
                    report.into(),
                ),
            ),
            // The refusals this endpoint decides itself (disabled on this node,
            // another pass running, enforce before a report) are conflicts; a
            // backend failure keeps its own status.
            Err(error) => Ok(match error.code {
                400 => AdminManagedLayersCollectPostResponse::Status400_BadRequest(error),
                403 => AdminManagedLayersCollectPostResponse::Status403_Forbidden(error),
                409 => {
                    AdminManagedLayersCollectPostResponse::Status409_ThePassIsDisabledOnThisNode(
                        error,
                    )
                }
                _ => AdminManagedLayersCollectPostResponse::Status500_ServerError(error),
            }),
        }
    }

    async fn nodes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::NodesGetQueryParams,
    ) -> Result<NodesGetResponse, ()> {
        let Some(observability) = self.observability() else {
            // When observability is disabled, the collection endpoint exposes
            // no nodes rather than returning a partial or synthetic record.
            return Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
                vec![],
            ));
        };
        if query_params
            .cluster_id
            .is_some_and(|cluster_id| cluster_id != observability.cluster_id())
        {
            return Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
                vec![],
            ));
        }
        let node = match observability.node_snapshot().await {
            Ok(node) => node,
            Err(err) => {
                return Ok(NodesGetResponse::Status500_ServerError(Self::error(
                    500,
                    err.to_string(),
                )));
            }
        };
        Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
            vec![models::Node::from(node)],
        ))
    }

    async fn nodes_node_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::NodesNodeIdGetPathParams,
        query_params: &models::NodesNodeIdGetQueryParams,
    ) -> Result<NodesNodeIdGetResponse, ()> {
        let Some(observability) = self.observability() else {
            // A disabled observability service behaves like node details are
            // unavailable on this process.
            return Ok(NodesNodeIdGetResponse::Status404_NotFound(Self::error(
                404,
                "observability is disabled on this node",
            )));
        };
        let cluster_mismatch = query_params
            .cluster_id
            .map(|cluster_id| cluster_id != observability.cluster_id())
            .unwrap_or(false);
        if path_params.node_id != observability.node_id() || cluster_mismatch {
            return Ok(NodesNodeIdGetResponse::Status404_NotFound(Self::error(
                404,
                format!("node {} not found", path_params.node_id),
            )));
        }

        let node = match observability.node_snapshot().await {
            Ok(node) => node,
            Err(err) => {
                return Ok(NodesNodeIdGetResponse::Status500_ServerError(Self::error(
                    500,
                    err.to_string(),
                )));
            }
        };

        let detail = models::NodeDetail::new(
            node.cluster_id.to_string(),
            node.version,
            node.commit,
            node.node_id,
            node.service_instance_id,
            node.machine_info.into(),
            models::NodeStatus::NodeStatusReady,
            node.sandbox_count,
            node.metrics.into(),
            node.create_successes,
            node.create_fails,
            node.paused_sandbox_count,
        );
        Ok(NodesNodeIdGetResponse::Status200_SuccessfullyReturnedTheNode(detail))
    }
}

impl ApiImpl {
    /// Runs one collection pass, or returns the client-facing error explaining
    /// why it did not run.
    async fn run_managed_layer_collect(
        &self,
        body: &models::ManagedLayerCollectRequest,
    ) -> Result<ManagedLayerCollectReport, models::Error> {
        let config = crate::cfg::ConfigManager::global_config();
        let collect = &config.snapshot.managed_layer_collect;
        if !collect.enabled {
            return Err(Self::error(
                409,
                "managed layer collection is disabled; set \
                 AENV_SNAPSHOT_MANAGED_LAYER_COLLECT_ENABLED=true to enable it",
            ));
        }
        let mode = match body.mode {
            models::ManagedLayerCollectMode::Report => ManagedLayerCollectMode::Report,
            models::ManagedLayerCollectMode::Enforce => ManagedLayerCollectMode::Enforce,
        };
        // Enforce is irreversible, so an operator must have seen a report from
        // this process first.
        if mode == ManagedLayerCollectMode::Enforce && !REPORT_RAN.load(Ordering::SeqCst) {
            return Err(Self::error(
                409,
                "enforce requires a report pass in this server process first",
            ));
        }
        let grace = body.grace_seconds.map_or_else(
            || Duration::from_secs(collect.grace_secs),
            Duration::from_secs,
        );
        let live = OrchestratorLiveSandboxes::new(self.orchestrator());
        let repository = self.repository();
        let report = collect_managed_layers(
            repository.as_ref(),
            &live,
            &ManagedLayerCollectRequest { mode, grace },
        )
        .await
        .map_err(|error| match error {
            ManagedLayerCollectorError::Locked => {
                Self::error(409, "another managed-layer collection pass is running")
            }
            ManagedLayerCollectorError::ReportRequired => {
                Self::error(409, "enforce requires a report pass first")
            }
            ManagedLayerCollectorError::Repository(error) => Self::repository_error(&error),
        })?;
        if mode == ManagedLayerCollectMode::Report {
            REPORT_RAN.store(true, Ordering::SeqCst);
        }
        Ok(report)
    }
}

impl From<ManagedLayerCollectMode> for models::ManagedLayerCollectMode {
    fn from(mode: ManagedLayerCollectMode) -> Self {
        match mode {
            ManagedLayerCollectMode::Report => Self::Report,
            ManagedLayerCollectMode::Enforce => Self::Enforce,
        }
    }
}

impl From<ManagedLayerCollectReport> for models::ManagedLayerCollectReport {
    fn from(report: ManagedLayerCollectReport) -> Self {
        models::ManagedLayerCollectReport {
            mode: report
                .mode
                .unwrap_or(ManagedLayerCollectMode::Report)
                .into(),
            cited_layers: report.cited_layers,
            cited_bytes: report.cited_bytes,
            orphan_layers: report.orphan_layers,
            orphan_bytes: report.orphan_bytes,
            unexpired_layers: report.unexpired_layers,
            unknown_layers: report.unknown_layers,
            unreadable_layers: report.unreadable_layers,
            deleted_layers: report.deleted_layers,
            reclaimed_bytes: report.reclaimed_bytes,
            downgraded_reason: report.downgraded_reason,
            failures: (!report.failures.is_empty()).then(|| {
                report
                    .failures
                    .into_iter()
                    .map(|failure| {
                        models::ManagedLayerCollectFailure::new(failure.key, failure.message)
                    })
                    .collect()
            }),
        }
    }
}
