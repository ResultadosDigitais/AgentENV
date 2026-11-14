//! Repository maintenance operations that no single resource catalog owns.
//!
//! These live in their own top-level module on purpose: collecting a managed
//! layer needs to read snapshot records, volume records and published image
//! descriptions at once, and `repository` may not import any of the catalogs
//! built on it, while `snapshot` may not import `volume`. Keeping the reader
//! here keeps every other module's dependency direction intact.

pub mod behavior_table;
mod deferred_compaction;
mod gateway_sandboxes;
mod live_sandboxes;
mod managed_layers;
mod paused_artifacts;

pub use deferred_compaction::{
    DeferredCompactionReport, DeferredCompactionWorker, DEFERRED_COMPACTION_INTERVAL,
    DEFERRED_COMPACTION_LOG,
};

pub use gateway_sandboxes::GatewaySandboxList;
pub use live_sandboxes::{
    ClusterLiveSandboxes, ClusterSandboxIds, ClusterSandboxList, OrchestratorLiveSandboxes,
};
pub use managed_layers::{
    collect_managed_layers, LiveSandboxes, ManagedLayerCollectFailure, ManagedLayerCollectMode,
    ManagedLayerCollectReport, ManagedLayerCollectRequest, ManagedLayerCollectorError,
};
pub use paused_artifacts::{
    sweep, ArtifactSweepFailure, ArtifactSweepReport, ArtifactVerdict, KeepReason, LocalSandbox,
    PausedArtifactSubject, ReapMode, SkipReason, ARTIFACT_GRACE, SWEEP_INTERVAL, TRASH_DIR,
    TRASH_GRACE,
};
