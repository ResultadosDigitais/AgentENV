//! Repository maintenance operations that no single resource catalog owns.
//!
//! These live in their own top-level module on purpose: collecting a managed
//! layer needs to read snapshot records, volume records and published image
//! descriptions at once, and `repository` may not import any of the catalogs
//! built on it, while `snapshot` may not import `volume`. Keeping the reader
//! here keeps every other module's dependency direction intact.

mod live_sandboxes;
mod managed_layers;

pub use live_sandboxes::OrchestratorLiveSandboxes;
pub use managed_layers::{
    collect_managed_layers, LiveSandboxes, ManagedLayerCollectFailure, ManagedLayerCollectMode,
    ManagedLayerCollectReport, ManagedLayerCollectRequest, ManagedLayerCollectorError,
};
