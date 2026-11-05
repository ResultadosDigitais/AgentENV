//! Firecracker-based sandbox backend implementation.
//!
//! Provides [`FirecrackerSandbox`] (the concrete VM-backed sandbox) and
//! [`FirecrackerSandboxFactory`] which wires sandbox configuration from the
//! global [`ConfigManager`][crate::cfg::ConfigManager].

mod config;
mod connector;
mod factory;
mod instance;
mod mmds;
mod overlaybd_snapshot;
mod pool;
mod process_vm_reader;
mod resources;
mod sandbox;
mod socket;
mod startup_pack;

pub use config::{
    FirecrackerCommonConfig, FirecrackerRuntimePolicy, FirecrackerSandboxConfig,
    FirecrackerSnapshotConfig,
};
pub use factory::FirecrackerSandboxFactory;
pub(super) use instance::FirecrackerInstance;
pub use pool::FirecrackerPool;
pub use sandbox::{FirecrackerCaptureArtifacts, FirecrackerPausedState, FirecrackerSandbox};
// Re-exported for the orchestrator's recoverable-pause branch: it must recognize
// the named chain-ceiling refusal by error type, never by message text, so the
// classification cannot drift from the code that produces it.
#[cfg(test)]
pub(crate) use overlaybd_snapshot::chain_ceiling_error;
pub(crate) use overlaybd_snapshot::is_chain_bytes_ceiling_exceeded;
pub(crate) use startup_pack::record_startup_pack;
