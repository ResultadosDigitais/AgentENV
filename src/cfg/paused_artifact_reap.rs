//! The paused-artifact sweep's kill switch.
//!
//! It lives in its own submodule so `cfg.rs`, already past the file-size
//! ceiling, grows by one field and not by an enum.

use serde::Deserialize;

/// The three states of the paused-artifact sweep.
///
/// `off` starts no task at all. `report` lists and counts every verdict without
/// moving or deleting anything, which is how a node is measured before enforce.
/// `enforce` moves an orphan directory to `artifacts/.trash/` and deletes it
/// after a second grace. The default is `off`, so upgrading changes nothing
/// until a rollout turns it on node by node.
#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PausedArtifactReapMode {
    /// No task runs; the artifacts root is never scanned.
    #[default]
    Off,
    /// Every verdict is listed and counted; nothing is moved or deleted.
    Report,
    /// An orphan directory moves to the trash, and a trash entry past the
    /// second grace is deleted.
    Enforce,
}
