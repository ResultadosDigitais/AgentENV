//! A node-local lease that fences a paused generation against the deferred
//! chain-compaction worker while its artifacts are published.
//!
//! Publishing reads the generation's image configs and layer files directly.
//! The worker rewrites a config's inputs and unlinks them one pass later, so a
//! publication that overlaps a pass could read a config that is about to point
//! at unlinked bytes. The lease closes that window from the other side: the
//! worker skips any candidate whose generation holds a lease, and the lease is
//! created before the manifest is read.
//!
//! The lease is node-local and single-writer per generation (the orchestrator
//! serializes a publication with the `Publishing` state). A process that dies
//! mid-publication leaves the file behind; [`clear_publish_lease`] runs at boot
//! for every retained record because no publication can be in flight then.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::SandboxId;

/// File name of the lease inside one paused generation directory.
pub(crate) const PUBLISH_LEASE_FILE: &str = "publish.lease";

const LEASE_VERSION: u32 = 1;

/// A held publish lease. Dropping it removes the file.
#[derive(Debug)]
pub struct PublishLease {
    path: PathBuf,
}

impl Drop for PublishLease {
    fn drop(&mut self) {
        remove_lease_file(&self.path);
    }
}

/// Take the lease for `generation_dir`.
///
/// The file is versioned and names the sandbox and start time so an operator
/// can tell a stale lease from a live one. It is written before the caller
/// reads the manifest, which is what makes the worker's candidate filter
/// sufficient.
pub fn acquire_publish_lease(
    generation_dir: &Path,
    sandbox_id: &SandboxId,
) -> io::Result<PublishLease> {
    let path = generation_dir.join(PUBLISH_LEASE_FILE);
    let started_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or(0);
    let body = serde_json::json!({
        "version": LEASE_VERSION,
        "sandboxId": sandbox_id.to_string(),
        "startedAtUnixMs": started_at_unix_ms,
    });
    let bytes = serde_json::to_vec(&body).map_err(io::Error::other)?;
    fs::write(&path, bytes)?;
    Ok(PublishLease { path })
}

/// Whether `generation_dir` currently holds a publish lease.
pub fn has_publish_lease(generation_dir: &Path) -> bool {
    generation_dir.join(PUBLISH_LEASE_FILE).is_file()
}

/// Remove a stale lease. Safe to call when none exists.
pub fn clear_publish_lease(generation_dir: &Path) {
    remove_lease_file(&generation_dir.join(PUBLISH_LEASE_FILE));
}

fn remove_lease_file(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                %error,
                "failed to remove paused publish lease"
            );
        }
    }
}
