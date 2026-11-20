//! Per-VM memory containment through a cgroup v2 domain subtree.
//!
//! The memory controller only distributes to domain children, and a cgroup
//! that holds processes cannot delegate a domain controller. The container
//! cgroup holds the runtime server, so containment first moves those processes
//! into a leaf and then enables the controller for the subtree:
//!
//! ```text
//! <container cgroup>        domain, no processes, +memory +cpu
//! ├── agentenv-server/      domain leaf, the runtime's own processes
//! └── agentenv-vms/         domain, +memory +cpu (delegates to each VM)
//!     └── <sandbox id>/     domain, memory.max = shape, memory.high < shape
//!         └── cpu/          threaded, cpu.max (when [resize] caps the VMM)
//! ```
//!
//! A VM's VMM lives in its per-VM cgroup, or in the threaded `cpu` child once
//! its CPU time is capped; the memory limit on the parent covers both.
//!
//! `memory.oom.group` stays 0 on the container: an OOM inside a per-VM cgroup
//! kills only that VM's subtree and never the pod.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{bail, Context, Result};
use tracing::{debug, error, info, warn};

use super::cpu_quota::{own_cgroup_dir, read_trimmed};
use crate::types::SandboxId;

const CGROUP_MOUNT: &str = "/sys/fs/cgroup";
const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";
/// The domain subtree that holds one cgroup per VM.
const VMS_DIR: &str = "agentenv-vms";
/// The leaf that holds the runtime's own processes.
const SERVER_DIR: &str = "agentenv-server";

static GLOBAL: OnceLock<Option<MemoryContainment>> = OnceLock::new();

/// The node's per-VM memory containment subtree.
#[derive(Debug)]
pub struct MemoryContainment {
    root: PathBuf,
}

impl MemoryContainment {
    /// Prepare the domain subtree once at startup. A node that cannot provide
    /// it keeps running with today's shared-pod behavior; the reason is logged
    /// so a failed canary is never mistaken for containment in force.
    pub fn init_global(enabled: bool) {
        GLOBAL.get_or_init(|| {
            if !enabled {
                return None;
            }
            let proc_self_cgroup = match fs::read_to_string(PROC_SELF_CGROUP) {
                Ok(content) => content,
                Err(error) => {
                    error!(
                        %error,
                        "per-VM memory containment unavailable: cannot read {PROC_SELF_CGROUP}"
                    );
                    return None;
                }
            };
            match Self::setup(Path::new(CGROUP_MOUNT), &proc_self_cgroup) {
                Ok(containment) => {
                    info!(root = %containment.root.display(), "per-VM memory containment enabled");
                    Some(containment)
                }
                Err(error) => {
                    error!(
                        error = %format_args!("{error:#}"),
                        "per-VM memory containment unavailable; VMs keep sharing the runtime pod cgroup"
                    );
                    None
                }
            }
        });
    }

    /// The node's containment subtree, if startup prepared one.
    pub fn global() -> Option<&'static MemoryContainment> {
        GLOBAL.get().and_then(Option::as_ref)
    }

    fn setup(cgroup_mount: &Path, proc_self_cgroup: &str) -> Result<Self> {
        let own = own_cgroup_dir(cgroup_mount, proc_self_cgroup)?;
        let controllers = read_trimmed(&own.join("cgroup.controllers"))?;
        if !controllers.split_whitespace().any(|name| name == "memory") {
            bail!(
                "the memory controller is not available in {} (controllers: {controllers:?})",
                own.display()
            );
        }

        // A cgroup can have either domain or threaded children. A threaded
        // root left under the container by a previous incarnation would forbid
        // the domain subtree below; it is empty because startup reaped the
        // orphaned VMMs already.
        remove_empty_threaded_children(&own);

        let moved = move_processes_into_server(&own)?;
        let root = match prepare_subtree(&own) {
            Ok(root) => root,
            Err(error) => {
                // Delegation has to be withdrawn before the processes go back:
                // a cgroup that delegates a domain controller cannot take
                // processes of its own.
                rollback_delegation(&own);
                restore_processes(&own, &moved);
                return Err(error);
            }
        };
        let containment = Self { root };
        containment.remove_stale_cgroups();
        Ok(containment)
    }

    /// Create the cgroup a sandbox's VMM is moved into. `memory_max_bytes` is
    /// the VM shape and `high_ratio` the measured fraction of it that
    /// `memory.high` may reclaim down to.
    pub fn create(
        &self,
        sandbox_id: SandboxId,
        memory_max_bytes: u64,
        high_ratio: f64,
    ) -> Result<SandboxMemoryCgroup> {
        anyhow::ensure!(
            memory_max_bytes > 0,
            "a VM's memory shape must be greater than zero bytes"
        );
        anyhow::ensure!(
            high_ratio.is_finite() && high_ratio > 0.0 && high_ratio < 1.0,
            "memory.high ratio must be strictly between 0 and 1, got {high_ratio}"
        );
        let high_bytes = (memory_max_bytes as f64 * high_ratio) as u64;
        anyhow::ensure!(
            high_bytes < memory_max_bytes,
            "memory.high {high_bytes} must be below memory.max {memory_max_bytes}"
        );

        let path = self.root.join(sandbox_id.to_string());
        match fs::create_dir(&path) {
            Ok(()) => {}
            // Left over by a VMM of the same sandbox that is gone: reuse it.
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(error).with_context(|| format!("create {}", path.display()));
            }
        }
        write_control(&path.join("memory.max"), &memory_max_bytes.to_string())?;
        write_control(&path.join("memory.high"), &high_bytes.to_string())?;
        // An OOM inside this VM kills its whole subtree as a unit, so one VM
        // goes down and the runtime pod around it stays. The container's own
        // `memory.oom.group` is left at 0.
        write_control(&path.join("memory.oom.group"), "1")?;
        Ok(SandboxMemoryCgroup { path: Some(path) })
    }

    /// Remove per-VM cgroups left by a previous server incarnation. Their VMMs
    /// were reaped at startup, so they are empty; one still in use fails
    /// `rmdir` and is kept.
    fn remove_stale_cgroups(&self) {
        let Ok(entries) = fs::read_dir(&self.root) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            match fs::remove_dir(&path) {
                Ok(()) => debug!(path = %path.display(), "removed stale per-VM memory cgroup"),
                Err(error) => {
                    debug!(path = %path.display(), %error, "kept stale per-VM memory cgroup")
                }
            }
        }
    }
}

/// One VM's memory cgroup. Removed when dropped; it must be empty by then,
/// which holds once the VMM process has exited.
#[derive(Debug)]
pub struct SandboxMemoryCgroup {
    path: Option<PathBuf>,
}

impl SandboxMemoryCgroup {
    /// Path of the cgroup a CPU quota nests under.
    pub fn path(&self) -> &Path {
        self.path
            .as_deref()
            .expect("per-VM memory cgroup path is present until drop")
    }

    /// Move every thread of process `pid` into this cgroup.
    pub fn attach(&self, pid: u32) -> Result<()> {
        let procs = self.path().join("cgroup.procs");
        fs::write(&procs, pid.to_string())
            .with_context(|| format!("move process {pid} into {}", procs.display()))
    }
}

impl Drop for SandboxMemoryCgroup {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        if let Err(error) = fs::remove_dir(&path) {
            if error.kind() != io::ErrorKind::NotFound {
                warn!(path = %path.display(), %error, "failed to remove per-VM memory cgroup");
            }
        }
    }
}

fn prepare_subtree(own: &Path) -> Result<PathBuf> {
    // The container keeps no processes of its own once they moved into the
    // server leaf, so it may delegate both domain controllers.
    enable_controllers(own, &["memory", "cpu"])?;
    let root = own.join(VMS_DIR);
    match fs::create_dir(&root) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(error).with_context(|| format!("create {}", root.display()));
        }
    }
    // `memory` makes `memory.max`/`memory.high` available to each per-VM
    // cgroup; `cpu` lets a CPU-limited VMM nest a threaded cgroup under its
    // per-VM memory cgroup.
    enable_controllers(&root, &["memory", "cpu"])?;
    Ok(root)
}

fn enable_controllers(dir: &Path, names: &[&str]) -> Result<()> {
    let subtree_control = dir.join("cgroup.subtree_control");
    let current = read_trimmed(&subtree_control)?;
    let missing: Vec<&str> = names
        .iter()
        .copied()
        .filter(|name| !current.split_whitespace().any(|enabled| enabled == *name))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let request: String = missing.iter().map(|name| format!("+{name} ")).collect();
    fs::write(&subtree_control, request.trim_end())
        .with_context(|| format!("enable {missing:?} in {}", subtree_control.display()))
}

fn write_control(path: &Path, value: &str) -> Result<()> {
    fs::write(path, value).with_context(|| format!("write {}", path.display()))
}

/// Move every process out of the container cgroup into the server leaf.
///
/// A failure part-way leaves some processes in the leaf, so the ones already
/// moved are put back before returning the error.
fn move_processes_into_server(own: &Path) -> Result<Vec<u32>> {
    let pids = read_pids(&own.join("cgroup.procs"))?;
    if pids.is_empty() {
        return Ok(Vec::new());
    }
    let server = own.join(SERVER_DIR);
    match fs::create_dir(&server) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(error).with_context(|| format!("create {}", server.display()));
        }
    }
    let target = server.join("cgroup.procs");
    let mut moved = Vec::with_capacity(pids.len());
    for pid in pids {
        if let Err(error) = fs::write(&target, pid.to_string()) {
            restore_processes(own, &moved);
            return Err(error)
                .with_context(|| format!("move process {pid} into {}", target.display()));
        }
        moved.push(pid);
    }
    debug!(
        count = moved.len(),
        target = %target.display(),
        "moved runtime processes out of the container cgroup"
    );
    Ok(moved)
}

fn rollback_delegation(own: &Path) {
    let path = own.join("cgroup.subtree_control");
    let Ok(current) = read_trimmed(&path) else {
        return;
    };
    let disable: String = current
        .split_whitespace()
        .map(|name| format!("-{name} "))
        .collect();
    if disable.trim().is_empty() {
        return;
    }
    if let Err(error) = fs::write(&path, disable.trim_end()) {
        error!(
            path = %path.display(),
            %error,
            "failed to roll back cgroup controller delegation"
        );
    }
}

fn restore_processes(own: &Path, pids: &[u32]) {
    let target = own.join("cgroup.procs");
    for pid in pids {
        if let Err(error) = fs::write(&target, pid.to_string()) {
            error!(
                pid,
                target = %target.display(),
                %error,
                "failed to move a runtime process back after memory containment setup failed"
            );
        }
    }
}

fn remove_empty_threaded_children(own: &Path) {
    let Ok(entries) = fs::read_dir(own) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Ok(kind) = fs::read_to_string(path.join("cgroup.type")) else {
            continue;
        };
        if kind.trim() != "threaded" {
            continue;
        }
        let empty = read_pids(&path.join("cgroup.procs"))
            .map(|pids| pids.is_empty())
            .unwrap_or(false);
        if !empty {
            continue;
        }
        match fs::remove_dir(&path) {
            Ok(()) => debug!(path = %path.display(), "removed stale threaded cgroup root"),
            Err(error) => debug!(path = %path.display(), %error, "kept threaded cgroup root"),
        }
    }
}

fn read_pids(path: &Path) -> Result<Vec<u32>> {
    let content = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.trim()
                .parse::<u32>()
                .with_context(|| format!("parse process id {line:?} in {}", path.display()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// A directory tree that looks like cgroupfs to the code under test:
    /// plain files stand in for the interface files the kernel provides.
    fn fake_container(mount: &Path, controllers: &str, process: &str) -> Result<PathBuf> {
        let own = mount.join("kubepods/pod/container");
        fs::create_dir_all(&own)?;
        fs::write(own.join("cgroup.controllers"), controllers)?;
        fs::write(own.join("cgroup.subtree_control"), "")?;
        fs::write(own.join("cgroup.type"), "domain\n")?;
        fs::write(own.join("cgroup.procs"), process)?;
        Ok(own)
    }

    /// A kernel creates the interface files with the cgroup directory; the
    /// fake has to seed the subtree the test pre-creates.
    fn seed_vms_root(own: &Path) -> Result<()> {
        let root = own.join(VMS_DIR);
        fs::create_dir_all(&root)?;
        fs::write(root.join("cgroup.subtree_control"), "")?;
        Ok(())
    }

    #[test]
    fn setup_moves_the_server_and_prepares_the_domain_subtree() -> Result<()> {
        let mount = TempDir::new()?;
        let own = fake_container(mount.path(), "cpuset cpu io memory pids", "111\n")?;
        seed_vms_root(&own)?;

        let containment = MemoryContainment::setup(mount.path(), "0::/kubepods/pod/container\n")?;

        let root = own.join(VMS_DIR);
        assert_eq!(containment.root, root);
        assert_eq!(
            read_trimmed(&own.join("cgroup.subtree_control"))?,
            "+memory +cpu"
        );
        assert_eq!(
            read_trimmed(&own.join(SERVER_DIR).join("cgroup.procs"))?,
            "111"
        );
        assert_eq!(
            read_trimmed(&root.join("cgroup.subtree_control"))?,
            "+memory +cpu"
        );
        Ok(())
    }

    #[test]
    fn setup_fails_without_the_memory_controller() -> Result<()> {
        let mount = TempDir::new()?;
        let own = fake_container(mount.path(), "cpu pids", "111\n")?;

        let error =
            MemoryContainment::setup(mount.path(), "0::/kubepods/pod/container\n").unwrap_err();

        assert!(error.to_string().contains("memory controller"), "{error:#}");
        assert!(!own.join(VMS_DIR).exists());
        assert!(!own.join(SERVER_DIR).exists());
        Ok(())
    }

    #[test]
    fn create_writes_max_high_and_oom_group() -> Result<()> {
        let mount = TempDir::new()?;
        let own = fake_container(mount.path(), "cpu memory", "111\n")?;
        seed_vms_root(&own)?;
        let containment = MemoryContainment::setup(mount.path(), "0::/kubepods/pod/container\n")?;
        let shape_bytes = 2 * 1024 * 1024 * 1024;

        let cgroup = containment.create(SandboxId::new(), shape_bytes, 0.9)?;
        let path = cgroup.path();

        assert_eq!(
            read_trimmed(&path.join("memory.max"))?,
            "2147483648",
            "memory.max must equal the VM shape"
        );
        assert_eq!(read_trimmed(&path.join("memory.high"))?, "1932735283");
        assert_eq!(read_trimmed(&path.join("memory.oom.group"))?, "1");
        Ok(())
    }

    #[test]
    fn create_rejects_a_ratio_that_does_not_lower_the_ceiling() -> Result<()> {
        let mount = TempDir::new()?;
        let own = fake_container(mount.path(), "cpu memory", "111\n")?;
        seed_vms_root(&own)?;
        let containment = MemoryContainment::setup(mount.path(), "0::/kubepods/pod/container\n")?;

        for ratio in [0.0, 1.0, -0.1, f64::NAN] {
            let error = containment
                .create(SandboxId::new(), 1024 * 1024, ratio)
                .unwrap_err();
            assert!(error.to_string().contains("ratio"), "{error:#}");
        }
        Ok(())
    }

    #[test]
    fn setup_keeps_a_threaded_root_that_still_has_a_process() -> Result<()> {
        let mount = TempDir::new()?;
        let own = fake_container(mount.path(), "cpu memory", "111\n")?;
        seed_vms_root(&own)?;
        let stale = own.join("agentenv-sandboxes");
        fs::create_dir_all(&stale)?;
        fs::write(stale.join("cgroup.type"), "threaded\n")?;
        fs::write(stale.join("cgroup.procs"), "4242\n")?;

        MemoryContainment::setup(mount.path(), "0::/kubepods/pod/container\n")?;

        assert!(stale.exists(), "a threaded root with a live VMM is kept");
        Ok(())
    }
}
