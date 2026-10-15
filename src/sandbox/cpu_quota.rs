//! Per-sandbox CPU quota through a cgroup v2 threaded subtree.
//!
//! Firecracker cannot hot-unplug vCPUs, so a sandbox's effective CPU below
//! its VM shape is a `cpu.max` quota on the VMM process. Each limited
//! sandbox's VMM moves into its own cgroup under the server's cgroup:
//!
//! ```text
//! <server cgroup>                  domain threaded (keeps the server's processes)
//! └── agentenv-sandboxes           threaded, +cpu
//!     └── <sandbox id>             threaded, cpu.max = "<quota> <period>"
//! ```
//!
//! Threaded mode is what makes this possible inside a container: a domain
//! cgroup that holds processes cannot enable controllers for its children
//! (the cgroup v2 "no internal processes" rule), but a threaded subtree is
//! exempt. The server's cgroup keeps its processes, so runtimes can still
//! exec into it, and memory/IO stay accounted to it as before.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{bail, Context, Result};
use tracing::{debug, info, warn};

use crate::types::SandboxId;

const CGROUP_MOUNT: &str = "/sys/fs/cgroup";
const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";
const SANDBOXES_DIR: &str = "agentenv-sandboxes";
/// CFS bandwidth period: one vCPU of quota is one full period.
const CPU_PERIOD_US: u64 = 100_000;

static GLOBAL: OnceLock<Option<CpuQuota>> = OnceLock::new();

/// The cgroup that holds one cgroup per CPU-limited sandbox.
#[derive(Debug)]
pub struct CpuQuota {
    root: PathBuf,
}

impl CpuQuota {
    /// Prepare the threaded subtree once at startup. A node that cannot
    /// provide it keeps running without CPU limits; resizes that need one are
    /// rejected.
    pub fn init_global(enabled: bool) {
        GLOBAL.get_or_init(|| {
            if !enabled {
                return None;
            }
            let proc_self_cgroup = match fs::read_to_string(PROC_SELF_CGROUP) {
                Ok(content) => content,
                Err(error) => {
                    warn!(%error, "CPU quota unavailable: cannot read {PROC_SELF_CGROUP}");
                    return None;
                }
            };
            match Self::setup(Path::new(CGROUP_MOUNT), &proc_self_cgroup) {
                Ok(quota) => {
                    info!(root = %quota.root.display(), "per-sandbox CPU quota enabled");
                    Some(quota)
                }
                Err(error) => {
                    warn!(
                        error = %format_args!("{error:#}"),
                        "CPU quota unavailable; sandboxes cannot be limited below their vCPU count"
                    );
                    None
                }
            }
        });
    }

    /// The node's CPU quota subtree, if startup prepared one.
    pub fn global() -> Option<&'static CpuQuota> {
        GLOBAL.get().and_then(Option::as_ref)
    }

    fn setup(cgroup_mount: &Path, proc_self_cgroup: &str) -> Result<Self> {
        let own = own_cgroup_dir(cgroup_mount, proc_self_cgroup)?;
        let controllers = read_trimmed(&own.join("cgroup.controllers"))?;
        if !controllers.split_whitespace().any(|name| name == "cpu") {
            bail!(
                "the cpu controller is not available in {} (controllers: {controllers:?})",
                own.display()
            );
        }

        let root = own.join(SANDBOXES_DIR);
        match fs::create_dir(&root) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(error).with_context(|| format!("create {}", root.display()));
            }
        }
        make_threaded(&root)?;
        enable_cpu_controller(&own)?;
        enable_cpu_controller(&root)?;

        let quota = Self { root };
        quota.remove_stale_cgroups();
        Ok(quota)
    }

    /// Create the cgroup a sandbox's VMM is moved into.
    pub fn create(&self, sandbox_id: SandboxId) -> Result<SandboxCpuCgroup> {
        let path = self.root.join(sandbox_id.to_string());
        match fs::create_dir(&path) {
            Ok(()) => {}
            // Left over by a VMM of the same sandbox that is gone (resume
            // after pause): reuse it.
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(error).with_context(|| format!("create {}", path.display()));
            }
        }
        let cgroup = SandboxCpuCgroup { path: Some(path) };
        make_threaded(cgroup.path())?;
        Ok(cgroup)
    }

    /// Remove sandbox cgroups left by a previous server incarnation. Their
    /// VMMs were reaped at startup, so they are empty; one still in use
    /// fails `rmdir` and is kept.
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
                Ok(()) => debug!(path = %path.display(), "removed stale sandbox CPU cgroup"),
                Err(error) => {
                    warn!(path = %path.display(), %error, "kept stale sandbox CPU cgroup")
                }
            }
        }
    }
}

/// One sandbox's CPU cgroup. Removed when dropped; it must be empty by then,
/// which holds once the VMM process has exited.
#[derive(Debug)]
pub struct SandboxCpuCgroup {
    path: Option<PathBuf>,
}

impl SandboxCpuCgroup {
    fn path(&self) -> &Path {
        self.path
            .as_deref()
            .expect("sandbox CPU cgroup path is present until drop")
    }

    /// Move every thread of process `pid` into this cgroup.
    pub fn attach(&self, pid: u32) -> Result<()> {
        let procs = self.path().join("cgroup.procs");
        fs::write(&procs, pid.to_string())
            .with_context(|| format!("move process {pid} into {}", procs.display()))
    }

    /// Cap the cgroup at `cpus` vCPUs of CPU time, or lift the cap with `None`.
    pub fn set_cpu_limit(&self, cpus: Option<u32>) -> Result<()> {
        let cpu_max = self.path().join("cpu.max");
        fs::write(&cpu_max, format_cpu_max(cpus))
            .with_context(|| format!("write {}", cpu_max.display()))
    }

    /// The cap currently in force, in whole vCPUs (rounded up).
    pub fn cpu_limit(&self) -> Result<Option<u32>> {
        let cpu_max = self.path().join("cpu.max");
        parse_cpu_max(&read_trimmed(&cpu_max)?)
            .with_context(|| format!("parse {}", cpu_max.display()))
    }
}

impl Drop for SandboxCpuCgroup {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        if let Err(error) = fs::remove_dir(&path) {
            if error.kind() != io::ErrorKind::NotFound {
                warn!(path = %path.display(), %error, "failed to remove sandbox CPU cgroup");
            }
        }
    }
}

fn own_cgroup_dir(cgroup_mount: &Path, proc_self_cgroup: &str) -> Result<PathBuf> {
    let relative = proc_self_cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .context("no cgroup v2 entry in /proc/self/cgroup (cgroup v1 is not supported)")?;
    let dir = cgroup_mount.join(relative.trim().trim_start_matches('/'));
    anyhow::ensure!(
        dir.join("cgroup.controllers").is_file(),
        "{} is not a cgroup v2 directory",
        dir.display()
    );
    Ok(dir)
}

fn make_threaded(dir: &Path) -> Result<()> {
    let cgroup_type = dir.join("cgroup.type");
    if read_trimmed(&cgroup_type)? == "threaded" {
        return Ok(());
    }
    fs::write(&cgroup_type, "threaded").with_context(|| format!("make {} threaded", dir.display()))
}

fn enable_cpu_controller(dir: &Path) -> Result<()> {
    let subtree_control = dir.join("cgroup.subtree_control");
    if read_trimmed(&subtree_control)?
        .split_whitespace()
        .any(|name| name == "cpu")
    {
        return Ok(());
    }
    fs::write(&subtree_control, "+cpu")
        .with_context(|| format!("enable the cpu controller in {}", subtree_control.display()))
}

fn read_trimmed(path: &Path) -> Result<String> {
    fs::read_to_string(path)
        .map(|content| content.trim().to_string())
        .with_context(|| format!("read {}", path.display()))
}

fn format_cpu_max(cpus: Option<u32>) -> String {
    match cpus {
        Some(cpus) => format!("{} {CPU_PERIOD_US}", u64::from(cpus) * CPU_PERIOD_US),
        None => format!("max {CPU_PERIOD_US}"),
    }
}

fn parse_cpu_max(content: &str) -> Result<Option<u32>> {
    let mut fields = content.split_whitespace();
    let quota = fields.next().context("empty cpu.max")?;
    let period: u64 = fields
        .next()
        .context("cpu.max has no period")?
        .parse()
        .context("invalid cpu.max period")?;
    if quota == "max" {
        return Ok(None);
    }
    anyhow::ensure!(period > 0, "cpu.max period is zero");
    let quota: u64 = quota.parse().context("invalid cpu.max quota")?;
    let cpus = u32::try_from(quota.div_ceil(period)).context("cpu.max quota is too large")?;
    Ok(Some(cpus))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// A directory tree that looks like cgroupfs to the code under test:
    /// plain files stand in for the interface files the kernel provides.
    fn fake_cgroupfs(own: &str, controllers: &str) -> Result<(TempDir, PathBuf)> {
        let mount = TempDir::new()?;
        let own_dir = mount.path().join(own.trim_start_matches('/'));
        fs::create_dir_all(&own_dir)?;
        fs::write(own_dir.join("cgroup.controllers"), controllers)?;
        fs::write(own_dir.join("cgroup.subtree_control"), "")?;
        fs::write(own_dir.join("cgroup.type"), "domain\n")?;
        // A kernel creates these files with the directory; seed them for the
        // directories setup creates.
        let root = own_dir.join(SANDBOXES_DIR);
        fs::create_dir_all(&root)?;
        fs::write(root.join("cgroup.type"), "domain\n")?;
        fs::write(root.join("cgroup.subtree_control"), "")?;
        Ok((mount, own_dir))
    }

    #[test]
    fn own_cgroup_dir_follows_the_unified_entry() -> Result<()> {
        let (mount, own) = fake_cgroupfs("/kubepods.slice/pod-a/container-a", "cpu memory")?;
        let content = "1:name=systemd:/ignored\n0::/kubepods.slice/pod-a/container-a\n";
        assert_eq!(own_cgroup_dir(mount.path(), content)?, own);
        Ok(())
    }

    #[test]
    fn own_cgroup_dir_supports_a_private_cgroup_namespace() -> Result<()> {
        let mount = TempDir::new()?;
        fs::write(mount.path().join("cgroup.controllers"), "cpu")?;
        assert_eq!(own_cgroup_dir(mount.path(), "0::/\n")?, mount.path());
        Ok(())
    }

    #[test]
    fn own_cgroup_dir_rejects_cgroup_v1() {
        let mount = TempDir::new().unwrap();
        let error = own_cgroup_dir(mount.path(), "4:cpu,cpuacct:/docker/abc\n").unwrap_err();
        assert!(error.to_string().contains("cgroup v1"), "{error:#}");
    }

    #[test]
    fn setup_builds_a_threaded_subtree_with_the_cpu_controller() -> Result<()> {
        let (mount, own) = fake_cgroupfs("/agentenv", "cpuset cpu io memory pids")?;
        fs::create_dir(own.join(SANDBOXES_DIR).join("stale"))?;

        let quota = CpuQuota::setup(mount.path(), "0::/agentenv\n")?;

        let root = own.join(SANDBOXES_DIR);
        assert_eq!(quota.root, root);
        assert_eq!(read_trimmed(&root.join("cgroup.type"))?, "threaded");
        assert_eq!(read_trimmed(&own.join("cgroup.subtree_control"))?, "+cpu");
        assert_eq!(read_trimmed(&root.join("cgroup.subtree_control"))?, "+cpu");
        assert!(
            !root.join("stale").exists(),
            "empty stale cgroups are removed"
        );
        Ok(())
    }

    #[test]
    fn setup_requires_the_cpu_controller() -> Result<()> {
        let (mount, _own) = fake_cgroupfs("/agentenv", "memory pids")?;
        let error = CpuQuota::setup(mount.path(), "0::/agentenv\n").unwrap_err();
        assert!(error.to_string().contains("cpu controller"), "{error:#}");
        Ok(())
    }

    #[test]
    fn sandbox_cgroup_moves_the_vmm_and_writes_the_quota() -> Result<()> {
        let (mount, own) = fake_cgroupfs("/agentenv", "cpu")?;
        let quota = CpuQuota::setup(mount.path(), "0::/agentenv\n")?;
        let sandbox_id = SandboxId::new();
        let path = own.join(SANDBOXES_DIR).join(sandbox_id.to_string());
        fs::create_dir(&path)?;
        fs::write(path.join("cgroup.type"), "domain invalid\n")?;

        let cgroup = quota.create(sandbox_id)?;
        assert_eq!(read_trimmed(&path.join("cgroup.type"))?, "threaded");

        cgroup.attach(4242)?;
        assert_eq!(read_trimmed(&path.join("cgroup.procs"))?, "4242");

        cgroup.set_cpu_limit(Some(3))?;
        assert_eq!(read_trimmed(&path.join("cpu.max"))?, "300000 100000");
        assert_eq!(cgroup.cpu_limit()?, Some(3));

        cgroup.set_cpu_limit(None)?;
        assert_eq!(read_trimmed(&path.join("cpu.max"))?, "max 100000");
        assert_eq!(cgroup.cpu_limit()?, None);
        Ok(())
    }

    #[test]
    fn parse_cpu_max_rounds_partial_cpus_up() -> Result<()> {
        assert_eq!(parse_cpu_max("max 100000")?, None);
        assert_eq!(parse_cpu_max("200000 100000")?, Some(2));
        assert_eq!(parse_cpu_max("150000 100000")?, Some(2));
        assert!(parse_cpu_max("").is_err());
        assert!(parse_cpu_max("100000 0").is_err());
        Ok(())
    }
}
