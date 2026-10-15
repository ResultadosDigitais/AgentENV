//! Per-sandbox CPU and memory inside the VM shape: request validation and
//! node admission for live resize.
//!
//! The VM shape (vCPUs, guest memory) is fixed at a VM's first boot and every
//! restore inherits it. A sandbox's effective limits move inside that shape;
//! see `docs/src/concepts/sandboxes/resources.md` for the mechanism.

use std::fs;

use tracing::warn;

use crate::cfg::ConfigManager;
use crate::sandbox::CpuQuota;
use crate::types::{ResourceLimits, SandboxResources};

/// A requested change to a sandbox's effective CPU and memory. A field left
/// `None` keeps its current value (the whole VM shape on create).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResourceRequest {
    pub cpu_count: Option<u32>,
    pub memory_mib: Option<u32>,
}

impl ResourceRequest {
    pub fn is_empty(&self) -> bool {
        self.cpu_count.is_none() && self.memory_mib.is_none()
    }
}

/// The node's CPUs and memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeCapacity {
    pub cpu_count: u32,
    pub memory_mib: u64,
}

impl NodeCapacity {
    /// Host CPUs and `MemTotal`, or `None` when either cannot be read.
    fn detect() -> Option<Self> {
        let cpu_count = std::thread::available_parallelism().ok()?.get();
        let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
        let memory_kib: u64 = meminfo
            .lines()
            .find_map(|line| line.strip_prefix("MemTotal:"))?
            .trim()
            .trim_end_matches("kB")
            .trim()
            .parse()
            .ok()?;
        Some(Self {
            cpu_count: u32::try_from(cpu_count).ok()?,
            memory_mib: memory_kib / 1024,
        })
    }
}

/// What the node allows: `[resize]` plus the node facts it depends on.
#[derive(Clone, Debug)]
pub struct ResizePolicy {
    pub enabled: bool,
    /// Whether startup prepared the cgroup v2 CPU quota subtree.
    pub cpu_quota_available: bool,
    pub min_memory_mib: u32,
    pub max_cpu_allocated_percent: Option<u32>,
    pub max_memory_allocated_percent: Option<u32>,
    /// `None` disables admission: the node's capacity is unknown.
    pub node: Option<NodeCapacity>,
}

impl ResizePolicy {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            cpu_quota_available: false,
            min_memory_mib: 0,
            max_cpu_allocated_percent: None,
            max_memory_allocated_percent: None,
            node: None,
        }
    }

    pub fn from_global_config() -> Self {
        let config = &ConfigManager::global_config().resize;
        if !config.enabled {
            return Self::disabled();
        }
        let node = NodeCapacity::detect();
        if node.is_none()
            && (config.max_cpu_allocated_percent.is_some()
                || config.max_memory_allocated_percent.is_some())
        {
            warn!("node capacity is unknown; resize admission limits are not enforced");
        }
        Self {
            enabled: true,
            cpu_quota_available: CpuQuota::global().is_some(),
            min_memory_mib: config.min_memory_mib,
            max_cpu_allocated_percent: config.max_cpu_allocated_percent,
            max_memory_allocated_percent: config.max_memory_allocated_percent,
            node,
        }
    }

    /// Limits for a sandbox launched from a VM of shape `shape`. `None` lets
    /// it use the whole shape.
    pub fn launch_limits(
        &self,
        shape: SandboxResources,
        request: ResourceRequest,
    ) -> Result<Option<ResourceLimits>, String> {
        let whole = ResourceLimits::whole(shape);
        let limits = self.target_limits(shape, whole, request)?;
        Ok((limits != whole).then_some(limits))
    }

    /// The limits a running sandbox of shape `shape` would get from `request`.
    pub fn resize_limits(
        &self,
        shape: SandboxResources,
        current: ResourceLimits,
        request: ResourceRequest,
    ) -> Result<ResourceLimits, String> {
        if !self.enabled {
            return Err("live resize is disabled on this node ([resize].enabled)".to_string());
        }
        if request.is_empty() {
            return Err("set cpuCount, memoryMB, or both".to_string());
        }
        self.target_limits(shape, current, request)
    }

    fn target_limits(
        &self,
        shape: SandboxResources,
        current: ResourceLimits,
        request: ResourceRequest,
    ) -> Result<ResourceLimits, String> {
        let limits = ResourceLimits {
            cpu_count: request.cpu_count.unwrap_or(current.cpu_count),
            memory_mib: request.memory_mib.unwrap_or(current.memory_mib),
        };
        if limits == ResourceLimits::whole(shape) {
            return Ok(limits);
        }
        if !self.enabled {
            return Err(format!(
                "cpuCount/memoryMB below the template's {} vCPUs / {} MiB need per-sandbox resources ([resize].enabled) on this node",
                shape.cpu_count, shape.memory_mib
            ));
        }
        if limits.cpu_count == 0 {
            return Err("cpuCount must be at least 1".to_string());
        }
        if limits.cpu_count > shape.cpu_count {
            return Err(format!(
                "cpuCount {} exceeds the VM's {} vCPUs; vCPUs cannot be added to a VM, so use a template built with more vCPUs",
                limits.cpu_count, shape.cpu_count
            ));
        }
        if limits.cpu_count < shape.cpu_count && !self.cpu_quota_available {
            return Err(format!(
                "cpuCount below the VM's {} vCPUs needs the cgroup v2 CPU quota, which is unavailable on this node",
                shape.cpu_count
            ));
        }
        if limits.memory_mib > shape.memory_mib {
            return Err(format!(
                "memoryMB {} exceeds the VM's {} MiB; guest memory cannot grow past the VM shape, so use a template built with more memory",
                limits.memory_mib, shape.memory_mib
            ));
        }
        let min_memory_mib = self.min_memory_mib.min(shape.memory_mib);
        if limits.memory_mib < min_memory_mib {
            return Err(format!(
                "memoryMB must be at least {min_memory_mib} ([resize].min_memory_mib)"
            ));
        }
        Ok(limits)
    }

    /// Admit growing a running sandbox from `current` to `target` on a node
    /// whose running sandboxes, this one included, are allocated
    /// `allocated_cpu` vCPUs and `allocated_memory_mib` MiB.
    pub fn admit_growth(
        &self,
        current: ResourceLimits,
        target: ResourceLimits,
        allocated_cpu: u64,
        allocated_memory_mib: u64,
    ) -> Result<(), String> {
        let Some(node) = self.node else {
            return Ok(());
        };
        if let Some(percent) = self.max_cpu_allocated_percent {
            if target.cpu_count > current.cpu_count {
                let after = allocated_cpu.saturating_sub(u64::from(current.cpu_count))
                    + u64::from(target.cpu_count);
                let limit = u64::from(node.cpu_count) * u64::from(percent) / 100;
                if after > limit {
                    return Err(format!(
                        "growing to {} vCPUs would allocate {after} of the node's {} vCPUs, above max_cpu_allocated_percent = {percent}",
                        target.cpu_count, node.cpu_count
                    ));
                }
            }
        }
        if let Some(percent) = self.max_memory_allocated_percent {
            if target.memory_mib > current.memory_mib {
                let after = allocated_memory_mib.saturating_sub(u64::from(current.memory_mib))
                    + u64::from(target.memory_mib);
                let limit = node.memory_mib * u64::from(percent) / 100;
                if after > limit {
                    return Err(format!(
                        "growing to {} MiB would allocate {after} of the node's {} MiB, above max_memory_allocated_percent = {percent}",
                        target.memory_mib, node.memory_mib
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(cpu_count: u32, memory_mib: u32) -> SandboxResources {
        SandboxResources {
            cpu_count,
            memory_mib,
            disk_size_mib: 10240,
        }
    }

    fn limits(cpu_count: u32, memory_mib: u32) -> ResourceLimits {
        ResourceLimits {
            cpu_count,
            memory_mib,
        }
    }

    fn request(cpu_count: Option<u32>, memory_mib: Option<u32>) -> ResourceRequest {
        ResourceRequest {
            cpu_count,
            memory_mib,
        }
    }

    fn enabled() -> ResizePolicy {
        ResizePolicy {
            enabled: true,
            cpu_quota_available: true,
            min_memory_mib: 512,
            max_cpu_allocated_percent: None,
            max_memory_allocated_percent: None,
            node: Some(NodeCapacity {
                cpu_count: 16,
                memory_mib: 65536,
            }),
        }
    }

    #[test]
    fn launch_limits_default_to_the_whole_shape() {
        let policy = ResizePolicy::disabled();
        assert_eq!(policy.launch_limits(shape(8, 16384), request(None, None)), Ok(None));
        assert_eq!(
            policy.launch_limits(shape(8, 16384), request(Some(8), Some(16384))),
            Ok(None),
            "asking for exactly the template shape needs no resize support"
        );
    }

    #[test]
    fn launch_limits_below_the_shape_need_resize_enabled() {
        let error = ResizePolicy::disabled()
            .launch_limits(shape(8, 16384), request(Some(4), None))
            .unwrap_err();
        assert!(error.contains("[resize].enabled"), "{error}");

        assert_eq!(
            enabled().launch_limits(shape(8, 16384), request(Some(4), Some(8192))),
            Ok(Some(limits(4, 8192)))
        );
        assert_eq!(
            enabled().launch_limits(shape(8, 16384), request(None, Some(8192))),
            Ok(Some(limits(8, 8192)))
        );
    }

    #[test]
    fn limits_cannot_exceed_the_vm_shape() {
        let error = enabled()
            .launch_limits(shape(8, 16384), request(Some(16), None))
            .unwrap_err();
        assert!(error.contains("exceeds the VM's 8 vCPUs"), "{error}");
        let error = enabled()
            .resize_limits(shape(8, 16384), limits(4, 8192), request(None, Some(32768)))
            .unwrap_err();
        assert!(error.contains("exceeds the VM's 16384 MiB"), "{error}");
    }

    #[test]
    fn limits_respect_minimums() {
        let policy = enabled();
        assert!(policy
            .resize_limits(shape(8, 16384), limits(8, 16384), request(Some(0), None))
            .unwrap_err()
            .contains("at least 1"));
        assert!(policy
            .resize_limits(shape(8, 16384), limits(8, 16384), request(None, Some(256)))
            .unwrap_err()
            .contains("at least 512"));
        // A VM smaller than the configured minimum keeps its whole memory.
        assert_eq!(
            policy.resize_limits(shape(2, 256), limits(2, 256), request(Some(1), None)),
            Ok(limits(1, 256))
        );
    }

    #[test]
    fn cpu_limits_below_the_shape_need_the_cpu_quota() {
        let policy = ResizePolicy {
            cpu_quota_available: false,
            ..enabled()
        };
        let error = policy
            .resize_limits(shape(8, 16384), limits(8, 16384), request(Some(4), None))
            .unwrap_err();
        assert!(error.contains("cgroup v2 CPU quota"), "{error}");
        assert_eq!(
            policy.resize_limits(shape(8, 16384), limits(8, 16384), request(None, Some(4096))),
            Ok(limits(8, 4096)),
            "memory-only resizes do not need the CPU quota"
        );
    }

    #[test]
    fn resize_requires_enabled_and_a_change() {
        let error = ResizePolicy::disabled()
            .resize_limits(shape(8, 16384), limits(8, 16384), request(Some(8), None))
            .unwrap_err();
        assert!(error.contains("disabled"), "{error}");
        let error = enabled()
            .resize_limits(shape(8, 16384), limits(8, 16384), request(None, None))
            .unwrap_err();
        assert!(error.contains("cpuCount, memoryMB"), "{error}");
    }

    #[test]
    fn resize_keeps_unrequested_fields() {
        assert_eq!(
            enabled().resize_limits(shape(8, 16384), limits(2, 4096), request(Some(6), None)),
            Ok(limits(6, 4096))
        );
    }

    #[test]
    fn admission_rejects_growth_past_the_configured_share() {
        let policy = ResizePolicy {
            max_cpu_allocated_percent: Some(200),
            max_memory_allocated_percent: Some(90),
            ..enabled()
        };
        // 16 CPUs at 200% admit 32 vCPUs; 64 GiB at 90% admit 58982 MiB.
        assert_eq!(
            policy.admit_growth(limits(2, 4096), limits(4, 8192), 30, 50000),
            Ok(())
        );
        let error = policy
            .admit_growth(limits(2, 4096), limits(6, 4096), 30, 50000)
            .unwrap_err();
        assert!(error.contains("34 of the node's 16 vCPUs"), "{error}");
        let error = policy
            .admit_growth(limits(2, 4096), limits(2, 16384), 30, 50000)
            .unwrap_err();
        assert!(error.contains("62288 of the node's 65536 MiB"), "{error}");
    }

    #[test]
    fn admission_never_blocks_shrinking() {
        let policy = ResizePolicy {
            max_cpu_allocated_percent: Some(10),
            max_memory_allocated_percent: Some(10),
            ..enabled()
        };
        assert_eq!(
            policy.admit_growth(limits(8, 16384), limits(2, 4096), 100, 1_000_000),
            Ok(())
        );
    }

    #[test]
    fn admission_is_skipped_without_node_capacity() {
        let policy = ResizePolicy {
            max_cpu_allocated_percent: Some(10),
            node: None,
            ..enabled()
        };
        assert_eq!(
            policy.admit_growth(limits(1, 512), limits(8, 512), 100, 0),
            Ok(())
        );
    }
}
