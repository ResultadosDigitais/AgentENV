use serde::{Deserialize, Serialize};

pub fn bytes_to_mib_ceil(bytes: u64) -> u32 {
    bytes.div_ceil(1 << 20).try_into().unwrap_or(u32::MAX)
}

/// The VM shape of a sandbox: what Firecracker was configured with when the
/// VM first booted. Every restore of that VM (template launch, snapshot
/// launch, resume, fork) inherits the shape unchanged.
#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxResources {
    pub cpu_count: u32,
    pub memory_mib: u32,
    pub disk_size_mib: u32,
}

impl Default for SandboxResources {
    fn default() -> Self {
        Self {
            cpu_count: 1,
            memory_mib: 128,
            disk_size_mib: 1024,
        }
    }
}

impl SandboxResources {
    /// The resources a sandbox may use: this VM shape narrowed by `limits`.
    /// `None` means the sandbox may use its whole VM shape.
    pub fn limited_by(self, limits: Option<ResourceLimits>) -> Self {
        match limits {
            Some(limits) => Self {
                cpu_count: limits.cpu_count,
                memory_mib: limits.memory_mib,
                ..self
            },
            None => self,
        }
    }
}

/// CPU and memory a running sandbox may use inside its VM shape.
///
/// Firecracker cannot change a VM's vCPU count or memory size after boot, so
/// live resize moves these effective limits within the shape instead: CPU
/// through a cgroup v2 `cpu.max` quota on the VMM process, memory through the
/// VM's virtio-balloon target. See `docs/src/concepts/sandboxes/resources.md`.
#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLimits {
    pub cpu_count: u32,
    pub memory_mib: u32,
}

impl ResourceLimits {
    /// The limits that let a sandbox use its whole VM shape.
    pub fn whole(shape: SandboxResources) -> Self {
        Self {
            cpu_count: shape.cpu_count,
            memory_mib: shape.memory_mib,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{bytes_to_mib_ceil, ResourceLimits, SandboxResources};

    #[test]
    fn bytes_to_mib_ceil_rounds_up() {
        assert_eq!(bytes_to_mib_ceil(0), 0);
        assert_eq!(bytes_to_mib_ceil(1), 1);
        assert_eq!(bytes_to_mib_ceil(1 << 20), 1);
        assert_eq!(bytes_to_mib_ceil((1 << 20) + 1), 2);
    }

    #[test]
    fn limited_by_replaces_cpu_and_memory_and_keeps_disk() {
        let shape = SandboxResources {
            cpu_count: 8,
            memory_mib: 16384,
            disk_size_mib: 20480,
        };
        assert_eq!(shape.limited_by(None), shape);
        assert_eq!(
            shape.limited_by(Some(ResourceLimits {
                cpu_count: 2,
                memory_mib: 4096,
            })),
            SandboxResources {
                cpu_count: 2,
                memory_mib: 4096,
                disk_size_mib: 20480,
            }
        );
        assert_eq!(
            shape.limited_by(Some(ResourceLimits::whole(shape))),
            shape
        );
    }
}
