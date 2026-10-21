use serde::{Deserialize, Serialize};

pub fn bytes_to_mib_ceil(bytes: u64) -> u32 {
    bytes.div_ceil(1 << 20).try_into().unwrap_or(u32::MAX)
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxResources {
    pub cpu_count: u32,
    pub memory_mib: u32,
    pub disk_size_mib: u32,
    /// Memory request in MiB used by scheduler admission control
    /// (`admission_mode = request`). `None` means "same as `memory_mib`"
    /// (the nominal limit), which is the historical behavior. The VM is
    /// always booted with `memory_mib`; this value never changes VM size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_request_mib: Option<u32>,
}

impl SandboxResources {
    /// Effective memory request in MiB, clamped to the nominal limit so a
    /// request can never exceed what the VM can actually use.
    pub fn effective_memory_request_mib(&self) -> u32 {
        self.memory_request_mib
            .map_or(self.memory_mib, |req| req.min(self.memory_mib))
    }
}

impl Default for SandboxResources {
    fn default() -> Self {
        Self {
            cpu_count: 1,
            memory_mib: 128,
            disk_size_mib: 1024,
            memory_request_mib: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{bytes_to_mib_ceil, SandboxResources};

    #[test]
    fn effective_memory_request_defaults_to_limit_and_is_clamped() {
        let mut r = SandboxResources {
            memory_mib: 1024,
            ..Default::default()
        };
        assert_eq!(r.effective_memory_request_mib(), 1024);
        r.memory_request_mib = Some(256);
        assert_eq!(r.effective_memory_request_mib(), 256);
        r.memory_request_mib = Some(4096);
        assert_eq!(r.effective_memory_request_mib(), 1024);
    }

    #[test]
    fn memory_request_is_backward_compatible_in_json() {
        let legacy: SandboxResources =
            serde_json::from_str(r#"{"cpu_count":1,"memory_mib":512,"disk_size_mib":0}"#).unwrap();
        assert_eq!(legacy.memory_request_mib, None);
        let json = serde_json::to_string(&legacy).unwrap();
        assert!(!json.contains("memory_request_mib"));
    }

    #[test]
    fn bytes_to_mib_ceil_rounds_up() {
        assert_eq!(bytes_to_mib_ceil(0), 0);
        assert_eq!(bytes_to_mib_ceil(1), 1);
        assert_eq!(bytes_to_mib_ceil(1 << 20), 1);
        assert_eq!(bytes_to_mib_ceil((1 << 20) + 1), 2);
    }
}
