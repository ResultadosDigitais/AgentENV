// Package admission holds the per-node admission ceiling rule shared by the
// scheduler (which filters nodes and publishes the layer verdicts) and the
// gateway (which publishes the per-node payload). Keeping one function here is
// what keeps the decision and the publication from drifting apart.
package admission

import "agentenv/services/shared/config"

// ContractPerNodeCeilingV1 is the marker the per-node payload carries under
// admission.contract once the including-paused ceilings are computed from the
// node's own reading. A reader that sees max_allocated_memory_margin_bytes
// declared but no recognized marker must treat capacity as unknown, never as
// the legacy path: after activation an unreadable capacity never authorizes a
// placement.
const ContractPerNodeCeilingV1 = "per_node_ceiling_v1"

// ReasonCapacityUnknown is the named reason a node is refused when the
// per-node ceiling cannot be computed from its snapshot (no memory total or no
// CPU count) or when the configured margin is below the floor. The published
// reason stays a string; this is the new named member of the closed set.
const ReasonCapacityUnknown = "capacity_unknown"

// MinAllocatedMemoryMarginBytes is the floor of
// max_allocated_memory_margin_bytes: the governor's movement reserve (8 GiB).
// A smaller margin would let a node admit a shape that leaves no room for the
// runtime to move, so the leaf refuses it instead of honouring the number.
const MinAllocatedMemoryMarginBytes = uint64(8) << 30

// IncludingPausedCeilings is the pair of effective ceilings computed from the
// node's snapshot at admission time. Applied is false on the legacy path (the
// margin key absent): the declared caps behave exactly as before and no
// per-node ceiling is derived. When Applied is true, CPUKnown and MemoryKnown
// report which of the two ceilings exists.
type IncludingPausedCeilings struct {
	Applied     bool
	CPUKnown    bool
	CPUCores    uint32
	MemoryKnown bool
	MemoryBytes uint64
}

// IncludingPausedCeilingsOf is the single definition of the effective
// including-paused ceilings:
//
//	memory = min(declared memory cap, if any; max(0, memory_total_bytes - margin))
//	cpu    = min(declared cpu cap, if any; floor(cpu_count * max_cpu_allocated_percent / 100))
//
// The legacy caps of 121332826112 bytes and 32 cores enter ONLY as min: they
// can tighten the per-node ceiling and never raise it. With the margin key
// absent the function is on the legacy path and returns the declared caps
// unchanged, which is byte for byte the historical decision and the rollback.
//
// The margin below MinAllocatedMemoryMarginBytes is a floor, not a preference,
// and a snapshot without memory total or CPU count cannot produce the
// ceilings: both return ReasonCapacityUnknown so the caller refuses instead of
// failing open.
func IncludingPausedCeilingsOf(limit *config.NodeResourceLimit, memoryTotalBytes uint64, cpuCount uint32) (IncludingPausedCeilings, string) {
	if limit == nil {
		return IncludingPausedCeilings{}, ""
	}

	margin := limit.MaxAllocatedMemoryMarginBytes
	if margin == nil {
		ceilings := IncludingPausedCeilings{}
		if limit.MaxAllocatedCPUIncludingPaused != nil {
			ceilings.CPUKnown = true
			ceilings.CPUCores = *limit.MaxAllocatedCPUIncludingPaused
		}
		if limit.MaxAllocatedMemoryBytesIncludingPaused != nil {
			ceilings.MemoryKnown = true
			ceilings.MemoryBytes = *limit.MaxAllocatedMemoryBytesIncludingPaused
		}
		return ceilings, ""
	}

	ceilings := IncludingPausedCeilings{Applied: true}
	if *margin < MinAllocatedMemoryMarginBytes {
		return ceilings, ReasonCapacityUnknown
	}
	if memoryTotalBytes == 0 || cpuCount == 0 {
		return ceilings, ReasonCapacityUnknown
	}

	memory := memoryTotalBytes
	if *margin >= memory {
		memory = 0
	} else {
		memory -= *margin
	}
	if limit.MaxAllocatedMemoryBytesIncludingPaused != nil && *limit.MaxAllocatedMemoryBytesIncludingPaused < memory {
		memory = *limit.MaxAllocatedMemoryBytesIncludingPaused
	}
	ceilings.MemoryKnown = true
	ceilings.MemoryBytes = memory

	if limit.MaxCPUAllocatedPercent != nil {
		cores := uint64(cpuCount) * uint64(*limit.MaxCPUAllocatedPercent) / 100
		if cores > uint64(^uint32(0)) {
			cores = uint64(^uint32(0))
		}
		ceilings.CPUKnown = true
		ceilings.CPUCores = uint32(cores)
	}
	if limit.MaxAllocatedCPUIncludingPaused != nil {
		if declared := *limit.MaxAllocatedCPUIncludingPaused; !ceilings.CPUKnown || declared < ceilings.CPUCores {
			ceilings.CPUKnown = true
			ceilings.CPUCores = declared
		}
	}
	return ceilings, ""
}
