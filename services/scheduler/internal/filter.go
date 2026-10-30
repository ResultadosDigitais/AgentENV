package scheduler

import (
	"fmt"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"
)

// AdmissionObserver receives the shadow-mode comparison for each node whose
// "nominal" and "request" memory verdicts differ. It must not block.
type AdmissionObserver func(nodeID string, nominalAdmits, requestAdmits bool, detail string)

// FilterByResourceLimit removes nodes that exceed any configured resource
// threshold. Nodes without a heartbeat snapshot are always kept (they have no
// metrics to evaluate). A nil limit disables all filtering.
func FilterByResourceLimit(nodes []RichNode, limit *config.NodeResourceLimit) []RichNode {
	return FilterByResourceLimitObserved(nodes, limit, nil)
}

// FilterByResourceLimitObserved is FilterByResourceLimit with an optional
// observer invoked in shadow admission mode when the hypothetical "request"
// verdict differs from the effective "nominal" one.
func FilterByResourceLimitObserved(nodes []RichNode, limit *config.NodeResourceLimit, observe AdmissionObserver) []RichNode {
	if limit == nil {
		return nodes
	}

	result := make([]RichNode, 0, len(nodes))
	for _, n := range nodes {
		if n.Snapshot == nil {
			// No heartbeat yet — cannot evaluate limits; keep the node.
			result = append(result, n)
			continue
		}
		if limit.ResolvedAdmissionMode() == config.AdmissionModeShadow {
			observeShadowAdmission(n, limit, observe)
		}
		if !withinLimit(n, limit) {
			continue
		}
		result = append(result, n)
	}
	return result
}

func withinLimit(n RichNode, limit *config.NodeResourceLimit) bool {
	s := n.Snapshot

	if limit.MaxSandboxCount != nil && s.GetSandboxCount() > *limit.MaxSandboxCount {
		return false
	}
	if limit.MaxSandboxStartingCount != nil && s.GetSandboxStartingCount() > *limit.MaxSandboxStartingCount {
		return false
	}
	if limit.MaxCPUUsedPercent != nil && s.GetCpuPercent() > *limit.MaxCPUUsedPercent {
		return false
	}
	if limit.MaxCPUAllocatedPercent != nil {
		if s.GetCpuCount() > 0 {
			allocatedPercent := s.GetAllocatedCpu() * 100 / s.GetCpuCount()
			if allocatedPercent > *limit.MaxCPUAllocatedPercent {
				return false
			}
		}
	}
	if limit.MaxMemoryUsedPercent != nil {
		if s.GetMemoryTotalBytes() > 0 {
			usedPercent := uint32(s.GetMemoryUsedBytes() * 100 / s.GetMemoryTotalBytes())
			if usedPercent > *limit.MaxMemoryUsedPercent {
				return false
			}
		}
	}
	// Only "request" mode changes the effective decision; "nominal" (default)
	// and "shadow" decide with nominal figures, exactly as before.
	if !memoryAdmits(s, limit, limit.ResolvedAdmissionMode() == config.AdmissionModeRequest) {
		return false
	}

	// "Including paused" ceilings sum the active running set with the paused
	// reservations reported in the snapshot. A node exceeding any of these is
	// dropped from scheduling candidates regardless of whether the active-only
	// counters are within limits.
	if limit.MaxSandboxCountIncludingPaused != nil {
		total := s.GetSandboxCount() + s.GetPausedSandboxCount()
		if total > *limit.MaxSandboxCountIncludingPaused {
			return false
		}
	}
	if limit.MaxAllocatedCPUIncludingPaused != nil {
		total := s.GetAllocatedCpu() + s.GetPausedAllocatedCpu()
		if total > *limit.MaxAllocatedCPUIncludingPaused {
			return false
		}
	}
	if limit.MaxAllocatedMemoryBytesIncludingPaused != nil {
		total := s.GetAllocatedMemoryBytes() + s.GetPausedAllocatedMemoryBytes()
		if total > *limit.MaxAllocatedMemoryBytesIncludingPaused {
			return false
		}
	}
	return true
}

// requestedMemoryBytes returns the node's memory-request sum. Nodes that do
// not report requested_memory_bytes (older agents) report 0 while having
// allocated memory; any real request is >= 1 MiB, so that combination is
// unambiguous and falls back to the nominal figure.
func requestedMemoryBytes(s *schedulerv1.NodeSnapshot) uint64 {
	if s.GetRequestedMemoryBytes() == 0 && s.GetAllocatedMemoryBytes() > 0 {
		return s.GetAllocatedMemoryBytes()
	}
	return s.GetRequestedMemoryBytes()
}

// memoryAdmits evaluates the allocated-memory admission rules. With
// useRequest=false it is the historical nominal check. With useRequest=true
// max_memory_allocated_percent compares the sum of requests, and the sum of
// nominal limits is additionally capped at ratio * memory_total_bytes.
func memoryAdmits(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit, useRequest bool) bool {
	total := s.GetMemoryTotalBytes()
	if total == 0 {
		return true
	}
	if limit.MaxMemoryAllocatedPercent != nil {
		bytes := s.GetAllocatedMemoryBytes()
		if useRequest {
			bytes = requestedMemoryBytes(s)
		}
		if uint32(bytes*100/total) > *limit.MaxMemoryAllocatedPercent {
			return false
		}
	}
	if useRequest {
		ratio := limit.ResolvedMaxMemoryLimitOvercommitRatio()
		if float64(s.GetAllocatedMemoryBytes()) > ratio*float64(total) {
			return false
		}
	}
	return true
}

func observeShadowAdmission(n RichNode, limit *config.NodeResourceLimit, observe AdmissionObserver) {
	s := n.Snapshot
	nominal := memoryAdmits(s, limit, false)
	request := memoryAdmits(s, limit, true)
	outcome := "agree"
	switch {
	case nominal && !request:
		outcome = "request_would_reject"
	case !nominal && request:
		outcome = "request_would_admit"
	}
	schedulerAdmissionShadow.WithLabelValues(outcome).Inc()
	if outcome == "agree" || observe == nil {
		return
	}
	observe(n.ID, nominal, request, fmt.Sprintf(
		"allocated=%d requested=%d memory_total=%d used=%d",
		s.GetAllocatedMemoryBytes(), requestedMemoryBytes(s), s.GetMemoryTotalBytes(), s.GetMemoryUsedBytes()))
}
