package scheduler

import (
	"fmt"
	"strconv"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"
)

// AdmissionObserver receives the shadow-mode comparison for each node whose
// "nominal" and "request" memory verdicts differ. It must not block.
type AdmissionObserver func(nodeID string, nominalAdmits, requestAdmits bool, detail string)

// RejectionObserver receives the named reason a node was removed from the
// candidate list. It must not block.
type RejectionObserver func(nodeID string, violation LimitViolation)

// LimitViolation names the node_resource_limit key that rejected a node, with
// the observed value, the configured threshold and any extra context. Limit is
// the JSON key of the limit, so the scheduler log reads like the config.
type LimitViolation struct {
	Limit     string
	Observed  string
	Threshold string
	Detail    string
}

// Node resource limit keys, used as the named reason in the rejection log.
const (
	limitMaxSandboxCount                   = "max_sandbox_count"
	limitMaxSandboxStartingCount           = "max_sandbox_starting_count"
	limitMaxCPUUsedPercent                 = "max_cpu_used_percent"
	limitMaxCPUAllocatedPercent            = "max_cpu_allocated_percent"
	limitMaxMemoryUsedPercent              = "max_memory_used_percent"
	limitMaxMemoryAllocatedPercent         = "max_memory_allocated_percent"
	limitMaxMemoryLimitOvercommitRatio     = "max_memory_limit_overcommit_ratio"
	limitMaxDiskUsedPercent                = "max_disk_used_percent"
	limitMaxSandboxCountIncludingPaused    = "max_sandbox_count_including_paused"
	limitMaxAllocatedCPUIncludingPaused    = "max_allocated_cpu_including_paused"
	limitMaxAllocatedMemoryIncludingPaused = "max_allocated_memory_bytes_including_paused"
)

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
	return FilterByResourceLimitRejected(nodes, limit, observe, nil)
}

// FilterByResourceLimitRejected is FilterByResourceLimitObserved with an
// optional observer invoked once per rejected node, carrying the named limit
// that rejected it.
func FilterByResourceLimitRejected(nodes []RichNode, limit *config.NodeResourceLimit, observe AdmissionObserver, reject RejectionObserver) []RichNode {
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
		violation, violated := limitViolated(n, limit)
		if violated {
			if reject != nil {
				reject(n.ID, violation)
			}
			continue
		}
		result = append(result, n)
	}
	return result
}

// limitViolated returns the first limit the node exceeds, named by its config
// key. The order of the checks is the order the limits were introduced in, so a
// node can only ever be attributed to the same limit it was rejected by
// before.
func limitViolated(n RichNode, limit *config.NodeResourceLimit) (LimitViolation, bool) {
	s := n.Snapshot

	if limit.MaxSandboxCount != nil && s.GetSandboxCount() > *limit.MaxSandboxCount {
		return numericViolation(limitMaxSandboxCount, uint64(s.GetSandboxCount()), uint64(*limit.MaxSandboxCount)), true
	}
	if limit.MaxSandboxStartingCount != nil && s.GetSandboxStartingCount() > *limit.MaxSandboxStartingCount {
		return numericViolation(limitMaxSandboxStartingCount, uint64(s.GetSandboxStartingCount()), uint64(*limit.MaxSandboxStartingCount)), true
	}
	if limit.MaxCPUUsedPercent != nil && s.GetCpuPercent() > *limit.MaxCPUUsedPercent {
		return numericViolation(limitMaxCPUUsedPercent, uint64(s.GetCpuPercent()), uint64(*limit.MaxCPUUsedPercent)), true
	}
	if limit.MaxCPUAllocatedPercent != nil {
		if s.GetCpuCount() > 0 {
			allocatedPercent := s.GetAllocatedCpu() * 100 / s.GetCpuCount()
			if allocatedPercent > *limit.MaxCPUAllocatedPercent {
				return numericViolation(limitMaxCPUAllocatedPercent, uint64(allocatedPercent), uint64(*limit.MaxCPUAllocatedPercent)), true
			}
		}
	}
	if limit.MaxMemoryUsedPercent != nil {
		if s.GetMemoryTotalBytes() > 0 {
			usedPercent := uint32(s.GetMemoryUsedBytes() * 100 / s.GetMemoryTotalBytes())
			if usedPercent > *limit.MaxMemoryUsedPercent {
				return numericViolation(limitMaxMemoryUsedPercent, uint64(usedPercent), uint64(*limit.MaxMemoryUsedPercent)), true
			}
		}
	}
	// The disk limit sits at the CPU level: like real CPU usage it measures a
	// physical resource the node cannot overcommit, so it applies in every
	// admission mode and never waits for the memory verdict.
	if limit.MaxDiskUsedPercent != nil {
		if violation, violated := diskViolation(s, limit); violated {
			return violation, true
		}
	}
	// Only "request" mode changes the effective decision; "nominal" (default)
	// and "shadow" decide with nominal figures, exactly as before.
	if violation, violated := memoryViolation(s, limit, limit.ResolvedAdmissionMode() == config.AdmissionModeRequest); violated {
		return violation, true
	}

	// "Including paused" ceilings sum the active running set with the paused
	// reservations reported in the snapshot. A node exceeding any of these is
	// dropped from scheduling candidates regardless of whether the active-only
	// counters are within limits.
	if limit.MaxSandboxCountIncludingPaused != nil {
		total := s.GetSandboxCount() + s.GetPausedSandboxCount()
		if total > *limit.MaxSandboxCountIncludingPaused {
			return numericViolation(limitMaxSandboxCountIncludingPaused, uint64(total), uint64(*limit.MaxSandboxCountIncludingPaused)), true
		}
	}
	if limit.MaxAllocatedCPUIncludingPaused != nil {
		total := s.GetAllocatedCpu() + s.GetPausedAllocatedCpu()
		if total > *limit.MaxAllocatedCPUIncludingPaused {
			return numericViolation(limitMaxAllocatedCPUIncludingPaused, uint64(total), uint64(*limit.MaxAllocatedCPUIncludingPaused)), true
		}
	}
	if limit.MaxAllocatedMemoryBytesIncludingPaused != nil {
		total := s.GetAllocatedMemoryBytes() + s.GetPausedAllocatedMemoryBytes()
		if total > *limit.MaxAllocatedMemoryBytesIncludingPaused {
			return numericViolation(limitMaxAllocatedMemoryIncludingPaused, total, *limit.MaxAllocatedMemoryBytesIncludingPaused), true
		}
	}
	return LimitViolation{}, false
}

// diskViolation compares the used/total ratio of the configured mount point.
// Only the exact mount point is read: the pod also exposes the config file
// through a subPath bind mount and several tmpfs entries, and none of those
// measure the node's sandbox volume. A node whose heartbeat does not carry the
// mount is admitted (fail-open) and counted, so an inert limit is visible in
// agentenv_scheduler_disk_admission_total instead of silently blocking nodes.
func diskViolation(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit) (LimitViolation, bool) {
	mountPoint := limit.ResolvedDiskMountPoint()
	var disk *schedulerv1.DiskMetric
	for _, candidate := range s.GetDisks() {
		if candidate.GetMountPoint() == mountPoint {
			disk = candidate
			break
		}
	}
	if disk == nil || disk.GetTotalBytes() == 0 {
		schedulerDiskAdmission.WithLabelValues("mount_missing").Inc()
		return LimitViolation{}, false
	}
	// Used/total as a percentage, kept in integer arithmetic so a node sitting
	// exactly on the threshold is not pushed over it by rounding.
	usedPercent := disk.GetUsedBytes() * 100 / disk.GetTotalBytes()
	outcome := "within_limit"
	if usedPercent > uint64(*limit.MaxDiskUsedPercent) {
		outcome = "over_limit"
	}
	schedulerDiskAdmission.WithLabelValues(outcome).Inc()
	if outcome != "over_limit" {
		return LimitViolation{}, false
	}
	return LimitViolation{
		Limit:     limitMaxDiskUsedPercent,
		Observed:  strconv.FormatUint(usedPercent, 10),
		Threshold: strconv.FormatUint(uint64(*limit.MaxDiskUsedPercent), 10),
		Detail:    "mount_point=" + mountPoint,
	}, true
}

// numericViolation names a limit whose observed and threshold values are
// plain integers (counts and percentages alike).
func numericViolation(limitName string, observed, threshold uint64) LimitViolation {
	return LimitViolation{
		Limit:     limitName,
		Observed:  strconv.FormatUint(observed, 10),
		Threshold: strconv.FormatUint(threshold, 10),
	}
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

// memoryViolation evaluates the allocated-memory admission rules. With
// useRequest=false it is the historical nominal check. With useRequest=true
// max_memory_allocated_percent compares the sum of requests, and the sum of
// nominal limits is additionally capped at ratio * memory_total_bytes.
func memoryViolation(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit, useRequest bool) (LimitViolation, bool) {
	total := s.GetMemoryTotalBytes()
	if total == 0 {
		return LimitViolation{}, false
	}
	if limit.MaxMemoryAllocatedPercent != nil {
		bytes := s.GetAllocatedMemoryBytes()
		if useRequest {
			bytes = requestedMemoryBytes(s)
		}
		if usedPercent := uint32(bytes * 100 / total); usedPercent > *limit.MaxMemoryAllocatedPercent {
			return numericViolation(limitMaxMemoryAllocatedPercent, uint64(usedPercent), uint64(*limit.MaxMemoryAllocatedPercent)), true
		}
	}
	if useRequest {
		ratio := limit.ResolvedMaxMemoryLimitOvercommitRatio()
		if float64(s.GetAllocatedMemoryBytes()) > ratio*float64(total) {
			return LimitViolation{
				Limit:     limitMaxMemoryLimitOvercommitRatio,
				Observed:  strconv.FormatUint(s.GetAllocatedMemoryBytes(), 10),
				Threshold: strconv.FormatFloat(ratio*float64(total), 'f', -1, 64),
				Detail:    fmt.Sprintf("memory_total=%d", total),
			}, true
		}
	}
	return LimitViolation{}, false
}

// memoryAdmits is the boolean view of memoryViolation, used by the shadow-mode
// comparison where only the verdict matters.
func memoryAdmits(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit, useRequest bool) bool {
	_, violated := memoryViolation(s, limit, useRequest)
	return !violated
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
