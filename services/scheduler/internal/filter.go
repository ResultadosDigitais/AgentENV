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

// DiskSkipObserver receives a node the disk limit could not evaluate because
// its heartbeat does not carry the target mount. The node is admitted
// (fail-open); reporting it keeps an inert limit visible in the log, not only
// in the outcome counter. It must not block.
type DiskSkipObserver func(nodeID string, violation LimitViolation)

// MeterFallbackObserver receives a node evaluated on the raw memory figure
// although memory_used_meter is working_set, because its heartbeat does not
// carry memory_working_set_bytes. It must not block.
type MeterFallbackObserver func(nodeID string)

// ResourceLimitObservers groups the optional observers of the resource limit
// filter. Every field may be nil.
type ResourceLimitObservers struct {
	Shadow        AdmissionObserver
	Reject        RejectionObserver
	SkipDisk      DiskSkipObserver
	MeterFallback MeterFallbackObserver
}

// LimitViolation names the node_resource_limit key that rejected a node, with
// the observed value, the configured threshold and any extra context. Limit is
// the JSON key of the limit, so the scheduler log reads like the config.
// Meter is the memory used meter that produced the node's memory figure.
type LimitViolation struct {
	Limit     string
	Observed  string
	Threshold string
	Detail    string
	Meter     string
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

// Admission reason codes. This is a closed set: a published verdict carries
// one of these five strings or no reason at all. The order below is also the
// precedence order of the published layer verdict.
const (
	ReasonMemoryAllocated = "memory_allocated"
	ReasonMemoryUsed      = "memory_used"
	ReasonCPU             = "cpu"
	ReasonStarting        = "starting"
	ReasonDisk            = "disk"
)

// LayerVerdict is the admission decision for one VM shape on one node.
type LayerVerdict struct {
	MemoryGB uint64
	Admitted bool
	Reason   string // empty when admitted
}

// capacityMemoryBytes resolves the memory base the admission ceiling is
// computed over. With the allocatable source and a node capacity available it
// is min(allocatable, memory_total) minus the runtime overhead, floored at
// zero; otherwise it is memory_total, exactly as before OPSAI-6200.
func capacityMemoryBytes(s *schedulerv1.NodeSnapshot, capacity *NodeCapacity, limit *config.NodeResourceLimit) (uint64, string) {
	total := s.GetMemoryTotalBytes()
	if limit.ResolvedMemoryCapacitySource() != config.MemoryCapacitySourceAllocatable ||
		capacity == nil || capacity.AllocatableMemoryBytes == 0 {
		return total, config.MemoryCapacitySourceTotal
	}
	base := capacity.AllocatableMemoryBytes
	if total > 0 && total < base {
		base = total
	}
	overhead := runtimeOverheadBytes(s)
	if overhead >= base {
		return 0, config.MemoryCapacitySourceAllocatable
	}
	return base - overhead, config.MemoryCapacitySourceAllocatable
}

// runtimeOverheadBytes estimates the runtime's own footprint on the node as
// memory_used - allocated. It includes page cache and guest anon the guest
// does not attribute to a sandbox, so it is conservative: it may defer a
// sandbox earlier than the measured demand suggests, never later.
func runtimeOverheadBytes(s *schedulerv1.NodeSnapshot) uint64 {
	used := s.GetMemoryUsedBytes()
	allocated := s.GetAllocatedMemoryBytes()
	if used <= allocated {
		return 0
	}
	return used - allocated
}

// projectSnapshotForLayer returns the snapshot with one VM shape added to the
// active allocated set. The original snapshot is never mutated.
func projectSnapshotForLayer(s *schedulerv1.NodeSnapshot, layer config.AdmissionLayer) *schedulerv1.NodeSnapshot {
	var cpuCount uint32
	if layer.CPUCount != nil {
		cpuCount = *layer.CPUCount
	}
	return projectSnapshotForShape(s, layer.MemoryGB<<30, cpuCount)
}

// projectSnapshotForShape projects an arbitrary shape (a configured layer or a
// paused sandbox's persisted resources) onto the active allocated set.
func projectSnapshotForShape(s *schedulerv1.NodeSnapshot, memoryBytes uint64, cpuCount uint32) *schedulerv1.NodeSnapshot {
	projected := cloneSnapshot(s)
	projected.AllocatedMemoryBytes += memoryBytes
	if projected.GetRequestedMemoryBytes() > 0 {
		projected.RequestedMemoryBytes += memoryBytes
	}
	projected.AllocatedCpu += cpuCount
	return projected
}

// EvaluateShapeAdmission decides whether an arbitrary shape fits on the node.
// It is the same capacity rule the published layer verdict uses.
func EvaluateShapeAdmission(n RichNode, memoryBytes uint64, cpuCount uint32, limit *config.NodeResourceLimit) (bool, string) {
	if limit == nil || n.Snapshot == nil {
		return true, ""
	}
	capacity, _ := capacityMemoryBytes(n.Snapshot, n.Capacity, limit)
	if capacity == 0 {
		return true, ""
	}
	reason := capacityViolation(projectSnapshotForShape(n.Snapshot, memoryBytes, cpuCount), limit, capacity)
	return reason == "", reason
}

// EvaluateLayerAdmission decides whether one VM shape fits on the node. It is
// the only place the published verdict is computed; the gateway serializes it
// and the contract test compares the service output with this function.
func EvaluateLayerAdmission(n RichNode, layer config.AdmissionLayer, limit *config.NodeResourceLimit) LayerVerdict {
	verdict := LayerVerdict{MemoryGB: layer.MemoryGB, Admitted: true}
	if limit == nil || n.Snapshot == nil {
		return verdict
	}
	capacity, _ := capacityMemoryBytes(n.Snapshot, n.Capacity, limit)
	if capacity == 0 {
		// No memory base to evaluate: keep the historical fail-open for a
		// node that never reported memory.
		return verdict
	}
	if reason := capacityViolation(projectSnapshotForLayer(n.Snapshot, layer), limit, capacity); reason != "" {
		verdict.Admitted = false
		verdict.Reason = reason
	}
	return verdict
}

// EvaluateNodeAdmission returns the verdict for every configured layer, or nil
// when the shape admission kill switch is off. It computes the verdict whenever
// the rule is observed (on or shadow): shadow exists to measure and publish the
// verdict without enforcing it, so the layers must be present there too. Only
// the scheduling filters gate a refusal on ShapeAdmissionEnabled, never this
// publication.
func EvaluateNodeAdmission(n RichNode, limit *config.NodeResourceLimit) []LayerVerdict {
	if limit == nil || !limit.ShapeAdmissionObserved() {
		return nil
	}
	layers := limit.ResolvedAdmissionLayers()
	verdicts := make([]LayerVerdict, 0, len(layers))
	for _, layer := range layers {
		verdicts = append(verdicts, EvaluateLayerAdmission(n, layer, limit))
	}
	return verdicts
}

// capacityViolation applies the published checks in their published order
// against an already-projected snapshot. It returns an empty string when the
// shape is admitted.
func capacityViolation(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit, capacity uint64) string {
	if limit == nil || s == nil || capacity == 0 {
		return ""
	}
	if _, _, over := memoryAllocatedCheck(s, limit, capacity, s.GetAllocatedMemoryBytes()); over {
		return ReasonMemoryAllocated
	}
	if _, _, over := memoryUsedCheck(s, limit, capacity); over {
		return ReasonMemoryUsed
	}
	if _, _, over := cpuUsedCheck(s, limit); over {
		return ReasonCPU
	}
	if _, _, over := cpuAllocatedCheck(s, limit); over {
		return ReasonCPU
	}
	if _, _, over := startingCheck(s, limit); over {
		return ReasonStarting
	}
	if limit.MaxDiskUsedPercent != nil {
		if _, _, evaluated, over := diskUsage(s, limit); evaluated && over {
			return ReasonDisk
		}
	}
	return ""
}

// percentOf is the integer percentage shared by every ratio check, so the
// layer verdict and the scheduling filter cannot round differently.
func percentOf(numerator, denominator uint64) uint32 {
	if denominator == 0 {
		return 0
	}
	return uint32(numerator * 100 / denominator)
}

// The *Check helpers are the single definition of every threshold comparison.
// Both the scheduling filter and the published layer verdict call them, so the
// two paths cannot drift.
func memoryAllocatedCheck(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit, base, bytes uint64) (uint32, uint32, bool) {
	if limit.MaxMemoryAllocatedPercent == nil || base == 0 {
		return 0, 0, false
	}
	observed := percentOf(bytes, base)
	return observed, *limit.MaxMemoryAllocatedPercent, observed > *limit.MaxMemoryAllocatedPercent
}

// memoryUsedBytes returns the figure max_memory_used_percent compares and the
// meter that produced it. The working set is read only when the meter asks
// for it and the node reports it; otherwise the raw memory_used applies.
func memoryUsedBytes(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit) (uint64, string) {
	if limit.ResolvedMemoryUsedMeter() == config.MemoryUsedMeterWorkingSet && s.MemoryWorkingSetBytes != nil {
		return s.GetMemoryWorkingSetBytes(), config.MemoryUsedMeterWorkingSet
	}
	return s.GetMemoryUsedBytes(), config.MemoryUsedMeterRaw
}

// memoryMeterFellBack reports a node evaluated on the raw figure although the
// working_set meter is configured.
func memoryMeterFellBack(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit) bool {
	_, meter := memoryUsedBytes(s, limit)
	return meter != limit.ResolvedMemoryUsedMeter()
}

func memoryUsedCheck(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit, base uint64) (uint32, uint32, bool) {
	if limit.MaxMemoryUsedPercent == nil || base == 0 {
		return 0, 0, false
	}
	used, _ := memoryUsedBytes(s, limit)
	observed := percentOf(used, base)
	return observed, *limit.MaxMemoryUsedPercent, observed > *limit.MaxMemoryUsedPercent
}

func cpuUsedCheck(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit) (uint32, uint32, bool) {
	if limit.MaxCPUUsedPercent == nil {
		return 0, 0, false
	}
	return s.GetCpuPercent(), *limit.MaxCPUUsedPercent, s.GetCpuPercent() > *limit.MaxCPUUsedPercent
}

func cpuAllocatedCheck(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit) (uint32, uint32, bool) {
	if limit.MaxCPUAllocatedPercent == nil || s.GetCpuCount() == 0 {
		return 0, 0, false
	}
	observed := percentOf(uint64(s.GetAllocatedCpu()), uint64(s.GetCpuCount()))
	return observed, *limit.MaxCPUAllocatedPercent, observed > *limit.MaxCPUAllocatedPercent
}

func startingCheck(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit) (uint32, uint32, bool) {
	if limit.MaxSandboxStartingCount == nil {
		return 0, 0, false
	}
	return s.GetSandboxStartingCount(), *limit.MaxSandboxStartingCount, s.GetSandboxStartingCount() > *limit.MaxSandboxStartingCount
}

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
	return FilterByResourceLimitReported(nodes, limit, observe, reject, nil)
}

// FilterByResourceLimitReported is FilterByResourceLimitRejected with an
// optional observer invoked for a node the disk limit admitted because its
// heartbeat does not carry the target mount.
func FilterByResourceLimitReported(nodes []RichNode, limit *config.NodeResourceLimit, observe AdmissionObserver, reject RejectionObserver, skipDisk DiskSkipObserver) []RichNode {
	return FilterByResourceLimitObservedBy(nodes, limit, ResourceLimitObservers{
		Shadow:   observe,
		Reject:   reject,
		SkipDisk: skipDisk,
	})
}

// FilterByResourceLimitObservedBy is the resource limit filter with every
// optional observer, including the memory meter fallback.
func FilterByResourceLimitObservedBy(nodes []RichNode, limit *config.NodeResourceLimit, observers ResourceLimitObservers) []RichNode {
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
			observeShadowAdmission(n, limit, observers.Shadow)
		}
		if observers.MeterFallback != nil && memoryMeterFellBack(n.Snapshot, limit) {
			observers.MeterFallback(n.ID)
		}
		violation, violated := limitViolated(n, limit)
		if violated {
			_, violation.Meter = memoryUsedBytes(n.Snapshot, limit)
			if observers.Reject != nil {
				observers.Reject(n.ID, violation)
			}
			continue
		}
		if skipped, ok := diskLimitSkipped(n.Snapshot, limit); ok && observers.SkipDisk != nil {
			observers.SkipDisk(n.ID, skipped)
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
	// Every memory check reads the same capacity base the published verdict
	// reads; without a node capacity it is memory_total, exactly as before.
	capacity, _ := capacityMemoryBytes(s, n.Capacity, limit)

	if limit.MaxSandboxCount != nil && s.GetSandboxCount() > *limit.MaxSandboxCount {
		return numericViolation(limitMaxSandboxCount, uint64(s.GetSandboxCount()), uint64(*limit.MaxSandboxCount)), true
	}
	if limit.MaxSandboxStartingCount != nil && s.GetSandboxStartingCount() > *limit.MaxSandboxStartingCount {
		return numericViolation(limitMaxSandboxStartingCount, uint64(s.GetSandboxStartingCount()), uint64(*limit.MaxSandboxStartingCount)), true
	}
	if observed, threshold, over := cpuUsedCheck(s, limit); over {
		return numericViolation(limitMaxCPUUsedPercent, uint64(observed), uint64(threshold)), true
	}
	if observed, threshold, over := cpuAllocatedCheck(s, limit); over {
		return numericViolation(limitMaxCPUAllocatedPercent, uint64(observed), uint64(threshold)), true
	}
	if observed, threshold, over := memoryUsedCheck(s, limit, capacity); over {
		return numericViolation(limitMaxMemoryUsedPercent, uint64(observed), uint64(threshold)), true
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
	if violation, violated := memoryViolation(s, limit, limit.ResolvedAdmissionMode() == config.AdmissionModeRequest, capacity); violated {
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

// diskLimitSkipped reports the fail-open case of the disk limit: the limit is
// configured, but the heartbeat carries no measurable metric for the target
// mount. It builds the payload for the log line only; the outcome counter is
// incremented once, by diskViolation.
func diskLimitSkipped(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit) (LimitViolation, bool) {
	if limit.MaxDiskUsedPercent == nil {
		return LimitViolation{}, false
	}
	mountPoint := limit.ResolvedDiskMountPoint()
	for _, candidate := range s.GetDisks() {
		if candidate.GetMountPoint() == mountPoint && candidate.GetTotalBytes() > 0 {
			return LimitViolation{}, false
		}
	}
	return LimitViolation{
		Limit:     limitMaxDiskUsedPercent,
		Threshold: strconv.FormatUint(uint64(*limit.MaxDiskUsedPercent), 10),
		Detail:    "mount_point=" + mountPoint + " reason=mount_missing",
	}, true
}

// diskViolation compares the used/total ratio of the configured mount point.
// Only the exact mount point is read: the pod also exposes the config file
// through a subPath bind mount and several tmpfs entries, and none of those
// measure the node's sandbox volume. A node whose heartbeat does not carry the
// mount is admitted (fail-open) and counted, so an inert limit is visible in
// agentenv_scheduler_disk_admission_total instead of silently blocking nodes.
func diskViolation(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit) (LimitViolation, bool) {
	usedPercent, mountPoint, evaluated, over := diskUsage(s, limit)
	if !evaluated {
		schedulerDiskAdmission.WithLabelValues("mount_missing").Inc()
		return LimitViolation{}, false
	}
	outcome := "within_limit"
	if over {
		outcome = "over_limit"
	}
	schedulerDiskAdmission.WithLabelValues(outcome).Inc()
	if !over {
		return LimitViolation{}, false
	}
	return LimitViolation{
		Limit:     limitMaxDiskUsedPercent,
		Observed:  strconv.FormatUint(usedPercent, 10),
		Threshold: strconv.FormatUint(uint64(*limit.MaxDiskUsedPercent), 10),
		Detail:    "mount_point=" + mountPoint,
	}, true
}

// diskUsage is the pure disk check shared by the scheduling filter and the
// published layer verdict. evaluated is false when the heartbeat carries no
// measurable metric for the target mount; over reports whether the usage is
// above the configured limit.
func diskUsage(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit) (usedPercent uint64, mountPoint string, evaluated, over bool) {
	mountPoint = limit.ResolvedDiskMountPoint()
	for _, candidate := range s.GetDisks() {
		if candidate.GetMountPoint() != mountPoint || candidate.GetTotalBytes() == 0 {
			continue
		}
		// Used/total as a percentage, kept in integer arithmetic so a node
		// sitting exactly on the threshold is not pushed over it by rounding.
		usedPercent = candidate.GetUsedBytes() * 100 / candidate.GetTotalBytes()
		return usedPercent, mountPoint, true, usedPercent > uint64(*limit.MaxDiskUsedPercent)
	}
	return 0, mountPoint, false, false
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

// memoryViolation evaluates the allocated-memory admission rules over the
// capacity base. With useRequest=false it is the historical nominal check.
// With useRequest=true max_memory_allocated_percent compares the sum of
// requests, and the sum of nominal limits is additionally capped at
// ratio * capacity.
func memoryViolation(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit, useRequest bool, capacity uint64) (LimitViolation, bool) {
	if capacity == 0 {
		return LimitViolation{}, false
	}
	bytes := s.GetAllocatedMemoryBytes()
	if useRequest {
		bytes = requestedMemoryBytes(s)
	}
	if observed, threshold, over := memoryAllocatedCheck(s, limit, capacity, bytes); over {
		return numericViolation(limitMaxMemoryAllocatedPercent, uint64(observed), uint64(threshold)), true
	}
	if useRequest {
		ratio := limit.ResolvedMaxMemoryLimitOvercommitRatio()
		if float64(s.GetAllocatedMemoryBytes()) > ratio*float64(capacity) {
			return LimitViolation{
				Limit:     limitMaxMemoryLimitOvercommitRatio,
				Observed:  strconv.FormatUint(s.GetAllocatedMemoryBytes(), 10),
				Threshold: strconv.FormatFloat(ratio*float64(capacity), 'f', -1, 64),
				Detail:    fmt.Sprintf("capacity=%d memory_total=%d", capacity, s.GetMemoryTotalBytes()),
			}, true
		}
	}
	return LimitViolation{}, false
}

// memoryAdmits is the boolean view of memoryViolation, used by the shadow-mode
// comparison where only the verdict matters.
func memoryAdmits(s *schedulerv1.NodeSnapshot, limit *config.NodeResourceLimit, useRequest bool, capacity uint64) bool {
	_, violated := memoryViolation(s, limit, useRequest, capacity)
	return !violated
}

func observeShadowAdmission(n RichNode, limit *config.NodeResourceLimit, observe AdmissionObserver) {
	s := n.Snapshot
	capacity, _ := capacityMemoryBytes(s, n.Capacity, limit)
	nominal := memoryAdmits(s, limit, false, capacity)
	request := memoryAdmits(s, limit, true, capacity)
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
