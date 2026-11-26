package scheduler

import (
	"testing"

	"agentenv/services/shared/config"
)

// Measured reading: a 14 vCPU node running 8 active and 32 paused sandboxes at
// 1% real CPU usage. Paused sandboxes released their vCPUs, so their share must
// not enter a CPU ceiling. With the margin declared and no explicit
// including-paused cap, only the active checks apply: 8/14 = 57% <= 200% and
// 1% <= 90%, so the node is admitted.
func TestFilterAdmitsPausedCPUWithoutExplicitIncludingPausedCap(t *testing.T) {
	node := memNode("t82s5", 126*gib, 0, 0, 0)
	node.Snapshot.CpuCount = 14
	node.Snapshot.AllocatedCpu = 8
	node.Snapshot.PausedAllocatedCpu = 32
	node.Snapshot.CpuPercent = 1

	limit := &config.NodeResourceLimit{
		MaxCPUAllocatedPercent:        uint32Ptr(200),
		MaxCPUUsedPercent:             uint32Ptr(90),
		MaxAllocatedMemoryMarginBytes: uint64Ptr(8 * gib),
	}
	if got := FilterByResourceLimit([]RichNode{node}, limit); len(got) != 1 {
		t.Fatalf("a node with paused CPU but within the active checks must be admitted, got %v", got)
	}
}

// Control: the active CPU ceiling still refuses a node whose running sandboxes
// alone exceed max_cpu_allocated_percent (29/14 = 207% > 200%).
func TestFilterRefusesActiveCPUAboveAllocatedPercent(t *testing.T) {
	node := memNode("t82s5", 126*gib, 0, 0, 0)
	node.Snapshot.CpuCount = 14
	node.Snapshot.AllocatedCpu = 29
	node.Snapshot.PausedAllocatedCpu = 0
	node.Snapshot.CpuPercent = 1

	limit := &config.NodeResourceLimit{
		MaxCPUAllocatedPercent:        uint32Ptr(200),
		MaxAllocatedMemoryMarginBytes: uint64Ptr(8 * gib),
	}
	var rejected []LimitViolation
	got := FilterByResourceLimitRejected([]RichNode{node}, limit, nil, func(_ string, violation LimitViolation) {
		rejected = append(rejected, violation)
	})
	if len(got) != 0 {
		t.Fatalf("active CPU above the allocated percent must be refused, got %v", got)
	}
	if len(rejected) != 1 || rejected[0].Limit != limitMaxCPUAllocatedPercent || rejected[0].Observed != "207" {
		t.Fatalf("unexpected rejection: %+v", rejected)
	}
}

// Control: the memory ceiling counting paused sandboxes keeps its meaning with
// the margin (126 GiB - 8 GiB = 118 GiB; 100 + 20 = 120 GiB is refused).
func TestFilterRefusesMemoryAboveMarginCeilingCountingPaused(t *testing.T) {
	node := memNode("6qb9t", 126*gib, 100*gib, 0, 100*gib)
	node.Snapshot.CpuCount = 14
	node.Snapshot.PausedAllocatedMemoryBytes = 20 * gib

	limit := &config.NodeResourceLimit{
		MaxCPUAllocatedPercent:        uint32Ptr(200),
		MaxMemoryAllocatedPercent:     uint32Ptr(90),
		MaxAllocatedMemoryMarginBytes: uint64Ptr(8 * gib),
	}
	var rejected []LimitViolation
	got := FilterByResourceLimitRejected([]RichNode{node}, limit, nil, func(_ string, violation LimitViolation) {
		rejected = append(rejected, violation)
	})
	if len(got) != 0 {
		t.Fatalf("memory including paused above the margin ceiling must be refused, got %v", got)
	}
	if len(rejected) != 1 || rejected[0].Limit != limitMaxAllocatedMemoryIncludingPaused {
		t.Fatalf("unexpected rejection: %+v", rejected)
	}
}

// An explicit including-paused CPU cap is honoured even though the active-only
// check passes: 8 + 32 = 40 cores above a declared cap of 20.
func TestFilterRefusesIncludingPausedCPUAboveExplicitCap(t *testing.T) {
	node := memNode("t82s5", 126*gib, 0, 0, 0)
	node.Snapshot.CpuCount = 14
	node.Snapshot.AllocatedCpu = 8
	node.Snapshot.PausedAllocatedCpu = 32
	node.Snapshot.CpuPercent = 1

	limit := &config.NodeResourceLimit{
		MaxCPUAllocatedPercent:         uint32Ptr(200),
		MaxAllocatedCPUIncludingPaused: uint32Ptr(20),
		MaxAllocatedMemoryMarginBytes:  uint64Ptr(8 * gib),
	}
	var rejected []LimitViolation
	got := FilterByResourceLimitRejected([]RichNode{node}, limit, nil, func(_ string, violation LimitViolation) {
		rejected = append(rejected, violation)
	})
	if len(got) != 0 {
		t.Fatalf("including-paused CPU above the explicit cap must be refused, got %v", got)
	}
	if len(rejected) != 1 || rejected[0].Limit != limitMaxAllocatedCPUIncludingPaused {
		t.Fatalf("unexpected rejection: %+v", rejected)
	}
	if rejected[0].Observed != "40" || rejected[0].Threshold != "20" {
		t.Fatalf("observed/threshold = %s/%s, want 40/20", rejected[0].Observed, rejected[0].Threshold)
	}
}
