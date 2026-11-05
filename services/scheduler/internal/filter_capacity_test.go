package scheduler

import (
	"testing"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"
)

const (
	mib = uint64(1) << 20
	// Measured on 05/11/2026: the node's total memory, the kubelet
	// allocatable under enforceNodeAllocatable=pods, the legacy 80% ceiling,
	// the runtime overhead and the capacity the new rule computes.
	measuredTotalMiB       = 128819
	measuredAllocatableMiB = 119336
	measuredLegacyCeilMiB  = 103055
	measuredOverheadMiB    = 4000
	measuredCapacityMiB    = 115336 // 119336 - 4000
	measuredCeilMiB        = 92268  // 80% of 115336
	measuredAllocatedMiB   = 90000
	layer8GiBMiB           = 8192
	measuredProjectedMiB   = 98192 // 90000 + 8192
)

func capacityNode(id string, totalBytes, allocatableBytes, allocatedBytes, usedBytes uint64) RichNode {
	return RichNode{
		Node: Node{ID: id},
		Snapshot: &schedulerv1.NodeSnapshot{
			MemoryTotalBytes:     totalBytes,
			AllocatedMemoryBytes: allocatedBytes,
			MemoryUsedBytes:      usedBytes,
		},
		Capacity: &NodeCapacity{AllocatableMemoryBytes: allocatableBytes, Source: "kubernetes"},
	}
}

func measuredNode(id string) RichNode {
	// memory_used = allocated + overhead, so the overhead derivation sees the
	// 4 GiB runtime footprint measured on 05/11.
	return capacityNode(id,
		measuredTotalMiB*mib,
		measuredAllocatableMiB*mib,
		measuredAllocatedMiB*mib,
		(measuredAllocatedMiB+measuredOverheadMiB)*mib)
}

func intPtr(v uint64) *uint64 { return &v }

// The 05/11 vector: the legacy rule over memory_total admits a 90 GiB node
// plus an 8 GiB layer (69% of 128819 MiB), while the capacity rule over
// allocatable minus overhead rejects it (85% of 115336 MiB).
func TestCapacityRuleMeasured0508LegacyAdmitsAndCapacityRejects(t *testing.T) {
	node := measuredNode("z9d4")
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(80),
		MemoryCapacitySource:      strPtr(config.MemoryCapacitySourceTotal),
	}
	legacy := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 8}, limit)
	if !legacy.Admitted {
		t.Fatalf("legacy total math must admit the 05/11 vector, got %+v", legacy)
	}

	limit.MemoryCapacitySource = strPtr(config.MemoryCapacitySourceAllocatable)
	got := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 8}, limit)
	if got.Admitted || got.Reason != ReasonMemoryAllocated {
		t.Fatalf("capacity math must reject with memory_allocated, got %+v", got)
	}

	capacity, source := capacityMemoryBytes(node.Snapshot, node.Capacity, limit)
	if source != config.MemoryCapacitySourceAllocatable {
		t.Fatalf("capacity source = %q", source)
	}
	if capacity != measuredCapacityMiB*mib {
		t.Fatalf("capacity = %d MiB, want %d MiB", capacity/mib, measuredCapacityMiB)
	}
	if got := percentOf(measuredProjectedMiB*mib, capacity); uint64(got) <= 80 {
		t.Fatalf("projected usage = %d%%, want > 80%%", got)
	}
	if legacy := measuredLegacyCeilMiB; percentOf(measuredProjectedMiB*mib, measuredTotalMiB*mib) > 80 {
		t.Fatalf("legacy projected usage must stay under 80%% (legacy ceiling %d MiB)", legacy)
	}
}

// Falling back to total must reproduce the historical behavior exactly.
func TestCapacityRuleFallsBackToTotalWithoutAllocatable(t *testing.T) {
	node := measuredNode("no-allocatable")
	node.Capacity = nil
	limit := &config.NodeResourceLimit{MaxMemoryAllocatedPercent: uint32Ptr(80)}
	capacity, source := capacityMemoryBytes(node.Snapshot, node.Capacity, limit)
	if source != config.MemoryCapacitySourceTotal || capacity != measuredTotalMiB*mib {
		t.Fatalf("fallback = %d/%s, want total/%d", capacity/mib, source, measuredTotalMiB)
	}
	if got := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 8}, limit); !got.Admitted {
		t.Fatalf("fallback must admit the legacy vector, got %+v", got)
	}
}

// The allocatable figure is capped by memory_total so a misreported
// allocatable cannot inflate the ceiling past the node's own memory.
func TestCapacityRuleCapsAllocatableAtTotal(t *testing.T) {
	snapshot := &schedulerv1.NodeSnapshot{MemoryTotalBytes: 100 * gib}
	capacity, _ := capacityMemoryBytes(snapshot,
		&NodeCapacity{AllocatableMemoryBytes: 200 * gib},
		&config.NodeResourceLimit{})
	if capacity != 100*gib {
		t.Fatalf("capacity = %d GiB, want 100 GiB", capacity/gib)
	}
}

// memory_used is the host backstop; the runtime overhead is included in the
// used figure and compared against capacity.
func TestCapacityRuleMemoryUsedReason(t *testing.T) {
	node := capacityNode("used-over", 100*gib, 100*gib, 10*gib, 90*gib)
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(95),
		MaxMemoryUsedPercent:      uint32Ptr(80),
	}
	got := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 8}, limit)
	if got.Admitted || got.Reason != ReasonMemoryUsed {
		t.Fatalf("want memory_used, got %+v", got)
	}
}

func TestCapacityRuleCPUReason(t *testing.T) {
	node := capacityNode("cpu-over", 100*gib, 100*gib, 0, 0)
	node.Snapshot.CpuPercent = 90
	node.Snapshot.CpuCount = 8
	limit := &config.NodeResourceLimit{MaxCPUUsedPercent: uint32Ptr(80)}
	got := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 8}, limit)
	if got.Admitted || got.Reason != ReasonCPU {
		t.Fatalf("want cpu, got %+v", got)
	}

	// The layer's cpu_count is projected into allocated CPU.
	node.Snapshot.CpuPercent = 0
	node.Snapshot.AllocatedCpu = 6
	limit = &config.NodeResourceLimit{MaxCPUAllocatedPercent: uint32Ptr(90)}
	got = EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 8, CPUCount: uint32Ptr(2)}, limit)
	if got.Admitted || got.Reason != ReasonCPU {
		t.Fatalf("projected 8/8 cpu must reject, got %+v", got)
	}
	got = EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 8, CPUCount: uint32Ptr(1)}, limit)
	if !got.Admitted {
		t.Fatalf("projected 7/8 cpu must admit, got %+v", got)
	}
}

func TestCapacityRuleStartingReason(t *testing.T) {
	node := capacityNode("starting", 100*gib, 100*gib, 0, 0)
	node.Snapshot.SandboxStartingCount = 3
	limit := &config.NodeResourceLimit{MaxSandboxStartingCount: uint32Ptr(2)}
	got := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 8}, limit)
	if got.Admitted || got.Reason != ReasonStarting {
		t.Fatalf("want starting, got %+v", got)
	}
}

func TestCapacityRuleDiskReason(t *testing.T) {
	node := capacityNode("disk", 100*gib, 100*gib, 0, 0)
	node.Snapshot.Disks = []*schedulerv1.DiskMetric{{MountPoint: "/workspace", UsedBytes: 80, TotalBytes: 100}}
	limit := &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(75)}
	got := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 8}, limit)
	if got.Admitted || got.Reason != ReasonDisk {
		t.Fatalf("want disk, got %+v", got)
	}
}

func TestCapacityRuleAdmitsUnderEveryLimit(t *testing.T) {
	node := capacityNode("roomy", 100*gib, 100*gib, 10*gib, 20*gib)
	node.Snapshot.Disks = []*schedulerv1.DiskMetric{{MountPoint: "/workspace", UsedBytes: 10, TotalBytes: 100}}
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(80),
		MaxMemoryUsedPercent:      uint32Ptr(80),
		MaxCPUUsedPercent:         uint32Ptr(80),
		MaxCPUAllocatedPercent:    uint32Ptr(80),
		MaxSandboxStartingCount:   uint32Ptr(2),
		MaxDiskUsedPercent:        uint32Ptr(75),
	}
	got := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 8}, limit)
	if !got.Admitted || got.Reason != "" {
		t.Fatalf("want admitted with no reason, got %+v", got)
	}
}

// The published reason precedence is memory_allocated, memory_used, cpu,
// starting, disk.
func TestCapacityRuleReasonPrecedence(t *testing.T) {
	base := func() RichNode {
		node := capacityNode("all-over", 100*gib, 100*gib, 90*gib, 99*gib)
		node.Snapshot.CpuPercent = 99
		node.Snapshot.CpuCount = 8
		node.Snapshot.AllocatedCpu = 8
		node.Snapshot.SandboxStartingCount = 9
		node.Snapshot.Disks = []*schedulerv1.DiskMetric{{MountPoint: "/workspace", UsedBytes: 99, TotalBytes: 100}}
		return node
	}
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(80),
		MaxMemoryUsedPercent:      uint32Ptr(80),
		MaxCPUUsedPercent:         uint32Ptr(80),
		MaxCPUAllocatedPercent:    uint32Ptr(80),
		MaxSandboxStartingCount:   uint32Ptr(2),
		MaxDiskUsedPercent:        uint32Ptr(75),
	}
	steps := []struct {
		name   string
		mutate func(*RichNode)
		want   string
	}{
		{"memory_allocated", func(*RichNode) {}, ReasonMemoryAllocated},
		{"memory_used", func(n *RichNode) {
			// overhead = 60 - 10 = 50 GiB, so capacity = 50 GiB and used is
			// 120% of it while allocated stays at 20%.
			n.Snapshot.AllocatedMemoryBytes = 10 * gib
			n.Snapshot.MemoryUsedBytes = 60 * gib
		}, ReasonMemoryUsed},
		{"cpu", func(n *RichNode) {
			// overhead = 40 - 10 = 30 GiB, capacity = 70 GiB, so both memory
			// checks pass and CPU is the first violated reason.
			n.Snapshot.AllocatedMemoryBytes = 10 * gib
			n.Snapshot.MemoryUsedBytes = 40 * gib
		}, ReasonCPU},
		{"starting", func(n *RichNode) {
			n.Snapshot.AllocatedMemoryBytes = 10 * gib
			n.Snapshot.MemoryUsedBytes = 40 * gib
			n.Snapshot.CpuPercent = 0
			n.Snapshot.AllocatedCpu = 0
		}, ReasonStarting},
		{"disk", func(n *RichNode) {
			n.Snapshot.AllocatedMemoryBytes = 10 * gib
			n.Snapshot.MemoryUsedBytes = 40 * gib
			n.Snapshot.CpuPercent = 0
			n.Snapshot.AllocatedCpu = 0
			n.Snapshot.SandboxStartingCount = 0
		}, ReasonDisk},
	}
	for _, step := range steps {
		node := base()
		step.mutate(&node)
		got := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 0}, limit)
		if got.Admitted || got.Reason != step.want {
			t.Fatalf("%s: got %+v, want reason %q", step.name, got, step.want)
		}
	}
}

// The contract: the verdict published for each layer is exactly the shared
// capacity check applied to the projected snapshot.
func TestPublishedVerdictMatchesSharedCapacityCheck(t *testing.T) {
	node := measuredNode("contract")
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(80),
		MaxMemoryUsedPercent:      uint32Ptr(90),
		AdmissionLayers: []config.AdmissionLayer{
			{MemoryGB: 8},
			{MemoryGB: 16},
			{MemoryGB: 32},
			{MemoryGB: 64},
		},
	}
	capacity, _ := capacityMemoryBytes(node.Snapshot, node.Capacity, limit)
	for _, verdict := range EvaluateNodeAdmission(node, limit) {
		layer := config.AdmissionLayer{MemoryGB: verdict.MemoryGB}
		projected := projectSnapshotForLayer(node.Snapshot, layer)
		wantReason := capacityViolation(projected, limit, capacity)
		if verdict.Admitted != (wantReason == "") || verdict.Reason != wantReason {
			t.Fatalf("layer %d: published %+v, shared check reason %q", verdict.MemoryGB, verdict, wantReason)
		}
	}
}

func TestShapeAdmissionKillSwitchDisablesVerdict(t *testing.T) {
	node := measuredNode("kill")
	limit := &config.NodeResourceLimit{ShapeAdmission: strPtr(config.ShapeAdmissionOff)}
	if got := EvaluateNodeAdmission(node, limit); got != nil {
		t.Fatalf("shape_admission off must publish no verdict, got %+v", got)
	}
}

func TestResumeVectorOwnerAtSeventyOfEightyGiBCeiling(t *testing.T) {
	// The node capacity is 100 GiB, so the 80% ceiling is 80 GiB. The owner
	// holds 70 GiB and the paused shape is 16 GiB.
	owner := capacityNode("owner", 100*gib, 100*gib, 70*gib, 70*gib)
	alternative := capacityNode("alt", 100*gib, 100*gib, 50*gib, 50*gib)
	limit := &config.NodeResourceLimit{MaxMemoryAllocatedPercent: uint32Ptr(80)}
	shape := config.AdmissionLayer{MemoryGB: 16}

	if got := EvaluateLayerAdmission(owner, shape, limit); got.Admitted || got.Reason != ReasonMemoryAllocated {
		t.Fatalf("owner must reject the 16 GiB resume, got %+v", got)
	}
	if got := EvaluateLayerAdmission(alternative, shape, limit); !got.Admitted {
		t.Fatalf("alternative must admit the 16 GiB resume, got %+v", got)
	}
}
