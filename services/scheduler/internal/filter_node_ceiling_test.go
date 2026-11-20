package scheduler

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/admission"
	"agentenv/services/shared/config"
)

// The same p-1 fixture the shared leaf loads, so the scheduler rule and the
// shared rule are proven against one set of vectors.
const nodeCeilingVectorsPath = "../../shared/admission/testdata/agentenv-admission-ceiling-vectors.json"

type ceilingVector struct {
	Name                   string  `json:"name"`
	Detail                 string  `json:"detail"`
	MemoryTotalBytes       uint64  `json:"memoryTotalBytes"`
	CPUCount               uint32  `json:"cpuCount"`
	MarginBytes            *uint64 `json:"marginBytes"`
	MarginAbsent           bool    `json:"marginAbsent"`
	ApplyLegacyCaps        bool    `json:"applyLegacyCaps"`
	WantReason             string  `json:"wantReason"`
	WantApplied            *bool   `json:"wantApplied"`
	WantMemoryCeilingBytes *uint64 `json:"wantMemoryCeilingBytes"`
	WantCPUCeilingCores    *uint32 `json:"wantCpuCeilingCores"`
}

type ceilingFixtures struct {
	MarginBytes            uint64          `json:"marginBytes"`
	MaxCPUAllocatedPercent uint32          `json:"maxCpuAllocatedPercent"`
	LegacyMemoryCapBytes   uint64          `json:"legacyMemoryCapBytes"`
	LegacyCPUCapCores      uint32          `json:"legacyCpuCapCores"`
	Vectors                []ceilingVector `json:"vectors"`
}

func loadCeilingFixtures(t *testing.T) ceilingFixtures {
	t.Helper()
	data, err := os.ReadFile(nodeCeilingVectorsPath)
	if err != nil {
		t.Fatalf("read %s: %v", nodeCeilingVectorsPath, err)
	}
	var fixtures ceilingFixtures
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatalf("decode %s: %v", nodeCeilingVectorsPath, err)
	}
	if len(fixtures.Vectors) == 0 {
		t.Fatalf("%s carries no vectors", nodeCeilingVectorsPath)
	}
	return fixtures
}

func (f ceilingFixtures) limitFor(vector ceilingVector) *config.NodeResourceLimit {
	limit := &config.NodeResourceLimit{MaxCPUAllocatedPercent: uint32Ptr(f.MaxCPUAllocatedPercent)}
	if !vector.MarginAbsent {
		margin := f.MarginBytes
		if vector.MarginBytes != nil {
			margin = *vector.MarginBytes
		}
		limit.MaxAllocatedMemoryMarginBytes = &margin
	}
	if vector.ApplyLegacyCaps {
		memoryCap := f.LegacyMemoryCapBytes
		cpuCap := f.LegacyCPUCapCores
		limit.MaxAllocatedMemoryBytesIncludingPaused = &memoryCap
		limit.MaxAllocatedCPUIncludingPaused = &cpuCap
	}
	return limit
}

func ceilingNode(vector ceilingVector) RichNode {
	return RichNode{
		Node: Node{ID: vector.Name},
		Snapshot: &schedulerv1.NodeSnapshot{
			MemoryTotalBytes: vector.MemoryTotalBytes,
			CpuCount:         vector.CPUCount,
		},
	}
}

// Every vector of the p-1 fixture drives the published capacity check: a
// memory allocation one byte above the effective including-paused ceiling is
// refused with memory_allocated, and one exactly at it is admitted. An
// unusable reading is refused by name, never failed open.
func TestNodeCeilingVectorsThroughPublishedVerdict(t *testing.T) {
	fixtures := loadCeilingFixtures(t)
	for _, vector := range fixtures.Vectors {
		t.Run(vector.Name, func(t *testing.T) {
			limit := fixtures.limitFor(vector)
			node := ceilingNode(vector)

			if vector.WantReason != "" {
				got := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 1}, limit)
				if got.Admitted || got.Reason != vector.WantReason {
					t.Fatalf("want refusal %q, got %+v (%s)", vector.WantReason, got, vector.Detail)
				}
				return
			}

			if vector.WantApplied != nil && !*vector.WantApplied {
				// Negative control: the margin key is absent, so the published
				// verdict keeps the historical fail-open and applies neither
				// ceiling; that is proven by TestNodeCeilingLegacyPathIsUnchanged.
				return
			}

			// The memory ceiling, with the excess parked in the paused set so
			// the active-only percent checks stay quiet.
			node.Snapshot.PausedAllocatedMemoryBytes = *vector.WantMemoryCeilingBytes + 1
			if got := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 0}, limit); got.Admitted || got.Reason != ReasonMemoryAllocated {
				t.Fatalf("one byte above the memory ceiling must be memory_allocated, got %+v (%s)", got, vector.Detail)
			}
			node.Snapshot.PausedAllocatedMemoryBytes = *vector.WantMemoryCeilingBytes
			if got := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 0}, limit); !got.Admitted {
				t.Fatalf("at the memory ceiling the layer must be admitted, got %+v (%s)", got, vector.Detail)
			}

			// The CPU ceiling, with the excess paused so max_cpu_allocated_percent
			// (the active-only check) cannot mask it.
			node = ceilingNode(vector)
			node.Snapshot.AllocatedCpu = 1
			node.Snapshot.PausedAllocatedCpu = *vector.WantCPUCeilingCores + 1
			if got := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 0}, limit); got.Admitted || got.Reason != ReasonCPU {
				t.Fatalf("one core above the cpu ceiling must be cpu, got %+v (%s)", got, vector.Detail)
			}
		})
	}
}

// The filter drops a node above the including-paused ceiling even while the
// shape rule is only observed (shadow), because the filter is the hard limit,
// not the published verdict.
func TestNodeCeilingFilterRejectsAboveCeilingInShadow(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxCPUAllocatedPercent:        uint32Ptr(200),
		MaxAllocatedMemoryMarginBytes: uint64Ptr(8 * gib),
		AdmissionMode:                 strPtr(config.AdmissionModeShadow),
	}
	// 100 GiB total - 8 GiB margin = 92 GiB ceiling.
	nodes := []RichNode{
		memNode("at-ceiling", 100*gib, 80*gib, 0, 0),
		memNode("above-ceiling", 100*gib, 85*gib, 0, 0),
	}
	nodes[0].Snapshot.CpuCount = 16
	nodes[0].Snapshot.PausedAllocatedMemoryBytes = 12 * gib // 80 + 12 = 92
	nodes[1].Snapshot.CpuCount = 16
	nodes[1].Snapshot.PausedAllocatedMemoryBytes = 13 * gib // 85 + 13 = 98

	var rejected []LimitViolation
	got := FilterByResourceLimitRejected(nodes, limit, nil, func(_ string, violation LimitViolation) {
		rejected = append(rejected, violation)
	})
	sameIDs(t, got, "at-ceiling")
	if len(rejected) != 1 || rejected[0].Limit != limitMaxAllocatedMemoryIncludingPaused {
		t.Fatalf("unexpected rejections: %+v", rejected)
	}
}

// The margin key absent preserves the historical decision byte for byte: the
// declared including-paused caps keep rejecting exactly as before, and the
// published verdict keeps NOT applying them (that is the behaviour the key
// switches on, not a silent default).
func TestNodeCeilingLegacyPathIsUnchanged(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxAllocatedMemoryBytesIncludingPaused: uint64Ptr(50 * gib),
		MaxAllocatedCPUIncludingPaused:         uint32Ptr(4),
	}
	node := memNode("legacy", 100*gib, 30*gib, 0, 0)
	node.Snapshot.CpuCount = 8
	node.Snapshot.PausedAllocatedMemoryBytes = 21 * gib // 51 GiB including paused > 50 GiB
	node.Snapshot.PausedAllocatedCpu = 1                // 1 core including paused <= 4

	if got := FilterByResourceLimit([]RichNode{node}, limit); len(got) != 0 {
		t.Fatalf("legacy declared cap must still reject, got %v", got)
	}

	node.Snapshot.PausedAllocatedMemoryBytes = 20 * gib // exactly 50 GiB
	node.Snapshot.AllocatedCpu = 4                      // 5 cores including paused > 4
	if got := FilterByResourceLimit([]RichNode{node}, limit); len(got) != 0 {
		t.Fatalf("legacy declared cpu cap must still reject, got %v", got)
	}

	node.Snapshot.PausedAllocatedCpu = 0 // 4 cores, at the cap
	if got := FilterByResourceLimit([]RichNode{node}, limit); len(got) != 1 {
		t.Fatalf("legacy node at both caps must be kept, got %v", got)
	}

	// Without the margin key the published verdict does not apply the
	// including-paused caps; that rule only exists once the key is declared.
	node.Snapshot.PausedAllocatedMemoryBytes = 80 * gib
	if got := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 0}, limit); !got.Admitted {
		t.Fatalf("legacy published verdict must stay fail-open on the including-paused cap, got %+v", got)
	}
}

// A snapshot that cannot produce the ceilings is refused by name instead of
// failed open, in both the filter and the published verdict.
func TestNodeCeilingCapacityUnknownRefuses(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxCPUAllocatedPercent:        uint32Ptr(200),
		MaxAllocatedMemoryMarginBytes: uint64Ptr(8 * gib),
	}
	node := memNode("no-total", 0, 10*gib, 0, 0)
	node.Snapshot.CpuCount = 16

	if got := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 1}, limit); got.Admitted || got.Reason != admission.ReasonCapacityUnknown {
		t.Fatalf("published verdict must be capacity_unknown, got %+v", got)
	}

	var rejected []LimitViolation
	got := FilterByResourceLimitRejected([]RichNode{node}, limit, nil, func(_ string, violation LimitViolation) {
		rejected = append(rejected, violation)
	})
	if len(got) != 0 {
		t.Fatalf("a node without memory total must be refused, got %v", got)
	}
	if len(rejected) != 1 || rejected[0].Limit != limitMaxAllocatedMemoryMargin || rejected[0].Detail != "reason="+admission.ReasonCapacityUnknown {
		t.Fatalf("unexpected rejection: %+v", rejected)
	}

	if ok, reason := EvaluateShapeAdmission(node, 8*gib, 2, limit); ok || reason != admission.ReasonCapacityUnknown {
		t.Fatalf("shape admission must refuse with capacity_unknown, got ok=%v reason=%q", ok, reason)
	}
}

// The create projection sees the paused reservations: a shape that fits the
// active set but crosses the including-paused ceiling is refused with the key
// declared, and keeps the historical fail-open without it.
func TestCreateShapeProjectionSeesPaused(t *testing.T) {
	shape := config.AdmissionLayer{MemoryGB: 8}
	node := capacityNode("projected", 100*gib, 100*gib, 84*gib, 84*gib)
	node.Snapshot.CpuCount = 16
	node.Snapshot.PausedAllocatedMemoryBytes = 8 * gib // 84 + 8 + 8 = 100 > 92

	legacy := &config.NodeResourceLimit{}
	if ok, reason := EvaluateShapeAdmission(node, shape.MemoryGB<<30, 0, legacy); !ok || reason != "" {
		t.Fatalf("without the margin key the projection stays fail-open, got ok=%v reason=%q", ok, reason)
	}

	limit := &config.NodeResourceLimit{
		MaxCPUAllocatedPercent:        uint32Ptr(200),
		MaxAllocatedMemoryMarginBytes: uint64Ptr(8 * gib),
	}
	if ok, reason := EvaluateShapeAdmission(node, shape.MemoryGB<<30, 0, limit); ok || reason != ReasonMemoryAllocated {
		t.Fatalf("with the margin key the projection must see paused and refuse, got ok=%v reason=%q", ok, reason)
	}
}

// Service level: a resolved create that crosses the per-node ceiling is
// refused in "on" mode even though every active-only figure fits; the same
// node without the key admits it, which is the rollback path.
func TestServiceCreateAboveNodeCeilingRefused(t *testing.T) {
	total := uint64(100) * gib
	limit := &config.NodeResourceLimit{
		MaxCPUAllocatedPercent:        uint32Ptr(200),
		MaxAllocatedMemoryMarginBytes: uint64Ptr(8 * gib),
		MemoryCapacitySource:          strPtr(config.MemoryCapacitySourceTotal),
		ShapeAdmission:                strPtr(config.ShapeAdmissionOn),
	}
	service := newReservationService(t, limit, true, nil, "n1")
	heartbeatSnapshot(t, service, "n1", &schedulerv1.NodeSnapshot{
		MemoryTotalBytes:           total,
		AllocatedMemoryBytes:       84 * gib,
		MemoryUsedBytes:            84 * gib,
		PausedAllocatedMemoryBytes: 8 * gib,
		CpuCount:                   16,
	}, nil, nil)

	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: resolvedHint(8, 2)}); err == nil {
		t.Fatalf("create crossing the including-paused ceiling must be refused")
	}

	limit.MaxAllocatedMemoryMarginBytes = nil
	service = newReservationService(t, limit, true, nil, "n1")
	heartbeatSnapshot(t, service, "n1", &schedulerv1.NodeSnapshot{
		MemoryTotalBytes:           total,
		AllocatedMemoryBytes:       84 * gib,
		MemoryUsedBytes:            84 * gib,
		PausedAllocatedMemoryBytes: 8 * gib,
		CpuCount:                   16,
	}, nil, nil)
	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: resolvedHint(8, 2)}); err != nil {
		t.Fatalf("without the margin key the same create must be admitted, got %v", err)
	}
}
