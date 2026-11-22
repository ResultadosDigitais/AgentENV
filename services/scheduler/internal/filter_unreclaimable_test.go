package scheduler

import (
	"testing"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"google.golang.org/protobuf/proto"
)

// Fixture measured on wzcn4 on 2026-11-22 around 06:50Z (OPSAI-6555): total
// 126 GiB, memory.current 117 GiB (92%), working set 114 GiB (90%) because
// active_file is 100 GiB, and roughly 24 GiB the container cannot reclaim
// (19%). The scheduler refused that node 75 times in 15 minutes on the working
// set; the unreclaimable figure admits it.
const (
	unreclaimableTotalBytes      = 126 * gib
	unreclaimableAllocatable     = 120 * gib
	unreclaimableRawBytes        = 117 * gib
	unreclaimableWorkingSetBytes = 114 * gib
	unreclaimableLiveBytes       = 24 * gib
	unreclaimableAllocatedBytes  = 24 * gib
)

func unreclaimableLimit() *config.NodeResourceLimit {
	return &config.NodeResourceLimit{
		MaxMemoryUsedPercent: uint32Ptr(85),
		MemoryCapacitySource: strPtr(config.MemoryCapacitySourceTotal),
		MemoryUsedMeter:      strPtr(config.MemoryUsedMeterUnreclaimable),
	}
}

func unreclaimableNode(id string, unreclaimable, workingSet *uint64) RichNode {
	return RichNode{
		Node: Node{ID: id},
		Snapshot: &schedulerv1.NodeSnapshot{
			MemoryTotalBytes:         unreclaimableTotalBytes,
			MemoryUsedBytes:          unreclaimableRawBytes,
			MemoryWorkingSetBytes:    workingSet,
			MemoryUnreclaimableBytes: unreclaimable,
		},
	}
}

// The wire field must round trip as an optional value and stay absent when the
// node does not report it, so a mixed fleet during the rollout is
// distinguishable from a node that measures zero.
func TestNodeSnapshotCarriesMemoryUnreclaimableBytes(t *testing.T) {
	original := &schedulerv1.NodeSnapshot{
		MemoryTotalBytes:         unreclaimableTotalBytes,
		MemoryUsedBytes:          unreclaimableRawBytes,
		MemoryWorkingSetBytes:    proto.Uint64(unreclaimableWorkingSetBytes),
		MemoryUnreclaimableBytes: proto.Uint64(unreclaimableLiveBytes),
	}
	encoded, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("marshal node snapshot: %v", err)
	}
	var decoded schedulerv1.NodeSnapshot
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal node snapshot: %v", err)
	}
	if decoded.MemoryUnreclaimableBytes == nil {
		t.Fatal("memory_unreclaimable_bytes must survive the round trip")
	}
	if got := decoded.GetMemoryUnreclaimableBytes(); got != unreclaimableLiveBytes {
		t.Fatalf("memory_unreclaimable_bytes = %d, want %d", got, unreclaimableLiveBytes)
	}

	absentEncoded, err := proto.Marshal(&schedulerv1.NodeSnapshot{MemoryTotalBytes: 1})
	if err != nil {
		t.Fatalf("marshal snapshot without the field: %v", err)
	}
	var absent schedulerv1.NodeSnapshot
	if err := proto.Unmarshal(absentEncoded, &absent); err != nil {
		t.Fatalf("unmarshal snapshot without the field: %v", err)
	}
	if absent.MemoryUnreclaimableBytes != nil {
		t.Fatalf("absent field must stay absent, got %v", *absent.MemoryUnreclaimableBytes)
	}
}

func TestUnreclaimableMeterAdmitsMeasuredNode(t *testing.T) {
	node := unreclaimableNode("wzcn4",
		proto.Uint64(unreclaimableLiveBytes),
		proto.Uint64(unreclaimableWorkingSetBytes))

	if got := FilterByResourceLimit([]RichNode{node}, meterLimit(strPtr(config.MemoryUsedMeterRaw))); len(got) != 0 {
		t.Fatalf("raw meter must refuse the measured node at 92%%, kept %d", len(got))
	}
	if got := FilterByResourceLimit([]RichNode{node}, meterLimit(strPtr(config.MemoryUsedMeterWorkingSet))); len(got) != 0 {
		t.Fatalf("working_set meter must refuse the measured node at 90%%, kept %d", len(got))
	}
	if got := FilterByResourceLimit([]RichNode{node}, unreclaimableLimit()); len(got) != 1 {
		t.Fatalf("unreclaimable meter must admit the measured node at 19%%, kept %d", len(got))
	}
	if verdict := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 8}, unreclaimableLimit()); !verdict.Admitted {
		t.Fatalf("published verdict must read the same unreclaimable figure, got %+v", verdict)
	}
}

// The refusal control has to sit on the threshold: 109 GiB of 126 GiB is 86%
// by the integer truncation percentOf uses (the same rule that reads 104 of
// 120 as 86 in the working set test), so it is refused, while 108 GiB is 85%
// and the check is ">", not ">=", so it is admitted. The 6qb9t measurement
// cited in the card (anon + mapped + kernel around 70 GiB of 126) sits at
// 55.6% and is therefore admitted by every meter; it cannot be the refusal
// control.
func TestUnreclaimableMeterRefusesAboveThreshold(t *testing.T) {
	var rejected []LimitViolation
	FilterByResourceLimitRejected(
		[]RichNode{unreclaimableNode("above", proto.Uint64(109*gib), nil)},
		unreclaimableLimit(),
		nil,
		func(_ string, v LimitViolation) { rejected = append(rejected, v) },
	)
	if len(rejected) != 1 {
		t.Fatalf("expected one refusal, got %d", len(rejected))
	}
	got := rejected[0]
	if got.Limit != limitMaxMemoryUsedPercent || got.Observed != "86" || got.Meter != config.MemoryUsedMeterUnreclaimable {
		t.Fatalf("unexpected violation %+v", got)
	}

	atThreshold := []RichNode{unreclaimableNode("at", proto.Uint64(108*gib), nil)}
	if kept := FilterByResourceLimit(atThreshold, unreclaimableLimit()); len(kept) != 1 {
		t.Fatalf("108 GiB of 126 GiB is 85%% and must be admitted, kept %d", len(kept))
	}
}

type meterFallbackEvent struct {
	nodeID string
	meter  string
	reason string
}

func TestUnreclaimableMeterFallsBackToWorkingSetWhenAbsent(t *testing.T) {
	node := unreclaimableNode("mixed", nil, proto.Uint64(unreclaimableWorkingSetBytes))
	var rejected []LimitViolation
	var fellBack []meterFallbackEvent
	FilterByResourceLimitObservedBy([]RichNode{node}, unreclaimableLimit(), ResourceLimitObservers{
		Reject: func(_ string, v LimitViolation) { rejected = append(rejected, v) },
		MeterFallback: func(nodeID, meter, reason string) {
			fellBack = append(fellBack, meterFallbackEvent{nodeID: nodeID, meter: meter, reason: reason})
		},
	})
	want := meterFallbackEvent{nodeID: "mixed", meter: config.MemoryUsedMeterWorkingSet, reason: "memory_unreclaimable_bytes_absent"}
	if len(fellBack) != 1 || fellBack[0] != want {
		t.Fatalf("fallback event = %v, want %v", fellBack, want)
	}
	if len(rejected) != 1 || rejected[0].Observed != "90" || rejected[0].Meter != config.MemoryUsedMeterWorkingSet {
		t.Fatalf("absent unreclaimable must be evaluated on the working set, got %+v", rejected)
	}
}

func TestUnreclaimableMeterFallsBackToRawWhenBothAbsent(t *testing.T) {
	node := unreclaimableNode("legacy", nil, nil)
	var rejected []LimitViolation
	var fellBack []meterFallbackEvent
	FilterByResourceLimitObservedBy([]RichNode{node}, unreclaimableLimit(), ResourceLimitObservers{
		Reject: func(_ string, v LimitViolation) { rejected = append(rejected, v) },
		MeterFallback: func(nodeID, meter, reason string) {
			fellBack = append(fellBack, meterFallbackEvent{nodeID: nodeID, meter: meter, reason: reason})
		},
	})
	want := meterFallbackEvent{nodeID: "legacy", meter: config.MemoryUsedMeterRaw, reason: "memory_unreclaimable_bytes_absent"}
	if len(fellBack) != 1 || fellBack[0] != want {
		t.Fatalf("fallback event = %v, want %v", fellBack, want)
	}
	if len(rejected) != 1 || rejected[0].Observed != "92" || rejected[0].Meter != config.MemoryUsedMeterRaw {
		t.Fatalf("a node without either field must be evaluated on raw, never admitted free, got %+v", rejected)
	}
}

func TestMemoryUsedMeterRawIgnoresUnreclaimable(t *testing.T) {
	node := unreclaimableNode("raw", proto.Uint64(unreclaimableLiveBytes), nil)
	var rejected []LimitViolation
	var fellBack []meterFallbackEvent
	FilterByResourceLimitObservedBy([]RichNode{node}, meterLimit(nil), ResourceLimitObservers{
		Reject: func(_ string, v LimitViolation) { rejected = append(rejected, v) },
		MeterFallback: func(nodeID, meter, reason string) {
			fellBack = append(fellBack, meterFallbackEvent{nodeID: nodeID, meter: meter, reason: reason})
		},
	})
	if len(rejected) != 1 || rejected[0].Observed != "92" || rejected[0].Meter != config.MemoryUsedMeterRaw {
		t.Fatalf("raw meter must keep reading memory_used_bytes, got %+v", rejected)
	}
	if len(fellBack) != 0 {
		t.Fatalf("raw meter has nothing to fall back from, got %v", fellBack)
	}
}

func TestReservationProjectionAddsToReportedUnreclaimableOnly(t *testing.T) {
	service := newReservationService(t, unreclaimableLimit(), true, nil, "ur", "raw")
	withUnreclaimable := emptyNode()
	withUnreclaimable.MemoryUnreclaimableBytes = proto.Uint64(10 * gib)
	service.reserve("ur", 8*gib, 2, "")
	service.reserve("raw", 8*gib, 2, "")

	projected := map[string]*schedulerv1.NodeSnapshot{}
	for _, node := range service.projectReservations([]RichNode{
		{Node: Node{ID: "ur"}, Snapshot: withUnreclaimable},
		{Node: Node{ID: "raw"}, Snapshot: emptyNode()},
	}) {
		projected[node.ID] = node.Snapshot
	}
	if got := projected["ur"].GetMemoryUnreclaimableBytes(); got != 18*gib {
		t.Fatalf("reserved bytes must count in the unreclaimable figure, got %d GiB", got/gib)
	}
	if projected["raw"].MemoryUnreclaimableBytes != nil {
		t.Fatalf("projection must not invent an unreclaimable figure for a node that reports none")
	}
}

func unreclaimableCapacityNode(id string) RichNode {
	node := capacityNode(id,
		unreclaimableTotalBytes,
		unreclaimableAllocatable,
		unreclaimableAllocatedBytes,
		unreclaimableRawBytes)
	node.Snapshot.MemoryWorkingSetBytes = proto.Uint64(unreclaimableWorkingSetBytes)
	node.Snapshot.MemoryUnreclaimableBytes = proto.Uint64(unreclaimableLiveBytes)
	return node
}

// With the allocatable source (the repository default) the base would collapse
// to 120 - (117 - 24) = 27 GiB on this node, and both checks would refuse at
// 88% (24/27): the meter alone would not admit the measured node. Under the
// unreclaimable meter the runtime's own pages are already in the numerator, so
// the base skips the raw overhead discount; raw and working_set keep today's
// math untouched.
func TestCapacityBaseSkipsRawOverheadUnderUnreclaimableMeter(t *testing.T) {
	node := unreclaimableCapacityNode("wzcn4")
	limit := &config.NodeResourceLimit{MemoryCapacitySource: strPtr(config.MemoryCapacitySourceAllocatable)}

	for _, meter := range []string{config.MemoryUsedMeterRaw, config.MemoryUsedMeterWorkingSet} {
		limit.MemoryUsedMeter = strPtr(meter)
		capacity, _ := capacityMemoryBytes(node.Snapshot, node.Capacity, limit)
		if capacity != 27*gib {
			t.Fatalf("%s capacity = %d GiB, want 27 GiB", meter, capacity/gib)
		}
		if overhead := runtimeOverheadBytes(node.Snapshot, limit); overhead != 93*gib {
			t.Fatalf("%s runtime overhead = %d GiB, want 93 GiB", meter, overhead/gib)
		}
	}

	limit.MemoryUsedMeter = strPtr(config.MemoryUsedMeterUnreclaimable)
	capacity, source := capacityMemoryBytes(node.Snapshot, node.Capacity, limit)
	if capacity != unreclaimableAllocatable || source != config.MemoryCapacitySourceAllocatable {
		t.Fatalf("unreclaimable capacity = %d GiB/%s, want %d GiB/allocatable",
			capacity/gib, source, unreclaimableAllocatable/gib)
	}
	if overhead := runtimeOverheadBytes(node.Snapshot, limit); overhead != 0 {
		t.Fatalf("unreclaimable runtime overhead = %d, want 0", overhead)
	}
}

func TestUnreclaimableMeterAdmitsMeasuredNodeUnderAllocatableSource(t *testing.T) {
	node := unreclaimableCapacityNode("wzcn4")
	limit := &config.NodeResourceLimit{
		MaxMemoryUsedPercent:      uint32Ptr(85),
		MaxMemoryAllocatedPercent: uint32Ptr(85),
		MemoryCapacitySource:      strPtr(config.MemoryCapacitySourceAllocatable),
		MemoryUsedMeter:           strPtr(config.MemoryUsedMeterUnreclaimable),
	}

	if got := FilterByResourceLimit([]RichNode{node}, limit); len(got) != 1 {
		t.Fatalf("unreclaimable meter with allocatable source must admit the measured node, kept %d", len(got))
	}
	if verdict := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 8}, limit); !verdict.Admitted {
		t.Fatalf("published verdict must admit under allocatable, got %+v", verdict)
	}
}
