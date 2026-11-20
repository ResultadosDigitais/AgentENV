package scheduler

import (
	"context"
	"testing"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"
)

// Shaped after the staging measurement of 2026-11-08 03:01Z: the runtime
// cgroup sat at 85-95% of memory.max while anon was 16-23 GiB and
// inactive_file 59-81 GiB. Raw memory.current is 90% of the base; the working
// set (memory.current - inactive_file) is 22%.
const (
	meterTotalBytes      = 120 * gib
	meterRawUsedBytes    = 108 * gib
	meterWorkingSetBytes = 27 * gib
)

func meterLimit(meter *string) *config.NodeResourceLimit {
	return &config.NodeResourceLimit{
		MaxMemoryUsedPercent: uint32Ptr(85),
		MemoryCapacitySource: strPtr(config.MemoryCapacitySourceTotal),
		MemoryUsedMeter:      meter,
	}
}

func meterNode(id string, workingSet *uint64) RichNode {
	return RichNode{
		Node: Node{ID: id},
		Snapshot: &schedulerv1.NodeSnapshot{
			MemoryTotalBytes:      meterTotalBytes,
			MemoryUsedBytes:       meterRawUsedBytes,
			MemoryWorkingSetBytes: workingSet,
		},
	}
}

func TestMemoryUsedMeterWorkingSetAdmitsMeasuredNode(t *testing.T) {
	node := meterNode("hxsd9", proto.Uint64(meterWorkingSetBytes))

	if got := FilterByResourceLimit([]RichNode{node}, meterLimit(strPtr(config.MemoryUsedMeterRaw))); len(got) != 0 {
		t.Fatalf("raw meter must refuse the measured node at 90%%, kept %d", len(got))
	}
	if got := FilterByResourceLimit([]RichNode{node}, meterLimit(strPtr(config.MemoryUsedMeterWorkingSet))); len(got) != 1 {
		t.Fatalf("working_set meter must admit the measured node at 22%%, kept %d", len(got))
	}
	verdict := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 8}, meterLimit(strPtr(config.MemoryUsedMeterWorkingSet)))
	if !verdict.Admitted {
		t.Fatalf("published verdict must read the same working set, got %+v", verdict)
	}
}

func TestMemoryUsedMeterWorkingSetRefusesAboveThreshold(t *testing.T) {
	node := meterNode("jvlgm", proto.Uint64(104*gib)) // 86% of 120 GiB
	var rejected []LimitViolation
	FilterByResourceLimitRejected([]RichNode{node}, meterLimit(strPtr(config.MemoryUsedMeterWorkingSet)), nil, func(_ string, v LimitViolation) {
		rejected = append(rejected, v)
	})
	if len(rejected) != 1 {
		t.Fatalf("expected one refusal, got %d", len(rejected))
	}
	got := rejected[0]
	if got.Limit != limitMaxMemoryUsedPercent || got.Observed != "86" || got.Meter != config.MemoryUsedMeterWorkingSet {
		t.Fatalf("unexpected violation %+v", got)
	}
	verdict := EvaluateLayerAdmission(node, config.AdmissionLayer{MemoryGB: 8}, meterLimit(strPtr(config.MemoryUsedMeterWorkingSet)))
	if verdict.Admitted || verdict.Reason != ReasonMemoryUsed {
		t.Fatalf("published verdict must refuse with memory_used, got %+v", verdict)
	}
}

func TestMemoryUsedMeterWorkingSetFallsBackToRawWhenAbsent(t *testing.T) {
	node := meterNode("q9rvm", nil)
	var rejected []LimitViolation
	var fellBack []string
	FilterByResourceLimitObservedBy([]RichNode{node}, meterLimit(strPtr(config.MemoryUsedMeterWorkingSet)), ResourceLimitObservers{
		Reject:        func(_ string, v LimitViolation) { rejected = append(rejected, v) },
		MeterFallback: func(nodeID string) { fellBack = append(fellBack, nodeID) },
	})
	if len(fellBack) != 1 || fellBack[0] != "q9rvm" {
		t.Fatalf("fallback must be reported for the node, got %v", fellBack)
	}
	if len(rejected) != 1 || rejected[0].Observed != "90" || rejected[0].Meter != config.MemoryUsedMeterRaw {
		t.Fatalf("absent working set must be evaluated on raw, got %+v", rejected)
	}
}

func TestMemoryUsedMeterRawDefaultIgnoresWorkingSet(t *testing.T) {
	node := meterNode("v6qcb", proto.Uint64(meterWorkingSetBytes))
	var rejected []LimitViolation
	var fellBack []string
	FilterByResourceLimitObservedBy([]RichNode{node}, meterLimit(nil), ResourceLimitObservers{
		Reject:        func(_ string, v LimitViolation) { rejected = append(rejected, v) },
		MeterFallback: func(nodeID string) { fellBack = append(fellBack, nodeID) },
	})
	if len(rejected) != 1 || rejected[0].Observed != "90" || rejected[0].Meter != config.MemoryUsedMeterRaw {
		t.Fatalf("default meter must stay raw, got %+v", rejected)
	}
	if len(fellBack) != 0 {
		t.Fatalf("raw meter has nothing to fall back from, got %v", fellBack)
	}
}

func TestReservationProjectionAddsToReportedWorkingSetOnly(t *testing.T) {
	limit := meterLimit(strPtr(config.MemoryUsedMeterWorkingSet))
	service := newReservationService(t, limit, true, nil, "ws", "raw")
	withWorkingSet := emptyNode()
	withWorkingSet.MemoryWorkingSetBytes = proto.Uint64(10 * gib)
	service.reserve("ws", 8*gib, 2, "")
	service.reserve("raw", 8*gib, 2, "")

	projected := map[string]*schedulerv1.NodeSnapshot{}
	for _, node := range service.projectReservations([]RichNode{
		{Node: Node{ID: "ws"}, Snapshot: withWorkingSet},
		{Node: Node{ID: "raw"}, Snapshot: emptyNode()},
	}) {
		projected[node.ID] = node.Snapshot
	}
	if got := projected["ws"].GetMemoryWorkingSetBytes(); got != 18*gib {
		t.Fatalf("reserved bytes must count in the working set, got %d GiB", got/gib)
	}
	if projected["raw"].MemoryWorkingSetBytes != nil {
		t.Fatalf("projection must not invent a working set for a node that reports none")
	}
}

func TestScheduleLogsMeterOnRefusalAndFallback(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	service := NewService(
		zap.New(core),
		NewAtomicNodeRegistry([]Node{{ID: "nktxk", Endpoint: "http://nktxk"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
		WithNodeResourceLimit(meterLimit(strPtr(config.MemoryUsedMeterWorkingSet))),
	)
	heartbeatSnapshot(t, service, "nktxk", &schedulerv1.NodeSnapshot{
		MemoryTotalBytes: meterTotalBytes,
		MemoryUsedBytes:  meterRawUsedBytes,
	}, nil, nil)

	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{}); err == nil {
		t.Fatal("expected the only node to be refused on the raw fallback")
	}
	fallback := logs.FilterMessage("scheduler memory meter fell back to raw").All()
	if len(fallback) != 1 || fallback[0].ContextMap()["node_id"] != "nktxk" {
		t.Fatalf("expected one named fallback line for nktxk, got %v", fallback)
	}
	refusals := logs.FilterMessage("scheduler rejected node by resource limit").All()
	if len(refusals) != 1 {
		t.Fatalf("expected one refusal line, got %d", len(refusals))
	}
	if got := refusals[0].ContextMap()["meter"]; got != config.MemoryUsedMeterRaw {
		t.Fatalf("refusal meter = %v, want raw", got)
	}
}
